---
slug: mcp-ranger-okta
title: "MCP with Apache Ranger and Okta on external data"
description: "SQE and Ranger only. Okta users reach SQE's MCP endpoint from Claude Code or Codex, Ranger policies per Okta group decide which Glue tables they see, and every setting is an environment variable ready for Fargate."
---

# MCP with Apache Ranger and Okta on external data

Two containers do the work: SQE and Apache Ranger (plus Ranger's Postgres).
There is no Keycloak, no Polaris and no RustFS. The data is yours: the AWS
Glue Data Catalog read live, files in S3. Okta is the only identity provider,
and nobody handles a token by hand: the MCP client opens the browser, the
user signs in to Okta, done.

```text
Claude Code / Codex --(browser login)--> Okta --access token--> SQE /mcp
SQE: token -> Session(user, groups) -> Ranger access policies -> Glue catalog -> S3 (engine credentials)
```

SQE runs with the same flags as a laptop gateway,
`sqe-server --glue-catalog glue --allow-all-buckets --mcp-only`, with Okta
in place of `--mcp-anonymous`. `GLUE_DATABASES` in `.env` scopes the catalog
to a comma-separated list of databases; empty exposes all of them.

Access is decided by SQE itself: a Glue table is readable when a Ranger
`select` grant matches the caller's user, roles or groups, and reads as not
found otherwise. The bootstrap writes two policies from two Okta groups:

| Okta group | Sees |
|---|---|
| `datalake_readonly` | every table except `SENSITIVE_TABLES` (explicit deny) |
| `datalake_sensitive_ro` | every table including `SENSITIVE_TABLES` |

Column masks and row filters come later from the same Ranger service.

## Run it

```bash
cp .env.example .env               # Okta issuer + client id, Glue databases, sensitive tables
aws sso login                      # or any other way to get credentials into the shell
eval "$(aws configure export-credentials --format env)"
./run.sh                           # starts the stack, protocol checks, writes .mcp.json
claude                             # /mcp -> sqe -> Authenticate: browser opens Okta
./run.sh --down
```

In Claude ask for `whoami`, `SHOW SCHEMAS FROM glue`,
`SHOW TABLES FROM glue.<database>` and a `SELECT ... LIMIT 5`. The Ranger UI
is on `http://localhost:26080` (admin, `RANGER_ADMIN_PASSWORD`).

## AWS credentials and region

SQE resolves tables from the Glue Data Catalog and reads their files from S3
with the credentials of its own process. Nothing in this directory holds a
key. Three ways to provide them:

| Where | How |
|---|---|
| Laptop, SSO | `aws sso login --profile <p>` then `eval "$(aws configure export-credentials --profile <p> --format env)"`. This exports `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`; Compose passes them into the container. |
| Laptop, static keys | `export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...` in the same shell. |
| Fargate | Leave them unset. The task role is picked up by the SDK credential chain inside the container. |

Session credentials expire (SSO: typically 1 to 12 hours). Expired credentials
show up as empty results or hangs, not as an error: re-export and
`docker compose up -d sqe` to hand the container the new values.

The region matters twice and both come from `AWS_REGION` in `.env`:
`--glue-region` for the catalog API and `SQE_STORAGE__S3_REGION` for the
buckets. Buckets in another region still work; the SDK follows the redirect.

The role or user needs `glue:GetDatabase(s)`, `glue:GetTable(s)`,
`glue:GetPartition(s)` and `s3:ListBucket` + `s3:GetObject` on the table
locations. `--allow-all-buckets` lets the engine read every bucket those
credentials reach; tighten later with `--allow-s3-prefix` or
`[storage.tvf] allowed_object_store_prefixes`.

## How the Okta login works

SQE's `/mcp` is an OAuth 2.1 resource server. It does not talk to Okta at
login time; it only verifies tokens.

1. The MCP client calls `/mcp` without a token and gets `401` plus a
   `WWW-Authenticate` header pointing at
   `/.well-known/oauth-protected-resource/mcp`, which names Okta as the
   authorization server.
2. The client reads Okta's metadata, opens the browser on Okta's `/authorize`
   with Authorization Code + PKCE and `redirect_uri=http://localhost:8400/callback`.
3. The user signs in (SSO, MFA, whatever the org policy says). Okta redirects to
   the client's loopback listener with a code; the client swaps it for an
   access token at Okta's `/token`. No client secret is involved: the app is a
   public, PKCE-only native client.
4. Every MCP call carries `Authorization: Bearer <token>`. SQE checks the
   signature against Okta's JWKS, `iss`, `aud` (= `mcp.resource_uri`) and
   `exp`, takes the user from `sub` and the roles/groups from the `groups`
   claim, and builds the session Ranger evaluates.
5. The client refreshes the token itself; the user sees the browser once.

Okta rejects anonymous dynamic client registration, so the client is told the
app's client ID up front. That is the only Okta value a user ever sees, and it
is not a secret.

## Okta setup

Five objects. Terraform in [`okta/`](./okta/) creates them; the manual list
is the same in console words.

### Terraform

```bash
cd okta
export OKTA_API_TOKEN=...              # Security > API > Tokens (admin, kept out of files)
terraform init
terraform apply -var org_name=<org>    # <org> from https://<org>.okta.com
terraform output                       # okta_issuer and okta_client_id for .env
```

Objects created by hand earlier are adopted, not duplicated:
`./import-ids.sh <org> <auth server id> <client id> > imports.tf` before the
first `plan`. Variables: `audience` (must equal `mcp.resource_uri`),
`redirect_uris` (loopback callbacks of the clients, exact ports),
`readonly_group`, `sensitive_group`, `groups_claim_regex`.

### By hand

1. **Groups** (Directory > Groups): `datalake_readonly` and
   `datalake_sensitive_ro`. Add your users. The names must equal the Ranger
   policy groups, case-sensitive.
2. **Authorization server** (Security > API > Authorization Servers > Add):
   name `sqemcp`, audience `http://localhost:19092/mcp`. The org server will
   not do: it issues opaque tokens SQE cannot verify.
   - Scopes: `sqe:mcp:read` (default scope) and `sqe:mcp:write`.
   - Claims: `groups`, include in **access token**, value type Groups, filter
     Matches regex `^datalake_.*`.
   - Access policy: assign to the app below (or all clients), one rule with
     grant types **Authorization Code**, Refresh Token and Device
     Authorization, any scope, any user. Without the policy Okta answers
     `access_denied` after login; without Authorization Code in the rule the
     browser says "You are not allowed to access this app".
3. **Application** (Applications > Create App Integration > OIDC > Native):
   name `sqemcp`, grant types Authorization Code + Refresh Token (+ Device
   Authorization for shells without a browser), sign-in redirect
   `http://localhost:8400/callback`, client authentication **None** with PKCE.
   Assign the two groups.
4. Copy the server's **Issuer** (`https://<org>.okta.com/oauth2/<server id>`)
   and the app's **Client ID** into `.env`.

Checks: `curl <issuer>/.well-known/oauth-authorization-server` lists the two
scopes; after a login, SQE logs `Bearer token authentication succeeded` with
the user and roles. Denials are explained in Okta's System Log
(`no_matching_policy`, user not assigned).

## Connect Claude Code or Codex

```bash
claude                # in this directory: run.sh wrote .mcp.json and .claude/ pre-approves it
# /mcp -> sqe -> Authenticate
# Codex: clients/codex-config.toml into ~/.codex/config.toml, then: codex mcp login sqe
```

Anywhere else: `claude mcp add --transport http --client-id "$OKTA_CLIENT_ID"
--callback-port 8400 sqe http://localhost:19092/mcp`. `clients/` holds both
templates. Any other MCP client that supports OAuth with a pre-registered
client ID works the same way; register its callback URL on the Okta app.

## Environment contract

Every variable in the compose `environment:` block is an
`SQE_<SECTION>__<FIELD>` override of [`sqe.toml`](./sqe.toml); a Fargate task
definition carries the same names. Arrays are addressed by index and the slot
must exist in the TOML (the single `[[auth.providers]]` entry is slot 0).

| Purpose | Where |
|---|---|
| Okta | `SQE_AUTH__PROVIDERS__0__ISSUER`, `__JWKS_URL`, `__AUDIENCE`, `SQE_MCP__AUTHORIZATION_SERVERS__0` |
| MCP | `SQE_MCP__RESOURCE_URI`, `SQE_MCP__ALLOW_WRITES`; `--mcp-only` in the command |
| Glue + S3 | `--glue-catalog`, `--glue-region`, `--glue-database`, `--allow-all-buckets` in the command; `SQE_STORAGE__S3_REGION`; `AWS_*` from the shell or the task role |
| Ranger | `SQE_POLICY__RANGER__URL`, `SQE_POLICY__RANGER__ADMIN_PASSWORD` |

The flags have TOML twins (`[[hive.roots]] kind = "glue"` with `databases`,
`[storage.tvf] allow_all_object_stores`, the SQL ports set to 0) for a
deployment that prefers a file over a command line.

## Fargate later

One task per container (SQE, Ranger Admin, Postgres or RDS), the same image
and TOML, the same variables with secrets from Secrets Manager, an ALB with TLS
in front of SQE's web port. Change `resource_uri`, `OKTA_AUDIENCE` and the
Okta server audience to the public HTTPS URL, and the redirect URIs to the
clients that will connect (Open WebUI has a fixed one). The task role
replaces the exported `AWS_*` variables.

## Gotchas

- `aud` must equal `resource_uri`. A mismatch is a 401.
- Group names are case-sensitive and must equal the Ranger policy groups.
- A table with no matching grant reads as not found, not as forbidden. That is
  the deny-by-default gate working.
- Expired SSO credentials show up as empty results or hangs on S3, not as an
  authentication error. Re-export and restart the SQE container.
- Glue stores database names lower-cased; write `GLUE_DATABASES` that way.
- `[catalog]` in `sqe.toml` is empty on purpose. A placeholder Iceberg URL
  becomes the default catalog and every SQL statement fails building its
  session on it; metadata tools keep working, which makes it look like a
  Ranger problem.
- `search_tables` issues one `SHOW TABLES` per database and each counts
  against `rate_limit.per_user_queries_per_minute`. Keep that limit above
  the number of Glue databases or the tool silently drops the tail.
- The Okta access policy rule must allow the Authorization Code grant, not
  only Device Authorization. Otherwise the browser shows "You are not allowed
  to access this app" even though the user is assigned; the System Log says
  `no_matching_policy`.
