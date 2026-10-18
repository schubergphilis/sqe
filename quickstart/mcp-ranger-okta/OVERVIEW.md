# MCP with Apache Ranger and Okta on external data

## Goal

Let Okta users reach SQE's MCP endpoint from Claude Code or Codex with a browser login, and let Apache Ranger policies per Okta group decide which AWS Glue Data Catalog tables they see. Nobody handles a token by hand: the MCP client opens the browser, the user signs in to Okta, done.

```text
Claude Code / Codex --(browser login)--> Okta --access token--> SQE /mcp
SQE: token -> Session(user, groups) -> Ranger access policies -> Glue catalog -> S3
```

There is no Keycloak, no Polaris and no local object store. The data is yours: the Glue Data Catalog read live, files in S3.

## Components

| Piece | Role |
|---|---|
| `sqe` | Runs as `sqe-server --glue-catalog glue --allow-all-buckets --mcp-only`. An OAuth 2.1 resource server: it only verifies Okta tokens, it never talks to Okta at login time. |
| Apache Ranger Admin (+ Postgres) | Access policies. A Glue table is readable when a Ranger `select` grant matches the caller's user, roles or groups, and reads as not found otherwise. |
| Okta | The only identity provider: a custom authorization server with `sqe:mcp:read` / `sqe:mcp:write` scopes and a `groups` claim, and a public, PKCE-only native app. Terraform for all of it is in `okta/`. |

The bootstrap writes two policies from two Okta groups:

| Okta group | Sees |
|---|---|
| `datalake_readonly` | every table except `SENSITIVE_TABLES` (explicit deny) |
| `datalake_sensitive_ro` | every table including `SENSITIVE_TABLES` |

## Configuration

Every setting is an `SQE_<SECTION>__<FIELD>` environment override of `sqe.toml`, so a Fargate task definition carries the same names:

| Purpose | Where |
|---|---|
| Okta | `SQE_AUTH__PROVIDERS__0__ISSUER`, `__JWKS_URL`, `__AUDIENCE`, `SQE_MCP__AUTHORIZATION_SERVERS__0` |
| MCP | `SQE_MCP__RESOURCE_URI`, `SQE_MCP__ALLOW_WRITES`; `--mcp-only` in the command |
| Glue + S3 | `--glue-catalog`, `--glue-region`, `--glue-database`, `--allow-all-buckets`; `SQE_STORAGE__S3_REGION`; AWS credentials from the shell or the task role |
| Ranger | `SQE_POLICY__RANGER__URL`, `SQE_POLICY__RANGER__ADMIN_PASSWORD` |

SQE reads Glue and S3 with the credentials of its own process; nothing in the directory holds a key. On a laptop, export SSO session credentials into the shell; on Fargate, the task role is picked up by the SDK credential chain.

```bash
cp .env.example .env     # Okta issuer + client id, Glue databases, sensitive tables
eval "$(aws configure export-credentials --format env)"
./run.sh                 # starts the stack, protocol checks, writes .mcp.json
claude                   # /mcp -> sqe -> Authenticate: the browser opens Okta
./run.sh --down
```

## The test

`run.sh` brings the stack up, bootstraps the Ranger policies and runs the MCP protocol checks, then writes `.mcp.json` for Claude Code. The login itself is the check that matters: the client gets a `401` with a `WWW-Authenticate` header pointing at the protected-resource metadata, reads Okta's metadata, runs Authorization Code + PKCE in the browser, and every later call carries the access token. SQE verifies the signature against Okta's JWKS plus `iss`, `aud` (equal to `mcp.resource_uri`) and `exp`, takes the user from `sub` and the groups from the `groups` claim, and builds the session Ranger evaluates.

## Output

In Claude Code, ask for `whoami`, `SHOW SCHEMAS FROM glue`, `SHOW TABLES FROM glue.<database>` and a `SELECT ... LIMIT 5`. A user in `datalake_readonly` sees every table except the sensitive ones, which read as not found; a user in `datalake_sensitive_ro` sees them all. After a login, SQE logs `Bearer token authentication succeeded` with the user and roles.
