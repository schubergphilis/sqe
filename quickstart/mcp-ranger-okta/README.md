---
slug: mcp-ranger-okta
title: "MCP with Apache Ranger and Okta"
description: "SQE MCP on the Polaris + Ranger stack, with Okta as the identity provider and every source setting supplied through the environment."
---

# MCP with Apache Ranger and Okta

Run SQE's MCP endpoint on top of the [`polaris-ranger-keycloak`](../polaris-ranger-keycloak/)
stack. MCP clients authenticate with Okta access tokens; Ranger row filters and
column masks apply to the Okta user's groups; catalogs, storage and Ranger
endpoints are all environment variables so the same config moves to Fargate
unchanged.

```text
Okta access token -> SQE MCP -> Session(user, groups) -> Ranger policies -> Polaris -> S3
```

Keycloak stays in the stack for one job: the seed container writes the demo
table as `carol` through the ROPC provider. Okta cannot run in Docker, so this
quickstart needs a real Okta tenant.

## Okta setup (once)

1. **Authorization server**: Security > API > Authorization Servers > Add.
   Audience = `http://localhost:19092/mcp` (the MCP `resource_uri`).
2. **Scopes** on that server: `sqe:mcp:read` (default scope) and `sqe:mcp:write`.
3. **Claim** `groups` on the access token, value expression `Groups` matching a
   regex like `.*`. Create Okta groups named after the Ranger roles the stack
   bootstraps: `analyst`, `engineer`, `sqe_admin`.
4. **Application**: OIDC Native app `sqemcp`, no secret, grant types Authorization
   Code + Refresh Token (+ Device Authorization for the token script), redirect URI
   `http://localhost:8400/callback`, assigned to the groups.
5. **Access policy** on the server for that client: all of its grant types, any
   assigned user, any scope. Without it Okta answers `access_denied` after login.

Copy the server's issuer URI and the app's client ID into `.env`.

## Run it

```bash
cd quickstart/mcp-ranger-okta
cp .env.example .env          # set OKTA_ISSUER and OKTA_CLIENT_ID
docker compose up -d --wait   # plain compose works: the Ranger stack is `include`d
./run.sh                      # same, plus metadata/challenge checks
./mint-okta-token.sh --decode  # device flow; shows iss/aud/scp/groups SQE will check
OKTA_TOKEN=$(./mint-okta-token.sh) ./run.sh   # adds whoami + a Ranger-masked read as that user
./run.sh --writes             # expose execute_sql_change to engineer / sqe_admin groups
./run.sh --down
```

## Connect Claude Code or Codex

The MCP authorization flow is Authorization Code + PKCE: the client hits `/mcp`,
gets a 401 pointing at Okta, opens the browser, and Okta redirects to a loopback
callback the client listens on. Okta rejects anonymous dynamic client
registration, so both clients are given the `sqemcp` app's client ID.

Register `http://localhost:8400/callback` as a sign-in redirect URI on the app
(Okta needs the exact port; no wildcards), then:

```bash
# Claude Code
claude mcp add --transport http --client-id "$OKTA_CLIENT_ID" --callback-port 8400 \
  sqe http://localhost:19092/mcp
# or copy clients/claude.mcp.json to .mcp.json; then in Claude Code: /mcp -> authenticate

# Codex: paste clients/codex-config.toml into ~/.codex/config.toml with the client ID, then
codex mcp login sqe
```

For a token in a shell (smoke tests, `run.sh`), the app also has the Device
Authorization grant: `./mint-okta-token.sh --decode`.

## Environment contract

Everything in `docker-compose.yml`'s `environment:` block is an
`SQE_<SECTION>__<FIELD>` override of [`sqe.toml`](./sqe.toml); a Fargate task
definition carries the same names. Array entries are addressed by index and the
slot must exist in the TOML:

| Purpose | Variables |
|---|---|
| Okta | `SQE_AUTH__PROVIDERS__1__ISSUER`, `__JWKS_URL`, `__AUDIENCE`, `SQE_MCP__AUTHORIZATION_SERVERS__0` |
| MCP | `SQE_MCP__RESOURCE_URI`, `SQE_MCP__ALLOW_WRITES` |
| Source catalogs | `SQE_CATALOGS__<NAME>__POLARIS_URL`, `SQE_CATALOGS__<NAME>__WAREHOUSE` (name lowercased; add a `[catalogs.<name>]` block per source) |
| Storage | `SQE_STORAGE__S3_ENDPOINT`, `__S3_REGION`, `__S3_ACCESS_KEY`, `__S3_SECRET_KEY` (leave keys empty on Fargate to use the task role) |
| Ranger | `SQE_POLICY__RANGER__URL`, `SQE_POLICY__RANGER__ADMIN_PASSWORD`, `SQE_ACCESS_CONTROL__URL`, `SQE_ACCESS_CONTROL__RANGER__ADMIN_PASSWORD` |

## Fargate later

Same image, same TOML baked in or mounted from EFS/SSM, same variables from the
task definition with secrets from Secrets Manager. Change `resource_uri`,
`OKTA_AUDIENCE` and the Okta server audience to the public HTTPS URL, put TLS on
the ALB, and drop the `[[auth.providers]]` Keycloak entry (then Okta becomes
index 0).

## Gotchas

- Okta access tokens from the **org** authorization server are opaque; only a
  **custom** authorization server issues JWTs SQE can verify.
- `aud` must equal `resource_uri`. A mismatch is a 401 with `invalid audience`.
- Group names are case-sensitive and must equal the Ranger role names.
- Ranger policies in the base stack are written for users alice/bob/carol; an
  Okta user gets masks through group membership only.
