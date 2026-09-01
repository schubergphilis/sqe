---
slug: mcp
title: "MCP with Codex and Claude"
description: "Run SQE's authenticated MCP server over the existing web port, test read/write authorization end to end, and connect Codex or Claude Code."
---

# MCP with Codex and Claude

Run SQE as a remote Streamable HTTP MCP server backed by real Keycloak users,
Apache Polaris and RustFS. The quickstart is read-only by default and includes
an explicit `--writes` mode that proves OAuth scope, SQE role and source-data
authorization remain independent gates.

It deliberately reuses the
[`polaris-keycloak-user-token`](../polaris-keycloak-user-token/) stack instead
of cloning it. Only the SQE MCP configuration, Compose overlay, tests and AI
client examples live here.

## What you get

- SQE `/mcp` on its existing web/health listener at `http://localhost:19091/mcp`.
- RFC 9728 OAuth protected-resource metadata and bearer challenges.
- Keycloak tokens for `adminuser` (`data_writer`) and `testuser` (reader only).
- Polaris and RustFS source data reached with the authenticated user's token.
- Protocol, identity, read-only parser, invalid-token and optional-write tests.
- Secret-free, reusable Codex and Claude Code configuration templates.

The important path is unchanged all the way through:

```text
adminuser/testuser bearer -> SQE MCP -> SQE Session -> Polaris -> RustFS
```

## Prerequisites

- Docker with Compose v2
- `curl`
- `jq`
- Optional: Codex CLI or Claude Code
- Optional: Python 3.11+ adds TOML parsing to `test-static.sh`

## Run it

```bash
cd quickstart/mcp
cp .env.example .env

./test-static.sh       # syntax + JSON/TOML + composed-stack validation
./run.sh --check       # read-only end-to-end tests
./run.sh               # same checks and capture output in OUTPUT.md
./run.sh --writes      # expose and test execute_sql_change
./run.sh --down        # remove containers and volumes
```

`./run.sh --writes` restarts the SQE service with `MCP_ALLOW_WRITES=true` when
needed. The write tool still requires `sqe:mcp:write`, a `data_writer` role,
and Polaris authorization. The test gives both users the OAuth scope and proves
that the reader is still denied because OAuth scope alone grants nothing.

## Connect Codex

Mint a short-lived user token in the shell that will start Codex:

```bash
export SQE_MCP_TOKEN="$(./mint-token.sh adminuser adminuser123)"
./setup-client.sh codex
```

Copy [`clients/codex-config.toml`](./clients/codex-config.toml) into either
`~/.codex/config.toml` or a trusted project's `.codex/config.toml`, then run:

```bash
codex mcp list
codex
# In Codex: /mcp
```

The configuration uses `bearer_token_env_var`, so the token is not committed to
disk. It also uses the `writes` approval mode: read-only annotated tools can run
normally while Codex prompts for write-capable tools. Codex supports Streamable
HTTP bearer tokens and interactive MCP OAuth; see the
[official Codex MCP documentation](https://developers.openai.com/codex/mcp/).

## Connect Claude Code

```bash
export SQE_MCP_TOKEN="$(./mint-token.sh adminuser adminuser123)"
./setup-client.sh claude
cp clients/claude.mcp.json /path/to/your/project/.mcp.json
cd /path/to/your/project
claude mcp list
claude
# In Claude Code: /mcp
```

The checked-in `.mcp.json` uses Claude Code's environment-variable expansion,
so every developer supplies their own token. Claude Code supports remote HTTP
MCP servers, header expansion and interactive OAuth from `/mcp`; see
[Anthropic's MCP setup](https://docs.anthropic.com/en/docs/claude-code/mcp).

Try either client with prompts such as:

```text
Use SQE to list the schemas and describe quickstart.demo.events.
Count events by kind using only read-only tools.
Show my SQE identity and roles.
```

## How it works

`run.sh` composes two files:

1. `../polaris-keycloak-user-token/docker-compose.yml` supplies Keycloak,
   Polaris, RustFS, bootstrap jobs and the SQE image build.
2. `docker-compose.yml` replaces only `sqe.toml`, exposes the existing SQE web
   port and supplies the optional write flag.

The shared Keycloak realm now defines `sqe:mcp:read` as a default client scope
and `sqe:mcp:write` as an optional scope. SQE validates the signed JWT first,
then MCP evaluates these claims. The same identity and token enter the normal
SQE query handler and catalog/storage path.

For this local Docker scenario the scripts provide a pre-minted token because
Keycloak's canonical issuer is the Docker-only hostname `keycloak`. In a real
deployment, give Keycloak and SQE public HTTPS URLs and use browser OAuth:
`codex mcp login sqe` or Claude Code's `/mcp` authentication action.

## Configuration explained

- [`sqe.toml`](./sqe.toml) enables MCP with `port = 0`, so it shares port 9091
  inside the container. `SQE_MCP_PORT` maps that existing listener to the host.
- [`docker-compose.yml`](./docker-compose.yml) is an overlay, not a duplicated
  stack. `SQE_MCP__ALLOW_WRITES` is the only runtime behavior override.
- [`.env.example`](./.env.example) contains local ports and demo credentials.
  Change `SQE_MCP_PORT` and the public `resource_uri` override together.
- [`clients/`](./clients/) contains templates only; no bearer token is stored.

Read responses are capped at 100 rows and 256 KiB for predictable model
context. Browser origins are empty/fail-closed; native Codex and Claude clients
do not send an `Origin` header.

## Output

A correct read-only run reports checks for protected-resource metadata, a 401
challenge, invalid-token rejection, MCP initialization, tool discovery, the
`adminuser` identity, a four-row source query, and parser rejection of `DROP`.
See [`OUTPUT.md`](./OUTPUT.md); a normal `./run.sh` refreshes it.

## How it is tested

- `test-static.sh` validates every shell script, JSON document, TOML file and
  the merged Compose model without starting services.
- `test-mcp.sh` sends raw JSON-RPC over HTTP so it tests the server independently
  of either AI client.
- `run.sh --check` mints two real signed JWTs, seeds data through Flight SQL and
  runs those MCP assertions against the same SQE process.
- `run.sh --writes` additionally proves a writer succeeds and a reader holding
  the same OAuth write scope fails the SQE role gate.

## Gotchas

- Tokens are intentionally short-lived. Mint a fresh token before starting an
  AI client if authentication fails.
- `--writes` is an explicit opt-in. Run `--down` before switching modes if the
  running Compose service was created with the other setting.
- Local HTTP and unbounded token audience are quickstart-only concessions.
  Production validation requires HTTPS and an audience equal to the MCP
  resource URI.
- Never commit the printed token or replace the environment reference in the
  client templates with a literal bearer.
- The shared realm lists `polaris-frontend-client`'s default client scopes
  explicitly. Keycloak skips realm-default scope assignment for any client whose
  import declares `defaultClientScopes` or `optionalClientScopes`, so adding the
  optional write scope alone strips `profile`, `basic`, `roles` and `catalog`
  from every token. Symptom: `Invalid or expired bearer token` on the very first
  Flight SQL statement.
