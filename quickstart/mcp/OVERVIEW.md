# MCP with Codex and Claude (Keycloak, Polaris)

## Goal

Run SQE as a remote Model Context Protocol server over Streamable HTTP, backed by real Keycloak users, Apache Polaris and RustFS, and connect Codex or Claude Code to it. SQE is an OAuth resource server, not a credential proxy: the caller's bearer becomes the same per-user session Flight SQL uses and travels to the catalog and storage unchanged, so an assistant reads exactly what its human can read.

```text
adminuser/testuser bearer -> SQE MCP -> SQE Session -> Polaris -> RustFS
```

The quickstart is read-only by default. An explicit `--writes` mode proves that the OAuth scope, the SQE role and source-data authorization stay three independent gates.

## Components

It reuses the `polaris-keycloak-user-token` stack instead of cloning it; only the MCP configuration, a Compose overlay, the tests and the client templates live here.

| Piece | Role |
|---|---|
| Keycloak | Identity provider. `adminuser` (`data_writer`) and `testuser` (reader only). `sqe:mcp:read` is a default client scope, `sqe:mcp:write` optional. |
| Apache Polaris | Iceberg REST catalog; authorizes every read and write with the user's token. |
| RustFS | S3-compatible object store holding the warehouse. |
| `sqe` | The engine, with `/mcp` on its existing web listener (`http://localhost:19091/mcp`). |

## Configuration

`sqe.toml` enables MCP with `port = 0`, so it shares the web/health port. The Compose overlay replaces only `sqe.toml`, exposes that port and carries the one runtime switch, `SQE_MCP__ALLOW_WRITES`. Storage keys and demo passwords arrive as `SQE_<SECTION>__<KEY>` environment overrides, so no secret is in the file. Every relaxed check is marked `# DEV ONLY:`.

SQE serves RFC 9728 protected-resource metadata and bearer challenges. The read-only SQL tool is gated by SQE's own parser as an allow-list. Responses are capped at 100 rows and 256 KiB for predictable model context.

```bash
cd quickstart/mcp
cp .env.example .env
./run.sh --check       # read-only end-to-end tests
./run.sh --writes      # expose and test execute_sql_change
./run.sh --down
```

To connect a client, mint a short-lived user token and point the client template at it; the token stays in an environment variable and never lands on disk:

```bash
export SQE_MCP_TOKEN="$(./mint-token.sh)"
./setup-client.sh claude     # or: ./setup-client.sh codex
```

In a real deployment, give Keycloak and SQE public HTTPS URLs and use the clients' browser OAuth login instead of a pre-minted token.

## The test

`test-static.sh` validates every script, JSON and TOML file and the merged Compose model without starting services. `test-mcp.sh` speaks raw JSON-RPC over HTTP, so the server is tested independently of either AI client. `run.sh --check` mints two real signed JWTs, seeds data through Flight SQL and runs the MCP assertions against the same SQE process. `run.sh --writes` additionally proves the writer succeeds and that a reader holding the same OAuth write scope still fails the SQE role gate: the scope alone grants nothing.

## Output

```
==> checking OAuth protected-resource metadata
  ok resource identifier
  ok read scope advertised
==> checking authentication challenges
  ok missing bearer returns 401
  ok challenge points to RFC 9728 metadata
  ok invalid bearer rejected
==> checking MCP initialization and tools
  ok protocol version
  ok read-only SQL tool
  ok metadata tools
==> checking per-user identity and governed reads
  ok admin identity reaches MCP
  ok source data read through SQE
  ok write rejected by read-only parser gate
==> checking the opt-in write gate
  ok write tool advertised
  ok writer role can change source data
  ok reader denied despite OAuth write scope

==> checks: 14 passed, 0 failed
```
