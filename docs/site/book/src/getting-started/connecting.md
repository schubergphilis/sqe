# Connecting clients

SQE speaks two wire protocols for SQL clients. Arrow Flight SQL is primary and Arrow-native, used by the CLI, ADBC drivers, and the dbt adapter. The Trino HTTP endpoint is an optional compatibility surface for existing Trino clients and BI tools. This page has copy-pasteable snippets for both. AI clients connect over the Model Context Protocol instead (see [Model Context Protocol](../deployment/mcp.md)), and an experimental Quack endpoint speaks the DuckDB wire protocol (see [Quack](../quickstart/quack.md)).

The ports below are the defaults from [Configuration](../deployment/configuration.md): Flight SQL on `50051`, Trino HTTP on `8080`. Local test compose files remap them (`60051` and `28080`); use the ports your deployment actually exposes.

Authentication is bearer-token passthrough. The username and password you supply are exchanged for an OIDC token at connect time, and that token rides through to the catalog and storage. There is no service account. The exact provider (Keycloak password grant, client credentials, a JWKS-validated bearer, and others) is set in the server's `[auth]` config, not on the client. See [Authentication Modes](../deployment/configuration.md#authentication-modes).

## Flight SQL with ADBC (Python)

The ADBC Flight SQL driver gives you a DB-API 2.0 connection and Arrow result batches. This is the same driver the dbt-sqe adapter uses.

```python
from adbc_driver_flightsql.dbapi import connect
from adbc_driver_manager import DatabaseOptions

conn = connect(
    "grpc://localhost:50051",
    db_kwargs={
        DatabaseOptions.USERNAME.value: "jacob",
        DatabaseOptions.PASSWORD.value: "your-password",
    },
)

cur = conn.cursor()
cur.execute("SELECT 1 AS one")
print(cur.fetch_arrow_table())   # Arrow table, no row-by-row tax
cur.close()
conn.close()
```

Use `grpc://` for plaintext and `grpc+tls://` when the coordinator runs with TLS (`[coordinator.tls]` cert and key set). The username and password are exchanged by the server for an OIDC token; what credentials are valid depends on the server's auth config.

A DB-API connection starts in a transaction, as the DB-API spec requires. `INSERT`, `UPDATE`, `DELETE`, `MERGE` and bulk ingest stay invisible to everyone else until `conn.commit()`, and closing the connection without it discards them. DDL and CTAS commit at once. Pass `autocommit=True` to `connect` when every statement should commit on its own, as a loader that opens a connection per statement needs. See [Transactions](../sql-reference/dml.md#transactions).

## dbt with the dbt-sqe adapter

The dbt-sqe adapter connects over ADBC Flight SQL. A `profiles.yml` target looks like this:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: sqe
      host: localhost
      port: 50051
      tls: false  # local plaintext coordinator; omit when it runs with TLS
      user: jacob
      password: "{{ env_var('SQE_PASSWORD') }}"
      catalog: production
      schema: finance
      threads: 4
```

The adapter connects over TLS by default. `tls: false` is for a local plaintext coordinator only, and `tls_root_certs` points at a PEM CA bundle when the coordinator's certificate comes from a private CA. `dbt debug` validates the connection and reports the SQE version. The adapter runs every dbt operation as the authenticated user, so models only see and write what that user is allowed to. See [dbt Compatibility](../design-notes/dbt-sqe.md) for the materialization details.

## Flight SQL over JDBC

The Arrow Flight SQL JDBC driver connects with a URL of this shape:

```
jdbc:arrow-flight-sql://localhost:50051?user=jacob&password=your-password&useEncryption=false
```

Set `useEncryption=true` when the coordinator runs with TLS. JDBC tools (DBeaver, query consoles, BI connectors that take a JDBC URL) point at this string with the Flight SQL driver on the classpath.

## Trino HTTP

The Trino HTTP endpoint is on by default (`[coordinator] trino_http_port = 8080`, set to `0` to disable). Trino clients, the Trino JDBC driver, and Trino-compatible BI tools (Metabase, Superset) point at it unchanged. Basic auth carries the user; the password is the OIDC secret (empty for a local root client).

```bash
curl -s -u jacob:your-password \
  -H "X-Trino-User: jacob" \
  -d "SELECT 1 AS one" \
  http://localhost:8080/v1/statement
```

The first response carries a `nextUri`; a client follows it until results are exhausted. A Trino JDBC client connects against `http://localhost:8080`. Basic auth is required to populate the session, not just the `X-Trino-User` header.

The Trino JDBC driver and the Trino CLI refuse to send a username and password over plain HTTP. The endpoint serves plain HTTP, so for password auth put a TLS-terminating reverse proxy in front and connect over `https://`. Bearer-token auth works over plain HTTP if you cannot terminate TLS. See [Connect BI tools](../quickstart/bi-tools.md) for Metabase, Superset, DBeaver, and CLI recipes, and [Trino HTTP connectivity](../features/trino-http.md) for the endpoint reference. Flight SQL is the recommended protocol for SQE-native clients; reach for Trino HTTP when a tool only speaks Trino.

## MCP (AI assistants)

SQE speaks Model Context Protocol over Streamable HTTP, so Claude Code, Codex and other MCP clients can query the lakehouse as the signed-in user. It is off by default; enable it with `[mcp] enabled = true` and point `resource_uri` at the public URL.

```bash
curl -s -H "Authorization: Bearer $SQE_MCP_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' \
  http://localhost:9091/mcp
```

The endpoint is an OAuth protected resource. It validates the caller's token, never mints one, and forwards that same bearer to the catalog and storage, so the assistant sees exactly what its human can see, masks and row filters included. Read tools unlock on `read_scope`; the write tool is hidden unless `allow_writes = true` and still needs a write scope, an SQE writer role, and source authorization.

Client configuration for Codex and Claude Code, plus a runnable Keycloak + Polaris stack, is in [MCP with Codex and Claude](../quickstart/mcp.md). Endpoint reference: [Model Context Protocol](../deployment/mcp.md).

## Which protocol

| You are | Use |
|---|---|
| Writing Python, an ETL job, or dbt | Flight SQL via ADBC |
| Connecting a JDBC tool that supports the Flight SQL driver | Flight SQL via JDBC |
| Pointing an existing Trino client or a Trino-only BI tool | Trino HTTP |
| Running ad-hoc SQL from a terminal | `sqe-cli` (Flight SQL); see [Using the CLI](cli.md) |
| Wiring an AI assistant or agent (Claude Code, Codex) | MCP over Streamable HTTP |
