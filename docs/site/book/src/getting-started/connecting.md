# Connecting clients

SQE speaks two wire protocols. Arrow Flight SQL is primary and Arrow-native, used by the CLI, ADBC drivers, and the dbt adapter. The Trino HTTP endpoint is an optional compatibility surface for existing Trino clients and BI tools. This page has copy-pasteable snippets for both.

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
      user: jacob
      password: "{{ env_var('SQE_PASSWORD') }}"
      catalog: production
      schema: finance
      threads: 4
```

`dbt debug` validates the connection and reports the SQE version. The adapter runs every dbt operation as the authenticated user, so models only see and write what that user is allowed to. See [dbt Compatibility](../design-notes/dbt-sqe.md) for the materialization details.

## Flight SQL over JDBC

The Arrow Flight SQL JDBC driver connects with a URL of this shape:

```
jdbc:arrow-flight-sql://localhost:50051?user=jacob&password=your-password&useEncryption=false
```

Set `useEncryption=true` when the coordinator runs with TLS. JDBC tools (DBeaver, query consoles, BI connectors that take a JDBC URL) point at this string with the Flight SQL driver on the classpath.

## Trino HTTP

When the Trino compat layer is enabled (`[trino_compat] enabled = true`), Trino clients, the Trino JDBC driver, and Trino-compatible BI tools point at the HTTP endpoint unchanged. Basic auth carries the user; the password is the OIDC secret (empty for a local root client).

```bash
curl -s -u jacob:your-password \
  -H "X-Trino-User: jacob" \
  -d "SELECT 1 AS one" \
  http://localhost:8080/v1/statement
```

The first response carries a `nextUri`; a client follows it until results are exhausted. A Trino JDBC client connects against `http://localhost:8080`. Basic auth is required to populate the session, not just the `X-Trino-User` header.

DBeaver, JDBC, and BI-tool specifics for the Trino path are covered in [Trino HTTP connectivity](../features/trino-http.md). Flight SQL is the recommended protocol for SQE-native clients; reach for Trino HTTP when a tool only speaks Trino.

## Which protocol

| You are | Use |
|---|---|
| Writing Python, an ETL job, or dbt | Flight SQL via ADBC |
| Connecting a JDBC tool that supports the Flight SQL driver | Flight SQL via JDBC |
| Pointing an existing Trino client or a Trino-only BI tool | Trino HTTP |
| Running ad-hoc SQL from a terminal | `sqe-cli` (Flight SQL); see [Using the CLI](cli.md) |
