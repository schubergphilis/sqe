# Trino Compatibility

SQE includes a Trino-compatible HTTP endpoint that allows existing Trino clients (JDBC drivers, DBeaver, CLI tools) to connect without modification.

## Enabling

The Trino HTTP endpoint is enabled by default on port 8080. Set port to 0 to disable:

```toml
[coordinator]
trino_http_port = 8080    # 0 to disable
```

## Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/v1/info` | GET | Node info (version, uptime, coordinator status) |
| `/v1/info/state` | GET | Plain text: `ACTIVE` or `STARTING` |
| `/v1/statement` | POST | Submit a SQL query |
| `/v1/statement/{id}/{token}` | GET | Fetch paginated results |
| `/v1/statement/{id}` | DELETE | Cancel a running query |

## Authentication

The Trino endpoint supports two authentication methods:

### Bearer Token

Pass an existing access token directly:

```bash
curl -H "Authorization: Bearer eyJhbG..." \
     -H "X-Trino-User: alice" \
     -d "SELECT 1" \
     http://localhost:8080/v1/statement
```

### Basic Auth

Username and password are exchanged for a token via the configured OIDC/OAuth2 backend:

```bash
curl -u alice:password \
     -d "SELECT 1" \
     http://localhost:8080/v1/statement
```

## Client Headers

SQE respects standard Trino client headers:

| Header | Purpose |
|---|---|
| `X-Trino-User` | Override username (used with Bearer auth) |
| `X-Trino-Catalog` | Set default catalog for the session |
| `X-Trino-Schema` | Set default schema for the session |
| `X-Trino-Source` | Client identifier (logged for audit) |

## Result Pagination

Query results are paginated. The initial response includes a `nextUri` field. Follow `nextUri` links to retrieve subsequent pages:

```json
{
  "id": "query-uuid",
  "stats": { "state": "FINISHED" },
  "columns": [{"name": "result", "type": "integer"}],
  "data": [[1]],
  "nextUri": "http://localhost:8080/v1/statement/query-uuid/1"
}
```

When `nextUri` is absent, all results have been consumed.

## Using with the CLI

```bash
sqe-cli --protocol http --host localhost --port 8080 --user alice
```

## Connecting External Tools

### DBeaver

1. Create a new **Trino** connection
2. Host: `localhost`, Port: `8080`
3. Authentication: Username/Password
4. Driver properties: no special settings needed

### JDBC (Java)

```java
String url = "jdbc:trino://localhost:8080";
Properties props = new Properties();
props.setProperty("user", "alice");
props.setProperty("password", "secret");
Connection conn = DriverManager.getConnection(url, props);
```

## Limitations

- The Trino endpoint returns results as JSON (Trino wire format), not Arrow. For maximum performance, use Flight SQL.
- Prepared statements are not supported via the Trino protocol.
- Transaction control (`START TRANSACTION`, `COMMIT`) is not supported. Queries execute in auto-commit mode.
- Type mapping covers common types; complex nested types may differ from native Trino behavior.
- Materialized views are not supported. `CREATE MATERIALIZED VIEW` returns a clear "not supported" error rather than creating a plain view. `DROP MATERIALIZED VIEW IF EXISTS` is treated as a no-op so client tooling that issues it on teardown can proceed.

## Flight SQL vs Trino HTTP

| Aspect | Flight SQL (default) | Trino HTTP |
|---|---|---|
| Port | 50051 | 8080 |
| Wire format | Arrow IPC (binary, columnar) | JSON |
| Performance | High (zero-copy) | Lower (serialization overhead) |
| Client support | ADBC, JDBC (Flight SQL), dbt | Trino JDBC, DBeaver, Tableau |
| Pagination | Arrow Flight streaming | nextUri polling |

Use Flight SQL for performance-sensitive workloads. Use Trino HTTP for compatibility with existing tools.
