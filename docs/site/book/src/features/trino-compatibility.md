# Trino Compatibility

SQE includes a Trino-compatible HTTP endpoint that allows existing Trino clients (JDBC drivers, CLI tools, DBeaver) and Trino-speaking BI tools (Metabase, Superset) to connect without modification.

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
| `/v1/statement/queued/{id}/{token}` | GET | Poll a queued/running query until results are ready |
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

## Async Submission

Submission is async, matching Trino's own protocol. `POST /v1/statement` spawns the query on a background task with a bounded initial wait. If the query does not finish in that window, the first response carries `state: QUEUED`, no `data`, and a `nextUri` pointing at `/v1/statement/queued/{id}/{token}`:

```json
{
  "id": "query-uuid",
  "stats": { "state": "QUEUED" },
  "nextUri": "http://localhost:8080/v1/statement/queued/query-uuid/0"
}
```

The client follows the queued links (state stays `QUEUED` or `RUNNING`) until the query finishes, at which point `nextUri` redirects to the results route at token 0 and the response starts carrying `columns` and `data`. Clients that only ever poll `nextUri` need no special handling: the queued and results routes chain transparently.

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

### Metabase and Superset

BI tools that speak Trino connect through the same endpoint. Metabase uses the Trino JDBC driver, Superset uses the Trino SQLAlchemy dialect (`trino://user@host:8080/catalog`). Both drive a metadata handshake on connect (prepare a statement, enumerate catalogs and schemas, list tables, describe columns) before running a chart, and SQE now matches Trino's exact response shape at each step:

- `PREPARE` and `DEALLOCATE PREPARE` are handled as session-control via the `X-Trino-Added-Prepare` / `X-Trino-Deallocated-Prepare` headers, so the JDBC connection test succeeds.
- `SHOW TABLES` returns a single `Table` column, `SHOW SCHEMAS` a `Schema` column, and `SHOW CATALOGS` a `Catalog` column, so schema sync reads the right values.
- `SHOW CATALOGS` and the `system.jdbc.*` / `system.metadata.*` tables enumerate every reachable catalog and skip the ones the caller is not authorized to list, and `SHOW TABLES` / `SHOW SCHEMAS` honor the session catalog (`X-Trino-Catalog`).
- `DESCRIBE` and `SHOW COLUMNS` resolve double-quoted identifiers (`"catalog"."schema"."table"`), so field discovery works.
- Types map to their Trino equivalents: `timestamp(6)` carries its precision in the type signature (so date bucketing over JDBC works), and computed unsigned-64 aggregates like `count(*)` map to `bigint` rather than `decimal`.

## Limitations

- The Trino endpoint returns results as JSON (Trino wire format), not Arrow. For maximum performance, use Flight SQL.
- Transaction control (`START TRANSACTION`, `COMMIT`) is not supported. Queries execute in auto-commit mode.
- Type mapping covers common types; complex nested types may differ from native Trino behavior.
- Iceberg hidden columns (`$path`, `$file_modified_time`, `$partition`) are not exposed on table scans. They need a per-row, per-source-file column that is resolvable by name but excluded from `SELECT *`. DataFusion has no such metadata-column mechanism yet (tracked upstream at apache/datafusion#20135, not in any release), and adding the column to the scan schema would make every `SELECT *` return it. For file-level introspection use the `table_files('ns', 't')` table function, which lists `file_path`, `record_count`, and `file_size_in_bytes` per data file.
- Materialized views are not supported. `CREATE MATERIALIZED VIEW` returns a clear "not supported" error rather than creating a plain view. `DROP MATERIALIZED VIEW IF EXISTS` is treated as a no-op so client tooling that issues it on teardown can proceed.

## Flight SQL vs Trino HTTP

| Aspect | Flight SQL (default) | Trino HTTP |
|---|---|---|
| Port | 50051 | 8080 |
| Wire format | Arrow IPC (binary, columnar) | JSON |
| Performance | High (zero-copy) | Lower (serialization overhead) |
| Client support | ADBC, JDBC (Flight SQL), dbt | Trino JDBC, DBeaver, Metabase, Superset |
| Pagination | Arrow Flight streaming | nextUri polling |

Use Flight SQL for performance-sensitive workloads. Use Trino HTTP for compatibility with existing tools.
