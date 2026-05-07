# Using the CLI

`sqe-cli` is the SQL client. By default it connects to a remote coordinator over Arrow Flight SQL or Trino HTTP. Pass `--embedded` to skip the network entirely and run an in-process engine. That mode is useful for ad-hoc analysis on local Parquet files without standing up a cluster.

## Usage

```
sqe-cli [OPTIONS]

Options:
  -H, --host <HOST>          Coordinator host [default: localhost]
  -p, --port <PORT>          Coordinator port [default: 50051]
      --protocol <PROTOCOL>  Wire protocol: flight or http [default: flight]
  -u, --user <USER>          Username (prompts if not set)
      --token <TOKEN>        Bearer token (skips password flow)
  -e, --execute <SQL>        Execute a single query and exit
      --file <PATH>          Read statements from a SQL script file
      --stop-on-error        Abort the script on first error (default: continue)
      --embedded             Run the engine in-process (no remote coordinator)
      --memory-limit <SIZE>  Per-process memory pool when --embedded [default: 1GB]
      --warehouse <PATH>     Override embedded warehouse path (default ~/.sqe/warehouse/)
      --memory               Skip the persistent catalog; ephemeral session only
  -f, --format <FORMAT>      Output format: table, csv, tsv, json [default: table]
      --tls                  Use HTTPS/TLS
      --insecure             Accept invalid TLS certificates
  -h, --help                 Print help
  -V, --version              Print version
```

## Embedded mode

`--embedded` boots a single-process `SessionContext` with the same DataFusion tuning the cluster coordinator uses (`parse_float_as_decimal`, 64MB hash-join broadcast threshold, dynamic filter pushdown, Parquet filter pushdown). It registers all the same scalar functions, Trino-dialect aliases, JSON helpers, and the `read_parquet(...)` table-valued function. No auth, no Polaris, no network listeners.

```bash
# One-shot query against a local Parquet file
sqe-cli --embedded -e "SELECT COUNT(*) FROM read_parquet('data.parquet')"

# Trino-dialect functions work out of the box
sqe-cli --embedded -e "SELECT year(DATE '2026-05-07')"

# Run a script of statements
sqe-cli --embedded --file setup.sql

# Combine: script first, then ad-hoc query
sqe-cli --embedded --file setup.sql -e "SELECT COUNT(*) FROM staging"

# Interactive REPL (the default if no -e or --file is given)
sqe-cli --embedded
```

S3 access works too. Pass credentials inline to `read_parquet`:

```sql
SELECT *
FROM read_parquet(
    's3://bucket/path/*.parquet',
    access_key  => 'AKIA...',
    secret_key  => '...',
    region      => 'eu-central-1'
);
```

### Persistent catalog

By default, `--embedded` attaches a SQLite-backed Iceberg catalog at `~/.sqe/warehouse/`. Tables created in this warehouse survive across sessions:

```bash
# Session 1: load + create
sqe-cli --embedded -e "CREATE SCHEMA iceberg.staging"
sqe-cli --embedded -e \
    "CREATE TABLE iceberg.staging.events (event_id BIGINT, ts TIMESTAMP, kind VARCHAR) \
     AS SELECT * FROM read_parquet('events.parquet')"

# Session 2: same warehouse, table is still there
sqe-cli --embedded -e "SELECT count(*) FROM iceberg.staging.events"
```

The on-disk layout:

```
~/.sqe/warehouse/
├── sqe.db              # SQLite catalog (namespaces, table pointers)
└── iceberg/            # Iceberg metadata + Parquet data files
    └── staging/
        └── events/
            ├── metadata/
            └── data/
```

The catalog name is `iceberg`. Three-part identifiers (`iceberg.staging.events`) work; unqualified names resolve against DataFusion's default in-memory catalog, so `SELECT * FROM read_parquet(...)` still works without any catalog interaction.

Override the path:

```bash
sqe-cli --embedded --warehouse /data/my-warehouse -e "..."
```

Skip the catalog entirely (ephemeral session, nothing written to disk):

```bash
sqe-cli --embedded --memory -e "SELECT 1"
```

Tables in the warehouse are valid Iceberg. If you later upgrade to a cluster deployment, point the cluster catalog at the same path and the tables come along. No migration, no re-export.

### Dot-commands

The REPL recognises sqlite/DuckDB-style commands that start with `.`. They run client-side, never reach the engine, and don't end with `;`:

```
sqe> .help
Dot commands:
  .help                show this list
  .exit, .quit         leave the REPL
  .tables [schema]     list tables (optionally filter by schema)
  .schema <table>      describe a table's columns
  .catalogs            list catalogs visible to the session
  .read <path>         execute a SQL script file
  .timer on|off        toggle per-query elapsed-time output
  .format [fmt]        show or set output format (table|csv|tsv|json)
```

Examples:

```
sqe> .timer on
Timer: on

sqe> SELECT count(*) FROM read_parquet('events.parquet');
+----------+
| count(*) |
+----------+
| 1500000  |
+----------+
Time: 0.243s

sqe> .tables
sqe> .schema iceberg.staging.events
sqe> .read setup.sql
sqe> .format json
```

The legacy `\format` and `\q` forms still work for backward compatibility.

### What embedded mode does not include

- Authentication, RBAC, or column masking. Embedded mode runs as the local user. Use the cluster path when you need policy enforcement.
- Distributed execution. Embedded mode is single-process by design.
- Concurrent writers. The SQLite catalog is single-process; running two `sqe-cli --embedded` instances against the same warehouse simultaneously will likely produce errors. The cluster path handles concurrent writes correctly.

## Script files

`--file` reads a SQL script and executes statements in order, separated by `;`. The splitter respects single-quoted strings, double-quoted identifiers, line comments (`--`), and block comments (`/* ... */`), so semicolons inside those don't accidentally split a statement.

By default, errors print to stderr and execution continues. Pass `--stop-on-error` to abort on the first failure. That is the right setting for CI scripts where any failure means the schema setup is broken.

```bash
sqe-cli --embedded --file setup.sql
```

## Interactive Mode

```bash
sqe-cli --host sqe-coordinator --port 50051 --user alice
```

```
Password: ****
sqe-cli 0.1.0 connected to http://sqe-coordinator:50051 (flight)
Type SQL queries, or \q to quit. End multi-line queries with ;

sqe> SELECT * FROM raw.orders LIMIT 3;
 order_id | customer_id | amount | region
----------+-------------+--------+--------
 1        | 100         | 250.00 | EU
 2        | 101         | 150.00 | US
 3        | 100         | 300.00 | EU
(3 rows)

sqe> \q
```

### Multi-line Queries

Queries are executed when you type `;`:

```
sqe> SELECT
  ->   region,
  ->   COUNT(*) AS orders,
  ->   SUM(amount) AS total
  -> FROM raw.orders
  -> GROUP BY region
  -> ORDER BY total DESC;
```

### Commands

| Command | Action |
|---|---|
| `\q` | Quit |
| `quit` | Quit |
| `exit` | Quit |
| `Ctrl+C` | Cancel current input / quit |
| `Ctrl+D` | Quit (EOF) |

History is saved to `~/.sqe_history`.

## Single Query Mode

Execute one query and exit. Useful for scripts:

```bash
sqe-cli -H localhost -p 50051 -u alice -e "SELECT COUNT(*) FROM raw.orders;"
```

## Output Formats

### Table (default)

```bash
sqe-cli -e "SELECT 1 AS a, 'hello' AS b;" --format table
```
```
 a | b
---+-------
 1 | hello
(1 rows)
```

### CSV

```bash
sqe-cli -e "SELECT 1 AS a, 'hello' AS b;" --format csv
```
```
a,b
1,hello
```

### JSON (newline-delimited)

```bash
sqe-cli -e "SELECT 1 AS a, 'hello' AS b;" --format json
```
```json
{"a":"1","b":"hello"}
```

## Authentication

### Username/Password

```bash
# Interactive prompt
sqe-cli --user alice

# Environment variables (no prompts)
export SQE_USER=alice
export SQE_PASSWORD=secret
sqe-cli -e "SHOW SCHEMAS;"
```

### Bearer Token

Skip the password flow entirely with a pre-obtained token:

```bash
sqe-cli --token eyJhbGciOiJSUzI1NiIs... -e "SELECT 1;"
```

## Connecting in Kubernetes

```bash
# Port-forward to the coordinator
kubectl port-forward svc/sqe-coordinator 50051:50051

# Then connect locally
sqe-cli --host localhost --port 50051

# Or exec directly into the pod
kubectl exec -it deploy/sqe-coordinator -- sqe-cli
```

## Using with Trino Protocol

For compatibility with tools that speak Trino HTTP:

```bash
sqe-cli --protocol http --host localhost --port 8080 --user alice
```

This uses the Trino-compatible `/v1/statement` endpoint instead of Flight SQL.
