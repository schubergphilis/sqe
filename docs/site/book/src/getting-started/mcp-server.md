# Run SQE as an MCP server

SQE speaks the Model Context Protocol. An AI assistant (Claude Code, Codex, or
anything that speaks MCP over Streamable HTTP) connects to `/mcp` and gets a set
of read tools for your catalogs: list schemas and tables, describe a table, run
a read-only query, sample rows. The assistant queries your lakehouse in the
same session it writes code in.

This page is the fastest path to a running server. For the operator reference
(OAuth, per-principal tokens, audit attribution, write access) see
[Model Context Protocol](../deployment/mcp.md).

## The fastest path: no config file

`sqe-server` starts from built-in defaults when there is no config file, so a
read-only MCP endpoint over an AWS Glue catalog is one command:

```bash
# Credentials from your SSO session (or any AWS profile).
aws sso login --profile my-profile
eval "$(aws configure export-credentials --profile my-profile --format env)"
export AWS_REGION=eu-central-1

sqe-server \
  --glue-catalog glue \
  --allow-s3-prefix s3://my-lake-raw/ \
  --allow-s3-prefix s3://my-lake-refined/ \
  --mcp-anonymous \
  --mcp-port 8098
```

That serves `/mcp` on port 8098 with no authentication, attaches every Glue
database in the region as the SQL catalog `glue`, and answers reads with your
own AWS credentials. Every `sqe-server` build carries the Glue client, so a
stock release binary or image works; no feature flag is needed. Build with
`--release`: a debug binary decodes Parquet and
runs DataFusion 5 to 10 times slower, and the server warns at startup when it
is one. The startup line and `/api/v1/status` report `build_profile` and
`git_sha` so you can check which binary answered.

The flags:

- `--glue-catalog <name>` attaches the region's Glue Data Catalog under that SQL
  name. Tables resolve as `<name>.<database>.<table>`. Databases and tables are
  read lazily, so one unreadable table does not take the catalog down. Athena
  views are expanded from their Glue definition and queried like tables.
- `--glue-database <db>` restricts the catalog to specific databases. Repeatable,
  and each value may be comma-separated (`--glue-database claude,afas`). Empty
  exposes every database. A table in any other database is not found, so a shared
  Glue catalog can be scoped to what one deployment should see.
- `--allow-s3-prefix <prefix>` (repeatable) is the read boundary. Reads use the
  engine's own AWS credentials, so a table whose location is not under one of
  these prefixes is refused. Name every bucket your tables live in. A prefix of
  `s3://` matches every S3 bucket, which is the same as `--allow-all-buckets`.
- `--allow-all-buckets` removes the read boundary: every object-store path is
  readable with the engine's credentials. Convenient on a single-tenant laptop
  or a trusted-network gateway, where the credentials already scope what is
  reachable. Do not use it on a multi-tenant coordinator. The startup log warns
  when it is set.
- `--mcp-only` serves just `/mcp`. It closes the Flight SQL, Trino HTTP and
  Quack query ports, leaving MCP and the ops health/metrics port. For an
  assistant-only deployment that should not expose the SQL front doors. It needs
  MCP enabled, so pair it with `--mcp-anonymous` (or static/OAuth in a config
  file).

## Credentials

SQE resolves AWS credentials through the standard SDK chain for both the Glue
catalog and the S3 data reads: environment keys, `AWS_PROFILE`, an SSO session,
`credential_process`, IRSA, and finally IMDS. So an SSO login is enough:

```bash
aws sso login --profile my-profile
export AWS_PROFILE=my-profile
export AWS_REGION=eu-central-1
sqe-server --glue-catalog glue --allow-all-buckets --mcp-only --mcp-anonymous --mcp-port 8098
```

No `aws configure export-credentials` step is needed. The SDK refreshes the SSO
or STS session in the background, so a long-running server keeps working after
the initial token would have expired.

The region resolves the same way: `SQE_STORAGE__S3_REGION` (or `[storage]
s3_region`) wins, then `AWS_REGION`, then `AWS_DEFAULT_REGION`, and finally the
active profile's `region` in `~/.aws/config`. So `export AWS_PROFILE=...` alone
gives both the credentials and the region.
- `--mcp-anonymous` serves `/mcp` with no auth, read-only. Every caller is one
  anonymous identity reading with the engine's credentials. For a laptop or a
  trusted network only. It is refused when an identity provider is configured
  and under `production_mode`.
- `--mcp-port <n>` is the MCP port. Zero mounts `/mcp` on the existing web
  listener instead.

The same flags work on top of a real config file (`--config sqe.toml`) when you
need TLS, a Polaris catalog, storage keys, or per-principal MCP tokens. The
flag-only default is only for the case where there is nothing else to configure.

For a catalog other than Glue, put a `[catalogs.*]` block (or a
`[[hive.roots]]` entry) in a config file and drop `--glue-catalog`. See
[Catalog backends](./catalogs.md).

## Connect a client

The endpoint is Streamable HTTP at `http://<host>:<port>/mcp`. In anonymous
mode no `Authorization` header is needed.

Claude Code (`.mcp.json`):

```json
{
  "mcpServers": {
    "sqe": {
      "type": "http",
      "url": "http://localhost:8098/mcp"
    }
  }
}
```

Codex (`~/.codex/config.toml`):

```toml
[mcp_servers.sqe]
url = "http://localhost:8098/mcp"
default_tools_approval_mode = "reads"
startup_timeout_sec = 20
tool_timeout_sec = 60
```

A quick check by hand:

```bash
curl -s -X POST http://localhost:8098/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":
       "list_tables","arguments":{"catalog":"glue","schema":"demo"}}}'
```

## The tools

Read tools, always available:

| Tool | What it does |
|---|---|
| `list_catalogs` | Catalogs the session can reach, including Hive roots and the Glue catalog. |
| `list_schemas` | Schemas (databases) in a catalog. |
| `list_tables` | Tables in a schema. |
| `describe_table` | Column names, types and nullability for a table. |
| `get_table_ddl` | `SHOW CREATE TABLE` output. |
| `get_table_stats` | Iceberg statistics; Hive external tables have none, so use `describe_table`. |
| `search_tables` | Find tables by name across a catalog's schemas (SQL LIKE pattern). |
| `sample_rows` | `SELECT * ... LIMIT n` as the caller (default 10, at most 100). |
| `execute_sql_readonly` | One read-only statement, checked with SQE's SQL parser, not a regex. |
| `explain_query` | The plan for a read query. |
| `whoami` | Identity metadata for the session; never tokens. |
| `refresh_metadata` | Drop cached catalog metadata for one table, one schema, or the session's catalog view, so the next call re-reads it. For a table created or changed outside SQE; rate-limited per scope. |

`execute_sql_readonly` accepts exactly one statement and rejects DDL, DML,
grants, secrets and session changes before they reach the query handler. The
write tool `execute_sql_change` is not advertised unless `allow_writes = true`,
which anonymous mode never allows.

## Auth modes

`--mcp-anonymous` is one of three shapes. The other two need a config file:

- **Anonymous** (`--mcp-anonymous`): no auth, read-only, engine credentials.
  Trusted network only.
- **Static** (`[mcp.auth] mode = "static"`): per-consumer bearer tokens you
  issue, each mapped to a SQE user and roles.
- **OAuth** (`[mcp.auth] mode = "oauth"`, the default): the caller brings a
  token from your identity provider; SQE validates the scopes and passes the
  identity through to the catalog.

Static and OAuth are covered in [Model Context Protocol](../deployment/mcp.md).

## Security

In anonymous mode there is no last line of defense at the source: the engine
reads whatever its credentials can reach, and `--allow-s3-prefix` is the only
boundary. Every caller is logged as one anonymous identity, so the audit trail
shows what was read but not by whom. Put the port behind a trusted network
boundary, keep the prefix list tight, and move to static or OAuth mode before
anyone but you can reach it.
