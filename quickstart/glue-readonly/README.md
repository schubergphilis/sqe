# Query an AWS Glue Hive catalog from SQE

Registers every table in a Glue Data Catalog and serves them over Flight SQL
and MCP, read-only, with no identity provider. Development shape; see the
hardening notes at the bottom of `sqe.toml`.

Nothing is written to your data lake. The mirror holds metadata only, and
the data stays exactly where Glue says it is.

## Why a mirror rather than a Glue backend

SQE has no Glue metadata source yet. `--catalog-backend glue` builds an
*Iceberg* Glue catalog, which fails outright on a catalog of Hive external
tables: the upstream provider eagerly loads every table and the first one
without a `metadata_location` aborts the whole attach.

What SQE does have is the Hive external-table provider, whose manifest
format mirrors Glue's `TableInput` and `StorageDescriptor` field for field.
So `glue-to-sidecar.py` reads Glue and writes those manifests. A manifest's
`storage_descriptor.location` is validated against the `[storage.tvf]`
allowlist rather than against the manifest root, which is what lets the
sidecars live outside the buckets they point at.

The mirror is a snapshot. Re-run the script to pick up Glue changes.

## Run it

```bash
# 1. Credentials. The standard AWS variables are enough.
eval "$(aws configure export-credentials --format env)"
export AWS_REGION=eu-central-1

# 2. Mirror the catalog (~610 tables, a couple of minutes).
python3 scripts/glue-to-sidecar.py --root ~/.sqe/gluemirror

# 3. Edit sqe.toml: set hive.roots.location to your mirror path and
#    allowed_object_store_prefixes to your buckets.

# 4. Start.
SQE_CONFIG=quickstart/glue-readonly/sqe.toml sqe-server
```

Or skip the `[mcp]` block entirely and use the flags, which set the same
thing and force read-only:

```bash
sqe-server --config your.toml --mcp-anonymous --mcp-port 8098
```

`--mcp-anonymous` is refused when `[[auth.providers]]` configures an
identity provider, and under `production_mode`. Those checks run on the
resulting config, so a flag cannot get you past one the TOML equivalent
would have failed.

If a query comes back with `Error performing PUT
http://169.254.169.254/latest/api/token`, the engine has no S3 credentials
and is falling through to instance metadata. On a laptop that almost always
means the SSO token expired: re-run `aws sso login`, then re-export the
credentials from step 1. Temporary credentials are read once at startup and
never refreshed, so a long-running server needs a restart after renewal.

Tables resolve as `glue.<database>.<table>`.

```bash
curl -s -X POST http://127.0.0.1:8098/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":
       "execute_sql_readonly","arguments":{"sql":
       "SELECT count(*) FROM glue.app_db_dotcomregistration.customer"}}}'
```

## Embedded, without a server

The CLI reads `AWS_*` directly, so credentials need no remapping, and it
accepts a local mirror without opening `allow_local_paths`:

```bash
eval "$(aws configure export-credentials --format env)"
AWS_REGION=eu-central-1 sqe-cli --embedded --memory \
  --hive-root glue=file://$HOME/.sqe/gluemirror/ \
  -e "SELECT count(*) FROM glue.app_db_dotcomregistration.customer"
```

## Who read what

`[metrics] audit_log_path` writes one JSON line per query naming the actor,
the fully-qualified tables the plan touched, the operation, timing and row
counts. It is empty by default, which makes the audit logger a silent
no-op and is easy to mistake for "SQE does not log table access".

```json
{"time":"2026-09-02T07:45:06Z","kind":"query","actor":{"username":"mcp-anonymous"},
 "status":"success",
 "resources":[{"catalog":"glue","namespace":["claude"],"name":"user_usage",
               "object_type":"table"}],
 "policy":{"row_filters_applied":0,"denied":false},
 "timing":{"duration_ms":844},
 "query":{"text":"SELECT count(*) ...","statement_type":"query"},
 "integrity":{"seq":0,"prev_hash":"000...","hash":"4fbf..."}}
```

`kind` separates `query` from `grant`, `admin_ddl`, `auth`, `session` and
`policy_decision`; `query.statement_type` distinguishes read from write
within a query. `resources` is resolved from the logical plan, so it lists
the base tables actually scanned, including through a view. The `integrity`
chain hashes each line against its predecessor, so a deleted line is
detectable.

Under `--mcp-anonymous` every caller logs as `mcp-anonymous`, which is the
honest consequence of anonymous mode: the trail shows what was read but not
by whom. Static or OAuth mode puts a real principal in `actor`.

Set `[metrics.audit] format = "ocsf"` for OCSF instead of the native shape,
and `[metrics.audit_export]` to ship to an OTLP collector.

## What works, and what does not

Verified against a live catalog of 692 Glue tables, through an
unauthenticated MCP endpoint (`--mcp-anonymous`) and through the embedded
engine:

- Plain Parquet and CSV tables.
- **Extensionless Parquet** (Athena CTAS output, 356 tables here). The file
  TVFs cannot read these at all; the Hive provider matches every object
  under the prefix regardless of suffix.
- **Partition projection**, including `NOW` ranges and multi-axis
  date x hour x minute tables. Partition columns are queryable columns.
- Tables whose Glue `Location` no longer exists (27 here) answer with the
  right schema and zero rows, the way Athena does.

Known gaps, in the order they will bite:

- **Gzipped JSON fails** (131 tables). `serde_map::listing_options` builds
  `JsonFormat::default()` and never sets a compression type, so gzip bytes
  reach the JSON parser: `Json error: Encountered unexpected ''`.
- **Nested types are rejected** (85 tables): `array<...>`, `struct<...>`,
  `map<...>` fail with "is a nested type, which is not supported".
- **Views are skipped** (56 `VIRTUAL_VIEW` tables). The script does not
  emit manifests for them; their Trino SQL lives base64-encoded in
  `presto_view`.
- `list_schemas` over MCP does not see Hive roots. Query through
  `execute_sql_readonly` instead.
- **`describe_table` and `information_schema.tables` fail**, because they
  enumerate every catalog including the deliberately-unreachable Iceberg
  anchor at `catalog_url`. Repeated attempts trip a 30-second circuit
  breaker that then blocks working queries too. Use `list_tables`, or
  `SELECT * ... LIMIT 1`, for column discovery.
- **The mirror goes stale.** Snapshot-style tables rotate their directory
  daily: a table reading 72,946 rows one day returns 0 the next, because
  Glue now points at `20260902-<uuid>/` while the mirror still names
  `20260901-<uuid>/` and that prefix has been deleted. Zero rows is also
  what a genuinely-missing prefix looks like, so re-run the mirror before
  concluding a table is empty. Cron it.
- ORC, Avro, SequenceFile and RCFile are rejected by name. None appear in
  this catalog; Avro is the cheapest to add.
