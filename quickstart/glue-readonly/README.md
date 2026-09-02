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
# 1. Credentials. The server reads SQE_STORAGE__* , not AWS_* .
eval "$(aws configure export-credentials --format env)"
export AWS_REGION=eu-central-1
export SQE_STORAGE__S3_ACCESS_KEY="$AWS_ACCESS_KEY_ID"
export SQE_STORAGE__S3_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export SQE_STORAGE__S3_SESSION_TOKEN="$AWS_SESSION_TOKEN"

# 2. Mirror the catalog (~610 tables, a couple of minutes).
python3 scripts/glue-to-sidecar.py --root ~/.sqe/gluemirror

# 3. Edit sqe.toml: set hive.roots.location to your mirror path and
#    allowed_object_store_prefixes to your buckets.

# 4. Start.
SQE_CONFIG=quickstart/glue-readonly/sqe.toml sqe-server
```

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

## What works, and what does not

Verified against a live catalog of 692 Glue tables:

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
- ORC, Avro, SequenceFile and RCFile are rejected by name. None appear in
  this catalog; Avro is the cheapest to add.
