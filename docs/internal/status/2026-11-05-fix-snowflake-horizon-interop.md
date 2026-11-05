**STATUS 2026-11-05: Snowflake Horizon reads: embedded mode, partial page index (`fix/snowflake-horizon-interop`).**

First live run against a Snowflake account's Horizon Iceberg REST endpoint.
The coordinator (`bearer_passthrough`, `[catalog] access_delegation =
"vended-credentials"`) loaded and planned every probe table: V2 and V3,
VARIANT, GEOMETRY, GEOGRAPHY, identity and month/bucket/truncate partitions,
nested object/array/map and the scalar types. The tables were still empty
then, so this proved schemas, not data. Embedded mode had two gaps, both fixed
here:

- No way to request vended credentials: new ATTACH option `ACCESS_DELEGATION`
  (`'vended-credentials'` | `'none'`, DuckDB's `ACCESS_DELEGATION_MODE`
  spelling accepted) in `sqe-catalog/src/mount.rs`.
- Horizon answered ATTACH's concurrent `loadTable` burst with 429, and the
  throttled tables vanished silently. Vendored REST client replays 429s
  (`SQE PATCH (catalog-429)`, `client.rs`), and datafusion-iceberg's
  `IcebergSchemaProvider::try_new` loads 16 tables at a time.

Seeding the probe tables surfaced a third: Snowflake writes no offset index
for VARIANT and nested columns, and SQE loaded the page index as
`PageIndexPolicy::Required` (vendored reader via `PageIndexPolicy::from(true)`,
`iceberg_scan.rs` small-file path, worker executor), so any read of such a
table failed with "missing offset index". All three are `Optional` now
(`SQE PATCH (partial-page-index)`); VARIANT paths and struct/array/map values
then read correctly. Snowflake also pins an Iceberg GEOMETRY column to SRID
4326, and SQE reads it as `geometry(4326)` with correct WKT and area.

Environment findings, not SQE bugs: an external volume encrypted with a
customer KMS key fails, because Snowflake's down-scoped vended session lacks
`kms:Decrypt` (even `loadTable` 403s), so use `SNOWFLAKE_MANAGED` storage.
Creating a table through Horizon needs the database's `EXTERNAL_VOLUME` and
`CATALOG = 'SNOWFLAKE'` parameters set.

Open gaps: Snowflake upper-cases unquoted names, and SQE folds them to lower
case (#337), so Snowflake tables need quoted names. Trino's
`case-insensitive-name-matching` is the model for a fix. sqe-cli installs no
tracing subscriber, so a table skipped at ATTACH (Delta in Unity Catalog, an
unreadable volume) still disappears without a message.

**NEXT:** live interop harness `scripts/interop/snowflake-horizon/` (both
directions, DML, value compare in embedded, coordinator and distributed), then
the matrix `snowflake-horizon-catalog` cells.
