# Tempto Iceberg test exclusions and findings

Upstream group: `io.trino.tests.product.iceberg` at trino tag 465.
Run model: curated allow-list (`allowlist.txt`). Everything not on the
allow-list is excluded for one of the reasons below.

## Headline finding (2026-06-29): SQE rejects all DDL/update over the Trino 465 JDBC client

Verified both directions on 2026-06-29 with the curated allow-list (3 tests:
`testIcebergConcurrentInsert`, `testRollbackToSnapshot`,
`testRollbackToSnapshotWithNullArgument`):

- Against the real Trino baseline (`--baseline`, Trino 481): **3 SUCCEEDED / 0
  FAILED** -- the harness runs full test bodies to green (concurrent insert ran
  28.6s, rollback_to_snapshot 5.4s).
- Against SQE: **0 SUCCEEDED / 3 FAILED**, every failure the same error below.

So the harness is sound; SQE fails solely on this bug. Every statement a Trino
465 JDBC client sends fails with:

```
java.sql.SQLException: Error executing query: Columns must be set when decoding data
  at io.trino.jdbc.$internal.client.ResultRowsDecoder.toRows(ResultRowsDecoder.java:88)
```

Root cause (primary sources on both sides):

- Trino 465 client `client/trino-client/.../ResultRowsDecoder.java`:
  ```java
  if (data == null || data.isNull()) return NULL_ROWS;   // real Trino: DDL/update sends data == null -> early return
  verify(columns != null && !columns.isEmpty(), "Columns must be set when decoding data");
  ```
- SQE `crates/sqe-trino-compat/src/server.rs` `build_page_response` (around
  line 627) always sets `data: Some(page_data)`. For a column-less DDL/update
  result `page_data` is `[]` (empty but non-null), and `columns` is `[]`
  (empty). The client sees non-null `data`, skips the early return, then the
  `!columns.isEmpty()` check fails.

Reproduce without tempto (note `data` is an empty array, not null):

```
curl -s -u root:s3cr3t -H "X-Trino-User: root" -H "X-Trino-Catalog: iceberg" \
  -H "X-Trino-Schema: default" -H "Content-Type: text/plain" \
  --data "CREATE TABLE iceberg.default.tmp(a bigint)" \
  http://localhost:28080/v1/statement
# -> "columns": [], "data": [], "updateType": "CREATE TABLE"
```

Suggested fix (SQE side, out of scope for this harness branch): in
`build_page_response`, emit `data: None` when the result has no columns
(`paginated.columns.is_empty()`), matching real Trino which omits `data` for
update/DDL statements. SQE's PREPARE path already does the equivalent (it leaves
`data` defaulted to `None` while setting `columns: Some(vec![])`).

Until that is fixed, the allow-list cannot pass against SQE. Run the same suite
against the real Trino in the compare stack to confirm the harness itself is
sound: `scripts/tempto-test.sh --baseline`.

## Excluded: require Spark/Hive (cannot run against SQE alone)
- `TestIcebergSparkCompatibility` -- sets up tables via onSpark() (308 calls).
- `TestIcebergSparkDropTableCompatibility` -- onSpark() table setup.
- `TestIcebergRedirectionToHive` -- needs a Hive catalog + redirection.
- `TestIcebergHiveViewsCompatibility` -- needs Hive views.
- `TestIcebergHiveMetadataListing` -- needs a Hive metastore listing.
- `TestIcebergFormatVersionCompatibility` -- onSpark() cross-version setup.

## Excluded: need HDFS / file-existence assertions (tempto hdfs config)
- `TestCreateDropSchema` -- every method injects `HdfsClient` and asserts file
  existence under the warehouse directory; needs `databases.hive.*` + an HDFS
  client, which this REST/S3 stack does not provide.

## Excluded: fails against real Trino too (needs Hive migrate setup)
- `TestIcebergProcedureCalls.testMigrateUnsupportedTransactionalTable` -- failed
  on the Trino 481 baseline (no Hive table to migrate in this stack), so it is
  not a clean SQE signal. The other `testMigrate*` methods use onSpark()/onHive().

## Allow-list (verified green on the real Trino baseline; see allowlist.txt)
- `TestIcebergInsert.testIcebergConcurrentInsert` -- pure onTrino despite the
  `hms_only` group label; CREATE TABLE + concurrent INSERT + SELECT.
- `TestIcebergProcedureCalls.testRollbackToSnapshot` -- exercises the
  `rollback_to_snapshot` procedure (SQE must support it for this to pass once
  the DDL blocker is fixed).
- `TestIcebergProcedureCalls.testRollbackToSnapshotWithNullArgument`.

## Partially included (future)
- `TestIcebergPartitionEvolution.testDroppedPartitionField` -- one onSpark()
  setup call; would need a `spark` tempto database to run.
- `TestIcebergOptimize` -- include only if SQE supports `ALTER TABLE ... EXECUTE optimize`.
