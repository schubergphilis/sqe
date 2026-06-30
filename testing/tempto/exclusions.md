# Tempto Iceberg test exclusions and findings

Upstream group: `io.trino.tests.product.iceberg` at trino tag 465.
Run model: curated allow-list (`allowlist.txt`). Everything not on the
allow-list is excluded for one of the reasons below.

## Headline finding (2026-06-29): SQE rejects all DDL/update over the Trino 465 JDBC client

**FIXED (issue #314):** `build_page_response` now emits `data: None` when the
result has no columns (`paginated.columns.is_empty()`), so column-less DDL/update
responses omit `data` the way real Trino does. The Trino 465 client's
`data == null -> NULL_ROWS` early return is now hit and the `!columns.isEmpty()`
assertion is never reached. The original finding is kept below for the record.

**Re-run after the fix (SQE rebuilt from main + MR !463, 2026-06-29):** the
`Columns must be set when decoding data` error is GONE (0 occurrences) -- DDL /
`USE` / `CREATE` now decode. With the wire protocol unblocked, the same three
allow-list tests surfaced the next layer of gaps (parser/semantic, not wire):

1. `INSERT INTO t VALUES 1` (bare single value, no parentheses) -> SQE parser
   `Expected: (, found: 1`. Trino accepts row values without parens; SQE
   (sqlparser-rs) requires `VALUES (1)`. Blocks the insert setup in all three.
   **FIXED (issue #315):** the Trino HTTP pre-parse chain now wraps bare
   `VALUES` rows (parse-gated tokenizer rewrite) so `VALUES 1` -> `VALUES (1)`.
2. `CALL system.rollback_to_snapshot(NULL, 'customer_orders', <id>)` ->
   `CALL system.* requires named arguments like table => 'ns.t'`. Trino allows
   positional procedure arguments; SQE requires named.
   **FIXED (issue #316):** SQE now accepts positional CALL args, mapping
   `rollback_to_snapshot('schema', 'table', <id>)` by Trino's verified
   parameter order and rejecting NULLs with Trino's `<field> cannot be null`
   phrasing.

**End-to-end re-run with all fixes merged (#314/#315/#316/#317/#318, SQE rebuilt
from main, 2026-06-30): 2 SUCCEEDED / 1 FAILED against SQE.**
- `TestIcebergInsert.testIcebergConcurrentInsert` -> SUCCESS (11.3s; real
  concurrent CREATE/INSERT/SELECT workload now passes on SQE).
- `TestIcebergProcedureCalls.testRollbackToSnapshotWithNullArgument` -> SUCCESS.
- `TestIcebergProcedureCalls.testRollbackToSnapshot` -> FAILURE, new gap:
  `This feature is not implemented: FETCH clause is not supported yet` on
  `SELECT snapshot_id FROM ...\"...$snapshots\" WHERE parent_id IS NOT NULL
  ORDER BY committed_at FETCH FIRST 1 ROW WITH TIES`. SQE/DataFusion does not
  implement `FETCH FIRST ... WITH TIES` (Trino supports it). The `$snapshots`
  read and positional `rollback_to_snapshot` CALL both work now. This is the
  next compatibility finding (candidate for its own issue).

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
