# Tempto Iceberg test exclusions and findings

Upstream group: `io.trino.tests.product.iceberg` at trino tag 465.
Run model: curated allow-list (`allowlist.txt`). Everything not on the
allow-list is excluded for one of the reasons below.

## Full-package sweep (2026-06-30)

Ran the entire `io.trino.tests.product.iceberg` package against SQE (389 tests,
4 passed) and harvested the distinct SQE-reported errors. Most failures are
harness-side (tests need a `hive`/`tpch` catalog or Spark/Hive setup this stack
does not provide -- e.g. `unknown catalog 'hive'`). The genuine SQE Trino-compat
gaps below were each re-confirmed with a direct curl against the `iceberg`
catalog. Filed as their own issues:

| Gap | Repro (iceberg catalog) | SQE error |
|---|---|---|
| ROW / struct types | `CREATE TABLE t(c ROW(a integer, b varchar))` ; `CAST(row('x') AS row(field real))` | `SQL type not supported for CREATE TABLE: ROW(...)` / parser `Expected: <, found: (` |
| CTAS `WITH [NO] DATA` | `CREATE TABLE t AS SELECT 1 a WITH DATA` | `Parse error: Expected: end of statement, found: WITH` |
| Session properties | `SET SESSION iceberg.compression_codec = 'ZSTD'` ; `SHOW SESSION LIKE '...'` | `Utility statement not supported: SET SESSION ...` / `SHOW LIKE` |
| Materialized views | `DROP MATERIALIZED VIEW IF EXISTS t` | `Utility statement not supported: DROP MATERIALIZED VIEW` |
| Iceberg hidden columns | `SELECT "$path" FROM t` | `Schema error: No field named "$path"` |
| UUID literal / write | `SELECT UUID '...'` ; CTAS of a uuid value | `Unsupported SQL type UUID` (note: a `uuid` column type and `CAST(... AS uuid)` in a bare SELECT parse OK) |

The `$snapshots` metadata-table column-shape gap (issue #320) was found the same
way (Trino `committed_at`/`parent_id` vs SQE `timestamp_ms`/`parent_snapshot_id`).

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

**After #319 (FETCH ... WITH TIES) merged (2026-06-30):** the FETCH error is gone;
`testRollbackToSnapshot` then failed one layer deeper on the `$snapshots`
metadata-table column shape (filed as #320). Allow-list stays 2/3 on SQE. The
full-package sweep above was run from this state.

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

## Full-package sweep batch (2026-06-30): issues #320-#328

A full-package tempto sweep surfaced nine more Trino-compat gaps. Status and
verification level (be precise: not all are stack-validated the way #314-#319
were):

| # | Gap | Status | Verified |
|---|---|---|---|
| #327 | DESCRIBE/SHOW COLUMNS ignore double-quoted identifiers (0 rows) | FIXED | Unit (quote-aware identifier split) |
| #322 | CTAS `WITH [NO] DATA` suffix parse-fails | FIXED | Unit + DataFusion execute |
| #328 | CTAS column-alias list `(a,b) AS ...` parse-fails | FIXED | Unit + DataFusion execute |
| #323 | `SET/RESET/SHOW SESSION` unsupported | FIXED | Unit + server integration |
| #324 | materialized views: DROP hard-errors / CREATE silently makes a view | FIXED | Unit (no-op DROP IF EXISTS, reject CREATE) + documented |
| #320 | `$snapshots` columns differ from Trino (`parent_id`, `committed_at`) | FIXED | Schema unit-verified; **live `$snapshots` query stack-pending** |
| #326 | `UUID '...'` literal rejected | FIXED | Unit + DataFusion execute (literal, CAST, CTAS) |
| #321 | ROW/STRUCT types in CREATE TABLE | FIXED (type mapping) | Type mapping unit-verified; **Iceberg struct write/read stack-pending** |
| #325 | Iceberg hidden columns (`$path`, ...) not exposed | **BLOCKED (upstream)** | n/a |

Notes:
- **#320, #321** ship the verifiable layer (schema/type mapping, with
  DataFusion execute tests where applicable) but their end-to-end Iceberg
  read/write is **not** validated against a live Polaris+S3 stack on this
  branch. Re-run the harness against a real stack to confirm before treating
  them as stack-validated.
- **#325 is blocked on DataFusion.** Trino's `$path` is a per-row, per-file
  column that resolves by name yet is excluded from `SELECT *`. DataFusion 54
  has no metadata/system-column mechanism (confirmed: a field marked
  `datafusion.system_column` is still returned by `SELECT *`). It is an open
  upstream proposal (apache/datafusion#20135), not in any release. Adding the
  column to the scan schema would break `SELECT *` parity, so it is documented
  as unsupported with `table_files('ns','t')` as the file-introspection
  workaround, rather than shipped as a veneer.

## Live-stack re-run batch (2026-06-30): reopened partials + new findings

The harness was run against a real Trino-481 baseline + live stack, which
re-opened three partial fixes and surfaced three new gaps.

| Issue | Status | Verification |
|---|---|---|
| #319 | FETCH WITH TIES dropped the ORDER BY column when absent from SELECT | FIXED | Unit + DataFusion execute (order col not in SELECT) |
| #330 | `CREATE TABLE ... AS TABLE <qualified.name>` parse-fails | FIXED | Unit + DataFusion execute (3-part source copy) |
| #331 | `ALTER TABLE ... EXECUTE optimize` parse-fails | FIXED (optimize) | Rewrite -> `CALL system.rewrite_data_files`; classifies as the existing maintenance procedure (unit + e2e classify). `file_size_threshold` dropped (no faithful map); destructive procs left unhandled |
| #321 | ROW/struct write: `Field id not found` on nested fields | FIXED (write path) | `arrow_schema_to_iceberg` now uses iceberg-rust `arrow_schema_to_schema_auto_assign_ids` to assign nested IDs recursively; unit-verified. **e2e struct write/read still stack-pending** |
| #326 | uuid Iceberg write "unsupported" | round-trip done; fidelity blocked upstream | Literal/CAST/CTAS already round-trip as string (Utf8) at HEAD. Genuine Iceberg `uuid` logical type needs Arrow `FixedSizeBinary(16)`, which DataFusion cannot produce from a string (no uuid type) -- same upstream class as #325 |
| #332 | LZ4 codec "not supported for Parquet/Avro" | **needs live repro** | Hypothesis disproven: parquet `lz4` feature is already enabled (via `default`), and iceberg-rust honors the passed `WriterProperties` compression as-is. The exact error string is in neither SQE nor vendored iceberg-rust. Separately, `SET SESSION iceberg.compression_codec` is echoed to the client but never reaches the writer (it reads static `config.catalog.parquet_compression`) -- a distinct gap |

Notes:
- **#319/#330/#331** are fully verified at the rewrite + DataFusion execute /
  classification layer (no live stack needed) and are closed.
- **#321** ships the real write-path fix (recursive nested field-id assignment)
  but the end-to-end struct write/read is still stack-pending; do not close on
  the unit test alone.
- **#326, #332** carry no code change this round: #326's round-trip already
  works at HEAD (genuine uuid fidelity is upstream-blocked); #332's simple
  hypotheses are disproven and it needs a live reproduction to capture the real
  error origin.

## Full-package sweep + SQL-surface probe (2026-06-30): issues #335-#344

SQE image rebuilt from main with #329/#321/#334 merged. Full
`io.trino.tests.product.iceberg` sweep: **349 tests, 2 passed, 347 failed**
(~70 failures are harness-side `unknown catalog 'hive'/'tpch'` -- need
Spark/Hive/extra catalogs this stack lacks). Merged fixes confirmed *absent*
from the entire error harvest: #314 wire error, FETCH WITH TIES, AS TABLE,
ALTER EXECUTE, bare VALUES, positional CALL, `$snapshots` shape, ANALYZE.
`rollback_to_snapshot` execution stub remains the 1 allow-list FAIL (tracked).

After the sweep, a direct curl probe battery over the broader Trino SQL surface
(functions, window fns, set ops, types, DDL, joins, subqueries, DML/MERGE,
struct access) ran against the `iceberg` catalog. Most of it passes (lambdas,
json/map/array fns, date/time, window frames, UNION/INTERSECT/EXCEPT,
approx_*, MERGE/UPDATE/DELETE, COMMENT ON, ADD/RENAME COLUMN, partitioned
CREATE, CTAS WITH/NO DATA, info_schema, struct field read). Genuine gaps filed:

| Issue | Gap | Repro | SQE error |
|---|---|---|---|
| #335 | ROW as CAST/expression type (nested + parameterized field types); blocks struct INSERT | `CAST(row(1,row(10)) AS row(a int,b row(x int)))`; `INSERT ... CAST(ROW(...) AS ROW(...))` | `Expected: type modifiers, found: (` / `Unsupported SQL type row(...)` |
| #336 | `ALTER TABLE DROP COLUMN nested.subfield` | `ALTER TABLE t DROP COLUMN _struct._field` | `Expected: end of statement, found: .` |
| #337 | unquoted ident vs case-preserved column (DESIGN DECISION) | col `"testInteger"`, `SELECT testInteger` | `No field named testinteger` |
| #338 | `ALTER TABLE SET PROPERTIES` | `ALTER TABLE t SET PROPERTIES format_version = 2` | `Expected: (, found: PROPERTIES` |
| #339 | `CAST AS VARBINARY` / varbinary type | `SELECT CAST('abc' AS varbinary)` | `Unsupported SQL type VARBINARY` |
| #340 | `listagg(...) WITHIN GROUP` | `listagg(x,',') WITHIN GROUP (ORDER BY x)` | `WITHIN GROUP is only supported for ordered-set aggregate functions` |
| #341 | `UNNEST ... WITH ORDINALITY` | `UNNEST(ARRAY[1,2]) WITH ORDINALITY` | `UNNEST with ordinality is not supported yet` |
| #342 | correlated scalar subquery & LATERAL not physically planned | `SELECT (SELECT a.x) FROM (VALUES 1) a(x)`; `LATERAL (SELECT a.x+1)` | `Physical plan does not support logical expression ScalarSubquery/OuterReferenceColumn` |
| #343 | `CREATE TABLE ... LIKE` | `CREATE TABLE c (LIKE src)` | `SQL type not supported for CREATE TABLE: src` |
| #344 | `SHOW FUNCTIONS` | `SHOW FUNCTIONS` | `Utility statement not supported: SHOW FUNCTIONS` |

False positives ruled out this round: `COMMENT ON TABLE` (test-ordering, table
absent at call time -> works once created); `PREPARE`/`DESCRIBE OUTPUT`
(prepared statements live in the Trino session via `X-Trino-Added-Prepare`
response headers that separate curl calls don't carry -- not a SQE gap). Struct
field *read* (`info.name`, `info['name']`) works; only struct *construction*
(ROW value) is blocked, by #335.
