**STATUS 2026-10-26: Spark clause order in CREATE TABLE parses (`fix/partitioned-by-tblproperties`).**

Found on the playground3 Databricks run and in `rewrite_data_files_distributed_parity::partitioned_with_deletes_parity`, which failed at its first statement and so had never run: `CREATE TABLE ... PARTITIONED BY (region) TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')` -> "Expected: end of statement, found: TBLPROPERTIES".

Cause: `normalize_partitioned_by` rewrites `PARTITIONED BY` to `PARTITION BY` (so the Iceberg transform list parses), and sqlparser's Generic dialect only takes `LOCATION`, `WITH (...)` and `TBLPROPERTIES` before `PARTITION BY` and `COMMENT` after it. Probed: `PARTITION BY (r) TBLPROPERTIES`, `... LOCATION`, `... WITH (...)` and `USING iceberg` in any position all failed.

- `partition::reorder_create_table_clauses`, run in `pre_parse_pipeline` right after `normalize_partitioned_by`: scans the clauses after the column list up to `AS` / end, puts them in the order LOCATION, WITH, TBLPROPERTIES, PARTITION BY, COMMENT, and drops `USING iceberg`. Clause text is spliced from the source by token offsets, so quoted strings are untouched. Parse-gated: SQL that parses is returned as is, and the result is only used when it parses. Any other clause (`STORED AS`, `CLUSTER BY`, `USING delta`) leaves the statement as written. `CREATE EXTERNAL TABLE` is skipped (Hive path).

Gates:
- `cargo test -p sqe-sql --lib` -> 585 passed (7 new in `partition::tests`); `cargo clippy -p sqe-sql --all-targets -- -D warnings` -> clean.
- `make test-integration FILTER=partitioned_with_deletes_parity` (native workers on :50052/:50053) -> ok, 1 passed.
- `make test-integration FILTER=partition` -> ok, 20 passed.

Left open (confirmed, not changed here):
- sqlparser keeps `COMMENT`, `WITH (...)` and `TBLPROPERTIES` in one `table_options` slot, so no order of two of them parses (`COMMENT 'c' TBLPROPERTIES (...)` fails both ways). Left with its parse error.
- Managed `CREATE TABLE` drops `COMMENT` and `LOCATION` silently: `write_handler` only reads `SqlOption::KeyValue` and always uses `unique_table_location`. Views already store `comment` (`catalog_ops.rs:2522`). LOCATION needs a decision (reject, or honour within the catalog's allowed locations), not a quiet switch.
- Parse-gated rewrites each require the final text to parse, so a statement that needs two of them (this reorder plus Trino `WITH NO DATA`) gets neither. Pre-existing property of the chain.

**NEXT:** store `COMMENT` as the `comment` table property and reject (or honour) `LOCATION` on managed CREATE TABLE; then fold `COMMENT` into `TBLPROPERTIES` here. After that, the embedded CLI findings (`FOR VERSION AS OF`, `SHOW TABLES IN`, DDL through sqe-sql).
