## 1. Index model and manifest plumbing

- [x] 1.1 Add `PartitionIndexEntry` / `PartitionIndex` serde types with `format_version: 1` to `crates/sqe-catalog/src/hive/manifest.rs`; round-trip test; unknown `format_version` rejected on load
- [x] 1.2 Enforce a new `max_partition_index_entries` cap at load (planted oversized index fails closed); wire the config value through `HiveConfig` in `crates/sqe-core/src/config.rs` and the example TOML
- [x] 1.3 Type partition values on load from declared key types (`string`, `varchar`, `int`, `bigint`, `date`); normalize `__HIVE_DEFAULT_PARTITION__` to NULL; untypeable value is a load error naming the entry
- [x] 1.4 Validate every index entry location through `[storage.tvf]` `check_path` on load, same rule as `storage_descriptor.location`, with tests

## 2. Statement classification and dispatch

- [x] 2.1 Classifier arms for `Statement::Msck`, `SHOW PARTITIONS` (pre-scan over `ShowVariable`), and `AlterTableOperation::{AddPartitions, DropPartitions}` on Hive tables in `crates/sqe-sql/src/classifier.rs`; table-driven tests
- [x] 2.2 Coordinator dispatch: replace the `reject_if_hive_table` hooks at `query_handler.rs:1312` (ADD PARTITION) and :1876 (ANALYZE) with `HiveDdlHandler` delegations; add MSCK and SHOW PARTITIONS arms; non-Hive routing unchanged (test pins it)
- [x] 2.3 One dispatch test per statement per the cycle-1 convention: supported paths execute, unsupported variants (`PURGE`) reject naming `hive_external`

## 3. Index maintenance DDL

- [x] 3.1 New `crates/sqe-catalog/src/hive/partition.rs`: index model helpers, add/drop/repair operations against `HiveManifestStore`
- [x] 3.2 `ALTER TABLE ADD PARTITION`: read-modify-write append with location metadata capture; duplicate without `IF NOT EXISTS` errors, with it no-op; audit event emitted; policy gate test
- [x] 3.3 `ALTER TABLE DROP PARTITION`: entry removal, data untouched; absent without `IF EXISTS` errors, with it no-op; `PURGE` rejected naming cycle 4
- [x] 3.4 `MSCK REPAIR TABLE`: recursive list, `k=v/` segment derivation reusing cycle-1 discovery rules, wholesale rewrite; cap enforcement leaves old index unchanged on failure
- [x] 3.5 `MSCK REPAIR TABLE` skip-and-continue: junk dirs, untypeable values, undeclared/missing keys, and files at the table root are skipped like Hive/Athena; projection-enabled tables are a successful no-op (no listing, index unchanged); ADD/DROP PARTITION on projected tables stay errors

## 4. Index-driven reads

- [x] 4.1 Build scan from explicit file groups derived from index entries in `HiveTableProvider::scan` when an index is present (no prefix LIST); reuse resolved `FileFormat` from `serde_map.rs`
- [x] 4.2 Reuse cycle-1 NULL-partition filter rewriting (`normalize_default_partition_filters` / `nullify_default_partition_columns`) on the indexed path; parity tests run identical queries against indexed and non-indexed fixture copies
- [x] 4.3 Partition pruning on typed index values for equality, IN-list, range, and IS NULL predicates before file-group construction
- [x] 4.4 ETag-keyed invalidation of cached manifests carrying indexes in `HiveMetadataCache`; TTL fallback where the store has no ETag; test that a changed ETag reloads the index
- [x] 4.5 Statistics from index metadata (file counts/sizes) replacing the one-shot statistics listing when an index is present
- [x] 4.6 `SHOW PARTITIONS`: index-read path plus listing fallback; output format matches Athena expectations

## 5. Partition projection

- [x] 5.1 Projection config parser/validator in `partition.rs`: `enum`|`integer`|`date`|`injected`, ranges, formats, intervals; unknown key or malformed value errors naming the property; validated at create AND on every manifest load
- [x] 5.2 Projection evaluator materializing bounded virtual entries at plan time from standard `k=v/` layout under the table location; respects `max_partition_index_entries`
- [x] 5.3 Predicate pruning of the computed set before file-group construction; zero-match query scans nothing
- [x] 5.4 `injected` columns: query-context injection when provided, NULL otherwise; documented
- [x] 5.5 Decision recorded: `DROP PARTITION` on projected tables errors pointing at projection ownership (design open question)

## 6. ANALYZE TABLE statistics

- [x] 6.1 `ANALYZE TABLE <hive table>` handler: Parquet row counts from footers via existing footer cache; CSV/JSON full-decode counts; persist into `sqe.statistics` (total + per entry)
- [x] 6.2 Planning consumes persisted counts when present; size-derived fallback otherwise; test both sides
- [x] 6.3 Policy gate on ANALYZE matching reads; audit event

## 7. End-to-end and docs

- [x] 7.1 Local-integration coverage from cycle 1's Task 11 brief: MinIO/RustFS root exercising `ATTACH`, `CREATE EXTERNAL TABLE`, ADD/DROP PARTITION, MSCK REPAIR, SHOW PARTITIONS, partitioned read, DROP TABLE leaving data in place
- [x] 7.2 Benchmark per the design's plan: partitioned CSV + Parquet fixture, cold/warm, with/without partition-pinning predicate; before/after numbers into `docs/evidence/perf/` and committed JSON to `benchmarks/results/`
- [x] 7.3 Rewrite the reference page limitations section (`docs/site/book/src/reference/hive-external-tables.md`): cycle-2 items move out, staleness model documented plainly, projection semantics documented, `docs/site` leak scan green
- [x] 7.4 Full workspace gates: `cargo test --all`, `cargo clippy --all-targets --all-features -- -D warnings`; update README roadmap, `nextsteps.md`, and this tasks.md per repo rules
