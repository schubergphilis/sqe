**STATUS 2026-10-20: Spark 4.1 + Iceberg 1.12 reads SQE's merge-on-read writes back; matrix evidence de-stubbed (`test/v3-spark-interop-853`, #853, #855).**

New module `crates/sqe-coordinator/tests/it/mor_spark_interop.rs`, nine tests.
Each does an SQE write, asserts the snapshot summary (so a copy-on-write
fallback cannot pass for merge-on-read), then requires Spark and SQE to return
the same rows:

- V2 MoR DELETE: `added-position-delete-files`, no `deleted-data-files`.
- V3 MoR DELETE twice on one file: `added-dvs` = 1, then `removed-dvs` = 1.
- Keyed DELETE, UPDATE and MERGE on V2 and V3: `added-equality-delete-files`.
- V3 row lineage after a MoR UPDATE and a MoR MERGE: updated rows keep
  `_row_id` with a newer sequence number, untouched rows keep both, and
  Spark reads the same values as SQE.

SQE has no DDL for identifier fields, so the keyed tables are created by Spark
(`ALTER TABLE ... SET IDENTIFIER FIELDS id`). Without them UPDATE and MERGE
run copy-on-write; `v3_e2e::equality_delete_update_on_v3_table` sets the
table property `write.identifier-field-ids`, which no product code reads, so
that test never exercised the equality-delete path. Same for
`row_lineage_survives_mor_update_and_merge_on_v3_table`: it runs
copy-on-write. The matrix no longer cites either.

The matrix cited empty `#[ignore]` stubs as evidence. Removed:
`mor_update_merge_integration::{mor_update_writes_data_plus_equality_delete,
mor_merge_commits_all_files_in_one_snapshot, spark_reads_sqe_mor_updated_table,
trino_reads_sqe_mor_updated_table}`, the two tautological shape tests in the
same file (`assert_eq!((1, 1, 0), (1, 1, 0))`), and
`equality_delete_integration::mor_delete_writes_one_equality_delete_file`.
`spark_reads_sqe_equality_delete_file` never read an SQE file (Spark wrote and
read its own table); renamed to `spark_harness_reads_its_own_mor_delete`.

Matrix (`docs/evidence/iceberg-matrix-state.json`): position-deletes:v2,
equality-deletes:v2/v3, merge-on-read:v2/v3, lineage:v3, deletion-vectors:v3
cite the new tests; the cross-engine and format-version caveats are gone; the
read-support:v3 caveat is geometry only (shredded variants read unshredded).
New caveat on equality deletes and merge-on-read: SQE cannot declare
identifier fields. Levels and score unchanged (148/162).

Gate: `make test-integration FILTER=mor_spark_interop` -> 9 passed, no skips
(Spark 4.1.3 + Iceberg 1.12.0 container up). The renamed smoke test is a plain
`#[test]`, which `make test-integration` (`--ignored`) does not run:
`cargo test -p sqe-coordinator --test it spark_harness_reads_its_own` -> 1
passed, not skipped. `cargo clippy -p sqe-coordinator --all-targets -D
warnings` clean. `cargo xtask matrix-report --print-only` -> 148/162 (91.4%).

Left open:
- #853 "run in a CI job": there is no integration CI job (#866). No Trino
  engine reads Iceberg here, so the old Trino stub had nothing to test.
- `equality_delete_integration::{concurrent_writer_produces_retryable_commit_conflict,
  cow_update_delete_retries_on_concurrency_conflict}` are still empty
  `#[ignore]` stubs (DML commit races; not matrix evidence).

**NEXT:** `ALTER TABLE ... SET / DROP IDENTIFIER FIELDS` in SQE, then the
two v3_e2e "MoR" tests use it and assert merge-on-read in the summary; then #856 (Iceberg bloom spec
keys), #858/#859 (nested unknown), the DataFusion decimal unwrap-cast fix.
