**STATUS 2026-10-17: V3 variants verified on Spark 4.1, the worker path and the embedded CLI (`test/variant-interop-worker`, #849).**

`variant_json_args` moved from `sqe-coordinator` to `sqe-core`, and the embedded CLI (`sqe-cli`) wraps the JSON functions too (`json_functions_read_a_variant_column`).

Spark 4.1.3 + iceberg-spark-runtime-4.1 1.12.0, `variant_spark_interop`: Spark reads an SQE V3 `JSON` table after a merge-on-read DELETE (`to_json`, `variant_get`, `typeof` = variant, the deletion vector applied), and SQE reads a Spark-written `VARIANT` table through `json_get_int`. The Spark helpers moved from `equality_delete_integration` to `common::spark_interop`. The compose stack needed three fixes: `user: root` (Ivy under `HOME=/nonexistent`), an in-container forward of `localhost:19000` to RustFS (the catalog's storage endpoint overrides `s3.endpoint`), and an in-memory Derby metastore per `spark-sql` run (the healthcheck and the tests shared `./metastore_db` and corrupted it).

Worker path, `variant_distributed` (skipped by `scripts/integration-test.sh`; needs a worker on :50052 started with `SQE_WORKER__ADVERTISE_URL=http://localhost:50052`): a V3 variant scan runs on the worker and comes back as variant storage, and the JSON functions read it. It uses `distributed_failure_e2e::rig`, which zeroes the distribution thresholds; under `setup_handler_with_workers` a small table stays on the coordinator (`distribution_file_threshold` 4, byte threshold 128 MiB). `integration_test::test_distributed_select` has both problems (a one-file CTAS under the default thresholds, and it looks for "worker" in `node_id`, which is the worker URL), so it cannot pass against a local worker.

`delete_file_threshold` counts delete files per data file, and a V3 data file has at most one deletion vector, so on the new default a threshold above 1 only counts equality deletes. `rewrite_data_files_deletes`' threshold test is pinned to V2 for that reason; the procedures and autonomous-compaction pages now say so.

**NEXT:**
- A V3-aware delete trigger for compaction and `table_health`: deleted-row share per data file (deletion-vector cardinality / record count).
- Fix `test_distributed_select`: zero the thresholds and match `node_id` against the worker URLs.
- Run `make test-access-control-spark` on the Ranger quickstart's Iceberg 1.12 Spark image.
