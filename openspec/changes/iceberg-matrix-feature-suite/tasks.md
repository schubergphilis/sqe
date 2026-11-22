## 1. Vertical slice

- [x] 1.1 `tests/matrix/docker/docker-compose.sqe.yml` + `start-sqe.sh` / `stop-sqe.sh` / `env.sh`: Polaris (upstream image tag) + RustFS + bucket + `demo` catalog + SQE (`SQE_IMAGE`), project `sqe-matrix`, offset ports
- [x] 1.2 Runner skeleton in `tests/matrix/sqe_feature_tests.py`: rubric loader, test registry, `TestResult`, exact verdict mapping, format-version helper, PyIceberg witness helper, JSON + Markdown report, exit code
- [x] 1.3 Three cells end to end: `table-creation:v2`, `position-deletes:v2` (witness: position-delete manifest), `deletion-vectors:v3` (witness: Puffin DV on a v3 table). All three agree, measured on a 0.54 dev image; manifests are read as raw Avro (fastavro) because PyIceberg 0.12 drops the DV fields `referenced_data_file`, `content_offset` and `content_size_in_bytes`
- [x] 1.4 `untested` cells fail the run; report says "measured N of 54"

## 2. Fan out (one group per MR or per agent run)

- [x] 2.1 Row-level: equality deletes, merge-on-read, copy-on-write (v2/v3)
- [x] 2.2 Schema: schema evolution, type promotion, column default values
- [x] 2.3 Partitioning: hidden partitioning, partition evolution, multi-arg transforms (v3 multi-arg is skipped: it needs a fixture table written by another engine)
- [x] 2.4 Time travel, branching and tagging, table maintenance (expire snapshots, rewrite data files, remove orphans)
- [x] 2.5 Statistics (Puffin), bloom filters
- [x] 2.6 v3 types: variant, shredded variant, geometry/geography, nanosecond timestamps, unknown; lineage. Witnessed from raw metadata JSON, manifests and Parquet; no Spark yet
- [ ] 2.6b Spark 4.x witness: cross-check the homegrown v3 reads (DV and equality-delete application, variant decode, lineage derivation)
- [x] 2.7 Catalog cells: REST + catalog integration measured locally; Glue, Unity, Horizon and Google Lakehouse skipped (credential opt-in not built)

## 3. Tooling

- [x] 3.1 `tests/matrix/export_upstream.py --check`: every rubric cell exported once at the state file's level, weighted total 159 of 162 (compared with the state file, not with `cargo xtask matrix-report`)
- [x] 3.2 `make matrix-suite` (`SQE_IMAGE` param, reports to `target/matrix-suite/`), `make matrix-export`
- [x] 3.3 `tests/matrix/README.md`: quickstart for an outsider, env vars, how to read the report

## 4. Measure and decide

- [ ] 4.1 Full run against the v0.57.0 image; commit the report under `docs/evidence/iceberg-matrix-suite/`
- [ ] 4.2 Maintainer review of each discrepancy: fix SQE, correct the state file, or record why the test is wrong
- [ ] 4.3 Decide the public image route (see proposal Impact)
