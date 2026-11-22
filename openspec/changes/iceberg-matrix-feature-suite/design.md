## Context

Upstream's suites (`trino_feature_tests.py` is the template, about 1250 lines) share one stack: Polaris 1.7 with an in-memory metastore, RustFS on :9000, bucket `warehouse`, catalog `demo`, and client credentials `root:s3cr3t` with scope `PRINCIPAL_ROLE:ALL`. Each test returns a `TestResult` of pass, fail, skip or error with an optional `partial_reason`. A runner compares the results with the engine's JSON file and writes reports.

SQE already authenticates to Polaris with client credentials (`crates/sqe-auth/src/oauth.rs`, default scope `PRINCIPAL_ROLE:ALL`). It also has a protocol client (`tests/parity/parity_client.py`: `TrinoHttp`, `FlightAdbc`). Every state-file entry names the Rust test that justifies it, which gives the SQL each Python test needs.

## Goals / Non-Goals

**Goals:** reproducible by a third party from a clean machine, black box, one command, an honest denominator, and drop-in compatibility with upstream's `tests/` layout.

**Non-goals:** changing ratings; testing the cloud catalogs (Glue, Unity, Horizon, BigQuery Lakehouse) in this run, since they need cloud accounts and are reported as unmeasured; replacing the Rust integration tests.

## Decisions

1. **Exact verdict mapping (Polars-style), not Trino's loose one.** Upstream's Trino runner counts any pass as agreeing with either `full` or `partial`. Ours uses: pass without `partial_reason` = `full`; pass with `partial_reason` = `partial`; fail = `none`; skip = unmeasured. A cell agrees only when the measured level equals the claimed level.
2. **Rubric-driven cell list.** The suite reads `docs/evidence/iceberg-matrix-rubric.json`, upstream's `features.json` pinned by commit, so the run can be compared with the pin. A rubric cell with no registered test is reported as `untested` and fails the run. A test for a cell not in the rubric is an error. New upstream features therefore show up as failures, not as silent gaps.
3. **Second witness for writes.** Every write-path cell asserts what is on storage through PyIceberg's catalog client against the same Polaris: snapshot summary operations, manifest content type (data, position deletes, equality deletes), delete-file format (Puffin deletion vectors for v3), `format-version`, partition spec and sort order ids, `next-row-id` and row lineage fields, and statistics files. Where PyIceberg 0.12 cannot model a v3 feature (variant, shredded variant, geometry, unknown) or write a v3 table, the witness is the raw `loadTable` JSON, the raw Avro manifests and the Parquet file, and v3 fixtures are PyIceberg v2 tables upgraded by a raw `upgrade-format-version` commit. A Spark 4.x cross-check of those reads is follow-up task 2.6b.
4. **`format-version` asserted on every measured table** from the catalog's `loadTable` response, so a v3 cell can never pass on a v2 table.
5. **Measured N of 54.** The report header gives measured, agreed, discrepancies, unmeasured and untested counts, and a score computed only over measured cells next to the claimed score. Unmeasured cells are listed by name.
6. **Black box.** The suite imports nothing from SQE and reads no SQE logs or internal endpoints. It uses the client protocol, the catalog and storage only. Where a phrase in the description cannot be observed from outside (a planner's use of statistics, "efficient", "faster"), the cell records what it observed as a note and does not lower the level: that is a limit of the suite, not of SQE. A test first looks for a black-box proof (read-support removes a data file a pruned scan must skip). This rule is the author's, not a maintainer decision, and moves results toward the claim, so the report lists such notes per cell for review.
7. **Layout mirrors upstream.** The files are `tests/matrix/sqe_feature_tests.py`, `tests/matrix/docker/{docker-compose.sqe.yml,start-sqe.sh,stop-sqe.sh,env.sh}` and `tests/matrix/requirements.txt`. Moving them to upstream's `tests/` and `tests/docker/` needs no code changes beyond paths. We do not vendor upstream's `spark_fixture.py`; our fixture code is our own.
8. **Isolation.** The compose project is `sqe-matrix`, and host ports are offset (Polaris 48181, RustFS 49000, SQE Trino HTTP 48080; the 18xxx range is taken by the repo's own dev stack), all overridable. Upstream defaults stay reachable via the env vars, so the same suite runs against upstream's own `start-polaris.sh` stack.
9. **Protocols: the level is the engine's capability.** A cell is measured over whichever SQE client protocol supports the feature: Trino HTTP by default, Flight SQL (ADBC, `SQE_FLIGHT_PORT`, default 48051) where needed. A required dependency, not an optional one. When a feature works over one protocol and not the other, the cell is not lowered; the gap goes in the report's "Protocol gaps" section and is filed as an SQE bug (maintainer decision, 2026-11-22).
10. **The bar is the rubric's feature description, read literally.** `features.json` describes each feature (for example schema evolution: "add, drop, rename, reorder columns or change column types"). A capability in the description that SQE lacks makes the cell `partial`, even where upstream rates a peer `full` for less (maintainer decision, 2026-11-22). The test names the missing capability in `partial_reason`.
11. **A refusal consistent with a partial claim is not a verdict** (author's rule, open to maintainer review; it applies only to multi-arg transforms v3 today). When the claim rests on behaviour the suite cannot exercise yet (for example reading a table another engine wrote), the cell is skipped as unmeasured with the reason. It does not fail on the part the claim already concedes.
12. **Disclosure.** The report names the image under test and every non-default SQE setting in `tests/matrix/docker/sqe-config.toml` (today `maintenance.orphan_min_age_secs = 1`).
13. **Homegrown witness reads.** Where PyIceberg 0.12 cannot apply a v3 deletion vector or an equality delete, the suite applies them itself from raw Avro manifests and Parquet / Puffin files. Cross-check those cells against Spark when the Spark witness lands (task 2.6).
14. **Export.** `export_upstream.py` writes `src/data/platforms/oss/sqe/sqe.json` in upstream's shape, with ratings from the state file and links from evidence. The suite's report can be attached to the submission PR as verification.

## Risks / Trade-offs

- **Polaris version drift**: upstream pins 1.7 and we run 1.9. The suite defaults to upstream's image tag so results match theirs. `POLARIS_IMAGE` overrides it.
- **Measured score below 159**: this is the point. Each discrepancy becomes an issue or a state-file correction made by a maintainer.
- **Runtime**: around 54 cells with metadata reads. Aim for under 10 minutes on a laptop, excluding image pulls.
