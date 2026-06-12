# SQE — Next Steps

> Status as of 2026-06-12 (night). **Trino-grade scan visibility in query profiles (`feat/scan-profile-detail`), driven by the SSB structural diagnosis.** Trino's EXPLAIN ANALYZE on SSB q2.2/q3.3 settled why SSB is the one suite Trino still wins: identical join order and broadcast strategy, but Trino collects the exact build-side key set (SortedRangeSet, e.g. 6494 point ranges for the brand-filtered part keys) in ~100ms, WAITS for it at split generation, and applies it row-level inside the scan: 99.84-99.99% of lineorder dies pre-join. SQE's range-only dynamic filters are structurally powerless on SSB's uniform FKs (min/max spans the whole key domain), so distributed scans ship all 60M rows and single-node decodes all 60M before the Tier-2 wrapper kills 99.8% post-decode. To make that one-profile-readable, `IcebergScanExec` now reports: `bytes_planned`/`bytes_scanned` (object-store bytes, both reader paths, Drop-flushed so LIMIT-terminated scans still report), `rows_prefilter`/`rows_decoded` (RowFilter vs decode kill rates), `rows_filtered_dynamic` + `rows_passed_filter_pending` (post-decode wrapper drops; rows that streamed through while a dynamic filter was still the lit(true) placeholder), `dynamic_filters_resolved/pending`, `files_matched`, `planning_time` (split-generation analog). Validated on SSB SF10: q2.2 single-node line reads `rows_decoded=59.96M rows_filtered_dynamic=59.86M bytes_scanned=765MB` vs Trino's `Input: 60M, Filtered: 99.84%, Physical input: 731MB` -- same bytes, wrong place to filter. **NEXT:** (1) build-side key-set/bloom dynamic filters: ship the membership set to workers (exact under threshold, bloom above; predicate_proto only carries range conjuncts today), apply pre-decode in the parquet RowFilter, bounded wait before fact-task open -- expected to move SSB 53.6s/42.0s toward or past Trino's 28-41s; then the prior NEXT list (worker scan backpressure, sorted loads, per-shape routing, DF upstream filing).

> Status as of 2026-06-12 (evening). **SF10 turned around in one day: parallel parquet decode (!352), level compare rig, greedy memory pool (!353).** Morning SF10 numbers showed SQE 3-5x slower than Trino on every scan-bound query; profiles (first SF10 run with `query_profile = "all"`) showed q06 waiting 6.3s of 6.4s on a scan decoding 8.5M rows on ONE core: iceberg-rust's `try_buffer_unordered` overlaps I/O but serializes decode onto the polling thread. !352 splits >=256MB whole-file parquet tasks into ~128MB byte-range subtasks (midpoint row-group assignment; overlap semantics would have double-read boundary row groups, regression-tested) each decoded on its own spawned runtime task, capped by the existing concurrency semaphore. Then the rig itself: host->Docker port-forward caps at ~96MB/s single / ~163MB/s aggregate while Trino read in-VM at ~320MB/s, so half the gap was the pipe; new `tests/compare/sqe-singlenode.toml` + compose rig runs both engines in-network with equal envelopes (8 VM CPUs, bounded heaps, 5GB/query). Then q39: failed at 8GB where Trino needs 5GB; NOT memory retention and NOT a cast bug: FairSpillPool hard-caps every registered spillable consumer at pool/N (q39's two CTE pipelines register ~90 -> ~95MB each) and the Partial aggregate cannot emit early because the optimizer derives PartiallySorted from the constant `d_moy = 1` and `GroupOrderingPartial::emit_to()` never advances past a constant key (unfixed on DF main; #20445 only fixed the panic; upstream filing pending). !353: `coordinator.memory_pool = "greedy"` (default, TrackConsumersPool<GreedyMemoryPool>) / `"fair"` rollback. q39 21.8s/3864 rows at 8GB (Trino 29.2s). Final SF10 level-rig table (Trino 481): TPC-H single 130.5s / dist-2w 95.5s / Trino 106.4-138.6s (SQE dist WINS); SSB 42.0 / 53.6 / 28.0-41.1 (single-node right for star shapes); TPC-DS 543.9 / 338.3 / 328.4-468.0. q86 "0 rows" was an h2 GoAway transport flake, fixed with a compare retry. **NEXT:** (1) worker scan backpressure: q23/q37/q72/q82 fail distributed at SF10 when the scan reservation hits 2GB of the 4GB worker pool because parallel decode outruns Flight shipment; (2) make Tier-1 dynamic filters land before fact-task open (Trino waits 1s at split generation) so the single-threaded Tier-2 wrapper stops eating 20s on q09-class queries; (3) sorted bench loads (`files_pruned_minmax=0` everywhere today); (4) per-shape routing single vs distributed; (5) file the DF upstream issue.

> Status as of 2026-06-12. **Generator fidelity v3 (DuckDB-validated) + dynamic filter pushdown into distributed scans.** The DuckDB oracle (validate-generator-tpcds.py vs `CALL dsdgen`) proved 16 of the 29 TPC-DS SF0.1 vacuous compare queries were generator gaps, not scale artifacts, and TPC-C was fully broken at fractional scales (`scale as i32` = 0 warehouses pinned every FK to a nonexistent w_id=0). Fixed with dsdgen-exact vocabularies and structures: real county list, official categories/classes/colors, (category, class)->brand-base correlation (q63's brand AND class conjunction is unsatisfiable without it), Midway/Williamson/TN stores, the deterministic 7200-row household_demographics cross product, log-uniform item prices, weekly half-item inventory snapshots, scale-aware web_page/warehouse null stripes. Validator failures 17 -> 5 at sf0.1 / 7 at sf1, every survivor a <=5-official-row correlation query (q04/q11/q17/q39/q74 sf0.1; q08/q24/q25/q41/q54/q85/q91 sf1 — these need cross-year/cross-channel/zip-overlap correlation machinery). TPC-DS SF1 vacuous 19 -> 8, TPC-C 2/8 -> 8/8 both scales. The one row-content DIFF across all 7 suites (q75, 57 vs 55 rows) is Trino rounding its DECIMAL(17,2) division to scale 2 (ratios 0.8983/0.8984 -> 0.90, dropped from `< 0.9`); DuckDB returns SQE's exact rows. Perf side, the new query profiles showed forced-distribution fact scans shipped EVERYTHING (SSB SF1 lineorder: 6M rows / 115MB per query): `DistributedScanExec` never received dynamic join filters (no pushdown hooks, and `try_distribute` swaps the scan node AFTER the optimizer deposited them on the Iceberg scan). Now it accepts them, carries them across the swap, waits up to 100ms for build sides (Trino-style), snapshots, converts to logical Exprs, and ANDs them into the ticket's `predicate_proto` (no wire change; worker RowFilter applies them) — SSB q3.3 ships 449 rows instead of 6M. `find_iceberg_scan` also picks the LARGEST scan by stats instead of first-DFS (q4.x was distributing a dimension while lineorder ran locally). tpce trade_result capped at LIMIT 1000 (21.6M-row result OOM-killed the Trino compare container twice). MRs: `fix/bench-generator-fidelity-v3`, `perf/distributed-dynamic-filters`. **NEXT:** correlation machinery for the 12 remaining vacuous queries (store-return -> catalog-repurchase chains, cross-year repeat customers, zip/store overlap), and per-query SSB SF1 parity under the forced-distribution rig (dispatch+S3 floor still loses to Trino's in-memory dims on sub-second queries).

> Status as of 2026-06-11. **Passive per-query profiling shipped (`[query] query_profile = "off" | "slow" | "all"`).** DataFusion populates per-operator metrics during normal execution; we used to throw them away when the stream finished, which is part of why the q72 hunt took five days (per-operator timings were only visible by re-running under EXPLAIN ANALYZE). The `StreamFinalizer` now renders `DisplayableExecutionPlan::with_metrics` on success AND on error (failures always profile when the mode is not off), prefixed with elapsed/rows and an `unpushed_scans=N` full-scan flag (scan nodes displaying `predicate=[]`), capped at 64 KiB, logged once under the `query_profile` target, and stored on the `QueryRecord` (surfaced on `/api/v1/queries/{id}` detail only). `DistributedScanExec` now implements `metrics()` with `BaselineMetrics` around its stream so the profile shows real rows/elapsed on that row instead of blanks. Benchmark sweeps at SF1 now leave per-operator evidence for slow queries without interactive re-runs.

> Status as of 2026-06-11. **Differential testing made honest: generator fidelity + fail-fast + VACUOUS status (!333), idle-timeout now errors like Trino (!334), deltalake-core pinned (!335), stall tracked (#261).** The SF1 full-suite compare looked healthy (133/134 "Match") but most of it validated nothing: the sqe-bench generators produced data the official queries cannot select (TPC-DS fact-table `*_date_sk` columns were 100% NULL because row builders emitted Date values into Int32 columns and `cols_to_arrays` silently coerced the mismatch to None; TPC-H `p_type` was a 15-of-150 hardcoded subset missing q08's literal and every customer had orders so q22's NOT EXISTS was empty; SSB brands were 3-digit instead of dbgen's `MFGR#mcnn` and cities were not the `%-9.9s%d` format q3.3/q3.4 probe), so both engines agreed on empty and the harness scored empty-vs-empty as Match. !333 fixes all four generators, makes the type-mismatch coercion a panic (the sweep test now generates every table of all 7 benchmarks fail-fast), and adds a `Vacuous` compare status so agreement-on-nothing is visible in every report. Validation on a fresh stack: TPC-H 22/22 with 0 vacuous, SSB 12/13 + 1 vacuous, TPC-DS 70/99 matched + 29 vacuous with 0 diffs and 0 failures; value-validated coverage went from ~54/134 to 104/134 queries. !334 fixes the sibling silent failure in the engine: the issue #75 stream idle-timeout guard ended a stalled query as a clean empty result; it now surfaces `Query aborted: produced no results for 300s (idle timeout)` through Flight and marks the query Failed (Trino's EXCEEDED_TIME_LIMIT semantics), proven in the wild when the intermittent distributed stall hit SSB q1.1 mid-suite. The stall itself (3 occurrences across 3 suites, ~1 per 100 sequential distributed queries on a long-lived stack, passes instantly in isolation, lost-wakeup suspected) is filed as #261 with a full evidence dossier. !335 pins `deltalake-core = "=0.32.1"`: delta-rs deleted `delta_datafusion::DeltaTableProvider` in patch release 0.32.4, and the floating spec let a lockfile regeneration break the optional `delta` feature invisibly until an --all-features build. Remaining TPC-DS vacuous queries mostly need correlated sales-to-returns rows (q01/q17/q24-class); future generator fidelity work. **NEXT:** root-cause #261 (instrument the distributed scan/stream wakeup path), and migrate read_delta.rs to 0.32.4's TableProviderBuilder as a deliberate bump.

> Status as of 2026-06-10. **Distributed projection pushdown restored (follow-up to !327).** The !327 "number of columns(2) must match number of fields(16)" failure was root-caused to the WORKER, not the coordinator schema contract: the streaming scan path (5bc4c02, 2026-05-15) returned `builder.schema()` (full parquet file schema) from `open_parquet_stream` while the built stream emits projected batches, so the Flight encoder advertised 16 fields and shipped 2-column batches; the coordinator's Flight decode failed before reassembly ran. The old buffering path used `batches[0].schema()` (projected), which is why the April distributed baseline was 22/22 WITH projection pushdown. Fix: worker takes the schema from the built `ParquetRecordBatchStream`; coordinator re-populates `projected_columns`/`projected_field_ids` (tested `scan_task_projection()` helper, all-or-nothing field IDs); `reassemble_worker_batch` hardened (equal width now also requires positional name equality, by-name reorder for parquet FILE-order batches, positional accept for renamed columns under field-ID projection, fail-closed otherwise). Validated vs Trino 465 on TPC-H SF0.1 (single worker, forced distribution): 22/22 matched on every run; median total 4794ms -> 1567ms (3.1x), scan-heavy q01/q06/q14/q15/q17/q19 subtotal 1800ms -> 415ms (4.3x; q06 9.0x, q14 10.3x). Workers now read only projected columns from S3 and ship only those over Flight. MR: `fix/restore-projection-pushdown`.

> Status as of 2026-06-01 (updated). **Web UI metrics dashboard extended: sparklines, tooltip, histogram legend, rows-out and latency time series.** Each of the six Activity stat cards now renders a 36px sparkline of its 15-min bucket series (Total/Finished/Failed/Running in blue, Failed in red, Avg Latency and Rows Out in blue). The Query activity histogram now shows a legend (Completed in blue, Failed in red) and a "15-min buckets, last 12h" caption. All charts are hoverable: a single shared tooltip (`#tip`, event-delegated from `document`, lives outside the rewritten `#overview` subtree) shows `HH:MM + value` on mouseover of any bar, sparkline column, or gauge sparkline segment. Backend: `MetricsSample` extended with `total_output_rows`, `finished_queries`, `exec_ms_sum`; `HistoryBucket` replaced `queriesCompleted`/`queriesFailed` with `total`, `finished`, `failed`, `rowsOut`, `avgLatencyMs`. Histogram bars now stack `finished` + `failed` (previously double-counted failures via `total+failed`). New unit tests: `bucket_samples_avg_latency_zero_when_no_finished`, extended `bucket_samples_two_buckets_delta` and `bucket_samples_clamps_negative_delta`. All 26 affected tests pass; clippy clean.

> Status as of 2026-06-01. **Read-only web UI shipped.** A network-gated ops dashboard is embedded in the coordinator's existing health server (`metrics_port + 1`): `/` serves a no-build single-page dashboard, and `/api/v1/queries`, `/api/v1/queries/{id}`, `/api/v1/workers` expose `QueryTracker` / `WorkerRegistry` state as JSON (Ballista/Trino-style). No login (protect at the network layer); toggle with `[metrics] web_ui` (default on). Spec/plan: `docs/superpowers/specs/2026-06-01-sqe-web-ui-design.md`, `docs/superpowers/plans/2026-06-01-sqe-web-ui.md`. **NEXT (phase 2):** an interactive SQL console + cancel + OIDC login on the UI.

> Status as of 2026-05-31. **Ballista wound down; bespoke distributed execution is the only engine.** After driving the ballista opt-in path to functional parity on the common path, we measured it honestly and removed it. It was ~2.2x slower where it completed (TPC-H), could not finish the TPC-DS analytical core (an upstream datafusion-proto aggregate-serialization bug plus an executor-eviction-on-task-error bug), and its scheduler is less capable than our `WeightedScheduler` (Ballista 53 has no consistent-hash affinity, no scan locality, no straggler handling). The `sqe-ballista` crate, the `[query] engine` switch, and all integration wiring are removed; the ADBC unpadded-base64 Flight handshake fix (a real dbt-sqe connectivity fix found during the work) is kept. Decision, architecture notes, and borrowable ideas: `docs/ballista-evaluation-learnings.md`. Full historical detail (design, phases, divergence ledger D1-D13): `docs/archive/ballista-evaluation/`. **NEXT:** an SQE web UI (queries/tasks/workers, Ballista- and Trino-style) over the existing `QueryTracker` / `FragmentInfo` / `WorkerRegistry` state.

> Status as of 2026-05-30 (SUPERSEDED by 2026-05-31). **Ballista parity gate, criterion #1 (per-user bearer passthrough) code-complete.** The user reframed the ballista relationship: SQE is the lakehouse SQL server (protocols, targets, speed, policy SQL are ours); ballista is narrowed to the distributed scheduler/task-management brain. Migration contract = Option 3 with parity-gated retirement (bespoke stays default, ballista opt-in, retire only at functional AND speed parity, functional blockers first). Spec: `docs/archive/ballista-evaluation/2026-05-28-sqe-on-ballista-cutover-design.md` ("Migration contract & parity gate"). First gate closed: the user bearer now threads through the PLAN (logical codec stamps it -> scheduler attaches to provider -> `IcebergScanExec` -> `EncodedSqeScan` -> executor mints a per-(user,table) `FileIO`, cached single-flight to keep D4's no-per-task-round-trip invariant), bypassing ballista's `ConfigExtension` propagation (D8). Trust model preserved (only the bearer travels). Unit-verified (wire round-trip, full-bearer cache keying, no-bearer fallback); per-user isolation is NOT E2E-verifiable on the single-principal dev stack. Plan: `docs/archive/ballista-evaluation/2026-05-30-ballista-bearer-passthrough.md`. **NEXT** on the gate: criterion #2 (policy-rewritten mask/row-filter plans survive the codec). Plus the standing E2E ballista-mode no-regression smoke (single-principal) and the multi-node speed gate (criterion #5, task 5b).

> Status as of 2026-05-15. **Four-wave audit-fix campaign: 130 issues filed, 19 themed MRs merged, ~110 issues closed.** A separate audit pass produced 130 GitLab issues. We ran them through four sequential waves of parallel agents (MR !195 to !213). Wave 1 (4 MRs): critical policy correctness, tests infrastructure, auth hardening, Trino/Flight protocol completeness. Wave 2 (5 MRs): worker-side auth, SecretString migration, scheduler isolation, write-path correctness, policy on DELETE/UPDATE. Wave 3 (5 MRs in two batches): async hygiene, auth/session config, code-quality refactor, caching/perf, build hygiene + observability. Wave 4 (5 MRs in two batches): remaining correctness, test coverage, hygiene tail, operator tunability, type-safety polish. Total 108 commits. Five rebases needed: four structural (config.rs anchor collisions), one semantic (SecretString migration did not thread through `with_worker_secret` / `start_credential_refresh_task` builder signatures, caught at the next agent's first build, patched in MR !205). The only remaining audit issue is #2 (`fix/bearer-concurrency-race`, existing WIP branch). Blog write-up: `docs/blog/2026-05-15-nineteen-mrs-four-waves.md`.
>
> Highlights from the campaign: SecretString newtype + sealed Session credentials, per-user FairSpillPool + query_semaphore, tonic channel pool, partition fan-out on IcebergScanExec + IncrementalScanExec, streaming worker output, mid-stream error termination, per-user TableMetadataCache keying (stops vended-cred leaks), field-ID parquet projection, time-travel provider scoping, MERGE namespace fix, CatalogCommitConflict retry, Drop-guard S3 cleanup on write cancel, HMAC mask key (opt-in via `policy.mask_key`), worker `do_get` + `refresh_credentials` auth gate, full TrinoStats + TrinoError fields, X-Trino-Set-* response headers, Flight SQL `GetSqlInfo` expansion, prepared-statement bind values, `do_get_tables` filter args, info_schema SQL-standard type names, AccessControlBackend + PolicyEngine enums, `[workspace.package]` + MSRV, default features flipped to rest-only with `full-backends` umbrella + `Dockerfile.full`, tonic HTTP/2 window + keepalive tuning, OPA circuit breaker + metrics, catalog roundtrip histogram, `error_code` label on `sqe_query_count_total`, audit `tables_touched`, per-worker `WorkerLoadTracker` reservation, idle-timeout for tracked streams, supervised `tokio::spawn` helper, constant-time API-key compare, base64 varbinary in Trino responses, decimal(20,0) for UInt64.

> Status as of 2026-05-04. **SQL surface lift: JSON + TIME + JDBC v3 live test, MoR confirmed already shipped** (branch `feat/iceberg-loader-s3tables`). The doc audit revealed three "missing features" that turned out to already be implemented; the code changes ship the two that genuinely needed wiring. **Score 163/189 (86.2%) -> 164/189 (86.8%).**
>
> - **`sqe:jdbc-catalog:v3` flips partial -> full.** Added `jdbc_postgres_v3_table_format_version_roundtrip` in `crates/sqe-catalog/tests/backends_integration.rs::sql_postgres`: creates a `format-version=3` table through the JDBC backend, drops the in-memory handle, reloads, and asserts the metadata still reports V3. Closes the engine-wiring caveat that has been on the cell since Phase L.
> - **`JSON` logical type shipped.** `SqlType::JSON -> Utf8` in `sql_type_to_arrow`. `CAST(json_col AS BIGINT|VARCHAR|DOUBLE)` rides DataFusion's built-in coercion; JSON extraction stays available via the existing `json_extract` / `json_get_*` UDFs. Trino-compat doc flips one ❌ to ✅ in the JSON section.
> - **`TIME` / `TIME(p)` shipped.** Maps to `Time64(Microsecond)` for precisions 0..=6 (Iceberg's `time` is microsecond-only). `localtime()` now returns Time64 (was incorrectly returning Timestamp). `extract_component` handles Time64Microsecond + Time64Nanosecond arrays/scalars; `hour() / minute() / second()` work on TIME columns; `year() / month() / day()` raise a clear plan error per Trino spec. `TIME WITH TIME ZONE` rejects with NotImplemented pointing at `TIMESTAMP WITH TIME ZONE`.
> - **MoR DELETE was already wired.** The Trino-compat doc claimed "MoR feasible but SQE uses CoW only". Reading `handle_delete_dispatch` shows that statement was stale: it has read `write.delete.mode` from table properties since Phase O+, routing to position-delete (no PK) or equality-delete (with PK) writers. Doc now reflects reality.
> - **Repaired tests broken by the loader refactor.** Commit 378bd9f deleted `crates/sqe-catalog/src/backends/{glue,hms,sql}.rs` but left `tests/backends_integration.rs` referencing the removed types. Migrated `mod glue` and `mod hms` to the upstream `GlueCatalogBuilder` / `HmsCatalogBuilder` directly (same path the loader takes). Replaced `mod sql` with a builder smoke test for the new vendored `iceberg-catalog-sql`.
> - Spark cross-engine read test (`spark_reads_sqe_equality_delete_file`) is plain `#[test]` and self-skips when docker is absent, not `#[ignore]`. Matrix evidence on `sqe:equality-deletes:v2` updated. All 4 maintenance procedures have dedicated live tests; matrix notes on `sqe:table-maintenance:v2/v3` updated.
>
> Status as of 2026-04-30. **Phase Q + Phase R: Unity OSS live test + bloom-filter footer probe shipped** (MR !115 and !116). Phase Q flips `sqe:unity-catalog:v2/v3` from partial to full via a read-only smoke against the bundled `unity.default.marksheet_uniform` table on the `unitycatalog/unitycatalog:main-2f2e32d` image. Phase R flips `sqe:bloom-filters:v2/v3` to full by closing the last evidence gap with a self-contained footer-inspection test (`writer_props_emit_bloom_filter_in_parquet_footer`) and corrects the misleading "missing worker-side data writer" caveat (no separate worker writer exists). The bench-bloom-on-write negative result is now consolidated in `docs/features/runtime-filter-pushdown.md`. **Score 158/189 (83.6%) -> 162/189 (85.7%).** OSS release artifacts (SECURITY.md, .github/ISSUE_TEMPLATE, PULL_REQUEST_TEMPLATE) added; pre-public docs audit pass cleaned up the AWS profile leak in the catalogs blog.
>
> Status as of 2026-04-29. **Phase O + Phase P: live catalog matrix integrated** (MR !113, branch `feat/matrix-phase-o-live-catalogs`). Five catalogs now have live integration tests in `crates/sqe-catalog/tests/backends_integration.rs`: Hive Metastore (apache/hive:standalone-metastore-4.1.0 over Thrift), Project Nessie (ghcr.io/projectnessie/nessie:0.107.5 over Iceberg REST), JDBC Postgres (docker-compose postgres), AWS Glue (real eu-central-1 account), AWS S3 Tables (federated Glue Iceberg REST endpoint with SigV4). Phase P added an `aws-sigv4` cargo feature to the vendored `iceberg-catalog-rest` crate that swaps the OAuth/Bearer authenticator for an AWS SigV4 signer when `rest.sigv4-enabled=true`. Five matrix cells flip partial -> full (HMS v2/v3, Nessie v3, Glue v2/v3); rest-catalog and aws-glue-catalog cell notes enriched with the SigV4 path. **Score 153/189 (81.0%) -> 158/189 (83.6%).** Default `sqe-catalog` build now ships every supported backend compiled in (rest, sql-postgres, hms, glue, hadoop). Engine session-manager wiring gap is the only remaining caveat on non-REST cells; a coordinator built with --features hms/glue/sql can construct the catalogs but the engine still routes SQL through the REST path. The S3 Tables case is unaffected because S3 Tables IS Iceberg REST and rides the existing path.
>
> Status as of 2026-04-28. **Runtime filter pushdown into IcebergTableScan integrated** (MR !112, branch `feat/iceberg-scan-runtime-filter`). New `iceberg::expr::DynamicPredicate` trait + `TableScanBuilder::with_dynamic_predicate(...)` in the vendored fork, plus an iceberg-datafusion bridge that absorbs DataFusion 53 runtime filters from `HashJoinExec` build sides and feeds them into the reader's existing row-group / page-index / row-filter pruning paths. **TPC-H SF1: 18.4s -> 14.5s (-21.3%, 22/22 match). TPC-H SF10: 163.9s -> 143.6s (-12.4%, q15 RowDiff resolved).** Five follow-up fix attempts at the per-task bind cost all reverted; the engineering log lives at `docs/features/runtime-filter-pushdown.md`. Upstream issue filed at apache/iceberg-rust#2376 with the API ask. Filed as MR !112; bloom-on-write branch (`feat/bench-bloom-on-join-keys`) deliberately stays unmerged because it regresses by +25.9s when layered on Path B-2 (bloom and runtime filters prune the same row groups, bloom adds eval overhead with no incremental benefit).
>
> Status as of 2026-04-26. **Iceberg matrix parity Phase N (partition-evolution) integrated.** Matrix score **153/189 (81.0%)**, up from 151/189 (79.9%) after Phase M, 129/189 (68.3%) after Phase I, 99/189 (52.4%) baseline. Phase N adds `ALTER TABLE ADD/DROP/REPLACE PARTITION FIELD` end-to-end (pre-parser + classifier + coordinator handler + writer fix for unpartitioned-but-evolved specs); both `partition-evolution:v2` and `partition-evolution:v3` flip from partial to full. Phase M added `PARTITIONED BY (...)` for the six standard Iceberg transforms (identity, year, month, day, hour, bucket, truncate, void) with TaskWriter routing. Phase I (V3 path validation) flipped 16 V3 cells: table-creation, write-insert, read-support, copy-on-write, write-merge-update-delete, merge-on-read, position-deletes, equality-deletes, schema-evolution, statistics, cdc-support, time-travel, type-promotion, catalog-integration, polaris, rest-catalog. Root-cause fix: Iceberg REST `CreateTableRequest` has no dedicated `format-version` field, so SQE now forwards it through the reserved table property. CREATE TABLE TBLPROPERTIES are forwarded to the catalog and re-emitted via SHOW CREATE TABLE. FOR VERSION AS OF registers snapshot-pinned providers under a writable schema alias. 13/13 V3 e2e tests pass against docker-compose.test.yml. **SQE wins 5 of 7 benchmark suites at SF1 vs Trino 465.** DataFusion 53. Star-schema join reorder. Broadcast threshold 64MB. Dynamic filter type coercion. GRANT/REVOKE SQL via platform API. Open-source release prep complete. **222/222 queries pass at SF1** (TPC-H 22, TPC-DS 99, SSB 13, TPC-C 17, TPC-E 18, TPC-BB 10, ClickBench 43). Full suite runs in 154.8s. TPC-E SF10: 18/18 pass (`trade_result_update_holding` 10.9s). TPC-E SF100: 17/18 pass, `trade_result_update_holding` times out at the 120s harness cap under CoW (MoR path now available as an opt-in for this case). **TPC-H SF1000 data generation in 6:23 on 32 cores** (lineitem 4:43, 29x speedup vs serial, 91% scaling efficiency, 2.2 GiB peak RSS for 6B rows in flight via the `bench-generate-parallel-streaming` change). Streaming Flight SQL results path. 8 MiB tokio worker stack. `sqe-trino-functions` split out of coordinator for faster incremental builds. 1,334+ unit tests, 60/60 integration tests, 13/13 V3 e2e tests. 43/43 security audit findings resolved. Known limitation: q72 (15.5s vs Trino 1.4s, upstream DF#3843). **Next: pluggable catalogs (HMS/Glue real implementations), worker-path bloom filters, OSS release.**

> **Monitoring:** OPA SPI refactor in Polaris (PR #3999, still draft) will affect Phase 5 OPA integration when it lands — do not implement OPA against Polaris until this stabilises. Remote S3 signing (Iceberg 1.12, not yet released) will affect the pluggable-catalogs design.

---

## ~~Step 1: Security and Functional Audit~~ ✅

See [AUDIT.md](AUDIT.md) for the full report. Completed 2026-04-08.

~~Before starting new feature development, audit the current codebase against the design intent. Do this as a structured review, not just a code read.~~

### 1a. Security Audit

| Area | What to Check | Files |
|---|---|---|
| Auth passthrough | Bearer token is never logged, never stored in memory longer than the session | `sqe-auth/`, `sqe-coordinator/src/session.rs` |
| Error messages | No stack traces, internal paths, or policy details leak to client | `sqe-coordinator/src/error.rs` |
| Query cancellation | In-flight queries are cleanly cancelled when client disconnects | `sqe-coordinator/src/` |
| Token validation | JWT expiry enforced; replay attacks mitigated | `sqe-auth/src/` |
| TLS | Flight SQL listener enforces TLS in non-dev mode | `sqe-coordinator/src/server.rs` |
| Rate limiting | Missing — not yet implemented (see Step 2) | — |
| Audit log | Missing — not yet implemented (see Step 2) | — |
| Config secrets | `sqe.toml.example` does not contain real credentials | `sqe.toml.example` |

### 1b. Functional Audit

| Area | What to Check | Files |
|---|---|---|
| EXPLAIN FULL | Metrics (elapsed_ms, output_rows) match actual query execution | `sqe-coordinator/src/explain.rs` |
| `fmt_val` | All Arrow data types render correctly (Utf8View, UInt32/64, Float32, decimals, dates) | `sqe-cli/src/fmt_val.rs` |
| Iceberg scan | Partition pruning is applied; snapshot time-travel works | `sqe-catalog/src/` |
| Policy rewriter | Column masks block predicate pushdown; row filters are transparent | `sqe-policy/src/` |
| Integration tests | All tests in `tests/` pass against a live Iceberg/MinIO stack | `scripts/integration-test.sh` |
| Docker build | `docker build` completes cleanly using pre-compiled binaries | `Dockerfile` |

### 1c. Audit Commands

```bash
# Static analysis
cargo clippy --all-targets --all-features -- -D warnings

# Tests (unit)
cargo test --all

# Integration tests (requires running stack)
./scripts/integration-test.sh

# Security advisory scan
cargo audit

# Check for unused dependencies
cargo +nightly udeps --all-targets
```

---

## ~~Step 2: Complete Core Engine Spec~~ ✅ (99/103)

**Spec:** `openspec/changes/sqe-core-engine/tasks.md`

Step 2 is effectively complete. All implementation and test tasks are done. Only 4 tasks remain, all blocked on upstream:

| Task | Ref | Status |
|---|---|---|
| ~~`DELETE FROM` — CoW rewrite_files~~ | 8.4 | ✅ Done — via RisingWave fork rewrite_files() |
| ~~`MERGE INTO` — CoW full-outer-join rewrite~~ | 8.5 | ✅ Done — via RisingWave fork rewrite_files() |
| ~~Integration test: MERGE INTO~~ | 8.13 | ✅ Done |
| ~~Integration test: DELETE FROM~~ | 8.14 | ✅ Done |

All 103/103 tasks complete. DELETE, UPDATE, and MERGE INTO use Copy-on-Write via the RisingWave iceberg-rust fork's `rewrite_files()` transaction API.

**Completed since last update (2026-03-22):** distributed execution (7.6, 7.10, 7.11, 9.5, 9.6, 9.7), predicate pushdown (6.3), Trino pagination + headers (11.3, 11.7), worker metrics (12.3), OTel trace propagation (12.6), sqe-auth unit tests (2.5), Keycloak realm registration (13.3), all integration tests (2.6, 3.10, 3.11, 7.12, 7.13, 8.11, 8.12, 8.15, 8.16, 9.8, 10.5, 11.10, 13.4, 13.5), e2e test script.

---

## ~~Step 3: OSS Security Hardening~~ ✅ (51/51)

**Spec:** `openspec/changes/oss-security-hardening/`

Step 3 is complete. All vendor-specific identifiers renamed and production security controls implemented.

**Completed (2026-03-22):** Keycloak → OIDC rename (`oidc_password.rs` + deprecated re-export), MinIO → generic S3 language, config validation (fail-fast on missing fields + port conflicts), TLS support (`[coordinator.tls]` with optional mTLS via `ca_file`), rate limiting (per-user + global via `governor`), query timeouts (per-role overrides), session lifecycle (idle + absolute timeouts with background sweeper), query cancellation (`CancellationToken` registry + Flight cancel handler), audit log enhancements (session_id, query_hash, client_ip), error sanitisation (`client_message()` + debug mode toggle), health endpoints (already existed).

---

## ~~Step 3b: Benchmark Suite~~ ✅

**Design:** `docs/superpowers/specs/2026-03-24-sqe-bench-design.md`

Benchmark suite is complete. `sqe-bench` CLI provides generate/load/test pipeline for 6 benchmark suites. The `read_parquet()` TVF enables zero-copy Parquet → Iceberg loading.

**Completed (2026-03-24):**
- `read_parquet()` TVF — local filesystem and S3 with inline credentials; glob patterns; registered on every `SessionContext`
- `sqe-bench generate` — Parquet data generation for TPC-H (22q), TPC-DS (99q), SSB (13q), TPC-C (8q), TPC-E (11q), TPC-BB (10q)
- `sqe-bench load` — CTAS-based table loading via `read_parquet()`, namespace creation, `--clean` flag
- `sqe-bench test` — query runner with correctness validation (PASS/FAIL/DIFF/SKIP/ERROR), Flight SQL + Trino HTTP clients, JSON reports
- Scripts: `benchmark-generate-all.sh`, `benchmark-load.sh`, `benchmark-test.sh`
- Query files and expected results for all benchmarks

**First results (TPC-H SF1, Flight SQL):** 20/22 PASS, 1 DIFF (decimal precision), 1 SKIP (unsupported feature).

---

## Step 4: Pluggable Auth

**Plan:** `docs/superpowers/plans/2026-03-19-pluggable-auth.md`
**Spec:** `openspec/changes/pluggable-auth/`

Replace the single Keycloak ROPC provider with a composable `AuthProvider` trait chain.

| Provider | Credential Detection | Use Case |
|---|---|---|
| `OidcPasswordProvider` | username + password, no `eyJ` prefix | JDBC/ODBC with OIDC password grant |
| `BearerTokenProvider` | password field starts with `eyJ` | pre-authenticated clients, CI/CD |
| `ApiKeyProvider` | password matches `sqe_` prefix or configured prefix | scripting, service accounts |
| `AnonymousProvider` | no credentials | dev/read-only public data |
| `MtlsProvider` | mTLS client certificate | internal service-to-service |

Config: `[[auth.providers]]` array; first-match chain; role mappings via `[auth.role_mappings]`.

---

## Step 5: Pluggable Catalogs

**Plan:** `docs/superpowers/plans/2026-03-19-pluggable-catalogs.md`
**Spec:** `openspec/changes/pluggable-catalogs/`

Replace the hard-coded Polaris REST catalog with a `CatalogBackend` trait.

| Backend | Notes |
|---|---|
| `IcebergRestBackend` | current default; Polaris, Lakeformation REST, any Iceberg REST |
| `AwsGlueBackend` | AWS SDK; IAM auth; read + write |
| `NessieBackend` | Project Nessie REST API; branch/tag awareness |
| `HiveMetastoreBackend` | Thrift HMS; for legacy Hive warehouse migration |
| `StorageOnlyBackend` | Scan base path for `metadata/v*.metadata.json`; no catalog server required |

Multi-cloud storage via `object_store`: S3 (+ endpoint override for R2/Ceph/Garage), Azure ADLS Gen2/Blob, GCS, local filesystem.

Delta Lake support (`delta-rs`) as Cargo feature flag `delta` — Unity Catalog serves both Iceberg and Delta tables.

---

## Step 6: Semantic AI Layer

**Plan:** `docs/superpowers/plans/2026-03-19-semantic-ai-layer.md`
**Spec:** `openspec/changes/semantic-ai-layer/`

Four sub-systems that make SQE agent-native and semantically aware.

### 6a. RDF Triple Store on Iceberg (`sqe-semantic`)
- Convention: `rdf.triples (subject, predicate, object, graph_name)` Iceberg table, partitioned by predicate
- SPARQL 1.1 SELECT compiled to DataFusion `LogicalPlan` via `spargebra` + `rdf-fusion`
- SPARQL auto-detected when input starts with `SELECT ?`, `CONSTRUCT`, `ASK`, `DESCRIBE`
- Ontology time-travel via Iceberg snapshot + `FOR SYSTEM_TIME AS OF`

### 6b. Property Graph / ISO GQL (`sqe-semantic`)
- Convention: `graph.nodes (id, labels[], properties json)` + `graph.edges (src_id, dst_id, label, properties json)`
- `graphlite` embedded ISO GQL engine (ISO 39075:2024)
- Small graphs (<threshold): load into graphlite in-memory, execute GQL, return Arrow
- Large graphs: compile MATCH patterns to DataFusion recursive CTEs

### 6c. Vector Search (`sqe-vector`)
- `lance` + `lance-datafusion` for Arrow-native vector format on object storage
- `LanceScanExec`: DataFusion physical plan node reading Lance datasets
- `vec_distance(col, query_vec, metric)` UDF (cosine, l2, dot)
- `embed(text)` async UDF: HTTP POST to configurable embedding endpoint; SHA256 cache

### 6d. AI Agent Interfaces
- **CLI-first** (primary): `sqe query`, `sqe schema search/describe/relationships/ontology`, `sqe explore`; `--output json|arrow|csv|table`; `--describe` flag for self-documentation; piped output auto-selects JSON
- **REST/OpenAPI** (secondary): axum HTTP server; `utoipa` generates OpenAPI 3.1; `/api/v1/openapi.json` LLM-readable
- **MCP** (tertiary): thin stdio wrapper over REST API; tools generated from OpenAPI spec, not hand-coded
- **TypeScript `@sqe/client`** (npm): `RestTransport` (browser) + `FlightTransport` (Node.js, `@grpc/grpc-js`); auto-selects by env

---

## Implementation Order Rationale

```
Step 1: audit               ✅ DONE (AUDIT.md: 1,218 tests, rsa removed, 5 config findings, no critical vulns)
Step 1+: OSS release        ✅ DONE (LICENSE, CONTRIBUTING, deny.toml, cliff.toml, CI pipelines, retro-tags, CHANGELOG, v0.15.0)
Step 2: core engine gaps    ✅ DONE (103/103 — DELETE, UPDATE, MERGE via CoW rewrite_files)
Step 3: security hardening  ✅ DONE (51/51 — TLS, rate limiting, timeouts, cancellation, audit, error sanitisation)
Step 3b: benchmark suite    ✅ DONE (sqe-bench: generate/load/test, 6 benchmarks, read_parquet() TVF, CI scripts)
Step 3c: hardening pass     ✅ DONE (type formatting, Flight SQL DoPut + metadata, clippy, decimal DIFF, token fingerprint)
Step 3d: query history+cache ✅ DONE (system.runtime.queries, in-memory history store, query result cache, config sections)
Step 3e: distributed wiring ✅ DONE (try_distribute in execute_query, fragment tracking, system.runtime.tasks shows workers)
Step 4: pluggable auth      ✅ DONE (11 providers: OIDC, bearer, API key, anonymous, mTLS, token exchange, AWS IAM, device code, auth code, OIDC discovery, chain)
Step 4b: streaming exec A   ✅ DONE (spill-to-disk, late materialization, scan planning, S3 I/O, SortMergeJoin — 21/22 TPC-H SF1 on 512MB)
Step 4c: streaming exec B   ✅ DONE (shuffle, distributed sort/join/aggregate, multi-endpoint Flight SQL, Trino function compat)
Step 4d: adaptive sort+metrics ✅ DONE (adaptive sort stripping, S3/auth/write Prometheus metrics)
Step 7.1: dbt-sqe adapter   ✅ DONE (ADBC Flight SQL, table/view/incremental/seed materializations)
Step 7.3: ALTER TABLE schema ✅ DONE (ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL, type widening)
Step 8: Trino parity        ✅ DONE (compatibility matrix, sqe-bench compare, client testing scaffold, operational comparison)
Step 8b: Trino UDF blitz    ✅ DONE (70+ UDFs + engine features — ~95% SQL coverage)
Step 8c: Iceberg time travel ✅ DONE (FOR SYSTEM_TIME AS OF + 6 metadata TVFs + COMMENT ON + SHOW STATS)
Step 8d: MoR DELETE path    ✅ DONE (PositionDeleteFileWriter + FastAppendAction, alongside existing CoW)
Step 9: streaming + perf    ✅ DONE (streaming CTAS/INSERT, IN-subquery rewrite, safe sort order, --compare-trino benchmarks)
Step 9b: 5-layer caching    ✅ DONE (RestCatalog cache, table metadata cache, manifest cache, SessionContext cache, OAuth token cache — warm query <1ms)
Step 9c: DECIMAL + correctness ✅ DONE (parse_float_as_decimal=true, COUNT(*) crash fix, cache invalidation after DDL/DML, Int64 date returns)
Step 9d: safe defaults       ✅ DONE (sort_mode=partition_only, FairSpillPool fallback, spill_to_disk=true, trust_sort_order=false)
Step 9e: Trino comparison    ✅ DONE (SQE 2.5-8.8x faster than Trino 465 across all 7 suites, 221/222 match)
Step 9f: scale hardening     ✅ DONE (streaming result path, tuple-IN view-lifted semi-join, 8 MiB worker stack, pre-flight port check -- SF1 222/222 pass; TPC-E trade_result streams 21M rows in 8.7s without OOM; CoW DML with IN (subquery) scales to TPC-E SF10 34K tuples without stack overflow via `lift_in_subqueries`)
Step 5: pluggable catalogs  ✅ DONE for catalog backends (Phase O+P, MR !113): HMS, Nessie, JDBC postgres, AWS Glue (SDK path + federated REST), AWS S3 Tables (REST + SigV4), Hadoop storage-only -- all live-tested. Engine session-manager wiring (Section 11 of pluggable-catalogs/tasks.md) deferred to a follow-up phase; Delta Lake + Azure + GCS deferred to a separate multi-cloud-storage change.
Step 5c: dynamic Polaris catalog discovery ✅ DONE: `[query] catalog_discovery = "polaris-auto"` lazily resolves an undeclared Polaris warehouse at query time using the caller's bearer (same SqeCatalogProvider path; per-user session scoping; unauthorized/nonexistent -> "unknown catalog", no leak). Default stays `static`. Live-tested (lazy hit / miss / static / in-session reuse). Also fixed a latent bug: REST_CATALOG_CACHE now keys on warehouse (same-URL warehouses no longer collide). Spec/plan in docs/superpowers.
Step 9g: SF100 CoW DML scaling (`openspec/changes/cow-dml-parallel-streaming`) -- parallelise per-file rewrite + stream writes + drop double-WHERE; unblocks SF100 `trade_result_update_holding` (currently 120s timeout) and other super-linear UPDATEs (settlement 24x, executor 16x, status 13x for 10x data) <- NEXT
Step 6: semantic layer      (new crates; fully additive; no existing code broken)
```

Step 9g (cow-dml-parallel-streaming) is the immediate SF100 unblock. Step 5 (pluggable catalogs) follows. Step 6 is independent and fully additive.

> **Upstream watch list (refreshed 2026-04-29):**
>
> _Resolved since the last refresh:_
>
> - **★ risingwavelabs/iceberg-rust caught up to DataFusion 53.** Commit `fb290e4c9` on the fork's main branch (2026-04-15) merges PR #148, which lands DF 53 + Arrow 58. SQE has been carrying a downstream rebase since Phase F; we can now align with the upstream fork on its next vendor refresh.
>
> _Blocking matrix v3 cells, still open:_
>
> - **apache/iceberg-rust#2188 (Variant)** — open, in active review, merge conflicts. Likely lands within weeks. Unblocks `variant-type:v3`.
> - **apache/arrow-rs#9790 (BorrowedShreddingState refactor)** — opened 2026-04-22, no traction yet. Parent shredded variant work has effectively landed in arrow-rs; this is cleanup. Worth re-checking what arrow-rs version SQE pins. `shredded-variant:v3` may already be partly reachable.
> - **apache/datafusion#12644 (User-defined types)** — open since 2024-09-27. Long-running design discussion (geoarrow extension types). No merge in sight; `geometry-type:v3` will not unblock soon via DataFusion proper. Practical path is to ride on arrow-rs extension-type metadata above DataFusion.
> - **Apache Iceberg V3 Java spec activity** — heavy traffic on variant (#15385 predicate pushdown, #16133 row-group skip, #14297 shredded write) and lineage (#15776 ORC `_row_id`); geometry stalled (#12347 since 2025-09). V3 is still landing pieces; the `multi-arg-transforms:v3`, `vector-type:v3`, `lineage:v3` cells track that progress.
>
> _SQE-filed, no upstream traction yet:_
>
> - **apache/iceberg-rust#2376 (DynamicPredicate API)** — SQE filed this; latest comment 2026-04-28 sharpens the cache-layer API ask (`is_sealed()` / `generation()`). MR !112 already shipped Path B-2 downstream so SQE is unblocked; the issue tracks getting the cache helper accepted upstream.
>
> _Affecting older watchlist items:_
>
> - **apache/datafusion#21570 (ROLLUP empty GROUP BY)** — open, an assignee took it on 2026-04-12 and committed to a PR. Should land in 1-2 release cycles. Still causes 6 TPC-DS DIFF results.
> - **apache/datafusion#20746 (MERGE INTO)** — open umbrella issue, no in-flight PR. Don't expect MERGE in DataFusion soon; SQE keeps its CoW MERGE path.
> - **DataFusion `IN (subquery)` on MemTable-referenced columns** — no specific upstream issue; closest open work (#14554, #15046) is stale. SQE's `lift_in_subqueries` workaround stays.
>
> _Pre-existing items (no change):_
>
> - **iceberg-rust MoR (Epic #2186, Q3 2026)** — could replace CoW DELETE/MERGE with a more efficient position-delete approach; matters for the longer-term follow-up to `cow-dml-parallel-streaming`.
> - **Polaris OPA SPI refactor (PR #3999)** — must stabilise before Phase 5 OPA integration.
> - **Remote S3 signing (Iceberg 1.12)** — will require revisiting credential vending in pluggable-catalogs once it ships.
