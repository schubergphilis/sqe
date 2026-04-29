# SQE — Next Steps

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
Step 9g: SF100 CoW DML scaling (`openspec/changes/cow-dml-parallel-streaming`) -- parallelise per-file rewrite + stream writes + drop double-WHERE; unblocks SF100 `trade_result_update_holding` (currently 120s timeout) and other super-linear UPDATEs (settlement 24x, executor 16x, status 13x for 10x data) <- NEXT
Step 6: semantic layer      (new crates; fully additive; no existing code broken)
```

Step 9g (cow-dml-parallel-streaming) is the immediate SF100 unblock. Step 5 (pluggable catalogs) follows. Step 6 is independent and fully additive.

> **Upstream watch list:** iceberg-rust MoR (Epic #2186, Q3 2026) could replace CoW DELETE/MERGE with a more efficient position-delete approach; this matters for the longer-term follow-up to `cow-dml-parallel-streaming` (default UPDATE mode -> MoR when the change fraction is small). Polaris OPA SPI refactor (PR #3999) must stabilise before Phase 5 OPA integration. Remote S3 signing (Iceberg 1.12) will require revisiting pluggable-catalogs credential vending design. DataFusion `IN (subquery)` still not supported in physical plan for MemTable-referenced columns: SQE lifts those to a scratch-MemTable + LEFT JOIN in `lift_in_subqueries` before planning (previously blocked 5 TPC-E DML queries; the old literal-inlining rewriter crashed at 34K tuples in SF10). The LEFT JOIN + COALESCE pattern prevents DataFusion's `EliminateOuterJoin` from producing a LeftSemi join; a follow-up to `cow-dml-parallel-streaming` will route `IN` directly through `DecorrelatePredicateSubquery` to emit native LeftSemi. Track upstream DataFusion for native `IN (subquery)` on MemTables. DataFusion ROLLUP returns 0 rows with empty GROUP BY input (apache/datafusion#21570): causes 6 TPC-DS DIFF results. DataFusion MERGE INTO (apache/datafusion#20746) is in progress; would enable a single-pass plan over the DELETE+UPDATE+INSERT combo that Spark already supports.
