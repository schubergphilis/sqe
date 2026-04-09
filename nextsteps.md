# SQE — Next Steps

> Status as of 2026-04-08. **Steps 1–4d + 7.1 + 7.3 + OSS Release done.** Step 1 (Security & Functional Audit) completed — see [AUDIT.md](AUDIT.md). OSS release readiness complete: Apache 2.0 [LICENSE](LICENSE), [CONTRIBUTING.md](CONTRIBUTING.md), cargo-deny ([deny.toml](deny.toml)), git-cliff ([cliff.toml](cliff.toml)), CI pipelines (clippy + audit + deny + release), retro-tagging (44 MRs → v0.1.0–v0.28.0), [CHANGELOG.md](CHANGELOG.md), crate versions bumped to 0.15.0. 1,218 tests passing, all advisory checks clean. **Next: Step 5 (pluggable catalogs).** ← NEXT

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
Step 8b: Trino UDF blitz    ✅ DONE (50+ UDFs: JSON/URL/encoding/regex/string/math/date — overall SQL coverage ~88%)
Step 5: pluggable catalogs  (AWS Glue, Nessie, Hive Metastore, storage-only, Delta Lake) ← NEXT
Step 6: semantic layer      (new crates; fully additive; no existing code broken)
```

Step 5 (pluggable catalogs) is next. Step 6 is independent and fully additive.

> **Upstream watch list:** iceberg-rust MoR (Epic #2186, Q3 2026) could replace CoW DELETE/MERGE with more efficient position-delete approach in the future; Polaris OPA SPI refactor (PR #3999) must stabilise before Phase 5 OPA integration; remote S3 signing (Iceberg 1.12) will require revisiting pluggable-catalogs credential vending design; DataFusion `IN (subquery)` not supported in physical plan for MemTable-referenced columns — blocks 5 TPC-E DML benchmark queries (market_feed_update, trade_result_update_holding, trade_result_update_status, trade_update_executor, trade_update_settlement). Workaround: rewrite as `EXISTS` or `JOIN`. Track upstream DataFusion for fix.
