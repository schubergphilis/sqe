# SQE Codebase Analysis

Generated: 2026-06-26

> **Status note.** Point-in-time snapshot, generated mid-session while
> `feat/trino-bi-wire-compat` was in progress, so a few findings are already
> resolved:
> - Issue #10 ("`todo!()` in Trino compat layer") captured TDD red-phase stubs in
>   `prepared.rs`; prepared statements (EXECUTE / EXECUTE IMMEDIATE over the Trino
>   HTTP path) are now implemented.
>
> Findings are tracked as GitLab issues under the `codebase-analysis` label.

---

## Issues

### Critical

1. **`panic!` in policy plan rewriting on every query path**
   `crates/sqe-policy/src/plan_rewriter.rs:1039-1068` -- four `panic!` arms for unexpected NULL literal types fire during plan rewriting for every query. A column with an unhandled NULL literal type crashes the coordinator mid-flight.

2. **`panic!` in policy UDFs**
   `crates/sqe-policy/src/mask_udf.rs:320-346`, `sha256_udf.rs:221-336`, `session_udf.rs:407-501` -- 15+ `panic!` calls in masking/session UDF evaluation. These fire during policy enforcement on query results.

3. **`unwrap()` in policy expression serialization**
   `crates/sqe-policy/src/policy_expr.rs:139-220` -- ~20 chained `expr_to_sql(&e).unwrap()` calls in the policy SQL parsing path. Every query triggering policy evaluation can panic if expression serialization fails.

4. **Error type source chain loss**
   `crates/sqe-core/src/error.rs` -- 9 of 10 `SqeError` variants use `String` fields instead of proper `#[from]`/`source()` wrapping. Only `Internal(anyhow::Error)` preserves the original error. All `Catalog`, `Auth`, `Execution`, `Config`, and `IcebergCommitConflict` errors discard their source chain, making debugging significantly harder.

5. **Parquet writer close errors silently swallowed**
   `crates/sqe-coordinator/src/writer.rs:555,602` -- `let _ = writer.close().await` discards close errors during empty-write paths. This can mask data corruption.

6. **JWKS HTTP response errors silently swallowed**
   `crates/sqe-auth/src/bearer_token.rs:923-924,962-963` -- `let _ = stream.write_all(..).await` / `let _ = stream.flush().await` discards JWKS endpoint send failures.

7. **Security audit: session token file persistence in plaintext**
   `AUDIT.md` finding -- when `session.persistence = "file"`, access tokens are serialized in plaintext JSON to `/tmp/sqe-sessions.json`. No encryption, no file permission controls.

8. **Security audit: audience validation silently disabled by default**
   `AUDIT.md` / `crates/sqe-auth/src/bearer_token.rs:254` -- when `audience` config field is absent (the default), `validation.validate_aud = false` is set silently. No warning emitted.

### High

9. **`panic!` in write handler DML path**
   `crates/sqe-coordinator/src/write_handler.rs:6447-7435` -- 7 `panic!` calls for unexpected AST shapes (e.g., `panic!("expected CreateTable")`, `panic!("expected UPDATE")`). These fire on DML write paths and crash the coordinator.

10. **`todo!()` in Trino compat layer**
    `crates/sqe-trino-compat/src/prepared.rs:23,37` -- `PREPARE` and `EXECUTE` statement handling is completely stubbed with `todo!()`. Any Trino client that sends prepared statements will crash the server.

11. **`downcast_ref::<T>().unwrap()` in Flight SQL service**
    `crates/sqe-coordinator/src/flight_sql.rs` -- 15+ `downcast_ref::<T>().unwrap()` calls. Schema changes or type mismatches between DataFusion plan serialization and deserialization cause immediate panics.

12. **Mutex poison risk in catalog ops**
    `crates/sqe-coordinator/src/catalog_ops.rs:1563-1565` -- `counter.lock().unwrap()` on `std::sync::Mutex`. If any thread panics while holding this lock, all subsequent access panics too.

13. **Dual legacy/modern auth configuration**
    `crates/sqe-core/src/config.rs` -- `AuthConfig` carries both legacy flat fields (`keycloak_url`, `realm`, `client_id`) AND the modern `[[auth.providers]]` array. Two code paths exist for auth configuration, creating ambiguity and maintenance burden.

14. **Serde deserialization `unwrap()` in production code**
    `crates/sqe-coordinator/src/credential_refresh.rs:561` -- `serde_json::to_vec(&creds).unwrap()` can panic if credential structs ever gain non-serializable fields.

### Medium

15. **Three files at critical size**
    - `write_handler.rs`: 7,505 lines (larger than many entire crates)
    - `config.rs`: 6,030 lines
    - `query_handler.rs`: 5,881 lines
    Deep nesting, hard to review, hard to test in isolation.

16. **Config struct bloat**
    26 structs, ~200 fields across the config hierarchy. `QueryConfig` alone has 23 fields. Many fields coexist for mutually exclusive backends.

17. **6 crates with zero integration tests**
    `sqe-auth`, `sqe-core`, `sqe-metrics`, `sqe-planner`, `sqe-trino-compat`, `sqe-worker` -- all have unit tests in source files but no `tests/` directory or external integration tests.

18. **47 `#[allow(dead_code)]` annotations**
    Heavily concentrated in `sqe-auth` (legacy config fields), `sqe-policy` (Ranger response structs), and `sqe-catalog` (incremental provider, hadoop backend). Signals incomplete cleanup after refactors.

19. **No `/// # Safety` or `/// # Panics` doc annotations**
    Zero annotations across ~300 source files despite many functions that have implicit preconditions (especially the plan rewriter and UDF executors).

20. **`sqe-metrics` crate has 6% module-level doc comment coverage**
    Worst in the codebase. The `audit/` submodule has no `//!` comments despite being a security-critical subsystem.

21. **4 crates missing from README structure table**
    `sqe-trino-functions`, `sqe-quack-wire`, `sqe-quack-server`, `sqe-quack-client` exist in `Cargo.toml` but are absent from the README.

22. **Catch-all match arms without documentation**
    11+ `_ => {}` arms in production code, several with no comment explaining why the unmatched case is safe to ignore (e.g., `write_handler.rs:4745`, `query_handler.rs:1383`).

23. **Audience validation warning not emitted**
    When `audience` is missing from config, the system silently disables audience validation without any log warning.

24. **`ssl_verification = false` active in example config**
    `sqe.toml.example:37` -- the disable-ssl-verification setting is active (not commented out), making it easy for users to copy-paste an insecure configuration.

25. **No validation of TLS cert file existence at startup**
    SqeConfig validation checks URL formats and port conflicts but does not verify that TLS certificate paths point to existing files.

---

## Improvements

### Architecture & Maintainability

1. **Split `write_handler.rs` and `query_handler.rs`**
   Each exceeds 5,000 lines. Extract decorrelation logic, merge/delete DML paths, and query plan dispatch into separate modules.

2. **Split `config.rs`**
   6,030 lines with 26 structs. Move to a directory module (`config/`) with one file per config concern (auth, catalog, query, security, etc.) to improve navigation and reviewability.

3. **Replace panics with `SqeError` returns in policy code**
   The plan rewriter, mask UDFs, and session UDFs should return `Result<_, SqeError>` instead of panicking. The coordinator should log the error and abort the query gracefully.

4. **Replace `unwrap()` with proper error handling in policy_expr.rs**
   The `expr_to_sql(&e).unwrap()` chain should propagate errors instead of panicking. Consider caching the serialized form if the conversion is expensive.

5. **Replace `String`-based error variants with source-wrapped errors**
   Migrate `SqeError` variants from `Catalog(String)` to `Catalog { source: Box<dyn std::error::Error + Send + Sync>, message: String }` or use `#[from]` derives.

6. **Add `/// # Panics` and `/// # Errors` annotations**
   Particularly on public functions in the plan rewriter, UDF executors, Flight SQL service, and write handler where the panic preconditions are non-obvious.

### Security

7. **Encrypt session persistence file**
   Use AES-GCM with a key derived from a config-provided secret, or warn loudly when plaintext persistence is used.

8. **Enable audience validation by default**
   Require explicit opt-out with a log warning, rather than silent omission.

9. **Add startup validation for TLS certificate paths**
   Check that `tls.cert_path` and `tls.key_path` point to readable files during `config.validate()`.

10. **Remove real-looking default credentials from example config**
    Replace `s3admin` / `s3admin` with placeholder values and add a clear comment.

11. **Comment out `ssl_verification = false` in example config**
    Make security-hardened defaults the path of least resistance.

12. **Log when `audience` validation is disabled**
    Emit a `warn!` log at startup when `validate_aud = false`.

### Testing

13. **Add integration tests for `sqe-auth`**
    Wiremock-based tests for `AuthChain` behavior with different provider combinations, token refresh, and error scenarios.

14. **Add integration tests for `sqe-worker`**
    Tests for fragment execution, shuffle data exchange, heartbeat, and credential forwarding.

15. **Add integration tests for `sqe-trino-compat`**
    Test the Trino HTTP server against a real client or with recorded HTTP requests.

16. **Replace `todo!()` in Trino prepared statements with graceful error**
    Return a structured error to the client saying prepared statements are not yet supported.

17. **Add `cfg(test)` mod to test-lib.rs files for core error types**
    Property-based tests for `SqeError` serialization/deserialization round-trips and error code mappings.

### Code Quality

18. **Audit and remove dead code**
    Clean up the 47 `#[allow(dead_code)]` annotations, especially the legacy auth config fields.

19. **Replace `let _ =` silent swallows with explicit error logging**
    At minimum, add `warn!` or `debug!` logs when critical operations (writer close, JWKS send, table deregistration) fail.

20. **Reduce `Arc<RwLock<Mutex<Nested>>>` patterns**
    The `catalog_ops.rs` table lock map uses `StdArc<std::sync::Mutex<HashMap<String, StdArc<tokio::sync::Mutex<()>>>>>` -- a doubly-wrapped mutex. Simplify to a single lock type.

21. **Add `//!` module doc comments to all lib.rs files and key modules**
    Especially in `sqe-metrics` (6% coverage), `sqe-trino-compat` (33%), `sqe-core` (44%), and `sqe-worker` (55%).

22. **Update README with all workspace crates**
    Add `sqe-trino-functions`, `sqe-quack-wire`, `sqe-quack-server`, `sqe-quack-client` to the crate table.

### Performance / Reliability

23. **Warn when coordinator starts without TLS**
    Currently a single INFO log line. Should be a WARN with the consequence stated.

24. **Add graceful degradation for policy backend failures**
    If OPA/Ranger backend is unreachable, consider using a cached policy or failing closed with a clear error message rather than letting `unwrap()` produce a panic.

---

## Good Points

### Architecture & Design

1. **Clean multi-crate separation** -- 19 focused crates with clear responsibilities. The dependency graph flows mostly one way (core -> auth/catalog/sql -> policy/planner -> coordinator/worker).

2. **No `unsafe` in production code** -- zero `unsafe` blocks outside vendor/ and test code. The audit confirms no `unsafe fn` or `unsafe {}` in any crate's `src/`.

3. **Auth chain is elegantly pluggable** -- 10 providers implemented as the `AuthProvider` trait with a composable `AuthChain`. Adding a new auth method means writing one struct + one `factory.rs` entry.

4. **Policy enforcement at the plan level** -- injecting row filters and column masks before DataFusion optimization is the right approach. It prevents information leakage downstream and allows the optimizer to push predicates through row filters naturally.

5. **Vendored iceberg-rust** -- the fork (RisingWave + 5 cherry-picks) gives control over the critical Iceberg integration path without waiting for upstream. The patches (`RewriteFilesAction`, `DynamicPredicate`, parallel manifest loading) are well-motivated by the architecture doc.

6. **OpenSpec process** -- 11 well-structured change proposals with `proposal.md`, `design.md`, `tasks.md`, and `specs/`. This provides clear audit trail and task breakdown for complex features.

7. **Config validation at startup** -- `SqeConfig::validate()` checks required fields, port conflicts, URL validity, byte-size formatting, and memory budget invariants before the server starts accepting connections.

8. **Workspace-level `deny.toml` and `audit` in CI** -- security advisory scanning is gated in the pipeline. Cargo deny checks advisories, licenses, and sources.

9. **Benchmark results committed to repo** -- 7 benchmark suites (222 queries) with JSON results in version control. Regressions can be caught before merge.

10. **Binary size control** -- the slim Dockerfile compiles with only `rest` catalog backend, keeping image size down. The `full-backends` feature enables everything for deployments that need it.

### Testing

11. **3,173 test functions** across the codebase (excluding vendor). Strong unit test presence in most modules.

12. **Snapshot testing for OpenLineage** -- `sqe-lineage` uses `insta`/snapshot testing for event serialization, catching unintended changes in emitted lineage payloads.

13. **118 integration tests behind `#[ignore]`** -- Docker-backed tests for Polaris, Ranger, HMS, Glue, S3 Tables, Nessie, and distributed scenarios. CI runs these scheduled+manual.

14. **Wiremock-based policy backend tests** -- `sqe-policy/tests/` has OPA and Ranger tests using HTTP mocks, enabling deterministic policy evaluation tests without real backends.

15. **Property-based tests for SQL parser** -- `sqe-sql/tests/parser_proptest.rs` uses proptest for fuzz-testing the extended SQL parser.

### Operations

16. **Comprehensive metrics** -- 30+ metric families (query count, duration, rows returned, active sessions, healthy workers, cache metrics, memory pressure, spill metrics, shuffle metrics, late materialization, S3 I/O, auth, adaptive sort, catalog roundtrip, policy backend, audit export, dashboard auth).

17. **OpenLineage support** -- column-level lineage emitted via configurable sinks (HTTP, file, spool). This is a differentiator vs. other DataFusion-based engines.

18. **Docker multi-stage builds with cargo-chef and sccache** -- fast incremental builds in CI.

19. **Docker Compose scenarios for 19 configurations** -- including Polaris, Nessie, Ranger, Glue, S3 Tables, Unity, observability, distributed, benchmark, and Spark parity.

20. **Health check endpoint** -- `/healthz` exposed on the coordintor.

### Code Quality

21. **Vendored dependency with documented patches** -- the iceberg-rust vendor is cleanly separated in `vendor/` with a pinned commit and change list.

22. **Consistent clippy enforcement in CI** -- `-D warnings` on clippy across all targets and features.

23. **Pipeline parallelism through compilation** -- `.cargo/config.toml` uses pipelined compilation and `jobs = 8`.

24. **Precise error code classification** -- 28 `SqeErrorCode` variants with Trino-compatible numeric mappings and a `is_user_error()` classification method.

25. **Audit logging subsystem** -- structured `AuditEvent` type with proper `Actor`, `Outcome`, and `AuditKind` enums. Batched export with configurable sinks.

---

## Future Ideas

### Strategic

1. **Native DuckDB integration via Quack protocol**
   The Quack server/client crates are in early stages. Once mature, this enables DuckDB clients to query Iceberg through SQE with zero-config. Potential for embedded mode where SQE acts as a DuckDB extension.

2. **Multi-engine federated query**
   Use the Quack client as a table provider to push down queries to DuckDB while SQE handles Iceberg. Let each engine do what it does best.

3. **Iceberg REST Catalog as a service**
   Package the coordinator as a standalone Polaris-compatible Iceberg REST catalog with SQE's auth and policy layers. This would let non-SQE engines (Spark, Flink, Trino) benefit from SQE's policy engine.

4. **Column-level lineage export to data catalogs**
   Push OpenLineage events to a data catalog (DataHub, Atlan, Marquez) via the existing sink framework. Close the governance loop: policies defined in the catalog are enforced by SQE, and lineage flows back.

5. **AI-assisted policy authoring**
   The `semantic-ai-layer` openspec change suggests an LLM interface for translating natural-language access rules into Rego/Cedar policies. This is a strong differentiator for enterprise adoption.

### Technical

6. **Config migration to structured, versioned config files**
   Replace the monolithic `SqeConfig` with versioned config schemas (e.g., `config_version = 2`). This enables safe migrations between config formats over releases.

7. **Dynamic schema evolution for write path**
   Add support for Iceberg schema evolution (ADD COLUMN, RENAME COLUMN, DROP COLUMN) through SQL DDL. Currently the write path assumes a fixed schema.

8. **Merge-on-read compaction as a background service**
   Build a compaction daemon that runs inside the coordinator (or as a sidecar) to compact delete files in MOR tables. Triggered by table maintenance procedures already stubbed in `maintenance.rs`.

9. **gRPC health checking protocol**
   Implement the standard gRPC health check protocol (grpc.health.v1.Health) alongside the HTTP `/healthz` endpoint for better Kubernetes liveness/readiness probes.

10. **Query result caching for identical queries**
    The `QueryCacheConfig` struct exists. Wire up the cache for repeated identical queries (common in dashboards). Invalidate on underlying table changes via Iceberg snapshot IDs.

11. **Adaptive query scheduling**
    Use the `adaptive_sort` metrics to dynamically adjust join strategies, broadcast thresholds, and parallelism based on previous query performance.

12. **Predicate transfer from build to probe side**
    The `predicate_transfer` module exists. Complete the integration with the join framework to push filter values from build-side tables to probe-side scans.

13. **Per-query memory budgeting**
    Track memory usage per query and enforce limits at the DataFusion memory pool level. Currently the `runtime.rs` memory pool is global.

14. **Table-valued functions for external systems**
    Extend the TVF pattern (`read_parquet`, `read_csv`, `read_json`) to support external systems (Kafka, Postgres, Elasticsearch) as transient data sources.

15. **Rate limiting per-user/per-catalog**
    `RateLimitConfig` exists but appears to be global. Extend to per-user quotas (based on session subject) and per-catalog limits for multi-tenant isolation.

### Ecosystem

16. **dbt-sqe adapter: incremental model support**
    The dbt adapter exists but incremental model strategies (merge, append, delete+insert) may not be fully wired. Completing this unlocks dbt production usage.

17. **JDBC driver packaging**
    Package a standalone SQE JDBC driver (wrapping Arrow Flight SQL JDBC) for BI tool integration (Tableau, Power BI, SuperSet).

18. **Trino compatibility certification**
    Run the Trino JDBC driver test suite against `sqe-trino-compat` and publish a compatibility matrix. Aim for >= 95% pass rate.

19. **Iceberg v3 writer support**
    The codebase has `detect_ns_timestamp` and `is_v3_only_type` utilities in `sqe-sql`, suggesting v3 awareness. Complete the write path for Iceberg v3 features (variant types, partition evolution).

20. **OpenTelemetry-native dashboard**
    Export traces to OTel collector and provide a pre-built Grafana dashboard for query lifecycle visualization. The `tracing-opentelemetry` integration is already wired.

21. **Plugin system for custom UDFs**
    Allow users to register custom Rust UDFs via dynamic loading or a WASM plugin interface, extending the `trino_functions` approach.

22. **GitOps for policy management**
    Store policies as YAML in a git repo. Use a webhook or poll loop to sync policies from git to the PolicyStore (OPA/Ranger). This gives teams the standard PR-based policy workflow.

23. **Blame analysis on the `_ => {}` and `unwrap()` patterns**
    Track down each `panic!` path and assign an owner for remediation. This is a prerequisite for SOC 2 / audit readiness.

24. **Generate crate dependency graph**
    Add a CI step that generates a D2 or Mermaid diagram of crate dependencies and cross-crate type usage. Useful for onboarding and architecture reviews.

25. **Semantic versioning for the Flight SQL protocol**
    Document the wire format version and add a capability negotiation handshake so clients can detect breaking changes.
