# Roadmap

SQE is developed in phases, each building on the previous.

## Phase Overview

```mermaid
gantt
    title SQE Implementation Phases
    dateFormat YYYY-MM
    axisFormat %b %Y

    section Core
    Phase 1 - Single Node        :done, p1, 2026-01, 2026-02
    Phase 2 - Write Path         :done, p2, 2026-02, 2026-03
    Phase 3 - Row-Level Writes   :done, p3, 2026-03, 2026-04

    section Scale
    Phase 3b - Benchmarks        :done, p3b, 2026-03, 2026-04
    Phase 4 - Pluggable Auth     :done, p4, 2026-03, 2026-04
    Phase 4b - Streaming/Distributed :done, p4b, 2026-04, 2026-04

    section Shipped
    Phase 2c - dbt Compatibility :done, p2c, 2026-03, 2026-04
    Phase 5 - Pluggable Catalogs :done, p5, 2026-04, 2026-05
    Phase 6 - Security Policies  :done, p6, 2026-05, 2026-06
    Phase 7 - Iceberg V3         :done, p7, 2026-04, 2026-05
    Phase 9 - OpenLineage        :done, p9, 2026-05, 2026-05

    section Next
    Phase 8 - Trino Decommission :p8, 2026-07, 2026-09
```

---

## Phase 1 - Single-Node Engine (Done)

The foundation: a working SQL engine that queries Iceberg tables through Polaris with Keycloak auth.

- DataFusion query execution
- Keycloak OIDC authentication (ROPC grant)
- Per-session catalog with bearer token passthrough
- Arrow Flight SQL server
- CLI client (`sqe-cli`)
- `SELECT`, `SHOW CATALOGS/SCHEMAS/TABLES`, `EXPLAIN`
- Prometheus metrics + structured JSON logging

## Phase 2 - Write Path & Views (Done)

SQL write operations and catalog DDL.

- `CREATE TABLE AS SELECT`
- `CREATE OR REPLACE TABLE`
- `INSERT INTO SELECT`
- `CREATE VIEW` / `DROP VIEW`
- `CREATE SCHEMA` / `DROP SCHEMA`
- `DROP TABLE` / `DROP TABLE IF EXISTS`
- Parquet writer (to S3 via Iceberg)
- Write-path memory safety: pool-tracked write buffers (oversized writes fail with a typed `ResourceExhausted` instead of OOM), streaming ingest/CTAS/INSERT, and an opt-in bounded fanout writer (see [Write Path, Memory Safety](../features/write-path.md#memory-safety))
- Audit logging (JSONL, OCSF): canonical `AuditEvent`, OCSF class mapping, tamper-evident hash chain, GDPR-tag masking, identity enrichment
- OpenTelemetry export (OTLP/gRPC)
- Trino-compatible HTTP endpoint

## Phase 2c - dbt Compatibility (Done)

Native dbt support via `dbt-sqe` adapter over ADBC Flight SQL.

- `information_schema` virtual providers (tables, schemata, columns)
- `dbt-sqe` Python adapter (connection manager, materializations)
- `ALTER TABLE RENAME`
- dbt `table`, `view`, and append-only `incremental` materializations
- `incremental` with `merge` strategy (CoW + MoR)
- Adapter lives at `adapters/dbt-sqe/dbt/`

---

## Phase 3 - Row-Level Writes (Done)

DELETE FROM, UPDATE, and MERGE INTO are implemented via Copy-on-Write using the iceberg-rust fork vendored at `vendor/iceberg-rust/` (DF 53.1 + Arrow 58 rebase of `risingwavelabs/iceberg-rust`), which provides `rewrite_files()` transaction support.

### Strategy: Copy-on-Write

```mermaid
graph TB
    subgraph "Copy-on-Write (Implemented)"
        READ["Read affected<br/>data files"] --> FILTER["Apply WHERE filter"]
        FILTER --> REWRITE["Rewrite without<br/>deleted/modified rows"]
        REWRITE --> COMMIT["Commit via<br/>rewrite_files()"]
    end

    subgraph "Merge-on-Read (Future)"
        DELFILE["Write position<br/>delete files"] --> COMMITDEL["Commit via<br/>RowDeltaAction"]
        COMMITDEL --> COMPACT["Background<br/>compaction"]
    end

    style READ fill:#6f9
    style FILTER fill:#6f9
    style REWRITE fill:#6f9
    style COMMIT fill:#6f9
```

CoW rewrites affected data files entirely. MoR has shipped: set `TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')` to opt in. SQE writes a position-delete file (no PK) or an equality-delete file (with PK) and commits via `FastAppendAction` / `RowDeltaAction`. CoW remains the default for backward compatibility.

### Delivered

- `DELETE FROM table WHERE condition` - removes matching rows; supports cross-table subqueries; DELETE without WHERE = truncate
- `UPDATE table SET col = expr WHERE condition` - modifies matching rows; supports CASE WHEN transformations and cross-table subqueries
- `MERGE INTO target USING source ON condition WHEN MATCHED/NOT MATCHED ...` - full outer join approach with WHEN MATCHED/NOT MATCHED clauses
- All operations atomic via Iceberg snapshot isolation
- dbt `incremental` with `merge` strategy
- Integration tests against Polaris + MinIO
- TPC-C write queries (17/17 pass), TPC-E write queries enabled

### Iceberg Dependency

Uses the iceberg-rust fork vendored at `vendor/iceberg-rust/` for `rewrite_files()`. When upstream apache/iceberg-rust ships `OverwriteAction` (tracked in Epic #2186), the dependency can be migrated back to the official crate.

### SQE Changes

| File | Change |
|---|---|
| `Cargo.toml` | Vendored iceberg-rust fork (DF 53 + Arrow 58 rebase) at `vendor/iceberg-rust/` |
| `crates/sqe-coordinator/src/delete_handler.rs` | DELETE FROM execution via CoW |
| `crates/sqe-coordinator/src/update_handler.rs` | UPDATE execution via CoW |
| `crates/sqe-coordinator/src/merge_handler.rs` | MERGE INTO execution via CoW |
| `crates/sqe-coordinator/src/query_handler.rs` | Routes Merge/Delete/Update to handlers |
| `crates/sqe-coordinator/src/write_handler.rs` | Shared CoW rewrite logic |

---

## Phase 7 - Iceberg V3 (Done)

Iceberg V3 table format support landed end-to-end. The vendored fork at `vendor/iceberg-rust/` is rebased onto DataFusion 53.1 + Arrow 58 and carries V3 spec coverage. SQE Iceberg matrix score: 167/189 = 88.4% (per `docs/iceberg-matrix.md`).

### V3 Features Shipped

| Feature | Status |
|---|---|
| Default values (`ALTER TABLE ADD COLUMN ... DEFAULT`) | Done |
| Schema evolution (`ALTER TABLE ADD/DROP COLUMN`) | Done |
| Nanosecond timestamps (`TIMESTAMP_NS`, `TIMESTAMPTZ_NS`) | Done |
| Partition evolution | Done |
| Equality deletes + position deletes (MoR) | Done |

### V3 Features Still Blocked Upstream

| Feature | Blocker |
|---|---|
| Variant type | iceberg-rust [#2188](https://github.com/apache/iceberg-rust/pull/2188) not merged |
| Geometry type | DataFusion UDT [#12644](https://github.com/apache/datafusion/issues/12644) |
| Vector / Embedding type | Iceberg V3 vector spec not finalised |
| Multi-arg partition transforms | Iceberg Java spec alignment in progress |
| Row lineage | Deferred upstream |

### Other Hardening

- Metadata cache invalidation on DDL
- Large result-set streaming (Flight SQL `do_get` back-pressure)
- Error messages tuned for catalog and auth failures
- Partition pruning across all predicate types

---

## Phase 4b/4c - Distributed Execution (Done)

Scale-out query execution with stateless workers. Implemented via streaming execution in two phases.

```mermaid
graph TB
    subgraph Coordinator
        PLAN["Query Planner"] --> STAGE["Stage Decomposition"]
        STAGE --> SCHED["Scheduler"]
    end

    subgraph Workers
        SCHED -->|ScanTask| W1["Worker 1"]
        SCHED -->|ScanTask| W2["Worker 2"]
        SCHED -->|ScanTask| W3["Worker 3"]
    end

    W1 <-->|DoExchange shuffle| W2
    W2 <-->|DoExchange shuffle| W3
    W1 -->|Arrow stream| MERGE["Multi-endpoint merge"]
    W2 -->|Arrow stream| MERGE
    W3 -->|Arrow stream| MERGE
    MERGE --> CLIENT["Client"]
```

### Delivered

- **Phase A (spill-to-disk):** FairSpillPool with watermarks, late materialization, file/page pruning, TopK, S3 I/O pipeline (coalescing, footer cache, prefetch), SortMergeJoin fallback
- **Phase B (distributed):** DoExchange shuffle, distributed sort (range-partition with sampling), two-phase aggregation, distributed joins (broadcast, shuffle hash, pre-sorted merge, predicate transfer), multi-endpoint Flight SQL, stage decomposition
- **Adaptive sort stripping** - memory-aware sort mode selection
- **Metrics** - spill, shuffle, late-mat, pruning, time-to-first-row, S3 I/O, auth, write path

### Benchmark Results (SF1, distributed 2-worker)

| Suite | Pass Rate | Time | Speedup vs single |
|---|---|---|---|
| TPC-H | 22/22 | 13.5s | 2.1x |
| TPC-DS | 98/99 | 36.1s | 2.8x |
| SSB | 13/13 | 5.3s | 2.7x |
| TPC-C | 17/17 | 8.6s | 2.6x |

---

## Phase 5 - Pluggable Catalogs (Done)

`CatalogBackend` trait replaced the hard-coded Polaris REST catalog. Seven catalog backends ship today:

| Backend | Status |
|---|---|
| Iceberg REST (Polaris, Lakeformation) | Done - default |
| AWS Glue | Done |
| Nessie | Done |
| Hive Metastore (Thrift) | Done |
| AWS S3 Tables | Done |
| Snowflake Horizon (REST-compatible) | Done |
| JDBC | Done |

Multi-cloud storage via `object_store`: S3 (+ endpoint override for R2, Ceph, Garage, MinIO), Azure ADLS Gen2/Blob, GCS, local filesystem. Engine-side session-manager wiring for Delta + remaining edge cases is the only deferred item; tracked in `nextsteps.md`.

---

## Phase 6 - Security Policies (Shipped, off by default)

Fine-grained access control via LogicalPlan rewriting. Implemented and pluggable, and **off by default**: the policy engine defaults to `passthrough` and `access_control.backend` defaults to `none`, so enforcement is opt-in. See [GRANT and REVOKE](../sql-reference/grant-revoke.md) for the SQL surface, backends, and known gaps.

Shipped:

- Plan-rewriting `PolicyEnforcer` (row filters and column masks injected before optimization)
- `GRANT/REVOKE` with `ROWS WHERE` and `MASKED WITH`
- `SHOW GRANTS` / `SHOW EFFECTIVE GRANTS` / `CHECK ACCESS`
- Column restriction (invisible columns)
- Policy caching with TTL (moka)
- No-information-leakage model (PostgreSQL RLS style)
- Wired backends: Apache Ranger (production) and an in-memory store (dev / tests)

Not yet wired:

- OPA (Rego) and Cedar policy engines are present as configuration options but remain experimental
- See the "Known gaps" in [GRANT and REVOKE](../sql-reference/grant-revoke.md) for SQL-surface limits (no `WITH GRANT OPTION`, table-level INSERT only, scalar-only masks)

---

## Phase 9 - OpenLineage Emitter (Done)

Coordinator-side OpenLineage 2-0-2 emitter with column-level lineage. Off by default; zero hot-path overhead when disabled.

- `sqe-lineage` crate: event types, observer, emitter task, file/HTTP/spool sinks, multi-catalog dataset extractor, column-lineage trace rules across 11 LogicalPlan node types
- `OpenLineageConfig` in `sqe-core` with TOML + env-var overrides + startup validation
- Hooks in `QueryHandler::execute_statement` (START / COMPLETE / FAIL)
- Documented at `docs/book/src/operations/openlineage.md`

Deferrals (documented): mTLS, per-event user-OIDC bearer forwarding, MERGE per-branch column annotations, embedded CLI emit, DDL hint extraction.

---

## Phase 10 - Performance & Reliability Testing (Planned)

Validate SQE is production-ready through systematic benchmarking and reliability testing.

### Performance Benchmarks

```mermaid
graph LR
    subgraph "Benchmark Suite"
        TPCH["TPC-H<br/>22 queries<br/>SF 10/100/1000"]
        TPCDS["TPC-DS<br/>99 queries<br/>SF 10/100"]
        CUSTOM["Custom workloads<br/>Iceberg-specific<br/>partition pruning"]
    end

    subgraph "Comparison Targets"
        TRINO["Trino<br/>(current prod)"]
        SQE["SQE"]
        SPARK["Spark SQL<br/>(reference)"]
    end

    TPCH --> TRINO
    TPCH --> SQE
    TPCH --> SPARK
    TPCDS --> TRINO
    TPCDS --> SQE
```

| Benchmark | Scale Factors | Purpose |
|---|---|---|
| **TPC-H** | SF10, SF100, SF1000 | Standard analytical workload, join-heavy |
| **TPC-DS** | SF10, SF100 | Complex analytics, subqueries, window functions |
| **Iceberg-specific** | Varies | Partition pruning, metadata operations, time travel |
| **Write path** | 1M, 10M, 100M rows | CTAS, INSERT, MERGE throughput |
| **Concurrent users** | 10, 50, 100 sessions | Connection handling, session isolation |

#### Key Metrics

- **Query latency** - P50, P95, P99 per query
- **Throughput** - queries/second under load
- **Memory usage** - peak RSS per query complexity
- **Startup time** - cold start to first query
- **Scan speed** - GB/s from S3 (single-node vs distributed)

#### Performance Targets

| Metric | Target | Rationale |
|---|---|---|
| TPC-H SF100 geometric mean | Within 2x of Trino | Parity goal for migration |
| Cold start to ready | < 2 seconds | K8s autoscaling responsiveness |
| Peak memory (SF100 query) | < 4GB coordinator | Fit in standard K8s pod limits |
| Concurrent session overhead | < 10MB per session | Support 100+ sessions |

### Reliability Testing

| Test | Method | What it validates |
|---|---|---|
| **Chaos: kill worker mid-query** | `kubectl delete pod` during scan | Coordinator retries/fails gracefully |
| **Chaos: kill coordinator** | SIGKILL during query | In-flight queries fail cleanly, no data corruption |
| **Chaos: Polaris unavailable** | Block network to Polaris | Graceful error, no hang, cached metadata still works |
| **Chaos: Keycloak unavailable** | Block network to Keycloak | Existing sessions continue, new auth fails cleanly |
| **Chaos: S3 latency spike** | tc netem delay on S3 | Query timeout, not hang |
| **Memory pressure** | Large query + small memory limit | Spill-to-disk or clean OOM, no silent corruption |
| **Token expiry during query** | Set very short token TTL | Refresh mid-query, or clean auth error |
| **Concurrent DDL + DML** | CTAS while DROP TABLE on same table | Iceberg conflict detection, clean error |
| **Long-running soak test** | 24h mixed workload | No memory leaks, no connection leaks, stable latency |

### Profiling & Optimization

- **CPU profiling** - `perf` + flamegraphs on hot queries
- **Memory profiling** - `jemalloc` stats, allocation tracking
- **I/O profiling** - S3 request counts, Parquet read amplification
- **Query plan analysis** - DataFusion `EXPLAIN ANALYZE` for bottleneck identification

### Deliverables

- Automated benchmark harness (run TPC-H/DS, collect results, compare)
- Performance regression CI (catch slowdowns before merge)
- Published benchmark results: SQE vs Trino on identical data
- Reliability test playbook with pass/fail criteria
- Memory/CPU profiling report with optimization recommendations
- Soak test (24h) passing without degradation

---

## Phase 8 - Trino Decommission (Future)

Complete migration from Trino DCAF fork. The BI-tool slice has largely landed: Metabase and the Trino JDBC driver connect, sync schemas, and run typed queries against the Trino HTTP endpoint, and Superset rides the same path (see [Trino Compatibility](../features/trino-compatibility.md)). What remains is the wind-down of the fork itself.

- Full Trino wire protocol compatibility for remaining tools (BI tools done; DBeaver/dbt-trino exercised)
- Dashboard migration playbook (Superset, Grafana, etc.)
- JDBC driver migration guide (Trino JDBC to Flight SQL JDBC)
- Performance parity validation (benchmark comparison)
- Runbook for operators
- Trino fork sunset and decommission
