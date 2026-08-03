# SQE production readiness review

**Date:** 2026-07-25  
**Revision reviewed:** `35ffc24`  
**Target:** a fast, reliable, secure SQL server for Apache Iceberg lakehouses, with Apache Polaris as the primary catalog and optional Apache Ranger integration  
**Scale target:** TPC-style SF100 and multi-terabyte query jobs  
**Method:** current-code review, existing benchmark evidence, prior audit reconciliation, and targeted tests

## Executive verdict

SQE is a serious engine, not a prototype. It has unusually strong Iceberg coverage, per-session Polaris identity, distributed scans, runtime-filter pushdown, spill-aware execution, signed worker tickets, production configuration validation, audit/lineage support, and a benchmark culture that checks results rather than only timing them.

The current code is credible for **controlled production at SF10-class scale**, provided the production guard is enabled and the deployment is sized and isolated correctly.

It is **not yet proven ready for SF100 or general multi-terabyte distributed queries**. The largest risks are not SQL compatibility. They are:

1. distributed shuffle is memory-only and bounded by batch count rather than bytes;
2. worker decode can outrun Flight delivery and already exhaust memory at SF10;
3. the best memory-pool policy depends on plan shape, but selection is static;
4. important scan shapes still funnel through one output partition;
5. the coordinator is a stateful single point of failure;
6. no clean, separate-host SF100 distributed qualification run exists.

For Polaris plus Ranger, the architecture is sound if the two authorization layers are kept distinct:

- Polaris/Ranger `polaris` service: coarse catalog/namespace/table authorization, enforced by Polaris.
- SQE/Ranger `hive` service: row filters, column masks, and Iceberg tag-based policies, enforced by SQE's logical-plan rewriter.

That composition needs stricter startup validation and operational diagnostics. Today, a plausible-looking but incomplete Ranger configuration can silently omit group-bound or namespace-mismatched policies.

### Readiness scorecard

| Area | Current assessment | SF100 / multi-TB gate |
|---|---|---|
| Iceberg semantics | Strong | Run mutation, maintenance, partition-evolution, delete-file, and time-travel soak tests at scale |
| Polaris integration | Strong but operationally complex | Prove token rotation, catalog churn, credential vending, and REST rate-limit behavior under concurrency |
| Ranger coarse access | Capable, delegated to Polaris | Enforce compatible Polaris/Ranger versions and test actual denial paths |
| Ranger row/mask policy | Strong core rewriter; important limitations remain | Groups, namespace identity, live tag-policy contract, and decision introspection |
| Single-node performance | Strong at SF1/SF10 | Remove single-stream and memory-policy cliffs |
| Distributed execution | Promising, partially fault tolerant | Spillable byte-bounded shuffle, worker backpressure, multi-node qualification |
| Reliability / HA | Good within one process | Coordinator failover and resumable/session-aware routing |
| Security | Strong controls when production mode is used | Secure-by-default chart/profile, per-user data-plane credentials, automated adversarial tests |
| Observability | Broad metrics/audit/lineage surface | Distributed stage/shuffle profiles, SLOs, slow-trace retention, Ranger decision visibility |
| Code maintainability | Good Rust discipline, oversized hot modules | Split query/write orchestration and add default distributed/write CI |

## What is already good

### 1. Iceberg-first design is real

The engine does not treat Iceberg as a thin file-list wrapper. The repository contains V2/V3 reads and writes, equality/position deletes, copy-on-write and merge-on-read paths, partition evolution, time travel, Puffin statistics, compaction, metadata tables, catalog backends, and Spark interoperability tests.

The vendored `iceberg-rust` fork is a maintenance burden, but the reason for it is documented and technically relevant: rewrite/overwrite transactions, delete writers, parallel manifest loading, and selected upstream fixes. This is much stronger than claiming Iceberg support based only on `SELECT`.

### 2. Query correctness is measured

The benchmark harness compares query outputs with Trino and, for several suites, an independent DuckDB oracle. The existing SF1/SF10 results are useful evidence because they track pass counts and row differences, not just elapsed time.

The repository is also honest about missing evidence: [the SF100 risk study](../../evidence/perf/sf100-scaling-risks.md) explicitly labels its conclusions as extrapolation rather than measurement.

### 3. Identity is preserved across the important boundaries

Per-user or per-connection identity can flow from Flight SQL / Trino HTTP through the session catalog to Polaris. Production validation rejects several identity-collapsing configurations, including a shared config-held client-credentials identity unless the operator explicitly accepts it.

Worker scan tickets are authenticated and signed over their exact bytes. That is important because tickets include file paths, predicates, limits, and credentials. The coordinator/worker boundary is treated as a trust boundary rather than an internal implementation detail.

### 4. The policy rewriter is deliberately fail-closed

The Ranger plan rewriter denies on unresolved table references, residual un-inlined views, invalid row-filter expressions, unsupported masks, tag-policy failures, and unsupported column-exclusion masks. Earlier critical findings around views and empty projections now have regression tests.

The separation between:

- coarse access in Polaris;
- Ranger fine-grained row/mask policy in SQE; and
- SQL `GRANT`/`REVOKE` administration

is conceptually clean, even though it needs simpler operator packaging.

### 5. Memory and failure behavior receive real engineering attention

The code has tracked memory pools, spill support, query admission controls, per-user budgets, worker health tracking, scan retries, local fallback, timeouts, cancellation, credential refresh, and graceful shutdown. Existing evidence documents both successful mitigations and failed experiments.

That engineering culture is a major asset. The remaining scale work is difficult, but the repository already contains the instrumentation and benchmark discipline needed to do it.

## Prioritized findings

Severity in this report means:

- **P0:** blocks the stated SF100/multi-TB or secure multi-tenant goal.
- **P1:** should be completed before broad production rollout.
- **P2:** important improvement after the main gates close.

### P0-1 — Distributed shuffle is memory-only and not byte bounded

**Evidence**

- [`crates/sqe-worker/src/shuffle.rs`](../../../crates/sqe-worker/src/shuffle.rs) uses one bounded Tokio channel per partition.
- The default capacity is 64 **record batches**, not a byte reservation.
- There is no shuffle-file writer, disk-backed exchange, byte quota, or spill/reload lifecycle.
- Existing SF100 notes independently identify “shuffle has no spill” as a blocker.

**Why this matters**

The memory ceiling is approximately:

`active stages × partitions × 64 × average RecordBatch bytes`

That ceiling is not tied to the configured DataFusion memory pool. A wide batch, many partitions, or several concurrent stages can consume gigabytes outside the intended operator budget. A bounded batch count prevents an infinite queue, but it is not memory safety.

At multi-TB scale, large hash joins and distributed aggregates must survive data volumes much larger than RAM. A memory-only exchange turns expected spill into query failure.

**Recommendation**

Build a shuffle subsystem with:

1. a byte-accounted buffer integrated with the runtime memory manager;
2. per-query, per-stage, and per-partition quotas;
3. local disk segments with checksums and atomic completion markers;
4. streamed readback and bounded merge fan-in;
5. cleanup on success, cancellation, worker restart, and coordinator loss;
6. metrics for produced, buffered, spilled, fetched, retried, and discarded bytes;
7. a protocol/version field so coordinator and worker upgrades fail clearly.

**Acceptance gate**

- A distributed join whose exchange is at least 5–10 times aggregate worker RAM completes by spilling.
- Peak RSS remains below the pod limit with at least four concurrent exchange-heavy queries.
- Worker termination during shuffle either retries safely or fails the query without leaking disk state.

### P0-2 — Worker scan backpressure is insufficient

**Evidence**

- Existing SF10 TPC-DS runs have exhausted a 4 GB worker pool because decode outran Flight shipment.
- The worker path streams results, but the effective pressure is not end-to-end byte accounting from object-store read through decode, Arrow buffers, compression, HTTP/2, and coordinator consumption.
- The SF100 preparation notes list worker backpressure as a certainty rather than a theoretical risk.

**Why this matters**

Parallel decode is valuable only while downstream transport can drain it. Otherwise additional parallelism converts I/O throughput into resident Arrow buffers and cgroup OOM kills. DataFusion pool accounting alone may not include all Flight, IPC, compression, and channel buffers.

**Recommendation**

- Introduce one byte semaphore per worker and a smaller per-query budget.
- Acquire before decode/materialization and release only after the Flight frame is consumed or dropped.
- Feed observed outbound queue depth and Flight send latency into adaptive scan concurrency.
- Separate “object-store requests in flight” from “decoded bytes waiting to ship.”
- Publish `decode_wait_seconds`, `outbound_buffer_bytes`, `flight_send_seconds`, and per-query high-water marks.

**Acceptance gate**

Run the currently failing distributed TPC-DS inventory queries with a deliberately slow coordinator/network and prove bounded RSS, no kernel OOM, and correct cancellation.

### P0-3 — SF100 and true multi-node behavior are not measured

**Evidence**

- Public evidence is strongest at SF1 and SF10.
- Existing distributed measurements co-locate coordinator, workers, and comparison engine on one host.
- [`docs/evidence/perf/sf100-scaling-risks.md`](../../evidence/perf/sf100-scaling-risks.md) explicitly says SF100 conclusions are projections.
- The generator/load pipeline still has suite-specific SF100 blockers.

**Why this matters**

SF10 rewards broadcast joins, in-memory hash tables, and modest exchange. SF100 changes join selection, intermediate size, spill pressure, catalog/file counts, and network behavior. Single-host distributed testing cannot establish horizontal scaling because all processes compete for the same CPU, RAM, disk, and NIC.

**Recommendation**

Create a repeatable qualification rig:

- one coordinator host;
- at least four separate worker hosts;
- separate object-storage service;
- persistent Polaris backing database;
- identical warehouse and network path for SQE and Trino;
- cold and warm runs;
- concurrency mixes, not only one query at a time;
- fault injection for worker death, slow worker, object-store throttling, Polaris 429/5xx, and expired tokens.

Start with SF30 if infrastructure limits SF100, but keep the plan and file counts representative.

**Exit criteria**

- 100% expected result correctness for the chosen TPC-H/TPC-DS/SSB qualification set.
- No OOM, orphaned shuffle/spill files, or stuck query permits.
- Defined p50/p95 and failure-rate targets.
- Measured scaling efficiency from 1→2→4 workers.
- At least one multi-TB scan and one exchange-heavy job complete successfully.

### P0-4 — Coordinator state is a single point of failure

**Evidence**

- The Helm chart defaults to one coordinator and explicitly states that the PDB does not provide HA.
- Sessions, query tracking, cancellation state, cache state, and distributed orchestration are process-local.
- Session snapshots intentionally omit tokens and [`SessionManager::restore_from_file`](../../../crates/sqe-coordinator/src/session_manager.rs) only logs that full restore is not implemented.

**Why this matters**

A coordinator restart ends in-flight queries and invalidates live sessions. Multiple replicas behind a generic load balancer are unsafe without session affinity, and affinity still does not provide recovery. This is acceptable for controlled batch use, but not for a highly available SQL service.

**Recommendation**

Choose and document one of two explicit service levels:

1. **Restartable, non-HA coordinator:** one active instance, clear client retry semantics, fast restart, no promise of query survival.
2. **HA control plane:** external session/query metadata, leases, idempotent stage dispatch, resumable result handles, and coordinator fencing.

Do not advertise multiple coordinators as HA until state ownership and fencing are implemented. A good intermediate step is active/passive leadership plus stateless bearer re-authentication and idempotent query submission tokens.

### P0-5 — Polaris authorization can be weakened by shared storage credentials

**Evidence**

- `SessionCatalog::new` injects configured S3 endpoint/access/secret properties as fallback file credentials.
- The Ranger access-control design notes correctly state that, once a caller can load table metadata, SQE may read files with its own S3 credentials; Polaris's `table-data-read` credential-vending decision is then not the data-path gate.

**Why this matters**

This makes `LOAD_TABLE` / table-properties authorization the effective data-access boundary. That can be secure, but only if every way of obtaining table metadata is governed consistently. A future privilege mapping or catalog behavior change could allow metadata access without the intended data privilege, after which shared engine credentials can read the objects.

It also weakens least privilege: object storage sees the engine identity, not the end user.

**Recommendation**

- Add a production option such as `catalog.require_vended_credentials = true`.
- In that mode, fail if Polaris does not return scoped credentials or remote-signing material; never fall back to global S3 credentials.
- Prefer short-lived, table/prefix-scoped credentials and include credential source in audit records.
- Add an integration test proving a user with namespace traversal but without table read cannot obtain either metadata or file bytes.
- Keep shared engine credentials only for explicitly single-tenant deployments.

### P0-6 — Memory-pool policy has known plan-shape cliffs

**Evidence**

- Greedy memory performs well for wide analytic plans but can starve non-spillable sort-merge reservations under concurrency.
- Fair spill forces earlier spill for a few large sorts but divides memory too aggressively across wide plans such as TPC-DS q39.
- The production guide recommends greedy while the SF100 study records a real concurrent-sort case where fair succeeds and greedy fails.

**Why this matters**

There is no single safe static choice for the stated workload mix. At SF100, large sorts, aggregates, joins, and concurrent users will encounter both failure modes.

**Recommendation**

- Add plan admission estimates: number of spillable consumers, non-spillable reservations, build-side sizes, and expected partition count.
- Select or parameterize the pool per query rather than once per process.
- Reserve protected merge/shuffle headroom.
- Cap concurrent heavy sort/write plans.
- Continue upstream work on proactive spill and per-consumer reservation limits.

The immediate operational workaround should remain documented, but it is not the final multi-tenant solution.

### P1-1 — Important scan shapes remain single-output-partition

**Evidence**

The Iceberg scan intentionally retains one output partition for several shapes because forcing partitioned scans can flip efficient broadcast joins into expensive fact-table shuffles. Existing SSB analysis shows scan-bound queries funneling hundreds of millions of rows through one stream.

**Recommendation**

Implement the designed parallel-probe path: parallel fact scan partitions while retaining a broadcast/collected build side. Make join strategy and probe parallelism independent choices. Add skew-aware morsel sizing so a few large files do not pin one worker.

### P1-2 — Scheduler cost and affinity signals are too coarse

**Evidence**

The weighted scheduler primarily estimates cost from file bytes and current fragment count. Affinity hashes the first file path. It does not account for:

- predicate selectivity;
- delete-file amplification;
- row-group count;
- object-store region/endpoint locality;
- worker CPU, memory pressure, spill pressure, or outbound network queue;
- historical throughput;
- skew after dynamic filters resolve.

**Recommendation**

Publish a worker load vector and schedule against dominant resource:

`scan bytes, expected decoded bytes, CPU, memory, network, spill`

Rebalance before dispatch where possible, split large tasks into smaller morsels, and add speculative execution only for idempotent read fragments after robust cancellation exists.

### P1-3 — Ranger group-bound policies are not enforced

**Evidence**

`RangerStore::item_matches` accepts `groups` but deliberately ignores them; a group-only item is skipped and logged at debug level. Tag-policy resolution also constructs a user with an empty groups vector.

**Why this matters**

Group membership is a normal enterprise Ranger authoring model. An operator can create a policy that looks valid in Ranger Admin but has no effect in SQE. For a row filter or mask, “skipped” can expose unfiltered data because no policy matched.

**Recommendation**

- Thread OIDC groups from `SessionUser` into Ranger matching.
- Define exact precedence and normalization for users, roles, and groups.
- Until implemented, reject startup when Ranger fine-grained mode is enabled with group-based policies, or emit a high-severity metric/health failure.
- Add `sqe_ranger_unsupported_policy_items_total{reason="groups"}`.

### P1-4 — Ranger namespace matching flattens multi-level namespaces

**Evidence**

The plan rewriter and Ranger store reduce a multi-level namespace to its last component for the Hive-style Ranger database key. Thus `tenant_a.finance` and `tenant_b.finance` both map to `finance`.

**Why this matters**

At enterprise scale this creates collisions and surprising policy scope. Debug logging helps diagnosis but does not prevent an ambiguous configuration.

**Recommendation**

- Define a reversible canonical encoding for full Iceberg namespace identity.
- Make it configurable only during migration, not per query.
- Detect collisions during catalog enumeration and refuse production startup if two visible namespaces map to the same Ranger database.
- Provide a migration report showing old key → canonical key.

### P1-5 — Ranger feature support is narrower than the Admin UI suggests

Current limitations include:

- exact resource names and bare `*`, but not general Ranger glob patterns;
- fine-grained group bindings skipped;
- live tag-policy JSON contract has limited real-stack validation;
- coarse `GRANT ... TO GROUP` is rejected;
- external policy edits are cache-TTL delayed;
- coarse and fine-grained Ranger use different service definitions.

**Recommendation**

Ship a supported-policy contract and validate downloaded bundles against it. Add a `SHOW EFFECTIVE POLICY FOR ...` command that displays:

- coarse Polaris/Ranger decision source;
- matched user/role/group;
- row filters;
- column masks/restrictions;
- tags and their source;
- policy version and cache age;
- unsupported/skipped items.

This is more valuable operationally than adding more mask types without introspection.

### P1-6 — Polaris authentication failure causes process-wide catalog-cache invalidation

**Evidence**

`invalidate_rest_catalog_cache_all` drops every cached REST catalog on an authentication failure because there is no user/token-fingerprint reverse index. Up to 2,000 entries may then lazily rebuild and repay `/v1/config` and namespace-list costs.

**Why this matters**

One expired or rejected token can cause a fleet-wide metadata latency spike and a thundering rebuild against Polaris. At high concurrency this can amplify an identity-provider or Polaris incident.

**Recommendation**

- Invalidate by catalog URL + warehouse + token fingerprint.
- Rotate the session catalog immediately when credentials rotate.
- Add single-flight refresh per identity and jittered backoff.
- Respect token expiry before issuing a request; the token-exchange provider currently re-exchanges unconditionally when asked to refresh.
- Load-test token rotation for thousands of concurrent sessions.

### P1-7 — Metadata introspection can turn outages into empty results

**Evidence**

`information_schema` and several `system.metadata` paths log non-authorization listing failures and return empty vectors.

**Why this matters**

During a Polaris outage, tooling may conclude that schemas or tables disappeared. For discovery clients, “empty” and “unavailable” are materially different.

**Recommendation**

- Preserve the current empty behavior only for explicit access denial.
- Surface transient failures as retryable catalog-unavailable errors.
- If partial results are necessary, return a warning/status relation and mark results incomplete.

### P1-8 — Production behavior is opt-in, not the deployment default

**Evidence**

- `coordinator.production_mode` defaults to false.
- rate limiting defaults to disabled.
- Helm audit logging is enabled but ephemeral unless a PVC is configured.
- TLS depends on engine or ingress configuration.
- the default chart is intentionally small and single-node oriented.

The production validator is good, but it only protects deployments that turn it on.

**Recommendation**

Add an opinionated `values-production.yaml` and CI-render it. It should enable production mode and rate limiting, require existing secrets, require durable audit storage, configure a NetworkPolicy, set resource/headroom invariants, and fail Helm templating when distributed mode lacks its prerequisites.

### P1-9 — Benchmark generation and load are part of the scale gate

**Evidence**

Some generators still return a full `Vec<RecordBatch>` for a table; the SSB SF100 estimate is roughly 68 GB resident for `lineorder`. Sort-on-write can fall back to unsorted output after memory pressure, changing data layout and therefore benchmark behavior.

**Recommendation**

- Port every large generator to deterministic range-based streaming.
- Rotate Parquet files by target bytes and row groups.
- Make sort-on-write failure explicit; do not silently benchmark a different layout.
- Persist the generated dataset and Polaris metadata so repeated engine runs use an immutable golden warehouse.
- Record file count, size distribution, sort order, partition spec, snapshot ID, and object-store endpoint in every benchmark result.

### P1-10 — Default CI does not fully protect distributed and write paths

The codebase has many tests, but the most expensive and risky behaviors still depend on stack-gated or manual runs. The July audit also identifies oversized coordinator modules: `write_handler.rs`, `query_handler.rs`, and `config.rs`.

**Recommendation**

- Add a small mandatory two-worker distributed correctness job.
- Add fault tests for worker loss, stream error, cancellation, and retry.
- Add an Iceberg write/commit-conflict matrix to default CI.
- Split orchestration from algorithms: query lifecycle, distributed dispatch, DML planning, file writing, and commit/retry should have narrower modules and contracts.

### P2 improvements

1. Make REST and policy cache TTLs refresh-ahead with jitter rather than synchronized expiry.
2. Add query plan fingerprints and regression baselines per scale factor.
3. Track object-store requests, bytes, retries, throttles, and first-byte latency by catalog.
4. Add delete-file amplification and manifest-count alerts.
5. Add automatic maintenance recommendations from table health, but keep execution separately authorized.
6. Replace documentation status tables that say “Open” after their individual finding files say “Resolved.”
7. Review the vendored Iceberg fork quarterly and maintain an explicit upstream/rebase ledger.
8. Add chaos tests for audit/lineage sink outage and spool exhaustion.

## Recommended implementation sequence

### Phase A — Make SF100 measurable

1. Stream all large benchmark generators.
2. Make the load path spill-safe and layout-explicit.
3. Build the separate-host qualification rig.
4. Establish immutable SF30/SF100 golden Iceberg snapshots.

### Phase B — Make distribution memory safe

1. End-to-end byte backpressure on worker scans.
2. Byte-accounted, spillable shuffle.
3. Per-query/stage resource quotas and cleanup.
4. Distributed profiles that expose stage, shuffle, spill, retry, and skew metrics.

### Phase C — Remove plan-shape cliffs

1. Parallel probe scans without forcing fact-side shuffle.
2. Plan-aware memory policy and admission.
3. Smaller morsel scheduling with richer worker load signals.
4. Re-test join strategies and runtime filters at SF100.

### Phase D — Harden Polaris and Ranger

1. Require vended/scoped storage credentials in multi-tenant production.
2. Scoped REST catalog invalidation and refresh single-flight.
3. Full namespace encoding and collision detection.
4. Group support or explicit policy-bundle rejection.
5. Effective-policy introspection and policy-version/cache-age audit fields.
6. Version-pinned live integration matrix for Polaris, Ranger, Keycloak, and Spark.

### Phase E — Define the HA service level

1. Publish restart semantics and client retry guidance now.
2. Add active/passive coordinator fencing or externalize the required state.
3. Only then support multiple coordinator replicas as HA.

## Proposed qualification matrix

| Dimension | Minimum cases |
|---|---|
| Scale | SF10 regression, SF30 rehearsal, SF100 qualification, one multi-TB scan |
| Topology | single node; 2 workers; 4 workers on separate hosts |
| Query mode | scan-heavy, broadcast join, shuffle join, large aggregate, global sort, DML, maintenance |
| Concurrency | 1, 4, 16 mixed users; one heavy query plus short interactive queries |
| Failure | worker kill, slow worker, coordinator restart, S3 throttle, Polaris 429/5xx, Ranger outage, token expiry |
| Security | unauthorized metadata, unauthorized file read, row-filter bypass attempts, view/subquery/DML paths, group/role mismatch |
| Iceberg | V2/V3, position/equality deletes, evolved partition specs, many manifests, branches/tags, commit conflict |
| Storage | AWS S3 and the production S3-compatible endpoint |
| Protocol | Flight SQL, Trino HTTP, dbt, Spark interoperability |

Every qualification result should capture:

- git revision and dependency lock hash;
- configuration with secrets removed;
- catalog and table snapshot IDs;
- file/manifest/delete-file counts;
- host/pod resources and engine memory limits;
- object-store/network topology;
- query plan and plan fingerprint;
- correctness result;
- wall time, CPU, peak RSS, tracked memory, spill, shuffle, retries, and bytes scanned/decoded/returned.

## Concrete go/no-go criteria

### Go for controlled SF10 production

- `production_mode = true` passes.
- OIDC audience/issuer validation is configured.
- no anonymous, bearer passthrough, or unacknowledged shared service identity.
- TLS is present on every untrusted hop.
- worker secrets and signed tickets are enabled.
- rate limiting, durable audit, and alerting are enabled.
- memory limit is below cgroup/pod memory with spill headroom.
- the exact workload passes an SF10 soak test without OOM.
- Ranger limitations are acceptable for the authored policies.

### No-go for general SF100/multi-TB claims until

- shuffle spills and is byte bounded;
- worker scan buffers are byte bounded;
- separate-host multi-node results exist;
- generator/load can create the qualification dataset reproducibly;
- concurrent sort/join memory cliffs are controlled;
- coordinator failure semantics meet the advertised SLO;
- Polaris data-plane credential behavior is explicitly tested.

## Targeted verification performed

The review ran:

```bash
cargo test -p sqe-policy --test view_bypass_policy
cargo test -p sqe-policy --lib
cargo test -p sqe-core coordinator_debug_does_not_leak_worker_secret
cargo test -p sqe-core validate_production_rejects_disabled_rate_limit
```

Results: 6/6 view-policy tests passed; 228/228 policy library tests passed with one live Ranger tag-policy fixture test intentionally ignored; both selected production-security configuration tests passed.

These tests validate the current Ranger policy core, including the repaired view path. Broader workspace, Docker, live Polaris/Ranger, and SF100 tests were not run as part of this static review; they require the external services and qualification hardware described above.

## Final assessment

The project has the right foundations and an unusually honest body of performance evidence. The shortest path to the stated goal is not another round of SQL functions. It is to make data movement and memory ownership explicit:

**byte-account every pipeline, spill every large exchange, measure on separate hosts, and make Polaris/Ranger security assumptions executable startup checks.**

If the P0 items are completed and the qualification matrix passes, SQE can credibly position itself as a fast and secure Iceberg/Polaris SQL server for SF100 and multi-terabyte workloads. Until then, the accurate positioning is: **strong SF10-class controlled-production engine with promising, but not yet proven, large-scale distributed execution.**
