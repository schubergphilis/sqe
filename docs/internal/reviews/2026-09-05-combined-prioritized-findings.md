# Combined architecture, security and scale backlog — 2026-09-05

This is the combined prioritization of **all 109 reported findings**: 102 from the September 4 architecture review and seven from the September 5 security/scale review. Every original finding appears exactly once in the area tables. Original IDs, files, severity and status are preserved.

**This index is the authoritative combined scheduling order.** The source reviews remain the evidence and acceptance-criteria records. This is prioritization of reported findings, not confirmation that all 102 older defects remain present: some code has changed since the earlier review. Revalidate each finding against current code before implementation and record already-fixed items with evidence.

## Progress

- **2026-09-05** (branch `fix/errors-audit-observability`): fixed 607, 800, 104, 305, 709, 306, 812, 410, 612 from area 10 (610 partial). See each finding file's Resolution section.
- **2026-09-05** (branch `fix/security-scale-review-2026-09-05`): fixed 900, 901, 902, 903, 904, 905, 906 (all seven security/scale findings) and 105, 200, 203, 206 from the September 4 review. Each finding file carries a "Resolution" section with the code location and the tests that cover it. The cache remediation for 902 disables the materialized result cache for every governing policy engine; re-enabling it behind a policy fingerprint is follow-up work.
- **2026-09-05** (branch `fix/authz-policy-enforcement-p1`, work package #653): fixed 201, 202, 211, 303, 400, 503, 510, the remaining P1s of area 2. Single policy-key derivation, MERGE target row filters and masks, time-travel identity restore before policy, Hive INSERT source enforcement and pre-write target check, tiered maintenance authorization, freshness-checked tag properties, and a rewriter shape suite. 18 of 109 findings closed.

GitLab tracking: every finding has an issue (see [the issue map](2026-09-05-gitlab-issue-map.md)); the eleven areas below are grouped into eight `work-package` umbrella issues #652–#659, listed at the end of that map.

## Priority definitions and ordering

- **P1 — immediate:** address or mitigate before a shared production rollout; prioritize current security exposure, data loss, silent wrong results and node-wide outage paths.
- **P2 — next:** address before production scale-up, or in the next reliability/performance iteration.
- **P3 — planned:** structural simplification, tooling or lower-impact diagnostics after operational risks are controlled.

Priority is independent of the original severity. Within each area, rows run from P1 to P3; row order breaks ties using exposure, impact, dependencies and implementation effort. Areas are grouped by ownership, **not** a requirement to finish an entire area before starting another. Across areas, take P1 work first. Tests needed to validate a fix ship with that fix, regardless of the priority of broader test-infrastructure findings.

Deployment conditions matter: promote partitioned fanout (905) if those writes are live, and centralized aggregation (408) if workloads already exceed coordinator capacity. Findings about optional MCP, Quack or distributed endpoints are urgent where those endpoints are enabled/reachable; otherwise fix or keep them disabled before enabling them.

## Coverage

| Area | P1 | P2 | P3 | Total |
|---|---:|---:|---:|---:|
| Authentication, sessions and transport security | 6 | 3 | 0 | 9 |
| Authorization, governance and policy enforcement | 9 | 2 | 0 | 11 |
| External data access and credential isolation | 7 | 0 | 0 | 7 |
| Result caches and metadata consistency | 4 | 3 | 0 | 7 |
| Memory safety and large-data execution | 6 | 8 | 0 | 14 |
| Writes, transactions and maintenance | 3 | 4 | 1 | 8 |
| Distributed scheduling and worker reliability | 2 | 4 | 0 | 6 |
| SQL planning, protocol behavior and result correctness | 3 | 10 | 0 | 13 |
| Configuration, startup and deployment | 2 | 5 | 2 | 9 |
| Errors, audit and observability | 0 | 9 | 1 | 10 |
| Test infrastructure, benchmarks and code structure | 1 | 3 | 11 | 15 |
| **Total** | **43** | **51** | **15** | **109** |

## 1. Authentication, sessions and transport security

Close authentication and identity propagation gaps before tuning session throughput. Treat all exposed protocols consistently.

| Priority | ID | Original severity | Finding | Coordination / condition |
|---|---|---|---|---|
| P1 | [203](2026-09-04-architecture-review/203-mcp-oauth-scopes-trusted-from-unverified-payload.md) | high | MCP OAuth mode reads scopes from the raw JWT payload and only refuses the anonymous provider | — |
| P1 | [204](2026-09-04-architecture-review/204-quack-session-drops-groups-identity.md) | high | Identity-to-Session conversion is copied five times and the Quack copy drops groups | — |
| P1 | [207](2026-09-04-architecture-review/207-bearer-token-validation-hardening.md) | medium | BearerTokenProvider leaves issuer, nbf and key-retirement to defaults | — |
| P1 | [606](2026-09-04-architecture-review/606-quack-server-session-is-a-bearer-uuid-with-no-expiry-or-result-cap.md) | medium | Quack server sessions are bearer UUIDs that outlive the token, results are fully materialised with no cap, and the policy-denial masking branch is dead | — |
| P1 | [702](2026-09-04-architecture-review/702-trino-poll-routes-reauthenticate-and-bypass-rate-limiter.md) | high | Trino poll and cancel routes re-run the OIDC password grant on every request and bypass the pre-auth rate limiter | Coordinate with 209; additionally avoid repeated password grants. |
| P1 | [209](2026-09-04-architecture-review/209-auth-rate-limit-placement-per-entry-point.md) | medium | Pre-auth rate limiting is decided per entry point and the Trino bearer path has none | Coordinate with 702; broader protocol rate-limiter coverage. |
| P2 | [405](2026-09-04-architecture-review/405-jwt-bearer-requests-mint-a-session-each.md) | medium | Every JWT-bearer Flight request mints a new Session and re-validates the token | — |
| P2 | [210](2026-09-04-architecture-review/210-tls-static-load-no-rotation-duplicated-builders.md) | medium | TLS material is read once at startup, in three separate builders, with no rotation or minimum version | — |
| P2 | [208](2026-09-04-architecture-review/208-token-material-outside-secretstring.md) | medium | Access and refresh tokens live in plain `String` in the legacy auth path | — |

## 2. Authorization, governance and policy enforcement

Close direct policy bypasses first, then policy-cache freshness and metadata visibility. Deliver negative tests with the fixes.

| Priority | ID | Original severity | Finding | Coordination / condition |
|---|---|---|---|---|
| P1 | [200](2026-09-04-architecture-review/200-plan-rewriter-skips-subquery-scans.md) | high | Plan rewriter never visits TableScans inside subqueries | — |
| P1 | [201](2026-09-04-architecture-review/201-policy-key-derivation-drifted-across-paths.md) | high | Policy lookup key is derived in four places and they disagree on the namespace | — |
| P1 | [400](2026-09-04-architecture-review/400-time-travel-aliases-bypass-policy.md) | high | Time-travel, version and incremental reads register under `datafusion.public.*` and bypass the policy rewriter | — |
| P1 | [202](2026-09-04-architecture-review/202-merge-target-bypasses-row-filters.md) | high | MERGE applies policy to its source but not to the target rows it updates or deletes | — |
| P1 | [503](2026-09-04-architecture-review/503-hive-insert-bypasses-policy-enforcer.md) | high | INSERT into a Hive external table bypasses the policy enforcer on its source SELECT and authorizes the target after the write | — |
| P1 | [510](2026-09-04-architecture-review/510-maintenance-authorization-is-role-name-heuristic.md) | medium | Maintenance procedure authorization is a role-name substring heuristic that allows sessions with no roles | — |
| P1 | [206](2026-09-04-architecture-review/206-ranger-policy-cache-key-omits-groups.md) | medium | Ranger resolved-policy cache key includes roles but not groups | — |
| P1 | [303](2026-09-04-architecture-review/303-tag-source-reads-stale-properties-from-any-token.md) | high | Column-tag masking reads table properties from any cached entry regardless of freshness | — |
| P1 | [211](2026-09-04-architecture-review/211-policy-negative-test-gaps.md) | medium | Policy and auth negative-case tests cover single scans and happy paths only | Ship with authorization fixes rather than waiting for a later testing phase. |
| P2 | [205](2026-09-04-architecture-review/205-show-stats-lists-restricted-columns.md) | medium | SHOW STATS lists restricted column names that information_schema hides | — |
| P2 | [212](2026-09-04-architecture-review/212-production-mode-does-not-require-a-policy-engine.md) | low | production_mode accepts `policy.engine = passthrough` and no grant backend | — |

## 3. External data access and credential isolation

Restrict outbound destinations and isolate credentials. Worker trust and transport changes are a joint design task.

| Priority | ID | Original severity | Finding | Coordination / condition |
|---|---|---|---|---|
| P1 | [900](2026-09-05-security-scale-review/900-http-allowlist-userinfo-bypass.md) | high | HTTP endpoint allowlist validates URL userinfo as the destination host | — |
| P1 | [604](2026-09-04-architecture-review/604-quack-query-tvf-is-an-ssrf-primitive-for-every-user.md) | high | quack_query() is registered in every session and lets any user make the coordinator POST to arbitrary URLs, blocking a runtime thread and materialising the reply unbounded | — |
| P1 | [901](2026-09-05-security-scale-review/901-shared-runtime-object-store-credentials.md) | high | Inline S3 stores overwrite a process-wide registry across sessions | — |
| P1 | [601](2026-09-04-architecture-review/601-single-shared-secret-flattens-worker-trust.md) | high | One symmetric worker secret authenticates every direction, so a compromised worker can harvest all users' S3 credentials | — |
| P1 | [401](2026-09-04-architecture-review/401-scan-tickets-carry-static-storage-credentials.md) | high | Every distributed scan ticket carries the coordinator's static S3 key pair; workers never see a user identity | Coordinate with 601/603; distinct credential scope, trust and transport controls. |
| P1 | [603](2026-09-04-architecture-review/603-worker-tls-is-server-side-only.md) | high | Worker TLS exists only on the listening side; no client in the cluster is configured to speak it | — |
| P1 | [600](2026-09-04-architecture-review/600-doexchange-shuffle-is-unreachable-but-exposed.md) | high | DoExchange shuffle subsystem is unreachable in production but exposed as a live worker endpoint | — |

## 4. Result caches and metadata consistency

Protect cached-result authorization and semantics first. Then align metadata/session invalidation and remove unnecessary global flushing.

| Priority | ID | Original severity | Finding | Coordination / condition |
|---|---|---|---|---|
| P1 | [902](2026-09-05-security-scale-review/902-result-cache-policy-revalidation.md) | high | Result-cache hits bypass current row filters, masks and local policy denials | Temporary mitigation for 902–904: disable result cache; verify latency impact. |
| P1 | [904](2026-09-05-security-scale-review/904-result-cache-session-context.md) | high | Result-cache keys omit default catalog/schema and stable principal identity | — |
| P1 | [903](2026-09-05-security-scale-review/903-result-cache-sql-literal-collisions.md) | high | Result-cache normalization changes SQL literal semantics | — |
| P1 | [302](2026-09-04-architecture-review/302-table-cache-invalidation-scope-inconsistent.md) | high | Table metadata cache invalidation is per-token for schema changes but per-table for data changes | — |
| P2 | [304](2026-09-04-architecture-review/304-table-cache-insert-after-invalidate-race.md) | medium | Table metadata cache has no version check, so an invalidated entry can be re-inserted stale | — |
| P2 | [507](2026-09-04-architecture-review/507-doput-ingest-skips-session-cache-invalidation.md) | medium | Flight DoPut ingest never invalidates the session context cache; invalidation lives in 20 call sites instead of at the commit | — |
| P2 | [407](2026-09-04-architecture-review/407-global-session-context-cache-flushed-on-every-dml.md) | medium | The process-global SessionContext cache is flushed for every user on every successful DML | — |

## 5. Memory safety and large-data execution

Bound attacker-controlled expansion, materialized results and cancellation lifetimes first. Tune scan/metadata efficiency after enforcing resource limits.

| Priority | ID | Original severity | Finding | Coordination / condition |
|---|---|---|---|---|
| P1 | [906](2026-09-05-security-scale-review/906-json-decompression-memory.md) | high | Buffered JSON cap checks compressed size but leaves decompression and decoding unbounded | — |
| P1 | [701](2026-09-04-architecture-review/701-trino-result-cache-materializes-everything-and-evicts-mid-page.md) | high | Trino result path materializes the full result as JSON and serves pages from a shared, evictable cache | — |
| P1 | [504](2026-09-04-architecture-review/504-mor-dml-paths-accumulate-untracked-buffers.md) | high | Merge-on-Read DML paths accumulate their whole output in untracked Vecs | — |
| P1 | [403](2026-09-04-architecture-review/403-cancel-and-timeout-do-not-reach-streams.md) | high | Flight SQL CancelQuery cannot cancel anything, and streamed queries have no wall-clock timeout | — |
| P1 | [300](2026-09-04-architecture-review/300-no-http-timeout-on-polaris-path.md) | high | No HTTP timeout on any Polaris request path | — |
| P1 | [301](2026-09-04-architecture-review/301-circuit-breaker-map-and-metric-cardinality-unbounded.md) | high | Per-token circuit breakers grow without bound and leak into Prometheus label cardinality | — |
| P2 | [905](2026-09-05-security-scale-review/905-partitioned-write-fanout.md) | high | Partitioned streaming writes are unbounded by default and bypass shared sink accounting | P1 if high-cardinality writes already run in production; extends 504, not a duplicate. |
| P2 | [408](2026-09-04-architecture-review/408-distribution-is-single-scan-offload.md) | medium | "Distributed execution" is single-scan offload; joins, aggregates and shuffles run on the coordinator, and the scheduler mixes units | P1 capacity/design gate if coordinator joins or aggregates already exceed one node. |
| P2 | [308](2026-09-04-architecture-review/308-manifest-walk-repeated-three-times-per-query.md) | medium | Every scan walks and clones all manifests up to three times per query with no per-snapshot statistics cache | — |
| P2 | [307](2026-09-04-architecture-review/307-metadata-plane-round-trip-amplification.md) | medium | Metadata-plane round trips multiply per query: per-lookup providers, HEAD on every cache hit, N+1 in information_schema.columns, full catalog rebuild per view | — |
| P2 | [806](2026-09-04-architecture-review/806-sync-catalog-hooks-block-runtime-and-hide-outages.md) | medium | Synchronous DataFusion catalog hooks do network IO through a block_on bridge and turn failures into empty results | Includes 306 listing behavior plus synchronous runtime blocking. |
| P2 | [509](2026-09-04-architecture-review/509-dml-per-batch-evaluators-leak-scratch-tables-and-plan-twice.md) | medium | Per-batch DML evaluators register fixed-name scratch tables in the shared session context, leak them on error, and plan each batch twice | — |
| P2 | [811](2026-09-04-architecture-review/811-std-mutex-lock-unwrap-across-await-in-trino-server.md) | low | `std::sync::Mutex::lock().unwrap()` in async request handlers, held across awaits | — |
| P2 | [512](2026-09-04-architecture-review/512-puffin-ndv-only-on-append-paths-and-unretried.md) | medium | Puffin NDV sidecars are emitted only by append-shaped writes; every rewrite drops statistics, and the statistics commit is not retried | — |

## 6. Writes, transactions and maintenance

Prevent destructive or incorrect commits before improving retry behavior and background scheduling.

| Priority | ID | Original severity | Finding | Coordination / condition |
|---|---|---|---|---|
| P1 | [500](2026-09-04-architecture-review/500-create-or-replace-ctas-drops-first-in-default-catalog.md) | high | CREATE OR REPLACE TABLE AS SELECT is drop-then-create and resolves the drop in the default catalog | — |
| P1 | [502](2026-09-04-architecture-review/502-partition-spec-evolution-blind-in-overwrite-and-cow-pruning.md) | high | Dynamic INSERT OVERWRITE and CoW partition pruning ignore the data file's partition spec id | — |
| P1 | [501](2026-09-04-architecture-review/501-merge-empty-source-skips-not-matched-by-source.md) | high | MERGE with an empty source returns before evaluating WHEN NOT MATCHED BY SOURCE | — |
| P2 | [506](2026-09-04-architecture-review/506-cow-merge-and-truncate-commits-have-no-conflict-retry.md) | medium | CoW MERGE and the MoR truncate fast paths commit once with no conflict retry; four copies of the CoW retry loop elsewhere | — |
| P2 | [505](2026-09-04-architecture-review/505-insert-wider-select-panics-in-apply-stamped-schema.md) | medium | INSERT never validates source arity against the target; a wider SELECT panics inside the streaming writer | — |
| P2 | [608](2026-09-04-architecture-review/608-compaction-endpoint-runs-detached-with-no-deadline-or-cleanup.md) | medium | compact_file_group runs as a detached task with no worker-side deadline, cancellation, or partial-upload cleanup | — |
| P2 | [511](2026-09-04-architecture-review/511-scheduler-tick-loads-every-table-runs-inline-and-grows-ledger.md) | medium | Scheduler tick loads every table in the catalog, runs compactions inline on the tick task, bursts missed ticks, and grows the ledger without bound | — |
| P3 | [514](2026-09-04-architecture-review/514-inconsistent-rows-affected-and-inflated-mor-delete-count.md) | low | DML no-op paths return an empty result instead of `rows_affected = 0`, and MoR position deletes over-count already-deleted rows | — |

## 7. Distributed scheduling and worker reliability

Make failure handling predictable before adding more distribution. Protocol tests must accompany retry and cancellation changes.

| Priority | ID | Original severity | Finding | Coordination / condition |
|---|---|---|---|---|
| P1 | [402](2026-09-04-architecture-review/402-distributed-fallbacks-hide-failures.md) | high | Distribution failures fall back to local execution silently, and mid-stream worker loss is not retried | — |
| P1 | [602](2026-09-04-architecture-review/602-worker-errors-have-no-taxonomy-user-faults-evict-workers.md) | high | Worker errors reach the coordinator as strings, so a user-caused failure evicts healthy workers and retries a task that cannot succeed | — |
| P2 | [609](2026-09-04-architecture-review/609-heartbeat-carries-only-a-url-no-version-capacity-or-generation.md) | medium | The heartbeat protocol carries only a URL, so the registry cannot detect version skew, capacity, or a restarted worker | — |
| P2 | [109](2026-09-04-architecture-review/109-scan-task-version-invariant-not-enforced.md) | medium | `ScanTask.version` is a hand-set integer; nothing ties it to the fields a worker will honour | — |
| P2 | [411](2026-09-04-architecture-review/411-no-integration-coverage-for-distributed-failure-paths.md) | medium | Worker loss, retry, straggler and cancel paths are covered only by in-process mocks | — |
| P2 | [611](2026-09-04-architecture-review/611-worker-integration-tests-cover-memory-only-not-the-protocol.md) | medium | Worker tests cover scan memory accounting and auth gates; nothing exercises registration, credential refresh, error propagation, or cancellation end to end | — |

## 8. SQL planning, protocol behavior and result correctness

Address user-triggered panics and silent wrong results, then consolidate planning semantics and prepared statements.

| Priority | ID | Original severity | Finding | Coordination / condition |
|---|---|---|---|---|
| P1 | [105](2026-09-04-architecture-review/105-unicode-uppercase-byte-slice-panics-in-sql-prescans.md) | high | Pre-parse keyword scans uppercase with Unicode rules then slice the original string by ASCII byte length; some user inputs panic | — |
| P1 | [703](2026-09-04-architecture-review/703-scalar-only-trino-udfs-return-null-for-every-row-on-column-input.md) | high | A dozen extended Trino UDFs only implement the scalar case and silently return NULL for every row when given a column | — |
| P1 | [700](2026-09-04-architecture-review/700-trino-dialect-pipeline-split-across-three-entry-points.md) | high | Trino dialect normalization is hand-assembled differently at three entry points | Coordinate with 106; retain Trino-specific correctness cases. |
| P2 | [605](2026-09-04-architecture-review/605-quack-wire-arrow-bridge-panics-on-hostile-input-no-fuzzing.md) | medium | sqe-quack-wire's arrow bridge panics on hostile DataChunks and the codec has no fuzz harness | — |
| P2 | [107](2026-09-04-architecture-review/107-classifier-prefix-dispatch-and-multi-statement-truncation.md) | medium | `parse_and_classify` is a keyword-prefix dispatcher ahead of the parser and silently drops every statement after the first | — |
| P2 | [704](2026-09-04-architecture-review/704-trino-metadata-postprocessing-keyed-on-sql-substring.md) | medium | Trino-path information_schema and EXPLAIN reshaping is triggered by SQL substring and applied to arbitrary user result sets | — |
| P2 | [708](2026-09-04-architecture-review/708-trino-session-statements-parsed-by-prefix-and-echoed-before-execution.md) | medium | Trino session-control and prepared-statement handling is prefix-string parsed, bypasses the SQL parser, and echoes state changes before the executor confirms them | — |
| P2 | [406](2026-09-04-architecture-review/406-prepared-statements-are-text-substitution.md) | medium | Flight SQL prepared statements are text substitution over a SQL-string handle; every query is planned twice | — |
| P2 | [404](2026-09-04-architecture-review/404-three-divergent-post-planning-pipelines.md) | medium | Three hand-rolled copies of the post-planning physical rule pipeline, run outside the optimizer with default config | — |
| P2 | [106](2026-09-04-architecture-review/106-pre-parse-pipeline-fragmented-per-protocol.md) | medium | The SQL rewrite chain is assembled differently by each entry point and re-parses the same text three to five times | Coordinate with 700; shared normalization pipeline, distinct entry-point coverage. |
| P2 | [312](2026-09-04-architecture-review/312-time-travel-declares-current-schema.md) | medium | Time-travel providers declare the current schema while the reader uses the snapshot schema | — |
| P2 | [311](2026-09-04-architecture-review/311-view-loading-ignores-current-version-and-hand-rolled-rest.md) | medium | View resolution picks the last version and first SQL representation, and view REST calls are hand-rolled JSON outside the breaker and metrics | — |
| P2 | [710](2026-09-04-architecture-review/710-trino-compat-has-no-wire-level-test-and-rewriters-lack-adversarial-inputs.md) | medium | The Trino compat layer has no wire-level test that runs anywhere, and the source-level rewriters are not tested against quoted identifiers, literals or comments | — |

## 9. Configuration, startup and deployment

Reject malformed security settings and close shipped-binary drift. Then standardize validation, deployment artifacts and runtime setup.

| Priority | ID | Original severity | Finding | Coordination / condition |
|---|---|---|---|---|
| P1 | [100](2026-09-04-architecture-review/100-typed-env-overrides-fail-open.md) | high | Malformed `SQE_*` environment overrides are ignored with a warning, so security knobs silently keep their file value | — |
| P1 | [801](2026-09-04-architecture-review/801-three-server-entry-points-drift.md) | high | Three server entry points with different features, argument syntax and runtime stacks | — |
| P2 | [101](2026-09-04-architecture-review/101-stringly-typed-mode-knobs-validated-at-use-site.md) | medium | Mode knobs are `String` fields parsed at the use site with silent fallbacks; `validate()` never sees them | — |
| P2 | [809](2026-09-04-architecture-review/809-deployment-artifact-drift.md) | medium | Deployment artifacts disagree with each other and with the binaries | — |
| P2 | [808](2026-09-04-architecture-review/808-stack-size-dependence-across-binaries-and-tests.md) | medium | Correctness depends on thread stack size, and the three runtimes pick three different sizes | — |
| P2 | [102](2026-09-04-architecture-review/102-hand-maintained-env-override-surface.md) | medium | The `SQE_*` env override surface is a 460-line hand-written list; unknown TOML keys are accepted silently | — |
| P2 | [802](2026-09-04-architecture-review/802-hand-maintained-env-override-allowlist.md) | medium | Environment overrides are a hand-maintained allowlist, and code reads env vars around it | Overlaps 102; implement one env/config surface change. |
| P3 | [711](2026-09-04-architecture-review/711-cli-embedded-mode-diverges-from-server-config-and-mutates-process-env.md) | low | sqe-cli embedded mode re-derives engine configuration by hand and mutates process environment after the runtime starts | — |
| P3 | [810](2026-09-04-architecture-review/810-documentation-drift-claude-md-and-readme.md) | low | CLAUDE.md and README describe an engine that no longer exists | — |

## 10. Errors, audit and observability

Preserve audit evidence and typed failure behavior. Specific auth/retry fixes should ship with their owning security or worker issues; the broad error migration follows.

| Priority | ID | Original severity | Finding | Coordination / condition |
|---|---|---|---|---|
| P2 | [607](2026-09-04-architecture-review/607-audit-hash-chain-resets-on-restart-and-writer-fails-open.md) | medium | Audit hash chain restarts at genesis on every boot, the writer queue is unbounded, and write failures drop events with no counter | — |
| P2 | [800](2026-09-04-architecture-review/800-error-classification-by-string-matching.md) | high | Client-facing error codes are derived by substring-matching error messages | — |
| P2 | [104](2026-09-04-architecture-review/104-error-classification-by-english-substring.md) | medium | Error codes are derived by substring-matching English messages; 739 call sites still build the untyped variants | Consolidate core error migration under 800. |
| P2 | [305](2026-09-04-architecture-review/305-error-classification-by-substring.md) | medium | Catalog error classification is substring matching on Display/Debug text | Catalog boundary of 800; retain catalog-specific acceptance criteria. |
| P2 | [709](2026-09-04-architecture-review/709-trino-error-classification-collapses-to-user-error-or-internal.md) | medium | Error classification on the Trino path collapses transport and auth failures to USER_ERROR and UDF argument errors to INTERNAL_ERROR | Trino boundary of 800; retain wire-code acceptance criteria. |
| P2 | [306](2026-09-04-architecture-review/306-listing-failures-degrade-to-empty-results.md) | medium | Metadata listing failures still degrade to empty results (P1-7 still holds) | Overlaps 806 for swallowed listing failures; preserve both acceptance criteria. |
| P2 | [812](2026-09-04-architecture-review/812-swallowed-errors-on-startup-and-write-paths.md) | low | Swallowed errors on startup, write-result and grant-backend paths | Mixed scope; split urgent startup/write behaviors from lower-impact diagnostics. |
| P2 | [410](2026-09-04-architecture-review/410-span-guards-across-await-and-unbounded-registries.md) | low | Query spans are entered across `.await`, and three coordinator registries only grow | — |
| P2 | [612](2026-09-04-architecture-review/612-observability-plumbing-shared-gauges-boot-panics-silent-metrics-bind.md) | low | Observability plumbing: concurrent streams overwrite shared gauges, telemetry init panics at boot, and a failed metrics bind is ignored | — |
| P3 | [610](2026-09-04-architecture-review/610-lineage-reports-masked-columns-as-identity-and-hides-incomplete-traces.md) | low | Column lineage reports policy-masked columns as identity passthrough and gives no signal when a plan node is untraced | — |

## 11. Test infrastructure, benchmarks and code structure

Enable useful integration coverage early and execute area-specific regression tests with each fix. Defer broad structural refactors until correctness and capacity risks are controlled.

| Priority | ID | Original severity | Finding | Coordination / condition |
|---|---|---|---|---|
| P1 | [807](2026-09-04-architecture-review/807-test-strategy-ignored-suites-are-the-only-integration-signal.md) | medium | The integration suites that protect the risky paths are `#[ignore]`d, undocumented, and have no CI runner | Enable regression CI alongside P1 fixes; does not replace issue-specific tests. |
| P2 | [513](2026-09-04-architecture-review/513-write-paths-only-tested-by-ignored-docker-e2e.md) | medium | Every committing write path is tested only by `#[ignore]` docker e2e; the in-process sqlite catalog rig exists but only maintenance uses it | — |
| P2 | [313](2026-09-04-architecture-review/313-test-gaps-cache-state-machine-and-backends.md) | medium | The REST cache state machine, statistics path and non-REST backends are untested outside ignored or docker-only tests | — |
| P2 | [707](2026-09-04-architecture-review/707-bench-rewrites-benchmark-sql-with-string-heuristics-instead-of-session-schema.md) | medium | The bench harness rewrites benchmark SQL with string heuristics to qualify table names instead of setting the session schema | — |
| P3 | [705](2026-09-04-architecture-review/705-three-trino-http-clients-with-divergent-bugs.md) | medium | sqe-cli and sqe-bench each hand-roll a Trino HTTP client and a Flight client, with divergent bugs | — |
| P3 | [706](2026-09-04-architecture-review/706-bench-binary-recompiles-library-modules-and-hides-logic-from-tests.md) | medium | sqe-bench declares its modules twice (bin and lib), so the binary carries a private copy of the generators and most bench logic is unreachable from tests | — |
| P3 | [409](2026-09-04-architecture-review/409-query-handler-god-object-and-duplicated-binaries.md) | medium | QueryHandler is a 9.9K-line God object with a 1,940-line `execute()`, wired twice by two coordinator binaries | — |
| P3 | [804](2026-09-04-architecture-review/804-coordinator-god-crate.md) | medium | sqe-coordinator is a God crate with an all-public module surface | Overlaps 409; use one coordinator decomposition plan. |
| P3 | [103](2026-09-04-architecture-review/103-config-monolith-and-sqe-core-scope.md) | medium | `config.rs` is a 9,900-line file and `sqe-core` carries runtime and authorization logic | — |
| P3 | [805](2026-09-04-architecture-review/805-config-model-monolith.md) | medium | One 9,900-line config file carries 39 config structs, two auth models and the production validator | Overlaps 103; implement one config decomposition. |
| P3 | [803](2026-09-04-architecture-review/803-crate-layering-catalog-depends-on-policy-and-metrics.md) | medium | Crate layering: catalog depends on policy, both depend on the telemetry crate, and wire types leak across protocol crates | Coordinate with 310; broader dependency-boundary scope. |
| P3 | [310](2026-09-04-architecture-review/310-catalog-crate-boundary-grab-bag.md) | medium | sqe-catalog carries non-catalog concerns: file TVFs, platform grant client, system.runtime mirrors, HTTP object stores | Coordinate with 803; overlapping catalog boundary scope. |
| P3 | [309](2026-09-04-architecture-review/309-scan-tuning-knobs-quadruplicated.md) | medium | Scan tuning knobs are copied through four structs with defaults repeated in each | — |
| P3 | [508](2026-09-04-architecture-review/508-dead-buffered-write-twins-and-two-stamping-paths.md) | medium | Dead buffered twins of CTAS/INSERT/ingest diverge from the live streaming paths; two field-stamping implementations | — |
| P3 | [108](2026-09-04-architecture-review/108-sqe-planner-dead-distributed-planning-code.md) | medium | More than half of `sqe-planner` is distributed-planning code no binary calls | — |

## Overlap and implementation bundling

These are consolidation candidates, not newly adjudicated duplicate closures. Keep every source issue traceable and preserve its acceptance criteria; 109 reports do not imply 109 independent fixes or pull requests.

| Workstream | Findings | Handling |
|---|---|---|
| Typed errors | [800](2026-09-04-architecture-review/800-error-classification-by-string-matching.md), [104](2026-09-04-architecture-review/104-error-classification-by-english-substring.md), [305](2026-09-04-architecture-review/305-error-classification-by-substring.md), [709](2026-09-04-architecture-review/709-trino-error-classification-collapses-to-user-error-or-internal.md) | 800 and 104 cover the same core migration; 305 and 709 retain catalog/Trino boundary requirements. Coordinate 602 worker eviction semantics separately. |
| Environment/config surface | [102](2026-09-04-architecture-review/102-hand-maintained-env-override-surface.md), [802](2026-09-04-architecture-review/802-hand-maintained-env-override-allowlist.md) | One implementation for overlapping env override inventories; fix 100 fail-open parsing first. |
| Config decomposition | [103](2026-09-04-architecture-review/103-config-monolith-and-sqe-core-scope.md), [805](2026-09-04-architecture-review/805-config-model-monolith.md) | One refactor; preserve both source records. |
| Coordinator decomposition | [409](2026-09-04-architecture-review/409-query-handler-god-object-and-duplicated-binaries.md), [804](2026-09-04-architecture-review/804-coordinator-god-crate.md) | One decomposition plan, coordinated with 801 bootstrap consolidation. |
| Catalog dependency boundaries | [310](2026-09-04-architecture-review/310-catalog-crate-boundary-grab-bag.md), [803](2026-09-04-architecture-review/803-crate-layering-catalog-depends-on-policy-and-metrics.md) | Overlapping architectural changes; 803 is broader than the catalog crate alone. |
| Catalog listing behavior | [306](2026-09-04-architecture-review/306-listing-failures-degrade-to-empty-results.md), [806](2026-09-04-architecture-review/806-sync-catalog-hooks-block-runtime-and-hide-outages.md) | Shared swallowed-error fix; 806 also requires runtime-blocking work. |
| SQL entry-point normalization | [106](2026-09-04-architecture-review/106-pre-parse-pipeline-fragmented-per-protocol.md), [700](2026-09-04-architecture-review/700-trino-dialect-pipeline-split-across-three-entry-points.md) | Shared pipeline implementation; preserve the Trino-specific cases and 404 physical-rule requirements. |
| Authentication rate limits | [209](2026-09-04-architecture-review/209-auth-rate-limit-placement-per-entry-point.md), [702](2026-09-04-architecture-review/702-trino-poll-routes-reauthenticate-and-bypass-rate-limiter.md) | Shared admission placement; repeated password grants remain a separate behavioral requirement. |
| Result-cache hardening | [902](2026-09-05-security-scale-review/902-result-cache-policy-revalidation.md), [904](2026-09-05-security-scale-review/904-result-cache-session-context.md), [903](2026-09-05-security-scale-review/903-result-cache-sql-literal-collisions.md) | Three distinct defects; one cache workstream and temporary disablement mitigation. |
| Worker trust and credentials | [601](2026-09-04-architecture-review/601-single-shared-secret-flattens-worker-trust.md), [401](2026-09-04-architecture-review/401-scan-tickets-carry-static-storage-credentials.md), [603](2026-09-04-architecture-review/603-worker-tls-is-server-side-only.md) | Coordinate identity/trust, scoped credentials and TLS end to end; none alone resolves all three. |
| Write memory | [504](2026-09-04-architecture-review/504-mor-dml-paths-accumulate-untracked-buffers.md), [905](2026-09-05-security-scale-review/905-partitioned-write-fanout.md) | Distinct MoR buffering and streaming fanout paths; shared memory-accounting approach. |
| Regression coverage | [807](2026-09-04-architecture-review/807-test-strategy-ignored-suites-are-the-only-integration-signal.md), [211](2026-09-04-architecture-review/211-policy-negative-test-gaps.md), [313](2026-09-04-architecture-review/313-test-gaps-cache-state-machine-and-backends.md), [411](2026-09-04-architecture-review/411-no-integration-coverage-for-distributed-failure-paths.md), [513](2026-09-04-architecture-review/513-write-paths-only-tested-by-ignored-docker-e2e.md), [611](2026-09-04-architecture-review/611-worker-integration-tests-cover-memory-only-not-the-protocol.md), [710](2026-09-04-architecture-review/710-trino-compat-has-no-wire-level-test-and-rewriters-lack-adversarial-inputs.md) | 807 supplies CI execution; specialist suites validate their owning fixes and retain distinct acceptance criteria. |

## Source reviews

- [September 4 architecture review — 102 findings](2026-09-04-architecture-review/README.md)
- [September 5 security and scale review — seven findings](2026-09-05-security-scale-review/README.md)

No implementation changes or production configuration changes were made as part of this reordering.
