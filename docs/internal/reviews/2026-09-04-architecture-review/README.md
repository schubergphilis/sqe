# Architecture review, 2026-09-04

> For the combined backlog of all 109 findings, grouped by area and priority, use the [combined prioritized index](../2026-09-05-combined-prioritized-findings.md). It takes precedence for scheduling; this review retains its original evidence and scope.

One file per issue, written so that another agent can pick a file up and act
on it without further context. Findings marked `fixed` carry a Resolution section; the rest are open.

## How to use

- Pick a file, read `Status`, set it to `in-progress` in a branch, do the work,
  set it to `done` with the MR number. One MR per issue unless two issues are
  the same change.
- Files are numbered by review scope so parallel reviewers did not collide:

| Range | Scope |
|---|---|
| 100 | sqe-core, sqe-sql, sqe-planner: config, errors, parsing, planning |
| 200 | sqe-auth, sqe-policy, sqe-mcp: authentication, authorization, MCP |
| 300 | sqe-catalog: Iceberg/Hive catalogs, information_schema, Puffin, mounts |
| 400 | sqe-coordinator, read side: sessions, Flight SQL, statement routing, query execution, distributed scheduling |
| 500 | sqe-coordinator, write side and maintenance: CTAS/INSERT/MERGE/DELETE, rewrite, compaction, maintenance scheduler; sqe-compaction, sqe-spill |
| 600 | sqe-worker, sqe-metrics, sqe-lineage, sqe-quack-* |
| 700 | sqe-bench, sqe-cli, sqe-trino-compat, sqe-trino-functions, xtask |
| 800 | Cross-cutting: dependency structure, error handling, async/blocking, panics, observability, configuration, testing strategy, deployment artifacts |

## Issue file format

```
# <number>: <one-line title>

- Scope: <crate or area>
- Severity: high | medium | low
- Category: architecture | reliability | security | performance | maintainability | testing | operability
- Effort: S (hours) | M (days) | L (week+)
- Status: open

## Problem

What is wrong, with `path:line` evidence. Quote the smallest snippet that shows it.

## Why it matters

The failure or cost this causes today or on the next change. Concrete.

## Proposed change

The direction, in enough detail to start. Name the files and types.

## Acceptance criteria

How the next agent knows it is done. Tests to add or commands to run.

## Related

Other issue files, existing plans under docs/internal/plans, nextsteps entries.
```

Severity means: high = correctness, data loss, security or an outage path;
medium = a real cost on every change or every deployment; low = debt worth
removing when nearby.

## Not in scope

Style, naming and formatting. Build and CI speed, which has its own plan at
`docs/internal/plans/2026-09-04-build-test-time-program.md`.

## Index

Sorted by severity, then number. 102 issues: 31 high, 62 medium, 9 low.

| # | Severity | Category | Effort | Scope | Title |
|---|---|---|---|---|---|
| [100](100-typed-env-overrides-fail-open.md) | high | security | S | sqe-core (config) | Malformed `SQE_*` environment overrides are ignored with a warning, so security knobs silently keep their file value |
| [105](105-unicode-uppercase-byte-slice-panics-in-sql-prescans.md) | high | reliability | S | sqe-sql (classifier | Pre-parse keyword scans uppercase with Unicode rules then slice the original string by ASCII byte length; some user inputs panic |
| [200](200-plan-rewriter-skips-subquery-scans.md) | high | security | S | sqe-policy (plan_rewriter) | Plan rewriter never visits TableScans inside subqueries |
| [201](201-policy-key-derivation-drifted-across-paths.md) | high | security | M | sqe-policy | Policy lookup key is derived in four places and they disagree on the namespace |
| [202](202-merge-target-bypasses-row-filters.md) | high | security | M | sqe-coordinator write path (write_handle | MERGE applies policy to its source but not to the target rows it updates or deletes |
| [203](203-mcp-oauth-scopes-trusted-from-unverified-payload.md) | high | security | S | sqe-mcp | MCP OAuth mode reads scopes from the raw JWT payload and only refuses the anonymous provider |
| [204](204-quack-session-drops-groups-identity.md) | high | security | S | sqe-coordinator (session_manager | Identity-to-Session conversion is copied five times and the Quack copy drops groups |
| [300](300-no-http-timeout-on-polaris-path.md) | high | reliability | S | sqe-catalog (rest_catalog.rs | No HTTP timeout on any Polaris request path |
| [301](301-circuit-breaker-map-and-metric-cardinality-unbounded.md) | high | operability | S | sqe-catalog (rest_catalog.rs) | Per-token circuit breakers grow without bound and leak into Prometheus label cardinality |
| [302](302-table-cache-invalidation-scope-inconsistent.md) | high | reliability | S | sqe-catalog (rest_catalog.rs) | Table metadata cache invalidation is per-token for schema changes but per-table for data changes |
| [303](303-tag-source-reads-stale-properties-from-any-token.md) | high | security | S | sqe-catalog (rest_catalog.rs TableMetada | Column-tag masking reads table properties from any cached entry regardless of freshness |
| [400](400-time-travel-aliases-bypass-policy.md) | high | security | M | sqe-coordinator | Time-travel, version and incremental reads register under `datafusion.public.*` and bypass the policy rewriter |
| [401](401-scan-tickets-carry-static-storage-credentials.md) | high | security | L | sqe-coordinator | Every distributed scan ticket carries the coordinator's static S3 key pair; workers never see a user identity |
| [402](402-distributed-fallbacks-hide-failures.md) | high | reliability | M | sqe-coordinator | Distribution failures fall back to local execution silently, and mid-stream worker loss is not retried |
| [403](403-cancel-and-timeout-do-not-reach-streams.md) | high | reliability | M | sqe-coordinator | Flight SQL CancelQuery cannot cancel anything, and streamed queries have no wall-clock timeout |
| [500](500-create-or-replace-ctas-drops-first-in-default-catalog.md) | high | reliability | M | sqe-coordinator write path (query_handle | CREATE OR REPLACE TABLE AS SELECT is drop-then-create and resolves the drop in the default catalog |
| [501](501-merge-empty-source-skips-not-matched-by-source.md) | high | reliability | S | sqe-coordinator write path (write_handle | MERGE with an empty source returns before evaluating WHEN NOT MATCHED BY SOURCE |
| [502](502-partition-spec-evolution-blind-in-overwrite-and-cow-pruning.md) | high | reliability | M | sqe-coordinator write path (write_handle | Dynamic INSERT OVERWRITE and CoW partition pruning ignore the data file's partition spec id |
| [503](503-hive-insert-bypasses-policy-enforcer.md) | high | security | S | sqe-coordinator write path (query_handle | INSERT into a Hive external table bypasses the policy enforcer on its source SELECT and authorizes the target after the write |
| [504](504-mor-dml-paths-accumulate-untracked-buffers.md) | high | reliability | M | sqe-coordinator write path (write_handle | Merge-on-Read DML paths accumulate their whole output in untracked Vecs |
| [600](600-doexchange-shuffle-is-unreachable-but-exposed.md) | high | architecture | M | sqe-worker (flight_service.rs | DoExchange shuffle subsystem is unreachable in production but exposed as a live worker endpoint |
| [601](601-single-shared-secret-flattens-worker-trust.md) | high | security | M | sqe-worker (flight_service.rs | One symmetric worker secret authenticates every direction, so a compromised worker can harvest all users' S3 credentials |
| [602](602-worker-errors-have-no-taxonomy-user-faults-evict-workers.md) | high | reliability | M | sqe-worker (flight_service.rs | Worker errors reach the coordinator as strings, so a user-caused failure evicts healthy workers and retries a task that cannot succeed |
| [603](603-worker-tls-is-server-side-only.md) | high | security | S | sqe-worker (tls.rs | Worker TLS exists only on the listening side; no client in the cluster is configured to speak it |
| [604](604-quack-query-tvf-is-an-ssrf-primitive-for-every-user.md) | high | security | S | sqe-quack-client (tvf.rs | quack_query() is registered in every session and lets any user make the coordinator POST to arbitrary URLs, blocking a runtime thread and materialising the reply unbounded |
| [700](700-trino-dialect-pipeline-split-across-three-entry-points.md) | high | architecture | M | sqe-trino-compat (server.rs) | Trino dialect normalization is hand-assembled differently at three entry points |
| [701](701-trino-result-cache-materializes-everything-and-evicts-mid-page.md) | high | reliability | L | sqe-trino-compat (server.rs | Trino result path materializes the full result as JSON and serves pages from a shared, evictable cache |
| [702](702-trino-poll-routes-reauthenticate-and-bypass-rate-limiter.md) | high | security | S | sqe-trino-compat (server.rs) | Trino poll and cancel routes re-run the OIDC password grant on every request and bypass the pre-auth rate limiter |
| [703](703-scalar-only-trino-udfs-return-null-for-every-row-on-column-input.md) | high | reliability | M | sqe-trino-functions (trino_functions_ext | A dozen extended Trino UDFs only implement the scalar case and silently return NULL for every row when given a column |
| [800](800-error-classification-by-string-matching.md) | high | reliability | L | sqe-core (error.rs) | Client-facing error codes are derived by substring-matching error messages |
| [801](801-three-server-entry-points-drift.md) | high | architecture | M | sqe-coordinator (`src/main.rs` | Three server entry points with different features, argument syntax and runtime stacks |
| [101](101-stringly-typed-mode-knobs-validated-at-use-site.md) | medium | reliability | M | sqe-core (config) | Mode knobs are `String` fields parsed at the use site with silent fallbacks; `validate()` never sees them |
| [102](102-hand-maintained-env-override-surface.md) | medium | maintainability | M | sqe-core (config) | The `SQE_*` env override surface is a 460-line hand-written list; unknown TOML keys are accepted silently |
| [103](103-config-monolith-and-sqe-core-scope.md) | medium | architecture | M | sqe-core | `config.rs` is a 9,900-line file and `sqe-core` carries runtime and authorization logic |
| [104](104-error-classification-by-english-substring.md) | medium | reliability | L | sqe-core (error) | Error codes are derived by substring-matching English messages; 739 call sites still build the untyped variants |
| [106](106-pre-parse-pipeline-fragmented-per-protocol.md) | medium | architecture | M | sqe-sql | The SQL rewrite chain is assembled differently by each entry point and re-parses the same text three to five times |
| [107](107-classifier-prefix-dispatch-and-multi-statement-truncation.md) | medium | reliability | M | sqe-sql (classifier) | `parse_and_classify` is a keyword-prefix dispatcher ahead of the parser and silently drops every statement after the first |
| [108](108-sqe-planner-dead-distributed-planning-code.md) | medium | maintainability | M | sqe-planner | More than half of `sqe-planner` is distributed-planning code no binary calls |
| [109](109-scan-task-version-invariant-not-enforced.md) | medium | reliability | S | sqe-planner (scan_task | `ScanTask.version` is a hand-set integer; nothing ties it to the fields a worker will honour |
| [205](205-show-stats-lists-restricted-columns.md) | medium | security | S | sqe-coordinator statement routing | SHOW STATS lists restricted column names that information_schema hides |
| [206](206-ranger-policy-cache-key-omits-groups.md) | medium | security | S | sqe-policy (ranger_store) | Ranger resolved-policy cache key includes roles but not groups |
| [207](207-bearer-token-validation-hardening.md) | medium | security | S | sqe-auth (bearer_token) | BearerTokenProvider leaves issuer, nbf and key-retirement to defaults |
| [208](208-token-material-outside-secretstring.md) | medium | security | S | sqe-auth (token_cache | Access and refresh tokens live in plain `String` in the legacy auth path |
| [209](209-auth-rate-limit-placement-per-entry-point.md) | medium | architecture | S | sqe-coordinator rate_limiter + mcp_serve | Pre-auth rate limiting is decided per entry point and the Trino bearer path has none |
| [210](210-tls-static-load-no-rotation-duplicated-builders.md) | medium | operability | M | sqe-coordinator tls.rs + mcp_server.rs | TLS material is read once at startup, in three separate builders, with no rotation or minimum version |
| [211](211-policy-negative-test-gaps.md) | medium | testing | M | sqe-policy tests | Policy and auth negative-case tests cover single scans and happy paths only |
| [304](304-table-cache-insert-after-invalidate-race.md) | medium | reliability | M | sqe-catalog (rest_catalog.rs TableMetada | Table metadata cache has no version check, so an invalidated entry can be re-inserted stale |
| [305](305-error-classification-by-substring.md) | medium | reliability | M | sqe-catalog (rest_catalog.rs) | Catalog error classification is substring matching on Display/Debug text |
| [306](306-listing-failures-degrade-to-empty-results.md) | medium | reliability | M | sqe-catalog (schema_provider.rs | Metadata listing failures still degrade to empty results (P1-7 still holds) |
| [307](307-metadata-plane-round-trip-amplification.md) | medium | performance | M | sqe-catalog (catalog_provider.rs | Metadata-plane round trips multiply per query: per-lookup providers, HEAD on every cache hit, N+1 in information_schema.columns, full catalog rebuild per view |
| [308](308-manifest-walk-repeated-three-times-per-query.md) | medium | performance | M | sqe-catalog (table_provider.rs | Every scan walks and clones all manifests up to three times per query with no per-snapshot statistics cache |
| [309](309-scan-tuning-knobs-quadruplicated.md) | medium | maintainability | S | sqe-catalog (catalog_provider.rs | Scan tuning knobs are copied through four structs with defaults repeated in each |
| [310](310-catalog-crate-boundary-grab-bag.md) | medium | architecture | L | sqe-catalog crate boundary | sqe-catalog carries non-catalog concerns: file TVFs, platform grant client, system.runtime mirrors, HTTP object stores |
| [311](311-view-loading-ignores-current-version-and-hand-rolled-rest.md) | medium | reliability | M | sqe-catalog (rest_catalog.rs view method | View resolution picks the last version and first SQL representation, and view REST calls are hand-rolled JSON outside the breaker and metrics |
| [312](312-time-travel-declares-current-schema.md) | medium | reliability | S | sqe-catalog (table_provider.rs | Time-travel providers declare the current schema while the reader uses the snapshot schema |
| [313](313-test-gaps-cache-state-machine-and-backends.md) | medium | testing | M | sqe-catalog tests | The REST cache state machine, statistics path and non-REST backends are untested outside ignored or docker-only tests |
| [404](404-three-divergent-post-planning-pipelines.md) | medium | architecture | M | sqe-coordinator | Three hand-rolled copies of the post-planning physical rule pipeline, run outside the optimizer with default config |
| [405](405-jwt-bearer-requests-mint-a-session-each.md) | medium | performance | S | sqe-coordinator | Every JWT-bearer Flight request mints a new Session and re-validates the token |
| [406](406-prepared-statements-are-text-substitution.md) | medium | architecture | M | sqe-coordinator | Flight SQL prepared statements are text substitution over a SQL-string handle; every query is planned twice |
| [407](407-global-session-context-cache-flushed-on-every-dml.md) | medium | performance | M | sqe-coordinator | The process-global SessionContext cache is flushed for every user on every successful DML |
| [408](408-distribution-is-single-scan-offload.md) | medium | architecture | L | sqe-coordinator | "Distributed execution" is single-scan offload; joins, aggregates and shuffles run on the coordinator, and the scheduler mixes units |
| [409](409-query-handler-god-object-and-duplicated-binaries.md) | medium | maintainability | L | sqe-coordinator (query_handler.rs | QueryHandler is a 9.9K-line God object with a 1,940-line `execute()`, wired twice by two coordinator binaries |
| [411](411-no-integration-coverage-for-distributed-failure-paths.md) | medium | testing | M | sqe-coordinator | Worker loss, retry, straggler and cancel paths are covered only by in-process mocks |
| [505](505-insert-wider-select-panics-in-apply-stamped-schema.md) | medium | reliability | S | sqe-coordinator write path (write_handle | INSERT never validates source arity against the target; a wider SELECT panics inside the streaming writer |
| [506](506-cow-merge-and-truncate-commits-have-no-conflict-retry.md) | medium | reliability | M | sqe-coordinator write path (write_handle | CoW MERGE and the MoR truncate fast paths commit once with no conflict retry; four copies of the CoW retry loop elsewhere |
| [507](507-doput-ingest-skips-session-cache-invalidation.md) | medium | architecture | S | sqe-coordinator write path (flight_sql D | Flight DoPut ingest never invalidates the session context cache; invalidation lives in 20 call sites instead of at the commit |
| [508](508-dead-buffered-write-twins-and-two-stamping-paths.md) | medium | maintainability | M | sqe-coordinator write path (write_handle | Dead buffered twins of CTAS/INSERT/ingest diverge from the live streaming paths; two field-stamping implementations |
| [509](509-dml-per-batch-evaluators-leak-scratch-tables-and-plan-twice.md) | medium | reliability | S | sqe-coordinator write path (write_handle | Per-batch DML evaluators register fixed-name scratch tables in the shared session context, leak them on error, and plan each batch twice |
| [510](510-maintenance-authorization-is-role-name-heuristic.md) | medium | security | S | sqe-coordinator maintenance (maintenance | Maintenance procedure authorization is a role-name substring heuristic that allows sessions with no roles |
| [511](511-scheduler-tick-loads-every-table-runs-inline-and-grows-ledger.md) | medium | operability | M | sqe-coordinator maintenance (maintenance | Scheduler tick loads every table in the catalog, runs compactions inline on the tick task, bursts missed ticks, and grows the ledger without bound |
| [512](512-puffin-ndv-only-on-append-paths-and-unretried.md) | medium | performance | M | sqe-coordinator write path and maintenan | Puffin NDV sidecars are emitted only by append-shaped writes; every rewrite drops statistics, and the statistics commit is not retried |
| [513](513-write-paths-only-tested-by-ignored-docker-e2e.md) | medium | testing | M | sqe-coordinator write path and maintenan | Every committing write path is tested only by `#[ignore]` docker e2e; the in-process sqlite catalog rig exists but only maintenance uses it |
| [605](605-quack-wire-arrow-bridge-panics-on-hostile-input-no-fuzzing.md) | medium | reliability | S | sqe-quack-wire (arrow_bridge.rs | sqe-quack-wire's arrow bridge panics on hostile DataChunks and the codec has no fuzz harness |
| [606](606-quack-server-session-is-a-bearer-uuid-with-no-expiry-or-result-cap.md) | medium | security | M | sqe-quack-server (app.rs | Quack server sessions are bearer UUIDs that outlive the token, results are fully materialised with no cap, and the policy-denial masking branch is dead |
| [607](607-audit-hash-chain-resets-on-restart-and-writer-fails-open.md) | medium | security | S | sqe-metrics (audit/logger.rs | Audit hash chain restarts at genesis on every boot, the writer queue is unbounded, and write failures drop events with no counter |
| [608](608-compaction-endpoint-runs-detached-with-no-deadline-or-cleanup.md) | medium | reliability | S | sqe-worker (flight_service.rs | compact_file_group runs as a detached task with no worker-side deadline, cancellation, or partial-upload cleanup |
| [609](609-heartbeat-carries-only-a-url-no-version-capacity-or-generation.md) | medium | architecture | M | sqe-worker (heartbeat.rs | The heartbeat protocol carries only a URL, so the registry cannot detect version skew, capacity, or a restarted worker |
| [611](611-worker-integration-tests-cover-memory-only-not-the-protocol.md) | medium | testing | M | sqe-worker (tests/ | Worker tests cover scan memory accounting and auth gates; nothing exercises registration, credential refresh, error propagation, or cancellation end to end |
| [704](704-trino-metadata-postprocessing-keyed-on-sql-substring.md) | medium | reliability | S | sqe-trino-compat (info_schema_compat.rs | Trino-path information_schema and EXPLAIN reshaping is triggered by SQL substring and applied to arbitrary user result sets |
| [705](705-three-trino-http-clients-with-divergent-bugs.md) | medium | maintainability | M | sqe-cli (http.rs | sqe-cli and sqe-bench each hand-roll a Trino HTTP client and a Flight client, with divergent bugs |
| [706](706-bench-binary-recompiles-library-modules-and-hides-logic-from-tests.md) | medium | maintainability | S | sqe-bench (main.rs | sqe-bench declares its modules twice (bin and lib), so the binary carries a private copy of the generators and most bench logic is unreachable from tests |
| [707](707-bench-rewrites-benchmark-sql-with-string-heuristics-instead-of-session-schema.md) | medium | testing | S | sqe-bench (query.rs | The bench harness rewrites benchmark SQL with string heuristics to qualify table names instead of setting the session schema |
| [708](708-trino-session-statements-parsed-by-prefix-and-echoed-before-execution.md) | medium | reliability | M | sqe-trino-compat (protocol.rs | Trino session-control and prepared-statement handling is prefix-string parsed, bypasses the SQL parser, and echoes state changes before the executor confirms them |
| [709](709-trino-error-classification-collapses-to-user-error-or-internal.md) | medium | operability | S | sqe-trino-compat (server.rs | Error classification on the Trino path collapses transport and auth failures to USER_ERROR and UDF argument errors to INTERNAL_ERROR |
| [710](710-trino-compat-has-no-wire-level-test-and-rewriters-lack-adversarial-inputs.md) | medium | testing | M | sqe-trino-compat | The Trino compat layer has no wire-level test that runs anywhere, and the source-level rewriters are not tested against quoted identifiers, literals or comments |
| [802](802-hand-maintained-env-override-allowlist.md) | medium | operability | M | sqe-core (`config.rs`) | Environment overrides are a hand-maintained allowlist, and code reads env vars around it |
| [803](803-crate-layering-catalog-depends-on-policy-and-metrics.md) | medium | architecture | M | sqe-catalog | Crate layering: catalog depends on policy, both depend on the telemetry crate, and wire types leak across protocol crates |
| [804](804-coordinator-god-crate.md) | medium | maintainability | L | sqe-coordinator | sqe-coordinator is a God crate with an all-public module surface |
| [805](805-config-model-monolith.md) | medium | maintainability | M | sqe-core (`config.rs`) | One 9,900-line config file carries 39 config structs, two auth models and the production validator |
| [806](806-sync-catalog-hooks-block-runtime-and-hide-outages.md) | medium | reliability | M | sqe-catalog (`runtime_bridge.rs` | Synchronous DataFusion catalog hooks do network IO through a block_on bridge and turn failures into empty results |
| [807](807-test-strategy-ignored-suites-are-the-only-integration-signal.md) | medium | testing | M | workspace tests | The integration suites that protect the risky paths are `#[ignore]`d, undocumented, and have no CI runner |
| [808](808-stack-size-dependence-across-binaries-and-tests.md) | medium | reliability | M | sqe-coordinator (`main.rs` | Correctness depends on thread stack size, and the three runtimes pick three different sizes |
| [809](809-deployment-artifact-drift.md) | medium | operability | M | Dockerfile | Deployment artifacts disagree with each other and with the binaries |
| [212](212-production-mode-does-not-require-a-policy-engine.md) | low | security | S | sqe-core config validation | production_mode accepts `policy.engine = passthrough` and no grant backend |
| [410](410-span-guards-across-await-and-unbounded-registries.md) | low | operability | S | sqe-coordinator | Query spans are entered across `.await`, and three coordinator registries only grow |
| [514](514-inconsistent-rows-affected-and-inflated-mor-delete-count.md) | low | reliability | S | sqe-coordinator write path (write_handle | DML no-op paths return an empty result instead of `rows_affected = 0`, and MoR position deletes over-count already-deleted rows |
| [610](610-lineage-reports-masked-columns-as-identity-and-hides-incomplete-traces.md) | low | architecture | S | sqe-lineage (extract/columns.rs | Column lineage reports policy-masked columns as identity passthrough and gives no signal when a plan node is untraced |
| [612](612-observability-plumbing-shared-gauges-boot-panics-silent-metrics-bind.md) | low | operability | S | sqe-metrics (otel.rs | Observability plumbing: concurrent streams overwrite shared gauges, telemetry init panics at boot, and a failed metrics bind is ignored |
| [711](711-cli-embedded-mode-diverges-from-server-config-and-mutates-process-env.md) | low | maintainability | S | sqe-cli (main.rs | sqe-cli embedded mode re-derives engine configuration by hand and mutates process environment after the runtime starts |
| [810](810-documentation-drift-claude-md-and-readme.md) | low | maintainability | S | CLAUDE.md | CLAUDE.md and README describe an engine that no longer exists |
| [811](811-std-mutex-lock-unwrap-across-await-in-trino-server.md) | low | reliability | S | sqe-trino-compat (`server.rs`) | `std::sync::Mutex::lock().unwrap()` in async request handlers, held across awaits |
| [812](812-swallowed-errors-on-startup-and-write-paths.md) | low | reliability | S | sqe-coordinator | Swallowed errors on startup, write-result and grant-backend paths |
