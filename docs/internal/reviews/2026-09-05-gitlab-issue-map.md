# GitLab Issue Mapping — 2026-09-05 Architecture and Security Reviews

Every finding from the September 4 architecture review and September 5 security/scale review has been assigned a GitLab issue number. MRs that address these findings should reference the issue using `Closes #n` or `Refs #n` in the commit message or PR description, enabling automatic linking and tracking.

| Finding | Issue | Title |
|---|---|---|
| 100 | #543 | Malformed `SQE_*` environment overrides are ignored with a warning, so security knobs silently keep their file value |
| 101 | #544 | Mode knobs are `String` fields parsed at the use site with silent fallbacks; `validate()` never sees them |
| 102 | #545 | The `SQE_*` env override surface is a 460-line hand-written list; unknown TOML keys are accepted silently |
| 103 | #546 | `config.rs` is a 9,900-line file and `sqe-core` carries runtime and authorization logic |
| 104 | #547 | Error codes are derived by substring-matching English messages; 739 call sites still build the untyped variants |
| 105 | #548 | Pre-parse keyword scans uppercase with Unicode rules then slice the original string by ASCII byte length; some user inputs panic |
| 106 | #549 | The SQL rewrite chain is assembled differently by each entry point and re-parses the same text three to five times |
| 107 | #550 | `parse_and_classify` is a keyword-prefix dispatcher ahead of the parser and silently drops every statement after the first |
| 108 | #551 | More than half of `sqe-planner` is distributed-planning code no binary calls |
| 109 | #552 | `ScanTask.version` is a hand-set integer; nothing ties it to the fields a worker will honour |
| 200 | #553 | Plan rewriter never visits TableScans inside subqueries |
| 201 | #554 | Policy lookup key is derived in four places and they disagree on the namespace |
| 202 | #555 | MERGE applies policy to its source but not to the target rows it updates or deletes |
| 203 | #556 | MCP OAuth mode reads scopes from the raw JWT payload and only refuses the anonymous provider |
| 204 | #557 | Identity-to-Session conversion is copied five times and the Quack copy drops groups |
| 205 | #558 | SHOW STATS lists restricted column names that information_schema hides |
| 206 | #559 | Ranger resolved-policy cache key includes roles but not groups |
| 207 | #560 | BearerTokenProvider leaves issuer, nbf and key-retirement to defaults |
| 208 | #561 | Access and refresh tokens live in plain `String` in the legacy auth path |
| 209 | #562 | Pre-auth rate limiting is decided per entry point and the Trino bearer path has none |
| 210 | #563 | TLS material is read once at startup, in three separate builders, with no rotation or minimum version |
| 211 | #564 | Policy and auth negative-case tests cover single scans and happy paths only |
| 212 | #565 | production_mode accepts `policy.engine = passthrough` and no grant backend |
| 300 | #566 | No HTTP timeout on any Polaris request path |
| 301 | #567 | Per-token circuit breakers grow without bound and leak into Prometheus label cardinality |
| 302 | #568 | Table metadata cache invalidation is per-token for schema changes but per-table for data changes |
| 303 | #569 | Column-tag masking reads table properties from any cached entry regardless of freshness |
| 304 | #570 | Table metadata cache has no version check, so an invalidated entry can be re-inserted stale |
| 305 | #571 | Catalog error classification is substring matching on Display/Debug text |
| 306 | #572 | Metadata listing failures still degrade to empty results (P1-7 still holds) |
| 307 | #573 | Metadata-plane round trips multiply per query: per-lookup providers, HEAD on every cache hit, N+1 in information_schema.columns, full catalog rebuild per view |
| 308 | #574 | Every scan walks and clones all manifests up to three times per query with no per-snapshot statistics cache |
| 309 | #575 | Scan tuning knobs are copied through four structs with defaults repeated in each |
| 310 | #576 | sqe-catalog carries non-catalog concerns: file TVFs, platform grant client, system.runtime mirrors, HTTP object stores |
| 311 | #577 | View resolution picks the last version and first SQL representation, and view REST calls are hand-rolled JSON outside the breaker and metrics |
| 312 | #578 | Time-travel providers declare the current schema while the reader uses the snapshot schema |
| 313 | #579 | The REST cache state machine, statistics path and non-REST backends are untested outside ignored or docker-only tests |
| 400 | #580 | Time-travel, version and incremental reads register under `datafusion.public.*` and bypass the policy rewriter |
| 401 | #581 | Every distributed scan ticket carries the coordinator's static S3 key pair; workers never see a user identity |
| 402 | #582 | Distribution failures fall back to local execution silently, and mid-stream worker loss is not retried |
| 403 | #583 | Flight SQL CancelQuery cannot cancel anything, and streamed queries have no wall-clock timeout |
| 404 | #584 | Three hand-rolled copies of the post-planning physical rule pipeline, run outside the optimizer with default config |
| 405 | #585 | Every JWT-bearer Flight request mints a new Session and re-validates the token |
| 406 | #586 | Flight SQL prepared statements are text substitution over a SQL-string handle; every query is planned twice |
| 407 | #587 | The process-global SessionContext cache is flushed for every user on every successful DML |
| 408 | #588 | "Distributed execution" is single-scan offload; joins, aggregates and shuffles run on the coordinator, and the scheduler mixes units |
| 409 | #589 | QueryHandler is a 9.9K-line God object with a 1,940-line `execute()`, wired twice by two coordinator binaries |
| 410 | #590 | Query spans are entered across `.await`, and three coordinator registries only grow |
| 411 | #591 | Worker loss, retry, straggler and cancel paths are covered only by in-process mocks |
| 500 | #592 | CREATE OR REPLACE TABLE AS SELECT is drop-then-create and resolves the drop in the default catalog |
| 501 | #593 | MERGE with an empty source returns before evaluating WHEN NOT MATCHED BY SOURCE |
| 502 | #594 | Dynamic INSERT OVERWRITE and CoW partition pruning ignore the data file's partition spec id |
| 503 | #595 | INSERT into a Hive external table bypasses the policy enforcer on its source SELECT and authorizes the target after the write |
| 504 | #596 | Merge-on-Read DML paths accumulate their whole output in untracked Vecs |
| 505 | #597 | INSERT never validates source arity against the target; a wider SELECT panics inside the streaming writer |
| 506 | #598 | CoW MERGE and the MoR truncate fast paths commit once with no conflict retry; four copies of the CoW retry loop elsewhere |
| 507 | #599 | Flight DoPut ingest never invalidates the session context cache; invalidation lives in 20 call sites instead of at the commit |
| 508 | #600 | Dead buffered twins of CTAS/INSERT/ingest diverge from the live streaming paths; two field-stamping implementations |
| 509 | #601 | Per-batch DML evaluators register fixed-name scratch tables in the shared session context, leak them on error, and plan each batch twice |
| 510 | #602 | Maintenance procedure authorization is a role-name substring heuristic that allows sessions with no roles |
| 511 | #603 | Scheduler tick loads every table in the catalog, runs compactions inline on the tick task, bursts missed ticks, and grows the ledger without bound |
| 512 | #604 | Puffin NDV sidecars are emitted only by append-shaped writes; every rewrite drops statistics, and the statistics commit is not retried |
| 513 | #605 | Every committing write path is tested only by `#[ignore]` docker e2e; the in-process sqlite catalog rig exists but only maintenance uses it |
| 514 | #606 | DML no-op paths return an empty result instead of `rows_affected = 0`, and MoR position deletes over-count already-deleted rows |
| 600 | #607 | DoExchange shuffle subsystem is unreachable in production but exposed as a live worker endpoint |
| 601 | #608 | One symmetric worker secret authenticates every direction, so a compromised worker can harvest all users' S3 credentials |
| 602 | #609 | Worker errors reach the coordinator as strings, so a user-caused failure evicts healthy workers and retries a task that cannot succeed |
| 603 | #610 | Worker TLS exists only on the listening side; no client in the cluster is configured to speak it |
| 604 | #611 | quack_query() is registered in every session and lets any user make the coordinator POST to arbitrary URLs, blocking a runtime thread and materialising the reply unbounded |
| 605 | #612 | sqe-quack-wire's arrow bridge panics on hostile DataChunks and the codec has no fuzz harness |
| 606 | #613 | Quack server sessions are bearer UUIDs that outlive the token, results are fully materialised with no cap, and the policy-denial masking branch is dead |
| 607 | #614 | Audit hash chain restarts at genesis on every boot, the writer queue is unbounded, and write failures drop events with no counter |
| 608 | #615 | compact_file_group runs as a detached task with no worker-side deadline, cancellation, or partial-upload cleanup |
| 609 | #616 | The heartbeat protocol carries only a URL, so the registry cannot detect version skew, capacity, or a restarted worker |
| 610 | #617 | Column lineage reports policy-masked columns as identity passthrough and gives no signal when a plan node is untraced |
| 611 | #618 | Worker tests cover scan memory accounting and auth gates; nothing exercises registration, credential refresh, error propagation, or cancellation end to end |
| 612 | #619 | Observability plumbing: concurrent streams overwrite shared gauges, telemetry init panics at boot, and a failed metrics bind is ignored |
| 700 | #620 | Trino dialect normalization is hand-assembled differently at three entry points |
| 701 | #621 | Trino result path materializes the full result as JSON and serves pages from a shared, evictable cache |
| 702 | #622 | Trino poll and cancel routes re-run the OIDC password grant on every request and bypass the pre-auth rate limiter |
| 703 | #623 | A dozen extended Trino UDFs only implement the scalar case and silently return NULL for every row when given a column |
| 704 | #624 | Trino-path information_schema and EXPLAIN reshaping is triggered by SQL substring and applied to arbitrary user result sets |
| 705 | #625 | sqe-cli and sqe-bench each hand-roll a Trino HTTP client and a Flight client, with divergent bugs |
| 706 | #626 | sqe-bench declares its modules twice (bin and lib), so the binary carries a private copy of the generators and most bench logic is unreachable from tests |
| 707 | #627 | The bench harness rewrites benchmark SQL with string heuristics to qualify table names instead of setting the session schema |
| 708 | #628 | Trino session-control and prepared-statement handling is prefix-string parsed, bypasses the SQL parser, and echoes state changes before the executor confirms them |
| 709 | #629 | Error classification on the Trino path collapses transport and auth failures to USER_ERROR and UDF argument errors to INTERNAL_ERROR |
| 710 | #630 | The Trino compat layer has no wire-level test that runs anywhere, and the source-level rewriters are not tested against quoted identifiers, literals or comments |
| 711 | #631 | sqe-cli embedded mode re-derives engine configuration by hand and mutates process environment after the runtime starts |
| 800 | #632 | Client-facing error codes are derived by substring-matching error messages |
| 801 | #633 | Three server entry points with different features, argument syntax and runtime stacks |
| 802 | #634 | Environment overrides are a hand-maintained allowlist, and code reads env vars around it |
| 803 | #635 | Crate layering: catalog depends on policy, both depend on the telemetry crate, and wire types leak across protocol crates |
| 804 | #636 | sqe-coordinator is a God crate with an all-public module surface |
| 805 | #637 | One 9,900-line config file carries 39 config structs, two auth models and the production validator |
| 806 | #638 | Synchronous DataFusion catalog hooks do network IO through a block_on bridge and turn failures into empty results |
| 807 | #639 | The integration suites that protect the risky paths are `#[ignore]`d, undocumented, and have no CI runner |
| 808 | #640 | Correctness depends on thread stack size, and the three runtimes pick three different sizes |
| 809 | #641 | Deployment artifacts disagree with each other and with the binaries |
| 810 | #642 | CLAUDE.md and README describe an engine that no longer exists |
| 811 | #643 | `std::sync::Mutex::lock().unwrap()` in async request handlers, held across awaits |
| 812 | #644 | Swallowed errors on startup, write-result and grant-backend paths |
| 900 | #645 | HTTP endpoint allowlist validates URL userinfo as the destination host |
| 901 | #646 | Inline S3 stores overwrite a process-wide registry across sessions |
| 902 | #647 | Result-cache hits bypass current row filters, masks and local policy denials |
| 903 | #648 | Result-cache normalization changes SQL literal semantics |
| 904 | #649 | Result-cache keys omit default catalog/schema and stable principal identity |
| 905 | #650 | Partitioned streaming writes are unbounded by default and bypass shared sink accounting |
| 906 | #651 | Buffered JSON cap checks compressed size but leaves decompression and decoding unbounded |

## Work packages

Eight umbrella issues (label `work-package`) group the 109 findings by concern so one owner can take a whole package and ship it as a few themed MRs. Each package lists its children as a task list in index scheduling order and links them as related issues. Every finding appears in exactly one package.

| Work package | Issue | Tickets | Findings |
|---|---|---:|---|
| Identity, credentials and transport trust | #652 | 16 | 203, 204, 207, 606, 702, 209, 405, 210, 208, 900, 604, 901, 601, 401, 603, 600 |
| Authorization, governance and policy enforcement | #653 | 11 | 200, 201, 400, 202, 503, 510, 206, 303, 211, 205, 212 |
| Data consistency: caches, writes and transactions | #654 | 15 | 902, 904, 903, 302, 304, 507, 407, 500, 502, 501, 506, 505, 608, 511, 514 |
| Memory safety and large-data execution | #655 | 14 | 906, 701, 504, 403, 300, 301, 905, 408, 308, 307, 806, 509, 811, 512 |
| SQL planning, protocol behavior and result correctness | #656 | 13 | 105, 703, 700, 605, 107, 704, 708, 406, 404, 106, 312, 311, 710 |
| Errors, audit and observability | #657 | 10 | 607, 800, 104, 305, 709, 306, 812, 410, 612, 610 |
| Distributed worker reliability and test infrastructure | #658 | 12 | 402, 602, 609, 109, 411, 611, 807, 513, 313, 707, 705, 706 |
| Configuration, startup and code structure | #659 | 18 | 100, 801, 101, 809, 808, 102, 802, 711, 810, 409, 804, 103, 805, 803, 310, 309, 508, 108 |
