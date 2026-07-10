# Changelog

All notable changes to SQE are documented in this file.

## [Unreleased]

### Added

- Per-caller namespace visibility in metadata listings

    `SHOW SCHEMAS`, `information_schema.schemata`, and Flight SQL
    `GetDbSchemas` no longer leak the NAMES of namespaces the caller holds
    no grants in. On the REST/Polaris backend, each namespace returned by
    `listNamespaces` is probed once per session-catalog build with the
    caller's bearer (Polaris `LOAD_NAMESPACE_METADATA`, answered per caller
    by the OPA bridge); a 403 drops the name from every metadata surface.
    Any other probe failure fails open and keeps the name — namespace
    contents remain protected by the per-operation checks regardless.
    `information_schema` now derives its schema list from the same filtered
    cache as `SHOW SCHEMAS`, removing the second unfiltered listing path.
    Single-identity backends (Glue/HMS/JDBC/Hadoop) skip the filter; the
    `[catalog] namespace_visibility_filter = false` flag restores the old
    behavior. Probes run 8-at-a-time, once per session, never per query.

### Removed

- Wind down the Apache Ballista distributed-execution integration

    Ballista 53 was evaluated as an opt-in distributed engine behind a
    `[query] engine = "ballista"` flag, with the bespoke layer staying the
    default. It reached correctness parity on the common path (TPC-H 22/22)
    but was ~2.2x slower where it completed, could not finish the TPC-DS
    analytical core (an upstream datafusion-proto aggregate-serialization
    assertion plus Ballista evicting an executor on a task-level error), and
    its scheduler is less capable than SQE's `WeightedScheduler` (Ballista 53
    dropped consistent-hash affinity, has no scan locality, no straggler
    handling). The `sqe-ballista` crate, the `QueryEngine` enum, the
    `[query] engine` config field, and all coordinator/worker integration
    wiring are removed. Bespoke distributed execution is the only engine.

    The ADBC unpadded-base64 Flight SQL Basic-auth handshake fix found during
    this work is kept (it fixes dbt-sqe / Go ADBC driver connectivity
    independent of the engine). Decision and borrowable findings:
    `docs/ballista-evaluation-learnings.md`. Historical detail:
    `docs/archive/ballista-evaluation/`.

### Bug Fixes

- Flight SQL accepts JWT bearer tokens via the configured provider chain

    Before this fix the coordinator built `SessionManager::new(authenticator)`
    in `main.rs` / `sqe_server.rs`, which gave Flight SQL only the legacy
    OIDC password-grant `Authenticator`. Bearer-only credentials (the
    common case for backend-bridged JWTs from a frontend BFF) returned
    `NotMyCredentials`, which surfaced to clients as
    `Authentication failed: credentials not handled by this provider`
    even when `[[auth.providers]]` declared a `bearer_token` provider
    that the Trino-compat HTTP path accepted on the same JWT.

    The fix builds the auth chain via `build_auth_chain` once and routes
    it into both endpoints. Flight SQL now goes through the same chain as
    Trino HTTP. Adds `SessionManager::with_provider_and_legacy(provider,
    authenticator)` so the chain handles new requests while the legacy
    `Authenticator`'s background refresh task keeps existing
    username/password sessions current.

    Workaround for older builds: put `bearer_token` first in the chain.
    Once this fix is rolled out, ordering no longer matters.

- Remove rsa crate. Static test keypair eliminates RUSTSEC-2023-0071

Replace runtime RSA key generation in bearer_token tests with a
    pre-generated static PEM keypair. The rsa and pkcs1 dev-dependencies
    are removed entirely, eliminating the Marvin Attack advisory.
- Remove unused base64::Engine import in bearer_token tests

Caught by cargo clippy during functional audit. The import was
    left over after removing the rsa crate (Task 4).

### Documentation

- Add OSS release + security audit implementation plan

17-task plan covering Spec A (OSS Release Readiness) and Spec B
    (Security & Functional Audit) from the design spec.
- Add CONTRIBUTING.md

Covers commit format, testing requirements, code review process,
    and license agreement for contributors.
- Add AUDIT.md — security and functional audit report

Structured review of auth passthrough, error sanitization, token
    validation, TLS, config secrets, query cancellation, and dependency
    security. 1,218 tests passing, all advisory checks clean.

    Notable findings:
    - rsa crate removed (RUSTSEC-2023-0071 eliminated)
    - JWT audience validation silently disabled when unconfigured
    - Session file persistence writes raw tokens (opt-in, not default)
    - Example config has MinIO defaults and ssl_verification=false
    No critical vulnerabilities found.

### Miscellaneous

- Add Apache 2.0 LICENSE file

Standard Apache License 2.0 at repo root. No per-file headers —
    repo-level LICENSE is sufficient for new projects.
- Add deny.toml for cargo-deny advisory checking

Ignores RUSTSEC-2024-0436 (paste crate, unmaintained but
    feature-complete proc macro, transitive from arrow/datafusion).
- Add retro-tagging script for 44 historical MRs

Maps each merged MR to a semver tag via glab API. Idempotent,
    supports --dry-run and --push flags. Creates annotated tags on
    the merge commit SHAs.
- Add cliff.toml for git-cliff changelog generation

Conventional commit grouping (feat/fix/docs/chore/perf/refactor).
    CHANGELOG.md generated after retro-tagging historical MRs.
- Bump all crate versions from 0.1.0 to 0.15.0

Aligns Cargo.toml versions with the actual release state after
    44 merged MRs and 493 commits.

### Security

- Add clippy, cargo-audit, cargo-deny to MR pipeline

New 'check' stage runs before tests with:
    - cargo clippy (strict, -D warnings)
    - cargo audit (security advisories)
    - cargo deny check advisories (with deny.toml policy)

### Ci

- Add release pipeline triggered on tag push

On v* tag push: generates release notes via git-cliff, creates
    GitLab release with the notes. Reuses existing test + build stages.

## [0.28.0] — 2026-04-08

### Documentation

- Cherry-pick scheduling evolution design spec before branch cleanup

Preserves the cost-aware, bin-packing, and adaptive scheduling design
    spec from the docs/scheduling-design branch — the only unmerged content
    not already on main.
- Add implementation plan for dbt-sqe adapter and ALTER TABLE schema evolution
- Mark dbt-sqe adapter and ALTER TABLE schema evolution as done

Update nextsteps.md and README.md roadmap. Fix minor unused import
    warnings in dbt adapter Python files.

### Features

- **sql:** Classify ALTER TABLE ADD/DROP/RENAME/ALTER COLUMN as AlterSchema

Add AlterSchema(Box<Statement>) variant to StatementKind for routing schema
    evolution operations (ADD COLUMN, DROP COLUMN, RENAME COLUMN, ALTER COLUMN)
    to a dedicated handler, distinct from RENAME TO (→ Rename) and unknown ops
    (→ Utility). Includes 6 new tests and updates 2 existing utility-expectation
    tests to the new variant.
- **coordinator:** Route AlterSchema to catalog_ops

Wire the AlterSchema StatementKind variant (added in A1) to
    catalog_ops.alter_table_schema so that ADD/DROP/RENAME COLUMN and
    ALTER COLUMN statements are dispatched to the new handler.
- **coordinator:** Implement ALTER TABLE schema evolution

Add CatalogOps::alter_table_schema that handles ADD COLUMN, DROP COLUMN,
    RENAME COLUMN, and ALTER COLUMN (SET/DROP NOT NULL, SET DATA TYPE) against
    Iceberg tables via the Polaris REST catalog.

    Implementation notes:
    - sql_type_to_arrow is made pub(crate) so catalog_ops can reuse it
    - Schema fields are loaded, mutated in-memory, then a new Schema is built
    - Commit goes through SessionCatalog::commit_schema_update (new method) which
      makes a direct REST POST rather than using TableCommit::builder().build(),
      whose build() is pub(crate) in the upstream iceberg crate and thus inaccessible
    - TableUpdate and TableRequirement are Serialize/Deserialize so they can be
      sent as JSON in the commit payload
- **dbt:** Scaffold dbt-sqe adapter package with connection manager and adapter classes

Adds the full dbt-sqe Python package under adapters/dbt-sqe/ — namespace packages,
    setup.cfg, SQECredentials/SQEConnectionManager (ADBC Flight SQL), SQEAdapter,
    SQEColumn, SQERelation, and include scaffolding (dbt_project.yml, sample_profiles.yml).
- **dbt:** Add SQL macros for metadata, DDL, materializations, and catalog

Add Jinja SQL macros for the dbt-sqe adapter covering metadata discovery
    (list_relations, get_columns, list_schemas, check_schema_exists), DDL helpers
    (create_table_as, create_view_as, drop/rename/create/drop schema),
    materializations (table, view, incremental with append/delete+insert/merge,
    seed), and catalog introspection. Also fix minor import/unused-import lint
    issues in connections.py, relation.py, and __init__.py.

### Miscellaneous

- Docs cleanup, archive completed work, add dbt+schema evolution spec

- Update nextsteps.md, README.md, features.md, roadmap.md to reflect
      current state: distributed execution done, pluggable auth done,
      DataFusion 52, adaptive sort, all dates corrected
    - Archive 5 completed openspec changes (core engine, security hardening,
      docker packaging, streaming execution, pluggable auth)
    - Archive 16 completed superpowers plan files
    - Mark pluggable-auth last 4 tasks as done (OAuth2 redirect implemented)
    - Fix "single-node only" limitation in features.md (distributed is done)
    - Add design spec for 7.1 (dbt-sqe adapter) and 7.3 (ALTER TABLE schema evolution)

## [0.27.0] — 2026-04-07

### Bug Fixes

- Resolve clippy needless_borrows_for_generic_args warnings

Remove unnecessary & on format!() calls passed to register_table()
    and deregister_table() — the functions accept impl Into<String>.
- Add bench-2w compose, fix port mappings and metrics checks

- Add docker-compose.bench-2w.yml with benchmark-matrix memory configs
    - Fix docker-compose.bench-4w.yml: expose all worker + metrics ports
    - Fix benchmark-matrix.sh: use correct metrics port (29090) for
      distributed configs, use bench-2w compose instead of distributed.yml

    Port map (no conflicts with production defaults):
      Test stack: 18181 (Polaris), 19000 (RustFS)
      Single-node bench: 60051, 18080, 19090
      Distributed bench: 60051, 28080, 29090, 60061-60064, 29091-29094
- Fix Docker memory parsing and add --yes flag for non-interactive mode

Memory parser was summing both used+limit columns from docker stats
    (reporting 151GB on a 36GB machine). Now parses only the left side
    (actual usage). Added --yes/-y flag to skip interactive prompts.
- Robust Docker memory parsing and safer container stop logic

- Use printf %d instead of print int() to avoid newline issues in bash
    - Parse GiB/MiB/KiB explicitly from docker stats used column only
    - Never stop sqlengine-* containers (test stack) when freeing memory
    - Stop by compose project, not individual containers
    - Default DOCKER_MEM_MB to 0 if parsing fails
- Use --config flag for sqe-server in bench compose files

sqe-server uses clap with --config flag, not positional arg.
    The bench-2w and bench-4w compose files were missing --config,
    causing coordinator to fail at startup.
- Stop previous distributed stacks before starting new ones

Docker Desktop crashes when two distributed stacks run simultaneously
    on 36GB machines (2w + 4w = 7 SQE processes + infra exceeds VM limit).

    Now tears down any running distributed stack before launching the next.
    Also adds memory safety check — skips distributed configs when
    insufficient free RAM to avoid Docker VM OOM crash.
- Use sqe_coordinator:: instead of crate:: in main.rs binary

### Documentation

- Add streaming execution design spec and implementation plan

Phase A (safe): coordinator spill-to-disk, late materialization,
    Iceberg scan planning (file pruning, sort-order detection, bloom filters),
    S3 I/O pipeline, SortMergeJoin fallback for large joins.

    Phase B (fast): Flight DoExchange shuffle, distributed range-partition
    sort, distributed joins (broadcast, shuffle hash, sort-merge, predicate
    transfer), multi-endpoint Flight SQL, two-phase aggregation.

    30 tasks across 10 parallel workstreams. Prerequisites: finish 3
    remaining pluggable auth tasks, verify DF 52 capabilities.

    Stays on DataFusion 52 / arrow-rs 57 — DF 53 upgrade deferred until
    RisingWave iceberg-rust fork rebases (fork provides rewrite_files()
    which upstream lacks).
- Update plan with DF 52 verification results

Key finding: pushdown_filters=true has no effect on Iceberg scans —
    SQE's custom IcebergScanExec bypasses DataFusion's ParquetExec. The
    full custom RowFilter implementation (Tasks 4-6) is required.

    Also confirmed: FairSpillPool, SortMergeJoinExec, iceberg manifest
    stats, and sort order API all available on DF 52. Actual iceberg-rust
    version in Cargo.lock is 0.8.0 (RisingWave fork), not 0.9.
- **config:** Expand [auth.external] example with all fields and flow docs

Add documentation for the Authorization Code + PKCE flow (Trino SSO)
    and Device Code flow (CLI). Include previously undocumented fields:
    accept_invalid_certs, authorization_endpoint, token_endpoint, and
    device_authorization_endpoint overrides.
- Update nextsteps for Phase A completion, mark Phase B as next

Phase A streaming execution complete: coordinator spill, late
    materialization, Iceberg scan planning, S3 I/O, SortMergeJoin fallback.
    21/22 TPC-H SF1 on 512MB. Pluggable auth also complete.
- Update documentation for streaming execution (Phase A + Phase B)

Update roadmap, nextsteps, book chapters, and ebook chapters to reflect
    the completed streaming execution work: coordinator spill-to-disk,
    late materialization, scan planning, S3 I/O pipeline, SortMergeJoin
    fallback (Phase A), DoExchange shuffle, distributed sort/join/aggregate,
    multi-endpoint Flight SQL, Trino function compat (Phase B).

    New files:
    - docs/book/src/architecture/streaming-execution.md
    - docs/book/src/architecture/research-papers.md
    - openspec/changes/streaming-execution/ (proposal, design, tasks)

    TPC-H SF1: 21/22 on 512MB coordinator with spill.
- Add historical benchmark comparison and benchmark storage guidance
- **ebook:** Add streaming execution benchmark analysis to Chapter 16

Per-query comparison table (Apr 2 baseline vs Apr 6 distributed, 3.1x).
    Three performance patterns analyzed: metadata-light (6-8x), scan-heavy
    (2-2.5x), join-heavy (3-4x). The 512MB spill test story. Why benchmark
    results are committed to the repo for historical tracking.
- Add full benchmark matrix analysis across all suites and configs

Analysis (benchmarks/results/analysis-2026-04-07.md):
    - 169 queries across 5 suites, 3 deployment configs
    - Spill: 512MB=30/1.1GB, 8GB=128/27.7GB (TPC-E), distributed=3/49MB
    - Key: 8GB spills MORE than 512MB (runs TPC-E that 512MB cannot start)
    - All SF1 queries ran locally (tables <4 files, below distribution threshold)
    - Metrics gaps: footer cache, pruning, late-mat counters not yet wired

### Features

- **trino:** Add bearer JWT passthrough fallback in submit_query

When the bearer token provider rejects a JWT-shaped token (e.g. JWKS
    unavailable), submit_query now falls back to accepting the raw JWT as a
    passthrough token — matching the Flight SQL BFF path behavior. Non-JWT
    opaque tokens are still rejected with 401.

    This completes Task 13.6: the no-credentials + OAuth2 case already
    returns 401 with WWW-Authenticate headers; this commit adds graceful
    degradation for the bearer path.
- Add coordinator memory_limit and spill_to_disk config

Add memory_limit, spill_to_disk, spill_dir, and spill_compression to
    [coordinator] config section. Defaults: 8GB limit, spill enabled,
    LZ4 compression. Mirrors existing [worker] memory config pattern.
    All new fields use serde defaults so existing configs don't break.
- Add S3 byte-range coalescing for Parquet column reads

Merge adjacent byte ranges within configurable threshold to reduce S3
    request count. Default threshold: 1MB.

    - Create crates/sqe-catalog/src/s3_io.rs with ByteRange struct,
      coalesce_ranges(), fetch_byte_ranges(), and fetch_column_chunks()
    - Create crates/sqe-catalog/src/footer_cache.rs with LRU Parquet
      footer cache (moka-backed, size-weighted)
    - Add parquet, bytes, prometheus deps to sqe-catalog
    - Register new modules in lib.rs with public exports
    - Comprehensive unit tests for coalescing logic (adjacent merge,
      gap threshold, overlapping, unsorted input, multiple groups)
- Wire FairSpillPool and spill-to-disk on coordinator runtime

Coordinator DataFusion SessionContext now uses FairSpillPool with
    configurable memory_limit and spill_dir. Sorts, hash aggregates, and
    other spillable operators will spill to disk instead of OOM.

    - Add crates/sqe-coordinator/src/runtime.rs mirroring worker pattern
    - Build runtime once in QueryHandler::new(), share across all queries
    - Create spill directory at startup if it doesn't exist
    - Pass shared runtime to create_session_context()
    - Add unit tests for memory limits, spill enable/disable, error cases
- Add JoinStrategyRule to fall back to SortMergeJoin for large joins

When hash join build side exceeds configurable threshold (default: 2GB),
    rewrite HashJoinExec to SortMergeJoinExec which spills gracefully via
    external sort. Prevents OOM on large joins where DataFusion's hash join
    has no spill-to-disk support (upstream issue #17267).

    The rule:
    - Walks the physical plan tree via transform_down()
    - Estimates build-side size from DataFusion Statistics
    - Preserves join type, conditions, filter, and null equality
    - Adds SortExec on both inputs if not already sorted on join keys
    - Threshold of 0 disables the rule (always use hash join)

    Includes 8 unit tests covering: threshold disabled, below/above
    threshold, join type preservation, sort wrapping, and idempotency.
- Add coordinator memory watermarks and admission control

Monitor coordinator FairSpillPool memory usage and classify into
    pressure levels (Green/Yellow/Orange/Red). Reject new queries at
    >95% utilization (Red) to prevent OOM.

    - Add crates/sqe-coordinator/src/memory.rs with MemoryPressure enum
    - Wire admission control into QueryHandler::execute() before semaphore
    - Add Prometheus gauges: sqe_coordinator_memory_used_bytes,
      sqe_coordinator_memory_limit_bytes, sqe_coordinator_memory_pressure
    - Unit tests for all pressure thresholds and FairSpillPool integration
- Add LRU Parquet footer cache with metrics

Cache parsed Parquet metadata (footers) across queries using moka.
    Eliminates repeated S3 reads for footers of frequently queried tables.
    Default: 256MB cache.

    - Create crates/sqe-catalog/src/footer_cache.rs with FooterCache struct
      backed by moka async LRU cache with size-based weigher
    - Wire footer cache into worker executor scan path via
      ParquetRecordBatchStreamBuilder::new_with_metadata()
    - Add footer_cache field to WorkerFlightService, plumbed through to
      execute_scan()
    - Add sqe_footer_cache_hits_total, sqe_footer_cache_misses_total,
      sqe_footer_cache_size_bytes metrics to MetricsRegistry
    - Add sqe-catalog dependency to sqe-worker for FooterCache type
    - Unit tests for cache hit/miss/invalidation behaviour
- **auth:** Wire external auth services into coordinator startup

Construct OidcDiscovery, AuthCodeService, and PendingAuthStore from
    the [auth.external] config section and pass the resulting OAuth2State
    to the Trino-compat HTTP server. When [auth.external] is configured,
    Trino JDBC clients that connect without credentials receive a 401 with
    WWW-Authenticate headers pointing to the OAuth2 initiate/token endpoints.

    Also moves the `external` field from SqeConfig (top-level) to AuthConfig
    so that [auth.external] in TOML parses correctly, and adds `external: None`
    to all existing AuthConfig constructors in tests.
- Parallel S3 byte-range reads and file prefetch

Fetch multiple column chunks concurrently within each file (default: 4).
    Prefetch next file's footer during current file decode. Configurable
    via [storage] section.

    - Add PrefetchHandle struct for background footer reads
    - Add prefetch_footer() to start async footer fetch
    - Add process_files_with_prefetch() pipeline for overlapping I/O with
      compute across multiple files
    - Export new types and functions from sqe-catalog crate
    - Add tests: parallel byte-range fetch (single, multiple, empty),
      column chunk coalescing, footer prefetch, multi-file prefetch
      pipeline (3 files, empty, single file)
    - Config fields already in place: concurrent_requests_per_file (4),
      max_concurrent_files (8), prefetch_buffer ("32MB")
- Add file-level min/max pruning from Iceberg manifest statistics

Implement PruningStatistics trait for Iceberg DataFile manifest entries,
    enabling DataFusion's PruningPredicate to skip files whose min/max
    value ranges cannot satisfy query predicates.

    - New pruning_stats module with IcebergManifestStatistics adapter
    - IcebergScanExec now stores DataFusion filter expressions (df_filters)
    - data_file_info() applies file-level pruning when filters are present
    - New data_file_info_with_pruning_stats() returns pruning count for metrics
    - collect_data_files() reads manifest entries with full statistics
    - prune_data_files() evaluates PruningPredicate against manifest bounds
    - files_pruned_minmax metric counter registered on execute()
    - SqeTableProvider passes filter expressions to IcebergScanExec
- Detect Iceberg sort order and use streaming merge

Read table sort order from Iceberg metadata via default_sort_order().
    When sort fields use identity transforms on projected columns, convert
    to PhysicalSortExpr and set EquivalenceProperties on IcebergScanExec.
    This tells DataFusion the data is pre-sorted, enabling
    SortPreservingMergeExec instead of full sort for matching ORDER BY.

    - New sort_order module with iceberg_sort_to_physical() converter
    - equivalence_with_sort() builds EquivalenceProperties with orderings
    - IcebergScanExec::new_with_filters() auto-detects sort order
    - Only identity transforms are supported (bucket/truncate stop prefix)
- Verify TopK pushdown for ORDER BY ... LIMIT N

Confirm DataFusion 52's SortExec uses heap-based TopK when LIMIT is set.
    O(N) memory regardless of table size. Add topk module with plan_uses_topk
    utility and tests verifying TopK(fetch=3) appears in physical plan for
    ORDER BY ... LIMIT queries.
- Enable page-level pruning in Parquet reader via PageIndex

Enable with_page_index(true) on ArrowReaderOptions for both cached
    and direct Parquet reads. This instructs the Parquet reader to read
    the PageIndex (column index + offset index) and skip individual data
    pages within row groups whose min/max values don't satisfy the
    predicate. Reduces bytes read from S3 within already-selected files.

    Bloom filter pruning is available via parquet::bloom_filter module
    but requires per-column bloom filter reads which are deferred to a
    future optimization pass (needs equality predicate detection).
- Add spill, shuffle, and late materialization Prometheus metrics

Register 14 new metrics in MetricsRegistry for streaming execution
    observability (Stream 10, Task 29):
- Return multiple FlightEndpoints for distributed query results

Task 28 (Stream 9: Multi-Endpoint Flight SQL). Add two builder methods
    to SqeFlightSqlService:

    - build_flight_info_single(): single-endpoint FlightInfo for
      non-distributed queries (backward compatible, no Location).
    - build_flight_info_distributed(): one FlightEndpoint per worker with
      Location URIs. Falls back to single endpoint when executor list is
      empty.

    Add QueryResultLocation enum so the query handler can signal whether
    results are local or distributed across workers.

    8 unit tests covering single endpoint, multiple endpoints, empty
    fallback, single worker, schema encoding, and QueryResultLocation
    variants.
- Implement Flight DoExchange handler and shuffle infrastructure

Add DoExchange support to worker Flight service for inter-worker data
    exchange. Workers can now receive Arrow RecordBatch streams from other
    workers via Flight DoExchange, the foundation for distributed shuffle.

    New module crates/sqe-worker/src/shuffle.rs provides:
    - ExchangeDescriptor: JSON-serialized descriptor for hash/range partitions
    - ShuffleReceiver: per-partition bounded mpsc channels for batch buffering
    - ShuffleManager: registry of receivers keyed by (query_id, stage_id)

    The do_exchange handler reads the descriptor from the first FlightData
    message, looks up the registered ShuffleReceiver, spawns an intake task
    to decode and buffer incoming batches, and returns a stream that drains
    the partition channel.

    Task 16 of streaming execution plan (Phase B, Stream 6).
- Add hash and range partitioners for shuffle

HashPartitioner splits RecordBatches by hashing key columns (using
    DataFusion's create_hashes with ahash) modulo partition count, then
    extracting rows per partition via arrow::compute::take().

    RangePartitioner splits RecordBatches using sorted boundary values,
    binary searching each row's key to find its target partition.

    Both partitioners handle edge cases: empty batches return empty results,
    single-partition returns input unchanged, missing columns return errors.

    Comprehensive tests cover multi-column hashing, negative range values,
    boundary semantics, determinism, and schema preservation.

    Task 17 of streaming execution plan (Phase B, Stream 6).
- Add ShuffleWriterExec and ShuffleReaderExec plan nodes

DataFusion ExecutionPlan implementations for distributed shuffle:
    - ShuffleWriterExec: partitions input data (hash/range) and sends to
      remote executors via Flight DoExchange. Returns empty stream since
      data is shipped out, not returned locally.
    - ShuffleReaderExec: reads shuffled RecordBatches from bounded mpsc
      channels (populated by DoExchange handler) and presents them as a
      standard SendableRecordBatchStream.
    - ShufflePartitioning enum: serializable descriptor for hash/range
      partitioning configuration.
    - EmptyRecordBatchStream/ChannelRecordBatchStream: stream adapters.

    Both nodes implement proper schema(), children(), with_new_children(),
    execute(), properties() with Incremental emission and Bounded semantics.
    Foundation for distributed sort/join/aggregate in Streams 7-8.
- Add missing Trino-compatible SQL functions to close JDBC/dbt compat gap

Add 10 new Trino-compatible functions and transaction stubs to improve
    compatibility from ~76% to ~95% for real Trino JDBC clients and dbt-trino
    models:

    New UDFs in trino_functions.rs:
    - date_format(timestamp, pattern) - Java/MySQL-style date formatting
    - date_parse(string, pattern) - inverse of date_format
    - now() - alias for CURRENT_TIMESTAMP
    - json_object(k1, v1, ...) - build JSON from key-value pairs
    - json_format(value) - identity pass-through (JSON stored as varchar)
    - strpos(string, substring) - 1-based position search
    - localtime() / localtimestamp() - Trino time aliases

    Transaction stubs in classifier.rs + query_handler.rs:
    - BEGIN / START TRANSACTION / COMMIT / ROLLBACK are no-op successes
    - Enables JDBC tools using setAutoCommit(false) without errors

    Verified DataFusion built-ins already match Trino signatures:
    - date_trunc('unit', timestamp) - works as-is
    - concat_ws(sep, s1, s2, ...) - works as-is
    - replace(string, from, to) - works as-is
    - split_part(string, delim, idx) - works as-is

    Format pattern conversion handles Trino/MySQL divergences:
    - %i (minutes) -> chrono %M
    - %s (seconds) -> chrono %S
- Add stage decomposition for multi-stage distributed queries

Break distributed queries into stages at shuffle boundaries. Each stage
    runs independently on assigned executors, connected by data shuffles.

    QueryStage struct captures: plan fragment, input dependencies, shuffle
    type (Hash/Range/Broadcast), and assigned executors.

    decompose_plan() walks the physical plan tree and splits at:
    - SortMergeJoinExec: both sides get Hash shuffle stages
    - HashJoinExec: build side (left) gets Hash shuffle stage
    - SortExec with multi-partition input: gets Range shuffle stage

    compute_waves() groups stages into parallel execution waves using
    topological ordering (leaf scans first, joins/aggs next, final last).

    Framework for Streams 7-8 to add distributed sort/join planning.
- Add distributed join strategies (broadcast, shuffle hash, pre-sorted)

Task 24: BroadcastJoinRule detects HashJoinExec where one side's
    estimated size < broadcast_threshold (default 64MB) and wraps it in
    BroadcastJoinPlan. Small side collected on coordinator and broadcast
    to all executors. No shuffle of the large side.

    Task 25: ShuffleHashJoinPlan wraps both sides in ShuffleWriterExec
    with hash partitioning on join keys. Each executor builds hash table
    for its partition, probes against incoming. Memory per executor:
    O(build_side / num_executors).

    Task 26: PreSortedJoinRule detects when both join sides are already
    sorted on join key columns (from Iceberg sort order) and rewrites
    HashJoinExec to SortMergeJoinExec directly. O(batch_size) memory,
    zero shuffle needed.

    Also adds select_join_strategy() to pick the optimal strategy and
    JoinStrategy enum for strategy selection.
- Add predicate transfer for join optimization

After scanning the build side of a join, extract distinct join-key
    values and push as an IN-list predicate to the probe side's Iceberg
    scan. This enables file-level pruning (min/max from manifests) and
    bloom filter pruning on the probe side.

    Size limit: only apply when distinct key set < 10,000 values to avoid
    bloating the IN-list predicate.

    Provides PredicateTransfer struct, extraction helpers for RecordBatch
    columns, and build_predicate_transfer() convenience function for the
    full pipeline.
- Add range boundary sampling and DistributedSortExec for distributed sort

Tasks 20-21: Implement range boundary computation from Iceberg manifest
    min/max statistics with fallback to reservoir sampling. Add
    DistributedSortExec plan node that replaces SortExec when distributed
    mode has enough executors and input exceeds size threshold. Add
    DistributedSortRule physical optimizer rule for automatic rewriting.
- Add two-phase distributed aggregation

Task 22: Implement PartialAggregateExec and FinalAggregateExec plan nodes
    that wrap DataFusion's AggregateExec in Partial and Final modes. Add
    DistributedAggregateRule optimizer that rewrites single-phase aggregation
    into two-phase when distributed mode is active. For high-cardinality
    GROUP BY (estimated from column stats), selects hash-partition shuffle
    merge; otherwise uses coordinator merge.
- Order predicate evaluation by selectivity for late materialization

Task 23: When multiple predicates exist in a WHERE clause, order their
    evaluation in the RowFilter by cost tier:
    1. Partition column predicates (free at manifest level)
    2. Predicates with bloom filter support
    3. Predicates on sort-order columns (best zone map pruning)
    4. Remaining by estimated selectivity from column stats

    Most selective/cheapest predicates evaluated first to maximize early row
    elimination and minimize S3 reads.
- Add repeatable benchmark matrix across deployment configs

Benchmark matrix script runs all suites (TPC-H, TPC-DS, SSB, TPC-C,
    TPC-E) across 4 deployment configurations:
    - single-512mb: spill stress test
    - single-8gb: single-node baseline
    - distributed-2w: coordinator + 2 workers
    - distributed-4w: coordinator + 4 workers (linear scaling)
- Add observability stack, metrics capture, and ASCII charts to benchmark matrix

- VictoriaMetrics + Grafana docker-compose (80MB RAM total)
    - Pre-built Grafana dashboard: memory, spill, cache, pruning, shuffle,
      query rate, latency, worker activity (14 panels, 5s auto-refresh)
    - Metrics snapshot before/after each suite with delta computation
    - Key metric deltas shown inline (spill, pruning, cache hits)
    - ASCII bar charts: total time per config + per-suite comparison
    - Grafana URL shown at start of benchmark run
    - Services left running after benchmark for inspection
    - Interactive cleanup prompt at the end (ask before stopping)
    - Metrics deltas saved to benchmarks/metrics/ for historical tracking
- Add metrics analytics section to benchmark matrix report

Per-config analytics with visual bars:
    - Memory: peak usage vs limit with bar chart
    - Spill: sort/join event counts and bytes
    - Cache: query + footer cache hit rates with bar chart
    - Pruning: files pruned (min/max, bloom) + pages pruned (index)
    - Late materialization: predicate vs projection I/O bytes
    - Workers: fragment count, S3 bytes read
    - Shuffle: bytes sent/received

    All saved in the matrix-*.txt report for historical tracking.
- Add background memory metrics reporter for Grafana

Memory gauges (used_bytes, limit_bytes, pressure) were only updated at
    query start, so Prometheus scrapes (every 5s) missed peak usage during
    1-3s queries, showing 0 between queries.

    Now spawns a background task that updates memory gauges every 1 second,
    giving Grafana continuous visibility into memory pressure and spill.
    Wired into both main.rs and sqe_server.rs coordinator startup paths.
- Add adaptive sort stripping, S3/auth/write metrics, and fix integration tests

- Add adaptive sort stripping module that strips unnecessary ORDER BY under
      memory pressure (configurable via sort_mode: auto|always|never)
    - Extend MetricsRegistry with S3 I/O, auth, and adaptive sort counters
    - Wire metrics into WriteHandler and write path for observability
    - Fix test_error_classification_live: DELETE/MERGE are now supported,
      so test expectations updated from NOT_SUPPORTED to CATALOG_ERROR/TABLE_NOT_FOUND
    - Add memory pressure rejection with sort-aware diagnostic messages

### Miscellaneous

- Clean up benchmark metrics, keep only latest run

## [0.26.0] — 2026-04-04

### Bug Fixes

- Invalidate query cache for DELETE, UPDATE, and MERGE operations

Previously only INSERT, CTAS, and DROP invalidated cached query results,
    causing stale reads after DML mutations on the same table.
- **bench:** Tag new DML queries with requires: full_schema

The new write-path benchmark queries (delivery_delete, payment_update_*,
    etc.) reference tables not yet in the benchmark data generator. Tag them
    with requires: full_schema so they skip cleanly instead of erroring.
- **config:** Bump test query memory to 2GB and timeout to 600s for SF1

SF1 benchmarks need more memory for HashJoins on multi-million row tables
    (TPC-H q17/q18/q21 failed with 256MB default). Also increase timeout for
    large CTAS loads (store_sales: 2.8M rows).
- **bench:** Tag DML queries as write_via_benchmark, enable ROLLUP

DML benchmark queries (UPDATE/DELETE) fail when run through the
    benchmark infrastructure because the write handler creates a separate
    REST catalog session that can't resolve tables loaded by the read path.
    The core DELETE/UPDATE/MERGE implementation works correctly (10
    integration tests pass). The benchmark DML path needs the write handler
    to share the query handler's session catalog — tracked as follow-up.
- Use SessionCatalog directly for DML table loading

Change handle_delete, handle_update, and handle_merge to accept
    Arc<SessionCatalog> instead of Arc<dyn Catalog>. Use
    SessionCatalog::load_table() directly (which has debug logging and
    circuit breaker) rather than going through SessionCatalogBridge which
    failed to find tables. For Transaction::commit(), use
    catalog.as_catalog().as_ref() to get the iceberg::Catalog reference.
- Resolve DML benchmark failures — two root causes

1. prefix_tables didn't qualify UPDATE target table names when UPDATE
       appeared at the start of a line (after comments). Fixed by also
       matching "\nUPDATE" as a table-introducing context.

    2. Write handler used a separate catalog session that couldn't find
       tables. Now shares the SessionCatalog from create_session_context
       via a new return tuple (SessionContext, Arc<SessionCatalog>).
- Enable cross-table subqueries in DELETE/UPDATE WHERE clauses

Pass the full SessionContext (with all catalog tables registered) to
    write handler methods instead of creating an isolated DFSessionContext.
    This allows WHERE/SET clauses containing subqueries that reference
    other tables (e.g. DELETE FROM new_order WHERE no_o_id = (SELECT
    MIN(no_o_id) FROM new_order ...)) to resolve those tables through
    the catalog provider.

    Affected methods: handle_delete, handle_update, handle_merge and
    their helpers filter_batch_negate, apply_update, count_matching_rows.
- Register DML temp tables in datafusion catalog, not Iceberg catalog

MemTable registrations in filter_batch_negate, apply_update,
    count_matching_rows, and handle_merge were targeting the default
    catalog (SqeCatalogProvider / Iceberg) which rejects dynamic table
    registration. Register under datafusion.public.<name> instead,
    using a MemoryCatalogProvider + MemorySchemaProvider bootstrapped
    in create_session_context.

### Documentation

- Update roadmap for DELETE/UPDATE/MERGE and write-path benchmarks

- nextsteps.md: mark tasks 8.4/8.5/8.13/8.14 complete (103/103), update
      status line for CoW DML + bearer token auth + write-path benchmarks
    - README.md: check off DML roadmap item, update TPC-C/E query counts,
      note bearer token progress
    - openspec tasks.md: mark DELETE, MERGE, and their integration tests done
- Update documentation for DELETE/UPDATE/MERGE and security audit

Reflect that DELETE FROM, UPDATE, and MERGE INTO are now implemented
    via Copy-on-Write using the RisingWave iceberg-rust fork rewrite_files().
    Update benchmark numbers (217/222 pass, 97.7%), mark security audit
    findings as resolved (12/12 fixed), and update all roadmap references
    from planned/blocked to implemented.

### Features

- **sql:** Classify UPDATE as its own StatementKind

Previously UPDATE was routed to Utility (unsupported). Now it gets its own
    variant for dispatch to the write handler.
- **sql:** Classify UPDATE as its own StatementKind

Add StatementKind::Update variant so UPDATE statements route to the write
    handler instead of falling through to Utility. Implement CoW DELETE and
    UPDATE handlers in WriteHandler using DataFusion in-memory evaluation.
- Implement DELETE and UPDATE via CoW rewrite_files

Fix compilation errors in handle_delete and handle_update handlers:
    - Correct sqlparser AST navigation for DELETE FROM (FromTable enum)
    - Fix Assignment.target access using AssignmentTarget::ColumnName/Tuple
    - Remove unused imports (futures::TryStreamExt, ParquetObjectReader)
    - Use table's FileIO for Parquet reads instead of raw S3 object_store
- Wire DELETE and UPDATE into query handler dispatch

Replace NotImplemented stubs for DELETE and UPDATE with actual calls to
    write_handler.handle_delete and write_handler.handle_update. MERGE INTO
    retains a NotImplemented error with updated messaging.
- Enable DML benchmark queries, update docs

Remove requires: tags from TPC-C (delivery, new_order, payment) and
    TPC-E (data_maintenance, market_feed, trade_order, trade_result, trade_update)
    benchmark queries — they are read-only SQL approximations that work now.

    Fix trade_order.sql JOIN ordering bug (referenced alias before definition).
    Set max_result_rows=0 in test config for large dimension table loads.
    Update nextsteps.md with DELETE/UPDATE status.
- Implement MERGE INTO via CoW rewrite_files

Full OUTER JOIN-based MERGE: registers target + source as MemTables,
    builds CASE WHEN expressions for MATCHED UPDATE/DELETE and
    NOT MATCHED INSERT clauses, writes new files, atomically swaps.
    Also fixes clippy too_many_arguments lint on resolve_insert_expr.
- **auth:** Add bearer token (JWT/JWKS) authentication provider

New BearerTokenProvider validates pre-obtained JWTs against a JWKS endpoint,
    extracts user identity and roles from configurable claims. Supports
    accept_invalid_certs for dev environments with self-signed TLS.
    Wire into auth factory, coordinator binaries, and Trino compat server.
- **bench:** Add TPC-C/E write-path DML benchmark queries

Now that DELETE and UPDATE are supported via CoW rewrite_files, add the
    actual write operations from TPC transaction profiles:

    TPC-C (9 queries):
    - Delivery: DELETE from new_order, UPDATE orders/order_line/customer
    - Payment: UPDATE warehouse/district/customer
    - New Order: UPDATE district (next_o_id), UPDATE stock

    TPC-E (7 queries):
    - Market Feed: UPDATE last_trade prices
    - Trade Update: UPDATE trade executor name, UPDATE settlement cash_type
    - Trade Result: UPDATE holding_summary, UPDATE trade status
    - Data Maintenance: DELETE old news_item, UPDATE zero-volume daily_market

    The original read-only inspection queries are preserved alongside these
    write-path versions for mixed-workload benchmarking.
- **bench:** Enable ROLLUP queries — DataFusion 52 supports them

DataFusion 52 has full GROUP BY ROLLUP support. The requires: rollup
    tags in TPC-DS q18/q27/q36/q67/q70/q86 were stale from an earlier
    DataFusion version. All 6 queries pass at SF0.1.

### Miscellaneous

- Switch iceberg deps to RisingWave fork for rewrite_files support
- Tag 5 TPC-E queries blocked by DataFusion IN(subquery) limitation

DataFusion 52 cannot execute IN (subquery) in the physical plan when the
    outer table is a MemTable in a non-default catalog. This blocks 5 TPC-E
    DML benchmark queries. Tagged with requires: in_subquery_memtable.
    Added to upstream watch list in nextsteps.md.

### Testing

- Add integration tests for DELETE and UPDATE

Add four integration tests validating the new CoW DELETE and UPDATE
    functionality against a live Polaris + RustFS test stack:
    - test_delete_with_where: DELETE with WHERE clause removes matching rows
    - test_delete_all: DELETE without WHERE truncates the table
    - test_update_with_where: UPDATE with WHERE modifies targeted rows only
    - test_update_all_rows: UPDATE without WHERE modifies every row
- Add MERGE INTO integration tests and expand DML coverage

Integration tests for MERGE matched update, matched delete,
    not-matched insert, and combined clauses. Extends the existing
    DELETE/UPDATE test suite.

## [0.25.0] — 2026-04-01

### Features

- **scheduler:** Add bin-packing file splitter (first-fit-decreasing)
- **scheduler:** File-size-based cost estimation from Iceberg manifest metadata

- ScanTask gains file_sizes_bytes field (parallel to data_file_paths)
    - estimate_cost() uses total MB instead of file count (fallback to count)
    - IcebergScanExec.data_file_info() returns (path, size) from manifest
    - distributed_scan uses real file sizes for task construction
- **scheduler:** Skip distribution for small queries (coordinator-only threshold)

- distribution_threshold (default "128MB") — skip distribution below this
    - distribution_file_threshold (default 4) — skip if fewer files than this
    - Uses real file sizes from manifest when available, file count as fallback
    - Queries on small tables execute 2-5x faster (no distribution overhead)
- **scheduler:** Wire bin-packing into distributed scan path

Replace round-robin file splitting in try_distribute() with
    bin_pack_files() from sqe-planner. Files are now grouped into
    size-balanced bins (target 256 MiB each, up to 3 bins per worker)
    using first-fit-decreasing, so large and small files are mixed
    efficiently across tasks. Add target_task_size to QueryConfig
    (default "256MB") to make the target configurable.
- **scheduler:** Add cache-affinity hints via consistent hashing (soft 20% preference)
- **scheduler:** Add straggler detection (WARN when fragment >3x median)

After all distributed fragments complete, compute per-fragment timing
    from QueryTracker and emit a WARN log for any fragment that took more
    than 3x the median duration. Also logs a summary (total/max/min ms) at
    INFO when the distributed scan finishes.

    Adds QueryTracker::all_fragments_done() which returns per-fragment
    timing tuples once every fragment reaches a terminal state, and four
    new unit tests covering the partial/full/failed/empty cases.
- Add scheduler Prometheus metrics and configurable OTel trace sampling (1% default)

## [0.24.1] — 2026-04-01

### Bug Fixes

- Reorder error classifier so TypeMismatch wins over FunctionNotFound

DataFusion concatenates "TypeSignatureClass... No function matches..."
    in type errors. The classifier now checks TypeMismatch first.

    Added test_error_classification_live integration test verifying 7 error
    scenarios produce correct SqeErrorCodes against the live stack.

### Documentation

- **ebook:** Add enterprise hardening section to Chapter 10

Covers structured error codes, startup security warnings, client IP logging,
    circuit breaker for Polaris, PII redaction in audit logs, per-query resource
    limits, session persistence, and the /readyz dependency-checking evolution.
- **ebook:** Add structured error handling section to Chapter 14

Adds 'When Every Error Looks the Same' covering the SqeErrorCode taxonomy
    (27 codes), gRPC/Trino mapping, auto-classification from DataFusion strings,
    before/after error examples, and the lesson that error messages are part of
    the client API.
- **ebook:** Add pluggable auth section to Chapter 4

Adds "Ten Ways to Prove You're You" section covering the AuthProvider
    trait, AuthChain first-match semantics, all 10 providers with when to
    use each, TOML configuration examples, OIDC Discovery, and the design
    philosophy that sovereign means the operator picks their auth model.

### Build

- **ebook:** Rebuild EPUB + HTML with updated chapters (90,450 words)
- **ebook:** Rebuild PDF with weasyprint fallback, update Makefile

## [0.24.0] — 2026-03-30

### Features

- Add PII redaction in audit logs (email, phone, SSN, card numbers)

Adds regex-lite based redact_pii() that scrubs email, SSN, phone, and
    credit card patterns from query_text before writing to the audit log;
    query_hash is left untouched as it is non-reversible.
- Enterprise hardening — connection pooling, circuit breaker, query limits, session persistence

- Connection pooling: shared reqwest::Client across all SessionCatalog instances
    - Circuit breaker: CircuitBreaker for Polaris REST calls (5 failures → 30s open)
    - Slow query log: WARN for queries exceeding slow_query_threshold_secs (default 30s)
    - Dependency health: /readyz checks Polaris reachability (returns 503 if down)
    - Backpressure: max_concurrent_queries semaphore (default 100, try_acquire)
    - Memory limit: per-query GreedyMemoryPool via DataFusion (default 256MB)
    - Session persistence: file-based snapshot/restore (opt-in, persistence="file")
    - Config: max_concurrent_queries, slow_query_threshold_secs, max_query_memory,
      session persistence/persistence_path/snapshot_interval_secs
- Add OTel spans to catalog/scan/write, improve readyz health checks

Instrumentation added:
    - SessionCatalog: #[instrument] on list_namespaces, list_tables, load_table,
      create_view, list_views, load_view_sql, drop_view
    - IcebergScanExec: info_span! on execute() with table, partition, predicates
    - write_data_files: #[instrument] with table, file_prefix, total_rows
    - catalog_ops: #[instrument] on drop_table, create_schema, drop_schema,
      create_view, drop_view
    - write_handler: #[instrument] on handle_ctas, handle_insert, handle_create_table

### Refactoring

- Extract session context setup into separate module

Move the large create_session_context() method body from query_handler.rs
    into a new crates/sqe-coordinator/src/session_context.rs module, reducing
    query_handler.rs by ~130 lines. The method in QueryHandler now delegates to
    the standalone pub fn create_session_context() with explicit parameters
    (config, session, policy_store, query_tracker).
- Extract flight_sql helpers into separate module

Move FetchResults, FlightStream type alias, and sqe_error_to_status()
    from flight_sql.rs into a new flight_sql_helpers.rs module. flight_sql.rs
    re-exports FetchResults for backward compatibility and imports the helper
    function and type locally. Reduces flight_sql.rs by ~162 lines.

### Testing

- Add unit tests for SQL classifier and policy enforcer

sqe-sql already had 35 classifier tests; no changes needed there.
    sqe-policy gains 22 new tests covering PassthroughEnforcer (plan
    unchanged for any user/role combination), InMemoryPolicyStore
    role-based lookup and priority semantics, all MaskType variants,
    apply_mask Hash/Custom paths, and parse_filter_expr/parse_mask_type
    edge cases (all operators, float literals, nullify alias, unknown mask
    fallback). Total sqe-policy tests: 12 → 34.
- Add unit tests for enterprise hardening features

Adds tests for QueryConfig defaults and TOML parsing, SessionConfig
    defaults and file-persistence TOML parsing, and session snapshot/restore
    (snapshot_to_file / restore_from_file) in the SessionManager.

### Security

- Warn at startup when TLS, rate limiting, or SSL verification is disabled
- Add max_result_rows limit and 30s handshake timeout

Adds a configurable max_result_rows guard (default 1,000,000) to
    QueryConfig that rejects oversized result sets before returning them to
    the client. Also wraps do_handshake's OIDC authenticate_credentials call
    in a 30-second tokio timeout, returning Status::deadline_exceeded on
    breach to prevent slow-credential-provider DoS.
- Add Retry-After header on rate limit, log client IP on failed auth

- sqe-trino-compat: detect rate limit errors in submit_query and attach
      Retry-After: 1 header to the response
    - sqe-coordinator: extract client IP from x-forwarded-for (with TCP peer
      fallback) and include it in the Authentication failed warn! log
    - sqe-trino-compat: include client IP (from x-forwarded-for) in Trino
      HTTP basic-auth failure warn! log
- Log client IP on every request (Flight SQL + Trino HTTP)

- Extract client_ip in get_session_from_request() so every Flight SQL
      call includes it in debug logs (not just handshake failures)
    - Add client_ip to Trino HTTP submit_query log for every query
    - Deduplicate IP extraction in handshake (reuse shared helper)

## [0.23.0] — 2026-03-30

### Bug Fixes

- Resolve clippy warnings in trino_functions.rs

### Documentation

- Add structured error handling design spec
- Add error handling implementation plan
- **ebook:** Add 'The First Real Run' section to Chapter 7 — dbt hardening journey
- **ebook:** Acknowledge Rafael Herrero — Kubernetes deployment, operator, and security

### Features

- **core:** Add SqeErrorCode enum with gRPC and Trino mappings

Introduces a structured SqeErrorCode enum alongside SqeError with 27
    codes covering parse/planning, catalog, auth, execution, and infra
    categories. Adds is_user_error(), name(), trino_error_code(), and
    trino_error_type() methods, plus auto-classification via
    classify_catalog_error() / classify_execution_error() message heuristics.
    Improves client_message() to surface detail for user errors and redact
    system errors, with a clean_error_message() helper that strips DataFusion
    wrapper noise.
- **trino:** Use structured error codes in Trino HTTP responses

Add query_id field to TrinoError, implement from_sqe_error constructor
    that maps SqeError → structured Trino error codes, and update submit_query
    to produce rich error payloads (error_code, error_name, error_type) instead
    of hardcoded INTERNAL_ERROR values.
- **flight:** Use structured error codes in gRPC Status responses

Replace all Status::internal() wrapping SqeError from query_handler
    calls with sqe_error_to_status(), which maps SqeErrorCode variants to
    correct gRPC codes (NotFound, InvalidArgument, PermissionDenied, etc.)
    and attaches x-sqe-error-code and x-sqe-query-id response metadata.
- Structured query lifecycle logging with error codes and query ID

- Add error_message field to QueryRecord for human-readable failure detail
    - Update QueryTracker::failed() to accept &SqeError, extracting error_type,
      error_code, and client_message from the structured error
    - Update all callers in query_handler.rs to pass SqeError directly
    - Move execute() info! log after query_id generation so query_id is included

## [0.22.0] — 2026-03-30

### Bug Fixes

- Fix distributed compose ports, CLI flags, and test script

- Offset host ports to avoid conflicts (60051, 28080, 29090)
    - Fix entrypoint for sqe-server (--config flag) and sqe-worker (positional)
    - Fix CLI flags (--user, --token, -e instead of --username, --password, -c)
    - 8/13 tests pass; 5 fail on CTAS due to Polaris STS credential vending
      vs RustFS (infrastructure issue, not code — needs Polaris config for
      static credential mode)
- Use user/password auth (SQE_PASSWORD env) in distributed test script
- Distributed compose uses test stack infra, add S3 endpoint override

- docker-compose.distributed.yml now extends docker-compose.test.yml
      (shares Polaris + RustFS, avoids duplication)
    - Added QUARKUS_S3_ENDPOINT_OVERRIDE to Polaris for RustFS compatibility
    - 8/13 distributed tests pass; CTAS fails due to Polaris credential
      vending with non-AWS S3 backends (iceberg-rust treats 422 as fatal
      during create_table — works in-process but not via Docker)
    - Usage: docker compose -f docker-compose.test.yml -f docker-compose.distributed.yml up --build -d
- Polaris storageConfigInfo with S3 endpoint for RustFS, pin to 1.3.0

Root cause: Polaris credential vending failed (422) because the warehouse
    storageConfigInfo was missing endpoint/endpointInternal/pathStyleAccess.
    Without these, Polaris tries to use AWS STS to vend credentials and
    resolves S3 buckets against the real AWS endpoint — both fail with
    non-AWS S3 backends (RustFS, MinIO).
- Replace scan leaf in plan tree instead of replacing entire plan

The try_distribute() was returning DistributedScanExec as the whole plan,
    discarding aggregation/filter/sort nodes above the scan. Now uses
    replace_scan_in_plan() to walk the tree and replace only the
    IcebergScanExec leaf, keeping all parent nodes intact.
- Project worker batches to match expected schema in DistributedScanExec

Workers return full table columns, but the coordinator plan may expect
    fewer columns (e.g., COUNT(*) expects 0 columns). The stream adapter
    now handles three cases:
    - Same column count: pass through
    - 0 expected columns (COUNT(*)): create empty-column batch with row count
    - Partial projection: select columns by name from worker batch
- Resolve missing role_mappings field, integration test arg, and clippy warnings
- Integration test robustness — skip when worker unavailable, retry view drop propagation
- Return empty result for tables with no snapshot instead of corrupt Parquet error
- Handle empty tables, CTAS with zero rows, and table lifecycle edge cases

- IcebergScanExec: return empty batch when table has no snapshot
    - execute_query: always return at least one batch (with schema) so CTAS
      can infer the output schema even when WHERE false returns 0 rows
    - Added test_table_lifecycle_edge_cases covering: empty table SELECT,
      COUNT on empty, DROP+re-CREATE, non-existent table, double CREATE
- Use UUID in data file names to prevent collision on multiple INSERTs

Each write operation now generates file names like `insert-019...-00000.parquet`
    instead of `insert-00000.parquet`. This prevents the Iceberg commit conflict
    "Cannot add files that are already referenced by table" when multiple INSERTs
    target the same table.

### Documentation

- Add distributed execution wiring design spec
- Add distributed execution wiring implementation plan
- Add device auth + Trino SSO tasks and config example

### Features

- Add distributed docker-compose with coordinator + 2 workers

- docker-compose.distributed.yml: coordinator, worker-1, worker-2, Polaris, RustFS
    - tests/distributed/coordinator.toml: coordinator config with worker_urls
    - tests/distributed/worker.toml: worker config with heartbeat to coordinator
    - scripts/bootstrap-distributed.sh: bootstrap script for distributed stack
    - Dockerfile: add sqe-worker binary to build
- Expose data_file_paths() on IcebergScanExec
- Add FragmentInfo tracking to QueryTracker
- Add optional FragmentCallback to DistributedScanExec
- Wire distributed execution into query pipeline via try_distribute()
- System.runtime.tasks shows real worker URLs from FragmentInfo

Add RuntimeFragmentInfo to sqe-catalog and populate fragments in the
    RuntimeQueryRecord snapshot. The tasks table now emits one row per
    fragment with the real worker URL as node_id for distributed queries,
    and falls back to a single synthetic task using the coordinator
    node_id for local (single-node) execution.
- Wire distributed execution into query pipeline

- try_distribute() in execute_query() inspects PhysicalPlan for
      IcebergScanExec, extracts data files, schedules across workers
    - FragmentInfo tracking in QueryTracker with set/update methods
    - FragmentCallback in DistributedScanExec fires on stream completion
    - system.runtime.tasks shows real worker URLs for distributed queries
    - data_file_paths() exposed on IcebergScanExec for file listing
    - Distributed test script updated with fragment verification
    - 625 tests pass, clippy clean
- Add ebook, pluggable auth providers, and policy enforcement
- **auth:** Add OIDC discovery with well-known endpoint fetching
- **config:** Add ExternalAuthConfig for device code and Trino SSO
- **auth:** Add PendingAuthStore and TokenSet for interactive auth flows
- **auth:** Add DeviceCodeService for RFC 8628 device authorization grant
- **auth:** Add AuthCodeService for authorization code + PKCE flow
- **trino:** Add OAuth2 external auth endpoints for browser SSO
- Wire external auth services into coordinator and Trino server

Add oauth2 field to TrinoState, thread it through start_trino_server,
    mount OAuth2 routes conditionally, and return a WWW-Authenticate
    challenge when no credentials are present and external auth is configured.
    Both coordinator call sites pass None until [auth.external] is wired.
- Add Trino-compatible date/time function aliases (year, month, day, etc.)

Registers year(), month(), day(), hour(), minute(), second(),
    day_of_week(), day_of_year(), quarter(), week() as UDFs that delegate
    to chrono extraction. These are needed for dbt models and Trino SQL
    compatibility — DataFusion only has extract() / date_part().

    9 unit tests + integration test covering all functions.
- Add date_add, date_diff, from_unixtime, to_unixtime, if(), typeof() Trino compat functions
- Add ApiKey + mTLS providers, device auth spec, and Cargo.lock update

- ApiKeyProvider: constant-time comparison, keys file hot-reload
    - MtlsProvider: CN/OU/SAN extraction from client certs
    - Design spec + implementation plan for device auth + Trino SSO
    - Cargo.lock updated for new dependencies (subtle, toml in sqe-auth)

### Refactoring

- Use UUID-only data file names matching Spark/Trino convention

File names are now {write_uuid}-{counter}.parquet (e.g.
    019abc12-...-00000.parquet) instead of insert-{uuid}-00000.parquet.
    This matches the standard Iceberg file naming used by Spark and Trino.

### Testing

- Add distributed integration test script

Tests coordinator + workers end-to-end:
    - Basic connectivity (SELECT 1)
    - system.runtime.nodes (coordinator + workers)
    - system.runtime.queries (query history)
    - system.runtime.tasks (per-query tasks)
    - system.metadata.catalogs, table_properties, table_comments
    - Query result cache (hit timing, invalidation on write)
    - Trino HTTP endpoint
    - CTAS + INSERT + SELECT roundtrip
- Add distributed execution verification to integration tests
- Add concurrent client load test script
- Add large multi-batch INSERT test (1000 rows x 2 commits)

Reproduces the file name collision bug where a 2000-row INSERT via dbt
    failed with "Cannot add files that are already referenced by table".
    Verifies that 100-batch INSERTs commit without conflict and row count
    is correct across multiple commits to the same table.

## [0.21.0] — 2026-03-28

### Documentation

- Add query history and result cache design spec
- Fix 7 review issues in query history/cache spec (security, mutability, planning_ms, uuid v7)
- Add query history and cache implementation plan
- Update config example and nextsteps for query history and cache

Add [query_cache] and [query_history] sections to sqe.toml.example with
    documented defaults. Update nextsteps.md status line and implementation
    order to reflect query history + result cache feature completion. Fix
    ResultCache::new test call sites to pass the metrics argument introduced
    in a previous task.

### Features

- Add query_cache and query_history config sections, uuid v7 feature

Add QueryCacheConfig and QueryHistoryConfig structs to sqe-core config,
    wire them into SqeConfig with serde defaults, and enable uuid v7 in the
    workspace Cargo.toml.
- Add QueryTracker with full lifecycle tracking and cancellation

Introduces QueryTracker backed by moka::sync::Cache + DashMap to track
    queries through Queued→Running→Finished/Failed/Canceled states with
    per-query CancellationToken support. Re-exports QueryHistoryConfig from
    sqe_core and adds moka sync+future features to workspace. Keeps
    query_registry temporarily until Task 4 migrates callers.
- Add ResultCache with user-scoped keys and write invalidation

Implements moka-backed query result cache with SHA-256(user:normalized_sql) keys,
    DashMap secondary index for table-level invalidation, non-deterministic bypass,
    and per-entry size limit enforcement. Exports QueryCacheConfig from sqe-core.
- Wire QueryTracker and ResultCache into query execution pipeline

- Add query_tracker and query_cache fields to QueryHandler, passed via constructor
    - Track full query lifecycle in execute(): start -> running -> complete/failed
    - Add result cache lookup before execution for read queries (cache hit path)
    - Store successful read query results in cache after execution
    - Invalidate cache entries on write operations (INSERT, CTAS, DROP)
    - Mark timed-out queries as failed in the tracker
    - Replace QueryRegistry with QueryTracker in SqeFlightSqlService
    - Update do_action_cancel_query to use QueryTracker (UUID-based)
    - Remove query_registry module (subsumed by query_tracker)
    - Update sqe_server.rs and main.rs to construct tracker + cache from config
    - Update all test call sites for new QueryHandler::new() signature
- Add system.metadata.catalogs/table_properties/schema_properties/table_comments
- Add system.runtime.queries/nodes/tasks virtual tables

Implements RuntimeSchemaProvider in sqe-catalog with three virtual tables:
    - queries (17 columns): query history from QueryTracker snapshots
    - nodes (5 columns): coordinator + worker cluster topology
    - tasks (7 columns): one task per finished query (single-node mode)

    Avoids circular dependency by using RuntimeQueryRecord snapshot types
    and a closure-based QueryRecordsFn instead of depending on sqe-coordinator.
    Wired into QueryHandler via SystemCatalogProvider.with_runtime().
- Add Prometheus metrics for query result cache

### Testing

- Add integration tests for scheduling, distributed execution, query tracking, and caching

## [0.20.0] — 2026-03-27

### Bug Fixes

- Resolve all clippy errors (too-many-args, is_multiple_of, unused var)

Introduce LoadArgs struct to reduce load_benchmark parameter count below
    the clippy limit, replace manual modulo check with is_multiple_of, and
    suppress unused _schema variable in flight_sql.rs.
- Normalize decimal trailing zeros in benchmark comparison
- Harden token_fingerprint to use hash instead of raw suffix, update docs

- token_fingerprint() now uses DefaultHasher instead of last 8 chars of
      raw token, eliminating partial token exposure in logs
    - README.md: mark DoPut, type formatting as done in roadmap
    - nextsteps.md: update status line, add Step 3c hardening pass
- Qualify table names in multiline FROM lists in benchmark queries

The prefix_tables function failed to qualify table names in multi-line
    comma-separated FROM lists (e.g. "FROM\n    part,\n    supplier,\n")
    because the comma check only looked for already-qualified tables before
    the comma. When tables are processed longest-first, shorter table names
    haven't been qualified yet. Now also checks if the word before the comma
    is a known table name from the benchmark schema.
- Handle aliased tables in comma-separated FROM lists

Simplified the comma continuation check: any trailing comma now signals
    a table list continuation. Handles both "FROM t1, t2" and multiline
    "FROM\n  table1 alias1,\n  table2 alias2" patterns.

    Fixes TPC-H q07, q08 (nation n1, nation n2) and q16 (partsupp, part).
- Context-aware comma qualification in prefix_tables, fix q54 ambiguity

The comma check now only qualifies table names when the trailing comma
    is inside a FROM/JOIN clause (not in SELECT, ORDER BY, GROUP BY, etc.).
    Scans the full output context for the last SQL clause keyword to
    determine which clause type we're in.
- Parenthesis-aware clause context for prefix_tables

Strip parenthesized subexpressions before checking SQL clause context,
    so that WHERE/SELECT inside subqueries don't interfere with outer FROM
    clause detection. Fixes TPC-DS queries with comma-separated tables
    after subquery aliases (e.g. "FROM (SELECT ... WHERE ...) sub, item").
- Rename column aliases that clash with table names in benchmark queries

Revert context-aware comma check (broke subquery qualification) and
    instead fix the root cause: column aliases named identically to table
    names cause prefix_tables to over-qualify.

    - TPC-DS q49: "item" alias → "item_sk" (avoids clash with item table)
    - TPC-DS q51: "web_sales"/"store_sales" aliases → "web_cume"/"store_cume"
    - TPC-DS q54: disambiguate c_customer_sk with customer.c_customer_sk
    - TPC-E broker_volume: "trade_type" alias → "tt_name" (avoids clash)
- Qualify remaining ambiguous c_customer_sk in TPC-DS q54

### Documentation

- Clarify credential vending TODO with Step 5 dependency tracking

### Features

- Add explicit Trino value serialization for Utf8View, Decimal, Time, Binary
- Complete Trino type mapping for all Arrow data types

Expand arrow_to_trino_type to handle Null, Float16, Utf8View, BinaryView,
    FixedSizeBinary, Time32/64, Duration, Interval, List, LargeList,
    FixedSizeList, Map, and Struct, ensuring Trino JDBC clients receive correct
    type strings for all Arrow DataType variants.
- Extend benchmark comparator with Timestamp, Time, Decimal256, Utf8View, human-readable dates
- Implement GetTableTypes and GetXdbcTypeInfo in Flight SQL for JDBC/BI compatibility
- Implement Flight SQL DoPut for Arrow data ingestion and statement updates

Adds DoPut support to the Flight SQL server: do_put_statement_ingest streams
    Arrow RecordBatches directly into an existing Iceberg table via fast-append, and
    do_put_statement_update executes a SQL statement and returns the affected row count.
    Also exposes write_handler() accessor on QueryHandler for use by the Flight SQL layer.
- Implement DoPut prepared statement query/update for JDBC compatibility

### Testing

- Add unit tests for Flight SQL helpers (FetchResults, table types, XDBC type info)

Adds a #[cfg(test)] mod tests block with 14 tests covering FetchResults
    protobuf encode/decode roundtrips, as_any/type_url correctness,
    batches_to_stream with empty and single-batch inputs, table types batch
    content and schema, XdbcTypeInfoDataBuilder with all 13 registered types,
    and SqlInfoDataBuilder server metadata fields.
- Add unit tests for SessionManager

Adds a #[cfg(test)] mod tests block to session_manager.rs covering all
    key behaviours: session creation and retrieval by ID, unknown ID returns
    None, expired-token eviction on get_session, remove_session (present and
    absent), sweep_expired_sessions for idle/absolute/mixed/empty cases, and
    concurrent DashMap access safety.
- Add unit tests for Authenticator config and OAuth client

Add backend-selection tests for Authenticator::new (client_credentials vs OIDC
    path), cache emptiness on fresh construction, and refresh_buffer_secs
    propagation. Add OAuthClient construction tests (valid params, accept_invalid_certs,
    empty endpoint/credentials) and TokenResponse deserialization tests.
- Extend system_jdbc tests with type mappings and table schemas

Add exhaustive iceberg_type_to_jdbc tests covering all 16 primitive types
    (Int, Float, Double, Decimal, Date, Time, Timestamp, Timestamptz, TimestampNs,
    TimestamptzNs, String, Uuid, Fixed, Binary, Boolean, Long) plus a complex-type
    fallback test. Add schema column count and column name tests for build_types_table
    (18 columns) and build_catalogs_table (1 column). Total: 24 tests from 6.
- Extend write_handler tests with sql_type_to_arrow and schema conversion

Adds 29 new unit tests covering sql_type_to_arrow (all SQL types: boolean,
    integers, floats, strings, binary, date, timestamp precision tiers, decimal
    variants, unsupported type error path), arrow_schema_to_iceberg (field IDs,
    nullable/required mapping, wide schemas, decimal, binary, temporal types,
    millisecond timestamp rejection), and ingest table-name parsing (2-part,
    3-part catalog prefix, and invalid input error cases).

## [0.19.0] — 2026-03-26

### Documentation

- Update roadmap and nextsteps for Trino JDBC compat work

## [0.18.0] — 2026-03-26

### Bug Fixes

- Only qualify table names in FROM/JOIN context, not column aliases
- Smarter comma handling in prefix_tables + fix TPC-BB q08 alias in WHERE

- Only treat commas as table-context when preceded by a qualified table
      (prevents qualifying column names in GROUP BY/ORDER BY comma lists)
    - TPC-BB q08: wrap in subquery so computed alias total_lifetime_value
      can be used in WHERE
- Ensure minimum 1 row per table at small scale factors
- Ensure minimum 1 row per table at small scale factors + gitignore results
- Use scaled() helper to ensure minimum 1 row at small scale factors

Adds super::scaled(scale, base) that returns max(scale*base, 1).
    Applied to all generators — prevents empty tables at SF0.01.
- Always include infoUri in Trino-compat responses
- Add typeSignature to Trino column metadata

The Trino JDBC driver calls ClientTypeSignature.getRawType() on every
    column. When typeSignature is missing from the JSON response, the field
    deserializes as null causing NPE. Add TrinoTypeSignature struct with
    rawType and arguments, populated for all column types including
    parameterized decimal(p,s).
- Add required arguments to varchar/varbinary type signatures

The Trino JDBC driver accesses typeSignature.arguments[0] for varchar
    and varbinary types to determine display size. Empty arguments array
    causes ArrayIndexOutOfBoundsException. Add max-length argument
    (2147483647) for varchar/varbinary and precision argument (6) for
    timestamp types.
- Add missing JDBC columns to system.jdbc.columns

DBeaver's getColumns() query selects all 24 JDBC-standard columns.
    Add the 9 missing columns (buffer_length, sql_data_type,
    sql_datetime_sub, char_octet_length, scope_catalog, scope_schema,
    scope_table, source_data_type, is_generatedcolumn) as nullable fields.

### Features

- Add benchmark-mvp.sh for MVP environment
- Add --catalog and --namespace flags to load and test commands

Allows specifying the catalog (e.g. main_warehouse) and namespace
    for multi-catalog environments. Tables are created as
    <catalog>.<namespace>.<table> when --catalog is set.
- Add --catalog to benchmark-mvp.sh (defaults to main_warehouse)
- Add system.jdbc.* virtual tables for Trino JDBC metadata browsing
- Implement Flight SQL GetDbSchemas and GetTables metadata

The do_get_schemas and do_get_tables handlers were stubbed, returning
    empty results. DBeaver uses these to browse catalog structure via
    Flight SQL. Implement both by executing SHOW SCHEMAS and querying
    information_schema.tables through the session-authenticated query
    handler.
- Flight SQL prepared statements, JWT auth, metadata browsing, type fixes

Flight SQL protocol improvements for DBeaver/JDBC compatibility:

    - Accept raw JWT bearer tokens on Flight endpoint (not just session IDs
      from handshake), matching the Trino-compat endpoint behavior
    - Implement prepared statements (create, get_flight_info, do_get) so
      DBeaver can execute queries via Flight SQL JDBC driver
    - Fix do_get_tables to read table_name (column 1) not namespace (column 0)
      from SHOW TABLES output
    - Use batches_to_stream for metadata responses (catalogs, schemas, tables)
      instead of FlightDataEncoderBuilder which caused client decode errors
    - Return empty results for primary/exported/imported keys and cross
      references instead of UNIMPLEMENTED errors
    - Add UInt8/16/32/64 support to Trino type mapper and value serializer
    - Map UInt32/UInt64 to bigint to prevent signed overflow in JDBC clients
    - Fix Trino timestamp serialization: space separator + fractional seconds
      instead of ISO 8601 T-separator that JDBC driver rejects

## [0.17.0] — 2026-03-24

### Bug Fixes

- Use tr instead of bash 4 uppercase syntax for macOS compatibility
- Add missing cs_coupon_amt column in TPC-DS catalog_sales and web_sales generators
- Prepend http:// to Flight SQL host if scheme is missing
- Split host and port in benchmark scripts to match CLI interface
- Specify --bin sqe-coordinator in benchmark scripts
- Pre-build coordinator binary, increase startup timeout to 300s
- Use PID-based log file instead of mktemp to avoid stale file conflicts
- Pass config as positional arg, not --config flag
- Route execute_update through execute path (SQE handles DDL via query)
- Use Flight SQL handshake auth (username + empty password for client_credentials mode)
- Offset test stack ports by +10000 to avoid conflicts with production stack

- Flight SQL: 50051 → 60051
    - Trino HTTP: 8080 → 18080
    - Polaris: 8181 → 18181
    - RustFS/S3: 9000 → 19000
    - Prometheus: 9090 → 19090
- Use dot-free namespace names (tpch_sf0_01 instead of tpch_sf0.01)
- Resolve benchmark query compatibility issues from first live test

- ClickBench: double-quote all CamelCase column names (42 queries)
    - TPC-DS: fix q05 (duplicate aliases), q08 (d_zip→ca_zip), q49 (JOIN syntax),
      q54 (CTE reference), q59 (ambiguous d_week_seq), q64 (typo), q90 (div by zero)
    - TPC-C: rename history→hist to avoid Polaris reserved word conflict
    - TPC-BB: load extra tables into tpcds namespace instead of separate tpcbb namespace
- Smarter table name qualifying — only after FROM/JOIN, not aliases

Replaces naive string replacement with a tokenizer that only qualifies
    table names after SQL keywords (FROM, JOIN, TABLE, INTO). Prevents
    false positives like qualifying column aliases (AS call_center) or
    column references. Also adds summary table output to benchmark-test.sh.
- Add gRPC keepalive and timeout to Flight SQL bench client

- HTTP/2 keepalive every 10s (prevents idle connection drops)
    - 5-minute per-query timeout (prevents infinite hangs)
    - 10-second connection timeout
- Stream test output in real-time instead of capturing (shows progress)
- Add per-query timeout in test runner (prevents infinite hangs)

Uses tokio::time::timeout wrapping the entire execute call. Defaults
    to 60s minimum, uses the query's -- timeout: header if larger.
    Timed-out queries are reported as ERROR with timeout message.
- Increase default per-query timeout to 120s
- Use tokio::select! for per-query timeout (works with stuck gRPC streams)
- Return empty gRPC stream for 0-row queries instead of Schema::empty()

When a query returns 0 rows, the previous code sent an empty-schema
    FlightData message via FlightDataEncoderBuilder. This caused the
    FlightRecordBatchStream decoder on the client side to hang, because
    get_flight_info advertised the real query schema but do_get sent a
    0-column schema. Now returns a truly empty stream — the client sees
    the stream close immediately and reports 0 rows.
- Fresh gRPC connection per query to avoid HTTP/2 stream accumulation

Reusing a single FlightSqlServiceClient across 50+ queries caused the
    HTTP/2 connection to accumulate state and eventually hang. Now each
    query gets a fresh channel+client (auth token is reused). Slightly
    more overhead per query (~10ms) but eliminates connection-level hangs.
- Handle double-quoted identifiers in prefix_tables tokenizer

The tokenizer had an infinite loop when encountering " characters
    (e.g. AS "30 days" in TPC-DS q50). The " was excluded from
    punctuation but not handled as a quoted string, so i never advanced.
    Now properly skips quoted identifiers by reading until the closing ".
- Replace tokenizer-based prefix_tables with word-boundary matching

The tokenizer approach missed table names in subqueries, correlated
    FROM clauses, and some comma-separated lists. The new approach uses
    simple word-boundary matching: qualifies any standalone occurrence of
    a known table name that isn't preceded by AS, a dot, or an underscore.
    Handles quoted identifiers and longest-first matching.
- Remove unused Schema import
- Include TPC-DS tables in prefix_tables for TPC-BB queries
- Gate flight debug logs behind BENCH_DEBUG env var

### Documentation

- Add sqe-bench benchmark suite design spec
- Add read_parquet TVF prerequisite and update load command in bench spec
- Add sqe-bench Phase 0+1 implementation plan
- Add benchmark suite documentation, book chapters, and blog post

- README.md: add Benchmarks section with supported suite table and quick-start commands; mark benchmark suite done in roadmap checklist
    - nextsteps.md: update status date, add completed Step 3b entry for sqe-bench with first-run results; add Step 3b to implementation order rationale
    - docs/book/src/SUMMARY.md: add read_parquet TVF and Benchmark Suite chapters under Features
    - docs/book/src/features/benchmarks.md: new chapter covering all six benchmarks, generate/load/test pipeline, result statuses, JSON report format, CI integration, and BenchmarkGenerator trait for adding new benchmarks
    - docs/book/src/features/read-parquet.md: new chapter documenting read_parquet() TVF syntax, local and S3 usage, glob patterns, CTAS patterns, and implementation notes
    - docs/book/src/development/testing.md: add Benchmark Testing section explaining sqe-bench alongside unit and integration tests
    - docs/blog/2026-03-24-benchmark-suite.md: blog post covering why we built it, benchmark selection rationale, generate/load/test architecture, read_parquet as loader, first TPC-H results, and lessons learned

### Features

- TPC-H query files (22 queries) and schema DDL

Add all 22 standard TPC-H benchmark queries (Q01–Q22) with DataFusion-
    compatible SQL (DATE literals, INTERVAL syntax) and the 8-table TPC-H
    schema DDL. Table names are unqualified so the benchmark runner can
    prepend the namespace at runtime.
- SSB query files (13 queries) and schema DDL

Add all 13 standard Star Schema Benchmark queries across 4 flights
    (Q1.1-Q1.3, Q2.1-Q2.3, Q3.1-Q3.4, Q4.1-Q4.3) and the 5-table SSB
    schema DDL. The date dimension table is named dim_date to avoid the SQL
    reserved keyword conflict. Table names are unqualified so the benchmark
    runner can prepend the namespace at runtime.
- Scaffold read_parquet TVF struct

Add the ReadParquetFunction skeleton: struct, module export in lib.rs,
    and datafusion-expr + object_store dependencies in sqe-catalog/Cargo.toml.
- Register read_parquet TVF on every SessionContext

Wire ReadParquetFunction into create_session_context so every DataFusion
    context automatically exposes read_parquet() to end users.
- Implement read_parquet TVF with S3 and local file support

Update Cargo.lock to reflect new datafusion-expr and object_store
    dependencies added to sqe-catalog.
- Scaffold sqe-bench crate with CLI

Add the sqe-bench crate to the workspace with three subcommands —
    Generate, Load, and Test — wired up via clap derive macros.  Workspace
    gains clap (with env feature), csv, and rand as shared dependencies.
- BenchmarkGenerator trait and Parquet writer

Introduce the BenchmarkGenerator trait (tables/generate_table), the
    GenerateStats and TableDef types, a Snappy-compressed Parquet file writer
    that splits at 128 MB, and stub implementations for TPC-H and SSB that
    will be filled out in Task 7.
- TPC-H data generator (8 tables)

Implement TpchGenerator with all 8 TPC-H tables (region, nation,
    supplier, customer, part, partsupp, orders, lineitem) using deterministic
    StdRng seeding and batch generation (10K rows/batch) to bound memory.
    Parquet output via existing parquet_writer. Also fix tls-ring feature
    for tonic so the client/flight module compiles.
- Load command — creates Iceberg tables via read_parquet + CTAS

Implements the `load` subcommand in sqe-bench. Resolves Task 11: connects
    to the SQE coordinator, creates a namespaced schema, and loads each
    benchmark table using a CTAS from `read_parquet`, with optional S3
    credential passthrough and a `--clean` flag for idempotent reloads.
- Result comparison engine for benchmark validation

Implements src/compare.rs (Task 14): parses expected CSV files, converts
    Arrow RecordBatches to string rows, sorts both sides for order-independent
    comparison, and applies configurable epsilon tolerance for floating-point
    columns. Returns Pass/Diff/Fail status. Includes six unit tests covering
    exact match, row-count mismatch, float within/outside epsilon, string
    mismatch, and order-independent matching.
- Test command — runs queries, validates results, generates reports

Implements src/test.rs and src/report.rs (Task 15): loads .sql files from
    benchmarks/queries/<benchmark>/, parses -- name/requires/timeout header
    comments, qualifies table names with the scale-factor namespace, executes
    each query via BenchClient, compares against optional expected CSV files,
    and collects Pass/Fail/Diff/Skip/Error results. Produces a terminal summary
    table and writes a timestamped JSON report under benchmarks/results/.
    Unit tests cover header parsing, ID normalisation, table prefixing, result
    counting, and JSON report serialisation.
- Trino HTTP client for sqe-bench
- SSB data generator (5 tables)
- Add benchmark-test.sh — generates, loads, and tests all benchmarks
- TPC-BB data generator (web_clickstreams + product_reviews)

Add TpcbbGenerator to sqe-bench with two TPC-BB-specific tables:
    - web_clickstreams  (SF×4,000,000): wcs_click_date_sk, wcs_click_time_sk,
      wcs_sales_sk, wcs_item_sk, wcs_web_page_sk, wcs_user_sk,
      wcs_referrer_url, wcs_search_keywords
    - product_reviews   (SF×100,000): pr_review_sk, pr_review_date,
      pr_review_time, pr_review_rating, pr_item_sk, pr_user_sk,
      pr_order_sk, pr_review_content, pr_title

    Register "tpcbb" in get_generator(). Generator notes that TPC-DS base
    tables must be generated separately with `sqe-bench generate tpcds`.
    Includes unit tests for schemas, row counts, null distribution, rating
    ranges, and parquet output. All 70 tests pass; clippy clean.
- TPC-BB query files (10 SQL-only queries) and schema DDL

Add 10 pure-SQL TPC-BB (BigBench) benchmark queries under
    benchmarks/queries/tpcbb/ and DDL for the 2 additional tables under
    benchmarks/schemas/tpcbb.sql.

    Queries cover:
      q01 – top products by revenue from web clickstreams
      q02 – items with high return rate per category
      q03 – product review sentiment by category
      q04 – abandoned shopping carts
      q05 – customer segmentation by purchase behavior
      q06 – revenue by marketing channel (store / catalog / web)
      q07 – product affinity analysis (co-viewed items)
      q08 – customer lifetime value across channels
      q09 – seasonal sales patterns with YoY comparison
      q10 – cross-channel customer behavior

    All queries join TPC-BB tables (web_clickstreams, product_reviews) with
    TPC-DS dimension tables using unqualified names.
- TPC-C query files and schema DDL

Add 8 SQL files under benchmarks/queries/tpcc/ covering all 5 TPC-C
    transaction types plus 3 analytical read queries:

    Read-only queries (runnable with sqe-bench test):
      order_status.sql     — look up customer's last order and line items
      stock_level.sql      — count items below threshold in recent orders
      warehouse_summary.sql — aggregate revenue and order stats per warehouse
      customer_balance.sql  — top 100 customers ranked by balance
      district_orders.sql   — order counts and delivery status per district

    Write transactions (annotated, read-only equivalents provided):
      new_order.sql   — requires: insert, update
      payment.sql     — requires: update
      delivery.sql    — requires: delete, update

    Add benchmarks/schemas/tpcc.sql with full DDL for all 9 TPC-C tables
    including primary key constraints and per-column type/length annotations.
- ClickBench data generator (hits table)

Add ClickBenchGenerator with a 105-column synthetic hits table matching
    the official Yandex web analytics schema. Small mode generates SF×100,000
    rows of deterministic random data for correctness testing; register in
    get_generator under the "clickbench" key.
- ClickBench query files (43 queries) and schema DDL

Add q00.sql through q42.sql covering the official ClickBench query suite:
    simple aggregations, LIKE filters, GROUP BY with HAVING, ORDER BY, COUNT
    DISTINCT, and a wide 90-column SUM stress test (q29). Add clickbench.sql
    DDL with the full 105-column hits table definition. EventTime-based
    minute extraction in q18 uses integer arithmetic for DataFusion compat.
- TPC-E data generator (33 tables)

Add TpceGenerator covering all 33 TPC-E tables across the customer,
    broker, market, company, and reference domains. Scale factor is in
    units of 1,000 customers (matching the TPC-E specification). Fixed
    tables (status_type, trade_type, exchange, sector, industry, taxrate,
    commission_rate, charge, zip_code) use deterministic synthetic data;
    all scaled tables use per-table seeded RNGs for reproducibility.

    Includes 20 unit tests covering row counts, schema column counts,
    symbol generation, and get_generator registry integration.
- TPC-E query files and schema DDL

Add 11 query files covering the 10 TPC-E transaction types as
    read-only analytical queries (write-path transactions are annotated
    with `-- requires:` headers so the test runner skips them). Also
    add benchmarks/schemas/tpce.sql with DDL for all 33 tables.
- TPC-DS data generator (24 tables)

Add TpcdsGenerator with all 24 TPC-DS tables (7 fact, 17 dimension).
    Uses a generic generate_batches helper with ColVal enum to reduce
    repetition. Schema definitions use a compact schema() helper.
    Registered as "tpcds" in get_generator. 104 tests pass, clippy clean.
- TPC-DS query files (99 queries) and schema DDL

Add all 99 TPC-DS benchmark queries (q01–q99) in DataFusion-compatible
    SQL, plus the full 24-table schema DDL. Each query carries -- name:
    and -- timeout: headers; queries requiring ROLLUP are marked with
    -- requires: rollup (q18, q27, q36, q67, q70, q86).
- Add benchmark-generate-all.sh — generate all datasets locally
- Add benchmark-load.sh — generate and load data into test stack
- Add OAuth2 client_credentials auth for benchmark tool

The lightweight test stack uses Polaris OAuth2 (not Keycloak OIDC), so the
    benchmark tool needs to fetch a bearer token via client_credentials grant
    before connecting to Flight SQL. Adds --token-endpoint, --client-id,
    --client-secret flags to load and test commands.
- Add per-benchmark summary table to benchmark-test.sh output
- Add per-query debug logging (BENCH_DEBUG=1 shows full SQL)

### Testing

- Integration test for read_parquet with local Parquet files

Adds test_read_parquet_local_file to the sqe-coordinator integration
    test suite: writes a 3-row Parquet file via ArrowWriter into a tempdir,
    loads it into an Iceberg table with CTAS + read_parquet(), queries it
    back, verifies the round-trip, and cleans up. Also adds parquet and
    tempfile to [dev-dependencies].

### Debug

- Add flight client step tracing to find hang location

## [0.16.4] — 2026-03-22

### Bug Fixes

- **security:** Sanitize error messages in Trino compat HTTP responses

Replace raw internal error strings (e.to_string()) in Trino HTTP responses
    with generic messages ("Query execution failed", "Authentication failed").
    Full errors are still logged server-side at warn! level so nothing is lost
    for operators, but catalog paths, S3 URIs, and OIDC provider details are no
    longer leaked to clients.
- **security:** Stop logging session IDs at info level

Session IDs act as bearer tokens; demote them from info! to debug! to
    prevent accidental exposure in centralised log systems. Operator logs
    still show the username; session_id remains available at debug level.
- **security:** Redact S3 credentials in ScanTask Debug output

Remove derived Debug from ScanTask and replace with a manual impl that
    substitutes [REDACTED] for s3_access_key and s3_secret_key, and either
    [REDACTED] or [empty] for s3_session_token, preventing credentials from
    appearing in logs or debug output.
- **security:** Authenticate worker heartbeat requests with shared secret

Add a `worker_secret` field to `CoordinatorConfig` (empty by default for
    backwards compatibility). When configured, the coordinator validates the
    `x-sqe-worker-secret` metadata header on incoming heartbeat actions,
    returning `Status::unauthenticated` if the secret is absent or incorrect.
    This prevents unauthenticated clients from registering arbitrary worker URLs.
- Handle server bind failures gracefully instead of panicking

Replace .unwrap() calls on TcpListener::bind() and axum::serve() in both
    the metrics server and Trino-compat server with match/if-let error handling
    that logs via tracing::error! and returns cleanly, preventing silent panics
    when a port is already occupied.
- Propagate HTTP errors from list_views and load_view_sql instead of silent empty results
- Log view list failures and table load errors in schema_provider

Replace silent `if let Ok` discard of `list_views` errors with a `match`
    that logs at `error!` level, and capture the error value in the `Err(_)`
    arm of `load_table` so the debug log includes the actual error message.
- AuditLogger returns Result, logs write failures instead of silently dropping
- Truncate OIDC error bodies, add SqeError::is_not_found for structured error matching

Truncate OIDC error response bodies to 500 chars in both exchange_credentials
    and refresh_token to prevent unbounded allocation from large HTML error pages.
    Add SqeError::is_not_found() checking for "HTTP 404" in Catalog errors, and
    replace brittle e.to_string().contains("404") in catalog_ops drop_view with
    the structured is_not_found() check.
- Add TTL eviction to Trino paginated result cache to prevent memory leaks

Adds a `created_at: Instant` field to `PaginatedResult` and a background
    sweep task (runs every 60 s) that removes entries older than 5 minutes,
    preventing abandoned queries from leaking memory indefinitely.
- Propagate table_exists errors, raise info_schema error log levels to warn

Replace unwrap_or(false) on table_exists in drop_table_if_exists with explicit
    match so network/auth errors are propagated as SqeError::Catalog instead of
    silently treated as "table doesn't exist". Change all info_schema debug! calls
    for table listing and loading failures to warn!, and add warn! to the previously
    silent Err(_) => continue arm in build_columns_table.
- **security:** Gate S3 allow_http on config instead of hardcoding true

Add `s3_allow_http: bool` (default false) to `StorageConfig` and `ScanTask`,
    then thread the value through to `AmazonS3Builder::with_allow_http` in the
    worker executor. Previously, HTTP was unconditionally allowed for all S3
    connections including production; now it defaults to false (HTTPS required)
    and must be explicitly enabled for dev/test environments such as local MinIO.

    Also adds `SQE_STORAGE__S3_ALLOW_HTTP` env-override support and a test
    asserting the secure default.
- Propagate table_exists errors and raise SHOW TABLES log level to warn

Replace unwrap_or(false) on table_exists in drop_table_if_exists with an
    explicit match arm so network/auth errors are propagated as SqeError::Catalog
    rather than silently masking the failure. Also raise the debug! for namespace
    table listing failures in handle_show_tables to warn!.
- Add List/Struct/Map Iceberg type mappings, warn on unknown fallback

Replace the silent string fallback in arrow_type_to_iceberg with proper
    Iceberg JSON schema representations for List/LargeList, Struct, and Map
    Arrow types. Unknown types now emit a tracing::warn instead of silently
    corrupting view metadata schemas. Adds unit tests for all four new branches.

## [0.16.3] — 2026-03-22

### Miscellaneous

- Remove stale worktree refs and update mdbook docs

Remove 9 orphaned .claude/worktrees submodule references and update
    mdbook documentation for auth-flow, security, configuration, testing,
    observability, and add trino-compatibility page.

## [0.16.2] — 2026-03-22

### Features

- **otel:** Propagate W3C trace context to Polaris HTTP calls

Add trace_context_http_headers() helper that collects traceparent/tracestate
    from the active tracing span, and inject these headers in all 4 Polaris REST
    API call sites (create_view, list_views, load_view_sql, drop_view). This
    enables distributed traces that span SQE → Polaris when OTLP is configured.

## [0.16.1] — 2026-03-22

### Bug Fixes

- Enable tls-ring feature for tonic and make trino test self-contained

The TLS module in sqe-coordinator uses tonic's Identity, ServerTlsConfig,
    and Certificate types which are gated behind the _tls-any feature. Added
    tls-ring feature to the tonic dependency to fix the build.

    Made test_trino_http_query self-contained by spinning up an in-process
    Trino compat server instead of expecting an external one on port 8080.

    Also removed unused .enumerate() in scheduler to fix clippy warning.

## [0.16.0] — 2026-03-22

### Documentation

- Update README, nextsteps, and CLAUDE.md for Step 3 completion

- Mark OSS security hardening as complete in README roadmap
    - Update nextsteps.md: Step 3 done (51/51), shift NEXT to Steps 4+5
    - Add "After Completing Work" section to CLAUDE.md requiring updates
      to README.md, nextsteps.md, and openspec tasks on every feature
- Add blog post — why we replaced Trino with a Rust SQL engine

Covers the journey from maintaining a 2M-line Trino fork (DCAF branch)
    to building SQE on DataFusion + iceberg-rust. Discusses bearer token
    passthrough architecture, what works today, what's blocked on upstream,
    and the roadmap ahead.
- Add two blog posts for planned features

1. "Making SQE Work Everywhere" — pluggable auth (5 providers,
       chain semantics, hot-reload API keys) and pluggable catalogs
       (Glue, Nessie, HMS, storage-only, multi-cloud storage, Delta Lake)

    2. "When Your SQL Engine Understands Meaning" — semantic AI layer
       with RDF/SPARQL on Iceberg, ISO GQL property graphs, Lance vector
       search, and AI-native interfaces (CLI-first, REST/OpenAPI, MCP,
       TypeScript client)
- Add blog post on AI-assisted development process

Covers the four-phase approach: brainstorm → plan & review → specify
    (OpenSpec) → build in parts with parallel agents. Includes concrete
    timelines from security hardening (51 tasks in one day), the OpenSpec
    format, why atomic tasks and continuous verification matter, and
    practical advice for teams adopting AI-assisted engineering.

### Features

- **security:** Implement OSS security hardening (47/51 tasks)

Vendor-neutral naming:
    - Rename keycloak.rs → oidc_password.rs with deprecated re-export
    - Update all log messages, comments, and docs to use OIDC language
    - Remove MinIO references; use generic S3-compatible language
    - Update CLAUDE.md and README.md

    Security controls:
    - Config validation: fail-fast on missing required fields and port conflicts
    - Rate limiting: per-user + global token buckets via governor crate
    - Query timeouts: tokio::time::timeout wrapping execution, per-role overrides
    - Session lifecycle: idle timeout + absolute timeout with background sweeper
    - Query cancellation: CancellationToken registry with Flight cancel handler
    - Audit log: add session_id, query_hash (SHA-256), client_ip fields
    - Error sanitisation: client_message() hides internals in production mode

    TLS (section 4) deferred to pluggable-auth change.
    Health endpoints already existed from core engine work.

    47/51 tasks complete. 4 remaining are deferred TLS tasks.
- **tls:** Add optional TLS support for Flight SQL listener

- Add [coordinator.tls] config: cert_file, key_file, ca_file
    - When cert+key are set, tonic server enables TLS automatically
    - When ca_file is set, mTLS client certificate verification is enabled
    - When no TLS configured, server runs in plaintext (dev mode)
    - Config validation: partial cert/key detection, missing file checks
    - Wired into coordinator (main.rs, sqe_server.rs) and worker startup
    - Env overrides: SQE_TLS__CERT_FILE, SQE_TLS__KEY_FILE, SQE_TLS__CA_FILE
    - Unit tests for config validation and TLS builder
    - Updated sqe.toml.example with commented TLS section

    All 51/51 oss-security-hardening tasks now complete.

## [0.15.2] — 2026-03-22

### Documentation

- Update nextsteps.md and README.md to reflect Step 2 completion

- Mark Step 2 (core engine) as effectively complete (99/103 tasks)
    - Update roadmap: check off distributed execution, predicate pushdown,
      Trino compat, worker observability, integration tests
    - Update DataFusion version 51→52, iceberg-rust 0.8→0.9
    - Add upcoming roadmap items: OSS security hardening, pluggable auth,
      pluggable catalogs, semantic AI layer
    - Summarize all tasks completed since last update

## [0.15.1] — 2026-03-22

### Features

- **tests:** Add sqe-auth unit tests, integration tests, and e2e test script

- Add 17 unit tests for sqe-auth: token cache CRUD + expiry detection,
      Keycloak JWT role extraction (valid/malformed/missing fields), URL
      construction
    - Add Keycloak auth integration tests: multi-user auth with role
      validation, token refresh, invalid credentials rejection
    - Add catalog visibility test for different user roles
    - Add Trino HTTP compat endpoint test (v1/info, v1/statement with
      Basic auth and pagination)
    - Add scripts/e2e-test.sh for full-stack end-to-end validation
      (Flight SQL + Trino HTTP)
    - Update tasks.md: mark 27 tasks as complete (11 previously
      implemented features + 16 tests now covered)
    - Add reqwest + base64 dev-dependencies to sqe-coordinator

    Remaining 4 tasks are blocked on iceberg-rust Merge-on-Read (ETA Q3 2026).

    Companion change needed: register sqe-client in data-platform
    quickstart/assets/keycloak-config/realm-config.json

## [0.13.1] — 2026-03-22

### Documentation

- Update CLAUDE.md with common commands and git workflow

## [0.29.0] — 2026-03-21

### Bug Fixes

- **credentials:** Address review findings for credential refresh push

- Add connect/request timeouts to prevent refresh loop stalls
    - Redact secrets in Debug impl for RefreshableCredentials
    - Unregister fragments from tracker on do_get failure
    - Safe index access in build_object_store_with_creds
- **distributed:** Address review findings for fragment failure handling

- Replace unwrap() with expect() in scheduler assignment path
    - Add exponential backoff between retry attempts
    - Enter OTel dispatch span for proper trace recording
- **worker:** Use correct DataFusion 52 APIs and wire SessionContext into executor

- Replace non-existent DiskManagerMode/DiskManagerBuilder with correct API
    - Use with_disk_manager_disabled() and with_disk_manager_specified()
    - Pass SessionContext through to WorkerFlightService and executor
    - Memory pool now actually enforced during query execution
- **catalog:** Address review findings for predicate pushdown

- Remove unnecessary unsafe block in IcebergRecordBatchStream
    - Fix TimestampNanosecond conversion to use microsecond precision
    - Document Inexact dependency in AND partial pushdown

### Features

- **distributed:** Implement credential refresh push from coordinator to workers (tasks 7.10, 9.6)

Coordinator side (task 7.10):
    - Add credential_refresh module with CredentialRefreshTracker that monitors
      active fragment credential expiry and triggers refresh before expiration
    - Add push_credentials_to_worker() that sends refreshed credentials to
      workers via Arrow Flight do_action("refresh_credentials")
    - Add refresh_expiring_credentials() orchestrator with pluggable credential
      vending callback
    - Extend DistributedScanExec with optional credential_expiry and
      credential_tracker to register fragments on dispatch

    Worker side (task 9.6):
    - Add credential_channel module with CredentialStore using tokio watch
      channels for per-fragment credential updates
    - Extend WorkerFlightService to handle do_action("refresh_credentials"),
      deserializing RefreshableCredentials and publishing to the store
    - Update executor::execute_scan to accept an optional credential watch
      receiver, checking for refreshed credentials before each file read
      and rebuilding the S3 ObjectStore with new credentials transparently

    Credential payload (JSON): fragment_id, access_key_id, secret_access_key,
    session_token, expiry (RFC 3339). Identical schema on both sides.
- **coordinator:** Implement fragment failure handling with retry and local fallback (task 7.11)

When a worker fails mid-execution, the coordinator now:
    1. Marks the worker as unhealthy immediately in the registry
    2. Re-assigns the failed fragment to another healthy worker (up to max_retries, default 2)
    3. Falls back to local execution if no healthy workers remain

    Adds WorkerRegistry.mark_unhealthy() for immediate removal from the
    active pool (vs mark_failed's 3-failure threshold), and a LocalExecutor
    trait for coordinator-side fallback execution.
- **worker:** Implement configurable memory limit and spill-to-disk (task 9.7)
- **catalog:** Implement Iceberg predicate pushdown from DataFusion filters (task 6.3)
- **coordinator:** Wire credential refresh background task into startup

- Add start_credential_refresh_task() that spawns a tokio loop polling
      every 60s for fragments with credentials approaching expiry
    - Add credential_tracker field to QueryHandler (available for
      DistributedScanExec construction when distributed routing is wired)
    - Create and start the tracker in run_coordinator() when workers are
      configured
    - Credential vending callback is a placeholder (returns None) until
      catalog vending is implemented

## [0.8.0] — 2026-03-21

### Features

- **catalog:** Add OpenDal StorageFactory for write support

RestCatalog requires a StorageFactory to perform write operations
    (CREATE TABLE, INSERT). Without it, iceberg-rust fails with
    "StorageFactory must be provided for RestCatalog".

    Add iceberg-storage-opendal dependency and configure the S3
    OpenDalStorageFactory on the catalog builder.

## [0.10.0] — 2026-03-21

### Features

- **metrics:** Instrument workers with fragment, row, and byte counters (task 12.3)

Add WorkerMetricsRegistry to sqe-metrics with four Prometheus metrics:
    - sqe_worker_fragments_executed_total (counter)
    - sqe_worker_rows_scanned_total (counter)
    - sqe_worker_bytes_read_total (counter)
    - sqe_worker_fragment_duration_seconds (histogram)

    Introduce HasRegistry trait to make the metrics HTTP server generic,
    so both coordinator and worker registries can serve /metrics.

    Wire metrics into executor::execute_scan and WorkerFlightService,
    and start the Prometheus HTTP server in the worker binary.

## [0.13.0] — 2026-03-21

### Features

- **worker:** Implement heartbeat to coordinator at 5s interval (task 9.5)

Add a background tokio task in the worker that sends periodic heartbeat
    signals to the coordinator via Arrow Flight do_action("heartbeat"). The
    coordinator handles these heartbeats in do_action_fallback, updating the
    worker registry to mark the sending worker as healthy (or dynamically
    registering it if not already known).

    - New heartbeat module in sqe-worker with start_heartbeat_task()
    - WorkerRegistry.register_heartbeat() for heartbeat-driven registration
    - SqeFlightSqlService.do_action_fallback() handles "heartbeat" actions
    - Worker reads coordinator_url and heartbeat_interval_secs from config
    - Fix pre-existing clippy warnings in both crates

## [0.12.0] — 2026-03-21

### Features

- **observability:** Propagate OTel trace context to workers via Flight metadata (task 12.6)

## [0.11.0] — 2026-03-21

### Features

- **trino:** Implement result pagination and header handling (tasks 11.3, 11.7)

Task 11.3 - Result pagination:
    - Replace single CachedResult with PaginatedResult storing columns + row pages
    - POST /v1/statement returns first page with nextUri pointing to token 1
    - GET /v1/statement/{id}/{token} returns the requested page with nextUri
    - Last page omits nextUri and auto-cleans the result from the cache
    - DELETE /v1/statement/{id} removes paginated results
    - Configurable page size (default 1000 rows) via TrinoState.page_size

    Task 11.7 - Header handling:
    - Extract X-Trino-Catalog, X-Trino-Schema, X-Trino-User, X-Trino-Source headers
    - Add default_catalog, default_schema, source fields to Session
    - Apply extracted headers to session via with_catalog/with_schema/with_source
    - Log extracted header values on query submission

## [0.9.0] — 2026-03-21

### Features

- **coordinator:** Implement weighted fragment scheduler (task 7.6)

Add FragmentScheduler trait and WeightedScheduler implementation that
    assigns scan tasks to workers based on estimated cost (file count) and
    current worker load, using a largest-first bin-packing heuristic.
    Unhealthy workers are automatically skipped.

## [0.8.1] — 2026-03-21

### Bug Fixes

- Resolve clippy warnings and security advisories

Clippy fixes:
    - Add #[derive(Default)] to WorkerFlightService
    - Remove needless borrow in query_handler.rs
    - Extract FlightStream type alias to reduce type complexity

    Security advisories resolved:
    - RUSTSEC-2024-0437: upgrade prometheus 0.13→0.14 (protobuf 2.x→3.x)
    - RUSTSEC-2026-0049: update rustls-webpki 0.103.9→0.103.10

## [0.7.2] — 2026-03-20

### Documentation

- Remove completed Step 0 from nextsteps, update task 6.3 note
- Remove completed Step 0 from nextsteps, update task 6.3 note

## [0.7.1] — 2026-03-20

### Bug Fixes

- **writer:** Cast all columns to Iceberg-expected Arrow types before writing

DataFusion produces Timestamp(Nanosecond) for CURRENT_TIMESTAMP and timestamp
    literals, but Iceberg tables store timestamps as Timestamp(Microsecond). The
    Parquet writer rejects type mismatches, causing write failures.

    stamp_field_ids now derives the canonical Arrow schema from the Iceberg schema
    via schema_to_arrow_schema and casts any column whose type differs from the
    expected type. This covers all precision mismatches (ns/us/ms), timezone string
    differences (UTC vs +00:00), Date64 vs Date32, Time64 unit differences, and
    any other Arrow type mismatch between DataFusion output and Iceberg schema.

## [0.7.0] — 2026-03-20

### Features

- **deps:** Step 0 dependency alignment sprint

Upgrades all core dependencies to align DataFusion 52, iceberg-rust 0.9,
    and arrow 57 — required before any further feature work.

    Dependency changes:
    - arrow/arrow-flight/arrow-schema/arrow-array: 54 → 57
    - parquet: 54 → 57 (object_store API: new(store, path).with_file_size())
    - object_store: 0.11 → 0.12
    - tonic: 0.12 → 0.14 (required by arrow-flight 57)
    - prost: 0.13 → 0.14
    - iceberg/iceberg-catalog-rest: 0.8 → 0.9
    - iceberg-storage-opendal: 0.9 added (OpenDAL storage split out)
    - iceberg-datafusion: 0.9 added (bridges iceberg ↔ DataFusion arrow types)
    - datafusion-proto: 52 added to sqe-coordinator

## [0.6.4] — 2026-03-20

### Bug Fixes

- **test:** Suppress dead_code warning on fmt_val

### Documentation

- OSS planning — security audit, market research, openspec changes, implementation plans

Adds all planning artefacts for the OSS release and next development phases:

    - nextsteps.md — ordered roadmap from audit → core gaps → security hardening → pluggable auth/catalogs → semantic AI layer
    - docs/security_audit.md — static security audit of crates/; 1 critical (no TLS), 3 high, 4 medium findings with recommended fixes
    - docs/market-research-sql-engines-iceberg.md — feature matrix and weighted scoring of 14 OSS SQL engines for Iceberg
    - openspec/changes/oss-security-hardening — rename Keycloak→OIDC, remove MinIO, add rate limiting/TLS/audit/timeouts
    - openspec/changes/pluggable-auth — AuthProvider trait chain (OIDC ROPC, bearer, API key, anonymous, mTLS)
    - openspec/changes/pluggable-catalogs — CatalogBackend trait (Iceberg REST, Glue, Nessie, HMS, storage-only); multi-cloud object_store; Delta feature flag
    - openspec/changes/semantic-ai-layer — RDF/SPARQL on Iceberg, ISO GQL property graph, Lance vector search, CLI-first AI agent interface
    - docs/superpowers/plans/ — TDD implementation plans for all four openspec changes plus explain-queries
- Align nextsteps and core-engine tasks with upstream dependency changes

- Add Step 0 (dependency alignment sprint): iceberg-rust 0.9.0, DataFusion 52,
      apply_expressions() on IcebergScanExec, IcebergTableProvider split, Polaris header fix
    - Flag DELETE FROM and MERGE INTO as blocked on iceberg-rust MoR Epic #2186 (ETA Q3 2026)
    - Update task 6.6 to reference PhysicalExtensionProtoCodec + ArrowScanExecNode pattern
    - Add upstream watch list to rationale section
    - Mark 8.13/8.14 integration tests as blocked in nextsteps.md

## [0.6.2] — 2026-03-19

## [0.6.1] — 2026-03-19

### Bug Fixes

- Fmt_val handles UInt64/UInt32/Float32 so window function results display correctly
- Fmt_val handles StringViewArray (Utf8View) returned by DataFusion string functions
- Add Dockerfile syntax directive for BuildKit cache mount compatibility
- Remove unused Int64Array/Float64Array imports from explain.rs
- **cli:** Use +---+ table borders to match Arrow pretty format

### Documentation

- Add EXPLAIN / EXPLAIN ANALYZE / EXPLAIN FULL design spec
- Fix explain spec — correct error types, IcebergScanExec, metrics units, snapshot API
- Add EXPLAIN feature docs, openspec, and book page

### Features

- SQL compat test runner, shared test helpers, and Docker build time improvements

- Extract init_tracing/test_config_path/setup_handler/fmt_val/print_results
      into tests/common/mod.rs; integration_test.rs now uses mod common
    - Add file-driven SQL compat test runner (tests/sql_compat_test.rs) with
      rgsql-inspired block format (--- name / SQL / --- expect / values)
    - Add 5 SQL test files covering: literals+arithmetic, NULL handling,
      CTEs+subqueries, string functions, aggregations (45 test blocks)
    - Update integration-test.sh to run all sqe-coordinator test binaries
      (drop --test filter so sql_compat_test also executes)
    - Dockerfile: add BuildKit --mount=type=cache for cargo registry, git,
      and sccache — persists caches across Docker builds for faster rebuilds
    - Cargo.toml: narrow tokio from ["full"] to only required features
      (rt-multi-thread, macros, sync, time, signal, net, io-util)
    - Add .cargo/config.toml: pipelined compilation, mold linker for Linux
- **sql:** Add ExplainFull statement kind with pre-scan
- **coordinator:** Add ExplainHandler with plan/analyze/full

Implements Tasks 2-4: creates ExplainHandler in sqe-coordinator with three
    async methods (plan, analyze, full) that apply policy enforcement before
    producing explain output. Also exports IcebergScanExec from sqe-catalog and
    adds a stub ExplainFull match arm in QueryHandler to satisfy exhaustiveness.
- **coordinator:** Wire ExplainHandler into QueryHandler routing

Adds explain_handler field to QueryHandler, routes Utility/Explain to
    plan() or analyze() based on the analyze flag, routes ExplainFull to
    full(), and removes the old handle_explain() stub method.
- **cli:** Add tsv format and \format / SET FORMAT runtime switching
- EXPLAIN FULL now executes query and includes actual metrics alongside Iceberg stats
- **catalog:** Add BaselineMetrics to IcebergScanExec for elapsed_ms and output_rows

### Testing

- Add integration tests for EXPLAIN / EXPLAIN ANALYZE / EXPLAIN FULL
- Tighten policy_aware assertion to check logical plan row
- Use Arrow pretty_format_batches for test output

### Build

- Improve local and Docker build performance

- Remove unused tokio io-util feature from workspace
    - Make tonic transport feature explicit in workspace Cargo.toml
    - Add dev profile with debug=1 and split-debuginfo=unpacked for
      ~60% faster incremental rebuilds on macOS (27s → 10s)
    - Dockerfile: replace mold with lld (mold doesn't support macOS/Mach-O;
      lld works natively on both amd64 and aarch64 Linux)
    - Dockerfile: add TARGETARCH to BuildKit cache mount IDs so amd64 and
      aarch64 builds don't share compiled artifact caches in multi-arch builds
    - integration-test.sh: clean up stale /tmp/sqe-test-*.log before mktemp
      to prevent failures from aborted previous runs
- **docker:** Replace cargo install with pre-compiled binaries for faster builds

cargo install cargo-chef and cargo install sccache each compile from source,
    adding ~15 min to any build that invalidates the chef layer. Replace with:
    - lukemathwalker/cargo-chef base image (cargo-chef pre-installed)
    - pre-compiled sccache binary downloaded at build time (~30s vs ~10 min)

    SCCACHE_VERSION ARG makes it easy to bump without touching other layers.

## [0.5.0] — 2026-03-18

### Bug Fixes

- Resolve test config path relative to workspace root

Use CARGO_MANIFEST_DIR to build absolute path to tests/sqe-test.toml,
    fixing "No such file or directory" when cargo test runs from crate dir.
- Enable information_schema and add test_ns namespace

- Enable DataFusion information_schema on SessionContext so
      SELECT FROM information_schema.tables/schemata works
    - Add test_ns namespace to bootstrap script (used by integration tests)
- Add AWS_REGION to Polaris and use multi_thread tokio runtime in tests

- Polaris needs AWS_REGION for S3 credential vending (was returning 422)
    - Integration tests need multi_thread runtime for schema_provider block_on
- Skip Polaris credential subscoping for RustFS (no STS needed)

Polaris was trying to vend credentials via AWS STS AssumeRole, which
    fails with local S3 (RustFS). Set SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION
    to use static AWS credentials from env vars instead.
- Use JAVA_OPTS for Polaris SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION
- All integration tests passing — S3, CTAS, INSERT, DROP

- Switch to apache/polaris:latest and use endpoint/endpointInternal
      storage config format (avoids STS credential vending bug in 1.3.0)
    - Fix Arrow→Parquet write: stamp Iceberg field IDs onto DataFusion
      RecordBatch schema metadata before writing (PARQUET:field_id)
    - Fix DataFusion catalog resolution: set default catalog to warehouse
      name so 2-part names like test_ns.table resolve correctly
    - Fix Polaris healthcheck: use /q/health on management port 8182

### Features

- Add Iceberg view support, 37 integration tests, and test observability

- sqe-catalog: add list_views() and load_view_sql() via Polaris REST API
    - sqe-catalog: SqeSchemaProvider falls back to ViewTable when load_table
      fails; plan_view() creates a mini SessionContext to resolve view SQL
    - sqe-catalog: SqeCatalogProvider propagates warehouse to SqeSchemaProvider
    - sqe-coordinator/writer: stamp_field_ids() now checks all batches for
      null values before determining Arrow field nullability, fixing CTAS with
      CAST(NULL AS T) in UNION ALL (DataFusion schema vs data mismatch)
    - docker-compose.test.yml: enable POLARIS_FEATURES_DEFAULTS_DROP_WITH_PURGE_ENABLED
      so DROP VIEW / DROP TABLE can purge metadata files from RustFS
    - bootstrap-test.sh: add polaris.config.drop-with-purge.enabled to catalog
      properties; set --test-threads=1 in integration-test.sh to avoid parallel
      fixture table conflicts
    - integration-test.sh: capture SQE tracing logs via tee, show Polaris and
      RustFS logs before docker compose down
    - integration_test.rs: 37 tests covering views, INNER/LEFT/RIGHT/FULL OUTER/
      CROSS/self/three-way joins, GROUP BY, HAVING, CTEs, subqueries (WHERE,
      scalar, IN, EXISTS), UNION ALL, ORDER BY/LIMIT/OFFSET, CASE, string and
      math functions, window functions (ROW_NUMBER, RANK, running total)
    - docs/testing.md: test inventory with all SQL queries and fixture data

## [0.4.0] — 2026-03-18

### Bug Fixes

- Support CREATE TABLE (columns) and fix SHOW commands via Flight SQL

Two bugs fixed:

    1. CREATE TABLE IF NOT EXISTS ns.table (columns) was classified as
       Utility and rejected. Now classified as CreateTable with a dedicated
       handler that converts SQL column defs → Arrow → Iceberg schema and
       creates the table in Polaris. Supports IF NOT EXISTS.

    2. SHOW CATALOGS/TABLES/SCHEMAS failed with "information_schema is not
       enabled" when executed via Flight SQL. The get_flight_info_statement
       path called get_schema() which passed raw SQL to DataFusion's planner.
       Non-query statements now return an empty schema from get_schema() and
       execute via the normal classified routing in do_get_statement.
- Fix docker-compose and bootstrap for Polaris in-memory + RustFS

- Polaris: POLARIS_BOOTSTRAP_CREDENTIALS sets exact client_id/secret
      (root/s3cr3t), no need to parse from logs
    - Polaris: healthcheck uses token endpoint (not /q/health which 404s)
    - RustFS: use RUSTFS_ACCESS_KEY/SECRET_KEY env vars, add volume mount
    - RustFS: remove broken /minio/health/live healthcheck
    - Bootstrap: use HTTP status code for Polaris readiness check
    - Bootstrap: use aws CLI for bucket creation when available
    - Add tests/.test-env to .gitignore

### Documentation

- Update openspec tasks.md checkboxes to reflect actual implementation status

Audit all crates against openspec tasks and mark completed items.
    ~80% of tasks were already implemented but not checked off.

    Remaining open items: auth/catalog/write integration tests, DELETE/MERGE
    operations, Iceberg predicate pushdown, proto codec, worker heartbeat +
    credential refresh, Trino pagination + header handling, worker metrics,
    OTel trace propagation, Keycloak client registration, e2e tests.
- Add lightweight test stack design spec

2-container test stack (Polaris in-memory + RustFS) replacing the
    5+ container quickstart. Adds client_credentials auth grant and
    bootstrap script for warehouse/namespace creation.
- Add lightweight test stack implementation plan

### Features

- Add bearer token auth to Trino HTTP endpoint

Support dual authentication on the Trino /v1/statement endpoint:
    - Bearer token: backend passes user's Keycloak JWT + X-Trino-User header,
      no Keycloak round-trip needed
    - Basic auth: CLI/curl/dashboards authenticate via Keycloak ROPC (unchanged)

    Bearer takes priority when both are present. This enables the data-platform
    backend to forward pre-authenticated requests without re-authenticating.

    Update openspec trino-compat spec with dual auth scenarios and tasks.
- **sqe-core:** Add token_endpoint field to AuthConfig for OAuth2 client_credentials

Add `token_endpoint` field with `#[serde(default)]` to AuthConfig to support
    generic OAuth2 client_credentials grant alongside existing Keycloak ROPC.
    Make `keycloak_url` and `realm` optional (serde default) so configs can omit
    them when using client_credentials mode. Add SQE_AUTH__TOKEN_ENDPOINT env
    override and include the field in the Debug impl.
- **sqe-auth:** Add OAuthClient for client_credentials grant

Create oauth.rs with OAuthClient struct that obtains bearer tokens via
    the OAuth2 client_credentials grant (POST with client_id, client_secret,
    scope=PRINCIPAL_ROLE:ALL). This supports generic OAuth2 endpoints like
    Polaris as an alternative to the existing Keycloak ROPC flow.
- **sqe-auth:** Support dual auth backends (Keycloak ROPC + client_credentials)

Introduce AuthBackend enum to select between Keycloak (ROPC) and generic
    OAuth2 client_credentials at startup. Selection logic: if token_endpoint
    is set and keycloak_url is empty, use client_credentials mode. In that
    mode, authenticate() ignores username/password and calls OAuthClient,
    refresh_session() re-fetches via client_credentials (no refresh_token),
    and start_refresh_task() handles both backends transparently.
- Add lightweight test stack docker-compose (Polaris in-memory + RustFS)
- Add bootstrap script for lightweight test stack
- Update test config for lightweight stack (client_credentials + RustFS)
- Update integration tests for lightweight stack
- Add integration-test.sh and build-test.sh scripts

- integration-test.sh: starts test stack, bootstraps, runs integration tests
    - build-test.sh: full pipeline (build + unit tests + integration tests)

### Miscellaneous

- Add chrono dependency to sqe-trino-compat

Required for bearer token session creation (token expiry calculation).

### Performance

- Speed up Docker build with cargo-chef and release profile tuning

Replace fragile dummy-source dependency caching with cargo-chef, which
    generates a deterministic recipe from the lockfile. Dependencies are
    pre-built in a separate stage that only invalidates when Cargo.toml or
    Cargo.lock change — no more silent failures from the old trick.

    Release profile changes:
    - codegen-units=4: more parallelism during codegen (was default 1)
    - lto="thin": ~80% of full LTO benefit, much faster link time
    - strip=true: strip debug symbols (~50% smaller binary)

    Also adds HEALTHCHECK to the Dockerfile and curl to the runtime image.
- Add mold linker and sccache to Docker build

- mold: 2-5x faster linking than default ld (via clang -fuse-ld=mold)
    - sccache: compilation cache (2GB local disk cache in Docker layer)
    - Both tools installed in the base chef stage, shared across all build stages
    - sccache stats printed after dep cook and final build for visibility

## [0.3.0] — 2026-03-17

### Features

- Add health endpoints (Trino /v1/info, Ballista /api/v1/status) and GitLab CI

Add Trino-compatible /v1/info and /v1/info/state endpoints on the Trino
    HTTP port (8080) for compatibility with Trino JDBC drivers and monitoring
    tools. Add Ballista/DataFusion-style /api/v1/status JSON endpoint on the
    health port (9091) reporting node role, version, uptime, DataFusion
    version, and worker cluster state.

    Add .gitlab-ci.yml using the shared ci-cd-pipelines Kaniko template
    (same pattern as data-platform) with cargo test and container build
    stages.

    Update openspec specs, tasks, and observability docs accordingly.
- Add health endpoints (Trino /v1/info, Ballista /api/v1/status) and GitLab CI

Add Trino-compatible /v1/info and /v1/info/state endpoints on the Trino
    HTTP port (8080) for compatibility with Trino JDBC drivers and monitoring
    tools. Add Ballista/DataFusion-style /api/v1/status JSON endpoint on the
    health port (9091) reporting node role, version, uptime, DataFusion
    version, and worker cluster state.

    Add .gitlab-ci.yml using the shared ci-cd-pipelines Kaniko template
    (same pattern as data-platform) with cargo test and container build
    stages.

    Update openspec specs, tasks, and observability docs accordingly.

## [0.2.0] — 2026-03-16

### Features

- Unified docker packaging, Helm chart, env config, and mdBook docs

- Add sqe-server unified binary (coordinator/worker via --mode flag, default coordinator)
    - Add --mode CLI flag with default for small single-node environments
    - Add /healthz and /readyz health endpoints for K8s probes
    - Add SIGTERM/SIGINT graceful shutdown with serve_with_shutdown
    - Add env var overrides for all config fields (SQE_<SECTION>__<FIELD>)
    - Add VERSION constant to sqe-core
    - Enhance sqe-cli: --token, --format (table/csv/json), version display
    - Rewrite Dockerfile: single debian:bookworm-slim image with both binaries
    - Add Helm chart (deploy/helm/sqe/) with coordinator, optional workers, secrets, ServiceMonitor
    - Add raw K8s manifests (deploy/k8s/)
    - Add mode selection unit tests (10 tests)
    - Add mdBook documentation site (20 pages, 28 mermaid diagrams) covering
      story, architecture, features, quickstart, deployment, and development
    - Update build script for sqe-server + sqe-cli
- Add configurable Iceberg table format version (v2/v3)

iceberg-rust 0.8 already supports v3 metadata. Wire the format version
    through to TableCreation so new tables can be created as v3.

## [0.1.1] — 2026-03-15

### Documentation

- Add openspec for row-level writes (MERGE/DELETE/UPDATE)

Documents the dependency on upstream iceberg-rust PRs (#2185
    OverwriteAction, #2203 RowDeltaAction, #2219 DeltaWriter) and the
    implementation plan for when they land. Covers CoW strategy, SQE
    architecture changes, testing strategy, and acceptance criteria.

### Features

- Add CREATE SCHEMA, CREATE OR REPLACE TABLE, update dbt status

- Add CREATE SCHEMA / DROP SCHEMA support via Polaris namespace API
    - Add CREATE OR REPLACE TABLE AS SELECT (drop-if-exists + CTAS)
    - Update dbt-sqe.md status table to reflect current implementation
    - Update features.md with new DDL capabilities

## [0.1.0] — 2026-03-15

### Bug Fixes

- Redact secrets in Debug, propagate errors, avoid double execution, wire refresh task
- Add partition guard and preserve schema metadata in IcebergScanExec
- Code review fixes — cache Trino results, stable metric labels, timezone types

### Documentation

- Add SQE core engine implementation plan (Chunk 1)

Detailed plan for workspace setup, sqe-core, sqe-auth, sqe-catalog,
    sqe-sql, sqe-policy, sqe-coordinator, and integration tests.
- Add Chunk 2 plan - write path (CTAS, INSERT, DELETE, DROP, RENAME)
- Add emoji indicators to SQL feature comparison

Replace Yes/No/Planned text with ✅/❌/🔜/⚠️ emoji for quick
    visual scanning. Also expanded Array & Map functions section and
    added legend.

### Features

- Initialize workspace and sqe-core crate with config, error, session types
- Sqe-auth crate with Keycloak OIDC client, token cache, and authenticator
- Sqe-policy with PolicyEnforcer trait and PassthroughEnforcer stub
- Sqe-sql with SQL statement parsing and classification

Implements a SQL statement classifier using sqlparser-rs 0.53. Parses
    raw SQL strings and classifies them into StatementKind variants (Query,
    Ctas, Insert, Merge, Delete, Drop, Rename, CreateView, DropView,
    ShowCatalogs, ShowSchemas, ShowTables, Policy, Utility) for routing
    by the coordinator. Includes 23 unit tests covering all classification
    paths.
- Sqe-catalog with per-session Iceberg REST catalog, S3 credential vending, DataFusion providers

Implements the catalog layer bridging Keycloak-authenticated sessions to
    Iceberg tables in Polaris:

    - SessionCatalog: per-session REST catalog wrapper with bearer token auth
    - SessionCatalogBridge: implements iceberg Catalog trait for interop
    - CredentialCache: moka-based cache for vended S3 credentials with TTL
    - SqeCatalogProvider: DataFusion CatalogProvider backed by Iceberg namespaces
    - SqeSchemaProvider: DataFusion SchemaProvider with lazy table loading
    - SqeTableProvider: DataFusion TableProvider using iceberg schema conversion

    Upgrades iceberg crates from 0.4 to 0.5 to resolve arrow 53 vs 55 /
    chrono 0.4.41+ compatibility issue. Drops iceberg-datafusion dependency
    since v0.5 targets datafusion 47 (incompatible with our datafusion 49);
    implements own TableProvider using iceberg's arrow schema conversion.
- Sqe-coordinator with Flight SQL server, session management, and query pipeline

Implements the coordinator binary that wires together all SQE crates:
    - SessionManager: Keycloak ROPC auth with DashMap-based session storage
    - QueryHandler: SQL classification, DataFusion context with per-session
      Polaris catalog, policy enforcement pipeline, SHOW command handlers
    - SqeFlightSqlService: Full Arrow Flight SQL protocol implementation
      with Basic auth handshake, query execution via do_get/get_flight_info,
      and catalog metadata endpoints
    - main.rs: Config loading, component initialization, tonic gRPC server
- Iceberg Parquet writer infrastructure for data file creation

Add `parquet` as workspace dependency and create `writer.rs` in
    sqe-coordinator that converts RecordBatches into Iceberg DataFiles
    using iceberg-rust's writer API (ParquetWriterBuilder,
    DataFileWriterBuilder, location/filename generators).

    Also includes catalog DDL operations (DROP TABLE, RENAME) from
    the catalog_ops module and wires them into QueryHandler.
- CTAS and INSERT INTO SELECT write handlers with Iceberg transaction commits
- DELETE FROM and MERGE INTO stubs with descriptive messages (pending Iceberg overwrite support)
- IcebergScanExec replaces EmptyExec for real Iceberg data reads
- ScanTask protocol and fragment splitting for distributed execution
- Worker registry with health-check-based liveness tracking

Add WorkerRegistry to sqe-coordinator that tracks worker health via periodic
    Flight SQL do_action health checks, marking workers unhealthy after 3 consecutive
    failures and recovering on success. Wire into coordinator main with background
    health check task. Add worker_urls to CoordinatorConfig and flight_port to
    WorkerConfig. Add sqe-planner dependency to sqe-coordinator.
- Sqe-worker binary with Parquet scan execution over Arrow Flight

Implements the sqe-worker crate: a stateless Arrow Flight server that
    receives ScanTask tickets from the coordinator, reads Parquet files from
    S3 via object_store 0.12, and streams Arrow RecordBatches back. Also
    upgrades workspace object_store dependency from 0.11 to 0.12 to match
    parquet 55's requirement.
- DistributedScanExec and coordinator distributed execution path

Add DistributedScanExec ExecutionPlan that dispatches ScanTasks to workers
    via Arrow Flight do_get, update QueryHandler to accept an optional
    WorkerRegistry (3-arg constructor) with a should_distribute() helper, and
    wire the registry into main.rs so the coordinator passes it when worker_urls
    are configured.
- Prometheus metrics registry, /metrics endpoint, and JSON audit logger

Implements Tasks 20 and 21: MetricsRegistry with counters/histograms/gauges
    for query tracking, axum HTTP server serving /metrics in Prometheus text
    format, and AuditLogger writing JSONL audit entries to disk (noop when path
    is empty). All 8 unit tests pass.
- Virtual information_schema (tables, columns, schemata) for dbt compatibility
- Trino v1/statement HTTP server with Arrow-to-JSON type mapping

Implement sqe-trino-compat crate with Arrow→Trino type mapping (types.rs),
    Trino JSON response protocol structs (protocol.rs), and axum 0.8
    v1/statement POST/GET/DELETE HTTP server with Basic Auth passthrough (server.rs).
    All 12 unit tests pass.
- Integrate metrics, audit logging, and Trino HTTP server into coordinator

Wire MetricsRegistry and AuditLogger into QueryHandler so every query
    records timing, row counts, and an audit trail. Start the Prometheus
    metrics server and Trino-compat HTTP server from main.rs via thin
    adapter types that implement TrinoAuthenticator and TrinoQueryExecutor.
- Add Dockerfile, build scripts, and sqe-cli with Flight SQL + HTTP backends

- Alpine multi-stage Dockerfile (coordinator, worker, cli targets)
    - Build scripts: build.sh, docker-build.sh, test.sh
    - sqe-cli crate with dual-protocol support:
      - --protocol flight (default): Arrow Flight SQL over gRPC/HTTP2
      - --protocol http: Trino-compat REST, works through any HTTP proxy
    - Interactive REPL with readline history, multi-line SQL
    - One-shot mode via -e flag
- Add OpenTelemetry support for traces, metrics, and logs via OTLP

- New sqe-metrics::otel module with init_telemetry() for full OTel stack
    - Exports traces, metrics, and logs via OTLP/gRPC when otlp_endpoint is set
    - Falls back to structured JSON logs only when endpoint is empty
    - OtelGuard RAII type for graceful provider shutdown
    - Both coordinator and worker binaries use init_telemetry()
    - Added #[tracing::instrument] spans on key query path:
      - QueryHandler::execute, execute_query, create_session_context
      - FlightSQL handshake, get_flight_info, do_get
      - Trino submit_query, get_results
      - Worker execute_scan
    - Anti-loop filter on OTel log bridge (hyper, tonic, h2, reqwest)
- Implement views, fix critical bugs, harden CLI and Docker

- Implement CREATE VIEW and DROP VIEW via Polaris REST API (bypasses
      iceberg-rust which lacks view support), with Arrow-to-Iceberg schema
      inference through DataFusion planning
    - Fix session token refresh: SessionManager now consults TokenCache for
      background-refreshed tokens and evicts expired sessions
    - Fix namespace flattening: use join(".") instead of flat_map for
      multi-level namespaces in catalog_provider and info_schema
    - Fix RenameColumn misroute: ALTER TABLE RENAME COLUMN now routes to
      Utility instead of Rename handler
    - Harden CLI: TLS support for Flight SQL, SSRF prevention in HTTP
      client, bounds checking in display, --insecure flag
    - Harden Docker: Alpine multi-stage build, dependency caching,
      non-root user, no baked config
    - Harden OTel: anti-telemetry-loop filter, try_init, correct shutdown
      order
    - Remove SQL text from debug logs to prevent data leakage
- Upgrade deps to latest compatible versions, add README and SQL feature comparison

Dependency upgrades:
    - DataFusion 49 → 51, Arrow 55 → 57, iceberg-rust 0.5 → 0.8
    - sqlparser 0.53 → 0.59, tonic 0.12 → 0.14, prost 0.13 → 0.14

    API migrations for breaking changes:
    - iceberg 0.8: CatalogBuilder::load() pattern, fast_append() no-arg,
      register_table() trait method, RollingFileWriterBuilder
    - sqlparser 0.59: ObjectNamePart, RenameTableNameKind, Set enum,
      Insert.table field rename
    - tonic 0.14: Endpoint::new().connect() replaces ::connect(),
      tls-native-roots feature replaces tls
    - DataFusion 51: DFSchema.as_arrow() replaces Into<Schema>

    Add docs/features.md with detailed SQL feature comparison (SQE vs
    Trino vs Spark) covering window functions, aggregates, joins, CTEs,
    DDL, and Iceberg-specific features.

    Replace default GitLab README with project documentation.

### Testing

- Integration tests for Flight SQL auth and Iceberg query via Polaris

Add integration test infrastructure for the SQE coordinator:
    - Create tests/sqe-test.toml with quickstart stack configuration
    - Add integration tests for Keycloak authentication, multi-user sessions,
      token fingerprinting, SELECT 1 query pipeline, and SQL classification
    - Extract sqe-coordinator lib.rs to expose QueryHandler and SessionManager
      for integration test access
    - Add .gitignore for workspace root

    Integration tests are #[ignore] and require the quickstart stack running.
    Run with: cargo test -p sqe-coordinator --test integration_test -- --ignored
- Write path integration tests for CTAS, INSERT, DROP TABLE

Add 5 new integration tests covering the write path:
    - test_ctas_roundtrip: CTAS + SELECT verification + cleanup
    - test_insert_into: CTAS + INSERT + row count verification
    - test_drop_table: CTAS + DROP + verify SELECT fails
    - test_drop_table_if_exists_no_error: IF EXISTS on missing table
    - test_delete_returns_not_implemented: verify descriptive error message

    Stack-dependent tests are marked #[ignore]; the DELETE error test runs locally.
- Distributed execution integration tests and config updates

Add worker/coordinator config sections to sqe-test.toml and append four
    Chunk 3 integration tests to sqe-coordinator: worker registry empty-state,
    local fallback without workers, ScanTask serialization roundtrip, and a
    full distributed SELECT (ignored, requires running stack). Add sqe-planner
    to dev-dependencies so tests can reference ScanTask directly.
- Chunk 4 integration tests for information_schema, metrics, and Trino compat

---
*Generated by [git-cliff](https://git-cliff.org)*
