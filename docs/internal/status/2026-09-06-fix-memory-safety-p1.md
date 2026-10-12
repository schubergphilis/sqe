**STATUS 2026-09-06: memory safety and large-data execution batch (#655) implemented on `fix/memory-safety-p1`; 54 of 109 combined findings closed.**

Branch `fix/memory-safety-p1` (work package #655) closes 300, 301, 307,
308, 403, 408, 504, 509, 512, 701, 806 and 811: every Polaris HTTP client
carries connect and request timeouts, the per-token circuit breakers
evict on idle and export one low-cardinality gauge per catalog, the HEAD
authorization on a fresh cache hit is cached for a short recheck window,
schema providers and the Puffin NDV cache are shared across lookups,
manifests are decoded once per snapshot through a `SnapshotStatsCache`,
`table_names()` serves an async-refreshed snapshot and never blocks a
runtime worker, Flight SQL tickets are UUIDs so `CancelQuery` reaches the
tracker, cancel wakes parked streams, `query.timeout_secs` is an absolute
deadline for streams, in-flight query records cannot be evicted by TTL,
the scheduler tracks bytes-in-flight and releases reservations when a scan
stream is dropped, heartbeats carry a protocol version, MoR DELETE /
UPDATE / MERGE buffer through the memory pool with per-file position
deletes and identifier-only key batches, DML evaluators use UUID scratch
tables with RAII cleanup and CoW UPDATE plans once per batch, Puffin NDV
statistics carry forward across rewrites and row deltas with a retried
statistics commit, the Trino path streams results through a per-query
cursor charged to the memory pool, and no `std::sync::Mutex` is held in an
async Trino handler (`await_holding_lock` denied workspace-wide).
Partial: 806 leaves 10 `block_on_compat` sites (file TVFs, Hive, sync
DDL); 408 is direction 1 (scan offload owned, no stage planner).
Pre-existing red on main: `flight_sql_smoke::handshake_then_statement_returns_rows_over_grpc`
fails because a dead catalog URL no longer degrades to empty namespaces.
**NEXT:** work package #656 SQL planning and protocol (107, 311, 312, 404,
406, 605, 700, 703, 704, 708, 710), then #658 distributed worker
reliability and test infrastructure.
