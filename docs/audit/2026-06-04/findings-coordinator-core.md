# Findings — Coordinator core: scheduler / session / write / distributed (`sqe-coordinator`)

**Scope:** Reliability + performance + cost of the scheduler/session/write/distributed paths:
`scheduler.rs`, `worker_registry.rs`, `distributed_scan.rs`, `query_tracker.rs`, `session_manager.rs`,
`query_cache.rs`, `credential_refresh.rs`, `writer.rs`, `write_handler.rs` (CTAS/INSERT/MERGE/DELETE commit
paths), `query_handler.rs`, `streaming.rs`, `memory.rs`, `channel_pool.rs`. The write commit path
(mark_committed ordering, bounded `commit_with_retry`, RAII `WriteCleanupGuard`) is well-engineered and
produced no reliability finding. The scheduler `expect`s, the `active` query map, and the zero-row
`writer.close()` were investigated and confirmed safe.

---

### COORD-01 — high — Result cache serves stale data after writes (invalidation key never matches index key)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/query_handler.rs:1229`, `:1256`, `:1267`; `crates/sqe-coordinator/src/query_cache.rs:116-121`, `:130-131`; `crates/sqe-lineage/src/extract/mod.rs:26`
- **Evidence:**
  ```rust
  // store-side: table_index keys come from the LOGICAL PLAN (qualified)
  // query_handler.rs:1229-1234 -> extract_table_names -> "datafusion.public.sales"
  Some(sqe_lineage::PlanOrHint::Plan(p)) =>
      sqe_lineage::extract::extract_table_names(p.as_ref()),
  // query_cache.rs:116-121: self.table_index.entry(table.clone())...insert(key)

  // invalidate-side: key is the RAW SQL identifier, e.g. "sales" or "ns.sales"
  // query_handler.rs:1267
  let table = ins.table.to_string();
  cache.invalidate(&table);
  // query_cache.rs:130-131: only removes on exact string match
  if let Some((_, keys)) = self.table_index.remove(table_name) { ... }
  ```
- **Impact:** After an INSERT/UPDATE/DELETE/MERGE/CTAS that succeeds (buffered path), `cache.invalidate("sales")`
  looks up the secondary index by the bare SQL name while the index was populated under the fully-qualified
  DataFusion reference (`datafusion.public.sales`). Strings do not match, the removal is a no-op, and the prior
  SELECT result stays in the moka cache until its TTL expires. Subsequent identical SELECTs by the same user
  return pre-write data. Silent data staleness across a correctness boundary, affecting every multi-statement
  read-after-write workload (dbt, BI tools) when the result cache is enabled.
- **Fix:** Normalize both sides to one canonical table key before indexing/invalidating (apply the
  `datafusion.public.` registration prefix in the invalidate path, or strip the catalog/schema prefix from the
  lineage-derived names). Add a test that stores a SELECT, writes the same table, and asserts the next SELECT
  misses the cache.
- **Effort:** small

---

### COORD-02 — high — CredentialRefreshTracker leaks an entry per successfully-completed distributed fragment (unbounded growth)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/distributed_scan.rs:328-336` (register), `:424` (success return), `:477-479` (only unregister); `crates/sqe-coordinator/src/query_handler.rs:2265-2320` (completion callback); `crates/sqe-coordinator/src/credential_refresh.rs:134`, `:156-174`
- **Evidence:**
  ```rust
  // distributed_scan.rs:328 — every dispatched fragment is registered
  if let Some(ref tracker) = credential_tracker {
      tracker.register(task.fragment_id.clone(), initial_worker_url.clone(), credential_expiry).await;
  }
  // ... success path returns the stream at :424 with NO unregister
  return Ok(wrapped);
  // the ONLY unregister is on the all-attempts-failed path (:477):
  if let Some(ref tracker) = credential_tracker { tracker.unregister(&task.fragment_id).await; }
  ```
- **Impact:** When credential vending/refresh is enabled (`credential_tracker` is `Some`), every distributed
  scan fragment that completes successfully is registered but never unregistered. The
  `fragments: HashMap<String, ActiveFragment>` grows by one entry per fragment for the life of the process. On a
  busy coordinator this is a slow memory leak toward OOM (a coordinator-wide SPOF). It also degrades
  performance: the background refresh loop calls `fragments_needing_refresh()` every interval, iterating the
  ever-growing map under a read lock, and may re-issue `push_credentials_to_worker` for finished fragments.
- **Fix:** Unregister on stream completion, not only on failure. Either wrap the returned success stream in a
  teardown that calls `tracker.unregister(fragment_id)` on Drop/EOF (mirror `TerminateOnErrorStream`), or have
  the existing `fragment_callback` (which already fires exactly once on success/error) call
  `credential_tracker.unregister(task_id)`.
- **Effort:** small

---

### COORD-03 — medium — query_cache `table_index` grows unbounded for read-mostly tables and cleanup is O(n^2)

- **Dimension:** performance
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/query_cache.rs:26`, `:113-121`, `:140-143`
- **Evidence:**
  ```rust
  // query_cache.rs:26 — secondary index, NOT bounded and NOT tied to cache eviction
  table_index: DashMap<String, HashSet<String>>,
  // :140-143 — invalidation cleanup is a nested full scan of the index
  for key in &keys {
      for mut entry in self.table_index.iter_mut() {
          entry.value_mut().remove(key);
      }
  }
  ```
- **Impact:** The moka `cache` is bounded (bytes + TTL) and evicts silently, but `table_index` has no eviction
  listener. For a table queried with many distinct SQL strings but rarely written, the inner `HashSet<String>`
  accumulates cache keys for entries long gone from `cache`, with no upper bound, until the table is finally
  written or `invalidate_all()` runs. Separately, the cleanup at :140-143 is O(keys x table_index_size): each
  write that invalidates K cache keys scans the whole index K times.
- **Fix:** Register a moka `eviction_listener` on `cache` that prunes the evicted key from `table_index`. For
  the cleanup, build a single `HashSet` of removed keys and do one pass over the index.
- **Effort:** small

---

### COORD-04 — low — SessionManager has no hard cap on the sessions map (TTL-only, UUID-per-auth)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/session_manager.rs:39`, `:119-134`; `crates/sqe-core/src/session.rs:77`
- **Evidence:**
  ```rust
  // session_manager.rs:39 — no max-count bound
  sessions: DashMap<String, Arc<Session>>,
  // session.rs:77 — a fresh UUID per call, so re-auth never reuses a slot
  id: Uuid::new_v4().to_string(),
  ```
- **Impact:** Each successful authentication allocates a brand-new session keyed by a random UUID; sessions are
  only reclaimed by the idle/absolute-expiry sweeper. No upper bound on map size. A client that re-authenticates
  per request, or a burst of valid logins, grows `sessions` + `last_activity` until the next sweep. The pre-auth
  `AuthRateLimiter` bounds the burst rate (keeping this low rather than medium), but is config-gated and provides
  no defense-in-depth cap on the map itself.
- **Fix:** Add a configurable `max_sessions` cap; on insert past the cap, evict the least-recently-active entry.
  Independently, deduplicate sessions per token fingerprint so repeated logins with the same bearer reuse a slot.
- **Effort:** medium

---

### COORD-05 — low — Worker health check runs workers sequentially with only the 30s pool request-timeout as a backstop

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/worker_registry.rs:318-352`; `crates/sqe-coordinator/src/channel_pool.rs:29`, `:77`; contrast `distributed_scan.rs:601-604`
- **Evidence:**
  ```rust
  // worker_registry.rs:324 — sequential loop over all workers
  for url in urls {
      let result = self.health_check_worker(&url).await; ...
  // :350 — do_action has no explicit per-call timeout wrapper
  let _response = client.do_action(tonic::Request::new(action)).await?;
  ```
- **Impact:** A single kernel-paused worker that accepts the TCP connection but stalls the `do_action` reply
  blocks the health-check loop for up to the 30s pool request-timeout before moving to the next worker. With N
  stalled workers the aggregate detection latency serializes, so the registry is slow to mark a degraded pool
  unhealthy, delaying failover. Note also the comment at distributed_scan.rs:601-603 claims pooled channels
  "don't carry an Endpoint-level timeout," contradicting channel_pool.rs:77 where the pool sets `.timeout(...)`;
  one is stale.
- **Fix:** Wrap `health_check_worker` in a short `tokio::time::timeout` (2-3s, independent of the 30s data-RPC
  budget) and run per-worker checks concurrently (`FuturesUnordered`/`join_all`). Reconcile the stale comment.
- **Effort:** small

---

### COORD-06 — info — WriteCleanupGuard orphan cleanup is best-effort fire-and-forget; failures leak S3 objects

- **Dimension:** cost
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/writer.rs:128-147`
- **Evidence:**
  ```rust
  // writer.rs:136-137 — delete failure only warns; orphan remains
  if let Err(e) = file_io.delete(&p).await {
      warn!(op, path = %p, error = %e, "orphan cleanup: delete failed");
  }
  // :141-147 — no tokio runtime at drop -> files left on S3
  ```
- **Impact:** On a write cancelled/failed before the Iceberg commit, orphan parquet files are deleted on a
  best-effort spawned task. If any individual S3 delete fails, or the guard drops during runtime shutdown, the
  files remain in the bucket uncommitted and unreferenced. Over time these accumulate as paid-for, never-queried
  S3 storage. No data corruption (the tracker only records files this write created).
- **Fix:** Record persistent orphan paths to a durable list (or rely on a periodic `remove_orphan_files`
  maintenance procedure) so cleanup failures are eventually reconciled, and emit a metric
  (e.g. `sqe_write_orphan_files_total`) so operators can alert on accumulation.
- **Effort:** medium
