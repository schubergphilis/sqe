# 608: compact_file_group runs as a detached task with no worker-side deadline, cancellation, or partial-upload cleanup

- Scope: sqe-worker (flight_service.rs, compaction.rs), sqe-coordinator (compaction_dispatch.rs)
- Severity: medium
- Category: reliability
- Effort: S (hours) to M (days)
- Status: fixed (branch `fix/data-consistency-p1`, 2026-09-05)
- Issue: #615

## Problem

The worker starts the rewrite on its own task and returns a stream that joins
it:

- `crates/sqe-worker/src/flight_service.rs:1194-1199`:
  `tokio::spawn(async move { compact_file_group(&session_ctx, &request_for_task, Some(progress_tx)).await })`.
- The response is `progress_stream.chain(done_stream)` (`:1241`). When the
  coordinator drops the RPC (its `group_heartbeat_timeout` fired, the job was
  cancelled, or the coordinator restarted), tonic drops the response stream.
  Nothing aborts `rewrite_task`; it keeps reading, sorting and uploading to S3
  until it finishes or fails.
- There is no worker-side ceiling. `scan_timeout` applies only to `do_get`
  (`flight_service.rs:1025-1042`); the compaction path has none, and
  `compaction.rs` has no `tokio::time::timeout` (grep confirmed).
- Uploaded file paths are collected in an `upload_tracker` and returned only in
  the success response (`crates/sqe-worker/src/compaction.rs`,
  `compact_pinned_table`, `uploaded_paths` built from `tracker`). On any `Err`
  from `rewrite_group` the tracker is dropped and the coordinator never learns
  which objects were written. The coordinator explicitly relies on a later
  orphan sweep: `crates/sqe-coordinator/src/compaction_dispatch.rs:365-369`
  ("whatever it already wrote to object storage becomes an orphan reclaimed by
  the age-thresholded orphan sweep, not cleaned up here").
- The progress channel is unbounded
  (`tokio::sync::mpsc::unbounded_channel::<u64>()`, `flight_service.rs:1194`).
  If the client stops reading, progress frames accumulate for the life of the
  rewrite.

## Why it matters

- A coordinator that gives up on a slow group retries it elsewhere while the
  first worker is still burning CPU, memory pool and S3 bandwidth on the same
  group. Under a stall storm, workers accumulate zombie rewrites that compete
  with live scans for the shared `SessionContext` pool.
- Every failed or abandoned group leaves full-size data files in the table
  location until the orphan sweep runs. For a 48 TB bank benchmark table that
  is a real storage cost and a real chance of the sweep deleting a file a
  concurrent writer just committed if the age threshold is tuned low.

## Proposed change

1. Tie the rewrite to the RPC: keep the `JoinHandle`, and make the returned
   stream own an `AbortOnDrop` guard so dropping the response aborts the task.
   Use `tokio_util::sync::CancellationToken` passed into `rewrite_group` so
   the writer can stop between batches and flush nothing further.
2. Add `worker.compaction_timeout_secs` (default derived from the coordinator's
   `group_heartbeat_timeout` times retries) and wrap the task in it.
3. On error or cancellation, best-effort delete the paths recorded in the upload
   tracker before returning, and include the list in the error `Status`
   details so the coordinator can retry deletion. Keep the orphan sweep as the
   backstop, not the primary path.
4. Bound the progress channel (`mpsc::channel(16)`) and drop progress frames
   when full; progress is advisory.

## Acceptance criteria

- Test with a stub `rewrite_group` that sleeps: dropping the `do_action`
  response stream stops the task within 100 ms (observe via a shared atomic).
- Test: a rewrite that fails after two uploads leaves zero objects in the test
  bucket prefix.
- `make test-integration FILTER=rewrite_data_files_distributed_parity` still
  passes.

## Related

- 500-series review (coordinator compaction dispatch and maintenance
  scheduler) owns the coordinator half.
- Memory: "Compaction roadmap" and "Puffin sidecar stale under compaction".

## Resolution (2026-09-05)

Fixed on `fix/data-consistency-p1`. All four proposed changes landed; the
worker half of the finding is closed, the coordinator half (retrying the
deletion of the leaked paths the worker now reports) stays with the 500-series
review.

### 1. The rewrite is tied to the RPC

`do_action("compact_file_group")` still spawns the rewrite, but the returned
`progress.chain(done)` stream now owns a `CompactionRpcGuard`
(`crates/sqe-worker/src/flight_service.rs`). The guard holds the `JoinHandle`,
and its `Drop` -- which fires exactly when the response stream is dropped --
cancels a `tokio_util::sync::CancellationToken`, aborts the task, and then
spawns a best-effort deletion of everything the rewrite had already uploaded.
The cleanup task first awaits the aborted handle, so no path can be generated
behind the tracker drain.

The token is threaded as far as `compact_pinned_table`, which runs
`rewrite_group` inside a `biased` `tokio::select!` against
`cancel.cancelled()`; it also short-circuits before, and races, the
`StaticTable` metadata load. `sqe-compaction::rewrite::rewrite_group` itself
was left alone deliberately: it is shared with the coordinator's local `CALL
system.rewrite_data_files` path, and dropping the future at the select stops
the writer between record batches just as promptly as an in-loop check would.
What the token buys over a bare `abort()` is that control returns to the
worker's own code, so cleanup runs instead of the task simply vanishing.

### 2. Worker-side deadline

New `worker.compaction_timeout_secs` (`crates/sqe-core/src/config.rs`,
`WorkerConfig` + `Default` + `default_compaction_timeout()`), default 3600, 0
disables. Wired through `bootstrap.rs` ->
`WorkerFlightService::with_compaction_timeout`. The `tokio::time::timeout`
lives *inside* the spawned task, not around the join: the response stream is
`progress.chain(done)`, so the `done` half is not polled until the progress
channel closes, and a timeout on the outer join would never be armed for
exactly the wedged rewrite it exists to bound. Expiry returns
`Code::DeadlineExceeded`, distinguishable from a broken group's
`Code::Internal`.

### 3. Cleanup and reporting on the failure path

The upload tracker is now allocated by the Flight handler and passed down
through the new `GroupRewriteCtx`, so the recorded paths outlive the rewrite
future. On any error, timeout or cancellation the worker best-effort deletes
them (`compaction::delete_orphan_uploads`, draining the tracker so a second
cleanup is a no-op) before returning. The failure `Status` carries a summary
in its message and the full `uploaded_paths` / `deleted_paths` /
`leaked_paths` lists as a JSON details payload, so the coordinator can retry
the deletion of whatever leaked instead of relying on the sweep. The
age-thresholded orphan sweep remains the backstop, as intended.

### 4. Bounded progress channel

`mpsc::unbounded_channel` -> `mpsc::channel(PROGRESS_CHANNEL_CAPACITY = 16)`,
and the writer callback uses `try_send`, so a coordinator that stops reading
drops advisory frames instead of growing a queue for the life of the rewrite.
No backpressure is ever applied to the rewrite itself.

### Tests

Unit tests only; a `#[cfg(test)] CompactionSeam` on `WorkerFlightService`
(same shape as `maintenance.rs`'s `with_test_race_seam`) swaps `rewrite_group`
for a stub and points orphan cleanup at an in-memory `FileIO`, so none of this
needs a live S3 endpoint.

`flight_service.rs`:

- `dropping_the_do_action_response_stops_the_rewrite` -- stub sleeps 30s;
  dropping the response stops it within 100 ms (shared `Notify` fired from a
  drop guard) and the cancellation token is observed as cancelled.
- `a_failed_rewrite_deletes_exactly_the_objects_it_uploaded` -- stub fails
  after two uploads; exactly those two objects are deleted, an unrelated
  object survives, and the details payload lists both.
- `a_rewrite_past_the_compaction_deadline_fails_and_cleans_up` -- returns
  `DeadlineExceeded` and removes the partial upload.
- `compaction_timeout_maps_zero_to_disabled` -- the seconds-to-`Duration`
  mapping (the deadline test uses a test-only sub-second deadline so it does
  not burn a wall-clock second).
- `progress_frames_are_dropped_not_queued_when_the_reader_stalls` -- 1000
  attempted sends against a stalled reader accept at most 16.
- `a_successful_rewrite_keeps_its_output_and_returns_a_done_frame` -- the
  other half of the contract: cleanup must not touch a successful rewrite's
  output.

`compaction.rs`: `delete_orphan_uploads_removes_recorded_paths_and_drains_the_tracker`,
`delete_orphan_uploads_is_a_noop_when_nothing_was_uploaded`,
`cleanup_summary_truncates_inline_paths_but_details_carry_them_all`,
`run_group_rewrite_returns_immediately_when_already_cancelled`.

### Not done

The `make test-integration FILTER=rewrite_data_files_distributed_parity`
acceptance item was NOT run: it needs the docker rig (Polaris + RustFS), which
was not available in this session. It should be run before merge.
