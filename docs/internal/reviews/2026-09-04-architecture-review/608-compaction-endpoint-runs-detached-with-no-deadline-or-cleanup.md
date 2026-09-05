# 608: compact_file_group runs as a detached task with no worker-side deadline, cancellation, or partial-upload cleanup

- Scope: sqe-worker (flight_service.rs, compaction.rs), sqe-coordinator (compaction_dispatch.rs)
- Severity: medium
- Category: reliability
- Effort: S (hours) to M (days)
- Status: open

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
