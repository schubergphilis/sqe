**STATUS 2026-11-16: shutdown drain and the idle gauge (`feat/admission-drain-902`, part 2 of #902).**

OpenSpec change `admission-queue-and-drain`, groups 1.1, 4, 5 and the drain part of 6. Stacked on `feat/admission-queue-902`.

- `AdmissionQueue::close()`: marks the queue closed and drops every waiter's sender, so queued statements are refused `draining` at once; later arrivals are refused too.
- `QueryHandler::start_drain()`: a `draining` flag that `admit` checks first (covers `max_concurrent_queries = 0`, where there is no queue), then closes the queue.
- `sqe-server` `shutdown_with_drain(ready, secs, stop_admitting, in_flight, before_close)`: flips readiness, stops admitting, polls `in_flight` every 250 ms until 0 or the bound, then `before_close`. The coordinator passes `QueryTracker::active_count`, the worker `WorkerStatus::inflight_scans`. The stale "not a true in-flight-query drain" comment is gone.
- Worker: `WorkerStatus::start_drain()`; `do_get` answers `Unavailable` after the secret check while draining. A bare `Unavailable` classifies as a transport failure, which the coordinator retries elsewhere (`distributed_scan::classify_status`).
- `QueryTracker` keeps `last_finished_ms` (set where a record leaves `active`) and `idle_seconds(now)`. `spawn_activity_reporter` sets `sqe_active_queries` and the new `sqe_idle_seconds` every second.
- Task 1.1 finding: `active_count()` does drop a stream that ends by client drop (assertion added to `tracked_stream_marks_cancelled_on_early_drop`), but the `sqe_active_queries` gauge was only set when a streamed statement ended, never on start, so it read 0 while statements ran. The reporter fixes it.

Tests: `close_refuses_queued_and_later_statements_with_draining` (queue), `after_start_drain_queued_and_new_statements_are_refused_draining` (handler), `the_drain_waits_for_in_flight_work_and_stops_at_the_bound` (acceptance 4, paused clock: a 20 s statement under a 60 s bound ends the drain at 20 s), `a_draining_worker_refuses_new_fragments_unavailable`, `idle_seconds_is_zero_while_running_and_counts_from_the_last_finish` (acceptance 5).

The `shutdown_drain_secs` doc comment and the Helm comments (`values.yaml`, both deployment templates) now say "until nothing runs or the bound", not "sleeps". The `sqe-coordinator` line ceiling in `.config/budgets.json` goes from 68200 to 68400: the drain flag, the queue `close()`, the tracker idle clock and their tests add about 50 lines.

Left open: task 4.4 as a real process test (SIGTERM to a running `sqe-server` during a 20 s query) is covered at the function level, not by a spawned process. A Trino HTTP client still paging a finished result when the drain ends loses the remaining pages.

**NEXT:** #904 snapshot id trailer on ingest (`feat/ingest-snapshot-id-904`).
