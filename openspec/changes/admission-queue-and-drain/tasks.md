## 1. Groundwork

- [ ] 1.1 Verify `QueryTracker::active_count()` and `sqe_active_queries` drop a streamed statement that ends by client drop; fix if not (D5)
- [x] 1.2 `RefusalReason` enum + `SqeError::refused(reason, msg)` + accessor in `sqe-core/src/error.rs`; unit test wire names
- [x] 1.3 Map the reason in Flight SQL (status metadata), Trino HTTP (`errorName`), HTTP 429 JSON; one test per protocol

## 2. Typed refusals (no queue yet)

- [x] 2.1 `admission.rs`: per-user limit -> `user_limit`, global -> `queue_full`, per-user memory -> `memory_budget`, pressure -> `memory_pressure`
- [x] 2.2 `rate_limiter.rs` and the sorted-write semaphore: `rate_limited`
- [x] 2.3 Integration test: acceptance 3 (memory budget) over Flight SQL

## 3. Queue

- [x] 3.1 Config `query.queue_max`, `query.queue_timeout_secs` (defaults keep today's behaviour) + `sqe.toml.example`
- [x] 3.2 Fair queue with per-user lanes and the permit-handoff guard (D2); unit tests: FIFO in lane, round robin, cancelled waiter, every drop path feeds the queue
- [x] 3.3 Wire into the shared admission path for `execute` and `execute_stream`
- [x] 3.4 Metrics `sqe_admission_queue_length`, `sqe_admission_queue_wait_seconds`
- [x] 3.5 Integration tests: acceptance 1 and 2

## 4. Drain

- [ ] 4.1 `draining` flag in the handler; admission refuses and queued waiters are woken with `draining`
- [ ] 4.2 `shutdown_with_drain` waits on `active_count()`; keep `before_close` after it; fix the stale comment in `main.rs`
- [ ] 4.3 Worker drain hook: refuse new fragments with `Unavailable`, wait for in-flight ones
- [ ] 4.4 Process-level test: acceptance 4 (SIGTERM during a 20 s query)

## 5. Idle gauge

- [ ] 5.1 `last_finished` timestamp + `sqe_idle_seconds` in the sampler
- [ ] 5.2 Test: acceptance 5

## 6. Docs

- [ ] 6.1 Operations guide: queueing, reasons table, drain semantics; changelog notes the `ExecutionFailed` -> `ResourceExhausted` change for clients
