### Added

- Admission queue: statements wait for a slot instead of being refused (#902)

    `[query] queue_max` (default 0, today's behaviour) lets a statement that
    finds all `max_concurrent_queries` slots taken wait up to
    `queue_timeout_secs` (default 30). Waiters are served round robin per
    user, FIFO within a user. A user at `max_concurrent_per_user` is still
    refused at once. New metrics `sqe_admission_queue_length` and
    `sqe_admission_queue_wait_seconds{outcome}`.

### Changed

- Busy refusals are `RESOURCE_EXHAUSTED` with a reason (#902)

    A statement refused before it ran (concurrency, queue, per-user memory,
    memory pressure, rate limit) now reports `ResourceExhausted` instead of
    `ExecutionFailed`, so clients that retried on `INTERNAL` or matched the
    message must match the code. The reason is `queue_full`,
    `queue_timeout`, `user_limit`, `memory_budget`, `memory_pressure` or
    `rate_limited`: Flight SQL metadata `x-sqe-refusal-reason`, Trino
    `errorName` `SQE_<REASON>` with errorType `INSUFFICIENT_RESOURCES`, MCP
    `structuredContent.error`.
