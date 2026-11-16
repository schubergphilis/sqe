## ADDED Requirements

### Requirement: A full coordinator queues statements up to a bound
When the global concurrency limit is reached and `query.queue_max` is above 0, the system SHALL hold a new statement in a queue until a slot frees, up to `query.queue_max` waiting statements and `query.queue_timeout_secs` per statement. A waiting statement SHALL hold no concurrency permit and no memory reservation. Waiters SHALL be admitted round robin across users and in arrival order within a user.

#### Scenario: Second statement waits, third is refused
- **GIVEN** `max_concurrent_queries = 1` and `queue_max = 1`
- **WHEN** three statements arrive while the first is still running
- **THEN** the second waits and runs after the first finishes
- **AND** the third is refused with `RESOURCE_EXHAUSTED` and reason `queue_full`

#### Scenario: A waiter times out
- **GIVEN** `queue_timeout_secs = 2` and a slot that stays busy for 10 s
- **WHEN** a statement is queued
- **THEN** it is refused after about 2 s with reason `queue_timeout`

#### Scenario: One user cannot starve another
- **GIVEN** user A has 5 statements queued
- **WHEN** user B queues one statement
- **THEN** B's statement runs before A's third queued statement

#### Scenario: Queue off keeps today's behaviour
- **GIVEN** `queue_max = 0`
- **WHEN** a statement arrives at a full coordinator
- **THEN** it is refused at once with reason `queue_full`

### Requirement: Refusals carry a machine-readable reason on every protocol
Admission, memory-budget, memory-pressure, rate-limit and drain refusals SHALL use `RESOURCE_EXHAUSTED` with one of the reasons `queue_full`, `queue_timeout`, `user_limit`, `memory_budget`, `memory_pressure`, `rate_limited`, `draining`. Flight SQL SHALL carry the reason in status metadata `x-sqe-refusal-reason`, Trino HTTP in `errorName` (`SQE_<REASON>`), and plain HTTP as status 429 with a JSON `reason`. A memory error raised while a statement runs SHALL NOT carry a reason.

#### Scenario: Memory budget refusal
- **GIVEN** a user whose in-flight statements hold their whole `per_user_memory_budget`
- **WHEN** they start another statement
- **THEN** it is refused with `RESOURCE_EXHAUSTED` and reason `memory_budget`

#### Scenario: Same reason over Flight SQL and Trino HTTP
- **WHEN** the same refusal happens over Flight SQL and over the Trino protocol
- **THEN** both clients read the reason `queue_full`

### Requirement: Shutdown drains running statements
On SIGTERM the coordinator SHALL stop admitting (refusing with reason `draining`, including queued statements), wait until no statement runs or `shutdown_drain_secs` passes, then close. A worker SHALL stop accepting fragments and finish the ones it runs within the same bound.

#### Scenario: A running query completes during drain
- **GIVEN** `shutdown_drain_secs = 60` and a 20 s query running
- **WHEN** the coordinator receives SIGTERM
- **THEN** the query completes and returns its rows
- **AND** a statement sent after SIGTERM is refused with reason `draining`
- **AND** the process exits shortly after the query finishes, not after 60 s

### Requirement: The coordinator reports how long it has been idle
The system SHALL export `sqe_idle_seconds`: 0 while any statement runs, otherwise the seconds since the last statement finished (or since start). It SHALL export `sqe_admission_queue_length` and `sqe_admission_queue_wait_seconds`.

#### Scenario: Idle gauge
- **WHEN** no statement runs for 30 s
- **THEN** `sqe_idle_seconds` reads at least 30
- **AND** while a statement runs it reads 0
