# Memory Safety (scan + write + retention)

## ADDED Requirements

### Requirement: Memory-gated scan parallelism
The system SHALL clamp the parallel-scan partition count to what the memory
pool can back, degrading toward a serial scan instead of aborting or
allocating past the pool.

#### Scenario: Plentiful memory keeps full parallelism
- **GIVEN** `execution.parallel_scan = true` and pool free space well above
  N x the estimated partition footprint
- **WHEN** a scan-bound query plans
- **THEN** the scan uses the threshold-derived N (no clamp)

#### Scenario: Pressure degrades, never aborts
- **GIVEN** `execution.parallel_scan = true` and pool free space below the
  footprint of 2 partitions
- **WHEN** a scan-bound query plans
- **THEN** the scan runs with N = 1 (today's behaviour)
- **AND** the query succeeds

#### Scenario: Decode buffers are visible to the pool
- **GIVEN** a running parallel scan
- **WHEN** pool reservations are dumped
- **THEN** the scan's decode buffering appears as a named reservation
  proportional to its in-flight channel capacity

### Requirement: Tracked write-sink buffers
The system SHALL account write-path buffering (ingest/CTAS, UPDATE/DELETE
rewrite, fanout) against the memory pool and SHALL degrade or fail with a
typed error instead of exhausting process memory.

#### Scenario: Oversized CTAS degrades then errors typed
- **GIVEN** a CTAS whose buffered write state approaches the pool cap
- **WHEN** the reservation cannot grow
- **THEN** the sink first flushes the current file early and drops the
  sort-on-write clustering
- **AND** only if a single batch cannot fit does the statement fail with
  `ResourceExhausted`
- **AND** the kernel OOM killer is never invoked (repro: TPC-DS SF10
  `inventory` CTAS at a 14GB cap on a 31GB host, 2026-07-06)

#### Scenario: Fanout closes files under pressure
- **GIVEN** a partitioned (fanout) write with many open partition files
- **WHEN** the summed reservation cannot grow
- **THEN** the largest open partition file is closed early and the write
  continues (more, smaller files)

#### Scenario: Kill-switch restores prior behaviour
- **GIVEN** `write.tracked_buffers = false`
- **WHEN** any write runs
- **THEN** buffering behaves exactly as before this change

### Requirement: Bounded cross-query retention
The system SHALL return per-query memory such that coordinator RSS stays
within a bounded envelope across long query sequences in one process.

#### Scenario: 200-query sweep stays bounded
- **GIVEN** a coordinator with an 8GB pool cap on a 31GB host
- **WHEN** the TPC-DS SF10 suite plus its 99-query comparison run
  sequentially in that one process (repro: 2026-07-06 kernel kill)
- **THEN** the process is not OOM-killed
- **AND** RSS after the final query is within 20% of RSS after the first

#### Scenario: Residue is attributable
- **GIVEN** any completed query
- **WHEN** the pool reservation dump is inspected
- **THEN** remaining reservations are zero or belong to named, budgeted
  caches
