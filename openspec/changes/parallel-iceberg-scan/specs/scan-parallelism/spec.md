## ADDED Requirements

### Requirement: Parallel single-node scan behind a flag
The system SHALL parallelize a single-node Iceberg scan across N partitions when `execution.parallel_scan` is enabled, and SHALL behave exactly as today (serial scan) when it is disabled.

#### Scenario: Flag off is unchanged
- **GIVEN** `execution.parallel_scan = false`
- **WHEN** any query runs
- **THEN** the scan advertises a single partition
- **AND** plans are identical to the pre-change behaviour

#### Scenario: Scan-bound query parallelizes
- **GIVEN** `execution.parallel_scan = true` on a 4+ core coordinator
- **AND** a scan-bound query (filter + projection over a large table)
- **WHEN** the query runs
- **THEN** the scan executes across N partitions
- **AND** the query is at least 2x faster than the single-partition baseline
- **AND** no `CoalescePartitionsExec` is inserted directly above the scan

### Requirement: No redundant exchange above a parallel scan
The system SHALL emit partitioning the optimizer can consume so that `EnforceDistribution` does not insert a redundant gather-and-rebuild above the scan.

#### Scenario: Join input stays partitioned (q72 guard)
- **GIVEN** `execution.parallel_scan = true`
- **AND** a query whose scan feeds a hash join (q72 shape)
- **WHEN** the physical plan is produced
- **THEN** the scan emits `RoundRobinBatch(N)` with an explicit `RepartitionExec(Hash(join_key), N)` above it
- **AND** the hash join is planned as `Partitioned`, not `CollectLeft`
- **AND** no `CoalescePartitionsExec` is inserted immediately above the scan

#### Scenario: Pipeline-only consumer needs no exchange
- **GIVEN** `execution.parallel_scan = true`
- **AND** a scan whose parent is a filter or projection with no distribution requirement
- **WHEN** the plan is produced
- **THEN** the scan emits `RoundRobinBatch(N)`
- **AND** no `RepartitionExec` or `CoalescePartitionsExec` is inserted

#### Scenario: Unrecognized parent stays serial
- **GIVEN** `execution.parallel_scan = true`
- **AND** a scan whose parent the partitioning pass does not recognize
- **WHEN** the plan is produced
- **THEN** the scan stays single-partition
- **AND** correctness does not depend on parallelizing it

### Requirement: q72 regression gate
The system SHALL gate the `parallel_scan` default-on flip on TPC-DS q72 not regressing.

#### Scenario: q72 within threshold
- **GIVEN** the committed baseline `compare-tpcds-sf1-2026-05-28T14:19:18.json` with q72 at 756ms
- **WHEN** TPC-DS q72 SF1 runs with `parallel_scan = true`
- **THEN** the q72 time is at most 1.1x the baseline
- **AND** only then does the `parallel_scan` default become `true`
