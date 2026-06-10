## ADDED Requirements

### Requirement: Multi-stage distributed execution
The system SHALL decompose a query into a stage DAG and execute the stages wave by wave when `distribution_mode = multi_stage`.

#### Scenario: Two-table join distributes both sides
- **GIVEN** `distribution_mode = multi_stage` and a join over two large tables
- **WHEN** the query runs across N workers
- **THEN** both join inputs are hash-partitioned on the join key and shuffled
- **AND** neither side is pulled in full to the coordinator
- **AND** the result matches single-node execution

#### Scenario: Aggregate runs partial-on-worker
- **GIVEN** `distribution_mode = multi_stage` and a `GROUP BY` aggregation over a large scan
- **WHEN** the query runs
- **THEN** workers compute partial aggregates
- **AND** the coordinator (or a shuffled final stage) merges them
- **AND** the result matches single-node execution

#### Scenario: Filters and projections push to workers
- **GIVEN** a query with a `WHERE` clause and a column projection over a distributed scan
- **WHEN** the query runs in multi-stage mode
- **THEN** the filter and projection execute inside the leaf scan stage on the workers
- **AND** only matching, projected rows cross the shuffle boundary

### Requirement: Shuffle write over do_exchange
The system SHALL partition and ship stage output to target executors over Flight `do_exchange`.

#### Scenario: ShuffleWriter ships all rows
- **GIVEN** a stage with a `ShuffleWriterExec` hash-partitioning on a key
- **WHEN** the stage executes
- **THEN** every input row is routed to exactly one target partition by hash
- **AND** the downstream `ShuffleReaderExec` receives all rows with none lost or duplicated

### Requirement: Shuffle completion is explicit
The system SHALL signal end-of-stream per shuffle partition so that an incomplete or failed shuffle fails the query instead of truncating the result.

#### Scenario: Decode error fails the query
- **GIVEN** an in-flight shuffle between two stages
- **WHEN** a `do_exchange` stream hits a batch decode error
- **THEN** the receiving partition is marked failed
- **AND** the query fails with a clear error
- **AND** no partial / short result is returned

#### Scenario: Missing completion marker fails the stage
- **GIVEN** a shuffle partition whose sender channel closes before the end-of-stream marker
- **WHEN** the reader detects the close
- **THEN** the stage fails
- **AND** the query fails rather than proceeding with fewer rows

#### Scenario: Backpressure bounds shuffle memory
- **GIVEN** a fast shuffle writer and a slow shuffle reader
- **WHEN** the bounded channel fills
- **THEN** the writer blocks until the reader drains
- **AND** shuffle buffering does not grow unbounded

### Requirement: Composition with scan distribution
The system SHALL keep `DistributedScanExec` as the leaf-stage source and support `scan_only` as the degenerate single-stage configuration.

#### Scenario: scan_only mode unchanged
- **GIVEN** `distribution_mode = scan_only`
- **WHEN** a query runs
- **THEN** only the scan is distributed and the rest executes on the coordinator
- **AND** behaviour matches the pre-change distributed path
