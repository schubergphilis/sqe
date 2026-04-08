## ADDED Requirements

### Requirement: Coordinator/worker architecture
The system SHALL support a coordinator process that plans and schedules queries, and 0..N stateless worker processes that execute plan fragments.

#### Scenario: Single-node mode (no workers)
- **GIVEN** no workers are registered
- **WHEN** a query is submitted
- **THEN** the coordinator executes the query locally using its own DataFusion runtime

#### Scenario: Distributed mode (workers available)
- **GIVEN** 2+ workers are registered
- **WHEN** a large query is submitted
- **THEN** the coordinator splits the PhysicalPlan into fragments and distributes them to workers

### Requirement: Worker registration and heartbeat
Workers SHALL register with the coordinator on startup and maintain liveness via periodic heartbeats.

#### Scenario: Worker registration
- **WHEN** a worker process starts
- **THEN** it registers with the coordinator via Arrow Flight
- **AND** begins sending heartbeats every 5 seconds

#### Scenario: Worker deregistration on missed heartbeats
- **GIVEN** a registered worker
- **WHEN** the coordinator misses 3 consecutive heartbeats (15 seconds)
- **THEN** the worker is removed from the registry
- **AND** any in-flight fragments are re-assigned or failed

### Requirement: Adaptive fragment splitting
The system SHALL split queries into fragments based on Iceberg manifest/data file groups, adapting granularity to data size.

#### Scenario: Small table stays local
- **GIVEN** a table with a single manifest
- **WHEN** a query is planned
- **THEN** a single fragment is created and executed locally

#### Scenario: Large table distributes across manifests
- **GIVEN** a table with 100 manifests
- **WHEN** a query is planned
- **THEN** fragments are created per manifest group and distributed across workers

### Requirement: Fragment transport via Arrow Flight
The system SHALL serialize PhysicalPlan fragments using datafusion-proto with custom codec extensions for iceberg-rust plan nodes, and transport them to workers via Arrow Flight do_exchange with credentials in metadata.

#### Scenario: Fragment with credentials
- **WHEN** a fragment is sent to a worker
- **THEN** the Arrow Flight metadata includes bearer_token, vended_s3_creds, fragment_id, session_id
- **AND** the payload contains the serialized PhysicalPlan fragment

### Requirement: Streaming results with backpressure
Workers SHALL stream Arrow RecordBatches back to the coordinator. Backpressure SHALL propagate from client to coordinator to workers.

#### Scenario: Backpressure on slow client
- **GIVEN** a client consuming results slowly
- **WHEN** the coordinator's output buffer fills
- **THEN** workers pause execution until buffer space is available

### Requirement: Spill-to-disk for memory management
The system SHALL use DataFusion's memory manager with configurable per-worker memory limits and disk spill for operations exceeding memory.

#### Scenario: Sort exceeds memory limit
- **GIVEN** a worker with 8GB memory limit
- **WHEN** a sort operation requires 12GB
- **THEN** DataFusion spills intermediate data to disk
- **AND** the query completes successfully

### Requirement: Failure handling
The system SHALL re-assign read fragments on worker failure and fall back to local execution if no workers are available.

#### Scenario: Worker failure mid-fragment
- **GIVEN** a worker executing a read fragment
- **WHEN** the worker dies
- **THEN** the coordinator re-assigns the fragment to another available worker

#### Scenario: All workers lost
- **GIVEN** all workers have failed
- **WHEN** a query is in progress
- **THEN** the coordinator falls back to local execution for remaining fragments
