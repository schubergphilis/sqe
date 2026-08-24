# hive-worker-scan

## ADDED Requirements

### Requirement: A Hive scan MAY execute on workers
Hive external table scans SHALL be dispatchable to workers as `ScanTask`s, rather than always executing coordinator-local.

#### Scenario: a partitioned Hive table scans across workers
- **GIVEN** a coordinator with two registered workers and a partitioned Hive external table
- **WHEN** a query scans more than one partition
- **THEN** the scan is dispatched as per-partition tasks
- **AND** the returned rows are identical to coordinator-local execution of the same query

### Requirement: Partition columns materialize identically on both paths
Directory-encoded partition values SHALL produce the same values, types, and column order on the worker path as `attach_partition_values` produces coordinator-local.

#### Scenario: default partition and typed key
- **GIVEN** a table partitioned by an integer key, with one partition written as `__HIVE_DEFAULT_PARTITION__`
- **WHEN** the same query runs coordinator-local and worker-distributed
- **THEN** both paths return the same value and the same Arrow type for every partition column

### Requirement: An Iceberg ticket is unchanged
Adding the Hive descriptor SHALL NOT alter the serialized form of a non-Hive `ScanTask`.

#### Scenario: byte-identical v2 encoding
- **GIVEN** a `ScanTask` for an Iceberg table
- **WHEN** it is serialized by a v3 coordinator
- **THEN** the bytes are identical to the v2 encoding of the same task

### Requirement: The fallback is named, not silent
A Hive table the worker path cannot serve SHALL run coordinator-local and SHALL be observable as having done so.

#### Scenario: an unservable table is visibly local
- **GIVEN** a Hive table whose format or feature the worker path does not support
- **WHEN** it is queried on a distributed deployment
- **THEN** the query succeeds with correct rows
- **AND** the plan reports that the scan ran coordinator-local, with a reason
