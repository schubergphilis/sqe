## ADDED Requirements

### Requirement: Black-box measurement

The suite SHALL measure SQE only through its client protocols (Trino HTTP and Flight SQL), the Iceberg REST catalog and object storage. It SHALL NOT import SQE code, read SQE logs or call SQE-internal endpoints.

#### Scenario: Feature missing on one protocol
- **GIVEN** time travel works over Flight SQL and is rejected over Trino HTTP
- **WHEN** the time-travel cell runs
- **THEN** the cell's level reflects the Flight SQL result
- **AND** the Trino HTTP rejection is listed under "Protocol gaps", not as a partial

#### Scenario: Run against a published image
- **GIVEN** a machine with Docker, Python 3.11+ and access to an SQE image
- **WHEN** `tests/matrix/docker/start-sqe.sh` runs with `SQE_IMAGE=<tag>` and then `python tests/matrix/sqe_feature_tests.py`
- **THEN** every result is produced without a source checkout build

### Requirement: Rubric-driven completeness

The set of cells SHALL come from the pinned rubric (`docs/evidence/iceberg-matrix-rubric.json`). Every rubric cell SHALL have exactly one registered test.

#### Scenario: Rubric gains a feature
- **GIVEN** the rubric pin is bumped and adds a feature
- **WHEN** the suite runs without a test for it
- **THEN** the cell is reported `untested` and the run exits non-zero

### Requirement: Exact verdict mapping

A cell's measured level SHALL be `full` for a pass without a partial reason, `partial` for a pass with one, and `none` for a fail. A skip is `unmeasured`. A cell agrees only when the measured and claimed levels are equal.

#### Scenario: Partial pass against a full claim
- **GIVEN** the state file claims `full` for a cell
- **WHEN** the test passes with a `partial_reason`
- **THEN** the cell is a discrepancy and the run exits non-zero

### Requirement: Independent write witness

Every cell that writes data SHALL confirm the result by reading the table through a reader other than SQE (PyIceberg, or the raw catalog metadata, manifests and data files for v3 features PyIceberg cannot decode). It SHALL assert the table's `format-version` from the catalog.

#### Scenario: Deletion vectors
- **WHEN** the v3 deletion-vector test deletes rows through SQE
- **THEN** PyIceberg shows a snapshot whose delete files are Puffin deletion vectors on a `format-version` 3 table
- **AND** an SQE-only readback of the remaining rows alone does not pass the cell

### Requirement: Honest report

The report SHALL state measured, agreed, discrepancy, unmeasured and untested counts out of the rubric total. It SHALL give a score over measured cells only beside the claimed score, and list each unmeasured cell with its skip reason.

#### Scenario: Cloud catalog cells without credentials
- **WHEN** the suite runs without AWS, Databricks, Snowflake or GCP credentials
- **THEN** those catalog cells are `unmeasured`, are not counted as agreement, and the header reads "measured N of 54"

### Requirement: Findings are not auto-applied

The suite SHALL NOT modify `iceberg-matrix-state.json`. Discrepancies SHALL be written as proposed edits (cell, claimed, measured, evidence) for a maintainer to apply or reject.

#### Scenario: Measured lower than claimed
- **WHEN** a cell measures `partial` against a `full` claim
- **THEN** the report lists it under proposed edits and the state file is unchanged

### Requirement: Upstream-compatible layout and export

The suite SHALL accept upstream's environment variables (`ICEBERG_REST_URI`, `ICEBERG_REST_WAREHOUSE`, `ICEBERG_REST_CREDENTIAL`, `ICEBERG_REST_SCOPE`, `ICEBERG_S3_*`) so it runs against upstream's own Polaris stack. `export_upstream.py` SHALL produce the upstream engine-file shape from the state file.

#### Scenario: Export round-trip
- **WHEN** `export_upstream.py --check` runs
- **THEN** it writes `{"platforms":[...],"support":{...}}`, the export's level for each of the 54 cells equals the state file's, and the total under the rubric weights equals the state file's score
