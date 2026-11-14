## ADDED Requirements

### Requirement: Hive locations are gated by the Hive data prefixes
A Hive table location or partition location SHALL be accepted on create, `ADD PARTITION` and load only when it lies under a prefix in `[hive] allowed_data_prefixes` or the root's `data_prefixes`, expanded for the session user, unless the fallback applies.

#### Scenario: location under a Hive data prefix, not a TVF prefix
- **WHEN** `[hive] allowed_data_prefixes = ["s3://lake/hive/"]`, no TVF prefix covers `s3://lake/hive/`, and a user creates an external table at `s3://lake/hive/sales/`
- **THEN** the table is created and a `SELECT` on it returns rows

#### Scenario: location outside every Hive data prefix
- **WHEN** a user creates an external table at `s3://lake/other/`, which no Hive data prefix covers
- **THEN** the statement fails naming the location, and no manifest is written

#### Scenario: fallback with no Hive list
- **WHEN** neither `allowed_data_prefixes` nor any root's `data_prefixes` is set
- **THEN** locations are checked against the TVF allow-list as before, and startup logs one deprecation warning

### Requirement: File TVFs refuse Hive data
A file TVF read with engine credentials SHALL be refused when its path lies under a Hive data prefix or under a Hive table location SQE has loaded, for every caller, admin roles and `allow_all_object_stores` included.

#### Scenario: raw read of Hive data refused
- **WHEN** a TVF prefix and a Hive data prefix both cover `s3://lake/hive/` and a user runs `read_parquet('s3://lake/hive/sales/part-0.parquet')`
- **THEN** the read fails naming the Hive prefix, and `SELECT` on the table still applies its policies

#### Scenario: loaded table under the fallback
- **WHEN** the fallback applies and a user has queried the Hive table at `s3://lake/hive/sales/`
- **THEN** `read_parquet` of a path under `s3://lake/hive/sales/` is refused

#### Scenario: inline credentials
- **WHEN** a TVF read under a Hive data prefix brings its own credentials
- **THEN** the Hive refusal does not apply

### Requirement: Startup names overlapping lists
At startup, SQE SHALL log an error for each pair of an allowed TVF prefix and a Hive data prefix that overlap, and SHALL keep starting.

#### Scenario: overlap at startup
- **WHEN** `allowed_object_store_prefixes = ["s3://lake/"]` and `allowed_data_prefixes = ["s3://lake/hive/"]`
- **THEN** startup logs one error naming both prefixes, and a TVF read under `s3://lake/hive/` is refused
