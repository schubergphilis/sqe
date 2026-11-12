## ADDED Requirements

### Requirement: Template-aware overlap test
SQE SHALL treat an Iceberg table location and an allowed object-store TVF prefix as overlapping when either is equal to the other or lies under it. The comparison SHALL be on path segments after the same canonicalisation the TVF read check uses. A prefix segment containing `{user}` or `{catalog}` SHALL match any single segment. Global `allowed_object_store_prefixes` and every catalog's `tvf_prefixes` SHALL both be tested.

#### Scenario: Location under a template prefix
- **WHEN** the allowed prefix is `s3://stage/{catalog}/{user}/` and a table location is `s3://stage/sales/ann/t1`
- **THEN** the two overlap

#### Scenario: Prefix inside a table
- **WHEN** a table location is `s3://wh/ns/t` and an allowed prefix is `s3://wh/ns/t/data/`
- **THEN** the two overlap

#### Scenario: Segment boundary
- **WHEN** the allowed prefix is `s3://stage/ann/` and a table location is `s3://stage/anna/t`
- **THEN** the two do not overlap

#### Scenario: Scheme alias
- **WHEN** the allowed prefix is `s3a://wh/` and a table location is `s3://wh/ns/t`
- **THEN** the two overlap

### Requirement: Creating an Iceberg table inside an allowed prefix is refused
SQE SHALL refuse CREATE TABLE with a LOCATION, CTAS and table registration when the Iceberg table location overlaps an allowed prefix, and SHALL do so before any data file is written. The error SHALL name the configured prefix. When the catalog chooses the location, SQE SHALL check it after the create and before writing. On overlap it SHALL drop the empty table and fail the statement. If the drop fails, the statement SHALL still fail, the error SHALL name the table, and its location SHALL be recorded as in the load requirement.

#### Scenario: Explicit location
- **WHEN** a user runs `CREATE TABLE s.t (a int) LOCATION 's3://stage/sales/ann/t'` and `s3://stage/{catalog}/{user}/` is allowed
- **THEN** the statement fails naming `s3://stage/{catalog}/{user}/`, and no table exists

#### Scenario: Catalog-chosen location
- **WHEN** a CTAS gets a catalog-chosen location under an allowed prefix
- **THEN** the statement fails before writing data, and the table is dropped

#### Scenario: Drop not permitted
- **WHEN** the same CTAS runs as a caller who may create but not drop tables
- **THEN** the statement fails naming the empty table left behind, and TVF reads under its location are refused

### Requirement: Loaded Iceberg tables protect their files from file TVFs
When SQE loads an Iceberg table whose location overlaps an allowed prefix, it SHALL record the canonical location and log one error naming the table and the prefix. The test SHALL run on every load, cache hits included. File TVFs SHALL refuse every object-store path under a recorded location. The table itself SHALL stay queryable under its policies.

#### Scenario: Raw read of a misplaced table
- **WHEN** a table at `s3://stage/sales/ann/t` has been loaded, and a caller runs `read_parquet('s3://stage/sales/ann/t/data/*.parquet')` with that prefix allowed
- **THEN** the read fails naming the table location, and the table still answers `SELECT` with its row filters and masks applied

### Requirement: File TVFs probe for an Iceberg table above the path
Before the allow-list check, file TVFs SHALL look for `*.metadata.json` in `<ancestor>/metadata/` for each ancestor of the requested path at or below the matched prefix root, and SHALL refuse the read on a hit. A glob SHALL be probed on the ancestors of its literal leading part. A listed key inside a `metadata/` directory that holds `*.metadata.json` SHALL be refused. Probe results SHALL be cached per statement.

#### Scenario: Unloaded Iceberg table, data path
- **WHEN** an Iceberg table that SQE has never loaded lies at `s3://stage/sales/ann/t` under an allowed prefix, and a caller reads `s3://stage/sales/ann/t/data/*.parquet`
- **THEN** the read fails naming `s3://stage/sales/ann/t` as an Iceberg table directory

#### Scenario: Plain staging files
- **WHEN** a caller reads `s3://stage/sales/ann/batch1/*.parquet` and no ancestor holds Iceberg metadata
- **THEN** the read succeeds

### Requirement: Order of the overlap refusals
The overlap refusals SHALL run after the trusted-local and inline-credential exits and before the allow-all, admin-role and allow-list checks.

#### Scenario: Admin role does not skip it
- **WHEN** a caller with an `object_store_admin_roles` role reads under a recorded location
- **THEN** the read is refused

#### Scenario: Inline credentials are not refused
- **WHEN** a caller reads under a recorded location with complete inline credentials and `allow_inline_credentials = true`
- **THEN** the overlap refusal does not apply, because the read uses the caller's own keys

### Requirement: Shared prefixes are warned about
At startup SQE SHALL log one warning for each allowed object-store prefix that has no `{user}` placeholder.

#### Scenario: Prefix without user
- **WHEN** `allowed_object_store_prefixes = ["s3://stage/{catalog}/"]`
- **THEN** startup logs a warning that every caller with access to a catalog shares that prefix
