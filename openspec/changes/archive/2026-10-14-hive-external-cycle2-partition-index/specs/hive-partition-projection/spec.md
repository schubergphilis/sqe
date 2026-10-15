## ADDED Requirements

### Requirement: Projection properties are implemented
The system SHALL honor Athena partition projection TBLPROPERTIES (`projection.enabled` plus per-column `projection.<col>.*`) on Hive external tables. Projected partitions SHALL be computed at plan time from the projection config and SHALL NOT require storage listing for planning.

#### Scenario: Projected table scans without listing
- **WHEN** a table declares `projection.enabled=true` with an `enum`-typed column and a query scans it without any storage-side partitions existing yet
- **THEN** the plan is built from the computed partition set and no LIST of the table location occurs

#### Scenario: Predicate prunes a projected set
- **WHEN** a query filters on a projected column to a value outside its declared range
- **THEN** zero file groups are planned and the query returns no rows

#### Scenario: MSCK on a projected table is a no-op
- **WHEN** `MSCK REPAIR TABLE` runs on a table with `projection.enabled=true`
- **THEN** the statement succeeds, no storage listing is performed, and the partition index is left unchanged

#### Scenario: ADD/DROP PARTITION on a projected table is an error
- **WHEN** `ALTER TABLE ... ADD PARTITION` or `DROP PARTITION` runs on a projection-enabled table
- **THEN** the statement fails naming projection ownership of the partition set

### Requirement: Projection validation
Projection configuration SHALL be validated when the DDL parses AND again on every manifest load, treating it as untrusted input. Supported types are `enum`, `integer`, `date`, and `injected`. An unknown `projection.*` key or a malformed config SHALL be an error naming the key. A projected column MUST be a declared partition key.

#### Scenario: Malformed planted manifest fails closed
- **WHEN** a manifest is loaded whose parameters declare `projection.dt.type=banana`
- **THEN** the load fails naming the property, and the table is not queryable until repaired

#### Scenario: Unknown projection key errors at create
- **WHEN** DDL carries `'projection.dt.bogus' = 'x'` in TBLPROPERTIES
- **THEN** creation fails naming `projection.dt.bogus`

### Requirement: Injected columns
An `injected` projection column SHALL take its value from query context when provided and NULL otherwise, matching Athena's documented semantics as stated in the SQE reference docs.

#### Scenario: Injected column reads as NULL without context
- **WHEN** a query selects from a table with only `injected` projection columns configured and no injection context is provided
- **THEN** rows return with those columns NULL

## ADDED Requirements

### Requirement: Cycle-1 silent-ignore hazard closed
Until projection validation runs, no code path SHALL silently ignore `projection.*` properties. The cycle-1 documentation claiming rejection SHALL match actual behavior once this change lands: implemented, validated, never ignored.

#### Scenario: Docs and behavior agree
- **WHEN** the reference page's projection section is read against a deployed engine
- **THEN** every documented projection behavior is observable in that build
