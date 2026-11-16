## ADDED Requirements

### Requirement: Only the virtual schemas of `system` are served by SQE
The system SHALL serve `system.runtime`, `system.metadata`, `system.jdbc` and `system.information_schema` from its virtual provider, and SHALL resolve every other schema under `system` through a Polaris REST catalog named `system` when one exists. Without such a catalog, behaviour SHALL be as before this change.

#### Scenario: Virtual tables unchanged
- **GIVEN** a Polaris catalog named `system` exists
- **WHEN** a user runs `SELECT * FROM system.runtime.queries`
- **THEN** the rows are the same as without the Polaris catalog

#### Scenario: No Polaris system catalog
- **GIVEN** no Polaris catalog named `system`
- **WHEN** the existing `system.*` tests run
- **THEN** they pass unchanged

### Requirement: The Polaris part of `system` is governed like any catalog
Tables in the Polaris catalog `system` SHALL be read and written with the caller's token and SHALL pass Ranger resource policies, masks, row filters, tag policies, the Polaris load check, audit and lineage, exactly as tables of any other catalog.

#### Scenario: Read as the caller
- **GIVEN** a Polaris table `system.access.audit`
- **WHEN** a user with a Ranger grant selects from it
- **THEN** the rows are returned
- **AND** a user without the grant is refused

#### Scenario: Tag mask applies
- **GIVEN** a tag mask on a column of `system.access.audit`
- **WHEN** a user subject to the mask selects that column
- **THEN** the masked values are returned

### Requirement: Reserved schema names cannot be created in the Polaris `system` catalog
The system SHALL refuse `CREATE SCHEMA` for `runtime`, `metadata`, `jdbc` or `information_schema` in catalog `system`, and SHALL ignore a Polaris namespace of such a name that already exists.

#### Scenario: Refuse reserved schema
- **WHEN** a user runs `CREATE SCHEMA system.runtime`
- **THEN** the statement is refused with a message naming the reserved schema

### Requirement: Listings merge both parts
`SHOW SCHEMAS FROM system` SHALL list the virtual schemas and the Polaris schemas the caller can see. The Trino compatibility layer and MCP SHALL follow the same split.

#### Scenario: SHOW SCHEMAS
- **GIVEN** a Polaris catalog `system` with namespace `access`
- **WHEN** a user runs `SHOW SCHEMAS FROM system`
- **THEN** the result contains `access` and `runtime`

### Requirement: System procedures keep their meaning
`CALL system.<procedure>` SHALL resolve to the built-in procedure whether or not a Polaris catalog `system` exists.

#### Scenario: Procedure with Polaris system present
- **GIVEN** a Polaris catalog named `system`
- **WHEN** a user runs `CALL system.rewrite_data_files(table => 's.t')` in catalog `c`
- **THEN** the procedure runs on `c.s.t`
