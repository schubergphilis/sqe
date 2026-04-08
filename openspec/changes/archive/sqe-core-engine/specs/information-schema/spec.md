## ADDED Requirements

### Requirement: Virtual information_schema tables
The system SHALL provide virtual information_schema views (tables, columns, schemata) backed by Polaris catalog metadata, scoped to the user's access.

#### Scenario: Query information_schema.tables
- **WHEN** user queries `SELECT * FROM information_schema.tables WHERE table_schema = 'finance'`
- **THEN** tables accessible to the user in the `finance` schema are returned

#### Scenario: Query information_schema.columns
- **WHEN** user queries `SELECT * FROM information_schema.columns WHERE table_name = 'transactions'`
- **THEN** column metadata (name, type, nullable, ordinal) is returned from Iceberg table schema

#### Scenario: information_schema respects user access
- **GIVEN** two users with different Polaris permissions
- **WHEN** both query `information_schema.tables`
- **THEN** each sees only tables they are authorized to access
