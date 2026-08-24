## ADDED Requirements

### Requirement: ANALYZE TABLE persists row counts
The system SHALL support `ANALYZE TABLE <t>` on Hive external tables by computing per-partition and total row counts and persisting them into the manifest's `sqe.statistics` slot. Parquet counts SHALL come from footer metadata; CSV and JSON counts require a full decode pass that ANALYZE explicitly pays. Planning statistics SHALL use persisted counts when present, falling back to cycle-1 size-derived estimates otherwise.

#### Scenario: Analyzed table plans from real cardinality
- **WHEN** `ANALYZE TABLE` runs on an indexed Parquet table and a join query follows
- **THEN** planning uses the persisted row counts, not size-derived estimates

#### Scenario: Un-analyzed table keeps size-only estimates
- **WHEN** a table has never been analyzed
- **THEN** planning statistics are unchanged from cycle 1

### Requirement: ANALYZE authorization
`ANALYZE TABLE` on a Hive external table SHALL pass the same per-user policy gate as reads.

#### Scenario: Denied user cannot analyze
- **WHEN** a user without read authorization runs `ANALYZE TABLE`
- **THEN** the statement is refused and no statistics are written
