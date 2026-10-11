## MODIFIED Requirements

### Requirement: One table per transaction
On a catalog that cannot commit several tables atomically, the system SHALL refuse a write to a second table inside a transaction, before any file is written.

#### Scenario: Second table on a catalog without an atomic multi-table commit
- **GIVEN** a transaction that wrote to `t1` in a catalog that does not advertise `POST /v1/{prefix}/transactions/commit`
- **WHEN** the user runs `INSERT INTO t2 VALUES (1)`
- **THEN** the statement fails with "Multi-table writes not supported in a single transaction on this catalog"
- **AND** no file is written for `t2`
- **AND** the transaction stays open with its writes to `t1`

## ADDED Requirements

### Requirement: Multi-table atomic commit
On a catalog that commits several tables atomically, the system SHALL publish every table a transaction wrote in one catalog commit, or none of them.

#### Scenario: Two tables commit together
- **GIVEN** a transaction that inserted into `a` and deleted from `b`
- **WHEN** the user runs `COMMIT`
- **THEN** both changes are visible to other sessions
- **AND** no `sqe-txn-*` branch remains on either table

#### Scenario: A conflict on one table aborts both
- **GIVEN** a transaction that wrote to `a` and `b`
- **AND** another session committed to `b` after the transaction's branch on `b` was created
- **WHEN** the user runs `COMMIT`
- **THEN** the statement fails with `TRANSACTION_CONFLICT`
- **AND** neither `a` nor `b` changed
- **AND** no `sqe-txn-*` branch remains on either table

### Requirement: One catalog per transaction
The system SHALL refuse a write to a table in a second catalog inside a transaction.

#### Scenario: Second catalog
- **GIVEN** a transaction that wrote to a table in catalog `c1`
- **WHEN** the user writes to a table in catalog `c2`
- **THEN** the statement fails with "A transaction cannot write tables in two catalogs"
- **AND** no file is written
