## ADDED Requirements

### Requirement: Statements in a transaction see each other's writes
The system SHALL resolve reads of the transaction's table to the transaction's branch head.

#### Scenario: Delete sees a row inserted earlier in the transaction
- **GIVEN** table `t` with no row where `id = 7`
- **WHEN** the user runs `BEGIN; INSERT INTO t VALUES (7); DELETE FROM t WHERE id = 7; SELECT count(*) FROM t WHERE id = 7`
- **THEN** the `SELECT` returns 0

#### Scenario: Other sessions do not see uncommitted writes
- **GIVEN** an open transaction that inserted a row into `t`
- **WHEN** another session runs `SELECT * FROM t`
- **THEN** that session reads `main` and does not see the row

### Requirement: COMMIT publishes atomically or not at all
The system SHALL move `main` to the branch head in a single catalog commit that requires `main` to still be at the base snapshot.

#### Scenario: Commit publishes
- **GIVEN** a transaction with an `INSERT` and a `DELETE` on `t`
- **WHEN** the user runs `COMMIT`
- **THEN** `main` points at the branch head
- **AND** the branch `sqe-txn-<id>` no longer exists

#### Scenario: Concurrent writer causes a serialization failure
- **GIVEN** a transaction that wrote to `t` while `main` was at snapshot S
- **AND** another session then committed to `t`, so `main` is no longer S
- **WHEN** the user runs `COMMIT`
- **THEN** the statement fails with SQLSTATE `40001`
- **AND** `main` keeps the other session's commit
- **AND** the transaction's branch and files are removed

### Requirement: ROLLBACK leaves no trace
The system SHALL remove the transaction's branch and delete the files its snapshots added.

#### Scenario: Rollback
- **GIVEN** a transaction that inserted rows into `t`
- **WHEN** the user runs `ROLLBACK`
- **THEN** `main` is unchanged
- **AND** no data or delete file added by the transaction remains in storage

### Requirement: One table per transaction
The system SHALL refuse a write to a second table inside a transaction.

#### Scenario: Second table
- **GIVEN** a transaction that wrote to `t1`
- **WHEN** the user runs `INSERT INTO t2 VALUES (1)`
- **THEN** the statement fails with "Multi-table writes not supported in a single transaction"
- **AND** the transaction stays open with its writes to `t1`
