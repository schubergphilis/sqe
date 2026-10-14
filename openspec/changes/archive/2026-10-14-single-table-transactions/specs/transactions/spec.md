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

#### Scenario: Concurrent writer causes a transaction conflict
- **GIVEN** a transaction that wrote to `t` while `main` was at snapshot S
- **AND** another session then committed to `t`, so `main` is no longer S
- **WHEN** the user runs `COMMIT`
- **THEN** the statement fails with `TRANSACTION_CONFLICT`
- **AND** `main` keeps the other session's commit
- **AND** the transaction's branch is removed and the transaction has ended

#### Scenario: Catalog unreachable at commit
- **GIVEN** a transaction that wrote to `t`
- **WHEN** `COMMIT` fails for a reason other than a conflict
- **THEN** the transaction stays open with its branch
- **AND** the user can send `COMMIT` again or `ROLLBACK`

### Requirement: ROLLBACK discards the transaction
The system SHALL remove the transaction's branch. Files its snapshots added are left for orphan-file cleanup.

#### Scenario: Rollback
- **GIVEN** a transaction that inserted rows into `t`
- **WHEN** the user runs `ROLLBACK`
- **THEN** `main` is unchanged
- **AND** the branch `sqe-txn-<id>` no longer exists

### Requirement: One table per transaction
The system SHALL refuse a write to a second table inside a transaction.
Narrowed by `multi-table-transactions` (#823) to catalogs that cannot commit several tables atomically.

#### Scenario: Second table
- **GIVEN** a transaction that wrote to `t1`
- **WHEN** the user runs `INSERT INTO t2 VALUES (1)`
- **THEN** the statement fails with "Multi-table writes not supported in a single transaction"
- **AND** no file is written for `t2`
- **AND** the transaction stays open with its writes to `t1`

### Requirement: In-flight transaction branches are not readable through SQE
The system SHALL refuse every way to name a `sqe-txn-*` branch or a snapshot reachable only from one.

#### Scenario: Time travel to a transaction branch
- **GIVEN** an open transaction on `t` with branch `sqe-txn-<id>`
- **WHEN** any session runs `SELECT * FROM t FOR VERSION AS OF 'sqe-txn-<id>'`
- **THEN** the statement fails and returns no rows

#### Scenario: Time travel to a branch-private snapshot id
- **GIVEN** a snapshot that only the branch `sqe-txn-<id>` reaches
- **WHEN** a session runs `FOR VERSION AS OF <that id>`, an incremental read starting or ending there, or `CREATE BRANCH` / `CREATE TAG` from it
- **THEN** the statement fails

#### Scenario: Metadata tables
- **GIVEN** an open transaction on `t`
- **WHEN** a session reads `t$refs` or `t$snapshots`
- **THEN** the transaction's branch and its private snapshots are not listed

#### Scenario: Write branch
- **WHEN** a session runs `SET WRITE_BRANCH 'sqe-txn-<id>'`, or `CREATE BRANCH` / `CREATE TAG` with a `sqe-txn-*` name
- **THEN** the statement fails

### Requirement: Transaction ids are owned
The system SHALL bind a transaction id to the user who opened it.

#### Scenario: Another user reuses an id
- **GIVEN** user A has an open transaction with id X
- **WHEN** user B sends a statement with transaction id X
- **THEN** the statement fails with "unknown transaction"

#### Scenario: Reusing an ended id
- **GIVEN** a transaction with id X that committed or rolled back
- **WHEN** a statement other than `ROLLBACK` is sent with transaction id X
- **THEN** it fails with `TRANSACTION_CONFLICT` saying the transaction has ended
