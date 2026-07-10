## MODIFIED Requirements

### Requirement: DELETE FROM with predicate

The system SHALL delete rows matching a WHERE predicate. When the predicate contains one or more `IN (subquery)` clauses, the subquery's result set SHALL be materialised once and joined into the per-file CoW SELECT as a semi-join. The plan text produced for CoW evaluation SHALL be O(1) in the subquery's row count.

#### Scenario: DELETE with single-column IN subquery, thousands of matches

- **GIVEN** a table `sales.orders` with 10,000 rows and a table `sales.cancelled_ids` with 3,500 rows
- **WHEN** user submits `DELETE FROM sales.orders WHERE order_id IN (SELECT id FROM sales.cancelled_ids)`
- **THEN** all 3,500 matching rows are removed via a CoW rewrite
- **AND** the coordinator does not abort
- **AND** the WHERE clause string fed to each per-file SELECT is O(1) in the keyset size

#### Scenario: DELETE with multi-column tuple IN subquery

- **GIVEN** a table `holdings` with columns `(account_id, symbol, qty)` and a source table `pending` with columns `(t_ca_id, t_s_symb)`
- **WHEN** user submits `DELETE FROM holdings WHERE (account_id, symbol) IN (SELECT t_ca_id, t_s_symb FROM pending)`
- **THEN** rows matching any `(t_ca_id, t_s_symb)` tuple from the pending subquery are removed
- **AND** the rewriter does not inline literals
- **AND** the pending subquery is executed once regardless of how many CoW data files are rewritten

#### Scenario: DELETE with NOT IN

- **GIVEN** a table `users` and a subquery listing active user ids
- **WHEN** user submits `DELETE FROM users WHERE id NOT IN (SELECT active_id FROM active_users)`
- **THEN** rows whose `id` does not equal any non-NULL value in the active_users subquery are removed
- **AND** NULL values in the active_users subquery are dropped from the matcher (matches current rewriter semantics; documented deviation from strict SQL)

#### Scenario: DELETE with empty IN subquery

- **GIVEN** a subquery `SELECT id FROM empty_table` that returns zero rows
- **WHEN** user submits `DELETE FROM t WHERE id IN (SELECT id FROM empty_table)`
- **THEN** no rows are deleted
- **AND** the corresponding `NOT IN` variant removes all rows in `t`

#### Scenario: DELETE with million-row IN subquery does not exhaust the stack

- **GIVEN** a table `t` with 100,000 rows and a keyset table `keys` with 1,000,000 rows
- **WHEN** user submits `DELETE FROM t WHERE k IN (SELECT k FROM keys)`
- **THEN** the statement completes within 30 seconds on a release build
- **AND** the coordinator process does not abort
- **AND** peak coordinator memory for the IN-subquery materialiser is less than 500 MB

### Requirement: UPDATE with predicate

The system SHALL update rows matching a WHERE predicate. When the predicate contains one or more `IN (subquery)` clauses, the subquery's result set SHALL be materialised once and joined into the per-file CoW SELECT that builds the `CASE WHEN <where> THEN <new> ELSE <old> END` projection. The WHERE expression passed into each per-file CASE WHEN SHALL reference a boolean flag column from the joined keyset, not a literal OR-chain.

#### Scenario: UPDATE TPC-E trade_result_update_holding at SF10

- **GIVEN** a TPC-E SF10 dataset loaded into Iceberg
- **AND** `trade` has 34,496 rows with `t_st_id = 'PNDG'`
- **WHEN** user submits the TPC-E `trade_result_update_holding` statement (UPDATE holding_summary ... WHERE (hs_ca_id, hs_s_symb) IN (SELECT t.t_ca_id, t.t_s_symb FROM trade t WHERE t.t_st_id = 'PNDG'))
- **THEN** the statement completes without stack overflow
- **AND** the correct number of holding_summary rows are updated
- **AND** a subsequent TPC-E trade_cleanup run matches the expected state

#### Scenario: UPDATE with correlated scalar SET and multi-column IN WHERE

- **GIVEN** an UPDATE whose `SET` clause contains a correlated scalar subquery and whose WHERE contains a multi-column IN subquery
- **WHEN** the statement is submitted
- **THEN** the correlated scalar subquery is decorrelated via the existing `decorrelate_scalar_subqueries` path
- **AND** the IN subquery is lifted to a LEFT JOIN via the new lifter
- **AND** both join sets are injected into the outer CoW SELECT's FROM clause, decorrelator joins first then IN-subquery joins

#### Scenario: UPDATE with multi-column IN whose subquery has duplicate tuples

- **GIVEN** a subquery producing duplicate `(c1, c2)` tuples
- **WHEN** the UPDATE is evaluated
- **THEN** each target row is updated at most once (no row multiplicity from the join)
- **AND** the behaviour matches what the old literal-inlining rewriter produced

### Requirement: MoR DELETE with predicate

The system SHALL support Merge-on-Read DELETE with `IN (subquery)` predicates without materialising the subquery into a literal OR chain.

#### Scenario: MoR DELETE with multi-column tuple IN

- **GIVEN** a table configured to use MoR DELETE
- **WHEN** user submits `DELETE FROM t WHERE (a, b) IN (SELECT a, b FROM src)`
- **THEN** position delete files are written for matching rows
- **AND** no stack overflow occurs regardless of the subquery cardinality

## ADDED Requirements

### Requirement: IN-subquery scratch MemTable lifecycle

The coordinator SHALL register each lifted IN-subquery as a scratch MemTable under a globally unique name prefixed `__sqe_in_subq_` and SHALL deregister every such MemTable before the DML statement returns to the caller, including on error paths.

#### Scenario: Scratch tables are deregistered on success

- **GIVEN** an UPDATE statement using two IN subqueries
- **WHEN** the statement commits
- **THEN** both scratch MemTables are removed from the session catalog
- **AND** a subsequent `SHOW TABLES` in the same session does not list them

#### Scenario: Scratch tables are deregistered on failure

- **GIVEN** an UPDATE statement using an IN subquery, where the CoW commit fails (catalog error, storage I/O error, etc.)
- **WHEN** the error propagates out of the handler
- **THEN** the scratch MemTable is still deregistered via the RAII guard
- **AND** no session-level resource leak remains

#### Scenario: Concurrent DML does not collide on scratch names

- **GIVEN** two concurrent DML statements in different sessions each lifting one IN subquery
- **WHEN** both statements execute at overlapping times
- **THEN** their scratch MemTables have distinct names (via a process-global atomic counter)
- **AND** neither statement sees the other's scratch MemTable

### Requirement: NULL handling in IN-subquery lifting

The coordinator SHALL drop rows from the lifted keyset that have NULL in any matcher column. Outer target rows with NULL in any matcher column SHALL NOT match the keyset. This matches the behaviour of the previous literal-inlining rewriter and may differ from strict SQL `IN`/`NOT IN` semantics in the presence of NULLs in the subquery.

#### Scenario: NULLs in the subquery are dropped from the keyset

- **GIVEN** a subquery `SELECT k FROM src` where some rows have `k = NULL`
- **WHEN** the UPDATE with `WHERE col IN (SELECT k FROM src)` runs
- **THEN** the NULL rows from the subquery do not match any outer row
- **AND** outer rows whose `col` happens to be NULL also do not match

#### Scenario: `NOT IN` with NULLs returns TRUE for non-NULL non-matches

- **GIVEN** a subquery containing NULL
- **WHEN** the UPDATE with `WHERE col NOT IN (SELECT k FROM src)` runs
- **THEN** outer rows whose `col` equals no non-NULL value in the subquery are updated
- **AND** this behaviour is documented as intentional (not strict SQL)
