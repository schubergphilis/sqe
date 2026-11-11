## ADDED Requirements

### Requirement: One audit record per statement
The coordinator SHALL write exactly one audit record for every statement that reaches classification, whether the statement succeeds, is answered from the result cache, fails, times out or is cancelled.

#### Scenario: Refusal before classification
- **WHEN** a statement is refused by the size check, the audit-closed check or admission, which all return before the failure-audit path exists
- **THEN** it writes whatever it writes today, pinned by a test added before the restructure, and the restructure does not change it

#### Scenario: Failure between classification and the query tracker
- **WHEN** a statement is refused during preflight after it was classified (classification error, pressure refusal, transaction guard, reserved property, file-function preparation)
- **THEN** one failure record is written and the completion stage writes none

#### Scenario: Result-cache hit
- **WHEN** a query is answered from the result cache
- **THEN** preflight writes one success record, with the resources and policy summary of the run that filled the entry, and the dispatch and completion stages do not run

#### Scenario: Failure during execution
- **WHEN** a statement fails after the query tracker has started
- **THEN** one record is written by the completion stage and none by preflight

#### Scenario: Timeout
- **WHEN** a statement exceeds its session timeout
- **THEN** one record is written with the timeout error

### Requirement: Partial lineage survives timeout and cancellation
The coordinator SHALL record the tables a statement touched in its audit record even when the statement did not finish.

#### Scenario: Cancelled write
- **WHEN** a write statement is cancelled after its plan was built
- **THEN** the audit record lists the tables from that plan

### Requirement: Exhaustive statement dispatch
Every statement kind SHALL be routed by a match without a wildcard arm, so that adding a kind without a handler fails to compile.

#### Scenario: New statement kind
- **WHEN** a variant is added to `StatementKind` and no dispatch arm is written
- **THEN** `sqe-coordinator` does not compile

### Requirement: Guards span the whole statement
The admission slot, the sorted-write permit, the abandoned-query guard and the cancellation token SHALL be held until the statement call returns, as they are today, and SHALL be released in today's order.

#### Scenario: Admission is released after completion
- **WHEN** a statement finishes
- **THEN** its admission slot is released after the audit record and metrics are written, not before

#### Scenario: Span fields
- **WHEN** a stage records a field on the request span or on the query span
- **THEN** the field lands on the same span as before the restructure, because the query span is created once in preflight and carried in the run context

#### Scenario: Abandoned request
- **WHEN** the client drops the request while the statement runs
- **THEN** the abandoned-query guard cancels the statement and the tracker entry is removed

### Requirement: Restructuring is behaviour-neutral
A change made under this specification SHALL NOT alter the result, error code, audit content, metrics or span fields of any statement.

#### Scenario: Integration suite
- **WHEN** a restructuring merge request is verified
- **THEN** the integration tests for the statement kinds it touches pass unchanged, and no test expectation is edited in that merge request
