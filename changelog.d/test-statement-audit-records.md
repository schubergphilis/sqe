### Added

- Tests pin how many audit records one statement leaves, per outcome

    `statement_audit_records_test` counts the records in the native and the OCSF file for a refusal after classification, a result-cache hit, an execution failure, a timeout and a cancellation (one record each), and for a cancelled CTAS (the record still names the plan's target and source tables). It also pins the statements that write nothing today: the SQL size check, admission refusal and the `fail_mode = closed` refusal. A safety net for the `execute_inner` restructure; no server code changes.
