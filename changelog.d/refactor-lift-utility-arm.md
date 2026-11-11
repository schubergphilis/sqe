### Changed

- Utility statement dispatch (EXPLAIN, SHOW COLUMNS, DESCRIBE, SET TIME ZONE, SHOW FUNCTIONS, SHOW ROLES) moved out of `execute_inner` into `handle_utility_statement` (no behaviour change)

    A move of the inline arm into its own method, the fourth of the six inline dispatch
    arms the statement-path restructure lifts. `execute_inner` goes from 1,114 to 979 lines.
