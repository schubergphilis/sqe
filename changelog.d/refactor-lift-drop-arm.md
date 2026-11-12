### Changed

- `DROP TABLE` dispatch moved out of `execute_inner` into `handle_drop_statement` (no behaviour change)

    A move of the 77-line inline arm into its own method, the first of the six inline
    dispatch arms the statement-path restructure lifts. It writes the lineage hint through
    `&mut captured_plan`, so a failed or cancelled DROP still names its table.
    `execute_inner` goes from 1,332 to 1,257 lines.
