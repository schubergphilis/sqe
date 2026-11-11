### Changed

- `MERGE INTO` dispatch moved out of `execute_inner` into `handle_merge_statement` (no behaviour change)

    A move of the inline arm into its own method, the second of the six inline dispatch
    arms the statement-path restructure lifts. It writes the lineage hint and the policy
    summary through `&mut` parameters, so completion still sees both after a failure or
    cancellation. `execute_inner` goes from 1,257 to 1,198 lines.
