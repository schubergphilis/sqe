### Changed

- `CALL system.<procedure>` dispatch moved out of `execute_inner` into `handle_procedure_statement` (no behaviour change)

    A move of the inline arm into its own method, the fifth of the six inline dispatch
    arms the statement-path restructure lifts. The maintenance lineage hint is written
    through `&mut captured_plan`. `execute_inner` goes from 979 to 841 lines.
