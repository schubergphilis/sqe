### Changed

- `CREATE TABLE ... AS SELECT` dispatch moved out of `execute_inner` into `handle_ctas_statement` (no behaviour change)

    A move of the inline arm into its own method, the last of the six inline dispatch
    arms the statement-path restructure lifts. The Hive external and Iceberg paths move
    together; the lineage hint and policy summary are written through `&mut` parameters.
    `execute_inner` goes from 841 to 702 lines, down from 2,100 before the restructure.
