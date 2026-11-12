### Changed

- `INSERT INTO` dispatch moved out of `execute_inner` into `handle_insert_statement` (no behaviour change)

    A move of the inline arm into its own method, the third of the six inline dispatch
    arms the statement-path restructure lifts. The Hive external and Iceberg paths move
    together; the lineage hint and policy summary are written through `&mut` parameters.
    `execute_inner` goes from 1,198 to 1,114 lines.
