### Fixed

- `EXECUTE ... USING` binds literals only, and a negative bound value no longer opens a `--` comment

    `EXECUTE name USING ...` over the Trino HTTP API rendered any expression
    back to SQL and spliced it into the template, so a function call or a
    subquery ran as part of the statement. It now accepts numbers, strings,
    booleans, `NULL`, typed literals (`DATE '...'`, `DECIMAL '...'`,
    `TIMESTAMP '...'`), intervals and signed numbers, and refuses anything
    else naming the parameter position. Both text binders (Trino `EXECUTE`
    and Flight SQL prepared DML) put a negative value in parentheses: `1-?`
    bound to `-5` used to read `1--5`, a line comment that dropped the rest
    of the line, including any predicate after the placeholder. Flight SQL
    binds NaN and infinity as `CAST('NaN' AS DOUBLE)` instead of a bare word
    that parsed as a column name.
