### Security

- Prepared statements bind their values into the plan on every path; no value becomes SQL text (finding 406)

    Trino `EXECUTE name USING ...`, Flight SQL prepared DML and DDL, and the
    re-plan of a prepared query whose vended credentials expired used to
    splice each value into the statement as a literal. They now plan the
    statement with `$1..$N` markers and bind typed values with DataFusion's
    `with_param_values`, as Flight SQL prepared queries already did. Every
    plan a write derives (a MERGE source, an UPDATE or DELETE filter, the
    partition pruning of a DELETE) binds the same values, before policy
    enforcement, so Ranger masks and row filters apply as they do to
    literal SQL. The query log, audit and lineage record the statement with
    its markers. A value whose type does not fit its placeholder is refused
    before execution, naming the position and types but not the value; a
    placeholder assigned in `UPDATE ... SET` or a MERGE clause takes the
    target column's type. `substitute_placeholders` and `render_bound_sql`
    are removed. Results of bound statements are not cached. Limits: a
    DataFusion execution error can still quote a value (a placeholder in a
    position DataFusion does not type, then a failing cast), and Trino
    `EXECUTE` of a template with markers in DDL (`DEFAULT`, table
    properties) now fails where text substitution ran it.
