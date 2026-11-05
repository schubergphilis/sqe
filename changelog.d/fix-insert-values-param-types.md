### Fixed

- A bound parameter in `INSERT ... VALUES` or `INSERT ... SELECT ?` is typed from its target column

    The INSERT source is planned on its own, so DataFusion left a placeholder
    written straight into a column untyped: a string bound to a BIGINT
    column then failed in the write's cast, and that error quoted the value
    in the answer and in the log. The placeholder now takes the column's
    type, with or without an explicit column list, so a numeric string is
    parsed and anything else is refused before the write as
    `parameter $n: cannot convert Utf8 to Int64`, as `UPDATE ... SET c = ?`
    already was. The check is the same as for UPDATE: a value of another
    type family (an integer for a STRING column, a DATE for a TIMESTAMP)
    is refused rather than cast.
