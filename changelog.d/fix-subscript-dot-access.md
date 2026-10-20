### Fixed

- A struct field reached by dot after a subscript, `items[1].sku`, is read

    DataFusion plans a dot only in the leading `t.s.a` run of a field
    access, so `items[1].sku` and `(s).a` failed with "Dot access not
    supported for non-string expr", although Trino and Spark accept both.
    The SQL front door now rewrites each such `.field` to `['field']` on
    every wire (Flight SQL, Trino HTTP, embedded, MCP). An unquoted name
    folds to lowercase; a quoted one keeps its case.
