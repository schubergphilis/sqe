### Fixed

- A view body plans like the statement that reads it (ai-portal-data#357, #359)

    An Iceberg view resolved unqualified names in schema `default`, whatever
    its version's `default-namespace` said, and SQE recorded the view's own
    namespace there rather than the schema the body was written in. A view
    created under `USE cat.staging` over `orders` therefore failed or read
    `default.orders`. SQE now records the creating session's schema as
    `default-namespace` and plans the body in it; the policy rewriter
    qualifies the body's scans with the same schema, so it governs the table
    the view actually reads.

    The body also planned in a bare DataFusion context: `year()`, `if()`
    and the other Trino functions, the JSON, variant and geo functions were
    unknown, lambdas did not parse, and `0.06` was a `DOUBLE` where a query
    has a `DECIMAL`. A view body now plans with the statement's parser
    dialect, session options and function set, shared through
    `session_tuning::register_engine_functions`.

    A view SQE created before this change recorded its own namespace, so its
    unqualified names now resolve there rather than in `default`. Recreate
    or qualify such a view if its body meant a table in `default`.
