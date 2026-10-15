### Added

- `query.max_cartesian_rows` refuses a read whose cross join or nested-loop join would compare more row pairs than the cap (#755)

    Every join runs on the coordinator, which all tenants share. Memory
    admission bounds a join's build side, but a cross join or a join with
    no equal-key condition can hold a core until the query timeout. The new
    `CartesianAdmissionRule` multiplies the two inputs' row-count statistics
    at plan time and refuses the query with a resource error that names the
    cap, never the estimates. Default 10,000,000,000; an input without
    statistics is admitted; `0` turns the gate off. Equi-joins, aggregations
    and the write path are not gated.
