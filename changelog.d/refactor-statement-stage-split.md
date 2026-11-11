### Changed

- The buffered statement path runs in three stages (no behaviour change)

    `QueryHandler::execute_inner` was one 2,100-line function. Its head, everything up to
    and including the result-cache lookup, is now `preflight`, which returns a
    `StatementRun` or a cache hit. Its tail after the timeout and cancellation race is now
    `complete`. The dispatch match and the race stay in `execute_inner`. The guards (the
    abandoned-query guard, cancel token, sorted-write permit and admission) and the query
    span move into `StatementRun`, declared so that they drop in the same order as before.
    Compile time is unchanged: the baseline profile bills `execute_inner` 4 ms of trait
    solving, and the split adds nothing measurable (evaluate_obligation 14.97 s median
    before, 14.71 s after).
