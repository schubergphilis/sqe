### Changed

- `sqe-coordinator` trait-proof compile time cut from about 20.5 s to about 8.5 s

    Five Flight SQL and table-provider entry points each made rustc prove `Send`
    over DataFusion's `SessionContext` / `LogicalPlan` / `Expr` type graph in their
    own parameter environment. `create_session_context` and `plan_prepared_statement`
    now return a boxed future; the `CreatePreparedStatement` body and the
    `DoPut` ingest call moved, unchanged, into boxed inherent functions;
    `do_get_prepared_statement` goes through the existing boxed
    `run_resolved_into_flight_response`; `QueryHistoryTable::scan` is written in
    `async_trait`'s expanded form. Measured with `scripts/compile-profile.sh
    sqe-coordinator`, before and after alternating: `evaluate_obligation` self time
    20.2-20.8 s to 8.1-8.8 s, `typeck` control flat at 1.4-1.5 s, profiled crate
    compile 47-49 s to 35-38 s. No behaviour change.
