### Changed

- `sqe-catalog` trait-proof compile time cut from about 16-27 s to about 7 s

    Four entry points made rustc prove `Send` over DataFusion's `SessionState` /
    `LogicalPlan` / `Expr` type graph in their own parameter environment:
    `HiveSchemaProvider::table`, `HiveTableProvider::scan` and `scan_with_args`,
    and `EngineQueryPlanner::create_physical_plan`, plus the `MemTable` held by
    `ReservedMemTable`. `HiveSchemaProvider::table` is written in `async_trait`'s
    expanded form and its body moved, unchanged, into an inherent `load_table`;
    the Hive scans and the query planner build their scoped `SessionState` before
    the boxed future and hold it as a `SendErased` value, with the planner's
    decision logic lifted into `EngineQueryPlanner::tuned_state`;
    `ReservedMemTable` keeps its inner table as `Arc<dyn TableProvider>`.
    Measured with `scripts/compile-profile.sh sqe-catalog`, before and after
    alternating over three pairs: `evaluate_obligation` self time 16.5-27.3 s to
    6.9-7.7 s, `typeck` control 1.1-2.0 s before and 1.0-1.7 s after (1.07 s and 0.98 s in the
    matched third pair, 16.5 s to 7.2 s). No behaviour change.
