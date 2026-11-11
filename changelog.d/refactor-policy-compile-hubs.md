### Changed

- `sqe-policy` compiles in about half the time

    Four `#[async_trait]` methods (`PolicyPlanRewriter::evaluate`,
    `PassthroughEnforcer::evaluate`, `RangerStore::resolve`,
    `InMemoryPolicyStore::resolve`) each made rustc prove `Send` over DataFusion's
    `Expr` / `LogicalPlan` type graph in the method's own parameter environment. Their
    bodies now live in non-generic inherent functions that return a boxed future, so
    the proof is made once for the crate. Measured with `scripts/compile-profile.sh
    sqe-policy`, before and after back to back: trait-proof time 20.5 s to 4.3-6.7 s,
    profiled crate compile 33-37 s to 12-19 s. No behaviour change.
