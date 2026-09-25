//! One integration-test binary for sqe-bench (was one per file, each linking
//! DataFusion). Add new tests as a module here, not as `tests/*.rs`.

mod comparison_classify;
mod direct_iceberg_sink;
mod generate_parallel;
mod load_ctas_snapshot;
mod query_qualify;
mod run_attach_gated;
mod single_pass_compare_gated;
