//! One integration-test binary for sqe-lineage (was 16, one per file; each
//! linked DataFusion). Add new tests as a module here, not as `tests/*.rs`.
//! The insta snapshots carry the module path: `it__event_serialise_test__*`.

mod emit_pipeline_test;
mod emitter_test;
mod event_serialise_test;
mod extract_columns_test;
mod extract_datasets_test;
mod extract_governed_test;
mod extract_hint_test;
mod extract_lineage_ctas_test;
mod extract_lineage_test;
mod extract_outputs_test;
mod lineage_ctas_emit_test;
mod observer_test;
mod sink_multi_test;
mod sinks_file_test;
mod sinks_http_test;
mod sinks_spool_test;
