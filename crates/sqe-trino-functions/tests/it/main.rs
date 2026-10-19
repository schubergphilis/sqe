//! One integration-test binary for sqe-trino-functions (was one per file,
//! each linking DataFusion). Add new tests as a module here.

mod column_input_test;
mod trino_udf_test;
mod variant_json_args_test;
