//! One integration-test binary for sqe-sql (was one per file, each linking
//! DataFusion). Add new tests as a module here, not as `tests/*.rs`.

mod attach_parser_test;
mod hive_ddl_test;
mod hive_partitions_test;
mod parser_proptest;
mod v3_types;
