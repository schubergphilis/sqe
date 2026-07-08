//! Output sinks for generated benchmark data.
//!
//! The default sink is the local Parquet directory layout handled inside
//! `generate` (staging files that `sqe-bench load` turns into Iceberg
//! tables through the engine). The `iceberg` sink here writes the data
//! straight into Iceberg tables through the catalog REST API instead:
//! every byte is written once, the engine stays out of the loop, and the
//! catalog owns table metadata from the moment of creation.

pub mod iceberg;
