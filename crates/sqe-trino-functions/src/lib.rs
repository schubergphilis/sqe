//! Trino-compatible scalar UDFs for DataFusion.
//!
//! DataFusion uses `extract(YEAR FROM d)` / `date_part('year', d)` while Trino
//! provides standalone functions like `year(d)`, `month(d)`, etc. This crate
//! bridges the gap so Trino SQL and dbt models work unmodified against an
//! SQE (DataFusion) backend.
//!
//! Two entry points:
//! - [`register_trino_functions`] — core date/time/url/encoding/json UDFs.
//! - [`register_extended_trino_functions`] — regex, unicode normalisation,
//!   stemming, timezone conversion, extended json helpers.
//!
//! Previously these lived in `sqe-coordinator` as sibling modules. Pulled out
//! into this crate so edits to the coordinator's hot files (write_handler,
//! query_handler) do not trigger a recompile of ~4.2k LOC of UDF code plus
//! its transitive deps (unicode-normalization, rust-stemmers, chrono-tz,
//! regex). See openspec change `split-trino-functions`.

pub mod trino_functions;
pub mod trino_functions_ext;

pub use trino_functions::register_trino_functions;
pub use trino_functions_ext::register_extended_trino_functions;
