pub mod access_control;
#[cfg(any(feature = "glue", feature = "s3tables"))]
pub mod aws_config;
pub mod backends;
pub mod circuit_breaker;
pub mod grant_chameleon;
#[cfg(feature = "rest")]
pub mod rest_catalog;
#[cfg(feature = "rest")]
pub mod catalog_provider;
#[cfg(feature = "rest")]
pub mod schema_provider;
#[cfg(feature = "rest")]
pub mod table_provider;
pub mod expr_to_predicate;
pub mod footer_cache;
#[cfg(feature = "rest")]
pub mod iceberg_metadata_tvf;
pub mod iceberg_scan;
pub mod incremental_provider;
pub mod incremental_scan;
#[cfg(feature = "rest")]
pub mod info_schema;
pub mod late_materialize;
pub mod mount;
pub mod parquet_writer_config;
pub mod pruning_stats;
pub mod puffin_stats;
pub mod file_tvf_common;
pub mod hf_tree_cache;
pub mod runtime_bridge;
pub mod lazy_object_store;
pub mod read_csv;
// `read_delta` is temporarily unwired for the DataFusion 54 bump: deltalake-core
// has no DF 54 release yet (its `DeltaTableProvider` targets an older DataFusion).
// The module file is kept on disk; restore this `pub mod` and the `delta` feature
// in Cargo.toml once delta-rs ships DF 54 support.
// pub mod read_delta;
pub mod read_json;
pub mod read_parquet;
pub mod scan_memory;
pub mod sort_order;
#[cfg(feature = "rest")]
pub mod system_catalog;
pub mod topk;
#[cfg(feature = "rest")]
pub mod system_jdbc;
#[cfg(feature = "rest")]
pub mod system_metadata;
pub mod system_runtime;
pub mod writable_iceberg_catalog;

pub use access_control::AccessControlClient;
#[cfg(any(feature = "glue", feature = "s3tables"))]
pub use aws_config::build_aws_config;
#[cfg(feature = "rest")]
pub use catalog_provider::SqeCatalogProvider;
pub use circuit_breaker::CircuitBreaker;
pub use footer_cache::FooterCache;
pub use iceberg_scan::IcebergScanExec;
pub use mount::build_catalog;
#[cfg(feature = "rest")]
pub use rest_catalog::{invalidate_rest_catalog_cache_all, SessionCatalog, TableMetadataCache};
pub use iceberg_scan::coalesce_file_entries;
#[cfg(feature = "rest")]
pub use system_catalog::{SystemCatalogEntry, SystemCatalogProvider};
