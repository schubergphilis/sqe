pub mod access_control;
#[cfg(any(feature = "glue", feature = "s3tables"))]
pub mod aws_config;
pub mod backends;
#[cfg(feature = "rest")]
pub mod catalog_provider;
pub mod expr_to_predicate;
pub mod file_tvf_common;
pub mod file_tvf_prepare;
pub mod footer_cache;
pub mod grant_chameleon;
#[cfg(feature = "hive-external")]
pub mod hive;
#[cfg(feature = "rest")]
pub mod iceberg_metadata_tvf;
pub mod iceberg_scan;
pub mod incremental_provider;
pub mod incremental_scan;
#[cfg(feature = "rest")]
pub mod info_schema;
pub mod late_materialize;
pub mod lazy_object_store;
pub mod mount;
pub mod physical_to_predicate;
pub mod pruning_stats;
pub mod puffin_stats;
pub mod read_csv;
#[cfg(feature = "rest")]
pub mod rest_catalog;
pub mod runtime_bridge;
mod send_erased;
pub mod txn;
pub mod vended_credentials;
pub mod session_tuning;
pub mod eliminate_cross_join;
#[cfg(feature = "sql-sqlite-rs")]
pub mod sqlite_rs_catalog;
pub use session_tuning::{engine_context, engine_session_config};
#[cfg(feature = "rest")]
pub mod schema_provider;
#[cfg(feature = "rest")]
pub mod table_provider;
pub mod read_json;
mod read_json_buffer;
pub mod read_parquet;
pub mod scan_memory;
pub mod snapshot_stats;
pub mod sort_order;
#[cfg(feature = "rest")]
pub mod system_catalog;
#[cfg(feature = "rest")]
pub mod system_jdbc;
#[cfg(feature = "rest")]
pub mod system_metadata;
pub mod writable_iceberg_catalog;

pub use access_control::AccessControlClient;
#[cfg(any(feature = "glue", feature = "s3tables"))]
pub use aws_config::build_aws_config;
#[cfg(feature = "rest")]
pub use catalog_provider::SqeCatalogProvider;
pub use sqe_core::circuit_breaker;
pub use sqe_core::CircuitBreaker;
pub use footer_cache::FooterCache;
pub use iceberg_scan::coalesce_file_entries;
pub use iceberg_scan::IcebergScanExec;
pub use mount::build_catalog;
#[cfg(feature = "rest")]
pub use rest_catalog::{
    invalidate_rest_catalog_cache_all, invalidate_rest_catalog_cache_for_token,
    rest_catalog_token_fingerprint, SessionCatalog, TableMetadataCache,
};
#[cfg(feature = "rest")]
pub use system_catalog::{SystemCatalogEntry, SystemCatalogProvider};

/// Object storage for every Iceberg catalog and `FileIO` SQE builds.
///
/// iceberg-rust core no longer bundles storage: without a factory the REST,
/// HMS and SQL catalogs fail on first table access. This dispatches on the
/// path scheme (`s3://`, `file://`, `memory:`) exactly like the pre-split
/// vendor's built-in OpenDAL storage; credentials and session tokens still
/// arrive through the same `s3.*` FileIO / catalog properties.
pub fn storage_factory() -> std::sync::Arc<dyn iceberg::io::StorageFactory> {
    std::sync::Arc::new(iceberg_storage_opendal::OpenDalResolvingStorageFactory::new())
}

mod scoped_table;
#[cfg(test)]
pub(crate) mod test_fixtures;
