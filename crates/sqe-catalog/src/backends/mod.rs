//! SQE-native catalog backends.
//!
//! Per-backend dispatch for Glue, HMS, JDBC, and AWS S3 Tables now goes
//! through the vendored `iceberg-catalog-loader` crate at the call site
//! in `rest_catalog.rs::for_session_other_backend`. Each upstream
//! `CatalogBuilder` consumes a uniform `(catalog_type, props)` shape
//! that made the historical SQE-side wrapper modules redundant.
//!
//! The one outlier is **Hadoop** (storage-only, walks an `object_store`
//! warehouse path). It has no upstream catalog loader equivalent because
//! it is not a real Iceberg catalog: there is no metadata service to
//! talk to, just a filesystem prefix to scan. `hadoop.rs` stays as the
//! SQE-native implementation.
//!
//! Old SQE wrappers `glue.rs`, `hms.rs`, `sql.rs` were removed in the
//! `feat/iceberg-loader-s3tables` change. See
//! `vendor/iceberg-rust/README.md` and
//! `docs/site/book/src/getting-started/catalogs.md` for the
//! supported config keys per backend.

#[cfg(feature = "hadoop")]
pub mod hadoop;
