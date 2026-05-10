//! Re-export shim for the writable iceberg catalog provider.
//!
//! `WritableIcebergCatalog` lifted into `sqe-catalog` so the runtime
//! ATTACH path (Phase F) can reach it without `sqe-coordinator`
//! depending on `sqe-cli`. The CLI keeps the same import path as
//! before via this single re-export so all the in-tree call sites
//! continue to compile unchanged.

pub use sqe_catalog::writable_iceberg_catalog::WritableIcebergCatalog;
