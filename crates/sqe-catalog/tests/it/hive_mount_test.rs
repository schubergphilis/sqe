#![cfg(feature = "hive-external")]

//! `build_hive_root` / `session_hive_catalog` (Task 8).
//!
//! Deviations from the original brief, agreed with the task owner:
//! - `TvfCaller::Remote` does not exist (`TvfCaller` is a struct with
//!   `for_user`/`trusted_local`/`Default`); `TvfCaller::default()` is the
//!   untrusted-remote-like substitute.
//! - `sqe_core::InMemorySecretStore` does not exist; the concrete
//!   `SecretStore` (`SecretStore::new()`) is the in-memory store, and
//!   `build_hive_root` does not take a secrets parameter at all (a Hive
//!   root's manifest store needs no credential until a table is actually
//!   read), so the import is dropped rather than substituted.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use sqe_core::config::StorageConfig;
use sqe_core::SecretStore;

#[tokio::test]
async fn attach_rejects_a_root_location_that_fails_the_allowlist() {
    // Default storage config denies local paths, matching the coordinator.
    // `HiveManifestStore` has no `Debug` impl (it holds an `Arc<dyn
    // ObjectStore>`), so `expect_err` (which requires `T: Debug`) doesn't
    // apply here; match instead.
    let result = sqe_catalog::mount::build_hive_root(
        "file:///etc",
        &BTreeMap::new(),
        &StorageConfig::default(),
        sqe_catalog::hive::manifest::ManifestLimits::default(),
        sqe_core::config::TvfCaller::default(),
    )
    .await;
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("must refuse"),
    };
    assert!(
        err.to_lowercase().contains("etc") || err.to_lowercase().contains("local"),
        "got {err}"
    );
}

#[tokio::test]
async fn attach_accepts_an_s3_root_without_touching_the_network() {
    let storage = StorageConfig {
        // Port 1 on loopback: nothing listens there in any environment
        // (privileged, unbound), so any TCP connect attempt fails instantly
        // with connection-refused. If `build_hive_root` performed a LIST or
        // HEAD against the store it built, this call would return `Err`
        // (or hang) instead of the `Ok` asserted below -- that is the
        // negative this test is actually pinning, not just "returned Ok".
        s3_endpoint: "http://127.0.0.1:1".to_string(),
        s3_allow_http: true,
        s3_region: "us-east-1".to_string(),
        tvf: sqe_core::config::TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://bucket/".to_string()],
            ..Default::default()
        },
        ..StorageConfig::default()
    };
    // ATTACH must not list or HEAD: a root is registered lazily, so attaching
    // an empty or not-yet-created prefix has to succeed.
    sqe_catalog::mount::build_hive_root(
        "s3://bucket/wh/",
        &BTreeMap::new(),
        &storage,
        sqe_catalog::hive::manifest::ManifestLimits::default(),
        sqe_core::config::TvfCaller::default(),
    )
    .await
    .expect("attach should not require the prefix to exist, and must not touch the network");
}

#[test]
fn parse_and_classify_accepts_the_hive_external_attach_form() {
    let kind =
        sqe_sql::parse_and_classify("ATTACH 's3://bucket/wh/' AS lake (TYPE hive_external)")
            .expect("classify");
    match kind {
        sqe_sql::StatementKind::Attach(a) => {
            assert_eq!(a.name, "lake");
            assert_eq!(a.kind, sqe_sql::CatalogKind::HiveExternal);
            assert_eq!(a.location, "s3://bucket/wh/");
        }
        other => panic!("expected Attach, got {other:?}"),
    }
}

/// A local root with nothing under it yet must still attach: `build_hive_root`
/// never lists or HEADs, so an empty (or not-yet-created) prefix succeeds.
#[tokio::test]
async fn attach_accepts_an_empty_local_root() {
    let dir = tempfile::tempdir().expect("tmp");
    let storage = StorageConfig {
        tvf: sqe_core::config::TvfPolicy {
            allow_local_paths: true,
            ..Default::default()
        },
        ..StorageConfig::default()
    };
    let location = format!("file://{}/does-not-exist-yet", dir.path().display());
    sqe_catalog::mount::build_hive_root(
        &location,
        &BTreeMap::new(),
        &storage,
        sqe_catalog::hive::manifest::ManifestLimits::default(),
        sqe_core::config::TvfCaller::trusted_local(),
    )
    .await
    .expect("attaching an empty prefix must succeed");
}

/// `session_hive_catalog` builds a usable `CatalogProvider`: schema/table
/// listing against a manifest actually written by `HiveManifestStore::put`
/// resolves, proving the two functions compose end to end.
#[tokio::test]
async fn session_hive_catalog_resolves_a_table_written_through_the_shared_root() {
    use datafusion::catalog::CatalogProvider;

    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let storage = StorageConfig {
        tvf: sqe_core::config::TvfPolicy {
            allow_local_paths: true,
            ..Default::default()
        },
        ..StorageConfig::default()
    };
    let caller = sqe_core::config::TvfCaller::trusted_local();
    let root = sqe_catalog::mount::build_hive_root(
        &format!("file://{}", dir.path().display()),
        &BTreeMap::new(),
        &storage,
        sqe_catalog::hive::manifest::ManifestLimits::default(),
        caller.clone(),
    )
    .await
    .expect("build root");

    // Plant a manifest directly through the shared store, mirroring what a
    // real `CREATE EXTERNAL TABLE` would leave behind.
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) STORED AS PARQUET LOCATION 'file://{}'",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let manifest = sqe_catalog::hive::manifest::HiveManifest::from_spec(
        &spec,
        "sales",
        "jacob",
        "2026-08-19T00:00:00Z",
    );
    root.put(&manifest, false).await.expect("put manifest");

    let runtime = RuntimeEnvBuilder::new().build_arc().expect("runtime");
    let manifest_cache = Arc::new(sqe_catalog::hive::provider::HiveMetadataCache::new());
    let secrets = SecretStore::new();
    let provider = sqe_catalog::mount::session_hive_catalog(
        "lake",
        root,
        &storage,
        &secrets,
        caller,
        runtime,
        manifest_cache,
        Duration::from_secs(60),
    );

    // The sync hooks serve snapshots that only the async refresh fills
    // (review 806): before it, the catalog reports no databases at all.
    assert!(provider.schema_names().is_empty());
    provider.refresh_all().await;
    assert_eq!(provider.schema_names(), vec!["sales".to_string()]);
    let schema = provider.schema("sales").expect("schema");
    let tables = schema.table_names();
    assert_eq!(tables, vec!["orders".to_string()]);
}

/// `build_hive_root`'s `limits` parameter must actually reach the store it
/// builds, not just exist on the signature: this proves the wiring, not the
/// plumbing. A `ManifestLimits` struct arriving intact would pass a test
/// that only inspected the value handed to `build_hive_root`; this instead
/// builds a root with a deliberately tiny `max_bytes`, writes a manifest
/// that exceeds it (writing is not size-checked; see `HiveManifestStore::put`),
/// and asserts that *reading it back* through the very store `build_hive_root`
/// returned fails with the size error. If a future change dropped `limits`
/// on the way through and hardcoded `ManifestLimits::default()` again (as an
/// earlier draft of this function did), this test would still pass with a
/// small manifest but must fail here because the manifest is deliberately
/// bigger than the tiny cap and default's 1MB cap would let it through.
#[tokio::test]
async fn build_hive_root_wires_max_bytes_into_the_store_it_returns() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let storage = StorageConfig {
        tvf: sqe_core::config::TvfPolicy {
            allow_local_paths: true,
            ..Default::default()
        },
        ..StorageConfig::default()
    };
    let tiny_limits = sqe_catalog::hive::manifest::ManifestLimits {
        max_bytes: 32,
        ..Default::default()
    };
    let root = sqe_catalog::mount::build_hive_root(
        &format!("file://{}", dir.path().display()),
        &BTreeMap::new(),
        &storage,
        tiny_limits,
        sqe_core::config::TvfCaller::trusted_local(),
    )
    .await
    .expect("build root");

    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) STORED AS PARQUET LOCATION 'file://{}'",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let manifest = sqe_catalog::hive::manifest::HiveManifest::from_spec(
        &spec,
        "sales",
        "jacob",
        "2026-08-19T00:00:00Z",
    );
    // `put` does not size-check (only `get`/`parse` does), so this succeeds
    // even though the serialized manifest is well over 32 bytes.
    root.put(&manifest, false).await.expect("put manifest");

    let err = root
        .get("sales", "orders")
        .await
        .expect_err("a manifest over the 32-byte cap must be refused on read");
    let msg = format!("{err}");
    assert!(
        msg.contains("byte cap") || msg.contains("exceeds"),
        "expected a size-cap error, got: {msg}"
    );
}

/// Counts `get_opts(head=true)` and `list` calls so a "no network touched"
/// assertion has teeth even against a plausible future regression that adds
/// an existence probe. Prior art: `crates/sqe-catalog/tests/hive_store_test.rs`.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    head_calls: Arc<AtomicUsize>,
    list_calls: Arc<AtomicUsize>,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({:?})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head {
            self.head_calls.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// `build_hive_root` is a public async fn with no store-injection seam, so
/// this test reaches `HiveManifestStore::new` directly (the same
/// constructor `build_hive_root` calls) with a counting wrapper, to prove
/// that constructing the shared root by itself performs zero list/head
/// calls -- the property `build_hive_root` must preserve internally.
#[tokio::test]
async fn constructing_the_manifest_store_alone_touches_neither_list_nor_head() {
    let dir = tempfile::tempdir().expect("tmp");
    let head_calls = Arc::new(AtomicUsize::new(0));
    let list_calls = Arc::new(AtomicUsize::new(0));
    let wrapped: Arc<dyn ObjectStore> = Arc::new(CountingStore {
        inner: Arc::new(LocalFileSystem::new()),
        head_calls: head_calls.clone(),
        list_calls: list_calls.clone(),
    });
    let url = datafusion::datasource::listing::ListingTableUrl::parse(format!(
        "file://{}",
        dir.path().display()
    ))
    .expect("url");
    let _store = sqe_catalog::hive::store::HiveManifestStore::new(
        url,
        wrapped,
        sqe_catalog::hive::manifest::ManifestLimits::default(),
        sqe_core::config::TvfPolicy::default(),
        sqe_core::config::TvfCaller::trusted_local(),
    );
    assert_eq!(head_calls.load(Ordering::SeqCst), 0);
    assert_eq!(list_calls.load(Ordering::SeqCst), 0);
}

/// A manifest naming a `SECRET` resolves through the `SecretStore` when
/// building the per-session data-store factory, and a missing secret errors
/// naming the secret -- the only credential indirection cycle 1 has (engine
/// credentials otherwise; see `mount::EngineHiveDataStoreFactory`).
#[tokio::test]
async fn a_missing_manifest_secret_errors_naming_the_secret() {
    use datafusion::catalog::CatalogProvider;

    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let storage = StorageConfig {
        tvf: sqe_core::config::TvfPolicy {
            allow_local_paths: true,
            ..Default::default()
        },
        ..StorageConfig::default()
    };
    let caller = sqe_core::config::TvfCaller::trusted_local();
    let root = sqe_catalog::mount::build_hive_root(
        &format!("file://{}", dir.path().display()),
        &BTreeMap::new(),
        &storage,
        sqe_catalog::hive::manifest::ManifestLimits::default(),
        caller.clone(),
    )
    .await
    .expect("build root");

    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) STORED AS PARQUET LOCATION 'file://{}' \
         TBLPROPERTIES ('sqe.secret' = 'missing-secret')",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let manifest = sqe_catalog::hive::manifest::HiveManifest::from_spec(
        &spec,
        "sales",
        "jacob",
        "2026-08-19T00:00:00Z",
    );
    assert_eq!(manifest.secret(), Some("missing-secret"));
    root.put(&manifest, false).await.expect("put manifest");

    let runtime = RuntimeEnvBuilder::new().build_arc().expect("runtime");
    let manifest_cache = Arc::new(sqe_catalog::hive::provider::HiveMetadataCache::new());
    let secrets = SecretStore::new();
    let provider = sqe_catalog::mount::session_hive_catalog(
        "lake",
        root,
        &storage,
        &secrets,
        caller,
        runtime,
        manifest_cache,
        Duration::from_secs(60),
    );
    let schema = provider.schema("sales").expect("schema");
    let err = schema
        .table("orders")
        .await
        .expect_err("a table naming an unresolvable secret must not resolve as Ok(None)");
    let msg = format!("{err}");
    assert!(
        msg.contains("missing-secret"),
        "error should name the missing secret: {msg}"
    );
}
