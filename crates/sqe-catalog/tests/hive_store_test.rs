#![cfg(feature = "hive-external")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::datasource::listing::ListingTableUrl;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits};
use sqe_catalog::hive::store::HiveManifestStore;
use sqe_core::config::{TvfCaller, TvfPolicy};

fn store_for(dir: &std::path::Path, tvf: TvfPolicy) -> HiveManifestStore {
    let url = ListingTableUrl::parse(format!("file://{}", dir.display())).expect("url");
    let os: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    HiveManifestStore::new(
        url,
        os,
        ManifestLimits::default(),
        tvf,
        TvfCaller::trusted_local(),
    )
}

/// A policy that allows local paths, which is what the embedded CLI uses.
fn permissive() -> TvfPolicy {
    TvfPolicy {
        allow_local_paths: true,
        ..TvfPolicy::default()
    }
}

fn manifest(location: &str) -> HiveManifest {
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) PARTITIONED BY (dt string) \
         STORED AS PARQUET LOCATION '{location}'"
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    HiveManifest::from_spec(&spec, "sales", "jacob", "2026-08-19T00:00:00Z")
}

#[tokio::test]
async fn manifest_path_follows_the_documented_layout() {
    let dir = tempfile::tempdir().expect("tmp");
    let s = store_for(dir.path(), permissive());
    let p = s.manifest_path("sales", "orders").to_string();
    assert!(p.ends_with("sales/orders/_sqe/table.json"), "got {p}");
}

#[tokio::test]
async fn put_then_get_round_trips() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let s = store_for(dir.path(), permissive());
    let m = manifest(&format!("file://{}", data.display()));
    s.put(&m, false).await.expect("put");
    let back = s.get("sales", "orders").await.expect("get");
    assert_eq!(back.name, "orders");
}

#[tokio::test]
async fn get_on_a_missing_table_is_not_found() {
    let dir = tempfile::tempdir().expect("tmp");
    let s = store_for(dir.path(), permissive());
    let err = s.get("sales", "nope").await.expect_err("must fail");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::NotFound(_)),
        "a missing manifest is table-not-found, got {err:?}"
    );
}

#[tokio::test]
async fn a_planted_manifest_pointing_at_a_forbidden_location_is_refused_on_load() {
    let dir = tempfile::tempdir().expect("tmp");
    // Write the manifest with a permissive policy, simulating an attacker who
    // can write to the warehouse prefix directly.
    let planted = manifest("file:///var/run/secrets/kubernetes.io/serviceaccount/token");
    store_for(dir.path(), permissive())
        .put(&planted, false)
        .await
        .expect("plant");

    // Read it back with the coordinator's fail-closed policy: local paths off.
    let guarded = store_for(dir.path(), TvfPolicy::default());
    let err = guarded
        .get("sales", "orders")
        .await
        .expect_err("must refuse");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::Forbidden(_)),
        "the location guard must run on every load, not only at create; got {err:?}"
    );
}

#[tokio::test]
async fn put_refuses_a_forbidden_location_up_front() {
    let dir = tempfile::tempdir().expect("tmp");
    let guarded = store_for(dir.path(), TvfPolicy::default());
    let err = guarded
        .put(&manifest("file:///etc/shadow"), false)
        .await
        .expect_err("must refuse");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::Forbidden(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn an_index_entry_pointing_at_a_forbidden_location_is_refused_on_load() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");

    // Plant a manifest whose table location is fine but whose index entry
    // points at a mounted secret. The attacker path is exactly the one that
    // makes the index dangerous: read planning would trust entry locations
    // as file sources without ever listing them.
    let mut planted = manifest(&format!("file://{}", data.display()));
    planted.sqe.partition_index =
        Some(sqe_catalog::hive::manifest::PartitionIndex {
            format_version: sqe_catalog::hive::manifest::PARTITION_INDEX_FORMAT_VERSION,
            entries: vec![sqe_catalog::hive::manifest::PartitionIndexEntry {
                values: std::collections::BTreeMap::from([(
                    "dt".to_string(),
                    "2026-01-02".to_string(),
                )]),
                location: "file:///var/run/secrets/kubernetes.io/serviceaccount/".to_string(),
                file_count: 1,
                total_bytes: 16,
            }],
        });
    store_for(dir.path(), permissive())
        .put(&planted, false)
        .await
        .expect("plant");

    let guarded = store_for(dir.path(), TvfPolicy::default());
    let err = guarded
        .get("sales", "orders")
        .await
        .expect_err("must refuse");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::Forbidden(_)),
        "an index entry location must pass the allowlist on every load; got {err:?}"
    );
}

#[tokio::test]
async fn put_refuses_an_index_entry_at_a_forbidden_location_up_front() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");

    let mut m = manifest(&format!("file://{}", data.display()));
    m.sqe.partition_index = Some(sqe_catalog::hive::manifest::PartitionIndex {
        format_version: sqe_catalog::hive::manifest::PARTITION_INDEX_FORMAT_VERSION,
        entries: vec![sqe_catalog::hive::manifest::PartitionIndexEntry {
            values: std::collections::BTreeMap::from([(
                "dt".to_string(),
                "2026-01-02".to_string(),
            )]),
            location: "file:///etc/".to_string(),
            file_count: 0,
            total_bytes: 0,
        }],
    });
    let err = store_for(dir.path(), TvfPolicy::default())
        .put(&m, false)
        .await
        .expect_err("must refuse");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::Forbidden(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn if_not_exists_does_not_overwrite() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let s = store_for(dir.path(), permissive());
    let mut m = manifest(&format!("file://{}", data.display()));
    s.put(&m, false).await.expect("first put");
    m.owner = Some("someone-else".into());
    let err = s.put(&m, true).await.expect_err("must not overwrite");
    assert!(
        format!("{err}").contains("orders"),
        "message should name the table: {err}"
    );
    assert_eq!(
        s.get("sales", "orders")
            .await
            .expect("get")
            .owner
            .as_deref(),
        Some("jacob")
    );
}

#[tokio::test]
async fn list_tables_finds_manifests_by_prefix() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let s = store_for(dir.path(), permissive());
    for name in ["orders", "customers"] {
        let mut m = manifest(&format!("file://{}", data.display()));
        m.name = name.to_string();
        s.put(&m, false).await.expect("put");
    }
    let mut tables = s.list_tables("sales").await.expect("list");
    tables.sort();
    assert_eq!(tables, vec!["customers".to_string(), "orders".to_string()]);
    assert_eq!(
        s.list_databases().await.expect("dbs"),
        vec!["sales".to_string()]
    );
}

/// Wraps a real `ObjectStore` and can (a) count `head()`-style `get_opts`
/// calls, to pin `list_databases`'s early exit, and (b) deny
/// `PutMode::Create`, to exercise `put`'s non-atomic head-then-put fallback
/// for stores that do not support a conditional put. `LocalFileSystem`
/// itself supports `PutMode::Create` natively (see the report), so this
/// wrapper is the only way to reach that fallback in a test.
#[derive(Debug)]
struct WrappedStore {
    inner: Arc<dyn ObjectStore>,
    head_calls: Arc<AtomicUsize>,
    deny_conditional_put: bool,
}

impl std::fmt::Display for WrappedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WrappedStore({:?})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for WrappedStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.deny_conditional_put && opts.mode == PutMode::Create {
            return Err(object_store::Error::NotSupported {
                source: "conditional put is not supported by this test double".into(),
            });
        }
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
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
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

#[tokio::test]
async fn list_databases_stops_at_the_first_table_found() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");

    let head_calls = Arc::new(AtomicUsize::new(0));
    let wrapped: Arc<dyn ObjectStore> = Arc::new(WrappedStore {
        inner: Arc::new(LocalFileSystem::new()),
        head_calls: head_calls.clone(),
        deny_conditional_put: false,
    });
    let url = ListingTableUrl::parse(format!("file://{}", dir.path().display())).expect("url");
    let s = HiveManifestStore::new(
        url,
        wrapped,
        ManifestLimits::default(),
        permissive(),
        TvfCaller::trusted_local(),
    );

    // Three tables in the same database, all with manifests: whichever one
    // list_with_delimiter happens to visit first already satisfies
    // has_any_table, so the early exit must land on exactly one head() call
    // no matter the listing order.
    for name in ["orders", "customers", "accounts"] {
        let mut m = manifest(&format!("file://{}", data.display()));
        m.name = name.to_string();
        s.put(&m, false).await.expect("put");
    }
    // `put(.., false)` is an unconditional put and issues no head() calls;
    // this is a sanity check, not a reset, so the count below is entirely
    // attributable to list_databases.
    assert_eq!(head_calls.load(Ordering::SeqCst), 0);

    let dbs = s.list_databases().await.expect("dbs");
    assert_eq!(dbs, vec!["sales".to_string()]);
    assert_eq!(
        head_calls.load(Ordering::SeqCst),
        1,
        "list_databases must stop at the first table found, not head every \
         table in the database"
    );
}

#[tokio::test]
async fn if_not_exists_falls_back_when_the_store_has_no_conditional_put() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");

    let wrapped: Arc<dyn ObjectStore> = Arc::new(WrappedStore {
        inner: Arc::new(LocalFileSystem::new()),
        head_calls: Arc::new(AtomicUsize::new(0)),
        deny_conditional_put: true,
    });
    let url = ListingTableUrl::parse(format!("file://{}", dir.path().display())).expect("url");
    let s = HiveManifestStore::new(
        url,
        wrapped,
        ManifestLimits::default(),
        permissive(),
        TvfCaller::trusted_local(),
    );

    let m = manifest(&format!("file://{}", data.display()));
    s.put(&m, true)
        .await
        .expect("first put must succeed via the non-atomic fallback");

    let mut m2 = m.clone();
    m2.owner = Some("someone-else".into());
    let err = s
        .put(&m2, true)
        .await
        .expect_err("the fallback must not clobber an existing manifest either");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::Validation(_)),
        "got {err:?}"
    );
    assert_eq!(
        s.get("sales", "orders")
            .await
            .expect("get")
            .owner
            .as_deref(),
        Some("jacob"),
        "the fallback's head-then-put must have refused the second write"
    );
}
