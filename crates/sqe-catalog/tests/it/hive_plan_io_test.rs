#![cfg(feature = "hive-external")]
//! Planning a Hive Parquet query must not read data files (#807).
//!
//! DataFusion's `ListingTable::scan` infers per-file statistics while planning
//! when the session's `datafusion.execution.collect_statistics` is on (the
//! default): one footer read per listed file, before a single row is wanted.
//! `HiveTableProvider` supplies its own size-derived statistics, so that work
//! buys nothing. These tests plan, never execute, against a store that counts
//! every read of a `.parquet` object.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits};
use sqe_catalog::hive::provider::{HiveCatalogProvider, HiveDataStoreFactory, HiveMetadataCache};
use sqe_catalog::hive::store::HiveManifestStore;
use sqe_core::config::{TvfCaller, TvfPolicy};

/// Counts reads of data files (`*.parquet`) on the way to the local
/// filesystem. In object_store 0.13 `get`, `get_range` and `head` all route
/// through `get_opts`, so `get_opts` and `get_ranges` see every read.
#[derive(Debug)]
struct DataReadCounter {
    inner: Arc<dyn ObjectStore>,
    data_reads: AtomicUsize,
}

impl DataReadCounter {
    fn count(&self, location: &ObjectPath) {
        if location.as_ref().ends_with(".parquet") {
            self.data_reads.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn reads(&self) -> usize {
        self.data_reads.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for DataReadCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataReadCounter({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for DataReadCounter {
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
        self.count(location);
        self.inner.get_opts(location, options).await
    }
    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.count(location);
        self.inner.get_ranges(location, ranges).await
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

#[derive(Debug)]
struct CountingFactory(Arc<DataReadCounter>);

impl HiveDataStoreFactory for CountingFactory {
    fn store_for(
        &self,
        _location: &str,
        _secret: Option<&str>,
    ) -> Result<Arc<dyn ObjectStore>, sqe_catalog::hive::HiveError> {
        Ok(self.0.clone())
    }
}

fn local_manifest_store(root: &str) -> HiveManifestStore {
    HiveManifestStore::new(
        datafusion::datasource::listing::ListingTableUrl::parse(root).expect("root url"),
        Arc::new(object_store::local::LocalFileSystem::new()),
        ManifestLimits::default(),
        TvfPolicy {
            allow_local_paths: true,
            ..TvfPolicy::default()
        },
        TvfCaller::trusted_local(),
    )
}

/// `partitions` x `files_per_partition` one-row Parquet files under
/// `dt=d{n}/`, and a `STORED AS PARQUET` manifest declaring `id BIGINT`.
async fn parquet_table(
    partitions: usize,
    files_per_partition: usize,
) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tmp");
    let data_path = dir.path().join("data");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let mut id = 0i64;
    for p in 0..partitions {
        let part = data_path.join(format!("dt=d{p}"));
        std::fs::create_dir_all(&part).expect("mkdir partition");
        for f in 0..files_per_partition {
            id += 1;
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![id]))])
                    .expect("batch");
            let file =
                std::fs::File::create(part.join(format!("part-{f}.parquet"))).expect("create");
            let mut w =
                parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None).expect("writer");
            w.write(&batch).expect("write");
            w.close().expect("close");
        }
    }

    let root = format!("file://{}/wh", dir.path().display());
    let data = format!("file://{}", data_path.display());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) PARTITIONED BY (dt string) \
         STORED AS PARQUET LOCATION '{data}'"
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let m = HiveManifest::from_spec(&spec, "sales", "test", "2026-10-08T00:00:00Z");
    local_manifest_store(&root)
        .put(&m, false)
        .await
        .expect("put manifest");
    (dir, root)
}

async fn register(ctx: &SessionContext, root: &str) -> Arc<DataReadCounter> {
    let counter = Arc::new(DataReadCounter {
        inner: Arc::new(object_store::local::LocalFileSystem::new()),
        data_reads: AtomicUsize::new(0),
    });
    // `ListingTable` resolves its store from the session registry. Hive
    // registers the factory's store there, but the counter goes in directly
    // as well, so it sees the reads whichever path put the store there.
    ctx.register_object_store(
        &url::Url::parse("file:///").expect("file url"),
        counter.clone(),
    );
    let provider = Arc::new(HiveCatalogProvider::new(
        "lake".to_string(),
        Arc::new(local_manifest_store(root)),
        Arc::new(CountingFactory(counter.clone())),
        ctx.runtime_env(),
        Arc::new(HiveMetadataCache::new()),
        Duration::from_secs(60),
    ));
    provider.refresh_all().await;
    ctx.register_catalog("lake", provider);
    counter
}

#[tokio::test]
async fn planning_a_hive_parquet_scan_reads_no_data_files() {
    let (dir, root) = parquet_table(2, 5).await;
    let ctx = SessionContext::new();
    assert!(
        ctx.state().config().collect_statistics(),
        "the session default this test guards against must still be on"
    );
    let counter = register(&ctx, &root).await;

    let plan = ctx
        .sql("SELECT id FROM lake.sales.orders WHERE id > 0")
        .await
        .expect("logical plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    assert_eq!(counter.reads(), 0, "planning read data files");

    // Executing does read them, which proves the counter sees this store.
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("execute");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 10);
    assert!(
        counter.reads() > 0,
        "execution must read through the counter"
    );
    drop(dir);
}

#[tokio::test]
async fn planning_a_partition_pruned_hive_query_reads_no_data_files() {
    let (dir, root) = parquet_table(3, 4).await;
    let ctx = SessionContext::new();
    let counter = register(&ctx, &root).await;

    ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE dt = 'd1'")
        .await
        .expect("logical plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    assert_eq!(counter.reads(), 0, "planning read data files");
    drop(dir);
}

/// The acceptance number: plan time and data-file reads over 1,000 files.
/// Run with `--ignored --nocapture` to see it.
#[tokio::test]
#[ignore = "timing report, not a gate"]
async fn plan_time_over_a_thousand_files() {
    let (dir, root) = parquet_table(10, 100).await;
    let ctx = SessionContext::new();
    let counter = register(&ctx, &root).await;

    let started = Instant::now();
    ctx.sql("SELECT id FROM lake.sales.orders WHERE id > 0")
        .await
        .expect("logical plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    eprintln!(
        "1000-file Hive plan: {:?}, data-file reads during planning: {}",
        started.elapsed(),
        counter.reads()
    );
    drop(dir);
}
