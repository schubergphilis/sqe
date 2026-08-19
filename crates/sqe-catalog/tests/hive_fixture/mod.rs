//! Shared fixture: a warehouse root on local disk with a partitioned CSV table.
//!
//! Kept in one place because four test files build the same tree, and a
//! divergent fixture is how a passing test starts asserting the wrong shape.

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;

use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits};
use sqe_catalog::hive::provider::HiveCatalogProvider;
use sqe_catalog::hive::store::HiveManifestStore;
use sqe_core::config::{TvfCaller, TvfPolicy};

pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub root: String,
}

/// A store on the local filesystem with local paths allowed, which is the
/// embedded CLI's trust model.
fn local_store(root: &str) -> HiveManifestStore {
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

/// Write the manifest for a CSV table with a single `dt` partition key and a
/// header row. `second_column` names the second declared column; the CSV
/// header written by [`write_partition`] always literally reads `id,amount`
/// regardless, so a caller can pass a name that disagrees with the header to
/// exercise ordinal (not name) binding.
///
/// `STORED AS CSV` is not a Hive/sqlparser-rs keyword (the `FileFormat` enum
/// has exactly seven variants: TEXTFILE, SEQUENCEFILE, ORC, PARQUET, AVRO,
/// RCFILE, JSONFILE), so this declares `STORED AS TEXTFILE` and spells out
/// the comma delimiter explicitly via `ROW FORMAT DELIMITED`. Without an
/// explicit delimiter, `serde_map::resolve_format` defaults to Hive's own
/// ctrl-A field delimiter, not comma, which would silently read every row of
/// this fixture's data as a single column.
async fn put_csv_manifest(root: &str, data: &str, second_column: &str) {
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT, {second_column} DOUBLE) \
         PARTITIONED BY (dt string) \
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
         STORED AS TEXTFILE LOCATION '{data}' \
         TBLPROPERTIES ('skip.header.line.count' = '1')"
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let m = HiveManifest::from_spec(&spec, "sales", "test", "2026-08-19T00:00:00Z");
    local_store(root)
        .put(&m, false)
        .await
        .expect("put manifest");
}

fn write_partition(data: &std::path::Path, dt: &str, rows: &str) {
    let dir = data.join(format!("dt={dt}"));
    std::fs::create_dir_all(&dir).expect("mkdir partition");
    std::fs::write(dir.join("part-0.csv"), format!("id,amount\n{rows}")).expect("write csv");
}

/// `dt=2026-01-01` with two rows and `dt=2026-01-02` with one, plus the
/// manifest under the warehouse root.
///
/// The data directory is given to `LOCATION` WITHOUT a trailing slash on
/// purpose, matching how a hand-written DDL statement would name it: see
/// [`HiveSchemaProvider::table`][provider] for why the provider must
/// normalize this before handing it to `ListingTableUrl`.
///
/// [provider]: sqe_catalog::hive::provider::HiveSchemaProvider
pub async fn partitioned_csv_root() -> Fixture {
    let dir = tempfile::tempdir().expect("tmp");
    let data_path = dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("mkdir data");
    write_partition(&data_path, "2026-01-01", "1,10.5\n2,20.0\n");
    write_partition(&data_path, "2026-01-02", "3,30.5\n");

    let root = format!("file://{}/wh", dir.path().display());
    let data = format!("file://{}", data_path.display());
    put_csv_manifest(&root, &data, "amount").await;
    Fixture { dir, root }
}

/// Same data as [`partitioned_csv_root`], but the manifest declares the
/// second column as `total` while the CSV header on disk still literally
/// reads `id,amount`. A binding that matched the declared name against the
/// header text would find no `total` field; ordinal binding reads position 2
/// regardless of what its header cell says.
pub async fn partitioned_csv_root_with_renamed_column() -> Fixture {
    let dir = tempfile::tempdir().expect("tmp");
    let data_path = dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("mkdir data");
    write_partition(&data_path, "2026-01-01", "1,10.5\n2,20.0\n");
    write_partition(&data_path, "2026-01-02", "3,30.5\n");

    let root = format!("file://{}/wh", dir.path().display());
    let data = format!("file://{}", data_path.display());
    put_csv_manifest(&root, &data, "total").await;
    Fixture { dir, root }
}

/// Same shape plus a `__HIVE_DEFAULT_PARTITION__` directory, which Hive writes
/// for a NULL partition value.
pub async fn root_with_default_partition() -> Fixture {
    let f = partitioned_csv_root().await;
    let data_path = f.dir.path().join("data");
    write_partition(&data_path, "__HIVE_DEFAULT_PARTITION__", "4,40.5\n");
    f
}

/// Register the root as catalog `lake` and return the provider so a test can
/// inspect its manifest cache.
pub async fn register_hive_root(ctx: &SessionContext, f: &Fixture) -> Arc<HiveCatalogProvider> {
    #[derive(Debug)]
    struct LocalFactory;
    impl sqe_catalog::hive::provider::HiveDataStoreFactory for LocalFactory {
        fn store_for(
            &self,
            _location: &str,
            _secret: Option<&str>,
        ) -> Result<Arc<dyn object_store::ObjectStore>, sqe_catalog::hive::HiveError> {
            Ok(Arc::new(object_store::local::LocalFileSystem::new()))
        }
    }

    // `ctx.runtime_env()`, not a fresh `RuntimeEnv`: `ListingTable` resolves
    // its object store from the session registry at scan time, so the store
    // registered in `HiveSchemaProvider::table` must live on the same
    // runtime the query executes against.
    let provider = Arc::new(HiveCatalogProvider::new(
        "lake".to_string(),
        Arc::new(local_store(&f.root)),
        Arc::new(LocalFactory),
        ctx.runtime_env(),
        Arc::new(sqe_catalog::hive::provider::HiveMetadataCache::new()),
        Duration::from_secs(60),
    ));
    ctx.register_catalog("lake", provider.clone());
    provider
}
