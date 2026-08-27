// Shared by two test binaries (`hive_differential_test` and
// `hive_write_test`), and each compiles the whole module while using only the
// part it needs. So every helper the OTHER binary uses reads as dead code here.
// Splitting the module to satisfy the lint would mean two fixtures for one
// warehouse shape, which is how a passing test starts asserting the wrong thing.
#![allow(dead_code)]

//! Cycle 5, task 4.1: the differential harness.
//!
//! Two execution paths must return the same rows for the same Hive scan:
//!
//! - coordinator-local, which is `HiveScanExec` wrapping the provider's plan;
//! - worker-distributed, which is `tasks_for_hive_scan` plus
//!   `execute_scan_streaming_with_store`.
//!
//! The acceptance criterion in design.md is differential rather than golden on
//! purpose: a golden file encodes whichever path produced it, including its
//! bugs, and the failure this cycle risks is not a crash but the two paths
//! disagreeing.
//!
//! ## Why this runs in-process rather than against a cluster
//!
//! `DistributedScanExec` ships tasks over Flight and collects results, and it
//! is not the part that can silently return different rows: it is
//! format-blind and schema-driven. What CAN differ is what a task contains and
//! how a worker opens it, and both of those are reachable in-process. So the
//! harness calls the same task builder production calls, and
//! `execute_scan_streaming_with_store` -- the function a Flight `DoGet`
//! eventually reaches.
//!
//! Not full parity, and the gap is worth naming. `DoGet` goes through
//! `execute_scan_streaming`, which BUILDS its object store from the task's S3
//! fields; this harness passes `HiveS3Access::default()` and injects a
//! `LocalFileSystem` instead. So store construction and credential refresh are
//! not covered here. What is covered is task content, reader selection,
//! partition materialization and schema -- the parts that can return wrong rows
//! rather than an error.
//!
//! ## Why the task builder is called, not hand-built
//!
//! `try_distribute_hive` cannot run here: it needs a `Session`, a worker
//! registry and healthy worker URLs. Hand-building a `HiveScanQuery` instead
//! would test a configuration production never produces -- in particular
//! `projected_columns` is ALWAYS the full schema and `predicate_proto` is
//! always `None` on the real path. So the task-building half was extracted
//! into `tasks_for_hive_scan` and both callers share it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::ObjectStore;

use sqe_catalog::hive::manifest::{
    HiveManifest, ManifestLimits, PartitionIndex, PartitionIndexEntry,
    PARTITION_INDEX_FORMAT_VERSION,
};
use sqe_catalog::hive::provider::{HiveCatalogProvider, HiveMetadataCache};
use sqe_catalog::hive::scan_exec::HiveScanExec;
use sqe_catalog::hive::store::HiveManifestStore;
use sqe_core::config::{TvfCaller, TvfPolicy};

pub struct Fixture {
    /// Held, not read: dropping the `TempDir` deletes the warehouse, so the
    /// fixture has to outlive every query run against it.
    #[allow(dead_code)]
    pub dir: tempfile::TempDir,
    pub root: String,
    pub data: String,
    /// Partition directory names in the order they were written.
    pub partitions: Vec<String>,
    /// The partition key's name and declared Hive type.
    pub key: (String, String),
}

/// A store on the local filesystem with local paths allowed, mirroring the
/// embedded CLI's trust model. Same shape as sqe-catalog's own hive fixture.
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

fn file_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
    ]))
}

/// Write one partition directory holding a single parquet file.
fn write_parquet_partition(data: &std::path::Path, part_dir: &str, ids: &[i64], amounts: &[f64]) {
    let dir = data.join(part_dir);
    std::fs::create_dir_all(&dir).expect("mkdir partition");
    let schema = file_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Float64Array::from(amounts.to_vec())),
        ],
    )
    .expect("batch");
    let f = std::fs::File::create(dir.join("part-0.parquet")).expect("create parquet");
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
}

/// The DDL clauses that select a file format, per format.
///
/// Spelled out per format rather than derived, because each one is a different
/// Hive spelling and getting it wrong resolves to a DIFFERENT format silently:
/// `STORED AS TEXTFILE` with no explicit delimiter resolves to Hive's ctrl-A
/// field delimiter, not comma, and would read every CSV row as one column.
fn format_clauses(format: &str) -> &'static str {
    match format {
        "parquet" => "STORED AS PARQUET",
        // `STORED AS CSV` is not a sqlparser-rs / Hive keyword; the comma has
        // to be stated or the default is ctrl-A.
        "csv" => "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE",
        // Line-delimited JSON, declared by SerDe class. This is what Athena's
        // JSON tables carry.
        "json" => "ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' STORED AS TEXTFILE",
        other => panic!("unknown fixture format `{other}`"),
    }
}

async fn put_manifest(root: &str, data: &str, key: &(String, String), format: &str) {
    let (key_name, key_type) = key;
    let clauses = format_clauses(format);
    // `skip.header.line.count` only for CSV: the fixture writes a header row,
    // and the differential test's job is to prove the worker skips the same row
    // the coordinator-local reader skips.
    let props = if format == "csv" {
        " TBLPROPERTIES ('skip.header.line.count' = '1')"
    } else {
        ""
    };
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT, amount DOUBLE) \
         PARTITIONED BY ({key_name} {key_type}) \
         {clauses} LOCATION '{data}'{props}"
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse ddl");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external table");
    let m = HiveManifest::from_spec(&spec, "sales", "test", "2026-08-27T00:00:00Z");
    local_store(root).put(&m, false).await.expect("put manifest");
}

/// Plant a `sqe.partition_index` covering every written partition.
///
/// Required, not incidental: `HiveTableProvider` only emits `HiveScanExec` when
/// `uses_index_scan()` holds, and `servability` refuses a scan with zero
/// entries. An unindexed Hive table never distributes, so an unindexed fixture
/// would compare the coordinator-local path against itself.
async fn plant_index(f: &Fixture) {
    let store = local_store(&f.root);
    let mut m = store.get("sales", "orders").await.expect("get manifest");
    let entries = f
        .partitions
        .iter()
        .map(|p| {
            let value = p.split_once('=').expect("k=v partition dir").1.to_string();
            PartitionIndexEntry {
                values: BTreeMap::from([(f.key.0.clone(), value)]),
                location: format!("{}/{p}/", f.data),
                file_count: 1,
                total_bytes: 0,
            }
        })
        .collect();
    m.sqe.partition_index = Some(PartitionIndex {
        format_version: PARTITION_INDEX_FORMAT_VERSION,
        entries,
    });
    store.put(&m, false).await.expect("put index");
}

/// Three string partitions: two rows, one row, two rows.
pub async fn multi_partition_parquet() -> Fixture {
    multi_partition("parquet").await
}

/// An Int64 partition key plus the `__HIVE_DEFAULT_PARTITION__` sentinel.
///
/// Both are silent-failure shapes: a partition key defaulted to Utf8 still
/// returns rows, and a sentinel read as its literal string still returns rows.
pub async fn typed_key_with_default_partition() -> Fixture {
    let dir = tempfile::tempdir().expect("tmp");
    let data_path = dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("mkdir data");
    write_parquet_partition(&data_path, "year=2024", &[1, 2], &[10.5, 20.0]);
    write_parquet_partition(&data_path, "year=2025", &[3], &[30.5]);
    write_parquet_partition(
        &data_path,
        "year=__HIVE_DEFAULT_PARTITION__",
        &[4, 5],
        &[40.5, 45.0],
    );

    let f = Fixture {
        root: format!("file://{}/wh", dir.path().display()),
        data: format!("file://{}", data_path.display()),
        partitions: vec![
            "year=2024".into(),
            "year=2025".into(),
            "year=__HIVE_DEFAULT_PARTITION__".into(),
        ],
        key: ("year".into(), "int".into()),
        dir,
    };
    put_manifest(&f.root, &f.data, &f.key, "parquet").await;
    plant_index(&f).await;
    f
}

#[derive(Debug)]
struct LocalFactory;

impl sqe_catalog::hive::provider::HiveDataStoreFactory for LocalFactory {
    fn store_for(
        &self,
        _location: &str,
        _secret: Option<&str>,
    ) -> Result<Arc<dyn ObjectStore>, sqe_catalog::hive::HiveError> {
        Ok(Arc::new(object_store::local::LocalFileSystem::new()))
    }
}

pub async fn session(f: &Fixture) -> SessionContext {
    let ctx = SessionContext::new();
    let provider = Arc::new(HiveCatalogProvider::new(
        "lake".to_string(),
        Arc::new(local_store(&f.root)),
        Arc::new(LocalFactory),
        ctx.runtime_env(),
        Arc::new(HiveMetadataCache::new()),
        Duration::from_secs(60),
    ));
    ctx.register_catalog("lake", provider);
    ctx
}

/// Find the one `HiveScanExec` in a plan tree.
///
/// By downcast, never by matching the plan's shape: the coordinator finds it
/// the same way, and a shape match would silently stop finding it the first
/// time a rule reshaped the tree (task 3c.4).
pub fn find_hive_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<HiveScanExec>().is_some() {
        return Some(Arc::clone(plan));
    }
    plan.children()
        .into_iter()
        .find_map(find_hive_scan)
}

/// The physical plan for `sql`, and the `HiveScanExec` inside it.
pub async fn plan_and_scan(
    ctx: &SessionContext,
    sql: &str,
) -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
    let plan = ctx
        .sql(sql)
        .await
        .expect("plan sql")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let scan = find_hive_scan(&plan).unwrap_or_else(|| {
        panic!(
            "no HiveScanExec in the plan for `{sql}`; without it nothing \
             distributes and this comparison would be the local path against \
             itself:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
        )
    });
    (plan, scan)
}

/// Execute a plan node coordinator-local, concatenating every partition.
pub async fn local_rows(
    node: &Arc<dyn ExecutionPlan>,
    ctx: &SessionContext,
) -> (SchemaRef, Vec<RecordBatch>) {
    let task_ctx = ctx.task_ctx();
    let mut out = Vec::new();
    for p in 0..node.properties().output_partitioning().partition_count() {
        let mut s = node.execute(p, Arc::clone(&task_ctx)).expect("execute local");
        while let Some(b) = s.next().await {
            out.push(b.expect("local batch"));
        }
    }
    (node.schema(), out)
}

/// Execute the same scan the way a worker would: build the production tasks,
/// then run each through the worker's scan entry point.
pub async fn distributed_rows(scan: &Arc<dyn ExecutionPlan>) -> (SchemaRef, Vec<RecordBatch>) {
    let hive = scan
        .downcast_ref::<HiveScanExec>()
        .expect("a HiveScanExec");
    let spec = hive.spec();
    let partitions = hive.partition_files().await.expect("list partitions");
    let tasks = sqe_coordinator::hive_scan_tasks::tasks_for_hive_scan(
        spec,
        partitions,
        &sqe_coordinator::hive_scan_tasks::HiveS3Access::default(),
        "diff",
    )
    .expect("build tasks");
    assert!(
        !tasks.is_empty(),
        "no tasks were built, so the distributed arm would trivially match an \
         empty result"
    );

    let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
    let mut out = Vec::new();
    for task in tasks {
        let (_schema, mut stream) = sqe_worker::executor::execute_scan_streaming_with_store(
            task,
            None,
            SessionContext::new(),
            Arc::clone(&store),
            None,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("worker scan setup");
        while let Some(item) = stream.next().await {
            out.push(item.expect("worker batch").get().clone());
        }
    }
    (Arc::clone(&spec.full_schema), out)
}

/// Render batches as sorted lines, so the comparison is order-insensitive.
///
/// Order-insensitive on purpose: the two paths concatenate partitions in
/// whatever order they enumerate them, and the scan promises no ordering.
/// Comparing rendered text rather than row structs also makes a type
/// difference visible -- `2024` and `"2024"` render differently -- which is
/// the failure `partition_scalar` sharing exists to prevent.
pub fn sorted_rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for b in batches {
        for row in 0..b.num_rows() {
            let cells: Vec<String> = (0..b.num_columns())
                .map(|c| {
                    let col = b.column(c);
                    if col.is_null(row) {
                        "NULL".to_string()
                    } else {
                        datafusion::arrow::util::display::array_value_to_string(col.as_ref(), row)
                            .unwrap_or_else(|e| format!("<{e}>"))
                    }
                })
                .collect();
            lines.push(cells.join("|"));
        }
    }
    lines.sort();
    lines
}

/// Assert the two arms agree on schema and on rows.
pub fn assert_same(
    what: &str,
    local: (SchemaRef, Vec<RecordBatch>),
    dist: (SchemaRef, Vec<RecordBatch>),
) {
    let (ls, lb) = local;
    let (ds, db) = dist;

    let lf: Vec<(String, DataType)> = ls
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    let df: Vec<(String, DataType)> = ds
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    assert_eq!(
        lf, df,
        "{what}: the two paths disagree on the scan's output schema. A column \
         order swap or a partition column typed Utf8 on one side returns rows \
         either way, which is why this is asserted before the rows."
    );

    let lr = sorted_rows(&lb);
    let dr = sorted_rows(&db);
    assert_eq!(
        lr.len(),
        dr.len(),
        "{what}: row counts differ, local {} vs distributed {}",
        lr.len(),
        dr.len()
    );
    assert_eq!(lr, dr, "{what}: same count, different rows");
}

/// Stand in for `DistributedScanExec` over already-collected batches.
///
/// The point of the substitution test is not the transport: it is that the node
/// replacing `HiveScanExec` is WIDER than what it replaced, and that every
/// parent indexes its child by position. So this reproduces the one property
/// that matters -- output schema is `spec.full_schema` -- and then runs the
/// same `align_to_scan_schema` production calls, against the real parent plan.
///
/// Deliberately NOT a mock of Flight. A fake transport would test the fake.
pub fn stand_in_for_distributed(
    scan: &Arc<dyn ExecutionPlan>,
    full_schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Arc<dyn ExecutionPlan> {
    let mem = datafusion::datasource::MemTable::try_new(
        Arc::clone(&full_schema),
        vec![batches],
    )
    .expect("mem table over the distributed batches");
    let wide: Arc<dyn ExecutionPlan> = futures::executor::block_on(async {
        let ctx = SessionContext::new();
        datafusion::datasource::TableProvider::scan(&mem, &ctx.state(), None, &[], None)
            .await
            .expect("scan the stand-in")
    });
    assert_eq!(
        wide.schema().fields(),
        full_schema.fields(),
        "the stand-in must be as wide as a real distributed hive scan, or the \
         substitution test proves nothing"
    );
    sqe_coordinator::hive_scan_tasks::align_to_scan_schema(wide, &scan.schema())
        .expect("align the distributed scan to the node it replaces")
}

/// Write one partition directory holding a single CSV file, with a header row.
///
/// The header is written on purpose. `skip.header.line.count = 1` is the
/// declaration, and a reader that ignores it turns the header into an extra row
/// -- either a parse error (`id` is not an Int64) or, if the reader is lenient,
/// a row of NULLs. The second case is silent, so the differential tests assert
/// row counts.
fn write_csv_partition(data: &std::path::Path, part_dir: &str, ids: &[i64], amounts: &[f64]) {
    let dir = data.join(part_dir);
    std::fs::create_dir_all(&dir).expect("mkdir partition");
    let mut body = String::from("id,amount\n");
    for (i, a) in ids.iter().zip(amounts) {
        body.push_str(&format!("{i},{a}\n"));
    }
    std::fs::write(dir.join("part-0.csv"), body).expect("write csv");
}

/// Write one partition directory holding a single line-delimited JSON file.
fn write_json_partition(data: &std::path::Path, part_dir: &str, ids: &[i64], amounts: &[f64]) {
    let dir = data.join(part_dir);
    std::fs::create_dir_all(&dir).expect("mkdir partition");
    let mut body = String::new();
    for (i, a) in ids.iter().zip(amounts) {
        body.push_str(&format!("{{\"id\":{i},\"amount\":{a}}}\n"));
    }
    std::fs::write(dir.join("part-0.json"), body).expect("write json");
}

/// The same three-partition, five-row shape as [`multi_partition_parquet`], in
/// `format`.
///
/// Identical data across formats on purpose: the differential test per format
/// then compares against a known row count rather than against whatever that
/// format's reader happened to produce.
pub async fn multi_partition(format: &str) -> Fixture {
    let dir = tempfile::tempdir().expect("tmp");
    let data_path = dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("mkdir data");

    let write: fn(&std::path::Path, &str, &[i64], &[f64]) = match format {
        "parquet" => write_parquet_partition,
        "csv" => write_csv_partition,
        "json" => write_json_partition,
        other => panic!("unknown fixture format `{other}`"),
    };
    write(&data_path, "dt=2026-01-01", &[1, 2], &[10.5, 20.0]);
    write(&data_path, "dt=2026-01-02", &[3], &[30.5]);
    write(&data_path, "dt=2026-01-03", &[4, 5], &[40.5, 45.0]);

    let f = Fixture {
        root: format!("file://{}/wh", dir.path().display()),
        data: format!("file://{}", data_path.display()),
        partitions: vec![
            "dt=2026-01-01".into(),
            "dt=2026-01-02".into(),
            "dt=2026-01-03".into(),
        ],
        key: ("dt".into(), "string".into()),
        dir,
    };
    put_manifest(&f.root, &f.data, &f.key, format).await;
    plant_index(&f).await;
    f
}

/// A CSV table whose second column is Utf8 and holds a quoted value containing
/// the field delimiter.
///
/// The narrow case that separates "the reader read the delimiter" from "the
/// reader read the SerDe's quoteChar". Without quoting honored, `"a,b"` becomes
/// two fields and every column after it shifts by one.
pub async fn quoted_csv() -> Fixture {
    let dir = tempfile::tempdir().expect("tmp");
    let data_path = dir.path().join("data");
    let part = data_path.join("dt=2026-01-01");
    std::fs::create_dir_all(&part).expect("mkdir partition");
    std::fs::write(
        part.join("part-0.csv"),
        "id,note\n1,\"a,b\"\n2,plain\n",
    )
    .expect("write csv");

    let f = Fixture {
        root: format!("file://{}/wh", dir.path().display()),
        data: format!("file://{}", data_path.display()),
        partitions: vec!["dt=2026-01-01".into()],
        key: ("dt".into(), "string".into()),
        dir,
    };
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT, note STRING) \
         PARTITIONED BY (dt string) \
         ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \
         WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '\"') \
         STORED AS TEXTFILE LOCATION '{}' \
         TBLPROPERTIES ('skip.header.line.count' = '1')",
        f.data
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse quoted-csv ddl");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external table");
    let m = HiveManifest::from_spec(&spec, "sales", "test", "2026-08-27T00:00:00Z");
    local_store(&f.root).put(&m, false).await.expect("put manifest");
    plant_index(&f).await;
    f
}

/// Run the partition-index discovery the coordinator runs after a write.
///
/// `HiveDdlHandler::refresh_index_after_write` delegates to exactly this
/// function, so a test calling it exercises the same rebuild rather than a
/// re-implementation of it.
pub async fn refresh_partition_index(f: &Fixture) {
    let store = local_store(&f.root);
    let mut m = store.get("sales", "orders").await.expect("get manifest");
    let max = store.limits().max_partition_index_entries;
    sqe_catalog::hive::partition::msck_repair(&store, &mut m, max)
        .await
        .expect("refresh the partition index");
}

/// A CSV table that declares NO header, with headerless data on disk.
///
/// The opposite half of the silent pair. With `skip.header.line.count` absent,
/// a writer that emits a header adds a phantom row on read-back whose columns
/// are the literal column names. Testing only the declared-1 case is passed by
/// a writer that gets this one wrong, which is why both exist.
pub async fn headerless_csv() -> Fixture {
    let dir = tempfile::tempdir().expect("tmp");
    let data_path = dir.path().join("data");
    let part = data_path.join("dt=2026-01-01");
    std::fs::create_dir_all(&part).expect("mkdir partition");
    // No header line, matching the absent declaration.
    std::fs::write(part.join("part-0.csv"), "1,10.5\n2,20.0\n").expect("write csv");

    let f = Fixture {
        root: format!("file://{}/wh", dir.path().display()),
        data: format!("file://{}", data_path.display()),
        partitions: vec!["dt=2026-01-01".into()],
        key: ("dt".into(), "string".into()),
        dir,
    };
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT, amount DOUBLE) \
         PARTITIONED BY (dt string) \
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
         STORED AS TEXTFILE LOCATION '{}'",
        f.data
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse headerless-csv ddl");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external table");
    let m = HiveManifest::from_spec(&spec, "sales", "test", "2026-08-27T00:00:00Z");
    local_store(&f.root).put(&m, false).await.expect("put manifest");
    plant_index(&f).await;
    f
}

/// The fixture's manifest store, for tests that need to inspect the manifest.
pub fn manifest_store(f: &Fixture) -> HiveManifestStore {
    local_store(&f.root)
}

/// Write the manifest for an arbitrary spec, the way `create_external_table`
/// does, so a CTAS test can perform step 1 of the two-step sequence.
pub async fn put_spec_manifest(f: &Fixture, spec: &sqe_sql::hive_ddl::HiveTableSpec) {
    let m = HiveManifest::from_spec(
        spec,
        spec.database.as_deref().unwrap_or("sales"),
        "test",
        "2026-08-27T00:00:00Z",
    );
    local_store(&f.root)
        .put(&m, false)
        .await
        .expect("put the CTAS manifest");
}

/// The index refresh, for a table other than the fixture's `orders`.
pub async fn refresh_named_index(f: &Fixture, table: &str) {
    let store = local_store(&f.root);
    let mut m = store.get("sales", table).await.expect("get manifest");
    let max = store.limits().max_partition_index_entries;
    sqe_catalog::hive::partition::msck_repair(&store, &mut m, max)
        .await
        .expect("refresh the partition index");
}
