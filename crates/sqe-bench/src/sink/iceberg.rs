//! Direct-to-Iceberg sink for the bank benchmark.
//!
//! Writes partition-aligned, compressed Parquet straight to the fact
//! table's storage location and commits the files to the catalog (Polaris)
//! through the Iceberg REST API using the vendored iceberg-rust client.
//! One `fast_append` per trading day; each snapshot carries a
//! `sqe-bench.day` summary property so an interrupted run can resume by
//! reading the snapshot history.
//!
//! Work is organized as `(table, day, shard)` units. Units run as tokio
//! tasks bounded by a semaphore sized to the configured thread count, so
//! peak memory is `permits x (one batch + one row-group buffer + one
//! multipart upload buffer)` regardless of total dataset size.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use futures::{Stream, StreamExt};
use iceberg::arrow::{
    arrow_schema_to_schema, arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema,
};
use iceberg::spec::{DataFile, Literal, PartitionKey, PartitionSpec, Struct, Transform};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use parquet::file::properties::WriterProperties;
use tokio::sync::{Mutex, Semaphore};

use crate::generate::bank::{self, BankPlan};
use crate::generate::config::partition as partition_rows;
use crate::generate::GenerateConfig;

/// Snapshot summary key marking a committed trading day.
pub const SNAPSHOT_DAY_PROP: &str = "sqe-bench.day";

/// Table property prefix marking a committed trading day. The property
/// (`sqe-bench.day.YYYY-MM-DD = done`) is the durable resume record: some
/// catalogs (Nessie) serve metadata with a trimmed snapshot history, so
/// snapshot summary markers alone are not reliable across catalogs.
pub const DAY_PROP_PREFIX: &str = "sqe-bench.day.";

/// Target rows per generation shard for the small fixed-width tables
/// (dimensions and balance snapshots). Fact shard counts come from the
/// caller, which sizes them against the byte target.
const SMALL_TABLE_SHARD_ROWS: u64 = 4_000_000;

/// Connection and auth settings for the catalog and object store.
#[derive(Debug, Clone, Default)]
pub struct IcebergTarget {
    /// Iceberg REST catalog endpoint, e.g. `https://polaris:8181/api/catalog`.
    pub catalog_uri: String,
    /// Warehouse name registered in the catalog.
    pub warehouse: String,
    /// Namespace to create the bank tables in.
    pub namespace: String,
    /// OAuth2 client credentials as `client_id:client_secret`.
    pub credential: Option<String>,
    /// Token endpoint for the client-credentials grant. When unset the
    /// catalog's own `/v1/oauth/tokens` endpoint is used.
    pub oauth2_server_uri: Option<String>,
    /// OAuth2 scope. Defaults to `catalog` inside the REST client.
    pub scope: Option<String>,
    /// Pre-acquired bearer token, an alternative to `credential`.
    pub bearer_token: Option<String>,
    /// Explicit S3-compatible storage settings for the data writes. When
    /// unset the catalog's vended credentials are used.
    pub s3_endpoint: Option<String>,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub s3_region: Option<String>,
    pub s3_path_style: bool,
}

impl IcebergTarget {
    /// Build the full catalog-connection property map (URI, warehouse,
    /// credentials, and S3 storage settings). `pub` so integration tests
    /// (a separate crate) can open a verification catalog connection that is
    /// provably identical to the one `run_direct` writes through.
    pub fn catalog_props(&self) -> HashMap<String, String> {
        let mut props = HashMap::from([
            (REST_CATALOG_PROP_URI.to_string(), self.catalog_uri.clone()),
            (
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                self.warehouse.clone(),
            ),
        ]);
        if let Some(ref cred) = self.credential {
            props.insert("credential".to_string(), cred.clone());
        }
        if let Some(ref uri) = self.oauth2_server_uri {
            props.insert("oauth2-server-uri".to_string(), uri.clone());
        }
        if let Some(ref scope) = self.scope {
            props.insert("scope".to_string(), scope.clone());
        }
        if let Some(ref token) = self.bearer_token {
            props.insert("token".to_string(), token.clone());
        }
        if let Some(ref ep) = self.s3_endpoint {
            props.insert(iceberg::io::S3_ENDPOINT.to_string(), ep.clone());
        }
        if let Some(ref key) = self.s3_access_key {
            props.insert(iceberg::io::S3_ACCESS_KEY_ID.to_string(), key.clone());
        }
        if let Some(ref key) = self.s3_secret_key {
            props.insert(iceberg::io::S3_SECRET_ACCESS_KEY.to_string(), key.clone());
        }
        if let Some(ref region) = self.s3_region {
            props.insert(iceberg::io::S3_REGION.to_string(), region.clone());
        }
        if self.s3_path_style {
            props.insert(
                iceberg::io::S3_PATH_STYLE_ACCESS.to_string(),
                "true".to_string(),
            );
        }
        props
    }
}

/// Sizing of one bank run against the Iceberg sink.
#[derive(Debug, Clone, Copy)]
pub struct BankRunSpec {
    pub plan: BankPlan,
    /// Transaction generation shards per day. Sized by the caller so one
    /// shard emits a handful of target-size files (see calibration).
    pub txn_shards_per_day: u32,
    /// Rollover threshold for data files, in bytes.
    pub target_file_size: usize,
    /// Skip days that already have a committed snapshot.
    pub resume: bool,
    /// Drop existing bank tables first (idempotent reload).
    pub clean: bool,
}

/// One generation unit: a disjoint slice of one table, optionally bound to
/// a partition day. Units are the scheduling and determinism granule.
struct Unit {
    table: &'static str,
    day_idx: Option<u32>,
    shard_idx: u32,
    /// Row range (facts: within the day; dimensions: within the table).
    rows: std::ops::Range<u64>,
    /// Account-id slice this shard draws from (transaction only).
    accounts: std::ops::Range<u64>,
}

impl Unit {
    fn seed(&self) -> u64 {
        bank::unit_seed(self.table, self.day_idx.unwrap_or(0), self.shard_idx)
    }

    fn file_prefix(&self) -> String {
        match self.day_idx {
            Some(d) => format!("{}-d{:04}-s{:04}", self.table, d, self.shard_idx),
            None => format!("{}-s{:04}", self.table, self.shard_idx),
        }
    }

    fn batches(&self, plan: &BankPlan) -> Box<dyn Iterator<Item = RecordBatch> + Send> {
        let seed = self.seed();
        match (self.table, self.day_idx) {
            ("customer", _) => Box::new(bank::customer_range(
                self.rows.start as usize..self.rows.end as usize,
                seed,
            )),
            ("account", _) => Box::new(bank::account_range(
                self.rows.start as usize..self.rows.end as usize,
                *plan,
                seed,
            )),
            ("kyc_profile", _) => Box::new(bank::kyc_profile_range(
                self.rows.start as usize..self.rows.end as usize,
                seed,
            )),
            ("transaction", Some(d)) => {
                let day = plan.start_day + d as i32;
                let t_id_start = d as i64 * plan.txn_rows_per_day as i64 + self.rows.start as i64;
                Box::new(bank::transaction_day_shard(
                    day,
                    self.rows.end - self.rows.start,
                    t_id_start,
                    self.accounts.clone(),
                    seed,
                ))
            }
            ("account_balance", Some(d)) => {
                let day = plan.start_day + d as i32;
                Box::new(bank::account_balance_day_shard(
                    day,
                    self.rows.clone(),
                    seed,
                ))
            }
            (t, d) => unreachable!("no generator for unit {t}/{d:?}"),
        }
    }
}

/// Format a Date32 day number as `YYYY-MM-DD`.
pub fn format_day(days_since_epoch: i32) -> String {
    chrono::DateTime::from_timestamp(days_since_epoch as i64 * 86_400, 0)
        .map(|dt| dt.date_naive().to_string())
        .unwrap_or_else(|| days_since_epoch.to_string())
}

/// Parse `YYYY-MM-DD` into a Date32 day number.
pub fn parse_day(s: &str) -> anyhow::Result<i32> {
    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("invalid date '{s}', expected YYYY-MM-DD"))?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    Ok((date - epoch).num_days() as i32)
}

/// Split `rows` into shard ranges of roughly `SMALL_TABLE_SHARD_ROWS`.
fn small_table_shards(rows: u64) -> Vec<std::ops::Range<u64>> {
    let shards = rows.div_ceil(SMALL_TABLE_SHARD_ROWS).max(1) as usize;
    partition_rows(rows as usize, shards)
        .into_iter()
        .map(|r| r.start as u64..r.end as u64)
        .collect()
}

/// Per-table state shared by all of its units.
struct TableCtx {
    table: Table,
    /// Arrow schema the writer expects; batches are rebound to it so field
    /// metadata matches the table schema exactly.
    write_schema: Arc<ArrowSchema>,
}

impl TableCtx {
    /// Cheap handle clone for fanning a table out across shard tasks:
    /// `iceberg::table::Table` is `Clone` (it wraps `FileIO` + an `Arc`'d
    /// metadata snapshot), so no `Arc<TableCtx>` wrapper is needed.
    ///
    /// `run_direct` (below) is the only caller.
    fn clone_handle(&self) -> TableCtx {
        TableCtx {
            table: self.table.clone(),
            write_schema: self.write_schema.clone(),
        }
    }
}

/// Everything the worker tasks share.
struct RunCtx {
    catalog: Box<dyn Catalog>,
    tables: HashMap<&'static str, TableCtx>,
    plan: BankPlan,
    writer_props: WriterProperties,
    target_file_size: usize,
    /// Serializes catalog commits: concurrent day commits to one table
    /// would force conflict-retry loops for no benefit.
    commit_lock: Mutex<()>,
}

pub const BANK_TABLES: [&str; 5] = [
    "customer",
    "account",
    "kyc_profile",
    "transaction",
    "account_balance",
];

fn bank_arrow_schema(table: &str) -> arrow_schema::SchemaRef {
    match table {
        "customer" => bank::customer_schema(),
        "account" => bank::account_schema(),
        "kyc_profile" => bank::kyc_profile_schema(),
        "transaction" => bank::transaction_schema(),
        "account_balance" => bank::account_balance_schema(),
        other => unreachable!("unknown bank table {other}"),
    }
}

/// Build a partition spec from a `PARTITIONED BY` expression as the layout
/// writes it: comma-separated fields, each a bare column (identity) or
/// `month(col)`. Field names follow the engine's CTAS: the bare column for
/// identity, `<col>_month` for a month transform.
fn partition_spec_from_expr(
    schema: &iceberg::spec::Schema,
    expr: &str,
) -> anyhow::Result<PartitionSpec> {
    let mut builder = PartitionSpec::builder(schema.clone());
    for field in expr.split(',').map(str::trim) {
        let (col, name, transform) = match field
            .strip_prefix("month(")
            .and_then(|f| f.strip_suffix(')'))
        {
            Some(col) => (col, format!("{col}_month"), Transform::Month),
            None => (field, field.to_string(), Transform::Identity),
        };
        builder = builder
            .add_partition_field(col, name, transform)
            .with_context(|| format!("partition field `{field}` of `{expr}`"))?;
    }
    Ok(builder.build()?)
}

/// Ensure one table exists in `ns`, creating it (partitioned by the
/// [`partition_spec_from_expr`] expression `partition`, if any) if missing.
/// With `clean`, an existing table is dropped and recreated first.
async fn ensure_table(
    catalog: &dyn Catalog,
    ns: &NamespaceIdent,
    name: &str,
    arrow_schema: &ArrowSchema,
    partition: Option<&str>,
    clean: bool,
    auto_assign_ids: bool,
) -> anyhow::Result<TableCtx> {
    let ident = TableIdent::new(ns.clone(), name.to_string());
    if clean && catalog.table_exists(&ident).await? {
        catalog
            .drop_table(&ident)
            .await
            .with_context(|| format!("dropping table {name}"))?;
        println!("Dropped table {ns:?}.{name}");
    }
    let table = if catalog.table_exists(&ident).await? {
        catalog.load_table(&ident).await?
    } else {
        let schema = if auto_assign_ids {
            arrow_schema_to_schema_auto_assign_ids(arrow_schema)
        } else {
            arrow_schema_to_schema(arrow_schema)
        }
        .with_context(|| format!("converting {name} schema"))?;
        let creation = if let Some(expr) = partition {
            let spec = partition_spec_from_expr(&schema, expr)
                .with_context(|| format!("building {name} partition spec"))?;
            TableCreation::builder()
                .name(name.to_string())
                .schema(schema)
                .partition_spec(spec.into_unbound())
                .build()
        } else {
            TableCreation::builder()
                .name(name.to_string())
                .schema(schema)
                .build()
        };
        let table = catalog
            .create_table(ns, creation)
            .await
            .with_context(|| format!("creating table {name}"))?;
        println!("Created table {name}");
        table
    };
    let write_schema = Arc::new(
        schema_to_arrow_schema(table.metadata().current_schema())
            .with_context(|| format!("deriving {name} write schema"))?,
    );
    Ok(TableCtx {
        table,
        write_schema,
    })
}

/// Create the namespace and any missing bank tables; return handles.
/// With `clean`, existing bank tables are dropped and recreated first.
async fn ensure_tables(
    catalog: &dyn Catalog,
    namespace: &str,
    clean: bool,
) -> anyhow::Result<HashMap<&'static str, TableCtx>> {
    let ns = NamespaceIdent::new(namespace.to_string());
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("checking namespace")?
    {
        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .context("creating namespace")?;
        println!("Created namespace {namespace}");
    }

    let mut tables = HashMap::new();
    for name in BANK_TABLES {
        let arrow_schema = bank_arrow_schema(name);
        let table_ctx = ensure_table(
            catalog,
            &ns,
            name,
            &arrow_schema,
            bank::partition_column(name),
            clean,
            false,
        )
        .await?;
        tables.insert(name, table_ctx);
    }
    Ok(tables)
}

/// Days already committed for `table`, read from the day-marker table
/// properties.
fn committed_days(table: &Table) -> HashSet<i32> {
    table
        .metadata()
        .properties()
        .keys()
        .filter_map(|k| k.strip_prefix(DAY_PROP_PREFIX))
        .filter_map(|d| parse_day(d).ok())
        .collect()
}

/// Write `batches` into `table_ctx`'s table as one or more data files
/// (rolling over at `target_file_size`); returns the finished `DataFile`s
/// without committing them. `file_prefix` should be deterministic per
/// caller so a re-run of the same unit overwrites its own partial output
/// from a crashed run.
async fn write_shard(
    writer_props: &WriterProperties,
    target_file_size: usize,
    table_ctx: &TableCtx,
    file_prefix: String,
    partition_key: Option<PartitionKey>,
    batches: impl Iterator<Item = RecordBatch>,
) -> anyhow::Result<Vec<DataFile>> {
    write_shard_stream(
        writer_props,
        target_file_size,
        table_ctx,
        file_prefix,
        partition_key,
        futures::stream::iter(batches),
    )
    .await
}

/// Consume batches as they arrive. Retain only the writer buffers and the
/// bounded producer queue, including when object storage is slower than generation.
async fn write_shard_stream(
    writer_props: &WriterProperties,
    target_file_size: usize,
    table_ctx: &TableCtx,
    file_prefix: String,
    partition_key: Option<PartitionKey>,
    batches: impl Stream<Item = RecordBatch>,
) -> anyhow::Result<Vec<DataFile>> {
    let table = &table_ctx.table;
    let location_gen =
        DefaultLocationGenerator::new(table.metadata()).context("building location generator")?;
    let file_name_gen =
        DefaultFileNameGenerator::new(file_prefix, None, iceberg::spec::DataFileFormat::Parquet);
    let parquet_builder = ParquetWriterBuilder::new(
        writer_props.clone(),
        table.metadata().current_schema().clone(),
    );
    let rolling = RollingFileWriterBuilder::new(
        parquet_builder,
        target_file_size,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );
    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(partition_key)
        .await
        .context("building data file writer")?;
    futures::pin_mut!(batches);
    while let Some(batch) = batches.next().await {
        // Rebind to the table-derived arrow schema: column data types are
        // identical, only field metadata (field ids) may differ in detail.
        let batch = RecordBatch::try_new(table_ctx.write_schema.clone(), batch.columns().to_vec())
            .context("rebinding batch to table schema")?;
        writer.write(batch).await.context("writing batch")?;
    }
    writer.close().await.context("closing writer")
}

/// Write one unit's batches as data files; returns the finished
/// `DataFile`s without committing them.
async fn write_unit(ctx: &RunCtx, unit: &Unit) -> anyhow::Result<Vec<DataFile>> {
    let tctx = &ctx.tables[unit.table];
    let table = &tctx.table;

    let partition_key = unit.day_idx.map(|d| {
        let day = ctx.plan.start_day + d as i32;
        PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::date(day))]),
        )
    });

    write_shard(
        &ctx.writer_props,
        ctx.target_file_size,
        tctx,
        unit.file_prefix(),
        partition_key,
        unit.batches(&ctx.plan),
    )
    .await
}

/// Build the day-specific snapshot and table property maps for a bank
/// commit. Dimension tables (no day) get no markers.
fn day_props(day: Option<i32>) -> (HashMap<String, String>, HashMap<String, String>) {
    match day {
        Some(d) => (
            HashMap::from([(SNAPSHOT_DAY_PROP.to_string(), format_day(d))]),
            HashMap::from([(
                format!("{DAY_PROP_PREFIX}{}", format_day(d)),
                "done".to_string(),
            )]),
        ),
        None => (HashMap::new(), HashMap::new()),
    }
}

/// Commit `files` to the table as one fast-append snapshot, applying
/// `snapshot_props` to the snapshot summary and `table_props` to the table
/// properties (both committed atomically with the append).
async fn commit_files(
    catalog: &dyn Catalog,
    table_ctx: &TableCtx,
    commit_lock: &Mutex<()>,
    files: Vec<DataFile>,
    snapshot_props: HashMap<String, String>,
    table_props: HashMap<String, String>,
) -> anyhow::Result<()> {
    if files.is_empty() {
        return Ok(());
    }
    let _guard = commit_lock.lock().await;
    // Reload so the transaction bases on the latest metadata; commits from
    // other days may have landed since this handle was created.
    let table = catalog
        .load_table(table_ctx.table.identifier())
        .await
        .context("reloading table before commit")?;
    let tx = Transaction::new(&table);
    let mut append = tx.fast_append().add_data_files(files);
    if !snapshot_props.is_empty() {
        append = append.set_snapshot_properties(snapshot_props);
    }
    let mut tx = append.apply(tx).context("applying append")?;
    if !table_props.is_empty() {
        let mut upd = tx.update_table_properties();
        for (k, v) in table_props {
            upd = upd.set(k, v);
        }
        tx = upd.apply(tx).context("applying table properties")?;
    }
    tx.commit(catalog).await.context("committing append")?;
    Ok(())
}

/// Human-readable byte count.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut v = bytes as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    format!("{v:.1} {}", UNITS[unit])
}

/// Run one group of units through the shared semaphore and commit the
/// resulting files as a single snapshot. Used for a dimension table (all
/// shards) and for one fact-table day (all of that day's shards).
async fn run_group(
    ctx: Arc<RunCtx>,
    sem: Arc<Semaphore>,
    table_name: &'static str,
    day: Option<i32>,
    units: Vec<Unit>,
) -> anyhow::Result<(u64, u64)> {
    let started = std::time::Instant::now();
    let mut handles = Vec::with_capacity(units.len());
    for unit in units {
        let ctx = ctx.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            write_unit(&ctx, &unit).await
        }));
    }
    let mut files = Vec::new();
    for h in handles {
        files.extend(h.await.context("unit task panicked")??);
    }

    let rows: u64 = files.iter().map(|f| f.record_count()).sum();
    let bytes: u64 = files.iter().map(|f| f.file_size_in_bytes()).sum();
    let count = files.len();
    let (snapshot_props, table_props) = day_props(day);
    commit_files(
        ctx.catalog.as_ref(),
        &ctx.tables[table_name],
        &ctx.commit_lock,
        files,
        snapshot_props,
        table_props,
    )
    .await?;
    let label = match day {
        Some(d) => format!("{table_name} {}", format_day(d)),
        None => table_name.to_string(),
    };
    println!(
        "  committed {label}: {rows} rows, {count} files, {}, {:.1}s",
        human_bytes(bytes),
        started.elapsed().as_secs_f64()
    );
    Ok((rows, bytes))
}

/// Generate the bank dataset straight into Iceberg tables.
pub async fn run_bank(
    target: &IcebergTarget,
    spec: &BankRunSpec,
    config: &GenerateConfig,
) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let plan = spec.plan;

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load("polaris", target.catalog_props())
        .await
        .context("connecting to catalog")?;
    let catalog: Box<dyn Catalog> = Box::new(catalog);

    let tables = ensure_tables(catalog.as_ref(), &target.namespace, spec.clean).await?;

    // Refuse to double-load: unless resuming, every table must be empty.
    let mut done_txn: HashSet<i32> = HashSet::new();
    let mut done_bal: HashSet<i32> = HashSet::new();
    let mut dims_done = false;
    if spec.resume {
        done_txn = committed_days(&tables["transaction"].table);
        done_bal = committed_days(&tables["account_balance"].table);
        dims_done = tables["customer"].table.metadata().snapshots().count() > 0;
        if !done_txn.is_empty() || !done_bal.is_empty() || dims_done {
            println!(
                "Resuming: dims {}, {} transaction day(s) and {} balance day(s) already committed",
                if dims_done { "done" } else { "pending" },
                done_txn.len(),
                done_bal.len()
            );
        }
    } else if tables
        .values()
        .any(|t| t.table.metadata().snapshots().count() > 0)
    {
        anyhow::bail!(
            "namespace {} already contains data; pass --resume to continue an \
             interrupted run or choose a fresh namespace",
            target.namespace
        );
    }

    let mut props_builder =
        WriterProperties::builder().set_compression(config.compression.to_parquet());
    if let Some(rgs) = config.row_group_size {
        props_builder = props_builder.set_max_row_group_row_count(Some(rgs));
    }

    let ctx = Arc::new(RunCtx {
        catalog,
        tables,
        plan,
        writer_props: props_builder.build(),
        target_file_size: spec.target_file_size,
        commit_lock: Mutex::new(()),
    });
    let sem = Arc::new(Semaphore::new(config.threads.max(1)));

    let mut groups: Vec<(&'static str, Option<i32>, Vec<Unit>)> = Vec::new();

    if !dims_done {
        for (name, rows) in [
            ("customer", plan.customers),
            ("account", plan.accounts()),
            ("kyc_profile", plan.customers),
        ] {
            let units = small_table_shards(rows)
                .into_iter()
                .enumerate()
                .map(|(i, r)| Unit {
                    table: name,
                    day_idx: None,
                    shard_idx: i as u32,
                    rows: r,
                    accounts: 0..0,
                })
                .collect();
            groups.push((name, None, units));
        }
    }

    let txn_shards = spec.txn_shards_per_day.max(1) as usize;
    let account_slices: Vec<std::ops::Range<u64>> =
        partition_rows(plan.accounts() as usize, txn_shards)
            .into_iter()
            .map(|r| r.start as u64..r.end as u64)
            .collect();
    for d in 0..plan.days {
        let day = plan.start_day + d as i32;
        if !done_txn.contains(&day) {
            let units = partition_rows(plan.txn_rows_per_day as usize, txn_shards)
                .into_iter()
                .enumerate()
                .map(|(i, r)| Unit {
                    table: "transaction",
                    day_idx: Some(d),
                    shard_idx: i as u32,
                    rows: r.start as u64..r.end as u64,
                    accounts: account_slices[i].clone(),
                })
                .collect();
            groups.push(("transaction", Some(day), units));
        }
        if !done_bal.contains(&day) {
            let units = small_table_shards(plan.accounts())
                .into_iter()
                .enumerate()
                .map(|(i, r)| Unit {
                    table: "account_balance",
                    day_idx: Some(d),
                    shard_idx: i as u32,
                    rows: r,
                    accounts: 0..0,
                })
                .collect();
            groups.push(("account_balance", Some(day), units));
        }
    }

    let total_units: usize = groups.iter().map(|(_, _, u)| u.len()).sum();
    println!(
        "Generating bank dataset into {} ({} days x {} txn rows/day, {} units, {} workers)",
        target.namespace, plan.days, plan.txn_rows_per_day, total_units, config.threads
    );

    // Every group runs concurrently; the semaphore bounds actual work. A
    // group commits the moment its own units finish.
    let mut group_handles = Vec::with_capacity(groups.len());
    for (name, day, units) in groups {
        group_handles.push(tokio::spawn(run_group(
            ctx.clone(),
            sem.clone(),
            name,
            day,
            units,
        )));
    }
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    for h in group_handles {
        let (rows, bytes) = h.await.context("group task panicked")??;
        total_rows += rows;
        total_bytes += bytes;
    }

    println!(
        "Done: {total_rows} rows, {} written in {:.1}s ({}/s)",
        human_bytes(total_bytes),
        started.elapsed().as_secs_f64(),
        human_bytes((total_bytes as f64 / started.elapsed().as_secs_f64().max(0.001)) as u64),
    );
    Ok(())
}

/// Why the direct sink cannot write `table` the way `sqe-bench load` does,
/// or `None` when it can. A table written differently prunes differently,
/// so its timings would not compare with a loaded baseline. Partitioning is
/// reproduced ([`write_partitioned`]); the sink never sorts, so a table
/// `load` sorts on write must come out of its generator in that order
/// ([`BenchmarkGenerator::sorted_by`](crate::generate::BenchmarkGenerator::sorted_by)).
pub fn direct_layout_gap(
    gen: &dyn crate::generate::BenchmarkGenerator,
    table: &str,
) -> Option<String> {
    let load_sort = crate::layout::table_layout(gen.name(), table).sort_key();
    sort_gap(load_sort, gen.sorted_by(table)).map(|key| {
        format!(
            "{}.{table}: `load` writes it sorted on write by {key} and its generator \
             does not emit that order; use generate + load, or pass --ignore-layout",
            gen.name()
        )
    })
}

/// The key `load` sorts by when the generator's emitted order does not
/// already satisfy it.
fn sort_gap<'a>(load_sort: Option<&'a str>, emitted: Option<&str>) -> Option<&'a str> {
    load_sort.filter(|key| emitted != Some(*key))
}

/// Settings for [`run_direct`] beyond the target and the generator.
#[derive(Debug, Clone, Copy)]
pub struct DirectOptions {
    /// Drop and recreate each table first.
    pub clean: bool,
    /// Skip a table already carrying its done marker.
    pub resume: bool,
    /// Data file rollover size in bytes.
    pub target_file_size: usize,
    /// Write tables whose `load` layout the sink cannot reproduce.
    pub ignore_layout: bool,
    /// In-memory bytes a partitioned table may buffer across all shards
    /// before its largest partition is written out as a file.
    pub partition_buffer: usize,
}

/// Write a partitioned table's shards through one shared buffer.
///
/// Every shard's producer feeds one channel; batches are split by partition
/// and buffered per partition. Once the buffer holds more than
/// `partition_buffer` bytes, the largest partition is written out as one
/// file (rolling at `target_file_size`) on its own task, up to `threads`
/// writes at a time, and waiting for a write slot is the backpressure. A
/// buffer per shard would multiply the file count by the thread count;
/// iceberg's fanout writer keeps an open row group per partition per shard,
/// which at SF100 holds most of the table in memory.
async fn write_partitioned(
    writer_props: &WriterProperties,
    table_ctx: &TableCtx,
    shards: Vec<crate::generate::BatchShard>,
    threads: usize,
    opts: &DirectOptions,
) -> anyhow::Result<Vec<DataFile>> {
    let table = &table_ctx.table;
    let splitter = iceberg::arrow::RecordBatchPartitionSplitter::try_new_with_computed_values(
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().clone(),
    )
    .context("building partition splitter")?;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<RecordBatch>(threads.max(1) * 4);
    let gen_slots = Arc::new(Semaphore::new(threads.max(1)));
    let mut producers = Vec::with_capacity(shards.len());
    for shard in shards {
        let tx = tx.clone();
        let gen_slots = gen_slots.clone();
        producers.push(tokio::spawn(async move {
            let _permit = gen_slots.acquire_owned().await.expect("semaphore closed");
            tokio::task::spawn_blocking(move || {
                for b in (shard.make)() {
                    if tx.blocking_send(b).is_err() {
                        break;
                    }
                }
            })
            .await
        }));
    }
    drop(tx);

    let write_slots = Arc::new(Semaphore::new(threads.max(1)));
    let mut writes = tokio::task::JoinSet::new();
    let mut flushes = 0usize;
    let mut flush = |key: PartitionKey, parts: Vec<RecordBatch>, permit| {
        let writer_props = writer_props.clone();
        let tctx = table_ctx.clone_handle();
        let prefix = format!("{flushes:05}");
        let target_file_size = opts.target_file_size;
        flushes += 1;
        writes.spawn(async move {
            let _permit: tokio::sync::OwnedSemaphorePermit = permit;
            write_shard(
                &writer_props,
                target_file_size,
                &tctx,
                prefix,
                Some(key),
                parts.into_iter(),
            )
            .await
        });
    };

    let mut buffered: HashMap<Struct, (PartitionKey, Vec<RecordBatch>, usize)> = HashMap::new();
    let mut total = 0usize;
    while let Some(batch) = rx.recv().await {
        // The splitter resolves source columns by field id: bind first.
        let batch = RecordBatch::try_new(table_ctx.write_schema.clone(), batch.columns().to_vec())
            .context("rebinding batch to table schema")?;
        for (key, part) in splitter.split(&batch).context("splitting batch")? {
            let bytes = part.get_array_memory_size();
            let entry = buffered
                .entry(key.data().clone())
                .or_insert_with(|| (key, Vec::new(), 0));
            entry.1.push(part);
            entry.2 += bytes;
            total += bytes;
        }
        while total > opts.partition_buffer {
            let largest = buffered
                .iter()
                .max_by_key(|(_, (_, _, bytes))| *bytes)
                .map(|(k, _)| k.clone())
                .expect("buffer over budget is not empty");
            let (key, parts, bytes) = buffered.remove(&largest).expect("key just found");
            total -= bytes;
            let permit = write_slots
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed");
            flush(key, parts, permit);
        }
    }
    for p in producers {
        p.await
            .context("producer task panicked")?
            .context("producer task panicked")?;
    }
    for (_, (key, parts, _)) in buffered {
        let permit = write_slots
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        flush(key, parts, permit);
    }
    let mut files = Vec::new();
    while let Some(written) = writes.join_next().await {
        files.extend(written.context("partition write task panicked")??);
    }
    Ok(files)
}

/// Writer properties for one table: `base` plus the bloom filters the
/// engine's writer adds on its own (`_sk` integer columns), so a direct
/// write carries the same footers as a CTAS.
fn direct_writer_props(config: &GenerateConfig, schema: &ArrowSchema) -> WriterProperties {
    let mut builder = WriterProperties::builder().set_compression(config.compression.to_parquet());
    if let Some(rgs) = config.row_group_size {
        builder = builder.set_max_row_group_row_count(Some(rgs));
    }
    for col in crate::layout::engine_auto_bloom_columns(schema) {
        let path = parquet::schema::types::ColumnPath::new(vec![col]);
        builder = builder
            .set_column_bloom_filter_enabled(path.clone(), true)
            .set_column_bloom_filter_fpp(path, crate::layout::ENGINE_BLOOM_FPP);
    }
    builder.build()
}

/// Generic direct-to-Iceberg sink: write every table of `gen` straight into
/// Iceberg, one fast_append per table, with a per-table resume marker.
///
/// Tables `load` partitions are created with the same spec and written
/// through [`write_partitioned`]. Refuses before writing anything when a
/// table's `load` layout is one it cannot reproduce ([`direct_layout_gap`]),
/// unless `opts.ignore_layout`.
pub async fn run_direct(
    target: &IcebergTarget,
    gen: &dyn crate::generate::BenchmarkGenerator,
    scale: f64,
    config: &GenerateConfig,
    opts: &DirectOptions,
) -> anyhow::Result<()> {
    const TABLE_DONE_PREFIX: &str = "sqe-bench.table.";
    let DirectOptions {
        clean,
        resume,
        target_file_size,
        ignore_layout,
        ..
    } = *opts;

    let gaps: Vec<String> = gen
        .tables()
        .iter()
        .filter_map(|t| direct_layout_gap(gen, &t.name))
        .collect();
    if !gaps.is_empty() {
        if !ignore_layout {
            anyhow::bail!(
                "direct sink layout differs from load:\n  {}",
                gaps.join("\n  ")
            );
        }
        for gap in &gaps {
            eprintln!("  warning (--ignore-layout): {gap}");
        }
    }

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load("polaris", target.catalog_props())
        .await
        .context("connecting to catalog")?;
    let catalog: Box<dyn Catalog> = Box::new(catalog);

    let ns = NamespaceIdent::new(target.namespace.clone());
    if !catalog
        .namespace_exists(&ns)
        .await
        .context("checking namespace")?
    {
        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .context("creating namespace")?;
        println!("Created namespace {}", target.namespace);
    }

    let commit_lock = Mutex::new(());

    for table_def in gen.tables() {
        let name = table_def.name.clone();
        let writer_props = direct_writer_props(config, table_def.schema.as_ref());
        let done_key = format!("{TABLE_DONE_PREFIX}{name}");

        // Resume: skip a table already marked done (unless clean forces a rebuild).
        let table_ctx = ensure_table(
            catalog.as_ref(),
            &ns,
            &name,
            table_def.schema.as_ref(),
            crate::layout::table_layout(gen.name(), &name).partition,
            clean,
            true,
        )
        .await?;
        if resume && !clean {
            if let Some(v) = table_ctx.table.metadata().properties().get(&done_key) {
                if v == "done" {
                    println!("  skip {name}: already committed");
                    continue;
                }
            }
        }

        let src = gen.generate_batches(&name, scale, config)?;
        let expected_rows = src.total_rows as u64;
        let partitioned = !table_ctx
            .table
            .metadata()
            .default_partition_spec()
            .is_unpartitioned();
        let files = if partitioned {
            write_partitioned(&writer_props, &table_ctx, src.shards, config.threads, opts).await?
        } else {
            write_unpartitioned(
                &writer_props,
                &table_ctx,
                src.shards,
                config.threads,
                target_file_size,
            )
            .await?
        };

        let rows: u64 = files.iter().map(|f| f.record_count()).sum();
        let bytes: u64 = files.iter().map(|f| f.file_size_in_bytes()).sum();
        let nfiles = files.len();
        let table_props = HashMap::from([(done_key, "done".to_string())]);
        commit_files(
            catalog.as_ref(),
            &table_ctx,
            &commit_lock,
            files,
            HashMap::new(),
            table_props,
        )
        .await?;
        if rows != expected_rows {
            eprintln!(
                "  warning: {name} committed {rows} rows, generator expected {expected_rows}"
            );
        }
        println!(
            "  committed {name}: {rows} rows (expected {expected_rows}), {nfiles} files, {}",
            human_bytes(bytes)
        );
    }
    Ok(())
}

/// Write an unpartitioned table: one writer per shard, up to `threads` at
/// a time.
async fn write_unpartitioned(
    writer_props: &WriterProperties,
    table_ctx: &TableCtx,
    shards: Vec<crate::generate::BatchShard>,
    threads: usize,
    target_file_size: usize,
) -> anyhow::Result<Vec<DataFile>> {
    let sem = Arc::new(Semaphore::new(threads.max(1)));
    let mut handles = Vec::with_capacity(shards.len());
    for (idx, shard) in shards.into_iter().enumerate() {
        let sem = sem.clone();
        let writer_props = writer_props.clone();
        let tctx = table_ctx.clone_handle();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            // Sync producer on a blocking thread -> bounded channel -> async writer.
            let (tx, rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);
            let make = shard.make;
            let producer = tokio::task::spawn_blocking(move || {
                for b in make() {
                    if tx.blocking_send(b).is_err() {
                        break;
                    }
                }
            });
            let batches = futures::stream::unfold(rx, |mut rx| async {
                rx.recv().await.map(|batch| (batch, rx))
            });
            let written = write_shard_stream(
                &writer_props,
                target_file_size,
                &tctx,
                format!("{idx:04}"),
                None,
                batches,
            )
            .await;
            // On a write error the receiver has been dropped, so the
            // blocking producer exits instead of staying parked on send.
            producer.await.context("producer task panicked")?;
            written
        }));
    }
    let mut files = Vec::new();
    for h in handles {
        files.extend(h.await.context("shard task panicked")??);
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every suite's tables can be written directly the way `load` writes
    /// them: partitioned ones through the shared buffer, sorted ones because
    /// their generator emits that order.
    #[test]
    fn every_suite_writes_directly_with_the_load_layout() {
        for suite in [
            "tpch",
            "ssb",
            "tpcds",
            "tpcbb",
            "tpcc",
            "tpce",
            "clickbench",
        ] {
            let gen = crate::generate::get_generator(suite).unwrap();
            for t in gen.tables() {
                assert_eq!(
                    direct_layout_gap(gen.as_ref(), &t.name),
                    None,
                    "{suite}.{}",
                    t.name
                );
            }
        }
    }

    /// A table `load` sorts is refused unless the generator emits exactly
    /// that order, so a new sorted table without a generator order cannot
    /// slip through.
    #[test]
    fn a_load_sort_the_generator_does_not_emit_is_a_gap() {
        assert_eq!(sort_gap(None, None), None);
        assert_eq!(sort_gap(None, Some("d")), None);
        assert_eq!(sort_gap(Some("d"), Some("d")), None);
        assert_eq!(sort_gap(Some("d"), None), Some("d"));
        assert_eq!(sort_gap(Some("d"), Some("other")), Some("d"));
    }

    /// Every partition expression the layout holds builds a spec whose
    /// field names match the engine's CTAS (`l_shipdate_month=1992-01/`,
    /// `ol_w_id=1/ol_d_id=1/` in a loaded warehouse).
    #[test]
    fn partition_specs_build_with_engine_field_names() {
        let mut built = 0;
        for suite in ["tpch", "tpcc"] {
            for t in crate::generate::get_generator(suite).unwrap().tables() {
                let Some(expr) = crate::layout::table_layout(suite, &t.name).partition else {
                    continue;
                };
                let schema = arrow_schema_to_schema_auto_assign_ids(&t.schema).unwrap();
                let spec = partition_spec_from_expr(&schema, expr)
                    .unwrap_or_else(|e| panic!("{suite}.{}: {e:#}", t.name));
                let names: Vec<String> = spec.fields().iter().map(|f| f.name.clone()).collect();
                let want: Vec<String> = expr
                    .split(',')
                    .map(|f| {
                        let f = f.trim();
                        f.strip_prefix("month(")
                            .and_then(|c| c.strip_suffix(')'))
                            .map_or(f.to_string(), |c| format!("{c}_month"))
                    })
                    .collect();
                assert_eq!(names, want, "{suite}.{}", t.name);
                built += 1;
            }
        }
        assert_eq!(built, 10, "2 TPC-H + 8 TPC-C partitioned tables");
        let lineitem = crate::generate::get_generator("tpch")
            .unwrap()
            .tables()
            .into_iter()
            .find(|t| t.name == "lineitem")
            .unwrap();
        let schema = arrow_schema_to_schema_auto_assign_ids(&lineitem.schema).unwrap();
        let spec = partition_spec_from_expr(&schema, "month(l_shipdate)").unwrap();
        assert_eq!(spec.fields()[0].transform, Transform::Month);
        assert!(partition_spec_from_expr(&schema, "no_such_column").is_err());
    }

    /// A direct write blooms the same columns the engine's writer would.
    #[test]
    fn direct_writer_blooms_the_engine_auto_columns() {
        let store_sales = crate::generate::get_generator("tpcds")
            .unwrap()
            .tables()
            .into_iter()
            .find(|t| t.name == "store_sales")
            .unwrap();
        let props = direct_writer_props(&GenerateConfig::default(), &store_sales.schema);
        let bloomed = |col: &str| {
            props
                .bloom_filter_properties(&parquet::schema::types::ColumnPath::new(vec![
                    col.to_string()
                ]))
                .is_some()
        };
        assert!(bloomed("ss_item_sk"));
        assert!(bloomed("ss_sold_date_sk"));
        assert!(!bloomed("ss_quantity"));
    }

    /// The generator's arrow schemas must survive the round trip through
    /// an Iceberg schema unchanged in data types; the sink rebinds batches
    /// to the round-tripped schema, which only works when they agree.
    #[test]
    fn bank_schemas_round_trip_through_iceberg() {
        for name in BANK_TABLES {
            let ours = bank_arrow_schema(name);
            let iceberg = arrow_schema_to_schema(&ours)
                .unwrap_or_else(|e| panic!("{name}: to iceberg failed: {e}"));
            let back = schema_to_arrow_schema(&iceberg)
                .unwrap_or_else(|e| panic!("{name}: back to arrow failed: {e}"));
            assert_eq!(ours.fields().len(), back.fields().len(), "{name}");
            for (a, b) in ours.fields().iter().zip(back.fields().iter()) {
                assert_eq!(a.name(), b.name(), "{name}");
                assert_eq!(a.data_type(), b.data_type(), "{name}.{}", a.name());
                assert_eq!(a.is_nullable(), b.is_nullable(), "{name}.{}", a.name());
            }
        }
    }

    /// Full write-path round trip for every non-bank generator: schema
    /// conversion (create, with auto-assigned field ids), schema-back
    /// conversion (write_schema derivation), and the exact rebind
    /// `write_shard` performs on every generated batch. A schema-only check
    /// is not enough -- Iceberg's type normalization (timestamps forced to
    /// microseconds, dictionary-encoded strings flattened to `Utf8`, etc.)
    /// only surfaces once a real batch is rebound to the round-tripped
    /// schema, which is what this test does.
    #[test]
    fn generic_generators_survive_write_path_round_trip() {
        use crate::generate::{get_generator, GenerateConfig};

        let config = GenerateConfig {
            threads: 1,
            ..GenerateConfig::default()
        };
        for bench in [
            "tpch",
            "ssb",
            "tpcds",
            "tpcc",
            "tpce",
            "tpcbb",
            "clickbench",
        ] {
            let gen = get_generator(bench).unwrap_or_else(|e| panic!("{bench}: {e}"));
            for table_def in gen.tables() {
                let src = gen
                    .generate_batches(&table_def.name, 0.01, &config)
                    .unwrap_or_else(|e| {
                        panic!("{bench}.{}: generate_batches failed: {e}", table_def.name)
                    });
                let shard =
                    src.shards.into_iter().next().unwrap_or_else(|| {
                        panic!("{bench}.{}: no shards produced", table_def.name)
                    });
                let batch = (shard.make)().next().unwrap_or_else(|| {
                    panic!(
                        "{bench}.{}: shard produced zero batches at scale 0.01",
                        table_def.name
                    )
                });

                let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&table_def.schema)
                    .unwrap_or_else(|e| {
                        panic!("{bench}.{}: to iceberg failed: {e}", table_def.name)
                    });
                let round_tripped = schema_to_arrow_schema(&iceberg_schema).unwrap_or_else(|e| {
                    panic!("{bench}.{}: back to arrow failed: {e}", table_def.name)
                });

                RecordBatch::try_new(Arc::new(round_tripped.clone()), batch.columns().to_vec())
                    .unwrap_or_else(|e| {
                        panic!(
                            "{bench}.{}: rebinding first batch to round-tripped schema \
                             failed (this is exactly what write_shard does per-batch): {e}",
                            table_def.name
                        )
                    });

                assert_eq!(
                    table_def.schema.fields().len(),
                    round_tripped.fields().len(),
                    "{bench}.{}: field count mismatch after round trip",
                    table_def.name
                );
                for (a, b) in table_def
                    .schema
                    .fields()
                    .iter()
                    .zip(round_tripped.fields().iter())
                {
                    assert_eq!(
                        a.name(),
                        b.name(),
                        "{bench}.{}: field name mismatch after round trip",
                        table_def.name
                    );
                    assert_eq!(
                        a.data_type(),
                        b.data_type(),
                        "{bench}.{}.{}: data type mismatch after round trip",
                        table_def.name,
                        a.name()
                    );
                }
            }
        }
    }

    #[test]
    fn day_format_and_parse_round_trip() {
        assert_eq!(format_day(0), "1970-01-01");
        let d = parse_day("2026-06-01").unwrap();
        assert_eq!(format_day(d), "2026-06-01");
        assert_eq!(d, bank::DEFAULT_START_DAY);
        assert!(parse_day("junk").is_err());
    }

    #[test]
    fn small_table_shards_cover_rows() {
        let shards = small_table_shards(10_000_000);
        assert_eq!(shards.len(), 3);
        assert_eq!(shards.first().unwrap().start, 0);
        assert_eq!(shards.last().unwrap().end, 10_000_000);
        let total: u64 = shards.iter().map(|r| r.end - r.start).sum();
        assert_eq!(total, 10_000_000);

        assert_eq!(small_table_shards(1).len(), 1);
    }

    #[test]
    fn day_props_sets_snapshot_and_table_markers() {
        let (snap, tprops) = day_props(Some(0));
        assert!(snap.contains_key(SNAPSHOT_DAY_PROP));
        assert!(tprops.keys().any(|k| k.starts_with(DAY_PROP_PREFIX)));
        let (snap0, tprops0) = day_props(None);
        assert!(snap0.is_empty() && tprops0.is_empty());
    }

    #[test]
    fn unit_prefixes_are_distinct_and_stable() {
        let a = Unit {
            table: "transaction",
            day_idx: Some(3),
            shard_idx: 7,
            rows: 0..10,
            accounts: 0..10,
        };
        assert_eq!(a.file_prefix(), "transaction-d0003-s0007");
        let b = Unit {
            table: "customer",
            day_idx: None,
            shard_idx: 0,
            rows: 0..10,
            accounts: 0..0,
        };
        assert_eq!(b.file_prefix(), "customer-s0000");
    }

    /// `run_direct` names each shard's data files `{idx:04}`; this documents
    /// the invariant that shard prefixes are disjoint and match the staging
    /// `{part_idx:04}` convention, so a re-run overwrites its own partial
    /// output rather than colliding with another shard's files.
    #[test]
    fn shard_prefix_is_zero_padded() {
        assert_eq!(format!("{:04}", 3usize), "0003");
        assert_eq!(format!("{:04}", 12usize), "0012");
    }
}
