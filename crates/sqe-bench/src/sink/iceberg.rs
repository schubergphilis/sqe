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
use iceberg::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
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
use iceberg_catalog_rest::{RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
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
    fn catalog_props(&self) -> HashMap<String, String> {
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
                let t_id_start =
                    d as i64 * plan.txn_rows_per_day as i64 + self.rows.start as i64;
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

/// Create the namespace and any missing bank tables; return handles.
async fn ensure_tables(
    catalog: &dyn Catalog,
    namespace: &str,
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
        let ident = TableIdent::new(ns.clone(), name.to_string());
        let table = if catalog.table_exists(&ident).await? {
            catalog.load_table(&ident).await?
        } else {
            let arrow_schema = bank_arrow_schema(name);
            let schema = arrow_schema_to_schema(&arrow_schema)
                .with_context(|| format!("converting {name} schema"))?;
            let creation = if let Some(col) = bank::partition_column(name) {
                let spec = PartitionSpec::builder(schema.clone())
                    .add_partition_field(col, col, Transform::Identity)
                    .and_then(|b| b.build())
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
                .create_table(&ns, creation)
                .await
                .with_context(|| format!("creating table {name}"))?;
            println!("Created table {namespace}.{name}");
            table
        };
        let write_schema = Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema())
                .with_context(|| format!("deriving {name} write schema"))?,
        );
        tables.insert(name, TableCtx {
            table,
            write_schema,
        });
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

    let location_gen = DefaultLocationGenerator::new(table.metadata().clone())
        .context("building location generator")?;
    // Deterministic names: a re-run of the same unit produces the same
    // file names and overwrites its own partial output from a crashed run.
    let file_name_gen = DefaultFileNameGenerator::new(
        unit.file_prefix(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        ctx.writer_props.clone(),
        table.metadata().current_schema().clone(),
    );
    let rolling = RollingFileWriterBuilder::new(
        parquet_builder,
        ctx.target_file_size,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );
    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(partition_key)
        .await
        .context("building data file writer")?;

    for batch in unit.batches(&ctx.plan) {
        // Rebind to the table-derived arrow schema: column data types are
        // identical, only field metadata (field ids) may differ in detail.
        let batch = RecordBatch::try_new(tctx.write_schema.clone(), batch.columns().to_vec())
            .context("rebinding batch to table schema")?;
        writer.write(batch).await.context("writing batch")?;
    }
    writer.close().await.context("closing writer")
}

/// Commit `files` to `table_name` as one fast-append snapshot, tagging it
/// with the day marker when the commit is for one trading day.
async fn commit_files(
    ctx: &RunCtx,
    table_name: &'static str,
    day: Option<i32>,
    files: Vec<DataFile>,
) -> anyhow::Result<()> {
    if files.is_empty() {
        return Ok(());
    }
    let _guard = ctx.commit_lock.lock().await;
    // Reload so the transaction bases on the latest metadata; commits from
    // other days may have landed since this handle was created.
    let table = ctx
        .catalog
        .load_table(ctx.tables[table_name].table.identifier())
        .await
        .context("reloading table before commit")?;
    let tx = Transaction::new(&table);
    let mut append = tx.fast_append().add_data_files(files);
    if let Some(day) = day {
        append = append.set_snapshot_properties(HashMap::from([(
            SNAPSHOT_DAY_PROP.to_string(),
            format_day(day),
        )]));
    }
    let mut tx = append.apply(tx).context("applying append")?;
    if let Some(day) = day {
        // Durable resume marker; committed atomically with the append.
        tx = tx
            .update_table_properties()
            .set(format!("{DAY_PROP_PREFIX}{}", format_day(day)), "done".to_string())
            .apply(tx)
            .context("applying day property")?;
    }
    tx.commit(ctx.catalog.as_ref())
        .await
        .context("committing append")?;
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
    commit_files(&ctx, table_name, day, files).await?;
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
        .load("polaris", target.catalog_props())
        .await
        .context("connecting to catalog")?;
    let catalog: Box<dyn Catalog> = Box::new(catalog);

    let tables = ensure_tables(catalog.as_ref(), &target.namespace).await?;

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

    let mut props_builder = WriterProperties::builder()
        .set_compression(config.compression.to_parquet());
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

    let mut groups: Vec<(
        &'static str,
        Option<i32>,
        Vec<Unit>,
    )> = Vec::new();

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
