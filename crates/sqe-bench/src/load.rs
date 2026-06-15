use crate::client::BenchClient;
use crate::generate;

pub struct S3Args {
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub endpoint: Option<String>,
    pub region: String,
}

pub struct LoadArgs<'a> {
    pub benchmark: &'a str,
    pub scale: f64,
    pub data_path: &'a str,
    pub s3_args: &'a S3Args,
    pub clean: bool,
    pub catalog: Option<&'a str>,
    pub namespace_override: Option<&'a str>,
}

/// Sort-on-write clustering key for a fact table.
///
/// Loads sort fact tables by their dominant date/range key so each Parquet
/// row group gets a tight min/max on that column. Most analytical queries
/// filter or range on the date key (directly or via a dim join whose dynamic
/// filter pushes a contiguous date-key set to the fact scan), so tight zone
/// maps let the reader prune whole row groups instead of decoding them
/// (`files_pruned_minmax` was 0 on unsorted loads). Dimensions are small and
/// left unsorted. Cross-engine fair: Trino reads the same sorted files.
///
/// Returns the column to `ORDER BY` in the load CTAS, or `None` to load as-is.
fn clustering_key(benchmark: &str, table: &str) -> Option<&'static str> {
    match (benchmark, table) {
        ("tpch", "lineitem") => Some("l_shipdate"),
        ("tpch", "orders") => Some("o_orderdate"),
        ("ssb", "lineorder") => Some("lo_orderdate"),
        ("tpcds" | "tpcbb", "store_sales") => Some("ss_sold_date_sk"),
        ("tpcds" | "tpcbb", "catalog_sales") => Some("cs_sold_date_sk"),
        ("tpcds" | "tpcbb", "web_sales") => Some("ws_sold_date_sk"),
        ("tpcds" | "tpcbb", "inventory") => Some("inv_date_sk"),
        ("tpcds" | "tpcbb", "store_returns") => Some("sr_returned_date_sk"),
        ("tpcds" | "tpcbb", "catalog_returns") => Some("cr_returned_date_sk"),
        ("tpcds" | "tpcbb", "web_returns") => Some("wr_returned_date_sk"),
        _ => None,
    }
}

/// Iceberg partition spec (transform expression) for a fact table's `PARTITIONED
/// BY` clause, or `None` to leave it unpartitioned. Partitioning enables
/// manifest-level file pruning (stronger than row-group min/max). Only
/// date-typed columns get `month()`; integer surrogate date keys (TPC-DS/SSB)
/// need bucket/truncate and are left for a later pass.
fn partition_spec(benchmark: &str, table: &str) -> Option<&'static str> {
    match (benchmark, table) {
        ("tpch", "lineitem") => Some("month(l_shipdate)"),
        ("tpch", "orders") => Some("month(o_orderdate)"),
        _ => None,
    }
}

/// True if `err` is a memory/resource-exhaustion failure (as opposed to a SQL
/// or transport error). Used to decide whether a failed sort-on-write CTAS
/// should fail over to an unsorted write.
fn is_resource_exhausted(err: &anyhow::Error) -> bool {
    let m = err.to_string().to_ascii_lowercase();
    m.contains("resources exhausted")
        || m.contains("failed to allocate")
        || m.contains("out of memory")
        || m.contains("memory limit")
}

pub async fn load_benchmark(
    client: &dyn BenchClient,
    args: &LoadArgs<'_>,
) -> anyhow::Result<()> {
    let benchmark = args.benchmark;
    let scale = args.scale;
    let data_path = args.data_path;
    let s3_args = args.s3_args;
    let clean = args.clean;
    let catalog = args.catalog;
    let namespace_override = args.namespace_override;

    // Build the namespace: user override > auto-generated
    let ns_base = match namespace_override {
        Some(ns) => ns.to_string(),
        None if benchmark == "tpcbb" => crate::bench_namespace("tpcds", scale),
        None => crate::bench_namespace(benchmark, scale),
    };

    // Full qualified prefix: catalog.namespace or just namespace
    let qualified_ns = match catalog {
        Some(cat) => format!("{cat}.{ns_base}"),
        None => ns_base.clone(),
    };

    let gen = generate::get_generator(benchmark)?;

    println!("Loading {benchmark} SF{scale} into {qualified_ns}");

    // Create namespace (ignore error if exists)
    let _ = client
        .execute_update(&format!("CREATE SCHEMA IF NOT EXISTS {qualified_ns}"))
        .await;

    for table_def in gen.tables() {
        let table_path = format!(
            "{data_path}/{benchmark}/sf{scale}/{}",
            table_def.name
        );

        if clean {
            let _ = client
                .execute_update(&format!(
                    "DROP TABLE IF EXISTS {qualified_ns}.{}",
                    table_def.name
                ))
                .await;
        }

        // Build the base CTAS: CREATE TABLE [PARTITIONED BY (...)] AS SELECT
        // * FROM read_parquet(...). Partitioning (coarse, manifest-level file
        // pruning) is part of the table definition; clustering (ORDER BY, the
        // sort-on-write hint below) is appended per the failover path.
        let mut base_sql = format!("CREATE TABLE {qualified_ns}.{}", table_def.name);
        if let Some(spec) = partition_spec(benchmark, &table_def.name) {
            base_sql.push_str(&format!(" PARTITIONED BY ({spec})"));
        }
        base_sql.push_str(&format!(
            " AS SELECT * FROM read_parquet('{}/*.parquet'",
            table_path
        ));

        // Append S3 credentials if provided
        if let Some(ref key) = s3_args.access_key {
            base_sql.push_str(&format!(", access_key => '{key}'"));
        }
        if let Some(ref key) = s3_args.secret_key {
            base_sql.push_str(&format!(", secret_key => '{key}'"));
        }
        if let Some(ref ep) = s3_args.endpoint {
            base_sql.push_str(&format!(", endpoint => '{ep}'"));
        }
        base_sql.push_str(&format!(", region => '{}'", s3_args.region));
        base_sql.push(')');

        println!("  Loading {}.{}...", qualified_ns, table_def.name);

        // Sort-on-write: cluster fact tables by their date/range key so
        // row-group zone maps are tight and the reader can prune row groups.
        // The sort is an optimization, not a correctness requirement, so if it
        // exhausts memory (DataFusion's sort-merge cannot spill and OOMs rather
        // than degrading) we fail over to an unsorted write -- the data lands
        // correctly, just without clustering. A user's own CTAS ORDER BY is
        // never touched; this fallback applies only to the bench clustering hint.
        match clustering_key(benchmark, &table_def.name) {
            Some(key) => {
                let sorted_sql = format!("{base_sql} ORDER BY {key}");
                if let Err(e) = client.execute_update(&sorted_sql).await {
                    if is_resource_exhausted(&e) {
                        eprintln!(
                            "  ! sort-on-write OOM on {}.{} (key {key}); failing over to unsorted write",
                            qualified_ns, table_def.name
                        );
                        // Drop the partial table the failed sort may have left.
                        let _ = client
                            .execute_update(&format!(
                                "DROP TABLE IF EXISTS {qualified_ns}.{}",
                                table_def.name
                            ))
                            .await;
                        client.execute_update(&base_sql).await?;
                    } else {
                        return Err(e);
                    }
                }
            }
            None => {
                client.execute_update(&base_sql).await?;
            }
        }
        println!("  Done: {}.{}", qualified_ns, table_def.name);
    }

    println!("Done. All tables loaded into {qualified_ns}.");
    Ok(())
}
