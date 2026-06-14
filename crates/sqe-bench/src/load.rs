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

        // Build CTAS with read_parquet
        let mut sql = format!(
            "CREATE TABLE {qualified_ns}.{} AS SELECT * FROM read_parquet('{}/*.parquet'",
            table_def.name, table_path
        );

        // Append S3 credentials if provided
        if let Some(ref key) = s3_args.access_key {
            sql.push_str(&format!(", access_key => '{key}'"));
        }
        if let Some(ref key) = s3_args.secret_key {
            sql.push_str(&format!(", secret_key => '{key}'"));
        }
        if let Some(ref ep) = s3_args.endpoint {
            sql.push_str(&format!(", endpoint => '{ep}'"));
        }
        sql.push_str(&format!(", region => '{}'", s3_args.region));
        sql.push(')');

        // Sort-on-write: cluster fact tables by their date/range key so
        // row-group zone maps are tight and the reader can prune row groups.
        if let Some(key) = clustering_key(benchmark, &table_def.name) {
            sql.push_str(&format!(" ORDER BY {key}"));
        }

        println!("  Loading {}.{}...", qualified_ns, table_def.name);
        client.execute_update(&sql).await?;
        println!("  Done: {}.{}", qualified_ns, table_def.name);
    }

    println!("Done. All tables loaded into {qualified_ns}.");
    Ok(())
}
