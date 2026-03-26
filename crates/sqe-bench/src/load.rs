use crate::client::BenchClient;
use crate::generate;

pub struct S3Args {
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub endpoint: Option<String>,
    pub region: String,
}

pub async fn load_benchmark(
    client: &dyn BenchClient,
    benchmark: &str,
    scale: f64,
    data_path: &str,
    s3_args: &S3Args,
    clean: bool,
    catalog: Option<&str>,
    namespace_override: Option<&str>,
) -> anyhow::Result<()> {
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

        println!("  Loading {}.{}...", qualified_ns, table_def.name);
        client.execute_update(&sql).await?;
        println!("  Done: {}.{}", qualified_ns, table_def.name);
    }

    println!("Done. All tables loaded into {qualified_ns}.");
    Ok(())
}
