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
) -> anyhow::Result<()> {
    let namespace = crate::bench_namespace(benchmark, scale);
    let gen = generate::get_generator(benchmark)?;

    println!("Loading {benchmark} SF{scale} into namespace {namespace}");

    // Create namespace (ignore error if exists)
    let _ = client
        .execute_update(&format!("CREATE SCHEMA IF NOT EXISTS {namespace}"))
        .await;

    for table_def in gen.tables() {
        let table_path = format!(
            "{data_path}/{benchmark}/sf{scale}/{}",
            table_def.name
        );

        if clean {
            let _ = client
                .execute_update(&format!(
                    "DROP TABLE IF EXISTS {namespace}.{}",
                    table_def.name
                ))
                .await;
        }

        // Build CTAS with read_parquet
        let mut sql = format!(
            "CREATE TABLE {namespace}.{} AS SELECT * FROM read_parquet('{}/*.parquet'",
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

        println!("  Loading {}.{}...", namespace, table_def.name);
        client.execute_update(&sql).await?;
        println!("  Done: {}.{}", namespace, table_def.name);
    }

    println!("Done. All tables loaded into {namespace}.");
    Ok(())
}
