use clap::{Parser, Subcommand, ValueEnum};

use crate::query::QualifyTables;
use crate::{client, comparison, generate, load, report, run, sink, suite};

#[derive(Parser)]
#[command(
    name = "sqe-bench",
    version,
    about = "SQE benchmark data generator, loader, and query tester"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Generate Parquet data files for a benchmark suite
    Generate {
        /// Benchmark suite to generate (tpch, ssb, tpcds, tpcc, tpce, tpcbb, clickbench)
        #[arg(value_name = "BENCHMARK")]
        benchmark: String,

        /// Scale factor (e.g. 1 = 1 GB for TPC-H, 10 = 10x)
        #[arg(long, default_value_t = 1.0)]
        scale: f64,

        /// Output directory for Parquet files
        #[arg(long, default_value = "data")]
        output: String,

        /// S3 endpoint URL (if writing directly to object storage)
        #[arg(long, env = "AWS_ENDPOINT_URL")]
        s3_endpoint: Option<String>,

        /// S3 access key ID
        #[arg(long, env = "AWS_ACCESS_KEY_ID")]
        s3_access_key: Option<String>,

        /// S3 secret access key
        #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
        s3_secret_key: Option<String>,

        /// S3 bucket name (required when writing to S3)
        #[arg(long, env = "BENCH_S3_BUCKET")]
        s3_bucket: Option<String>,

        /// S3 region
        #[arg(long, env = "AWS_DEFAULT_REGION", default_value = "us-east-1")]
        s3_region: String,

        /// Worker threads for parallel per-partition generation. Default
        /// is `std::thread::available_parallelism()`, clamped to [1, 256].
        /// Overrides the `BENCH_GEN_THREADS` env var when set.
        #[arg(long)]
        threads: Option<usize>,

        /// Parquet compression codec: `zstd1`, `zstd3` (default), `zstd9`,
        /// `snappy`, or `none`. Overrides `BENCH_GEN_COMPRESSION`.
        #[arg(long)]
        compression: Option<String>,

        /// Max rows per parquet row group. Overrides `BENCH_GEN_ROW_GROUP_SIZE`.
        /// Default is the parquet writer's default.
        #[arg(long)]
        row_group_size: Option<usize>,

        /// Output sink: `parquet` writes staging files to --output (the
        /// default; load them later with `sqe-bench load`); `iceberg`
        /// writes straight into Iceberg tables through the catalog REST
        /// API for any benchmark.
        #[arg(long, default_value = "parquet")]
        sink: Sink,

        /// Trading days to generate (iceberg sink)
        #[arg(long, default_value_t = 12u32)]
        days: u32,

        /// First trading day, YYYY-MM-DD (iceberg sink)
        #[arg(long)]
        start_date: Option<String>,

        /// Compressed bytes to land per trading day, e.g. `4t` or `500g`.
        /// Rows per day are derived from a pilot calibration. Mutually
        /// exclusive with --rows-per-day (iceberg sink)
        #[arg(long, conflicts_with = "rows_per_day")]
        bytes_per_day: Option<String>,

        /// Explicit transaction rows per trading day (iceberg sink)
        #[arg(long)]
        rows_per_day: Option<u64>,

        /// Number of customers; accounts are 2.5x this (iceberg sink)
        #[arg(long, default_value_t = 10_000_000u64)]
        customers: u64,

        /// Iceberg REST catalog URI, e.g. http://localhost:8181/api/catalog
        /// (iceberg sink)
        #[arg(long, env = "ICEBERG_CATALOG_URI")]
        catalog_uri: Option<String>,

        /// Warehouse name in the catalog (iceberg sink)
        #[arg(long, env = "ICEBERG_WAREHOUSE")]
        warehouse: Option<String>,

        /// Namespace for the tables (iceberg sink)
        #[arg(long, env = "SQE_NAMESPACE", default_value = "bank")]
        namespace: String,

        /// OAuth2 client id for the catalog's client-credentials grant
        /// (iceberg sink)
        #[arg(long, env = "SQE_CLIENT_ID")]
        client_id: Option<String>,

        /// OAuth2 client secret (iceberg sink)
        #[arg(long, env = "SQE_CLIENT_SECRET")]
        client_secret: Option<String>,

        /// OAuth2 token endpoint; defaults to the catalog's own
        /// /v1/oauth/tokens (iceberg sink)
        #[arg(long, env = "SQE_TOKEN_ENDPOINT")]
        oauth2_server_uri: Option<String>,

        /// OAuth2 scope (iceberg sink)
        #[arg(long)]
        scope: Option<String>,

        /// Pre-acquired bearer token, alternative to client credentials
        /// (iceberg sink)
        #[arg(long, env = "ICEBERG_BEARER_TOKEN")]
        bearer_token: Option<String>,

        /// Use path-style S3 addressing (MinIO/RustFS style endpoints)
        /// (iceberg sink)
        #[arg(long, default_value_t = false)]
        s3_path_style: bool,

        /// Data file rollover size, e.g. `512m` (iceberg sink)
        #[arg(long, default_value = "512m")]
        target_file_size: String,

        /// Print the sized plan (calibration included) and exit without
        /// writing any table data (iceberg sink)
        #[arg(long, default_value_t = false)]
        dry_run: bool,

        /// Skip days that already have a committed snapshot; use after an
        /// interrupted run (iceberg sink)
        #[arg(long, default_value_t = false)]
        resume: bool,

        /// Drop and recreate the bank tables before loading, making the
        /// run idempotent (iceberg sink)
        #[arg(long, default_value_t = false, conflicts_with = "resume")]
        clean: bool,
    },

    /// Load generated data into SQE via Iceberg REST catalog
    Load {
        /// Benchmark suite to load (tpch, ssb, tpcds, tpcc, tpce, tpcbb, clickbench)
        #[arg(value_name = "BENCHMARK")]
        benchmark: String,

        /// Scale factor used when generating the data
        #[arg(long, default_value_t = 1.0)]
        scale: f64,

        /// Directory containing generated Parquet files
        #[arg(long, default_value = "data")]
        data: String,

        /// Wire protocol to use for loading
        #[arg(long, default_value = "flight")]
        protocol: Protocol,

        /// Coordinator host
        #[arg(long, default_value = "localhost")]
        host: String,

        /// Coordinator port
        #[arg(long, default_value_t = 50051u16)]
        port: u16,

        /// Catalog name (e.g. main_warehouse). If set, tables are created as
        /// <catalog>.<namespace>.<table> instead of <namespace>.<table>.
        #[arg(long, env = "SQE_CATALOG")]
        catalog: Option<String>,

        /// Override the auto-generated namespace (default: <benchmark>_sf<scale>)
        #[arg(long, env = "SQE_NAMESPACE")]
        namespace: Option<String>,

        /// Drop and recreate tables before loading
        #[arg(long, default_value_t = false)]
        clean: bool,

        /// S3 endpoint URL
        #[arg(long, env = "AWS_ENDPOINT_URL")]
        s3_endpoint: Option<String>,

        /// S3 access key ID
        #[arg(long, env = "AWS_ACCESS_KEY_ID")]
        s3_access_key: Option<String>,

        /// S3 secret access key
        #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
        s3_secret_key: Option<String>,

        /// S3 bucket name
        #[arg(long, env = "BENCH_S3_BUCKET")]
        s3_bucket: Option<String>,

        /// S3 region
        #[arg(long, env = "AWS_DEFAULT_REGION", default_value = "us-east-1")]
        s3_region: String,

        /// Username for authentication (OIDC password grant)
        #[arg(long, env = "SQE_USER")]
        username: Option<String>,

        /// Password for authentication (OIDC password grant)
        #[arg(long, env = "SQE_PASSWORD")]
        password: Option<String>,

        /// OAuth2 token endpoint for client_credentials auth
        #[arg(long, env = "SQE_TOKEN_ENDPOINT")]
        token_endpoint: Option<String>,

        /// OAuth2 client ID
        #[arg(long, env = "SQE_CLIENT_ID")]
        client_id: Option<String>,

        /// OAuth2 client secret
        #[arg(long, env = "SQE_CLIENT_SECRET")]
        client_secret: Option<String>,

        /// Emit Parquet bloom filters on join-key columns of the loaded
        /// tables (TPC-H/SSB) via the `write.parquet.bloom-filter-columns`
        /// table property. Off by default so baselines stay comparable.
        /// A/B lever: generate once, load twice (off vs on) for a clean
        /// comparison on identical data. (The benchmark scripts translate
        /// `BENCH_BLOOM_FILTER=1` into this flag.)
        #[arg(long, default_value_t = false)]
        bloom_filter: bool,

        /// Write Puffin NDV sidecars (`write.puffin.stats = 'true'`) for the
        /// loaded tables so the coordinator's `query.puffin_statistics`
        /// experiment has distinct-count estimates to plan from. Off by
        /// default so baselines stay comparable. (The benchmark scripts
        /// translate `BENCH_PUFFIN_STATS=1` into this flag.)
        #[arg(long, default_value_t = false)]
        puffin_stats: bool,

        /// Load as Hive external tables instead of Iceberg: each table is a
        /// Hive CTAS (`STORED AS PARQUET LOCATION '<this>/<ns>/<table>/'`)
        /// under the Hive root named by `--catalog`. The prefix must be
        /// empty: Hive `DROP TABLE` never deletes data, so the caller clears
        /// it (scripts/benchmark-test.sh with BENCH_TABLE_FORMAT=hive).
        #[arg(long)]
        hive_location: Option<String>,
    },

    /// Run benchmark queries and report timing
    Test {
        /// Benchmark suite to test (tpch, ssb, tpcds, tpcc, tpce, tpcbb, clickbench)
        #[arg(value_name = "BENCHMARK")]
        benchmark: String,

        /// Scale factor (informational, used in result metadata)
        #[arg(long, default_value_t = 1.0)]
        scale: f64,

        /// Wire protocol to use for queries
        #[arg(long, default_value = "flight")]
        protocol: Protocol,

        /// Coordinator host
        #[arg(long, default_value = "localhost")]
        host: String,

        /// Coordinator port
        #[arg(long, default_value_t = 50051u16)]
        port: u16,

        /// Run only the specified query (e.g. "q1" or "1"); runs all if omitted
        #[arg(long)]
        query: Option<String>,

        /// Catalog name (must match the value used during load)
        #[arg(long, env = "SQE_CATALOG")]
        catalog: Option<String>,

        /// Override namespace (must match the value used during load)
        #[arg(long, env = "SQE_NAMESPACE")]
        namespace: Option<String>,

        /// Username for authentication (OIDC password grant)
        #[arg(long, env = "SQE_USER")]
        username: Option<String>,

        /// Password for authentication (OIDC password grant)
        #[arg(long, env = "SQE_PASSWORD")]
        password: Option<String>,

        /// OAuth2 token endpoint for client_credentials auth
        #[arg(long, env = "SQE_TOKEN_ENDPOINT")]
        token_endpoint: Option<String>,

        /// OAuth2 client ID
        #[arg(long, env = "SQE_CLIENT_ID")]
        client_id: Option<String>,

        /// OAuth2 client secret
        #[arg(long, env = "SQE_CLIENT_SECRET")]
        client_secret: Option<String>,

        /// How bare table names in the query files reach the engine. `auto`
        /// (default) sends the text untouched when the connection carries a
        /// default schema (Trino protocol: `X-Trino-Schema`) and qualifies
        /// names with the namespace otherwise; `always` / `never` force it.
        #[arg(long, value_enum, default_value_t = QualifyTables::Auto)]
        qualify_tables: QualifyTables,

        /// Suffix for the suite name in the report: `hive` writes
        /// `tpch-hive-sf1-flight-*.json`, so a Hive run never counts as an
        /// Iceberg baseline for the same suite and scale.
        #[arg(long)]
        result_label: Option<String>,
    },

    /// Run one or more benchmark suites against a running coordinator using a
    /// config profile: ATTACH the golden catalog, run queries with zero load,
    /// emit BENCH_SUMMARY + JSON, and optionally compare against Trino.
    Run {
        /// Benchmark suites to run (tpch, ssb, tpcds, clickbench, ...).
        #[arg(value_name = "SUITE", required = true)]
        suites: Vec<String>,

        /// Config profile: `local` | `storagegrid` | `r2` | `aws` | a .toml path.
        #[arg(long, default_value = "local")]
        profile: String,

        /// Scale factor of the golden dataset (must match what was provisioned).
        #[arg(long, default_value_t = 1.0)]
        scale: f64,

        /// Coordinator host to connect to.
        #[arg(long, default_value = "localhost")]
        host: String,

        /// Coordinator Flight SQL port.
        #[arg(long, default_value_t = 60051)]
        port: u16,

        /// Also run the Trino comparison and emit compare-*.json.
        #[arg(long)]
        compare_trino: bool,

        /// Run only a single query id (e.g. `q05`).
        #[arg(long)]
        query: Option<String>,

        /// See `test --qualify-tables`.
        #[arg(long, value_enum, default_value_t = QualifyTables::Auto)]
        qualify_tables: QualifyTables,
    },

    /// Compare SQE vs Trino: run identical benchmark queries against both and diff results
    Compare {
        /// Benchmark suite (tpch, tpcds, ssb)
        #[arg(value_name = "BENCHMARK")]
        benchmark: String,

        /// Scale factor
        #[arg(long, default_value_t = 1.0)]
        scale: f64,

        /// SQE Flight SQL host
        #[arg(long, default_value = "localhost")]
        sqe_host: String,

        /// SQE Flight SQL port
        #[arg(long, default_value_t = 50051u16)]
        sqe_port: u16,

        /// SQE auth username
        #[arg(long, env = "SQE_USER")]
        sqe_username: Option<String>,

        /// SQE auth password
        #[arg(long, env = "SQE_PASSWORD")]
        sqe_password: Option<String>,

        /// OAuth2 token endpoint for client_credentials auth
        #[arg(long, env = "SQE_TOKEN_ENDPOINT")]
        token_endpoint: Option<String>,

        /// OAuth2 client ID
        #[arg(long, env = "SQE_CLIENT_ID")]
        client_id: Option<String>,

        /// OAuth2 client secret
        #[arg(long, env = "SQE_CLIENT_SECRET")]
        client_secret: Option<String>,

        /// Trino HTTP URL (e.g., http://localhost:8080)
        #[arg(long)]
        trino_url: String,

        /// Trino user
        #[arg(long, default_value = "admin")]
        trino_user: String,

        /// Trino catalog (default: iceberg)
        #[arg(long)]
        trino_catalog: Option<String>,

        /// Trino schema (default: same as benchmark namespace)
        #[arg(long)]
        trino_schema: Option<String>,

        /// Single query to compare (e.g., "q1" or "1")
        #[arg(long)]
        query: Option<String>,

        /// Output directory for comparison report
        #[arg(long, default_value = "benchmarks/results")]
        output: String,

        /// See `test --qualify-tables`.
        #[arg(long, value_enum, default_value_t = QualifyTables::Auto)]
        qualify_tables: QualifyTables,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Sink {
    /// Local (or staged) Parquet files for a later `sqe-bench load`
    Parquet,
    /// Direct write + commit into Iceberg tables via the REST catalog
    Iceberg,
}

#[derive(Clone, ValueEnum)]
pub enum Protocol {
    /// Arrow Flight SQL (gRPC/HTTP2)
    Flight,
    /// Trino-compat HTTP REST
    Http,
}

impl Protocol {
    fn as_str(&self) -> &'static str {
        match self {
            Protocol::Flight => "flight",
            Protocol::Http => "trino",
        }
    }
}

/// Parse the command line and run the selected verb. This is the whole
/// binary; `src/main.rs` only supplies the runtime.
pub async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Generate {
            benchmark,
            scale,
            output,
            threads,
            compression,
            row_group_size,
            sink,
            days,
            start_date,
            bytes_per_day,
            rows_per_day,
            customers,
            catalog_uri,
            warehouse,
            namespace,
            client_id,
            client_secret,
            oauth2_server_uri,
            scope,
            bearer_token,
            s3_endpoint,
            s3_access_key,
            s3_secret_key,
            s3_region,
            s3_path_style,
            target_file_size,
            dry_run,
            resume,
            clean,
            ..
        } => {
            let config =
                generate::GenerateConfig::resolve(threads, compression.as_deref(), row_group_size)?;

            if let Sink::Iceberg = sink {
                if benchmark == "bank" {
                    anyhow::ensure!(
                        bytes_per_day.is_some() || rows_per_day.is_some(),
                        "--sink iceberg needs --bytes-per-day (e.g. 4t) or --rows-per-day"
                    );
                    let start_day = match start_date.as_deref() {
                        Some(s) => sink::iceberg::parse_day(s)?,
                        None => generate::bank::DEFAULT_START_DAY,
                    };
                    let mut plan = generate::bank::BankPlan {
                        customers,
                        start_day,
                        days,
                        txn_rows_per_day: rows_per_day.unwrap_or(0),
                    };
                    let target_file_size = sink::plan::parse_size(&target_file_size)? as usize;

                    println!("Calibrating transaction bytes/row (pilot shard)...");
                    let calibration = sink::plan::calibrate(&config, plan.accounts())?;
                    let mut run_plan = match bytes_per_day.as_deref() {
                        Some(b) => sink::plan::RunPlan::from_bytes_per_day(
                            sink::plan::parse_size(b)?,
                            plan,
                            calibration,
                            target_file_size,
                            resume,
                        ),
                        None => {
                            plan.txn_rows_per_day = rows_per_day.unwrap_or(1).max(1);
                            sink::plan::RunPlan::from_rows_per_day(
                                plan,
                                calibration,
                                target_file_size,
                                resume,
                            )
                        }
                    };
                    run_plan.spec.clean = clean;
                    run_plan.print(config.threads);
                    if dry_run {
                        println!("--dry-run: nothing written.");
                        return Ok(());
                    }
                    let catalog_uri = catalog_uri
                        .ok_or_else(|| anyhow::anyhow!("--sink iceberg needs --catalog-uri"))?;
                    let warehouse = warehouse
                        .ok_or_else(|| anyhow::anyhow!("--sink iceberg needs --warehouse"))?;

                    let target = sink::iceberg::IcebergTarget {
                        catalog_uri,
                        warehouse,
                        namespace,
                        credential: match (client_id, client_secret) {
                            (Some(id), Some(secret)) => Some(format!("{id}:{secret}")),
                            _ => None,
                        },
                        oauth2_server_uri,
                        scope,
                        bearer_token,
                        s3_endpoint,
                        s3_access_key,
                        s3_secret_key,
                        s3_region: Some(s3_region),
                        s3_path_style,
                    };
                    return sink::iceberg::run_bank(&target, &run_plan.spec, &config).await;
                } else {
                    let catalog_uri = catalog_uri
                        .ok_or_else(|| anyhow::anyhow!("--sink iceberg needs --catalog-uri"))?;
                    let warehouse = warehouse
                        .ok_or_else(|| anyhow::anyhow!("--sink iceberg needs --warehouse"))?;

                    let target = sink::iceberg::IcebergTarget {
                        catalog_uri,
                        warehouse,
                        namespace,
                        credential: match (client_id, client_secret) {
                            (Some(id), Some(secret)) => Some(format!("{id}:{secret}")),
                            _ => None,
                        },
                        oauth2_server_uri,
                        scope,
                        bearer_token,
                        s3_endpoint,
                        s3_access_key,
                        s3_secret_key,
                        s3_region: Some(s3_region),
                        s3_path_style,
                    };
                    let file_size = sink::plan::parse_size(&target_file_size)? as usize;
                    let gen = generate::get_generator(&benchmark)?;
                    return sink::iceberg::run_direct(
                        &target,
                        gen.as_ref(),
                        scale,
                        &config,
                        clean,
                        resume,
                        file_size,
                    )
                    .await;
                }
            }

            let gen = generate::get_generator(&benchmark)?;
            println!(
                "Generating {benchmark} (threads={}, compression={:?}, row_group_size={:?})",
                config.threads, config.compression, config.row_group_size,
            );
            for table_def in gen.tables() {
                println!("Generating {}.{}...", benchmark, table_def.name);
                let stats = gen.generate_table(&table_def.name, scale, &output, &config)?;
                println!(
                    "  {} rows, {} files, {:.1}s",
                    stats.rows,
                    stats.files,
                    stats.duration.as_secs_f64()
                );
            }
            println!("Done.");
            Ok(())
        }

        Command::Load {
            benchmark,
            scale,
            data,
            protocol,
            host,
            port,
            clean,
            s3_endpoint,
            s3_access_key,
            s3_secret_key,
            s3_region,
            username,
            password,
            token_endpoint,
            client_id,
            client_secret,
            catalog,
            namespace,
            bloom_filter,
            puffin_stats,
            hive_location,
            ..
        } => {
            let protocol_str = protocol.as_str();
            let endpoint = format!("http://{host}:{port}");
            if std::env::var("BENCH_DEBUG").is_ok() {
                eprintln!("[sqe-bench] connecting to {endpoint} via {protocol_str}...");
            }
            let ns = crate::suite_namespace(&benchmark, scale, namespace.as_deref());
            let bench_client = client::create_client(&client::ConnectArgs {
                protocol: protocol_str,
                endpoint: &endpoint,
                username: username.as_deref(),
                password: password.as_deref(),
                token_endpoint: token_endpoint.as_deref(),
                client_id: client_id.as_deref(),
                client_secret: client_secret.as_deref(),
                catalog: catalog.as_deref(),
                schema: Some(&ns),
            })
            .await?;

            let s3_args = load::S3Args {
                access_key: s3_access_key,
                secret_key: s3_secret_key,
                endpoint: s3_endpoint,
                region: s3_region,
            };

            load::load_benchmark(
                bench_client.as_ref(),
                &load::LoadArgs {
                    benchmark: &benchmark,
                    scale,
                    data_path: &data,
                    s3_args: &s3_args,
                    clean,
                    catalog: catalog.as_deref(),
                    namespace_override: namespace.as_deref(),
                    bloom_filter,
                    puffin_stats,
                    hive_location: hive_location.as_deref(),
                },
            )
            .await
        }

        Command::Compare {
            benchmark,
            scale,
            sqe_host,
            sqe_port,
            sqe_username,
            sqe_password,
            token_endpoint,
            client_id,
            client_secret,
            trino_url,
            trino_user,
            trino_catalog,
            trino_schema,
            query,
            output,
            qualify_tables,
        } => {
            let sqe_endpoint = format!("http://{sqe_host}:{sqe_port}");
            // Scoped like `test` and like the Trino client below, so both
            // engines get the committed text (review 707).
            let namespace = crate::suite_namespace(&benchmark, scale, None);
            let sqe_client = client::create_client(&client::ConnectArgs {
                protocol: "flight",
                endpoint: &sqe_endpoint,
                username: sqe_username.as_deref(),
                password: sqe_password.as_deref(),
                token_endpoint: token_endpoint.as_deref(),
                client_id: client_id.as_deref(),
                client_secret: client_secret.as_deref(),
                catalog: None,
                schema: Some(&namespace),
            })
            .await?;

            // The default schema rides on the connection (`X-Trino-Schema`);
            // the query text itself is sent as committed (review 707).
            let trino_client =
                client::trino::TrinoBenchClient::new(&trino_url, Some(&trino_user), None)?
                    .with_catalog(trino_catalog.as_deref().unwrap_or("iceberg"))
                    .with_schema(trino_schema.as_deref().unwrap_or(&namespace));

            comparison::run_comparison(
                &benchmark,
                scale,
                sqe_client.as_ref(),
                &trino_client,
                &sqe_endpoint,
                &trino_url,
                query.as_deref(),
                &output,
                qualify_tables,
            )
            .await?;

            Ok(())
        }

        Command::Test {
            benchmark,
            scale,
            protocol,
            host,
            port,
            query,
            username,
            password,
            token_endpoint,
            client_id,
            client_secret,
            catalog,
            namespace,
            qualify_tables,
            result_label,
        } => {
            let protocol_str = protocol.as_str();
            let endpoint = format!("http://{host}:{port}");
            if std::env::var("BENCH_DEBUG").is_ok() {
                eprintln!("[sqe-bench] connecting to {endpoint} via {protocol_str}...");
            }
            let ns = crate::suite_namespace(&benchmark, scale, namespace.as_deref());
            let bench_client = client::create_client(&client::ConnectArgs {
                protocol: protocol_str,
                endpoint: &endpoint,
                username: username.as_deref(),
                password: password.as_deref(),
                token_endpoint: token_endpoint.as_deref(),
                client_id: client_id.as_deref(),
                client_secret: client_secret.as_deref(),
                catalog: catalog.as_deref(),
                schema: Some(&ns),
            })
            .await?;
            if std::env::var("BENCH_DEBUG").is_ok() {
                eprintln!("[sqe-bench] connected, running tests...");
            }

            let results = suite::run_suite(
                bench_client.as_ref(),
                None,
                &benchmark,
                scale,
                query.as_deref(),
                catalog.as_deref(),
                namespace.as_deref(),
                "",
                None,
                qualify_tables,
            )
            .await?
            .results;

            let report_name = match result_label.as_deref() {
                Some(label) => format!("{benchmark}-{label}"),
                None => benchmark.clone(),
            };
            report::print_summary(&report_name, scale, protocol_str, &results);

            let report_path =
                report::write_json_report(&report_name, scale, protocol_str, &results)?;
            println!("\nReport written to: {report_path}");

            Ok(())
        }

        Command::Run {
            suites,
            profile,
            scale,
            host,
            port,
            compare_trino,
            query,
            qualify_tables,
        } => {
            let golden_token = std::env::var("BENCH_GOLDEN_TOKEN").map_err(|_| {
                anyhow::anyhow!("run requires BENCH_GOLDEN_TOKEN (bearer for the golden Polaris)")
            })?;
            let trino_endpoint = std::env::var("BENCH_TRINO_ENDPOINT").ok();
            run::run(run::RunArgs {
                suites,
                profile,
                scale,
                host,
                port,
                compare_trino,
                query,
                golden_token,
                trino_endpoint,
                qualify_tables,
            })
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_subcommand_parses() {
        let args = Cli::parse_from([
            "sqe-bench",
            "compare",
            "tpch",
            "--scale",
            "1",
            "--sqe-host",
            "localhost",
            "--sqe-port",
            "50051",
            "--trino-url",
            "http://localhost:8080",
        ]);
        match args.command {
            Command::Compare {
                benchmark,
                scale,
                sqe_host,
                sqe_port,
                trino_url,
                qualify_tables,
                ..
            } => {
                assert_eq!(benchmark, "tpch");
                assert_eq!(scale, 1.0);
                assert_eq!(sqe_host, "localhost");
                assert_eq!(sqe_port, 50051);
                assert_eq!(trino_url, "http://localhost:8080");
                assert_eq!(qualify_tables, QualifyTables::Auto);
            }
            _ => panic!("expected Compare command"),
        }
    }

    #[test]
    fn qualify_tables_flag_parses_all_modes() {
        for (flag, want) in [
            ("auto", QualifyTables::Auto),
            ("always", QualifyTables::Always),
            ("never", QualifyTables::Never),
        ] {
            let cli = Cli::parse_from(["sqe-bench", "test", "tpch", "--qualify-tables", flag]);
            match cli.command {
                Command::Test { qualify_tables, .. } => assert_eq!(qualify_tables, want),
                _ => panic!("expected Test"),
            }
        }
    }
}

#[cfg(test)]
mod run_cli_tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_run_with_multiple_suites() {
        let cli = Cli::parse_from([
            "sqe-bench",
            "run",
            "tpch",
            "ssb",
            "--profile",
            "local",
            "--scale",
            "10",
        ]);
        match cli.command {
            Command::Run {
                suites,
                profile,
                scale,
                compare_trino,
                ..
            } => {
                assert_eq!(suites, vec!["tpch", "ssb"]);
                assert_eq!(profile, "local");
                assert_eq!(scale, 10.0);
                assert!(!compare_trino);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn run_defaults_host_and_port() {
        let cli = Cli::parse_from(["sqe-bench", "run", "tpch", "--profile", "local"]);
        match cli.command {
            Command::Run {
                host, port, scale, ..
            } => {
                assert_eq!(host, "localhost");
                assert_eq!(port, 60051);
                assert_eq!(scale, 1.0);
            }
            _ => panic!("expected Run"),
        }
    }
}
