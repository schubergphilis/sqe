mod cli;
mod client;
mod compare;
mod comparison;
mod generate;
mod load;
mod sink;

/// Format a scale factor as an identifier-safe string (no dots).
/// 0.01 → "0_01", 1.0 → "1", 10.0 → "10"
pub fn format_scale(scale: f64) -> String {
    if scale == scale.floor() {
        format!("{}", scale as u64)
    } else {
        format!("{scale}").replace('.', "_")
    }
}

/// Build the namespace name for a benchmark at a given scale factor.
pub fn bench_namespace(benchmark: &str, scale: f64) -> String {
    format!("{}_sf{}", benchmark, format_scale(scale))
}
mod report;
mod test;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = cli::Cli::parse();
    match cli.command {
        cli::Command::Generate {
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
            let config = generate::GenerateConfig::resolve(
                threads,
                compression.as_deref(),
                row_group_size,
            )?;

            if let cli::Sink::Iceberg = sink {
                anyhow::ensure!(
                    benchmark == "bank",
                    "--sink iceberg currently supports only the bank benchmark"
                );
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

        cli::Command::Load {
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
            ..
        } => {
            let protocol_str = match protocol {
                cli::Protocol::Flight => "flight",
                cli::Protocol::Http => "trino",
            };
            let endpoint = format!("http://{host}:{port}");
            if std::env::var("BENCH_DEBUG").is_ok() {
                eprintln!("[sqe-bench] connecting to {endpoint} via {protocol_str}...");
            }
            let bench_client = client::create_client(
                protocol_str,
                &endpoint,
                username.as_deref(),
                password.as_deref(),
                token_endpoint.as_deref(),
                client_id.as_deref(),
                client_secret.as_deref(),
            )
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
                },
            )
            .await
        }

        cli::Command::Compare {
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
            trino_catalog: _,
            trino_schema: _,
            query,
            output,
        } => {
            let sqe_endpoint = format!("http://{sqe_host}:{sqe_port}");
            let sqe_client = client::create_client(
                "flight",
                &sqe_endpoint,
                sqe_username.as_deref(),
                sqe_password.as_deref(),
                token_endpoint.as_deref(),
                client_id.as_deref(),
                client_secret.as_deref(),
            )
            .await?;

            let trino_client = client::trino::TrinoBenchClient::new(
                &trino_url,
                Some(&trino_user),
                None,
            ).with_catalog("iceberg");

            comparison::run_comparison(
                &benchmark,
                scale,
                sqe_client.as_ref(),
                &trino_client,
                &sqe_endpoint,
                &trino_url,
                query.as_deref(),
                &output,
            )
            .await?;

            Ok(())
        }

        cli::Command::Test {
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
        } => {
            let protocol_str = match protocol {
                cli::Protocol::Flight => "flight",
                cli::Protocol::Http => "trino",
            };
            let endpoint = format!("http://{host}:{port}");
            if std::env::var("BENCH_DEBUG").is_ok() {
                eprintln!("[sqe-bench] connecting to {endpoint} via {protocol_str}...");
            }
            let bench_client = client::create_client(
                protocol_str,
                &endpoint,
                username.as_deref(),
                password.as_deref(),
                token_endpoint.as_deref(),
                client_id.as_deref(),
                client_secret.as_deref(),
            )
            .await?;
            if std::env::var("BENCH_DEBUG").is_ok() {
                eprintln!("[sqe-bench] connected, running tests...");
            }

            let results = test::run_benchmark_test(
                bench_client.as_ref(),
                &benchmark,
                scale,
                query.as_deref(),
                catalog.as_deref(),
                namespace.as_deref(),
            )
            .await?;

            report::print_summary(&benchmark, scale, protocol_str, &results);

            let report_path =
                report::write_json_report(&benchmark, scale, protocol_str, &results)?;
            println!("\nReport written to: {report_path}");

            Ok(())
        }
    }
}
