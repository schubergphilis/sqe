mod cli;
mod client;
mod compare;
mod generate;
mod load;

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
            ..
        } => {
            let gen = generate::get_generator(&benchmark)?;
            for table_def in gen.tables() {
                println!("Generating {}.{}...", benchmark, table_def.name);
                let stats = gen.generate_table(&table_def.name, scale, &output)?;
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
            ..
        } => {
            let protocol_str = match protocol {
                cli::Protocol::Flight => "flight",
                cli::Protocol::Http => "trino",
            };
            let endpoint = format!("http://{host}:{port}");
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
                &benchmark,
                scale,
                &data,
                &s3_args,
                clean,
            )
            .await
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
        } => {
            let protocol_str = match protocol {
                cli::Protocol::Flight => "flight",
                cli::Protocol::Http => "trino",
            };
            let endpoint = format!("http://{host}:{port}");
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

            let results = test::run_benchmark_test(
                bench_client.as_ref(),
                &benchmark,
                scale,
                query.as_deref(),
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
