mod client;
mod display;
mod flight;
mod http;

use clap::{Parser, ValueEnum};
use client::SqlClient;

#[derive(Parser)]
#[command(name = "sqe-cli", version = sqe_core::VERSION, about = "SQE SQL client")]
struct Cli {
    /// Coordinator host
    #[arg(short = 'H', long, default_value = "localhost")]
    host: String,

    /// Coordinator port (Flight SQL or HTTP depending on --protocol)
    #[arg(short, long, default_value_t = 50051)]
    port: u16,

    /// Wire protocol to use
    #[arg(long, default_value = "flight")]
    protocol: Protocol,

    /// Username (prompts if not provided and --token is not set)
    #[arg(short, long)]
    user: Option<String>,

    /// Bearer token for authentication (skips username/password flow)
    #[arg(long)]
    token: Option<String>,

    /// Execute a single query and exit
    #[arg(short = 'e', long)]
    execute: Option<String>,

    /// Output format
    #[arg(short, long, default_value = "table")]
    format: OutputFormat,

    /// Use HTTPS/TLS (applies to both protocols)
    #[arg(long, default_value_t = false)]
    tls: bool,

    /// Accept invalid TLS certificates (insecure, use for development only)
    #[arg(long, default_value_t = false)]
    insecure: bool,
}

#[derive(Clone, ValueEnum)]
enum Protocol {
    /// Arrow Flight SQL (gRPC/HTTP2)
    Flight,
    /// Trino-compat HTTP REST (works through any HTTP proxy)
    Http,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    /// Aligned ASCII table
    Table,
    /// Comma-separated values
    Csv,
    /// Newline-delimited JSON objects
    Json,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let scheme = if cli.tls { "https" } else { "http" };

    let mut client: Box<dyn SqlClient> = if let Some(ref token) = cli.token {
        // Token-based auth: skip username/password
        match cli.protocol {
            Protocol::Flight => {
                let url = format!("{scheme}://{}:{}", cli.host, cli.port);
                Box::new(flight::FlightClient::connect_with_token(&url, token).await?)
            }
            Protocol::Http => {
                return Err("--token is only supported with Flight protocol".into());
            }
        }
    } else {
        // Username/password auth
        let username = cli.user.clone().unwrap_or_else(|| {
            std::env::var("SQE_USER").unwrap_or_else(|_| {
                eprint!("Username: ");
                let mut buf = String::new();
                std::io::stdin().read_line(&mut buf).unwrap();
                buf.trim().to_string()
            })
        });

        let password = std::env::var("SQE_PASSWORD").unwrap_or_else(|_| {
            rpassword::prompt_password("Password: ")
                .expect("Failed to read password (not a terminal?)")
        });

        match cli.protocol {
            Protocol::Flight => {
                let url = format!("{scheme}://{}:{}", cli.host, cli.port);
                Box::new(flight::FlightClient::connect(&url, &username, &password).await?)
            }
            Protocol::Http => {
                let url = format!("{scheme}://{}:{}", cli.host, cli.port);
                Box::new(http::HttpClient::new(&url, &username, &password, cli.insecure))
            }
        }
    };

    let proto_label = match cli.protocol {
        Protocol::Flight => "flight",
        Protocol::Http => "http",
    };
    eprintln!(
        "sqe-cli {} connected to {scheme}://{}:{} ({proto_label})",
        sqe_core::VERSION,
        cli.host,
        cli.port
    );

    if let Some(sql) = cli.execute {
        let result = client.execute(&sql).await?;
        display::print_query_result(&result, &cli.format);
        return Ok(());
    }

    repl(client.as_mut(), &cli.format).await
}

async fn repl(
    client: &mut dyn SqlClient,
    format: &OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rl = rustyline::DefaultEditor::new()?;
    let history_path = dirs_home().join(".sqe_history");
    let _ = rl.load_history(&history_path);

    eprintln!("Type SQL queries, or \\q to quit. End multi-line queries with ;");

    let mut buf = String::new();

    loop {
        let prompt = if buf.is_empty() { "sqe> " } else { "  -> " };

        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                if trimmed == "\\q" || trimmed == "quit" || trimmed == "exit" {
                    break;
                }

                if trimmed.is_empty() {
                    continue;
                }

                buf.push_str(trimmed);
                buf.push(' ');

                if trimmed.ends_with(';') {
                    let sql = buf.trim().trim_end_matches(';').trim();
                    if !sql.is_empty() {
                        rl.add_history_entry(sql)?;
                        match client.execute(sql).await {
                            Ok(result) => display::print_query_result(&result, format),
                            Err(e) => eprintln!("Error: {e}"),
                        }
                    }
                    buf.clear();
                }
            }
            Err(
                rustyline::error::ReadlineError::Interrupted
                | rustyline::error::ReadlineError::Eof,
            ) => {
                break;
            }
            Err(e) => {
                eprintln!("Readline error: {e}");
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
    Ok(())
}

fn dirs_home() -> std::path::PathBuf {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
}
