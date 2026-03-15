mod client;
mod display;
mod flight;
mod http;

use clap::{Parser, ValueEnum};
use client::SqlClient;

#[derive(Parser)]
#[command(name = "sqe-cli", version, about = "SQE SQL client")]
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

    /// Username
    #[arg(short, long)]
    user: Option<String>,

    /// Execute a single query and exit
    #[arg(short = 'e', long)]
    execute: Option<String>,

    /// Use HTTPS/TLS (applies to both protocols)
    #[arg(long, default_value_t = false)]
    tls: bool,
}

#[derive(Clone, ValueEnum)]
enum Protocol {
    /// Arrow Flight SQL (gRPC/HTTP2)
    Flight,
    /// Trino-compat HTTP REST (works through any HTTP proxy)
    Http,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let username = cli.user.unwrap_or_else(|| {
        std::env::var("SQE_USER").unwrap_or_else(|_| {
            eprint!("Username: ");
            let mut buf = String::new();
            std::io::stdin().read_line(&mut buf).unwrap();
            buf.trim().to_string()
        })
    });

    let password = std::env::var("SQE_PASSWORD").unwrap_or_else(|_| {
        rpassword::prompt_password("Password: ").unwrap_or_default()
    });

    let scheme = if cli.tls { "https" } else { "http" };
    let mut client: Box<dyn SqlClient> = match cli.protocol {
        Protocol::Flight => {
            let url = format!("{scheme}://{}:{}", cli.host, cli.port);
            Box::new(flight::FlightClient::connect(&url, &username, &password).await?)
        }
        Protocol::Http => {
            let url = format!("{scheme}://{}:{}", cli.host, cli.port);
            Box::new(http::HttpClient::new(&url, &username, &password))
        }
    };

    let proto_label = match cli.protocol {
        Protocol::Flight => "flight",
        Protocol::Http => "http",
    };
    eprintln!(
        "Connected to {}://{}:{} as {} ({})",
        scheme, cli.host, cli.port, username, proto_label
    );

    if let Some(sql) = cli.execute {
        let result = client.execute(&sql).await?;
        display::print_query_result(&result);
        return Ok(());
    }

    repl(client.as_mut()).await
}

async fn repl(client: &mut dyn SqlClient) -> Result<(), Box<dyn std::error::Error>> {
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
                            Ok(result) => display::print_query_result(&result),
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
