mod client;
mod display;
mod dotcommands;
mod embedded;
mod flight;
mod http;
mod script;
mod writable_iceberg_catalog;

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

    /// Run an in-process engine instead of connecting to a remote
    /// coordinator. No auth, no Polaris, no network listeners. The
    /// `read_parquet(...)` TVF lets you query files directly.
    #[arg(long, default_value_t = false)]
    embedded: bool,

    /// Per-process query memory limit when running embedded. Accepts
    /// suffixes (`512MB`, `2GB`, ...). Floored to 64MB. Ignored
    /// unless `--embedded` is set.
    #[arg(long, default_value = "1GB")]
    memory_limit: String,

    /// Override the embedded warehouse path with a single catalog
    /// named `iceberg`. Shorthand for `--catalog iceberg=<path>`.
    /// Default is `~/.sqe/warehouse/`. Mutually exclusive with
    /// `--memory` and with `--catalog`.
    #[arg(long, conflicts_with_all = ["memory", "catalog"])]
    warehouse: Option<std::path::PathBuf>,

    /// Attach a named persistent Iceberg catalog. Format
    /// `NAME=PATH`, repeatable. Each catalog gets its own SQLite
    /// metadata + data root and shows up in 3-part SQL names. For
    /// example: `--catalog prod=/data/prod --catalog stage=/data/stage`
    /// then query with `SELECT * FROM prod.sales.orders JOIN
    /// stage.sales.orders ...`. Mutually exclusive with `--memory`
    /// and `--warehouse`.
    #[arg(long, value_parser = parse_catalog_spec, conflicts_with_all = ["memory", "warehouse"])]
    catalog: Vec<(String, std::path::PathBuf)>,

    /// Skip persistent catalogs entirely. `CREATE TABLE` works
    /// within the session via DataFusion's in-memory catalog but
    /// nothing survives the process. Mutually exclusive with
    /// `--warehouse` and `--catalog`.
    #[arg(long, default_value_t = false, conflicts_with_all = ["warehouse", "catalog"])]
    memory: bool,

    /// Embedded cloud catalog backend: `glue` or `s3tables`. When set,
    /// the embedded engine attaches that AWS catalog read-only instead
    /// of a local filesystem catalog. Credentials come from the AWS
    /// provider chain (`AWS_PROFILE`, SSO, instance profile). Requires
    /// the `aws` cargo feature (default-on). Mutually exclusive with
    /// `--memory`, `--warehouse`, and `--catalog`.
    #[arg(long, value_enum, conflicts_with_all = ["memory", "warehouse", "catalog"])]
    catalog_backend: Option<CloudBackend>,

    /// Cloud warehouse for `--catalog-backend`. For `glue`, an
    /// `s3://bucket/prefix` URI. For `s3tables`, the table-bucket ARN
    /// (`arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME`).
    #[arg(long, requires = "catalog_backend")]
    catalog_warehouse: Option<String>,

    /// AWS region for `--catalog-backend`. Sets the AWS SDK region for
    /// the catalog; falls back to the AWS provider chain when omitted.
    #[arg(long)]
    region: Option<String>,

    /// SQL identifier to mount the cloud catalog under. Defaults to the
    /// backend name, so tables resolve as `glue.namespace.table`.
    #[arg(long, requires = "catalog_backend")]
    catalog_name: Option<String>,

    /// Read SQL statements from a file and execute them in order.
    /// Statements are separated by `;`. When combined with
    /// `-e/--execute`, the script runs first and the `-e` query
    /// follows. (No short alias because `-f` is already taken by
    /// `--format`.)
    #[arg(long)]
    file: Option<std::path::PathBuf>,

    /// On `-f`, abort on the first failing statement. By default
    /// errors are printed and execution continues.
    #[arg(long, default_value_t = false)]
    stop_on_error: bool,
}

#[derive(Clone, ValueEnum)]
enum Protocol {
    /// Arrow Flight SQL (gRPC/HTTP2)
    Flight,
    /// Trino-compat HTTP REST (works through any HTTP proxy)
    Http,
}

/// Cloud catalog backend selectable in embedded mode.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum CloudBackend {
    /// AWS Glue Data Catalog (native AWS SDK).
    Glue,
    /// AWS S3 Tables (managed Iceberg).
    S3tables,
}

#[derive(Clone, Debug, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    /// Aligned ASCII table
    Table,
    /// Comma-separated values
    Csv,
    /// Tab-separated values
    Tsv,
    /// Newline-delimited JSON objects
    Json,
}

impl OutputFormat {
    fn name(&self) -> &'static str {
        match self {
            OutputFormat::Table => "table",
            OutputFormat::Csv => "csv",
            OutputFormat::Tsv => "tsv",
            OutputFormat::Json => "json",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "table" => Some(OutputFormat::Table),
            "csv" => Some(OutputFormat::Csv),
            "tsv" => Some(OutputFormat::Tsv),
            "json" => Some(OutputFormat::Json),
            _ => None,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let scheme = if cli.tls { "https" } else { "http" };

    let mut client: Box<dyn SqlClient> = if cli.embedded {
        let limit = sqe_core::parse_memory_limit(&cli.memory_limit).unwrap_or(1024 * 1024 * 1024);
        if let Some(backend_kind) = cli.catalog_backend {
            // Cloud catalog (Glue / S3 Tables), read-only. Region goes
            // into the AWS SDK env so both the catalog builder and the
            // credential chain see it.
            if let Some(r) = &cli.region {
                std::env::set_var("AWS_REGION", r);
            }
            let warehouse = cli
                .catalog_warehouse
                .clone()
                .ok_or("--catalog-warehouse is required with --catalog-backend")?;
            let backend = match backend_kind {
                CloudBackend::Glue => sqe_core::config::CatalogBackend::Glue {
                    region: cli.region.clone().unwrap_or_default(),
                    warehouse,
                    endpoint: None,
                },
                CloudBackend::S3tables => sqe_core::config::CatalogBackend::S3tables {
                    table_bucket_arn: warehouse,
                    endpoint_url: None,
                },
            };
            let backend_label = match backend_kind {
                CloudBackend::Glue => "glue",
                CloudBackend::S3tables => "s3tables",
            };
            let name = cli
                .catalog_name
                .clone()
                .unwrap_or_else(|| backend_label.to_string());
            let client = embedded::EmbeddedClient::with_backend(limit, backend, &name).await?;
            eprintln!(
                "sqe-cli {} embedded engine ({} memory pool, {} catalog: {})",
                sqe_core::VERSION,
                cli.memory_limit,
                backend_label,
                name
            );
            Box::new(client)
        } else {
            let mode = if cli.memory {
                embedded::WarehouseMode::Memory
            } else if !cli.catalog.is_empty() {
                embedded::WarehouseMode::Persistent {
                    catalogs: cli
                        .catalog
                        .iter()
                        .map(|(name, path)| embedded::EmbeddedCatalog {
                            name: name.clone(),
                            path: path.clone(),
                        })
                        .collect(),
                }
            } else if let Some(path) = cli.warehouse.clone() {
                embedded::WarehouseMode::single(path)
            } else {
                embedded::WarehouseMode::default_persistent()
            };
            let client = embedded::EmbeddedClient::with_warehouse(limit, &mode).await?;
            eprintln!(
                "sqe-cli {} embedded engine ({} memory pool, {})",
                sqe_core::VERSION,
                cli.memory_limit,
                warehouse_banner(&mode)
            );
            Box::new(client)
        }
    } else if let Some(ref token) = cli.token {
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

    if !cli.embedded {
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
    }

    // Script file runs first, then -e, then REPL falls through if neither
    // returns non-interactive.
    let mut ran_non_interactive = false;
    if let Some(ref path) = cli.file {
        run_script(client.as_mut(), path, &cli.format, cli.stop_on_error).await?;
        ran_non_interactive = true;
    }

    if let Some(sql) = cli.execute {
        let result = client.execute(&sql).await?;
        display::print_query_result(&result, &cli.format);
        ran_non_interactive = true;
    }

    if ran_non_interactive {
        return Ok(());
    }

    repl(client.as_mut(), cli.format).await
}

/// Read a SQL script from `path`, split on top-level `;`, and execute
/// statements in order. With `stop_on_error = false` (the default),
/// errors are printed to stderr and execution continues to the next
/// statement.
async fn run_script(
    client: &mut dyn SqlClient,
    path: &std::path::Path,
    format: &OutputFormat,
    stop_on_error: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;

    // Local format / timer that .format and .timer dot-commands can
    // mutate without leaking back into the caller. Scripts are
    // typically one-shot, so per-script overrides are fine.
    let mut local_format = format.clone();
    let mut timer_on = false;

    let mut failures = 0usize;
    for (idx, stmt) in script::split_statements(&contents).into_iter().enumerate() {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Dot-commands work in scripts too: `.summarize tbl`,
        // `.tables`, `.schema tbl`, etc. Mirrors the REPL behaviour
        // so a script can preview a table's stats without the user
        // hand-rolling a UNION ALL. `.read` inside a script is
        // refused (would be recursive); use multiple `--file` flags
        // or concatenate the scripts.
        if let Some(parsed) = dotcommands::parse_dot_command(trimmed) {
            match parsed {
                Ok(dotcommands::DotCommand::Read { .. }) => {
                    eprintln!(
                        "[stmt {}] .read is not supported inside a script; use multiple --file flags",
                        idx + 1
                    );
                    failures += 1;
                    if stop_on_error {
                        return Err(
                            format!("aborted after statement {}", idx + 1).into()
                        );
                    }
                }
                Ok(cmd) => {
                    handle_dot_command(cmd, client, &mut local_format, &mut timer_on).await;
                }
                Err(msg) => {
                    eprintln!("[stmt {}] dot-command error: {msg}", idx + 1);
                    failures += 1;
                    if stop_on_error {
                        return Err(format!("aborted after statement {}", idx + 1).into());
                    }
                }
            }
            continue;
        }

        match client.execute(trimmed).await {
            Ok(result) => display::print_query_result(&result, &local_format),
            Err(e) => {
                eprintln!("[stmt {}] error: {e}", idx + 1);
                failures += 1;
                if stop_on_error {
                    return Err(format!("aborted after statement {}", idx + 1).into());
                }
            }
        }
    }
    if failures > 0 && !stop_on_error {
        eprintln!("{failures} statement(s) failed; continued because --stop-on-error not set");
    }
    Ok(())
}

async fn repl(
    client: &mut dyn SqlClient,
    initial_format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rl = rustyline::DefaultEditor::new()?;
    let history_path = dirs_home().join(".sqe_history");
    let _ = rl.load_history(&history_path);

    let mut format = initial_format;

    eprintln!("Type SQL queries, or .help for commands. End multi-line queries with ;");

    let mut buf = String::new();
    let mut timer_on = false;

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

                // Dot-commands take precedence over SQL parsing. They
                // only fire on the first line of a multi-line entry —
                // mid-statement `.foo` is sent as SQL.
                if buf.is_empty() {
                    if let Some(parsed) = dotcommands::parse_dot_command(trimmed) {
                        match parsed {
                            Ok(cmd) => {
                                if handle_dot_command(
                                    cmd,
                                    client,
                                    &mut format,
                                    &mut timer_on,
                                )
                                .await
                                {
                                    break;
                                }
                            }
                            Err(msg) => eprintln!("Error: {msg}"),
                        }
                        continue;
                    }
                }

                // Backslash form: \format [value]. Kept for users who
                // already had it in their muscle memory; new users
                // should prefer .format.
                if let Some(rest) = trimmed.strip_prefix("\\format") {
                    let arg = rest.trim();
                    if arg.is_empty() {
                        eprintln!("Output format: {}", format.name());
                    } else if let Some(f) = OutputFormat::from_str(arg) {
                        format = f;
                        eprintln!("Output format set to: {}", format.name());
                    } else {
                        eprintln!("Unknown format '{arg}'. Valid: table, csv, tsv, json");
                    }
                    continue;
                }

                // SET format = '...' intercepted client-side.
                {
                    let upper = trimmed.to_ascii_uppercase();
                    let stripped = upper
                        .trim_end_matches(';')
                        .trim()
                        .strip_prefix("SET FORMAT")
                        .map(|s| s.trim().trim_start_matches('=').trim().trim_matches('\'').trim_matches('"').to_ascii_lowercase());
                    if let Some(val) = stripped {
                        if let Some(f) = OutputFormat::from_str(&val) {
                            format = f;
                            eprintln!("Output format set to: {}", format.name());
                        } else {
                            eprintln!("Unknown format '{val}'. Valid: table, csv, tsv, json");
                        }
                        continue;
                    }
                }

                buf.push_str(trimmed);
                buf.push(' ');

                if trimmed.ends_with(';') {
                    let sql = buf.trim().trim_end_matches(';').trim();
                    if !sql.is_empty() {
                        rl.add_history_entry(sql)?;
                        run_one(client, sql, &format, timer_on).await;
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

/// Run one SQL statement and print the result. Wraps the existing
/// `client.execute` + display call so the REPL and the dot-command
/// path can share the timer logic.
async fn run_one(
    client: &mut dyn SqlClient,
    sql: &str,
    format: &OutputFormat,
    timer_on: bool,
) {
    let start = std::time::Instant::now();
    match client.execute(sql).await {
        Ok(result) => {
            display::print_query_result(&result, format);
            if timer_on {
                eprintln!("Time: {:.3}s", start.elapsed().as_secs_f64());
            }
        }
        Err(e) => eprintln!("Error: {e}"),
    }
}

/// Execute one parsed [`dotcommands::DotCommand`]. Returns `true`
/// when the command means "leave the REPL" (`.exit` / `.quit`).
async fn handle_dot_command(
    cmd: dotcommands::DotCommand,
    client: &mut dyn SqlClient,
    format: &mut OutputFormat,
    timer_on: &mut bool,
) -> bool {
    use dotcommands::DotCommand;
    match cmd {
        DotCommand::Help => {
            println!("{}", dotcommands::help_text());
            false
        }
        DotCommand::Exit => true,
        DotCommand::Tables { schema } => {
            let q = dotcommands::build_tables_query(schema.as_deref());
            run_one(client, &q, format, *timer_on).await;
            false
        }
        DotCommand::Schema { table } => {
            let q = dotcommands::build_schema_query(&table);
            run_one(client, &q, format, *timer_on).await;
            false
        }
        DotCommand::Summarize { table } => {
            // Two-step: fetch (column_name, data_type) from the
            // information schema, then build a UNION ALL of per-column
            // aggregates and execute that against the same client.
            let columns_query = dotcommands::build_schema_query(&table);
            match client.execute(&columns_query).await {
                Ok(result) => {
                    let columns: Vec<(String, String)> = result
                        .rows
                        .iter()
                        .filter_map(|row| {
                            let name = row.first()?.clone();
                            let data_type = row.get(1).cloned().unwrap_or_default();
                            Some((name, data_type))
                        })
                        .collect();
                    match dotcommands::build_summarize_query(&table, &columns) {
                        Some(q) => run_one(client, &q, format, *timer_on).await,
                        None => eprintln!("No columns found for table `{table}`."),
                    }
                }
                Err(e) => eprintln!("Error fetching columns for `{table}`: {e}"),
            }
            false
        }
        DotCommand::Catalogs => {
            let q = dotcommands::build_catalogs_query();
            run_one(client, q, format, *timer_on).await;
            false
        }
        DotCommand::Read { path } => {
            // Reuse the same loop as `--file` so error handling and
            // splitter rules stay in one place. `run_script` now
            // dispatches dot-commands itself, which means
            // `handle_dot_command` -> `run_script` -> `handle_dot_command`
            // is mutually recursive across an `async fn` boundary.
            // Box::pin breaks the cycle so the compiler accepts it
            // without requiring a generated state machine of unbounded
            // size. Scripts that try to `.read` themselves are caught
            // upstream in `run_script`.
            let res = Box::pin(run_script(client, &path, format, false)).await;
            if let Err(e) = res {
                eprintln!("Error: {e}");
            }
            false
        }
        DotCommand::Timer(on) => {
            *timer_on = on;
            eprintln!("Timer: {}", if on { "on" } else { "off" });
            false
        }
        DotCommand::Format(opt) => {
            match opt {
                None => eprintln!("Output format: {}", format.name()),
                Some(f) => {
                    *format = f;
                    eprintln!("Output format set to: {}", format.name());
                }
            }
            false
        }
    }
}

/// Render the warehouse part of the welcome banner. Memory mode says
/// `ephemeral`; one persistent catalog says `warehouse: <path>` (the
/// V2 phrasing); two or more list each as `name=path`.
fn warehouse_banner(mode: &embedded::WarehouseMode) -> String {
    match mode {
        embedded::WarehouseMode::Memory => "ephemeral".to_string(),
        embedded::WarehouseMode::Persistent { catalogs } if catalogs.is_empty() => {
            "ephemeral".to_string()
        }
        embedded::WarehouseMode::Persistent { catalogs } if catalogs.len() == 1 => {
            format!("warehouse: {}", catalogs[0].path.display())
        }
        embedded::WarehouseMode::Persistent { catalogs } => {
            let joined: Vec<String> = catalogs
                .iter()
                .map(|c| format!("{}={}", c.name, c.path.display()))
                .collect();
            format!("catalogs: {}", joined.join(", "))
        }
    }
}

/// Parse `NAME=PATH` for the repeatable `--catalog` flag.
/// Rejects empty names, paths, and identifiers that contain `.`,
/// since 3-part SQL identifiers split on `.` and a catalog name
/// like `prod.eu` would silently misroute. Whitespace around `=`
/// is allowed for readability.
fn parse_catalog_spec(s: &str) -> Result<(String, std::path::PathBuf), String> {
    let (name, path) = s
        .split_once('=')
        .ok_or_else(|| format!("expected NAME=PATH, got `{s}`"))?;
    let name = name.trim();
    let path = path.trim();
    if name.is_empty() {
        return Err(format!("catalog name is empty in `{s}`"));
    }
    if path.is_empty() {
        return Err(format!("catalog path is empty in `{s}`"));
    }
    if name.contains('.') {
        return Err(format!(
            "catalog name `{name}` cannot contain `.` (it would clash with 3-part SQL names)"
        ));
    }
    Ok((name.to_string(), std::path::PathBuf::from(path)))
}

fn dirs_home() -> std::path::PathBuf {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
}
