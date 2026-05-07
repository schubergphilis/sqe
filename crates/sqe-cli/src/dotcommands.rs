//! Dot-command parsing for the REPL.
//!
//! Mirrors the sqlite3 / DuckDB shell convention: lines beginning with
//! `.` are interpreted as client-side commands rather than SQL.
//! Examples: `.tables`, `.schema users`, `.read script.sql`,
//! `.timer on`, `.help`, `.exit`.
//!
//! The REPL forwards the trimmed line to [`parse_dot_command`]. If
//! it returns `Some(action)`, the REPL handles the action without
//! sending anything to the engine. Otherwise the input is treated as
//! SQL.
//!
//! Why not the existing backslash form (`\format`, `\q`)? Backslash
//! commands are kept for backward compatibility, but dot-commands
//! match the convention every Postgres / sqlite / DuckDB user
//! already knows. Both work.

use std::path::PathBuf;

use crate::OutputFormat;

/// One parsed dot-command. Each variant maps to the action the REPL
/// performs.
#[derive(Debug, PartialEq)]
pub enum DotCommand {
    /// `.help` — print the help text.
    Help,
    /// `.exit` / `.quit` — leave the REPL.
    Exit,
    /// `.tables [schema]` — list tables, optionally filtered to
    /// one schema. The REPL turns this into an
    /// `information_schema.tables` query.
    Tables { schema: Option<String> },
    /// `.schema <table>` — describe a table's columns. Maps to an
    /// `information_schema.columns` query. Accepts unqualified
    /// (`users`), 2-part (`public.users`), or 3-part
    /// (`iceberg.staging.users`) names.
    Schema { table: String },
    /// `.databases` / `.catalogs` — list catalogs visible to the
    /// session. Maps to `information_schema.schemata`.
    Catalogs,
    /// `.read <path>` — execute a SQL script file, exactly like
    /// `--file` from the command line.
    Read { path: PathBuf },
    /// `.timer on|off` — toggle per-query elapsed time output.
    Timer(bool),
    /// `.format [table|csv|tsv|json]` — set or query the output
    /// format. `None` means show current.
    Format(Option<OutputFormat>),
}

/// Parse a single line. Returns `Some(cmd)` when the line begins
/// with `.` and is a recognised command; `None` otherwise so the
/// REPL falls through to SQL.
///
/// Recognises both `.exit` and `.quit` for the exit action because
/// users coming from sqlite type one or the other.
pub fn parse_dot_command(line: &str) -> Option<Result<DotCommand, String>> {
    let trimmed = line.trim();
    let rest = trimmed.strip_prefix('.')?;
    let mut parts = rest.splitn(2, char::is_whitespace);
    let cmd = parts.next()?.to_ascii_lowercase();
    let arg = parts.next().map(str::trim).unwrap_or("");

    let result = match cmd.as_str() {
        "help" | "h" | "?" => Ok(DotCommand::Help),
        "exit" | "quit" | "q" => Ok(DotCommand::Exit),
        "tables" => Ok(DotCommand::Tables {
            schema: if arg.is_empty() {
                None
            } else {
                Some(arg.to_string())
            },
        }),
        "schema" => {
            if arg.is_empty() {
                Err(".schema needs a table name (try `.schema <table>`)".to_string())
            } else {
                Ok(DotCommand::Schema {
                    table: arg.to_string(),
                })
            }
        }
        "databases" | "catalogs" => Ok(DotCommand::Catalogs),
        "read" => {
            if arg.is_empty() {
                Err(".read needs a file path (try `.read script.sql`)".to_string())
            } else {
                Ok(DotCommand::Read {
                    path: PathBuf::from(arg),
                })
            }
        }
        "timer" => match arg.to_ascii_lowercase().as_str() {
            "on" | "true" | "1" => Ok(DotCommand::Timer(true)),
            "off" | "false" | "0" => Ok(DotCommand::Timer(false)),
            "" => Err(".timer needs `on` or `off`".to_string()),
            other => Err(format!(".timer expects on|off, got `{other}`")),
        },
        "format" => {
            if arg.is_empty() {
                Ok(DotCommand::Format(None))
            } else if let Some(f) = OutputFormat::from_str(arg) {
                Ok(DotCommand::Format(Some(f)))
            } else {
                Err(format!(
                    "unknown format `{arg}`; valid: table, csv, tsv, json"
                ))
            }
        }
        other => Err(format!(
            "unknown command `.{other}`; type `.help` for the list"
        )),
    };

    Some(result)
}

/// Pretty help text rendered by `.help`.
pub fn help_text() -> &'static str {
    "Dot commands:\n  \
     .help                show this list\n  \
     .exit, .quit         leave the REPL\n  \
     .tables [schema]     list tables (optionally filter by schema)\n  \
     .schema <table>      describe a table's columns\n  \
     .catalogs            list catalogs visible to the session\n  \
     .read <path>         execute a SQL script file\n  \
     .timer on|off        toggle per-query elapsed-time output\n  \
     .format [fmt]        show or set output format (table|csv|tsv|json)\n\n\
     SQL: type a query and end it with `;`. End-of-input or .exit to quit."
}

/// Build the `information_schema.tables` query for `.tables [schema]`.
/// We exclude DataFusion's `information_schema` itself so the listing
/// is just user data.
pub fn build_tables_query(schema: Option<&str>) -> String {
    let mut q = String::from(
        "SELECT table_catalog, table_schema, table_name \
         FROM information_schema.tables \
         WHERE table_schema NOT IN ('information_schema')",
    );
    if let Some(s) = schema {
        // information_schema is case-sensitive on schema name; users
        // normally type lowercase. We pass through verbatim to avoid
        // surprising the case-sensitive case.
        q.push_str(&format!(" AND table_schema = '{}'", s.replace('\'', "''")));
    }
    q.push_str(" ORDER BY table_catalog, table_schema, table_name");
    q
}

/// Build the `information_schema.columns` query for `.schema <table>`.
/// Accepts 1-, 2-, or 3-part names. Unmatched parts default to a
/// wildcard so `.schema users` works against any catalog/schema.
pub fn build_schema_query(table: &str) -> String {
    let parts: Vec<&str> = table.split('.').collect();
    let (catalog, schema, name) = match parts.as_slice() {
        [name] => (None, None, *name),
        [schema, name] => (None, Some(*schema), *name),
        [catalog, schema, name] => (Some(*catalog), Some(*schema), *name),
        _ => (None, None, table), // 4+ parts — let the engine reject it
    };

    let mut q = String::from(
        "SELECT column_name, data_type, is_nullable \
         FROM information_schema.columns \
         WHERE 1=1",
    );
    if let Some(c) = catalog {
        q.push_str(&format!(" AND table_catalog = '{}'", c.replace('\'', "''")));
    }
    if let Some(s) = schema {
        q.push_str(&format!(" AND table_schema = '{}'", s.replace('\'', "''")));
    }
    q.push_str(&format!(
        " AND table_name = '{}' ORDER BY ordinal_position",
        name.replace('\'', "''")
    ));
    q
}

/// Build the `information_schema.schemata` query for `.catalogs`.
pub fn build_catalogs_query() -> &'static str {
    "SELECT catalog_name, schema_name \
     FROM information_schema.schemata \
     ORDER BY catalog_name, schema_name"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_help_aliases() {
        for s in [".help", ".h", ".?"] {
            assert_eq!(parse_dot_command(s).unwrap().unwrap(), DotCommand::Help);
        }
    }

    #[test]
    fn parses_exit_aliases() {
        for s in [".exit", ".quit", ".q"] {
            assert_eq!(parse_dot_command(s).unwrap().unwrap(), DotCommand::Exit);
        }
    }

    #[test]
    fn tables_no_arg() {
        assert_eq!(
            parse_dot_command(".tables").unwrap().unwrap(),
            DotCommand::Tables { schema: None }
        );
    }

    #[test]
    fn tables_with_schema_arg() {
        assert_eq!(
            parse_dot_command(".tables staging").unwrap().unwrap(),
            DotCommand::Tables {
                schema: Some("staging".into())
            }
        );
    }

    #[test]
    fn schema_requires_arg() {
        assert!(parse_dot_command(".schema").unwrap().is_err());
    }

    #[test]
    fn schema_three_part_name_query() {
        let q = build_schema_query("iceberg.staging.events");
        assert!(q.contains("table_catalog = 'iceberg'"));
        assert!(q.contains("table_schema = 'staging'"));
        assert!(q.contains("table_name = 'events'"));
    }

    #[test]
    fn schema_unqualified_name_query_skips_filters() {
        let q = build_schema_query("events");
        assert!(!q.contains("table_catalog ="));
        assert!(!q.contains("table_schema ="));
        assert!(q.contains("table_name = 'events'"));
    }

    #[test]
    fn schema_two_part_name() {
        let q = build_schema_query("staging.events");
        assert!(!q.contains("table_catalog ="));
        assert!(q.contains("table_schema = 'staging'"));
        assert!(q.contains("table_name = 'events'"));
    }

    #[test]
    fn timer_on_off_aliases() {
        for s in [".timer on", ".timer true", ".timer 1"] {
            assert_eq!(parse_dot_command(s).unwrap().unwrap(), DotCommand::Timer(true));
        }
        for s in [".timer off", ".timer false", ".timer 0"] {
            assert_eq!(parse_dot_command(s).unwrap().unwrap(), DotCommand::Timer(false));
        }
    }

    #[test]
    fn timer_invalid_arg_errors() {
        assert!(parse_dot_command(".timer banana").unwrap().is_err());
    }

    #[test]
    fn read_requires_arg() {
        assert!(parse_dot_command(".read").unwrap().is_err());
        assert_eq!(
            parse_dot_command(".read foo.sql").unwrap().unwrap(),
            DotCommand::Read {
                path: PathBuf::from("foo.sql")
            }
        );
    }

    #[test]
    fn unknown_dot_command_errors() {
        assert!(parse_dot_command(".banana").unwrap().is_err());
    }

    #[test]
    fn non_dot_input_returns_none() {
        assert!(parse_dot_command("SELECT 1").is_none());
        assert!(parse_dot_command("").is_none());
        assert!(parse_dot_command("   ").is_none());
    }

    #[test]
    fn sql_injection_in_schema_arg_is_escaped() {
        // `'or'1'='1` should be quoted, not opened as SQL.
        let q = build_schema_query("admin'--.users");
        assert!(q.contains("table_schema = 'admin''--'"));
    }
}
