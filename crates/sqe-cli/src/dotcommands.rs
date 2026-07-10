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
    /// `.schema <table>` / `.describe <table>` — describe a table's
    /// columns. Maps to an `information_schema.columns` query.
    /// Accepts unqualified (`users`), 2-part (`public.users`), or
    /// 3-part (`iceberg.staging.users`) names.
    Schema { table: String },
    /// `.summarize <table>` — per-column count, distinct, null,
    /// min, max. The REPL handles this via a two-step flow: read
    /// columns from `information_schema.columns`, then build a
    /// UNION ALL of per-column aggregates.
    Summarize { table: String },
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
        "schema" | "describe" | "d" => {
            if arg.is_empty() {
                Err(format!(".{cmd} needs a table name (try `.{cmd} <table>`)"))
            } else {
                Ok(DotCommand::Schema {
                    table: arg.to_string(),
                })
            }
        }
        "summarize" | "summary" => {
            if arg.is_empty() {
                Err(".summarize needs a table name (try `.summarize <table>`)".to_string())
            } else {
                Ok(DotCommand::Summarize {
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
     .describe <table>    alias for .schema\n  \
     .summarize <table>   per-column count, distinct, null, min, max\n  \
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

/// Build the per-column statistics query for `.summarize <table>`.
///
/// `columns` is the list of `(column_name, data_type)` pairs the REPL
/// fetched from `information_schema.columns` for the target table.
/// The resulting SQL is a UNION ALL where each branch produces one
/// row of column-level stats: row count, null count, distinct count,
/// min, and max. Min/max are cast to text so columns of mixed types
/// render in one table without an Arrow schema clash.
///
/// Returns `None` when the column list is empty (no such table /
/// no columns).
pub fn build_summarize_query(table: &str, columns: &[(String, String)]) -> Option<String> {
    if columns.is_empty() {
        return None;
    }

    let table_ref = quote_table_for_summarize(table);

    let branches: Vec<String> = columns
        .iter()
        .map(|(name, data_type)| {
            let escaped_name = name.replace('\'', "''");
            let escaped_type = data_type.replace('\'', "''");
            let quoted_col = format!("\"{}\"", name.replace('"', "\"\""));
            format!(
                "SELECT \
                 '{escaped_name}' AS column_name, \
                 '{escaped_type}' AS column_type, \
                 COUNT(*) AS count, \
                 COUNT(*) - COUNT({quoted_col}) AS null_count, \
                 COUNT(DISTINCT {quoted_col}) AS distinct_count, \
                 CAST(MIN({quoted_col}) AS VARCHAR) AS min, \
                 CAST(MAX({quoted_col}) AS VARCHAR) AS max \
                 FROM {table_ref}"
            )
        })
        .collect();

    Some(branches.join(" UNION ALL "))
}

/// Quote a 1-, 2-, or 3-part table name for use in a `FROM` clause.
/// Each segment becomes a double-quoted identifier so reserved words
/// and case-sensitive names work without surprise.
fn quote_table_for_summarize(table: &str) -> String {
    table
        .split('.')
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
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

    // -----------------------------------------------------------------------
    // V9: .describe (alias for .schema), .summarize
    // -----------------------------------------------------------------------

    #[test]
    fn describe_is_alias_for_schema() {
        let cmd = parse_dot_command(".describe events").unwrap().unwrap();
        assert_eq!(
            cmd,
            DotCommand::Schema {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn d_short_alias_for_describe() {
        let cmd = parse_dot_command(".d events").unwrap().unwrap();
        assert_eq!(
            cmd,
            DotCommand::Schema {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn describe_requires_arg() {
        let result = parse_dot_command(".describe").unwrap();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("describe"));
    }

    #[test]
    fn summarize_parses() {
        let cmd = parse_dot_command(".summarize events").unwrap().unwrap();
        assert_eq!(
            cmd,
            DotCommand::Summarize {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn summarize_summary_alias() {
        let cmd = parse_dot_command(".summary events").unwrap().unwrap();
        assert_eq!(
            cmd,
            DotCommand::Summarize {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn summarize_requires_arg() {
        assert!(parse_dot_command(".summarize").unwrap().is_err());
    }

    #[test]
    fn summarize_empty_columns_returns_none() {
        assert_eq!(build_summarize_query("t", &[]), None);
    }

    #[test]
    fn summarize_builds_union_all_per_column() {
        let cols = vec![
            ("id".to_string(), "Int64".to_string()),
            ("name".to_string(), "Utf8".to_string()),
        ];
        let q = build_summarize_query("events", &cols).expect("non-empty");
        // Two SELECT branches separated by UNION ALL.
        assert_eq!(q.matches("UNION ALL").count(), 1);
        assert_eq!(q.matches("SELECT").count(), 2);
        // Each column appears as a literal name AND a quoted identifier
        // for COUNT / MIN / MAX.
        assert!(q.contains("'id' AS column_name"));
        assert!(q.contains("'name' AS column_name"));
        assert!(q.contains("\"id\""));
        assert!(q.contains("\"name\""));
        // Stats columns present.
        assert!(q.contains("COUNT(*)"));
        assert!(q.contains("COUNT(DISTINCT"));
        assert!(q.contains("MIN("));
        assert!(q.contains("MAX("));
    }

    #[test]
    fn summarize_quotes_three_part_table() {
        let cols = vec![("x".to_string(), "Int64".to_string())];
        let q = build_summarize_query("iceberg.staging.events", &cols).unwrap();
        assert!(q.contains("\"iceberg\".\"staging\".\"events\""));
    }

    #[test]
    fn summarize_escapes_quotes_in_column_name() {
        let cols = vec![("col\"with\"quotes".to_string(), "Utf8".to_string())];
        let q = build_summarize_query("t", &cols).unwrap();
        // Doubled quotes inside the SQL identifier.
        assert!(q.contains("\"col\"\"with\"\"quotes\""));
    }

    #[test]
    fn summarize_escapes_apostrophes_in_data_type() {
        let cols = vec![("x".to_string(), "type'with'apos".to_string())];
        let q = build_summarize_query("t", &cols).unwrap();
        assert!(q.contains("'type''with''apos'"));
    }
}
