//! Benchmark query files and (opt-in) table qualification.
//!
//! Review 707: the harness used to rewrite every benchmark query with string
//! heuristics to prefix bare table names, and the committed TPC text had been
//! edited to dodge the rewriter's false positives. The default is now to send
//! the file text as committed and let the connection carry the schema
//! (`X-Trino-Schema` on the Trino protocol). Qualification survives only as
//! [`QualifyTables`] for connections that cannot express a default schema
//! (today: the coordinator's Flight path, see `client::flight`), and it is
//! implemented on `sqlparser`'s tokenizer so aliases, string literals and
//! comments are tokens that are never touched.

use std::collections::HashSet;

use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

pub struct QueryFile {
    pub id: String,
    /// Human-readable name from `-- name:` header comment.
    pub name: String,
    pub sql: String,
    pub requires: Vec<String>,
    /// Per-query timeout from `-- timeout:` header comment.
    pub timeout_secs: u64,
}

/// How bare table names in a query file reach the engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum QualifyTables {
    /// Send the text untouched when the client carries a default schema
    /// (`BenchClient::default_schema()` is `Some`); qualify otherwise.
    #[default]
    Auto,
    /// Always qualify bare table names with the namespace.
    Always,
    /// Never rewrite; the engine must resolve bare names itself.
    Never,
}

impl QualifyTables {
    /// Whether a client with `default_schema` needs qualified table names.
    pub fn applies(self, default_schema: Option<&str>) -> bool {
        match self {
            QualifyTables::Always => true,
            QualifyTables::Never => false,
            QualifyTables::Auto => default_schema.is_none(),
        }
    }
}

/// The SQL to send to a client for a benchmark query: the committed text, or
/// the tokenizer-qualified text when [`QualifyTables::applies`] says so.
pub fn sql_for_client(
    sql: &str,
    namespace: &str,
    benchmark: &str,
    mode: QualifyTables,
    default_schema: Option<&str>,
) -> anyhow::Result<String> {
    if mode.applies(default_schema) {
        prefix_tables(sql, namespace, benchmark)
    } else {
        Ok(sql.to_string())
    }
}

// ---------------------------------------------------------------------------
// Query file loader
// ---------------------------------------------------------------------------

/// Load all `.sql` files from `benchmarks/queries/<benchmark>/`, sorted by
/// filename.  Header comments (`-- key: value`) are extracted from lines that
/// appear before any non-comment, non-blank line.
pub fn load_query_files(benchmark: &str) -> anyhow::Result<Vec<QueryFile>> {
    let dir = format!("benchmarks/queries/{benchmark}");
    let mut entries: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .map_err(|e| anyhow::anyhow!("Cannot read query directory '{dir}': {e}"))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("sql"))
        .collect();

    entries.sort();

    let mut queries = Vec::new();
    for path in &entries {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Cannot read query file '{}': {e}", path.display()))?;

        // Derive id from stem (e.g. "q01" from "q01.sql")
        let id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        let (name, requires, timeout_secs, sql) = parse_query_file(&content);

        queries.push(QueryFile {
            id,
            name,
            sql,
            requires,
            timeout_secs,
        });
    }

    Ok(queries)
}

/// Extract header metadata from query file content.
///
/// Lines at the top that start with `--` are inspected for:
/// - `-- name: <text>`
/// - `-- requires: <comma-separated features>`
/// - `-- timeout: <N>s`
///
/// Everything after the header block is the SQL body.
pub fn parse_query_file(content: &str) -> (String, Vec<String>, u64, String) {
    let mut name = String::new();
    let mut requires: Vec<String> = Vec::new();
    let mut timeout_secs: u64 = 300; // 5-minute default
    let mut in_header = true;
    let mut sql_lines: Vec<&str> = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();

        if in_header {
            if trimmed.is_empty() {
                sql_lines.push(line);
                continue;
            }
            if let Some(rest) = trimmed.strip_prefix("--") {
                let rest = rest.trim();
                if let Some(v) = rest.strip_prefix("name:") {
                    name = v.trim().to_string();
                } else if let Some(v) = rest.strip_prefix("requires:") {
                    requires = v
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                } else if let Some(v) = rest.strip_prefix("timeout:") {
                    let v = v.trim().trim_end_matches('s').trim();
                    timeout_secs = v.parse().unwrap_or(300);
                }
                // Other comment lines are kept as SQL
                sql_lines.push(line);
                continue;
            }
            // First non-comment, non-blank line ends the header
            in_header = false;
        }
        sql_lines.push(line);
    }

    let sql = sql_lines.join("\n");
    (name, requires, timeout_secs, sql)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Strip leading zeros / "q" prefix for flexible filter matching.
/// "q01" → "1", "1" → "1", "q1" → "1"
pub fn normalize_query_id(id: &str) -> String {
    let trimmed = id.trim_start_matches('q');
    // Integer ids normalize numerically ("q01" == "1"). Dotted ids (SSB's
    // "q3.1") fail the parse and previously all collapsed to "0", which
    // made every filter match every query — `--query q3.1` silently ran
    // the whole suite. Compare those as plain strings instead.
    match trimmed.parse::<u64>() {
        Ok(n) => n.to_string(),
        Err(_) => trimmed.to_string(),
    }
}

/// Table names of a benchmark suite (TPC-BB queries also reference the
/// TPC-DS tables they are loaded next to).
pub fn suite_tables(benchmark: &str) -> anyhow::Result<Vec<String>> {
    let gen = crate::generate::get_generator(benchmark)?;
    let mut tables: Vec<String> = gen.tables().into_iter().map(|t| t.name).collect();
    if benchmark == "tpcbb" {
        let tpcds = crate::generate::get_generator("tpcds")?;
        tables.extend(tpcds.tables().into_iter().map(|t| t.name));
    }
    Ok(tables)
}

/// Qualify the bare table names of `benchmark` in `sql` with `namespace`.
/// Unknown benchmarks pass the text through unchanged (there is no table
/// list to match against).
pub fn prefix_tables(sql: &str, namespace: &str, benchmark: &str) -> anyhow::Result<String> {
    let tables = match suite_tables(benchmark) {
        Ok(t) => t,
        Err(_) => return Ok(sql.to_string()),
    };
    qualify_tables(sql, namespace, &tables)
}

/// Per-parenthesis-depth parser state for [`qualify_tables`].
#[derive(Default, Clone, Copy)]
struct Scope {
    /// The next identifier is a table reference (right after FROM/JOIN/INTO/
    /// UPDATE/TABLE/USING, or after a comma inside an active FROM clause).
    expect_table: bool,
    /// Inside a FROM clause: a comma at this depth introduces another table.
    from_active: bool,
    /// Inside a function-call argument list: `FROM` here is `EXTRACT(x FROM
    /// y)` / `SUBSTRING(... FROM ...)`, not a table introducer.
    function_args: bool,
}

/// Qualify bare table references in `sql` with `namespace`, using
/// `sqlparser`'s tokenizer so only identifier tokens in table position are
/// touched. Column aliases, string literals, comments, quoted identifiers
/// that are not tables, CTE names and already-qualified names are left as
/// they are. The output is the input text with `namespace.` spliced in front
/// of each qualified table token; nothing else is re-rendered.
pub fn qualify_tables(sql: &str, namespace: &str, tables: &[String]) -> anyhow::Result<String> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .map_err(|e| anyhow::anyhow!("cannot tokenize query for table qualification: {e}"))?;

    let table_set: HashSet<String> = tables.iter().map(|t| t.to_ascii_lowercase()).collect();
    let cte_names = collect_cte_names(&tokens);

    // Significant (non-whitespace) tokens, by index into `tokens`.
    let sig: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t.token, Token::Whitespace(_)))
        .map(|(i, _)| i)
        .collect();

    let mut stack: Vec<Scope> = vec![Scope::default()];
    let mut insert_at: Vec<Location> = Vec::new();

    for (k, &i) in sig.iter().enumerate() {
        let tok = &tokens[i].token;
        let prev = k.checked_sub(1).map(|p| &tokens[sig[p]].token);
        let next = sig.get(k + 1).map(|&n| &tokens[n].token);
        let depth = stack.len() - 1;
        let scope = &mut stack[depth];

        match tok {
            Token::LParen => {
                // `FROM (`, `JOIN (`, `USING (` open a derived table; the
                // parent keeps its FROM clause active for what follows `)`.
                let derived = scope.expect_table;
                scope.expect_table = false;
                if derived {
                    scope.from_active = true;
                }
                let function_args = match prev {
                    // `IN (`, `EXISTS (`, `AS (`, `FROM (`, `= (`, `, (` ...
                    // open a subquery or grouping, not a call.
                    Some(Token::Word(w)) if w.quote_style.is_none() => {
                        !is_paren_opener_keyword(w.keyword)
                    }
                    Some(Token::Word(_)) => true,
                    _ => false,
                };
                stack.push(Scope {
                    function_args,
                    ..Scope::default()
                });
            }
            Token::RParen => {
                if stack.len() > 1 {
                    stack.pop();
                }
            }
            Token::Comma => {
                if scope.from_active {
                    scope.expect_table = true;
                }
            }
            Token::SemiColon => {
                stack.clear();
                stack.push(Scope::default());
            }
            Token::Word(w) => {
                if w.quote_style.is_none() && !scope.function_args {
                    match w.keyword {
                        Keyword::FROM
                        | Keyword::JOIN
                        | Keyword::INTO
                        | Keyword::UPDATE
                        | Keyword::TABLE
                        | Keyword::USING => {
                            scope.expect_table = true;
                            scope.from_active = true;
                            continue;
                        }
                        // `DROP TABLE IF EXISTS t`, `FROM ONLY t`, `JOIN LATERAL (`.
                        Keyword::IF
                        | Keyword::EXISTS
                        | Keyword::NOT
                        | Keyword::ONLY
                        | Keyword::LATERAL
                            if scope.expect_table =>
                        {
                            continue;
                        }
                        // Clause boundaries: the FROM list is over.
                        Keyword::WHERE
                        | Keyword::GROUP
                        | Keyword::HAVING
                        | Keyword::ORDER
                        | Keyword::LIMIT
                        | Keyword::OFFSET
                        | Keyword::FETCH
                        | Keyword::UNION
                        | Keyword::EXCEPT
                        | Keyword::INTERSECT
                        | Keyword::WINDOW
                        | Keyword::QUALIFY
                        | Keyword::SET
                        | Keyword::RETURNING
                        | Keyword::SELECT
                        | Keyword::VALUES
                        | Keyword::WITH => {
                            scope.expect_table = false;
                            scope.from_active = false;
                            continue;
                        }
                        _ => {}
                    }
                }
                if scope.expect_table {
                    // This identifier fills the table slot whether or not we
                    // qualify it (an alias, a CTE, a qualified name, or a
                    // function-table such as `read_parquet(...)`).
                    scope.expect_table = false;
                    let preceded_by_period = matches!(prev, Some(Token::Period));
                    let followed_by_period = matches!(next, Some(Token::Period));
                    let name = w.value.to_ascii_lowercase();
                    if !preceded_by_period
                        && !followed_by_period
                        && table_set.contains(&name)
                        && !cte_names.contains(&name)
                    {
                        insert_at.push(tokens[i].span.start);
                    }
                }
            }
            _ => {
                // A literal or operator in table position: nothing to qualify.
                scope.expect_table = false;
            }
        }
    }

    if insert_at.is_empty() {
        return Ok(sql.to_string());
    }

    let mut offsets: Vec<usize> = insert_at
        .iter()
        .map(|loc| {
            byte_offset(sql, *loc).ok_or_else(|| {
                anyhow::anyhow!(
                    "tokenizer location {}:{} is outside the query text",
                    loc.line,
                    loc.column
                )
            })
        })
        .collect::<anyhow::Result<_>>()?;
    offsets.sort_unstable();
    offsets.dedup();

    let prefix = format!("{namespace}.");
    let mut out = String::with_capacity(sql.len() + offsets.len() * prefix.len());
    let mut last = 0usize;
    for off in offsets {
        out.push_str(&sql[last..off]);
        out.push_str(&prefix);
        last = off;
    }
    out.push_str(&sql[last..]);
    Ok(out)
}

/// Keywords after which `(` opens a subquery / grouping rather than a
/// function-call argument list.
fn is_paren_opener_keyword(k: Keyword) -> bool {
    matches!(
        k,
        Keyword::IN
            | Keyword::EXISTS
            | Keyword::AS
            | Keyword::FROM
            | Keyword::JOIN
            | Keyword::ON
            | Keyword::AND
            | Keyword::BETWEEN
            | Keyword::OR
            | Keyword::NOT
            | Keyword::SELECT
            | Keyword::WHERE
            | Keyword::USING
            | Keyword::VALUES
            | Keyword::THEN
            | Keyword::ELSE
            | Keyword::WHEN
            | Keyword::BY
            | Keyword::UNION
            | Keyword::ALL
            | Keyword::ANY
            | Keyword::SOME
            | Keyword::LATERAL
            | Keyword::HAVING
            | Keyword::RETURNING
            | Keyword::WITH
            | Keyword::INTERSECT
            | Keyword::EXCEPT
            | Keyword::CASE
            | Keyword::DISTINCT
    )
}

/// Names bound by `WITH name [(cols)] AS (...) [, name2 AS (...)]` at any
/// depth. A CTE named like a base table must not be qualified.
fn collect_cte_names(tokens: &[TokenWithSpan]) -> HashSet<String> {
    #[derive(PartialEq)]
    enum Mode {
        Off,
        /// Expecting the next CTE name at `depth`.
        ExpectName(usize),
        /// Inside/after a CTE body at `depth`; a comma there starts another.
        InList(usize),
    }
    let mut names = HashSet::new();
    let mut depth = 0usize;
    let mut mode = Mode::Off;
    for t in tokens {
        match &t.token {
            Token::Whitespace(_) => {}
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::Word(w) if w.quote_style.is_none() && w.keyword == Keyword::WITH => {
                mode = Mode::ExpectName(depth);
            }
            Token::Word(w) => match mode {
                Mode::ExpectName(d) if d == depth => {
                    names.insert(w.value.to_ascii_lowercase());
                    mode = Mode::InList(d);
                }
                Mode::InList(d) if d == depth && w.keyword != Keyword::AS => {
                    // `WITH x AS (...) SELECT` -- the list is over.
                    mode = Mode::Off;
                }
                _ => {}
            },
            Token::Comma => {
                if let Mode::InList(d) = mode {
                    if d == depth {
                        mode = Mode::ExpectName(d);
                    }
                }
            }
            _ => {}
        }
    }
    names
}

/// Byte offset of a 1-based tokenizer `(line, column)`; columns count chars.
fn byte_offset(sql: &str, loc: Location) -> Option<usize> {
    let line_idx = usize::try_from(loc.line).ok()?.checked_sub(1)?;
    let col_idx = usize::try_from(loc.column).ok()?.checked_sub(1)?;
    let mut line_start = 0usize;
    for (n, line) in sql.split_inclusive('\n').enumerate() {
        if n == line_idx {
            return line
                .char_indices()
                .nth(col_idx)
                .map(|(b, _)| line_start + b);
        }
        line_start += line.len();
    }
    None
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_query_header_extracts_metadata() {
        let content = "-- name: Pricing Summary Report\n-- timeout: 60s\nSELECT 1;\n";
        let (name, requires, timeout, sql) = parse_query_file(content);
        assert_eq!(name, "Pricing Summary Report");
        assert!(requires.is_empty());
        assert_eq!(timeout, 60);
        assert!(sql.contains("SELECT 1"));
    }

    #[test]
    fn parse_query_header_requires() {
        let content = "-- name: Test\n-- requires: window_functions, lateral_join\nSELECT 1;\n";
        let (_, requires, _, _) = parse_query_file(content);
        assert_eq!(requires, vec!["window_functions", "lateral_join"]);
    }

    #[test]
    fn parse_query_default_timeout() {
        let content = "SELECT 1;\n";
        let (_, _, timeout, _) = parse_query_file(content);
        assert_eq!(timeout, 300);
    }

    #[test]
    fn normalize_query_id_strips_prefix_and_zeros() {
        assert_eq!(normalize_query_id("q01"), "1");
        assert_eq!(normalize_query_id("q1"), "1");
        assert_eq!(normalize_query_id("1"), "1");
        assert_eq!(normalize_query_id("22"), "22");
    }

    #[test]
    fn normalize_query_id_keeps_dotted_ids_distinct() {
        assert_eq!(normalize_query_id("q3.1"), "3.1");
        assert_eq!(normalize_query_id("3.1"), "3.1");
        assert_ne!(normalize_query_id("q3.1"), normalize_query_id("q4.2"));
    }

    #[test]
    fn qualify_mode_applies() {
        assert!(QualifyTables::Auto.applies(None));
        assert!(!QualifyTables::Auto.applies(Some("tpch_sf1")));
        assert!(QualifyTables::Always.applies(Some("tpch_sf1")));
        assert!(!QualifyTables::Never.applies(None));
    }

    #[test]
    fn byte_offset_handles_multibyte_and_lines() {
        let sql = "SELECT 'é' FROM t\nJOIN u";
        // 'J' of JOIN is line 2, column 1.
        let off = byte_offset(sql, Location { line: 2, column: 1 }).unwrap();
        assert_eq!(&sql[off..off + 4], "JOIN");
        // 't' on line 1: after "SELECT 'é' FROM " = 16 chars, so column 17.
        let off = byte_offset(
            sql,
            Location {
                line: 1,
                column: 17,
            },
        )
        .unwrap();
        assert_eq!(&sql[off..off + 1], "t");
    }
}
