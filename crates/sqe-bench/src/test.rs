use crate::client::BenchClient;
use crate::compare::{compare_results, CompareStatus};

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

pub struct QueryFile {
    pub id: String,
    /// Human-readable name from `-- name:` header comment.
    #[allow(dead_code)]
    pub name: String,
    pub sql: String,
    pub requires: Vec<String>,
    /// Per-query timeout from `-- timeout:` header comment (future use).
    #[allow(dead_code)]
    pub timeout_secs: u64,
}

pub struct QueryResult {
    pub id: String,
    pub status: TestStatus,
    pub duration: std::time::Duration,
    pub rows: usize,
}

pub enum TestStatus {
    Pass,
    Fail(String),
    Diff(String),
    Skip(String),
    Error(String),
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
fn parse_query_file(content: &str) -> (String, Vec<String>, u64, String) {
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
// Test runner
// ---------------------------------------------------------------------------

/// Run all (or a filtered subset of) benchmark queries and collect results.
pub async fn run_benchmark_test(
    client: &dyn BenchClient,
    benchmark: &str,
    scale: f64,
    query_filter: Option<&str>,
) -> anyhow::Result<Vec<QueryResult>> {
    // TPC-BB queries reference TPC-DS tables, so use the tpcds namespace for resolution.
    let namespace = if benchmark == "tpcbb" {
        crate::bench_namespace("tpcds", scale)
    } else {
        crate::bench_namespace(benchmark, scale)
    };
    let queries = load_query_files(benchmark)?;
    let mut results = Vec::new();

    for query in &queries {
        // Skip if filter provided and this query doesn't match
        if let Some(filter) = query_filter {
            // Accept both "q01" and "1" style filters
            let normalized_filter = normalize_query_id(filter);
            let normalized_id = normalize_query_id(&query.id);
            if normalized_id != normalized_filter {
                continue;
            }
        }

        // Skip if requires unsupported features
        if !query.requires.is_empty() {
            results.push(QueryResult {
                id: query.id.clone(),
                status: TestStatus::Skip(format!("requires: {}", query.requires.join(", "))),
                duration: std::time::Duration::ZERO,
                rows: 0,
            });
            continue;
        }

        // Qualify unqualified table names with the benchmark namespace
        let sql = prefix_tables(&query.sql, &namespace, benchmark);

        let start = std::time::Instant::now();
        match client.execute(&sql).await {
            Ok(batches) => {
                let duration = start.elapsed();
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

                let status = match load_expected(benchmark, scale, &query.id) {
                    Ok(Some(expected)) => match compare_results(&batches, &expected, 1e-4) {
                        Ok(CompareStatus::Pass) => TestStatus::Pass,
                        Ok(CompareStatus::Diff(msg)) => TestStatus::Diff(msg),
                        Ok(CompareStatus::Fail(msg)) => TestStatus::Fail(msg),
                        Err(e) => TestStatus::Error(format!("compare error: {e}")),
                    },
                    // No expected file — just verify the query executes
                    Ok(None) => TestStatus::Pass,
                    Err(e) => TestStatus::Error(format!("failed to load expected: {e}")),
                };

                results.push(QueryResult {
                    id: query.id.clone(),
                    status,
                    duration,
                    rows,
                });
            }
            Err(e) => {
                results.push(QueryResult {
                    id: query.id.clone(),
                    status: TestStatus::Error(e.to_string()),
                    duration: start.elapsed(),
                    rows: 0,
                });
            }
        }
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Strip leading zeros / "q" prefix for flexible filter matching.
/// "q01" → "1", "1" → "1", "q1" → "1"
fn normalize_query_id(id: &str) -> String {
    let trimmed = id.trim_start_matches('q');
    let n: u64 = trimmed.parse().unwrap_or(0);
    n.to_string()
}

/// Replace unqualified table names in the query SQL with
/// `<namespace>.<table>` qualified names.
///
/// Tables are processed longest-name-first to avoid partial replacements
/// (e.g. "partsupp" must be handled before "part").
fn prefix_tables(sql: &str, namespace: &str, benchmark: &str) -> String {
    let gen = match crate::generate::get_generator(benchmark) {
        Ok(g) => g,
        Err(_) => return sql.to_string(),
    };

    let mut tables: Vec<String> = gen.tables().into_iter().map(|t| t.name).collect();
    // Longest first to prevent "part" matching inside "partsupp"
    tables.sort_by_key(|t| std::cmp::Reverse(t.len()));

    let mut result = sql.to_string();
    for table in &tables {
        // Skip tables that are already qualified (contain a dot before them)
        // We replace the most common boundary patterns: surrounding whitespace,
        // comma, newline, tab.
        let qualified = format!("{namespace}.{table}");
        for (pat, rep) in [
            (format!(" {table} "), format!(" {qualified} ")),
            (format!(" {table}\n"), format!(" {qualified}\n")),
            (format!(" {table},"), format!(" {qualified},")),
            (format!(" {table}\t"), format!(" {qualified}\t")),
            (format!(" {table})"), format!(" {qualified})")),
            (format!("\n{table} "), format!("\n{qualified} ")),
            (format!("\n{table}\n"), format!("\n{qualified}\n")),
            (format!("\n{table},"), format!("\n{qualified},")),
        ] {
            result = result.replace(&pat, &rep);
        }
    }
    result
}

/// Try to load the expected results CSV for a query.
///
/// Returns `Ok(None)` when the file does not exist (query runs without
/// validation), `Ok(Some(content))` when found, and `Err` for I/O errors.
fn load_expected(benchmark: &str, scale: f64, query_id: &str) -> anyhow::Result<Option<String>> {
    let path = format!("benchmarks/expected/{benchmark}/sf{scale}/{query_id}.csv");
    match std::fs::read_to_string(&path) {
        Ok(content) => Ok(Some(content)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
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
    fn prefix_tables_qualifies_tpch_tables() {
        let sql = " lineitem WHERE l_shipdate > DATE '1998-01-01'";
        let result = prefix_tables(sql, "tpch_sf1", "tpch");
        assert!(
            result.contains("tpch_sf1.lineitem"),
            "expected qualified table in: {result}"
        );
    }

    #[test]
    fn prefix_tables_longest_first_no_partial() {
        // "partsupp" must be prefixed before "part" to avoid "tpch_sf1.parttpch_sf1.supp"
        let sql = " partsupp , part WHERE ps_partkey = p_partkey";
        let result = prefix_tables(sql, "tpch_sf1", "tpch");
        assert!(
            result.contains("tpch_sf1.partsupp"),
            "partsupp not qualified: {result}"
        );
        // "part" should also be qualified (appears after comma + space)
        assert!(
            result.contains("tpch_sf1.part"),
            "part not qualified: {result}"
        );
        // Should not have double-qualification
        assert!(
            !result.contains("tpch_sf1.tpch_sf1"),
            "double-qualified: {result}"
        );
    }
}
