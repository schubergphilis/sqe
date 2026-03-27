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
    catalog: Option<&str>,
    namespace_override: Option<&str>,
) -> anyhow::Result<Vec<QueryResult>> {
    let ns_base = match namespace_override {
        Some(ns) => ns.to_string(),
        None if benchmark == "tpcbb" => crate::bench_namespace("tpcds", scale),
        None => crate::bench_namespace(benchmark, scale),
    };
    let namespace = match catalog {
        Some(cat) => format!("{cat}.{ns_base}"),
        None => ns_base,
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

        eprintln!("[bench] Running {} ({} chars)...", query.id, sql.len());
        if std::env::var("BENCH_DEBUG").is_ok() {
            eprintln!("[bench] SQL:\n{sql}\n---");
        }

        let timeout_secs = query.timeout_secs.max(120);
        let start = std::time::Instant::now();

        // Use tokio::select! so the timeout fires even if the gRPC stream
        // is stuck in a non-cancellation-safe recv. The losing branch gets
        // dropped, which closes the connection.
        let execute_result = tokio::select! {
            result = client.execute(&sql) => {
                Some(result)
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => {
                eprintln!("[bench] {} TIMEOUT after {}s — skipping", query.id, timeout_secs);
                None
            }
        };

        match execute_result {
            None => {
                results.push(QueryResult {
                    id: query.id.clone(),
                    status: TestStatus::Error(format!("Timed out after {timeout_secs}s")),
                    duration: start.elapsed(),
                    rows: 0,
                });
                continue;
            }
            Some(Err(e)) => {
                results.push(QueryResult {
                    id: query.id.clone(),
                    status: TestStatus::Error(e.to_string()),
                    duration: start.elapsed(),
                    rows: 0,
                });
                continue;
            }
            Some(Ok(batches)) => {
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
/// Uses word-boundary matching: a table name is qualified when it appears
/// as a standalone word (not preceded/followed by `_` or `.`).
/// Tables are processed longest-name-first to avoid partial replacements.
fn prefix_tables(sql: &str, namespace: &str, benchmark: &str) -> String {
    let gen = match crate::generate::get_generator(benchmark) {
        Ok(g) => g,
        Err(_) => return sql.to_string(),
    };

    let mut tables: Vec<String> = gen.tables().into_iter().map(|t| t.name).collect();

    // TPC-BB queries reference TPC-DS tables — include them for qualification
    if benchmark == "tpcbb" {
        if let Ok(tpcds_gen) = crate::generate::get_generator("tpcds") {
            tables.extend(tpcds_gen.tables().into_iter().map(|t| t.name));
        }
    }
    // Longest first to prevent "part" matching inside "partsupp"
    tables.sort_by_key(|t| std::cmp::Reverse(t.len()));

    let mut result = sql.to_string();

    for table in &tables {
        let qualified = format!("{namespace}.{table}");
        let mut output = String::with_capacity(result.len() + 256);
        let mut remaining = result.as_str();

        while let Some(pos) = remaining.find(table.as_str()) {
            // Check character before the match (word boundary)
            let before_ok = if pos == 0 {
                true
            } else {
                let before = remaining.as_bytes()[pos - 1];
                // Not preceded by alphanumeric, underscore, or dot
                !before.is_ascii_alphanumeric() && before != b'_' && before != b'.'
            };

            // Check character after the match
            let end = pos + table.len();
            let after_ok = if end >= remaining.len() {
                true
            } else {
                let after = remaining.as_bytes()[end];
                // Not followed by alphanumeric or underscore (would be part of a longer identifier)
                !after.is_ascii_alphanumeric() && after != b'_'
            };

            if before_ok && after_ok {
                let before_str = &remaining[..pos];
                let trimmed_before = before_str.trim_end();
                let upper_before = trimmed_before.to_uppercase();

                // Skip if preceded by "AS " (this is an alias, not a table ref)
                if upper_before.ends_with(" AS") {
                    output.push_str(&remaining[..end]);
                    remaining = &remaining[end..];
                    continue;
                }

                // Only qualify if preceded by a table-introducing context:
                // FROM, JOIN, TABLE, INTO, UPDATE, or a comma within a FROM/JOIN clause.
                //
                // To determine if a trailing comma is in FROM/JOIN context (vs SELECT/ORDER BY),
                // scan the full text so far (output + current segment) for the last SQL clause keyword.
                let in_table_context = upper_before.ends_with(" FROM")
                    || upper_before.ends_with(" JOIN")
                    || upper_before.ends_with(" TABLE")
                    || upper_before.ends_with(" INTO")
                    || upper_before.ends_with(" UPDATE")
                    || upper_before.ends_with(" EXISTS")
                    // Trailing comma — only qualifies if we're inside a FROM/JOIN clause
                    || (trimmed_before.ends_with(',') && {
                        // Build full context: output so far + current segment before match
                        let full_ctx = format!("{}{}", output, before_str);
                        let full_upper = full_ctx.to_uppercase();
                        // Find the last FROM/JOIN keyword (handles start-of-string, after space, or after newline)
                        let find_keyword = |kw: &str| -> Option<usize> {
                            full_upper.rfind(&format!(" {kw}"))
                                .or_else(|| full_upper.rfind(&format!("\n{kw}")))
                                .or_else(|| if full_upper.starts_with(kw) { Some(0) } else { None })
                        };
                        let from_pos = find_keyword("FROM").unwrap_or(0);
                        let join_pos = find_keyword("JOIN").unwrap_or(0);
                        let table_clause_pos = from_pos.max(join_pos);
                        let has_from = find_keyword("FROM").is_some() || find_keyword("JOIN").is_some();
                        // Check no non-table clause keyword appears after the last FROM/JOIN
                        let after_clause = &full_upper[table_clause_pos..];
                        has_from
                            && !after_clause.contains(" WHERE ")
                            && !after_clause.contains(" SELECT ")
                            && !after_clause.contains(" GROUP ")
                            && !after_clause.contains(" ORDER ")
                            && !after_clause.contains(" HAVING ")
                            && !after_clause.contains(" LIMIT ")
                            && !after_clause.contains(" UNION ")
                    })
                    // Also handle newline after FROM/JOIN (table on next line)
                    || {
                        let words: Vec<&str> = trimmed_before.split_whitespace().collect();
                        words.last().map(|w| {
                            let u = w.to_uppercase();
                            u == "FROM" || u == "JOIN" || u == "TABLE" || u == "INTO"
                        }).unwrap_or(false)
                    };

                if !in_table_context {
                    output.push_str(&remaining[..end]);
                    remaining = &remaining[end..];
                    continue;
                }

                // Check it's not inside a quoted string (simple heuristic:
                // count double quotes before this position — odd means inside quotes)
                let quotes_before = remaining[..pos].matches('"').count();
                if quotes_before.is_multiple_of(2) {
                    output.push_str(&remaining[..pos]);
                    output.push_str(&qualified);
                    remaining = &remaining[end..];
                    continue;
                }
            }

            // Not a match — copy up to and including the found text, continue searching
            output.push_str(&remaining[..end]);
            remaining = &remaining[end..];
        }

        output.push_str(remaining);
        result = output;
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
        let sql = "SELECT * FROM lineitem WHERE l_shipdate > DATE '1998-01-01'";
        let result = prefix_tables(sql, "tpch_sf1", "tpch");
        assert!(
            result.contains("tpch_sf1.lineitem"),
            "expected qualified table in: {result}"
        );
    }

    #[test]
    fn prefix_tables_longest_first_no_partial() {
        // "partsupp" must be prefixed before "part" to avoid partial match
        let sql = "SELECT * FROM partsupp, part WHERE ps_partkey = p_partkey";
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

    #[test]
    fn prefix_tables_does_not_qualify_aliases() {
        let sql = "SELECT cc_call_center_id AS call_center FROM call_center WHERE 1=1";
        let result = prefix_tables(sql, "tpcds_sf1", "tpcds");
        assert!(
            result.contains("FROM tpcds_sf1.call_center"),
            "FROM table should be qualified: {result}"
        );
        assert!(
            result.contains("AS call_center"),
            "alias should not be qualified: {result}"
        );
    }

    #[test]
    fn prefix_tables_comma_list() {
        let sql = "FROM catalog_returns, call_center, customer WHERE 1=1";
        let result = prefix_tables(sql, "tpcds_sf1", "tpcds");
        assert!(result.contains("tpcds_sf1.catalog_returns"), "catalog_returns: {result}");
        assert!(result.contains("tpcds_sf1.call_center"), "call_center: {result}");
        assert!(result.contains("tpcds_sf1.customer"), "customer: {result}");
    }

    #[test]
    fn prefix_tables_q16_partsupp_part() {
        // q16: FROM partsupp, part (same line) + subquery FROM supplier
        let sql = "FROM\n    partsupp,\n    part\nWHERE 1=1 AND ps_suppkey NOT IN (\n    SELECT s_suppkey FROM supplier\n)";
        let result = prefix_tables(sql, "tpch_sf1", "tpch");
        assert!(result.contains("tpch_sf1.partsupp"), "partsupp: {result}");
        assert!(result.contains("tpch_sf1.part"), "part not qualified: {result}");
        assert!(result.contains("FROM tpch_sf1.supplier"), "supplier: {result}");
    }

    #[test]
    fn prefix_tables_aliased_tables() {
        // q07: nation n1, nation n2 — table with alias
        let sql = "FROM\n    supplier,\n    lineitem,\n    orders,\n    customer,\n    nation n1,\n    nation n2\nWHERE 1=1";
        let result = prefix_tables(sql, "tpch_sf1", "tpch");
        assert!(result.contains("tpch_sf1.nation n1"), "nation n1: {result}");
        assert!(result.contains("tpch_sf1.nation n2"), "nation n2: {result}");
    }

    #[test]
    fn prefix_tables_select_comma_no_qualify() {
        // Column aliases named like tables should NOT be qualified in SELECT/ORDER BY
        let sql = "SELECT channel, item, SUM(sales) FROM (SELECT * FROM in_store) x GROUP BY channel, item ORDER BY channel, item";
        let result = prefix_tables(sql, "tpcds_sf1", "tpcds");
        // "item" in SELECT and GROUP BY/ORDER BY should NOT be qualified
        assert!(!result.contains("tpcds_sf1.item"), "item in SELECT should not be qualified: {result}");
        // "store" in CTE name "in_store" should not be qualified either (preceded by _)
        assert!(result.contains("in_store"), "in_store should not be mangled: {result}");
    }

    #[test]
    fn prefix_tables_order_by_no_qualify() {
        // trade_type as alias in ORDER BY should not be qualified
        let sql = "SELECT b_name, tt_name AS trade_type FROM trade JOIN trade_type ON 1=1 ORDER BY trade_type";
        let result = prefix_tables(sql, "tpce_sf1", "tpce");
        // trade_type in FROM/JOIN context should be qualified
        assert!(result.contains("tpce_sf1.trade_type ON"), "FROM trade_type should be qualified: {result}");
        // trade_type in ORDER BY should NOT be qualified (it's a column alias)
        assert!(result.ends_with("ORDER BY trade_type"), "ORDER BY alias should not be qualified: {result}");
    }

    #[test]
    fn prefix_tables_multiline_comma_list() {
        // TPC-H q02 style: FROM with each table on its own line
        let sql = "SELECT * FROM\n    part,\n    supplier,\n    partsupp,\n    nation,\n    region\nWHERE 1=1";
        let result = prefix_tables(sql, "tpch_sf1", "tpch");
        assert!(result.contains("tpch_sf1.part"), "part: {result}");
        assert!(result.contains("tpch_sf1.supplier"), "supplier: {result}");
        assert!(result.contains("tpch_sf1.partsupp"), "partsupp: {result}");
        assert!(result.contains("tpch_sf1.nation"), "nation: {result}");
        assert!(result.contains("tpch_sf1.region"), "region: {result}");
    }
}
