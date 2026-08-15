//! `CALL system.suggest_bloom_filter_columns` handler.
//!
//! Walks the query log (one SQL string per entry), parses each statement,
//! and counts equality predicates that reference the target table. Returns
//! up to five top columns ranked by predicate count, with a `recommended`
//! flag set on those that appear at least 10% of the time.
//!
//! The design is deliberately simple: no catalog lookups, no join reasoning.
//! Point queries and `=` filters drive bloom filter value; that is what we
//! count. A later enhancement can weight by scanned bytes per query (Phase
//! G once CDC is in).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use sqe_core::SqeError;
use sqe_sql::TableRef;
use sqlparser::ast::{BinaryOperator, Expr, Query, Select, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Default number of history entries to examine.
pub const DEFAULT_HISTORY_LIMIT: usize = 1000;

/// Number of rows returned by `suggest_bloom_filter_columns`.
const TOP_N: usize = 5;

/// Fraction of queries below which a column is not recommended.
const RECOMMEND_THRESHOLD_PCT: f64 = 0.10;

/// Run the analysis. `queries` is a slice of SQL strings; unparseable
/// statements are skipped silently. The returned record batch has columns
/// `(column: Utf8, equality_predicate_count: Int64, recommended: Boolean)`.
pub fn suggest_bloom_filter_columns(
    target: &TableRef,
    queries: &[String],
    history_limit: Option<usize>,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let limit = history_limit.unwrap_or(DEFAULT_HISTORY_LIMIT);
    let window = if queries.len() > limit {
        &queries[queries.len() - limit..]
    } else {
        queries
    };

    let counts = count_equality_predicates(target, window);
    let total_queries = window.len() as f64;

    let mut ranked: Vec<(String, usize)> = counts.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    ranked.truncate(TOP_N);

    build_result_batch(ranked, total_queries)
}

/// Walk every statement, tally equality predicates that reference the target
/// table's columns. Case-folding: SQL is case-insensitive for identifiers; we
/// normalise on lower case and trust the caller to supply a lower-cased
/// target name.
fn count_equality_predicates(target: &TableRef, queries: &[String]) -> HashMap<String, usize> {
    let dialect = GenericDialect {};
    let mut counts: HashMap<String, usize> = HashMap::new();

    for sql in queries {
        let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
            continue;
        };
        for stmt in statements {
            if let Statement::Query(query) = stmt {
                collect_equality_cols_in_query(&query, target, &mut counts);
            }
        }
    }

    counts
}

fn collect_equality_cols_in_query(
    query: &Query,
    target: &TableRef,
    counts: &mut HashMap<String, usize>,
) {
    if let SetExpr::Select(select) = query.body.as_ref() {
        if !select_touches_target(select, target) {
            return;
        }
        if let Some(pred) = &select.selection {
            collect_equality_cols_in_expr(pred, counts);
        }
    }
}

/// Crude table match: look at each FROM item and compare the last identifier
/// segment against `target.name` case-insensitively. A two-part or three-part
/// match also checks namespace. Missing namespace falls through.
fn select_touches_target(select: &Select, target: &TableRef) -> bool {
    for tbl in &select.from {
        let name = table_factor_name(&tbl.relation);
        if matches_table(&name, target) {
            return true;
        }
        for join in &tbl.joins {
            let jname = table_factor_name(&join.relation);
            if matches_table(&jname, target) {
                return true;
            }
        }
    }
    false
}

fn table_factor_name(factor: &sqlparser::ast::TableFactor) -> Vec<String> {
    if let sqlparser::ast::TableFactor::Table { name, .. } = factor {
        return name
            .0
            .iter()
            .filter_map(|p| p.as_ident())
            .map(|p| p.value.to_lowercase())
            .collect();
    }
    Vec::new()
}

fn matches_table(parts: &[String], target: &TableRef) -> bool {
    let tname = target.name.to_lowercase();
    let tns = target.namespace.to_lowercase();
    match parts {
        [name] => *name == tname,
        [ns, name] => *name == tname && *ns == tns,
        [_cat, ns, name] => *name == tname && *ns == tns,
        _ => false,
    }
}

/// Walk an expression tree. Each leaf `col = literal` (or `literal = col`)
/// bumps the column's counter. Boolean connectives (`AND`, `OR`) recurse.
fn collect_equality_cols_in_expr(expr: &Expr, counts: &mut HashMap<String, usize>) {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq => {
                if let Some(col) = extract_column_from_equality(left, right) {
                    *counts.entry(col).or_insert(0) += 1;
                }
            }
            BinaryOperator::And | BinaryOperator::Or => {
                collect_equality_cols_in_expr(left, counts);
                collect_equality_cols_in_expr(right, counts);
            }
            _ => {}
        },
        Expr::InList { expr, list, .. } if !list.is_empty() => {
            // `col IN (lit, lit, ...)` is equivalent to equality probes.
            if let Expr::Identifier(ident) = expr.as_ref() {
                *counts.entry(ident.value.to_lowercase()).or_insert(0) += 1;
            } else if let Expr::CompoundIdentifier(parts) = expr.as_ref() {
                if let Some(last) = parts.last() {
                    *counts.entry(last.value.to_lowercase()).or_insert(0) += 1;
                }
            }
        }
        Expr::Nested(inner) => collect_equality_cols_in_expr(inner, counts),
        _ => {}
    }
}

/// Return the column name if the pair is `col = literal` or `literal = col`.
fn extract_column_from_equality(left: &Expr, right: &Expr) -> Option<String> {
    if is_literal(left) && is_column(right) {
        return column_name(right);
    }
    if is_literal(right) && is_column(left) {
        return column_name(left);
    }
    None
}

fn is_column(e: &Expr) -> bool {
    matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
}

fn column_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.to_lowercase()),
        _ => None,
    }
}

fn is_literal(e: &Expr) -> bool {
    matches!(
        e,
        Expr::Value(_) | Expr::TypedString { .. } | Expr::Cast { .. }
    )
}

fn build_result_batch(
    ranked: Vec<(String, usize)>,
    total_queries: f64,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("column", DataType::Utf8, false),
        Field::new("equality_predicate_count", DataType::Int64, false),
        Field::new("recommended", DataType::Boolean, false),
    ]));

    let columns: Vec<String> = ranked.iter().map(|(c, _)| c.clone()).collect();
    let counts: Vec<i64> = ranked.iter().map(|(_, n)| *n as i64).collect();
    let recommended: Vec<bool> = ranked
        .iter()
        .map(|(_, n)| {
            if total_queries == 0.0 {
                false
            } else {
                (*n as f64 / total_queries) >= RECOMMEND_THRESHOLD_PCT
            }
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(columns)),
            Arc::new(Int64Array::from(counts)),
            Arc::new(BooleanArray::from(recommended)),
        ],
    )
    .map_err(|e| SqeError::Execution(format!("Failed to build suggestion batch: {e}")))?;
    Ok(vec![batch])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    fn tref(ns: &str, name: &str) -> TableRef {
        TableRef {
            catalog: None,
            namespace: ns.to_string(),
            name: name.to_string(),
        }
    }

    #[test]
    fn empty_query_log_returns_empty_batch() {
        let t = tref("ns", "t");
        let batches = suggest_bloom_filter_columns(&t, &[], None).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn counts_equality_predicates_on_target_table() {
        let t = tref("ns", "orders");
        let q = vec![
            "SELECT * FROM ns.orders WHERE customer_id = 42".to_string(),
            "SELECT * FROM ns.orders WHERE customer_id = 99".to_string(),
            "SELECT * FROM ns.orders WHERE product_id = 'x'".to_string(),
        ];
        let batches = suggest_bloom_filter_columns(&t, &q, None).unwrap();
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2);
        let cols = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let counts = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(cols.value(0), "customer_id");
        assert_eq!(counts.value(0), 2);
        assert_eq!(cols.value(1), "product_id");
        assert_eq!(counts.value(1), 1);
    }

    #[test]
    fn recommended_above_threshold() {
        // 100 queries, 80 on customer_id, 20 on product_id.
        let t = tref("ns", "orders");
        let mut q: Vec<String> = Vec::new();
        for _ in 0..80 {
            q.push("SELECT * FROM ns.orders WHERE customer_id = 1".to_string());
        }
        for _ in 0..20 {
            q.push("SELECT * FROM ns.orders WHERE product_id = 'x'".to_string());
        }
        let batches = suggest_bloom_filter_columns(&t, &q, None).unwrap();
        let b = &batches[0];
        let cols = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let rec = b.column(2).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(cols.value(0), "customer_id");
        assert!(rec.value(0), "customer_id should be recommended");
        assert_eq!(cols.value(1), "product_id");
        assert!(rec.value(1), "product_id still >= 10% of total");
    }

    #[test]
    fn below_threshold_not_recommended() {
        // 1000 queries, 50 on column x (5%, below 10%)
        let t = tref("ns", "orders");
        let mut q: Vec<String> = Vec::new();
        for _ in 0..50 {
            q.push("SELECT * FROM ns.orders WHERE x = 1".to_string());
        }
        for _ in 0..950 {
            q.push("SELECT * FROM ns.orders".to_string());
        }
        let batches = suggest_bloom_filter_columns(&t, &q, None).unwrap();
        let b = &batches[0];
        let rec = b.column(2).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!rec.value(0), "5% should not be recommended");
    }

    #[test]
    fn in_list_treated_as_equality() {
        let t = tref("ns", "orders");
        let q = vec!["SELECT * FROM ns.orders WHERE customer_id IN (1, 2, 3)".to_string()];
        let batches = suggest_bloom_filter_columns(&t, &q, None).unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 1);
        let cols = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(cols.value(0), "customer_id");
    }

    #[test]
    fn non_target_queries_ignored() {
        let t = tref("ns", "orders");
        let q = vec![
            "SELECT * FROM other.table WHERE x = 1".to_string(),
            "SELECT * FROM ns.other WHERE y = 2".to_string(),
        ];
        let batches = suggest_bloom_filter_columns(&t, &q, None).unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn and_predicates_all_counted() {
        let t = tref("ns", "t");
        let q = vec!["SELECT * FROM ns.t WHERE a = 1 AND b = 2".to_string()];
        let batches = suggest_bloom_filter_columns(&t, &q, None).unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2);
    }

    #[test]
    fn caps_history_window() {
        let t = tref("ns", "t");
        let mut q: Vec<String> = Vec::new();
        for _ in 0..50 {
            q.push("SELECT * FROM ns.t WHERE old = 1".to_string());
        }
        for _ in 0..10 {
            q.push("SELECT * FROM ns.t WHERE new = 1".to_string());
        }
        // Only the last 10 are inspected
        let batches = suggest_bloom_filter_columns(&t, &q, Some(10)).unwrap();
        let b = &batches[0];
        let cols = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(cols.value(0), "new");
    }

    #[test]
    fn only_top_five_returned() {
        let t = tref("ns", "t");
        let mut q: Vec<String> = Vec::new();
        for col_idx in 0..10 {
            for _ in 0..(10 - col_idx) {
                q.push(format!("SELECT * FROM ns.t WHERE c{col_idx} = 1"));
            }
        }
        let batches = suggest_bloom_filter_columns(&t, &q, None).unwrap();
        assert!(batches[0].num_rows() <= TOP_N);
    }
}
