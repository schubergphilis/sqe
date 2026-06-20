//! Write-path predicate extraction.
//!
//! DELETE and UPDATE handlers in the coordinator read parquet files directly
//! and evaluate the user's WHERE clause per batch through a synthesised SQL
//! statement. The standard `PolicyEnforcer::evaluate` injects row filters
//! and column masks above a TableScan, which the SELECT path consumes by
//! re-executing the rewritten plan. The DML path needs the same effect but
//! in SQL-fragment form because the per-batch evaluator is a string.
//!
//! This module runs the rewriter against a synthetic TableScan over the
//! target's Arrow schema, then walks the rewritten plan to pull out:
//! - the row-filter expression (multiple `Filter` nodes are ANDed),
//! - the per-column mask expression (entries of the `Projection` whose
//!   alias body is not a plain `Expr::Column` reference to the same name).
//!
//! Each `Expr` is unparsed to SQL text so the coordinator can splice it
//! into the WHERE clause and the SET RHS.
//!
//! Multi-level namespaces are flattened to the last component, matching
//! the SELECT-side rewriter that does the same when keying its policy
//! lookup. A namespace key drift here would silently bypass policies on
//! DML, so the SELECT path and DML path resolve through the same string.
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::sql::unparser::expr_to_sql;

use sqe_core::error::SqeError;
use sqe_core::SessionUser;

use crate::PolicyEnforcer;

/// SQL fragments derived from a resolved policy for use by DELETE / UPDATE.
#[derive(Debug, Clone, Default)]
pub struct WritePolicyPredicates {
    /// Row-filter SQL. AND of every row filter on the target. `None` when
    /// the policy is empty.
    pub row_filter_sql: Option<String>,
    /// Per-column mask SQL. Each entry maps a target column name to the
    /// SQL expression that should be substituted for a bare reference to
    /// that column in WHERE or SET RHS.
    pub column_mask_sqls: HashMap<String, String>,
}

impl WritePolicyPredicates {
    /// `true` when both halves of the predicate are empty.
    pub fn is_empty(&self) -> bool {
        self.row_filter_sql.is_none() && self.column_mask_sqls.is_empty()
    }
}

/// Run the configured `PolicyEnforcer` against a synthetic TableScan over
/// the target table and project its row filter and column masks into SQL
/// fragments the DML handlers can splice into their per-batch evaluator.
///
/// `namespace_key` and `table_name` form the `TableReference` the rewriter
/// uses to look up policy. The SELECT path keys lookup the same way; see
/// `plan_rewriter::PolicyPlanRewriter::evaluate`.
pub async fn extract_write_predicates(
    enforcer: &dyn PolicyEnforcer,
    user: &SessionUser,
    namespace_key: &str,
    table_name: &str,
    schema: Arc<ArrowSchema>,
) -> sqe_core::Result<WritePolicyPredicates> {
    let table_ref = TableReference::partial(
        namespace_key.to_string(),
        table_name.to_string(),
    );

    let empty_batches: Vec<Vec<arrow_array::RecordBatch>> = vec![vec![]];
    let mem = Arc::new(
        MemTable::try_new(schema, empty_batches)
            .map_err(|e| SqeError::Execution(format!("policy probe MemTable: {e}")))?,
    );
    let plan = LogicalPlanBuilder::scan(table_ref, provider_as_source(mem), None)
        .and_then(|b| b.build())
        .map_err(|e| SqeError::Execution(format!("policy probe scan: {e}")))?;

    let (rewritten, _summary) = enforcer.evaluate(user, plan).await?;
    Ok(unparse_predicates(&rewritten))
}

/// Walk a rewritten plan and pull row filters + masks out as SQL fragments.
/// Public so tests in this crate and downstream crates can exercise the
/// unparser without rebuilding the full extraction pipeline.
pub fn unparse_predicates(rewritten: &LogicalPlan) -> WritePolicyPredicates {
    let mut row_filters: Vec<Expr> = Vec::new();
    let mut column_mask_sqls: HashMap<String, String> = HashMap::new();
    let mut node = rewritten;

    loop {
        match node {
            LogicalPlan::Filter(f) => {
                row_filters.push(f.predicate.clone());
                node = f.input.as_ref();
            }
            LogicalPlan::Projection(p) => {
                for expr in &p.expr {
                    let (alias_name, body) = match expr {
                        Expr::Alias(a) => (a.name.clone(), a.expr.as_ref()),
                        Expr::Column(c) => (c.name.clone(), expr),
                        _ => continue,
                    };
                    if matches!(body, Expr::Column(c) if c.name == alias_name) {
                        continue;
                    }
                    if let Ok(sql) = expr_to_sql(body) {
                        column_mask_sqls.insert(alias_name, sql.to_string());
                    }
                }
                node = p.input.as_ref();
            }
            _ => break,
        }
    }

    let row_filter_sql = if row_filters.is_empty() {
        None
    } else {
        let combined = row_filters
            .into_iter()
            .reduce(|a, b| a.and(b))
            .expect("non-empty by branch");
        expr_to_sql(&combined).ok().map(|s| s.to_string())
    };

    WritePolicyPredicates {
        row_filter_sql,
        column_mask_sqls,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_rewriter::PolicyPlanRewriter;
    use crate::policy_store::InMemoryPolicyStore;
    use crate::{MaskType, ResolvedPolicy};
    use arrow_schema::{DataType, Field};
    use datafusion::logical_expr::{col, lit};

    fn user(name: &str) -> SessionUser {
        SessionUser {
            username: name.to_string(),
            roles: vec![],
        }
    }

    fn employees_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("ssn", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
        ]))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_policy_yields_no_predicates() {
        let store = Arc::new(InMemoryPolicyStore::new());
        let enf = PolicyPlanRewriter::new(store);
        let out = extract_write_predicates(
            &enf,
            &user("alice"),
            "default",
            "employees",
            employees_schema(),
        )
        .await
        .unwrap();
        assert!(out.is_empty(), "expected empty predicates, got {out:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn row_filter_round_trips_to_sql() {
        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.row_filters.push(col("region").eq(lit("EU")));
        store.add_table_policy("default", "employees", pol).await;
        let enf = PolicyPlanRewriter::new(Arc::new(store));

        let out = extract_write_predicates(
            &enf,
            &user("alice"),
            "default",
            "employees",
            employees_schema(),
        )
        .await
        .unwrap();

        let s = out.row_filter_sql.expect("row filter present");
        assert!(s.contains("region"), "row filter sql: {s}");
        assert!(s.contains("EU"), "row filter sql: {s}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn redact_mask_round_trips_to_sql() {
        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.column_masks
            .insert("ssn".to_string(), MaskType::Redact("***".to_string()));
        store.add_table_policy("default", "employees", pol).await;
        let enf = PolicyPlanRewriter::new(Arc::new(store));

        let out = extract_write_predicates(
            &enf,
            &user("alice"),
            "default",
            "employees",
            employees_schema(),
        )
        .await
        .unwrap();

        let mask = out
            .column_mask_sqls
            .get("ssn")
            .cloned()
            .expect("ssn mask present");
        assert!(mask.contains("***"), "redact mask sql: {mask}");
        assert!(
            !out.column_mask_sqls.contains_key("id"),
            "unmasked column should have no entry"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn nullify_on_int_round_trips_to_typed_null() {
        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.column_masks
            .insert("id".to_string(), MaskType::Nullify);
        store.add_table_policy("default", "employees", pol).await;
        let enf = PolicyPlanRewriter::new(Arc::new(store));

        let out = extract_write_predicates(
            &enf,
            &user("alice"),
            "default",
            "employees",
            employees_schema(),
        )
        .await
        .unwrap();

        let mask = out
            .column_mask_sqls
            .get("id")
            .cloned()
            .expect("id mask present");
        let upper = mask.to_uppercase();
        assert!(upper.contains("NULL"), "nullify mask sql: {mask}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn row_filter_and_mask_extracted_together() {
        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.row_filters.push(col("region").eq(lit("EU")));
        pol.column_masks
            .insert("ssn".to_string(), MaskType::Redact("***".to_string()));
        store.add_table_policy("default", "employees", pol).await;
        let enf = PolicyPlanRewriter::new(Arc::new(store));

        let out = extract_write_predicates(
            &enf,
            &user("alice"),
            "default",
            "employees",
            employees_schema(),
        )
        .await
        .unwrap();

        assert!(out.row_filter_sql.is_some());
        assert!(out.column_mask_sqls.contains_key("ssn"));
    }
}
