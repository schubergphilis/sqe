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
//! into the WHERE clause and the SET RHS. A HASH mask unparses to the
//! internal keyed function, which no user session registers; the write path
//! plans masked relations in a separate context with [`WritePolicyPredicates::mask_udfs`]
//! registered and never splices mask text into SQL planned in the user's
//! session.
//!
//! The probe scan carries the target's FULL dotted namespace as its schema,
//! which is exactly what the SELECT planner puts in a `TableScan`'s
//! `TableReference`, so the rewriter derives the identical policy key for
//! reads and DML (`plan_rewriter::resolve_policy_key`). Never flatten the
//! namespace here: a key drift silently bypasses row filters on DML
//! (architecture review 201).
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::expr_rewriter::unnormalize_col;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF};
use datafusion::sql::unparser::expr_to_sql;

use sqe_core::error::{SqeError, SqeErrorCode};
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
    /// Internal functions the mask SQL calls (the keyed HASH mask,
    /// [`crate::sha256_udf::MASK_HASH_UDF_NAME`]). They are never registered
    /// in a user session, so the write path plans the masked relation in a
    /// separate context that has them and hands the user SQL only the
    /// resulting relation. Each carries the key the rewriter applied.
    pub mask_udfs: Vec<Arc<ScalarUDF>>,
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
/// `namespace_key` is the target's full dotted namespace (`ns.to_string()`
/// of the Iceberg `NamespaceIdent`) and together with `table_name` forms the
/// `TableReference` the rewriter keys policy on, the same way the SELECT
/// planner does; see `plan_rewriter::resolve_policy_key`.
///
/// `catalog` is the catalog the target lives in (the statement's qualifier,
/// else the session's). With it the probe reference is `catalog.ns.table`,
/// exactly what a `SELECT` on the same table is governed as; a
/// `catalog_scoped` store keys on it, and without it that store denies. A
/// Hive target passes `None` with its `{root}.{database}` key as
/// `namespace_key`, which is already the Hive key.
pub async fn extract_write_predicates(
    enforcer: &dyn PolicyEnforcer,
    user: &SessionUser,
    catalog: Option<&str>,
    namespace_key: &str,
    table_name: &str,
    schema: Arc<ArrowSchema>,
) -> sqe_core::Result<(WritePolicyPredicates, crate::PolicySummary)> {
    let table_ref = match catalog.filter(|c| !c.is_empty()) {
        Some(catalog) => TableReference::full(
            catalog.to_string(),
            namespace_key.to_string(),
            table_name.to_string(),
        ),
        None => TableReference::partial(namespace_key.to_string(), table_name.to_string()),
    };

    let empty_batches: Vec<Vec<arrow_array::RecordBatch>> = vec![vec![]];
    let mem = Arc::new(
        MemTable::try_new(schema, empty_batches)
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("policy probe MemTable: {e}")))?,
    );
    let plan = LogicalPlanBuilder::scan(table_ref, provider_as_source(mem), None)
        .and_then(|b| b.build())
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("policy probe scan: {e}")))?;

    // The reference above is already qualified; no scope to apply.
    let (rewritten, summary) = enforcer
        .evaluate(user, &crate::TableScope::default(), plan)
        .await?;
    Ok((unparse_predicates(&rewritten)?, summary))
}

/// Walk a rewritten plan and pull row filters + masks out as SQL fragments.
/// Public so tests in this crate and downstream crates can exercise the
/// unparser without rebuilding the full extraction pipeline.
///
/// Fails closed: a row filter or mask that cannot be rendered as SQL is an
/// error, never silently dropped (a dropped row filter would let DELETE
/// without WHERE remove rows the caller cannot see).
pub fn unparse_predicates(rewritten: &LogicalPlan) -> sqe_core::Result<WritePolicyPredicates> {
    let unrenderable = |what: String, e: datafusion::error::DataFusionError| {
        SqeError::with_code(
            SqeErrorCode::AccessDenied,
            format!("write refused: the {what} cannot be applied to this statement: {e}"),
        )
    };
    let mut row_filters: Vec<Expr> = Vec::new();
    let mut column_mask_sqls: HashMap<String, String> = HashMap::new();
    let mut mask_udfs: Vec<Arc<ScalarUDF>> = Vec::new();
    let mut node = rewritten;

    loop {
        match node {
            LogicalPlan::Filter(f) => {
                row_filters.push(f.predicate.clone());
                node = f.input.as_ref();
            }
            // The rewriter's row-filter guard (`CASE WHEN <filter> THEN col
            // END`) is not a mask; the DML path applies the row filter
            // itself. Matched by shape: the mask projection also sits
            // directly over the row filters when there is no guard.
            LogicalPlan::Projection(p) if is_row_filter_guard(p) => {
                node = p.input.as_ref();
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
                    // A Nullify mask or restricted column is a typed NULL. For
                    // struct / map / list types that is a nested ScalarValue
                    // (or a CAST to a nested type) the unparser cannot render,
                    // which would refuse every DML on the table even when the
                    // column is never referenced. Plain `NULL` hides the value
                    // just as well, and is what the unparser already emits for
                    // scalar typed NULLs (and what the WITH CHECK nullify test
                    // matches on).
                    let sql = if is_null_literal(body) {
                        "NULL".to_string()
                    } else {
                        collect_mask_udfs(body, &mut mask_udfs);
                        // The rewriter's projection qualifies column refs with
                        // the probe table (`employees.ssn`). The write path
                        // evaluates a mask over the bare scratch relation, where
                        // that qualifier does not resolve; the mask reads one
                        // table, so the bare name is exact.
                        expr_to_sql(&unnormalize_col(body.clone()))
                            .map_err(|e| unrenderable(format!("column mask on `{alias_name}`"), e))?
                            .to_string()
                    };
                    column_mask_sqls.insert(alias_name, sql);
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
        let sql = expr_to_sql(&combined).map_err(|e| unrenderable("row filter".to_string(), e))?;
        Some(sql.to_string())
    };

    Ok(WritePolicyPredicates {
        row_filter_sql,
        column_mask_sqls,
        mask_udfs,
    })
}

/// True for the rewriter's row-filter guard: every entry is
/// `CASE WHEN <cond> THEN col END AS col`, over a row filter.
fn is_row_filter_guard(p: &datafusion::logical_expr::Projection) -> bool {
    matches!(p.input.as_ref(), LogicalPlan::Filter(_))
        && !p.expr.is_empty()
        && p.expr.iter().all(|e| match e {
            Expr::Alias(a) => match a.expr.as_ref() {
                Expr::Case(c) => {
                    c.expr.is_none()
                        && c.else_expr.is_none()
                        && c.when_then_expr.len() == 1
                        && matches!(c.when_then_expr[0].1.as_ref(),
                            Expr::Column(col) if col.name == a.name)
                }
                _ => false,
            },
            _ => false,
        })
}

/// Collect the internal mask functions `expr` calls (the keyed HASH mask is
/// wrapped in a CAST for non-string columns, hence the walk), once per name.
fn collect_mask_udfs(expr: &Expr, out: &mut Vec<Arc<ScalarUDF>>) {
    // The closure never fails; the walk result carries no information.
    let _ = expr.apply(|e| {
        if let Expr::ScalarFunction(f) = e {
            let name = f.func.name();
            if name == crate::sha256_udf::MASK_HASH_UDF_NAME
                && !out.iter().any(|u| u.name() == name)
            {
                out.push(Arc::clone(&f.func));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

/// `true` for a NULL literal of any type, or a CAST / TRY_CAST of one (the
/// shapes `plan_rewriter::typed_null` produces).
fn is_null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(v, _) => v.is_null(),
        Expr::Cast(c) => is_null_literal(&c.expr),
        Expr::TryCast(c) => is_null_literal(&c.expr),
        _ => false,
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
            subject: None,
            email: None,
            groups: vec![],
        }
    }

    fn employees_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("ssn", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
        ]))
    }

    // A row filter or mask the unparser cannot render must fail the write,
    // never be dropped (a dropped row filter turns DELETE into a truncate).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unrenderable_policy_fails_closed() {
        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        let duration = || lit(datafusion::common::ScalarValue::DurationSecond(Some(1)));
        pol.row_filters.push(col("region").eq(lit("EU")).and(duration().is_not_null()));
        store.add_table_policy("default", "employees", pol).await;
        let enf = PolicyPlanRewriter::new(Arc::new(store));
        let res = extract_write_predicates(
            &enf,
            &user("alice"),
            None,
            "default",
            "employees",
            employees_schema(),
        )
        .await;
        assert!(res.is_err(), "unrenderable row filter must error, got {res:?}");

        let scan = LogicalPlanBuilder::scan(
            "employees",
            provider_as_source(Arc::new(
                MemTable::try_new(employees_schema(), vec![vec![]]).unwrap(),
            )),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let masked = LogicalPlanBuilder::from(scan)
            .project(vec![
                col("id"),
                duration().alias("ssn"),
                col("region"),
            ])
            .unwrap()
            .build()
            .unwrap();
        assert!(unparse_predicates(&masked).is_err(), "unrenderable mask must error");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_policy_yields_no_predicates() {
        let store = Arc::new(InMemoryPolicyStore::new());
        let enf = PolicyPlanRewriter::new(store);
        let (out, _summary) = extract_write_predicates(
            &enf,
            &user("alice"),
            None,
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

        let (out, summary) = extract_write_predicates(
            &enf,
            &user("alice"),
            None,
            "default",
            "employees",
            employees_schema(),
        )
        .await
        .unwrap();

        assert_eq!(summary.row_filters_applied, 1);
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

        let (out, summary) = extract_write_predicates(
            &enf,
            &user("alice"),
            None,
            "default",
            "employees",
            employees_schema(),
        )
        .await
        .unwrap();

        assert_eq!(summary.columns_masked, vec!["ssn".to_string()]);
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

    // With a row filter and no guard, the mask projection sits directly over
    // the row filter; it must still be read as the mask.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mask_over_a_row_filter_is_extracted() {
        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.row_filters.push(col("region").eq(lit("EU")));
        pol.column_masks
            .insert("ssn".to_string(), MaskType::Redact("***".to_string()));
        store.add_table_policy("default", "employees", pol).await;
        let enf = PolicyPlanRewriter::new(Arc::new(store));

        let (out, _summary) = extract_write_predicates(
            &enf,
            &user("alice"),
            None,
            "default",
            "employees",
            employees_schema(),
        )
        .await
        .unwrap();

        assert!(out.row_filter_sql.is_some(), "row filter present");
        let mask = out.column_mask_sqls.get("ssn").expect("ssn mask present");
        assert!(mask.contains("***"), "redact mask sql: {mask}");
    }

    // The row-filter guard is skipped, not read as a mask on every column.
    #[test]
    fn row_filter_guard_is_not_a_mask() {
        use datafusion::logical_expr::when;
        let scan = LogicalPlanBuilder::scan(
            "employees",
            provider_as_source(Arc::new(
                MemTable::try_new(employees_schema(), vec![vec![]]).unwrap(),
            )),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let rf = col("region").eq(lit("EU"));
        let guard = ["id", "ssn", "region"]
            .map(|c| when(rf.clone(), col(c)).end().unwrap().alias(c))
            .to_vec();
        let plan = LogicalPlanBuilder::from(scan)
            .filter(rf.clone())
            .unwrap()
            .project(guard)
            .unwrap()
            .project(vec![col("id"), lit("***").alias("ssn"), col("region")])
            .unwrap()
            .build()
            .unwrap();
        let out = unparse_predicates(&plan).unwrap();
        assert!(out.row_filter_sql.is_some(), "row filter present");
        assert_eq!(out.column_mask_sqls.len(), 1, "only ssn is masked: {out:?}");
        assert!(out.column_mask_sqls["ssn"].contains("***"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn nullify_on_int_round_trips_to_typed_null() {
        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.column_masks.insert("id".to_string(), MaskType::Nullify);
        store.add_table_policy("default", "employees", pol).await;
        let enf = PolicyPlanRewriter::new(Arc::new(store));

        let (out, summary) = extract_write_predicates(
            &enf,
            &user("alice"),
            None,
            "default",
            "employees",
            employees_schema(),
        )
        .await
        .unwrap();

        assert_eq!(summary.columns_masked, vec!["id".to_string()]);
        let mask = out
            .column_mask_sqls
            .get("id")
            .cloned()
            .expect("id mask present");
        let upper = mask.to_uppercase();
        assert!(upper.contains("NULL"), "nullify mask sql: {mask}");
    }

    // A restricted or Nullify-masked struct / map column is a typed NULL the
    // unparser cannot render; it must still yield a predicate set (plain
    // NULL), not refuse every DML on the table.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restricted_and_nullified_nested_columns_render_as_null() {
        use arrow_schema::Fields;
        let addr = DataType::Struct(Fields::from(vec![
            Field::new("street", DataType::Utf8, true),
            Field::new("zip", DataType::Int32, true),
        ]));
        let attrs = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("addr", addr, true),
            Field::new("attrs", attrs, true),
        ]));

        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.restricted_columns.push("addr".to_string());
        pol.column_masks
            .insert("attrs".to_string(), MaskType::Nullify);
        store.add_table_policy("default", "people", pol).await;
        let enf = PolicyPlanRewriter::new(Arc::new(store));

        let (out, _) = extract_write_predicates(&enf, &user("alice"), None, "default", "people", schema)
            .await
            .expect("nested typed NULL masks must render");
        assert_eq!(
            out.column_mask_sqls.get("addr").map(String::as_str),
            Some("NULL")
        );
        assert_eq!(
            out.column_mask_sqls.get("attrs").map(String::as_str),
            Some("NULL")
        );
        assert!(!out.column_mask_sqls.contains_key("id"));
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

        let (out, summary) = extract_write_predicates(
            &enf,
            &user("alice"),
            None,
            "default",
            "employees",
            employees_schema(),
        )
        .await
        .unwrap();

        assert_eq!(summary.row_filters_applied, 1);
        assert_eq!(summary.columns_masked, vec!["ssn".to_string()]);
        assert!(out.row_filter_sql.is_some());
        assert!(out.column_mask_sqls.contains_key("ssn"));
    }

    // A HASH mask renders as the internal keyed function, and the predicates
    // carry that function (with its key) so the write path can plan the mask
    // without registering it in the user's session.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hash_mask_carries_internal_udf() {
        use crate::sha256_udf::MASK_HASH_UDF_NAME;
        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.column_masks.insert("ssn".to_string(), MaskType::Hash);
        pol.column_masks
            .insert("region".to_string(), MaskType::Redact("***".to_string()));
        store.add_table_policy("default", "employees", pol).await;
        let enf =
            PolicyPlanRewriter::new(Arc::new(store)).with_mask_key(Some(Arc::new(b"k".to_vec())));

        let (out, _) = extract_write_predicates(
            &enf,
            &user("alice"),
            None,
            "default",
            "employees",
            employees_schema(),
        )
        .await
        .unwrap();

        let mask = out.column_mask_sqls.get("ssn").expect("ssn mask present");
        assert!(mask.contains(MASK_HASH_UDF_NAME), "hash mask sql: {mask}");
        assert!(
            !mask.contains("employees."),
            "the mask reads the bare scratch relation, so its columns are unqualified: {mask}"
        );
        let names: Vec<&str> = out.mask_udfs.iter().map(|u| u.name()).collect();
        assert_eq!(names, vec![MASK_HASH_UDF_NAME]);
    }

    /// The DML probe is governed as `catalog.ns.table` like a SELECT: a
    /// catalog-scoped store finds the target's row filter under its catalog,
    /// and without a catalog the probe is denied rather than keyed bare.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catalog_scoped_probe_keys_on_the_target_catalog() {
        use crate::PolicyStore;
        struct Scoped(InMemoryPolicyStore);
        #[async_trait::async_trait]
        impl PolicyStore for Scoped {
            fn catalog_scoped(&self) -> bool {
                true
            }
            async fn resolve(
                &self,
                user: &SessionUser,
                table: &str,
                namespace: &str,
            ) -> sqe_core::Result<ResolvedPolicy> {
                self.0.resolve(user, table, namespace).await
            }
        }
        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.row_filters.push(col("region").eq(lit("EU")));
        store
            .add_table_policy("ws_a.default", "employees", pol)
            .await;
        let enf = PolicyPlanRewriter::new(Arc::new(Scoped(store)));

        let probe = |catalog: Option<&'static str>| {
            let enf = &enf;
            async move {
                extract_write_predicates(
                    enf,
                    &user("alice"),
                    catalog,
                    "default",
                    "employees",
                    employees_schema(),
                )
                .await
                .unwrap()
            }
        };
        let (a, _) = probe(Some("ws_a")).await;
        assert!(
            a.row_filter_sql
                .as_deref()
                .is_some_and(|f| f.contains("EU")),
            "{a:?}"
        );
        let (b, _) = probe(Some("ws_b")).await;
        assert!(
            b.row_filter_sql.is_none(),
            "another catalog's table is not filtered: {b:?}"
        );
        let (_, none) = probe(None).await;
        assert!(none.denied, "no catalog: denied, not keyed without one");
    }
}
