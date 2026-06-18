//! Policy plan rewriter — injects row filters, column masks, and column
//! restrictions into a DataFusion LogicalPlan before optimization.
//!
//! The rewriter walks the plan tree top-down. When it encounters a TableScan,
//! it looks up the resolved policy for that table and user, then:
//! 1. Injects Filter nodes above the TableScan for row-level security
//! 2. Wraps column references in mask expressions for column masking
//! 3. Removes restricted columns from projections
//!
//! This happens BEFORE the optimizer runs, so:
//! - User predicates CAN push through row filters (same semantics)
//! - User predicates CANNOT push through column masks (expression boundary)
//! - Restricted columns are invisible (not errors)

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::DataType;
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{col, lit, Cast, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::scalar::ScalarValue;
use tracing::{debug, warn};

use sqe_core::SessionUser;

use crate::{MaskType, PolicyEnforcer, PolicyStore, ResolvedPolicy};

/// Plan-rewriting policy enforcer. Uses a PolicyStore to resolve policies
/// and rewrites the LogicalPlan accordingly.
pub struct PolicyPlanRewriter {
    store: Arc<dyn PolicyStore>,
    /// HMAC key used by `MaskType::Hash`. When `None` the rewriter falls
    /// back to plain SHA-256, which is vulnerable to offline brute force
    /// against low-entropy columns (issue #37).
    mask_key: Option<Arc<Vec<u8>>>,
}

impl PolicyPlanRewriter {
    pub fn new(store: Arc<dyn PolicyStore>) -> Self {
        Self { store, mask_key: None }
    }

    /// Set the HMAC key used by Hash-type column masks. Pass `None` to
    /// keep the legacy unsalted SHA-256 behavior.
    #[must_use = "with_mask_key consumes self; bind the returned rewriter"]
    pub fn with_mask_key(mut self, mask_key: Option<Arc<Vec<u8>>>) -> Self {
        self.mask_key = mask_key;
        self
    }
}

#[async_trait]
impl PolicyEnforcer for PolicyPlanRewriter {
    async fn evaluate(
        &self,
        user: &SessionUser,
        plan: LogicalPlan,
    ) -> sqe_core::Result<LogicalPlan> {
        let store = self.store.clone();
        let mask_key = self.mask_key.clone();
        let username = user.username.clone();
        let _roles = user.roles.clone();
        let user_clone = user.clone();

        // Collect all TableScan nodes (keyed by their stringified reference,
        // which is what the rewrite phase below matches on) together with the
        // structured `TableReference`. We resolve from the structured form so
        // multi-level Iceberg namespaces survive instead of being lost to a
        // naive split on '.' (issue #205).
        let mut table_refs: HashMap<String, datafusion::common::TableReference> = HashMap::new();
        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                let table_name = scan.table_name.to_string();
                table_refs
                    .entry(table_name)
                    .or_insert_with(|| scan.table_name.clone());
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .map_err(|e| sqe_core::error::SqeError::Execution(format!("Plan traversal failed: {e}")))?;

        // Resolve policies for all tables
        let mut table_policies: HashMap<String, ResolvedPolicy> = HashMap::new();
        for (table_name, table_ref) in &table_refs {
            // Derive the (namespace, table) policy key from the structured
            // reference. This MUST match the write path's scheme
            // (write_handler.rs keys by `namespace().last()`), otherwise
            // reads and writes resolve different policies for the same table.
            let Some((namespace, table)) = resolve_policy_key(table_ref) else {
                // FAIL CLOSED: a reference we cannot confidently map to a
                // policy key must never pass through. Deny all rows rather
                // than risk leaking an unguarded table (issue #205).
                warn!(
                    user = %username,
                    table = %table_name,
                    "Could not resolve policy key for table reference, denying access"
                );
                let mut deny = ResolvedPolicy::default();
                deny.row_filters.push(lit(false));
                table_policies.insert(table_name.clone(), deny);
                continue;
            };

            match store.resolve(&user_clone, &table, &namespace).await {
                Ok(policy) => {
                    debug!(
                        user = %username,
                        table = %table_name,
                        namespace = %namespace,
                        row_filters = policy.row_filters.len(),
                        column_masks = policy.column_masks.len(),
                        restricted = policy.restricted_columns.len(),
                        "Resolved policy for table"
                    );
                    table_policies.insert(table_name.clone(), policy);
                }
                Err(e) => {
                    warn!(
                        user = %username,
                        table = %table_name,
                        error = %e,
                        "Failed to resolve policy, denying access"
                    );
                    // On policy resolution failure, inject a FALSE filter (deny all)
                    let mut deny = ResolvedPolicy::default();
                    deny.row_filters.push(lit(false));
                    table_policies.insert(table_name.clone(), deny);
                }
            }
        }

        // Rewrite the plan
        let rewritten = plan
            .transform_down(|node| {
                if let LogicalPlan::TableScan(ref scan) = node {
                    let table_name = scan.table_name.to_string();
                    if let Some(policy) = table_policies.get(&table_name) {
                        if policy.row_filters.is_empty()
                            && policy.column_masks.is_empty()
                            && policy.restricted_columns.is_empty()
                        {
                            return Ok(Transformed::no(node));
                        }

                        // Build the security wrappers with LogicalPlanBuilder so
                        // injected expressions are NORMALIZED against the real
                        // scan schema. An Iceberg TableScan exposes fully
                        // qualified fields (`catalog.schema.table.col`), while
                        // the policy row filters (parsed schema-free) and the
                        // mask UDF args use BARE column names. Manual
                        // Filter/Projection construction left those refs
                        // unqualified, so physical planning failed with
                        // "No field named <qualified>.col". `filter()` and
                        // `project()` both run `normalize_col`, qualifying every
                        // column reference (including those nested inside Hash /
                        // Custom mask expressions) to the child schema.
                        let mut builder = LogicalPlanBuilder::from(node);

                        // 1. Inject row filters above the TableScan.
                        for filter_expr in &policy.row_filters {
                            builder = builder.filter(filter_expr.clone()).map_err(|e| {
                                datafusion::error::DataFusionError::Internal(format!(
                                    "Failed to create policy filter: {e}"
                                ))
                            })?;
                        }

                        // 2. Apply column masks and/or restrictions via a
                        //    projection that drops restricted columns, masks
                        //    masked columns, and passes the rest through with
                        //    their real (qualified) column reference.
                        if !policy.column_masks.is_empty()
                            || !policy.restricted_columns.is_empty()
                        {
                            let schema = builder.schema().clone();
                            let exprs: Vec<Expr> = schema
                                .iter()
                                .filter(|(_q, f)| {
                                    !policy.restricted_columns.contains(f.name())
                                })
                                .map(|(qualifier, field)| {
                                    let name = field.name();
                                    if let Some(mask) = policy.column_masks.get(name) {
                                        apply_mask(
                                            name,
                                            field.data_type(),
                                            mask,
                                            mask_key.clone(),
                                        )
                                        .alias(name.clone())
                                    } else {
                                        Expr::Column(datafusion::common::Column::new(
                                            qualifier.cloned(),
                                            name,
                                        ))
                                    }
                                })
                                .collect();

                            if !exprs.is_empty() {
                                builder = builder.project(exprs).map_err(|e| {
                                    datafusion::error::DataFusionError::Internal(format!(
                                        "Failed to create policy projection: {e}"
                                    ))
                                })?;
                            }
                        }

                        let current = builder.build().map_err(|e| {
                            datafusion::error::DataFusionError::Internal(format!(
                                "Failed to build policy plan: {e}"
                            ))
                        })?;

                        // Jump: skip descending into the wrappers we just
                        // injected. Continue would re-enter the inner
                        // TableScan and rewrap forever.
                        return Ok(Transformed::new(current, true, TreeNodeRecursion::Jump));
                    }
                }
                Ok(Transformed::no(node))
            })
            .map_err(|e| {
                sqe_core::error::SqeError::Execution(format!("Plan rewrite failed: {e}"))
            })?;

        Ok(rewritten.data)
    }
}

/// Derive the `(namespace, table)` policy key from a structured DataFusion
/// `TableReference`.
///
/// DataFusion's `TableReference` carries at most three slots: catalog,
/// schema, table. Multi-level Iceberg namespaces (`a.b`) are registered as a
/// single DataFusion schema name `"a.b"` (see
/// `sqe-catalog/catalog_provider.rs`), so a 4-part qualified name like
/// `cat.a.b.t` lands here as schema `"a.b"`, table `"t"`.
///
/// The policy key MUST match the write path, which keys by
/// `TableIdent::namespace().last()` (`write_handler.rs`). For a schema of
/// `"a.b"` the last namespace component is `"b"`. We take the last dotted
/// component of the schema so reads and writes resolve the same policy.
///
/// Returns `None` when no table component can be determined. The caller
/// treats `None` as fail-closed (deny all rows) rather than passthrough.
fn resolve_policy_key(
    table_ref: &datafusion::common::TableReference,
) -> Option<(String, String)> {
    let table = table_ref.table();
    if table.is_empty() {
        return None;
    }

    // `schema()` is the (possibly multi-level) namespace string. Take its
    // last dotted component to match the write path's `namespace().last()`.
    // A bare table name with no schema falls back to "default", preserving
    // the existing 1-part behavior.
    let namespace = match table_ref.schema() {
        Some(schema) if !schema.is_empty() => {
            schema.rsplit('.').next().unwrap_or(schema).to_string()
        }
        _ => "default".to_string(),
    };

    Some((namespace, table.to_string()))
}

/// Apply a mask type to a column, returning the masking expression.
///
/// `data_type` is the Arrow type of the original column. The returned
/// expression has the same type so downstream operators (Filter, Join,
/// GroupBy) see the column shape they expect. A Utf8 NULL substituted for
/// a BIGINT column would either fail optimization or, worse, coerce both
/// sides of a predicate to Utf8 and leak masked rows.
fn apply_mask(
    column_name: &str,
    data_type: &DataType,
    mask: &MaskType,
    mask_key: Option<Arc<Vec<u8>>>,
) -> Expr {
    match mask {
        MaskType::Nullify => match ScalarValue::try_from(data_type) {
            Ok(scalar) => lit(scalar),
            // Unsupported Arrow type for typed NULL: cast a Utf8 NULL into
            // the target type so the optimizer still sees the right shape.
            Err(_) => Expr::Cast(Cast::new(
                Box::new(lit(ScalarValue::Utf8(None))),
                data_type.clone(),
            )),
        },
        MaskType::Redact(value) => {
            if matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) {
                lit(value.clone())
            } else {
                Expr::Cast(Cast::new(
                    Box::new(lit(value.clone())),
                    data_type.clone(),
                ))
            }
        }
        MaskType::Hash => {
            // sha256 UDF returns Utf8; cast back to the column type so the
            // projection schema matches. Non-string columns will fail the
            // cast at runtime, which is the correct signal: hashing a
            // BIGINT into itself is meaningless. The UDF runs HMAC-SHA256
            // when `mask_key` is `Some`, plain SHA-256 otherwise. Plain
            // mode is vulnerable to offline brute force on low-entropy
            // values (issue #37).
            let hash = Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    Arc::new(crate::sha256_udf::sha256_udf(mask_key)),
                    vec![col(column_name)],
                ),
            );
            if matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) {
                hash
            } else {
                Expr::Cast(Cast::new(Box::new(hash), data_type.clone()))
            }
        }
        MaskType::Custom(expr) => expr.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_mask_nullify_utf8() {
        let expr = apply_mask("ssn", &DataType::Utf8, &MaskType::Nullify, None);
        match expr {
            Expr::Literal(ScalarValue::Utf8(None), _) => {}
            other => panic!("Expected Utf8 NULL literal, got: {other:?}"),
        }
    }

    #[test]
    fn test_apply_mask_nullify_int64_produces_typed_null() {
        let expr = apply_mask("customer_id", &DataType::Int64, &MaskType::Nullify, None);
        match expr {
            Expr::Literal(ScalarValue::Int64(None), _) => {}
            other => panic!("Expected Int64 NULL literal, got: {other:?}"),
        }
    }

    #[test]
    fn test_apply_mask_nullify_decimal_produces_typed_null() {
        let dt = DataType::Decimal128(18, 2);
        let expr = apply_mask("salary", &dt, &MaskType::Nullify, None);
        match expr {
            Expr::Literal(ScalarValue::Decimal128(None, 18, 2), _) => {}
            other => panic!("Expected Decimal128(None,18,2), got: {other:?}"),
        }
    }

    #[test]
    fn test_apply_mask_nullify_timestamp_produces_typed_null() {
        let dt = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None);
        let expr = apply_mask("ts", &dt, &MaskType::Nullify, None);
        match expr {
            Expr::Literal(ScalarValue::TimestampMicrosecond(None, None), _) => {}
            other => panic!("Expected TimestampMicrosecond NULL, got: {other:?}"),
        }
    }

    #[test]
    fn test_apply_mask_redact_utf8_returns_literal() {
        let expr = apply_mask(
            "ssn",
            &DataType::Utf8,
            &MaskType::Redact("***".to_string()),
            None,
        );
        assert!(matches!(expr, Expr::Literal(ScalarValue::Utf8(Some(_)), _)));
    }

    #[test]
    fn test_apply_mask_redact_int_wraps_in_cast() {
        let expr = apply_mask(
            "id",
            &DataType::Int64,
            &MaskType::Redact("0".to_string()),
            None,
        );
        assert!(
            matches!(expr, Expr::Cast(_)),
            "Expected Cast for Redact on Int64, got: {expr:?}"
        );
    }

    #[test]
    fn test_apply_mask_hash_utf8_produces_scalar_function() {
        let expr = apply_mask("email", &DataType::Utf8, &MaskType::Hash, None);
        assert!(
            matches!(expr, Expr::ScalarFunction(_)),
            "Expected ScalarFunction for Hash mask, got: {expr:?}"
        );
    }

    /// Regression for #37: when a mask key is configured the Hash branch
    /// must still produce a scalar function, but the registered UDF
    /// carries the key so it runs HMAC-SHA256 at execution time. We can't
    /// reach into the boxed ScalarFunction from the LogicalPlan layer, so
    /// the assertion focuses on the structural invariant (still a
    /// function call wrapped in a Cast for non-string types) plus the
    /// keyed-vs-unkeyed branch in the underlying UDF (covered in
    /// `sha256_udf::tests`).
    #[test]
    fn test_apply_mask_hash_with_key_still_produces_scalar_function() {
        let key = Some(Arc::new(b"deployment-key".to_vec()));
        let utf8_expr = apply_mask("email", &DataType::Utf8, &MaskType::Hash, key.clone());
        assert!(matches!(utf8_expr, Expr::ScalarFunction(_)));

        let int_expr = apply_mask("id", &DataType::Int64, &MaskType::Hash, key);
        assert!(matches!(int_expr, Expr::Cast(_)));
    }

    // ── resolve_policy_key (issue #205) ──────────────────────────
    // The policy key MUST match the write path, which uses
    // `namespace().last()`. For a multi-level schema "ns1.ns2" the key is
    // ("ns2", table). A bare table falls back to "default".

    #[test]
    fn resolve_policy_key_bare_table_uses_default_namespace() {
        let r = datafusion::common::TableReference::bare("employees");
        assert_eq!(
            resolve_policy_key(&r),
            Some(("default".to_string(), "employees".to_string()))
        );
    }

    #[test]
    fn resolve_policy_key_two_part_uses_schema_as_namespace() {
        let r = datafusion::common::TableReference::partial("hr", "employees");
        assert_eq!(
            resolve_policy_key(&r),
            Some(("hr".to_string(), "employees".to_string()))
        );
    }

    #[test]
    fn resolve_policy_key_three_part_uses_schema_as_namespace() {
        let r = datafusion::common::TableReference::full("cat", "hr", "employees");
        assert_eq!(
            resolve_policy_key(&r),
            Some(("hr".to_string(), "employees".to_string()))
        );
    }

    #[test]
    fn resolve_policy_key_multilevel_takes_last_namespace_component() {
        // cat.ns1.ns2.employees -> schema "ns1.ns2" -> last component "ns2".
        let r = datafusion::common::TableReference::full("cat", "ns1.ns2", "employees");
        assert_eq!(
            resolve_policy_key(&r),
            Some(("ns2".to_string(), "employees".to_string()))
        );
    }

    #[test]
    fn resolve_policy_key_empty_table_fails_closed() {
        // An empty table component cannot be confidently keyed; the caller
        // treats None as deny-all rather than passthrough.
        let r = datafusion::common::TableReference::bare("");
        assert_eq!(resolve_policy_key(&r), None);
    }

    #[test]
    fn test_apply_mask_custom_returns_provided_expr() {
        let custom_expr = datafusion::logical_expr::lit("REDACTED");
        let result = apply_mask(
            "secret",
            &DataType::Utf8,
            &MaskType::Custom(custom_expr.clone()),
            None,
        );
        assert_eq!(
            format!("{result:?}"),
            format!("{custom_expr:?}"),
            "Custom mask should return the provided expression unchanged"
        );
    }
}
