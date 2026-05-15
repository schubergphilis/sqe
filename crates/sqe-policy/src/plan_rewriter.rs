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
use datafusion::logical_expr::{col, lit, Cast, Expr, Filter, LogicalPlan, Projection};
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

        // Collect all TableScan nodes and their policies
        let mut table_policies: HashMap<String, ResolvedPolicy> = HashMap::new();
        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                let table_name = scan.table_name.to_string();
                // We'll resolve policies after collecting all table names
                table_policies.entry(table_name).or_default();
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .map_err(|e| sqe_core::error::SqeError::Execution(format!("Plan traversal failed: {e}")))?;

        // Resolve policies for all tables
        for table_name in table_policies.keys().cloned().collect::<Vec<_>>() {
            let parts: Vec<&str> = table_name.split('.').collect();
            let (namespace, table) = match parts.as_slice() {
                [_catalog, ns, t] => (ns.to_string(), t.to_string()),
                [ns, t] => (ns.to_string(), t.to_string()),
                [t] => ("default".to_string(), t.to_string()),
                _ => continue,
            };

            match store.resolve(&user_clone, &table, &namespace).await {
                Ok(policy) => {
                    debug!(
                        user = %username,
                        table = %table_name,
                        row_filters = policy.row_filters.len(),
                        column_masks = policy.column_masks.len(),
                        restricted = policy.restricted_columns.len(),
                        "Resolved policy for table"
                    );
                    table_policies.insert(table_name, policy);
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
                    table_policies.insert(table_name, deny);
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

                        let mut current = node;

                        // 1. Inject row filters above the TableScan
                        for filter_expr in &policy.row_filters {
                            current = LogicalPlan::Filter(
                                Filter::try_new(filter_expr.clone(), Arc::new(current))
                                    .map_err(|e| {
                                        datafusion::error::DataFusionError::Internal(format!(
                                            "Failed to create policy filter: {e}"
                                        ))
                                    })?,
                            );
                        }

                        // 2. Apply column masks via projection
                        if !policy.column_masks.is_empty() {
                            let schema = current.schema();
                            let exprs: Vec<Expr> = schema
                                .fields()
                                .iter()
                                .filter(|f| !policy.restricted_columns.contains(f.name()))
                                .map(|f| {
                                    if let Some(mask) = policy.column_masks.get(f.name()) {
                                        apply_mask(f.name(), f.data_type(), mask, mask_key.clone())
                                            .alias(f.name())
                                    } else {
                                        col(f.name())
                                    }
                                })
                                .collect();

                            if !exprs.is_empty() {
                                current = LogicalPlan::Projection(
                                    Projection::try_new(exprs, Arc::new(current)).map_err(
                                        |e| {
                                            datafusion::error::DataFusionError::Internal(
                                                format!(
                                                    "Failed to create policy projection: {e}"
                                                ),
                                            )
                                        },
                                    )?,
                                );
                            }
                        }
                        // 3. Apply column restrictions (remove columns entirely)
                        else if !policy.restricted_columns.is_empty() {
                            let schema = current.schema();
                            let exprs: Vec<Expr> = schema
                                .fields()
                                .iter()
                                .filter(|f| !policy.restricted_columns.contains(f.name()))
                                .map(|f| col(f.name()))
                                .collect();

                            if !exprs.is_empty() {
                                current = LogicalPlan::Projection(
                                    Projection::try_new(exprs, Arc::new(current)).map_err(
                                        |e| {
                                            datafusion::error::DataFusionError::Internal(
                                                format!(
                                                    "Failed to create restriction projection: {e}"
                                                ),
                                            )
                                        },
                                    )?,
                                );
                            }
                        }

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
