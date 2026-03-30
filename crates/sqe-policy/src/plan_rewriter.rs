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

use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{col, lit, Expr, Filter, LogicalPlan, Projection};
use tracing::{debug, warn};

use sqe_core::SessionUser;

use crate::{MaskType, PolicyEnforcer, PolicyStore, ResolvedPolicy};

/// Plan-rewriting policy enforcer. Uses a PolicyStore to resolve policies
/// and rewrites the LogicalPlan accordingly.
pub struct PolicyPlanRewriter {
    store: Arc<dyn PolicyStore>,
}

impl PolicyPlanRewriter {
    pub fn new(store: Arc<dyn PolicyStore>) -> Self {
        Self { store }
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
                                        apply_mask(f.name(), mask).alias(f.name())
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

                        return Ok(Transformed::yes(current));
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
fn apply_mask(column_name: &str, mask: &MaskType) -> Expr {
    match mask {
        MaskType::Nullify => lit(datafusion::scalar::ScalarValue::Utf8(None)),
        MaskType::Redact(value) => lit(value.clone()),
        MaskType::Hash => {
            // Uses the registered sha256 UDF. The UDF must be registered on the
            // SessionContext before queries are executed.
            datafusion::logical_expr::Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    Arc::new(crate::sha256_udf::sha256_udf()),
                    vec![col(column_name)],
                ),
            )
        }
        MaskType::Custom(expr) => expr.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_mask_nullify() {
        let expr = apply_mask("ssn", &MaskType::Nullify);
        assert!(matches!(expr, Expr::Literal(..)));
    }

    #[test]
    fn test_apply_mask_redact() {
        let expr = apply_mask("ssn", &MaskType::Redact("***".to_string()));
        assert!(matches!(expr, Expr::Literal(..)));
    }
}
