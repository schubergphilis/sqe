// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! DataFusion's `eliminate_cross_join`, copied from datafusion-optimizer
//! 55.1.0 (`src/eliminate_cross_join.rs`, plus the crate-private
//! `JoinKeySet`) with one change: an `ON` key the rebuild cannot place is
//! kept as a filter instead of dropped (apache/datafusion#26058, SQE #806).
//!
//! The rule flattens a tree of inner joins into its leaves, pools every
//! join's `on` pairs with the equalities of the `WHERE` above it, and
//! rebuilds the tree greedily: a pair is placed where one side fits the
//! joins built so far and the other a single remaining leaf. Upstream puts
//! join filters and the `WHERE` back on top afterwards, but an `on` pair that
//! was never placed is lost, and the join returns too many rows:
//!
//! ```sql
//! SELECT k.k, b.id
//! FROM (SELECT 1 AS t, 17 AS x) a
//! CROSS JOIN (SELECT 0 AS k UNION ALL SELECT 1) k
//! JOIN b ON b.t = a.t AND b.id = a.x + k.k   -- second key dropped
//! ```
//!
//! Here every unplaced `on` pair goes back on top next to the join filters
//! (`IS NOT DISTINCT FROM` for a NULL-safe join), where `push_down_filter`
//! and `extract_equijoin_predicate` move it to the join that has both
//! sides. A plan with no unplaced pair comes out exactly as upstream builds
//! it. Drop this copy when DataFusion fixes #26058; the tripwire test below
//! fails then.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{NullEquality, Result};
use datafusion::logical_expr::expr::{BinaryExpr, Expr};
use datafusion::logical_expr::logical_plan::{
    Filter, Join, JoinConstraint, JoinType, LogicalPlan, Projection,
};
use datafusion::logical_expr::utils::{can_hash, find_valid_equijoin_key_pair};
use datafusion::logical_expr::{and, binary_expr, build_join_schema, ExprSchemable, Operator};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

/// `eliminate_cross_join` that keeps unplaced `ON` keys; see the module
/// documentation.
#[derive(Default, Debug)]
pub struct EliminateCrossJoin;

impl EliminateCrossJoin {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateCrossJoin {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !plan_has_joins(&plan) {
            return Ok(Transformed::no(plan));
        }

        let plan_schema = Arc::clone(plan.schema());
        let mut possible_join_keys = JoinKeySet::new();
        let mut on_keys = Vec::new();
        let mut all_inputs: Vec<LogicalPlan> = vec![];
        let mut all_filters: Vec<Expr> = vec![];
        let mut null_equality = NullEquality::NullEqualsNothing;

        let parent_predicate = if let LogicalPlan::Filter(filter) = plan {
            // if input isn't a join that can potentially be rewritten
            // avoid unwrapping the input
            let rewritable = matches!(
                filter.input.as_ref(),
                LogicalPlan::Join(Join {
                    join_type: JoinType::Inner,
                    ..
                })
            );

            if !rewritable {
                // recursively try to rewrite children
                return rewrite_children(self, LogicalPlan::Filter(filter), config);
            }

            if !can_flatten_join_inputs(&filter.input) {
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            }

            let Filter {
                input, predicate, ..
            } = filter;

            // Extract null_equality setting from the input join
            if let LogicalPlan::Join(join) = input.as_ref() {
                null_equality = join.null_equality;
            }

            flatten_join_inputs(
                Arc::unwrap_or_clone(input),
                &mut possible_join_keys,
                &mut on_keys,
                &mut all_inputs,
                &mut all_filters,
            )?;

            extract_possible_join_keys(&predicate, &mut possible_join_keys);
            Some(predicate)
        } else {
            match plan {
                LogicalPlan::Join(Join {
                    join_type: JoinType::Inner,
                    null_equality: original_null_equality,
                    ..
                }) => {
                    if !can_flatten_join_inputs(&plan) {
                        return Ok(Transformed::no(plan));
                    }
                    flatten_join_inputs(
                        plan,
                        &mut possible_join_keys,
                        &mut on_keys,
                        &mut all_inputs,
                        &mut all_filters,
                    )?;
                    null_equality = original_null_equality;
                    None
                }
                _ => {
                    // recursively try to rewrite children
                    return rewrite_children(self, plan, config);
                }
            }
        };

        // Join keys are handled locally:
        let mut all_join_keys = JoinKeySet::new();
        let mut left = all_inputs.remove(0);
        while !all_inputs.is_empty() {
            left = find_inner_join(
                left,
                &mut all_inputs,
                &possible_join_keys,
                &mut all_join_keys,
                null_equality,
            )?;
        }

        // SQE: an `on` key the rebuild did not place goes back as a filter
        // (apache/datafusion#26058). Upstream drops it here.
        for (l, r, key_null_equality) in on_keys {
            if !all_join_keys.contains(&l, &r) {
                all_filters.push(match key_null_equality {
                    NullEquality::NullEqualsNothing => binary_expr(l, Operator::Eq, r),
                    NullEquality::NullEqualsNull => binary_expr(l, Operator::IsNotDistinctFrom, r),
                });
            }
        }

        left = rewrite_children(self, left, config)?.data;

        if &plan_schema != left.schema() {
            left = LogicalPlan::Projection(Projection::new_from_schema(
                Arc::new(left),
                Arc::clone(&plan_schema),
            ));
        }

        if !all_filters.is_empty() {
            // Add any filters on top - PushDownFilter can push filters down to applicable join
            let first = all_filters.swap_remove(0);
            let predicate = all_filters.into_iter().fold(first, and);
            left = LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(left))?);
        }

        let Some(predicate) = parent_predicate else {
            return Ok(Transformed::yes(left));
        };

        // If there are no join keys then do nothing:
        if all_join_keys.is_empty() {
            Filter::try_new(predicate, Arc::new(left))
                .map(|filter| Transformed::yes(LogicalPlan::Filter(filter)))
        } else {
            // Remove join expressions from filter:
            match remove_join_expressions(predicate, &all_join_keys) {
                Some(filter_expr) => Filter::try_new(filter_expr, Arc::new(left))
                    .map(|filter| Transformed::yes(LogicalPlan::Filter(filter))),
                _ => Ok(Transformed::yes(left)),
            }
        }
    }

    fn name(&self) -> &str {
        "eliminate_cross_join"
    }
}

/// Whether `plan`, or a subquery inside it, holds a `Join`.
fn plan_has_joins(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply_with_subqueries(|node| {
        if matches!(node, LogicalPlan::Join(_)) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
}

fn rewrite_children(
    optimizer: &impl OptimizerRule,
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    // Process uncorrelated subqueries in expressions, then direct children.
    let transformed_plan = plan
        .map_uncorrelated_subqueries(|input| optimizer.rewrite(input, config))?
        .transform_sibling(|plan| plan.map_children(|input| optimizer.rewrite(input, config)))?;

    // recompute schema if the plan was transformed
    if transformed_plan.transformed {
        transformed_plan.map_data(|plan| plan.recompute_schema())
    } else {
        Ok(transformed_plan)
    }
}

/// Recursively accumulate possible_join_keys and inputs from inner joins
/// (including cross joins). SQE: also records each join's `on` pairs, with
/// that join's null equality, in `on_keys`.
///
/// Assumes can_flatten_join_inputs has returned true and thus the plan can be
/// flattened. Adds all leaf inputs to `all_inputs` and join_keys to
/// possible_join_keys
fn flatten_join_inputs(
    plan: LogicalPlan,
    possible_join_keys: &mut JoinKeySet,
    on_keys: &mut Vec<(Expr, Expr, NullEquality)>,
    all_inputs: &mut Vec<LogicalPlan>,
    all_filters: &mut Vec<Expr>,
) -> Result<()> {
    match plan {
        LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
            if let Some(filter) = join.filter {
                all_filters.push(filter);
            }
            on_keys.extend(
                join.on
                    .iter()
                    .map(|(l, r)| (l.clone(), r.clone(), join.null_equality)),
            );
            possible_join_keys.insert_all_owned(join.on);
            flatten_join_inputs(
                Arc::unwrap_or_clone(join.left),
                possible_join_keys,
                on_keys,
                all_inputs,
                all_filters,
            )?;
            flatten_join_inputs(
                Arc::unwrap_or_clone(join.right),
                possible_join_keys,
                on_keys,
                all_inputs,
                all_filters,
            )?;
        }
        _ => {
            all_inputs.push(plan);
        }
    };
    Ok(())
}

/// Returns true if the plan is a Join or Cross join could be flattened with
/// `flatten_join_inputs`
///
/// Must stay in sync with `flatten_join_inputs`
fn can_flatten_join_inputs(plan: &LogicalPlan) -> bool {
    // can only flatten inner / cross joins
    match plan {
        LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {}
        _ => return false,
    };

    for child in plan.inputs() {
        if let LogicalPlan::Join(Join {
            join_type: JoinType::Inner,
            ..
        }) = child
        {
            if !can_flatten_join_inputs(child) {
                return false;
            }
        }
    }
    true
}

/// Finds the next to join with the left input plan,
///
/// Finds the next `right` from `rights` that can be joined with `left_input`
/// plan based on the join keys in `possible_join_keys`.
///
/// If such a matching `right` is found:
/// 1. Adds the matching join keys to `all_join_keys`.
/// 2. Returns `left_input JOIN right ON (all join keys)`.
///
/// If no matching `right` is found:
/// 1. Removes the first plan from `rights`
/// 2. Returns `left_input CROSS JOIN right`.
fn find_inner_join(
    left_input: LogicalPlan,
    rights: &mut Vec<LogicalPlan>,
    possible_join_keys: &JoinKeySet,
    all_join_keys: &mut JoinKeySet,
    null_equality: NullEquality,
) -> Result<LogicalPlan> {
    for (i, right_input) in rights.iter().enumerate() {
        let mut join_keys = vec![];

        for (l, r) in possible_join_keys.iter() {
            let key_pair =
                find_valid_equijoin_key_pair(l, r, left_input.schema(), right_input.schema())?;

            // Save join keys
            if let Some((valid_l, valid_r)) = key_pair {
                if can_hash(&valid_l.get_type(left_input.schema())?) {
                    join_keys.push((valid_l, valid_r));
                }
            }
        }

        // Found one or more matching join keys
        if !join_keys.is_empty() {
            all_join_keys.insert_all(join_keys.iter());
            let right_input = rights.remove(i);
            let join_schema = Arc::new(build_join_schema(
                left_input.schema(),
                right_input.schema(),
                &JoinType::Inner,
            )?);

            return Ok(LogicalPlan::Join(Join {
                left: Arc::new(left_input),
                right: Arc::new(right_input),
                join_type: JoinType::Inner,
                join_constraint: JoinConstraint::On,
                on: join_keys,
                filter: None,
                schema: join_schema,
                null_equality,
                null_aware: false,
            }));
        }
    }

    // no matching right plan had any join keys, cross join with the first right
    // plan
    let right = rights.remove(0);
    let join_schema = Arc::new(build_join_schema(
        left_input.schema(),
        right.schema(),
        &JoinType::Inner,
    )?);

    Ok(LogicalPlan::Join(Join {
        left: Arc::new(left_input),
        right: Arc::new(right),
        schema: join_schema,
        on: vec![],
        filter: None,
        join_type: JoinType::Inner,
        join_constraint: JoinConstraint::On,
        null_equality,
        null_aware: false,
    }))
}

/// Extract join keys from a WHERE clause
fn extract_possible_join_keys(expr: &Expr, join_keys: &mut JoinKeySet) {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
        match op {
            Operator::Eq => {
                // insert handles ensuring  we don't add the same Join keys multiple times
                join_keys.insert(left, right);
            }
            Operator::And => {
                extract_possible_join_keys(left, join_keys);
                extract_possible_join_keys(right, join_keys)
            }
            // Fix for issue#78 join predicates from inside of OR expr also pulled up properly.
            Operator::Or => {
                let mut left_join_keys = JoinKeySet::new();
                let mut right_join_keys = JoinKeySet::new();

                extract_possible_join_keys(left, &mut left_join_keys);
                extract_possible_join_keys(right, &mut right_join_keys);

                join_keys.insert_intersection(&left_join_keys, &right_join_keys)
            }
            _ => (),
        };
    }
}

/// Remove join expressions from a filter expression
///
/// # Returns
/// * `Some()` when there are few remaining predicates in filter_expr
/// * `None` otherwise
fn remove_join_expressions(expr: Expr, join_keys: &JoinKeySet) -> Option<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) if join_keys.contains(&left, &right) => {
            // was a join key, so remove it
            None
        }
        // Fix for issue#78 join predicates from inside of OR expr also pulled up properly.
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if op == Operator::And => {
            let l = remove_join_expressions(*left, join_keys);
            let r = remove_join_expressions(*right, join_keys);
            match (l, r) {
                (Some(ll), Some(rr)) => Some(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(ll),
                    op,
                    Box::new(rr),
                ))),
                (Some(ll), _) => Some(ll),
                (_, Some(rr)) => Some(rr),
                _ => None,
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if op == Operator::Or => {
            let l = remove_join_expressions(*left, join_keys);
            let r = remove_join_expressions(*right, join_keys);
            match (l, r) {
                (Some(ll), Some(rr)) => Some(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(ll),
                    op,
                    Box::new(rr),
                ))),
                // When either `left` or `right` is empty, it means they are `true`
                // so OR'ing anything with them will also be true
                _ => None,
            }
        }
        _ => Some(expr),
    }
}

/// Upstream's crate-private `JoinKeySet`: equality join keys in insert order,
/// `(l, r)` and `(r, l)` counted as the same key. A `Vec` instead of an
/// `IndexSet`; a join tree holds a handful of keys.
#[derive(Debug, Default)]
struct JoinKeySet {
    inner: Vec<(Expr, Expr)>,
}

impl JoinKeySet {
    fn new() -> Self {
        Self::default()
    }

    fn contains(&self, left: &Expr, right: &Expr) -> bool {
        self.inner
            .iter()
            .any(|(l, r)| (l == left && r == right) || (l == right && r == left))
    }

    fn insert(&mut self, left: &Expr, right: &Expr) -> bool {
        self.insert_owned(left.clone(), right.clone())
    }

    fn insert_owned(&mut self, left: Expr, right: Expr) -> bool {
        if self.contains(&left, &right) {
            false
        } else {
            self.inner.push((left, right));
            true
        }
    }

    fn insert_all<'a>(&mut self, iter: impl IntoIterator<Item = &'a (Expr, Expr)>) {
        for (left, right) in iter {
            self.insert(left, right);
        }
    }

    fn insert_all_owned(&mut self, iter: impl IntoIterator<Item = (Expr, Expr)>) {
        for (left, right) in iter {
            self.insert_owned(left, right);
        }
    }

    fn insert_intersection(&mut self, s1: &JoinKeySet, s2: &JoinKeySet) {
        for (left, right) in &s1.inner {
            if s2.contains(left, right) {
                self.insert(left, right);
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn iter(&self) -> impl Iterator<Item = (&Expr, &Expr)> {
        self.inner.iter().map(|(l, r)| (l, r))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::SessionContext;

    use crate::session_tuning::{engine_context, engine_session_config};

    const A: &str = "(SELECT 1 AS t, 17 AS x, 18 AS y, 0 AS id) a";
    const K: &str = "(SELECT 0 AS k, 0 AS id UNION ALL SELECT 1, 0) k";
    const B: &str = "(SELECT 1 AS t, CAST(value AS INT) AS id FROM generate_series(0, 20)) b";

    /// Shapes that lose a key on plain DataFusion 55.1: 2 rows are right,
    /// 42 come back.
    fn dropping_shapes() -> Vec<String> {
        vec![
            format!(
                "SELECT k.k, b.id FROM {A} CROSS JOIN {K} JOIN {B} \
                 ON b.t = a.t AND b.id = CASE k.k WHEN 0 THEN a.x ELSE a.y END"
            ),
            format!(
                "SELECT k.k, b.id FROM {A} CROSS JOIN {K} JOIN {B} \
                 ON b.t = a.t AND b.id = a.x + k.k"
            ),
            format!(
                "SELECT k.k, b.id FROM {A} JOIN {K} ON a.t >= k.k JOIN {B} \
                 ON b.t = a.t AND b.id = a.x + k.k"
            ),
            // `a` and `k` are equi-joined, but `b` comes first in the
            // flattened leaves, so `b` is cross joined with `a` before `k`
            // arrives and the key fits neither side.
            format!(
                "SELECT k.k, b.id FROM {B} JOIN ({A} JOIN {K} ON a.id = k.id) \
                 ON b.t = a.t AND b.id = a.x + k.k"
            ),
        ]
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> usize {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn engine() -> SessionContext {
        engine_context(engine_session_config(), Arc::new(RuntimeEnv::default()))
    }

    fn plain() -> SessionContext {
        SessionContext::new_with_config(engine_session_config())
    }

    async fn optimized(ctx: &SessionContext, sql: &str) -> String {
        let plan = ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();
        let shown = plan.display_indent().to_string();
        shown
    }

    #[tokio::test]
    async fn a_key_the_rebuild_cannot_place_is_kept() {
        let ctx = engine();
        for sql in dropping_shapes() {
            assert_eq!(rows(&ctx, &sql).await, 2, "{sql}");
        }
    }

    /// The same keys written in `WHERE`, and a NULL-safe key, agree with the
    /// `ON` form.
    #[tokio::test]
    async fn on_and_where_forms_agree() {
        let ctx = engine();
        let where_form = format!(
            "SELECT k.k, b.id FROM {A}, {K}, {B} \
             WHERE b.t = a.t AND b.id = CASE k.k WHEN 0 THEN a.x ELSE a.y END"
        );
        assert_eq!(rows(&ctx, &where_form).await, 2);
        let null_safe = format!(
            "SELECT k.k, b.id FROM {A} CROSS JOIN {K} JOIN {B} \
             ON b.t = a.t AND b.id IS NOT DISTINCT FROM a.x + k.k"
        );
        assert_eq!(rows(&ctx, &null_safe).await, 2);
    }

    /// Keys the rebuild places, including one spanning two equi-joined
    /// relations, leave the plan exactly as upstream builds it.
    #[tokio::test]
    async fn placed_keys_leave_the_plan_unchanged() {
        for sql in [
            format!("SELECT b.id FROM {A} CROSS JOIN {K} JOIN {B} ON b.t = a.t AND b.id = a.x"),
            format!(
                "SELECT k.k, b.id FROM ({A} JOIN {K} ON a.id = k.id) JOIN {B} \
                 ON b.t = a.t AND b.id = a.x + k.k"
            ),
            format!("SELECT b.id FROM {A}, {K}, {B} WHERE a.id = k.id AND b.id = a.x + k.k"),
        ] {
            let ours = optimized(&engine(), &sql).await;
            assert_eq!(ours, optimized(&plain(), &sql).await, "{sql}");
            assert!(ours.contains("Inner Join: "), "{ours}");
            assert_eq!(
                rows(&engine(), &sql).await,
                rows(&plain(), &sql).await,
                "{sql}"
            );
        }
    }

    /// Tripwire: when this fails, DataFusion has fixed #26058 and this copy
    /// of `eliminate_cross_join` can go.
    #[tokio::test]
    async fn plain_datafusion_still_drops_the_key() {
        let ctx = plain();
        for sql in dropping_shapes() {
            assert_eq!(rows(&ctx, &sql).await, 42, "{sql}");
        }
    }
}
