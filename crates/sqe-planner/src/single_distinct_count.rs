//! Logical optimizer rule extending DataFusion's `SingleDistinctToGroupBy`
//! rewrite to admit `count()` companions (issue #366).
//!
//! DataFusion 54's built-in rule turns `AGG(DISTINCT x) .. GROUP BY k` into a
//! two-phase aggregation (inner `GROUP BY k, x`, outer `GROUP BY k`) but only
//! when every non-distinct companion aggregate is `sum`, `min`, or `max` —
//! functions whose outer phase re-applies the same function to the inner
//! partials. A single `COUNT(*)` companion blocks the rewrite, and the plan
//! falls back to one boxed distinct accumulator holding a HashSet *per group*
//! (strings have no GroupsAccumulator path). At scale that state is gigabytes
//! of nested pointers and its spill path degenerates (bank q03 at SF10:
//! 8-12GB for a ~3GB aggregation, >300s without output).
//!
//! `count` maps through the two-phase form like `sum`, with two twists the
//! built-in rule does not need:
//!
//! - the outer phase is `SUM(partial_count)`, not `count(partial_count)`
//!   (which would count inner groups);
//! - `SUM` over zero rows is NULL where `count` must return 0, so the final
//!   projection wraps the outer sum in `COALESCE(.., 0)`. Zero outer rows can
//!   only happen for a global aggregate over empty input — every group of a
//!   grouped aggregate has at least one inner partial — but the guard is
//!   uniform and free.
//!
//! The rule only fires on plans the built-in rule rejects (at least one
//! `count` companion present); everything the built-in rule already handles
//! is left to it, so the two rules are strictly complementary. It is
//! registered via `SessionContext::add_optimizer_rule`, i.e. appended after
//! the default set: the built-in rule no-ops on the count-companion shape,
//! this rule rewrites it, and on the next optimizer pass both rules see a
//! plan without distinct aggregates and no-op.
//!
//! This file started as a copy of `single_distinct_to_groupby.rs` from
//! datafusion-optimizer 54.0.0 (Apache-2.0, Apache Software Foundation) —
//! the alias scheme (`alias1` for the distinct argument, `alias{N}` for
//! companions, `group_alias_{i}` for complex group expressions) is kept
//! identical so rewritten plans read the same in EXPLAIN. Upstreaming the
//! `count` extension is tracked as follow-up.

use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{DataFusionError, HashSet, Result};
use datafusion::functions::expr_fn::coalesce;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::builder::project;
use datafusion::logical_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion::logical_expr::logical_plan::{Aggregate, LogicalPlan};
use datafusion::logical_expr::{col, lit, Expr};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

const SINGLE_DISTINCT_ALIAS: &str = "alias1";

/// Extends the single-distinct-to-group-by rewrite to plans where the
/// non-distinct companions include `count` (see module docs).
///
/// ```text
/// Before:
///   SELECT k, COUNT(DISTINCT x), COUNT(*), SUM(c) FROM t GROUP BY k
///
/// After:
///   SELECT k, count(alias1), COALESCE(sum(alias2), 0) AS "count(*)", sum(alias3)
///   FROM (
///     SELECT k, x AS alias1, count(*) AS alias2, sum(c) AS alias3
///     FROM t GROUP BY k, x
///   )
///   GROUP BY k
/// ```
#[derive(Default, Debug)]
pub struct SingleDistinctCountCompanionRule {}

impl SingleDistinctCountCompanionRule {
    /// Create a new instance of the rule.
    pub fn new() -> Self {
        Self {}
    }
}

fn is_count(func_name: &str) -> bool {
    func_name.eq_ignore_ascii_case("count")
}

fn is_reaggregatable(func_name: &str) -> bool {
    func_name.eq_ignore_ascii_case("sum")
        || func_name.eq_ignore_ascii_case("min")
        || func_name.eq_ignore_ascii_case("max")
}

/// Mirror of upstream `is_single_distinct_agg`, with two changes: `count` is
/// admitted as a non-distinct companion, and the check demands at least one
/// such `count` companion so the rule never overlaps the built-in one.
fn is_single_distinct_agg_with_count_companion(aggr_expr: &[Expr]) -> Result<bool> {
    let mut fields_set = HashSet::new();
    let mut aggregate_count = 0;
    let mut count_companions = 0;
    for expr in aggr_expr {
        if let Expr::AggregateFunction(AggregateFunction {
            func,
            params:
                AggregateFunctionParams {
                    distinct,
                    args,
                    filter,
                    order_by,
                    null_treatment: _,
                },
        }) = expr
        {
            if filter.is_some() || !order_by.is_empty() {
                return Ok(false);
            }
            aggregate_count += 1;
            if *distinct {
                for e in args {
                    fields_set.insert(e);
                }
            } else if is_count(func.name()) {
                count_companions += 1;
            } else if !is_reaggregatable(func.name()) {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
    }
    Ok(aggregate_count == aggr_expr.len() && fields_set.len() == 1 && count_companions > 0)
}

/// Check if the first expr is [Expr::GroupingSet].
fn contains_grouping_set(expr: &[Expr]) -> bool {
    matches!(expr.first(), Some(Expr::GroupingSet(_)))
}

impl OptimizerRule for SingleDistinctCountCompanionRule {
    fn name(&self) -> &str {
        "single_distinct_count_companion_to_group_by"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        match plan {
            LogicalPlan::Aggregate(Aggregate {
                input,
                aggr_expr,
                schema,
                group_expr,
                ..
            }) if is_single_distinct_agg_with_count_companion(&aggr_expr)?
                && !contains_grouping_set(&group_expr) =>
            {
                let group_size = group_expr.len();
                // Alias all original group_by exprs. Bare columns pass through;
                // complex expressions get a `group_alias_{i}` so the outer
                // aggregate can reference the inner aggregate's output field
                // (same scheme as the upstream rule).
                let (mut inner_group_exprs, out_group_expr_with_alias): (
                    Vec<Expr>,
                    Vec<(Expr, _)>,
                ) = group_expr
                    .into_iter()
                    .enumerate()
                    .map(|(i, group_expr)| {
                        if let Expr::Column(_) = group_expr {
                            (group_expr.clone(), (group_expr, None))
                        } else {
                            let alias_str = format!("group_alias_{i}");
                            let (qualifier, field) = schema.qualified_field(i);
                            (
                                group_expr.alias(alias_str.clone()),
                                (col(alias_str), Some((qualifier, field.name()))),
                            )
                        }
                    })
                    .unzip();

                // Rewrite each aggregate: the distinct arg becomes an inner
                // group key; companions become inner partials re-aggregated
                // outside. `needs_zero_default` marks count companions whose
                // outer SUM must be coalesced back to 0 (count is never NULL).
                let mut index = 1;
                let mut group_fields_set = HashSet::new();
                let mut inner_aggr_exprs = vec![];
                let mut needs_zero_default = vec![];
                let outer_aggr_exprs = aggr_expr
                    .into_iter()
                    .map(|aggr_expr| match aggr_expr {
                        Expr::AggregateFunction(AggregateFunction {
                            func,
                            params:
                                AggregateFunctionParams {
                                    mut args,
                                    distinct,
                                    filter,
                                    order_by,
                                    null_treatment,
                                },
                        }) => {
                            if distinct {
                                if args.len() != 1 {
                                    return Err(DataFusionError::Internal(
                                        "DISTINCT aggregate should have exactly one argument"
                                            .to_string(),
                                    ));
                                }
                                let arg = args.swap_remove(0);

                                if group_fields_set.insert(arg.schema_name().to_string()) {
                                    inner_group_exprs.push(arg.alias(SINGLE_DISTINCT_ALIAS));
                                }
                                needs_zero_default.push(false);
                                Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                                    func,
                                    vec![col(SINGLE_DISTINCT_ALIAS)],
                                    false, // intentional to remove distinct here
                                    filter,
                                    order_by,
                                    null_treatment,
                                )))
                            } else {
                                index += 1;
                                let alias_str = format!("alias{index}");
                                let count_companion = is_count(func.name());
                                inner_aggr_exprs.push(
                                    Expr::AggregateFunction(AggregateFunction::new_udf(
                                        Arc::clone(&func),
                                        args,
                                        false,
                                        filter,
                                        order_by,
                                        null_treatment,
                                    ))
                                    .alias(&alias_str),
                                );
                                // sum/min/max re-apply themselves over the
                                // inner partials; count's partials are summed.
                                let outer_func = if count_companion { sum_udaf() } else { func };
                                needs_zero_default.push(count_companion);
                                Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                                    outer_func,
                                    vec![col(&alias_str)],
                                    false,
                                    None,
                                    vec![],
                                    None,
                                )))
                            }
                        }
                        _ => Ok(aggr_expr),
                    })
                    .collect::<Result<Vec<_>>>()?;

                // construct the inner AggrPlan
                let inner_agg = LogicalPlan::Aggregate(Aggregate::try_new(
                    input,
                    inner_group_exprs,
                    inner_aggr_exprs,
                )?);

                let outer_group_exprs = out_group_expr_with_alias
                    .iter()
                    .map(|(expr, _)| expr.clone())
                    .collect();

                // Final projection restores the original output names (and,
                // for count companions, the 0-for-empty contract).
                let alias_expr: Vec<_> = out_group_expr_with_alias
                    .into_iter()
                    .map(|(group_expr, original_name)| match original_name {
                        Some((qualifier, name)) => {
                            group_expr.alias_qualified(qualifier.cloned(), name)
                        }
                        None => group_expr,
                    })
                    .chain(
                        outer_aggr_exprs
                            .iter()
                            .cloned()
                            .zip(needs_zero_default.iter())
                            .enumerate()
                            .map(|(idx, (expr, zero_default))| {
                                let idx = idx + group_size;
                                let (qualifier, field) = schema.qualified_field(idx);
                                let expr = if *zero_default {
                                    coalesce(vec![expr, lit(0i64)])
                                } else {
                                    expr
                                };
                                expr.alias_qualified(qualifier.cloned(), field.name())
                            }),
                    )
                    .collect();

                let outer_aggr = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(inner_agg),
                    outer_group_exprs,
                    outer_aggr_exprs,
                )?);
                Ok(Transformed::yes(project(outer_aggr, alias_expr)?))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn test_batch(rows: &[(Option<&str>, Option<i64>, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("iban", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
            Field::new("account", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            Arc::new(Schema::clone(&schema)),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(i, _, _)| *i).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, a, _)| *a).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, _, k)| Some(*k)).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("test batch")
    }

    fn ctx_with_table(
        rows: &[(Option<&str>, Option<i64>, &str)],
        with_rule: bool,
    ) -> SessionContext {
        // Single partition + no repartitioning keeps plans deterministic.
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        if with_rule {
            ctx.add_optimizer_rule(Arc::new(SingleDistinctCountCompanionRule::new()));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("iban", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
            Field::new("account", DataType::Utf8, false),
        ]));
        let batches = if rows.is_empty() {
            vec![vec![RecordBatch::new_empty(Arc::clone(&schema))]]
        } else {
            vec![vec![test_batch(rows)]]
        };
        let table = MemTable::try_new(schema, batches).expect("mem table");
        ctx.register_table("t", Arc::new(table)).expect("register");
        ctx
    }

    async fn logical_plan(ctx: &SessionContext, sql: &str) -> String {
        let df = ctx.sql(sql).await.expect("plan");
        format!(
            "{}",
            df.into_optimized_plan().expect("optimize").display_indent()
        )
    }

    async fn results(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        let formatted = arrow::util::pretty::pretty_format_batches(&batches)
            .expect("format")
            .to_string();
        formatted.lines().map(str::to_owned).collect()
    }

    const MIXED_SQL: &str = "SELECT account, COUNT(DISTINCT iban) AS d, COUNT(*) AS n, \
         SUM(amount) AS s FROM t GROUP BY account ORDER BY account";

    fn sample_rows() -> Vec<(Option<&'static str>, Option<i64>, &'static str)> {
        vec![
            (Some("NL01"), Some(10), "a"),
            (Some("NL01"), Some(20), "a"),
            (Some("NL02"), None, "a"),
            (None, Some(40), "a"), // NULL iban: counted by COUNT(*), not by COUNT(DISTINCT)
            (Some("NL03"), Some(5), "b"), // single-row group
        ]
    }

    /// The core plan-shape assertion: with the rule, the mixed
    /// COUNT(DISTINCT)+COUNT(*)+SUM aggregation becomes a two-level
    /// aggregate with no distinct accumulator left in the plan. Fails
    /// without the fix (the default optimizer keeps `count(DISTINCT ..)`).
    #[tokio::test]
    async fn mixed_count_distinct_rewrites_to_two_phase() {
        let rows = sample_rows();
        let without = ctx_with_table(&rows, false);
        let plan = logical_plan(&without, MIXED_SQL).await;
        assert!(
            plan.contains("DISTINCT"),
            "precondition: default optimizer must NOT rewrite this shape, got:\n{plan}"
        );

        let with = ctx_with_table(&rows, true);
        let plan = logical_plan(&with, MIXED_SQL).await;
        assert!(
            !plan.contains("DISTINCT"),
            "rule must remove the distinct aggregate, got:\n{plan}"
        );
        assert_eq!(
            plan.matches("Aggregate:").count(),
            2,
            "expected the two-phase aggregate shape, got:\n{plan}"
        );
    }

    /// NULLs in the distinct column: COUNT(*) includes those rows, the
    /// distinct count does not. Results must match the unrewritten plan.
    #[tokio::test]
    async fn results_match_with_nulls_in_distinct_column() {
        let rows = sample_rows();
        let expected = results(&ctx_with_table(&rows, false), MIXED_SQL).await;
        let actual = results(&ctx_with_table(&rows, true), MIXED_SQL).await;
        assert_eq!(expected, actual);
        // Belt and braces against both plans agreeing on wrong numbers:
        // group a = 4 rows, 2 distinct non-NULL ibans, sum 10+20+40.
        assert!(
            actual
                .iter()
                .any(|l| l.contains("a") && l.contains("2") && l.contains("4") && l.contains("70")),
            "unexpected group-a row in:\n{}",
            actual.join("\n")
        );
    }

    /// Empty input, global aggregate: COUNT(*) must be 0 (not NULL) after
    /// the rewrite — the outer SUM over zero rows yields NULL and the
    /// projection's COALESCE restores the count contract.
    #[tokio::test]
    async fn empty_input_global_aggregate_counts_zero() {
        let sql = "SELECT COUNT(DISTINCT iban) AS d, COUNT(*) AS n FROM t";
        let without = ctx_with_table(&[], false);
        let with = ctx_with_table(&[], true);
        let expected = results(&without, sql).await;
        let actual = results(&with, sql).await;
        assert_eq!(expected, actual);
        assert!(
            actual.iter().any(|l| l.contains("0")),
            "expected zero counts, got:\n{}",
            actual.join("\n")
        );
        // The rewrite must actually fire on the global-aggregate shape.
        let plan = logical_plan(&with, sql).await;
        assert!(!plan.contains("DISTINCT"), "rule must fire, got:\n{plan}");
    }

    /// COUNT(col) companion counts non-NULL values of that column only.
    #[tokio::test]
    async fn count_column_companion_skips_nulls() {
        let sql = "SELECT account, COUNT(DISTINCT iban) AS d, COUNT(amount) AS n_amount \
                   FROM t GROUP BY account ORDER BY account";
        let rows = sample_rows();
        let with = ctx_with_table(&rows, true);
        let expected = results(&ctx_with_table(&rows, false), sql).await;
        let actual = results(&with, sql).await;
        assert_eq!(expected, actual);
        let plan = logical_plan(&with, sql).await;
        assert!(!plan.contains("DISTINCT"), "rule must fire, got:\n{plan}");
    }

    /// Cases the rule must NOT touch: multiple distinct columns, and an
    /// inadmissible companion (avg).
    #[tokio::test]
    async fn fallback_shapes_left_alone() {
        let rows = sample_rows();
        let with = ctx_with_table(&rows, true);
        for sql in [
            "SELECT COUNT(DISTINCT iban), COUNT(DISTINCT amount), COUNT(*) FROM t",
            "SELECT COUNT(DISTINCT iban), COUNT(*), AVG(amount) FROM t",
        ] {
            let plan = logical_plan(&with, sql).await;
            assert!(
                plan.contains("DISTINCT"),
                "rule must not fire on {sql}, got:\n{plan}"
            );
            // Still executes correctly through the fallback path.
            let expected = results(&ctx_with_table(&rows, false), sql).await;
            assert_eq!(expected, results(&with, sql).await);
        }
    }

    /// Complex (non-column) group expressions go through the
    /// `group_alias_{i}` scheme; results must be unchanged.
    #[tokio::test]
    async fn complex_group_expression() {
        let sql = "SELECT upper(account) AS k, COUNT(DISTINCT iban) AS d, COUNT(*) AS n \
                   FROM t GROUP BY upper(account) ORDER BY k";
        let rows = sample_rows();
        let with = ctx_with_table(&rows, true);
        let expected = results(&ctx_with_table(&rows, false), sql).await;
        assert_eq!(expected, results(&with, sql).await);
        let plan = logical_plan(&with, sql).await;
        assert!(!plan.contains("DISTINCT"), "rule must fire, got:\n{plan}");
    }
}
