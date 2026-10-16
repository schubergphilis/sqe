//! Keep SQE's function overrides winning when DataFusion rebuilds a session.
//!
//! `register_udf` files a function under its name and every alias, so an
//! override only replaces the keys it names. A DataFusion built-in that
//! claims the same name as an alias (`to_char` claims `date_format`) stays in
//! the map under its other keys. Rebuilding the state
//! (`SessionStateBuilder::from(ctx.state())`, done for every query with a
//! memory quota) registers each function again in `HashMap` order, and the
//! built-in takes back the key whenever it comes last. Which function a query
//! runs was then down to chance.
//!
//! [`release_alias`] re-registers such a built-in without the alias, so every
//! function in the map holds every key it claims and the order a rebuild goes
//! in no longer matters. [`shadowed_keys`] is the check for it.

use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{ExprSchema, Result as DFResult, ScalarValue};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::preimage::PreimageResult;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, StructFieldMapping,
};
use datafusion::prelude::SessionContext;

/// Re-register the scalar built-in `builtin` without `alias`, which an SQE
/// override owns. No-op when `builtin` is not registered or does not claim
/// `alias`. The key `alias` is left alone, so this works before or after the
/// override is registered.
pub fn release_alias(ctx: &SessionContext, builtin: &str, alias: &str) {
    let Some(udf) = ctx.state().scalar_functions().get(builtin).cloned() else {
        return;
    };
    if udf.name() != builtin || !udf.aliases().iter().any(|a| a == alias) {
        return;
    }
    ctx.register_udf(ScalarUDF::new_from_impl(WithoutAliases::new(
        udf.as_ref().clone(),
        &[alias],
    )));
}

/// A scalar function minus some of its aliases. Delegates everything else.
#[derive(Debug, PartialEq, Eq, Hash)]
struct WithoutAliases {
    inner: ScalarUDF,
    aliases: Vec<String>,
}

impl WithoutAliases {
    fn new(inner: ScalarUDF, released: &[&str]) -> Self {
        let aliases = inner
            .aliases()
            .iter()
            .filter(|a| !released.contains(&a.as_str()))
            .cloned()
            .collect();
        Self { inner, aliases }
    }

    fn imp(&self) -> &Arc<dyn ScalarUDFImpl> {
        self.inner.inner()
    }
}

#[warn(clippy::missing_trait_methods)] // Delegates, so it should implement every single trait method
impl ScalarUDFImpl for WithoutAliases {
    fn name(&self) -> &str {
        self.imp().name()
    }

    fn display_name(&self, args: &[Expr]) -> DFResult<String> {
        #[expect(deprecated)]
        self.imp().display_name(args)
    }

    fn schema_name(&self, args: &[Expr]) -> DFResult<String> {
        self.imp().schema_name(args)
    }

    fn signature(&self) -> &Signature {
        self.imp().signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.imp().return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        self.imp().return_field_from_args(args)
    }

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        #[expect(deprecated)]
        self.imp().is_nullable(args, schema)
    }

    fn is_strict(&self) -> bool {
        self.imp().is_strict()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        self.imp().invoke_with_args(args)
    }

    // A rebuild calls this; the updated function must stay narrowed or it
    // would claim the released alias again.
    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        let updated = self.imp().with_updated_config(config)?;
        let released: Vec<&str> = self
            .inner
            .aliases()
            .iter()
            .filter(|a| !self.aliases.contains(a))
            .map(String::as_str)
            .collect();
        Some(ScalarUDF::new_from_impl(Self::new(updated, &released)))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> DFResult<ExprSimplifyResult> {
        self.imp().simplify(args, info)
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> DFResult<PreimageResult> {
        self.imp().preimage(args, lit_expr, info)
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.imp().conditional_arguments(args)
    }

    fn short_circuits(&self) -> bool {
        self.imp().short_circuits()
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> DFResult<Interval> {
        self.imp().evaluate_bounds(input)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> DFResult<Option<Vec<Interval>>> {
        self.imp().propagate_constraints(interval, inputs)
    }

    fn struct_field_mapping(
        &self,
        literal_args: &[Option<ScalarValue>],
    ) -> Option<StructFieldMapping> {
        self.imp().struct_field_mapping(literal_args)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> DFResult<SortProperties> {
        self.imp().output_ordering(inputs)
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> DFResult<bool> {
        self.imp().preserves_lex_ordering(inputs)
    }

    fn strictly_order_preserving(&self, inputs: &[ExprProperties]) -> DFResult<bool> {
        self.imp().strictly_order_preserving(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        self.imp().coerce_types(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.imp().documentation()
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.imp().placement(args)
    }
}

/// Every `(function, key)` where the function claims the key, as its name or
/// an alias, but the key resolves to another function. Empty when a rebuild
/// in any order reproduces the same map.
pub fn shadowed_keys(state: &SessionState) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (kind, claims) in [
        ("scalar", scalar_claims(state)),
        ("aggregate", aggregate_claims(state)),
        ("window", window_claims(state)),
    ] {
        for (function, key, holds) in claims {
            if !holds {
                out.push((format!("{kind} {function}"), key));
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

fn scalar_claims(state: &SessionState) -> Vec<(String, String, bool)> {
    let map = state.scalar_functions();
    map.values()
        .flat_map(|f| {
            std::iter::once(f.name().to_string())
                .chain(f.aliases().iter().cloned())
                .map(|key| {
                    let holds = map.get(&key).is_some_and(|g| std::sync::Arc::ptr_eq(f, g));
                    (f.name().to_string(), key, holds)
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn aggregate_claims(state: &SessionState) -> Vec<(String, String, bool)> {
    let map = state.aggregate_functions();
    map.values()
        .flat_map(|f| {
            std::iter::once(f.name().to_string())
                .chain(f.aliases().iter().cloned())
                .map(|key| {
                    let holds = map.get(&key).is_some_and(|g| std::sync::Arc::ptr_eq(f, g));
                    (f.name().to_string(), key, holds)
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn window_claims(state: &SessionState) -> Vec<(String, String, bool)> {
    let map = state.window_functions();
    map.values()
        .flat_map(|f| {
            std::iter::once(f.name().to_string())
                .chain(f.aliases().iter().cloned())
                .map(|key| {
                    let holds = map.get(&key).is_some_and(|g| std::sync::Arc::ptr_eq(f, g));
                    (f.name().to_string(), key, holds)
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::execution::session_state::SessionStateBuilder;

    /// The function set the coordinator registers on every session.
    fn sqe_ctx() -> SessionContext {
        let mut ctx = crate::duckdb_test_ctx();
        crate::register_trino_functions(&ctx);
        crate::register_extended_trino_functions(&ctx);
        datafusion_functions_json::register_all(&mut ctx).expect("register json udfs");
        ctx
    }

    /// Every key and the function it resolves to, told apart by `Debug` so
    /// DataFusion's `now` and SQE's, which share a name, differ.
    fn resolved(state: &SessionState) -> Vec<(String, String)> {
        let mut out: Vec<_> = state
            .scalar_functions()
            .iter()
            .map(|(k, f)| (k.clone(), format!("{:?}", f.inner())))
            .collect();
        out.sort();
        out
    }

    #[test]
    fn every_function_holds_the_keys_it_claims() {
        let shadowed = shadowed_keys(&sqe_ctx().state());
        assert!(shadowed.is_empty(), "shadowed keys: {shadowed:?}");
    }

    /// What a query with a memory quota does to its session. A rebuild
    /// re-registers in `HashMap` order, which is fixed for one map but varies
    /// with each map's hash seed, so every round builds a fresh session, as
    /// each connection does. Fifty rounds make a lucky order all but
    /// impossible.
    #[test]
    fn a_rebuild_resolves_every_key_to_the_same_function() {
        for round in 0..50 {
            let state = sqe_ctx().state();
            let before = resolved(&state);
            let after = resolved(&SessionStateBuilder::from(state).build());
            let changed: Vec<_> = after.iter().filter(|e| !before.contains(e)).collect();
            assert!(changed.is_empty(), "rebuild {round} changed {changed:?}");
            assert_eq!(
                after.len(),
                before.len(),
                "rebuild {round} changed the keys"
            );
        }
    }

    async fn first_row(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let batch = &batches[0];
        (0..batch.num_columns())
            .map(|c| array_value_to_string(batch.column(c), 0).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn rebuilt_sessions_run_the_trino_functions() {
        for _ in 0..20 {
            let state = sqe_ctx().state();
            let ctx = SessionContext::new_with_state(SessionStateBuilder::from(state).build());
            let row = first_row(
                &ctx,
                "SELECT arrow_typeof(now()), arrow_typeof(current_timestamp), \
                 date_format(TIMESTAMP '2024-01-02 03:04:05', '%Y-%m-%d'), \
                 arrow_typeof(element_at(MAP {'a': 1}, 'a'))",
            )
            .await;
            assert_eq!(
                row,
                ["Timestamp(µs)", "Timestamp(µs)", "2024-01-02", "Int64"]
            );
        }
    }

    #[tokio::test]
    async fn released_builtins_keep_their_own_names() {
        let ctx = sqe_ctx();
        let row = first_row(
            &ctx,
            "SELECT to_char(DATE '2024-01-02', '%Y'), map_extract(MAP {'a': 1}, 'a')",
        )
        .await;
        assert_eq!(row, ["2024", "[1]"]);
    }
}
