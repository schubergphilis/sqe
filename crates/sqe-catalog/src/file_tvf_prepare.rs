//! Ahead-of-planning preparation of the file table functions (architecture
//! review 806).
//!
//! `TableFunctionImpl::call` is synchronous and DataFusion invokes it while
//! planning, but `read_parquet` / `read_csv` / `read_json` have to reach
//! object storage to infer a schema. Until this module they bridged onto the
//! network from inside the hook (`runtime_bridge::block_on_compat`), which is
//! the same shape as the catalog listing hooks the rest of 806 removed: a
//! blocking call in the middle of the planner, and on a current-thread
//! runtime a dedicated thread with a hard deadline.
//!
//! The split here: every file table function implements
//! [`PreparableTableFunction::prepare`], an `async fn` that parses the
//! arguments, runs the path policy, builds the provider and stores it in the
//! function's [`PreparedProviders`] under a key derived from the arguments.
//! `call` then only parses, checks the policy again (cheap, and it keeps the
//! hook itself a gate) and looks the provider up. The coordinator, the
//! embedded CLI and any other planner entry point call
//! [`prepare_statement`] with the final SQL text right before `ctx.sql`; it
//! finds the file-function calls in the statement, converts their literal
//! arguments to the `Expr`s DataFusion would hand to `call`, and awaits each
//! function's `prepare`.
//!
//! A `call` whose arguments were never prepared is a planning error naming
//! the function and the entry point that forgot, not a silent fallback: the
//! bridge is gone, and the error is the only way a new planner path that
//! skips [`prepare_statement`] can be noticed.
//!
//! The session's file functions are recorded in a [`FileTableFunctions`]
//! stored as a `SessionConfig` extension by [`FileTableFunctions::register`],
//! which is also what registers them with DataFusion, so a session cannot
//! have a file function DataFusion knows about that `prepare_statement`
//! does not.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex, PoisonError};

use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_expr::{lit, Expr};
use futures::future::BoxFuture;
use datafusion::sql::sqlparser::ast::{
    self as sql, FunctionArg, FunctionArgExpr, TableFactor, Visit, Visitor,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::file_tvf_common::scalar_to_str;

/// Providers built by [`PreparableTableFunction::prepare`] for one function
/// instance, keyed by the argument list that `call` will see.
#[derive(Debug, Default)]
pub struct PreparedProviders {
    inner: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl PreparedProviders {
    /// The lookup key for an argument list: every literal rendered as its
    /// string value, `col = 'v'` named forms as `col=v`, so the `Expr`s the
    /// SQL planner builds and the ones [`prepare_statement`] builds from the
    /// same text agree regardless of how each spells a literal internally.
    pub fn key(exprs: &[Expr]) -> String {
        exprs
            .iter()
            .map(|e| match e {
                Expr::Literal(sv, _) => scalar_to_str(sv)
                    .map(str::to_string)
                    .unwrap_or_else(|| sv.to_string()),
                Expr::BinaryExpr(b) => match (b.left.as_ref(), b.right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(sv, _)) => format!(
                        "{}={}",
                        c.name,
                        scalar_to_str(sv)
                            .map(str::to_string)
                            .unwrap_or_else(|| sv.to_string())
                    ),
                    _ => format!("{e:?}"),
                },
                other => format!("{other:?}"),
            })
            .collect::<Vec<_>>()
            .join("\u{1f}")
    }

    /// Store the provider built for `exprs`, replacing an older one.
    pub fn insert(&self, exprs: &[Expr], provider: Arc<dyn TableProvider>) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(Self::key(exprs), provider);
    }

    /// The provider prepared for `exprs`, if [`Self::insert`] ran for it.
    pub fn get(&self, exprs: &[Expr]) -> Option<Arc<dyn TableProvider>> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&Self::key(exprs))
            .cloned()
    }

    /// The error `call` returns when nothing was prepared for its arguments.
    pub fn not_prepared(fn_name: &str, exprs: &[Expr]) -> DataFusionError {
        let path = exprs
            .first()
            .and_then(|e| match e {
                Expr::Literal(sv, _) => scalar_to_str(sv).map(str::to_string),
                _ => None,
            })
            .unwrap_or_default();
        DataFusionError::Plan(format!(
            "{fn_name}('{path}', ...): the table function was not prepared before planning. \
             Every planner entry point must await \
             sqe_catalog::file_tvf_prepare::prepare_statement(&ctx, sql) before ctx.sql(sql) \
             (architecture review 806: no network IO inside DataFusion's synchronous hooks)"
        ))
    }
}

/// A table function whose provider is built ahead of planning.
pub trait PreparableTableFunction: TableFunctionImpl {
    /// Parse `exprs`, enforce the path policy, build the provider and store it
    /// so the following `call(exprs)` can return it without blocking.
    fn prepare<'a>(&'a self, exprs: &'a [Expr]) -> BoxFuture<'a, DFResult<()>>;
}

/// The file table functions registered on one session, so
/// [`prepare_statement`] can find them by name. Lives in the session's
/// `SessionConfig` extensions.
#[derive(Default)]
pub struct FileTableFunctions {
    functions: Mutex<HashMap<String, Arc<dyn PreparableTableFunction>>>,
}

impl std::fmt::Debug for FileTableFunctions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<String> = self
            .functions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .cloned()
            .collect();
        f.debug_struct("FileTableFunctions").field("functions", &names).finish()
    }
}

impl FileTableFunctions {
    /// Register `function` with DataFusion under `name` and record it for
    /// [`prepare_statement`]. The one way a file table function reaches a
    /// session: `ctx.register_udtf` alone would leave `call` with nothing
    /// prepared.
    pub fn register(ctx: &SessionContext, name: &str, function: Arc<dyn PreparableTableFunction>) {
        let registry = match ctx.state().config().get_extension::<FileTableFunctions>() {
            Some(existing) => existing,
            None => {
                let fresh = Arc::new(FileTableFunctions::default());
                ctx.state_ref()
                    .write()
                    .config_mut()
                    .set_extension(Arc::clone(&fresh));
                fresh
            }
        };
        registry
            .functions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_ascii_lowercase(), Arc::clone(&function));
        ctx.register_udtf(name, function);
    }

    fn get(&self, name: &str) -> Option<Arc<dyn PreparableTableFunction>> {
        self.functions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&name.to_ascii_lowercase())
            .cloned()
    }

    /// Prepare every call to a registered file table function in `sql`.
    ///
    /// `sql` must be the text that is about to be handed to `ctx.sql`, after
    /// every rewrite (named `k => v` arguments are already positional
    /// `'k=v'` literals by then; both forms are accepted anyway). Text that
    /// does not parse prepares nothing and returns `Ok`: the planner reports
    /// the syntax error with its own wording. A call whose arguments are not
    /// literals is an error here, as it would be in `call`.
    pub async fn prepare_statement(&self, sql: &str) -> DFResult<()> {
        let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
            return Ok(());
        };
        let mut calls = FileFunctionCalls::default();
        for statement in &statements {
            let _ = statement.visit(&mut calls);
        }
        for (name, args) in calls.calls {
            let Some(function) = self.get(&name) else {
                continue;
            };
            let exprs = args
                .iter()
                .map(convert_arg)
                .collect::<DFResult<Vec<Expr>>>()
                .map_err(|e| DataFusionError::Plan(format!("{name}: {e}")))?;
            function.prepare(&exprs).await?;
        }
        Ok(())
    }
}

/// The file table functions this module knows how to prepare. A statement
/// that mentions none of them (case-insensitively) has nothing to prepare,
/// which lets callers skip the parse and the session-context build.
pub const FILE_TABLE_FUNCTIONS: [&str; 4] = ["read_parquet", "read_csv", "read_json", "read_delta"];

/// True when `sql` mentions one of [`FILE_TABLE_FUNCTIONS`]; a textual test
/// that may say yes for a comment or a string, never no for a real call.
pub fn mentions_file_function(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    FILE_TABLE_FUNCTIONS.iter().any(|f| lower.contains(f))
}

/// Prepare the file table functions `sql` calls on `ctx`; a no-op for a
/// session that registered none or a statement that calls none. Call right
/// before `ctx.sql(sql)`.
pub async fn prepare_statement(ctx: &SessionContext, sql: &str) -> DFResult<()> {
    if !mentions_file_function(sql) {
        return Ok(());
    }
    let Some(registry) = ctx.state().config().get_extension::<FileTableFunctions>() else {
        return Ok(());
    };
    registry.prepare_statement(sql).await
}

/// Every `FROM f(args)` in a statement, by lowercased function name.
#[derive(Default)]
struct FileFunctionCalls {
    calls: Vec<(String, Vec<FunctionArg>)>,
}

impl Visitor for FileFunctionCalls {
    type Break = ();

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
        if let TableFactor::Table {
            name,
            args: Some(args),
            ..
        } = factor
        {
            if let Some(last) = name.0.last().and_then(|p| p.as_ident()) {
                self.calls
                    .push((last.value.to_ascii_lowercase(), args.args.clone()));
            }
        }
        ControlFlow::Continue(())
    }
}

/// One sqlparser function argument as the `Expr` DataFusion's planner would
/// build for it: literals stay literals, `name => 'v'` and `name = 'v'`
/// become the positional `'name=v'` literal `parse_file_tvf_args` accepts.
fn convert_arg(arg: &FunctionArg) -> DFResult<Expr> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(sql::Expr::BinaryOp {
            left,
            op: sql::BinaryOperator::Eq,
            right,
        })) => match (left.as_ref(), literal_text(right)) {
            (sql::Expr::Identifier(ident), Some(value)) => {
                Ok(lit(format!("{}={value}", ident.value)))
            }
            _ => Err(DataFusionError::Plan(format!(
                "named argument must be `name = <literal>`, got `{arg}`"
            ))),
        },
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => literal_expr(expr)
            .ok_or_else(|| DataFusionError::Plan(format!("argument must be a literal, got `{arg}`"))),
        FunctionArg::Named { name, arg, .. } | FunctionArg::ExprNamed { name: sql::Expr::Identifier(name), arg, .. } => {
            match arg {
                FunctionArgExpr::Expr(expr) => literal_text(expr)
                    .map(|value| lit(format!("{}={value}", name.value)))
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "named argument `{}` must be a literal, got `{arg}`",
                            name.value
                        ))
                    }),
                _ => Err(DataFusionError::Plan(format!(
                    "named argument `{}` cannot be a wildcard",
                    name.value
                ))),
            }
        }
        other => Err(DataFusionError::Plan(format!(
            "unsupported table function argument `{other}`"
        ))),
    }
}

/// The `Expr::Literal` for a sqlparser literal expression, or `None` when it
/// is not one.
fn literal_expr(expr: &sql::Expr) -> Option<Expr> {
    match expr {
        sql::Expr::Value(v) => match &v.value {
            sql::Value::SingleQuotedString(s)
            | sql::Value::DoubleQuotedString(s)
            | sql::Value::EscapedStringLiteral(s)
            | sql::Value::TripleSingleQuotedString(s) => Some(lit(s.as_str())),
            sql::Value::Number(n, _) => {
                let text = n.to_string();
                text.parse::<i64>()
                    .map(lit)
                    .or_else(|_| text.parse::<f64>().map(lit))
                    .ok()
            }
            sql::Value::Boolean(b) => Some(lit(*b)),
            sql::Value::Null => Some(Expr::Literal(ScalarValue::Null, None)),
            _ => None,
        },
        sql::Expr::Nested(inner) => literal_expr(inner),
        sql::Expr::UnaryOp {
            op: sql::UnaryOperator::Minus,
            expr,
        } => match literal_expr(expr)? {
            Expr::Literal(ScalarValue::Int64(Some(i)), _) => Some(lit(-i)),
            Expr::Literal(ScalarValue::Float64(Some(f)), _) => Some(lit(-f)),
            _ => None,
        },
        _ => None,
    }
}

/// The text of a literal used as a named-argument value.
fn literal_text(expr: &sql::Expr) -> Option<String> {
    match literal_expr(expr)? {
        Expr::Literal(sv, _) => Some(
            scalar_to_str(&sv)
                .map(str::to_string)
                .unwrap_or_else(|| sv.to_string()),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Records what it was asked to prepare; `call` serves a stub only when
    /// prepared, like the real functions.
    #[derive(Debug, Default)]
    struct Recording {
        prepared: PreparedProviders,
        calls: Mutex<Vec<Vec<Expr>>>,
        prepares: AtomicUsize,
    }

    impl TableFunctionImpl for Recording {
        fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
            self.calls.lock().unwrap().push(exprs.to_vec());
            self.prepared
                .get(exprs)
                .ok_or_else(|| PreparedProviders::not_prepared("recording", exprs))
        }
    }

    impl PreparableTableFunction for Recording {
        fn prepare<'a>(&'a self, exprs: &'a [Expr]) -> BoxFuture<'a, DFResult<()>> {
            Box::pin(async move {
                self.prepares.fetch_add(1, Ordering::SeqCst);
                use datafusion::arrow::datatypes::{DataType, Field, Schema};
                let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
                let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![]])?;
                self.prepared.insert(exprs, Arc::new(table));
                Ok(())
            })
        }
    }

    fn session_with(name: &str) -> (SessionContext, Arc<Recording>) {
        let ctx = SessionContext::new();
        let f = Arc::new(Recording::default());
        FileTableFunctions::register(&ctx, name, Arc::clone(&f) as Arc<dyn PreparableTableFunction>);
        (ctx, f)
    }

    #[tokio::test]
    async fn a_call_without_preparation_is_a_planning_error_naming_the_function() {
        let (ctx, _f) = session_with("read_parquet");
        let err = ctx
            .sql("SELECT * FROM read_parquet('s3://b/x.parquet')")
            .await
            .expect_err("nothing was prepared");
        let msg = err.to_string();
        assert!(msg.contains("recording('s3://b/x.parquet'") && msg.contains("not prepared"), "{msg}");
        assert!(msg.contains("prepare_statement"), "{msg}");
    }

    #[tokio::test]
    async fn prepare_statement_builds_the_provider_the_planner_then_finds() {
        let (ctx, f) = session_with("read_parquet");
        let sql = "SELECT x FROM read_parquet('s3://b/x.parquet', 'region=eu') WHERE x > 1";
        prepare_statement(&ctx, sql).await.expect("prepare");
        assert_eq!(f.prepares.load(Ordering::SeqCst), 1);
        ctx.sql(sql).await.expect("planning finds the prepared provider");
        let calls = f.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            PreparedProviders::key(&calls[0]),
            PreparedProviders::key(&[lit("s3://b/x.parquet"), lit("region=eu")]),
            "the planner's argument list and the prepared one key the same"
        );
    }

    #[tokio::test]
    async fn named_and_equals_arguments_prepare_as_positional_literals() {
        let (ctx, f) = session_with("read_parquet");
        prepare_statement(
            &ctx,
            "SELECT * FROM read_parquet('/tmp/x.csv', delimiter => ';', header = true, n => 5)",
        )
        .await
        .expect("prepare");
        assert_eq!(f.prepares.load(Ordering::SeqCst), 1);
        assert!(f
            .prepared
            .get(&[lit("/tmp/x.csv"), lit("delimiter=;"), lit("header=true"), lit("n=5")])
            .is_some());
    }

    #[tokio::test]
    async fn unregistered_functions_and_unparsable_text_prepare_nothing() {
        let (ctx, f) = session_with("read_parquet");
        prepare_statement(&ctx, "SELECT * FROM other_fn('a') JOIN t ON true")
            .await
            .expect("other functions are not ours");
        prepare_statement(&ctx, "SELEC broken FROM").await.expect("syntax is the planner's job");
        assert_eq!(f.prepares.load(Ordering::SeqCst), 0);

        let plain = SessionContext::new();
        prepare_statement(&plain, "SELECT 1").await.expect("no registry is a no-op");
    }

    #[tokio::test]
    async fn a_non_literal_argument_is_rejected_at_prepare_time() {
        let (ctx, _f) = session_with("read_parquet");
        let err = prepare_statement(&ctx, "SELECT * FROM read_parquet(some_column)")
            .await
            .expect_err("columns are not arguments");
        assert!(err.to_string().contains("literal"), "{err}");
    }

    #[test]
    fn keys_ignore_how_a_string_literal_is_spelled_internally() {
        let utf8 = [Expr::Literal(ScalarValue::Utf8(Some("p".into())), None)];
        let large = [Expr::Literal(ScalarValue::LargeUtf8(Some("p".into())), None)];
        assert_eq!(PreparedProviders::key(&utf8), PreparedProviders::key(&large));
        assert_ne!(PreparedProviders::key(&utf8), PreparedProviders::key(&[lit("q")]));
    }
}
