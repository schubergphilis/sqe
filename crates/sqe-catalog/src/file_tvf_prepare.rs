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

tokio::task_local! {
    /// Providers prepared for the statement being executed. See
    /// [`in_statement`].
    static STATEMENT_PROVIDERS: Arc<StatementProviders>;
}

/// The file-function providers of one statement, keyed by function name and
/// argument list.
///
/// Per statement, not per function instance: the instances live on the
/// session's cached `SessionContext`, and a statement may plan on a context
/// other than the one it prepared on. Another statement's commit invalidates
/// the user's session cache, so the write handler's context for a CTAS can
/// be a fresh one whose functions never saw the front door's `prepare`. With
/// four dbt threads on one token that made `CREATE TABLE ... AS SELECT ...
/// FROM read_parquet(...)` fail intermittently with "not prepared before
/// planning". Keying by name lets any context's function find what this
/// statement prepared, and nothing outlives the statement.
#[derive(Default)]
struct StatementProviders {
    inner: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

/// Run `fut`, one statement's execution or planning, with a statement-scoped
/// provider map. Nested calls reuse the outer scope, so a statement's
/// front-door preparation is visible to every plan it makes.
pub async fn in_statement<F: std::future::Future>(fut: F) -> F::Output {
    if STATEMENT_PROVIDERS.try_with(|_| ()).is_ok() {
        fut.await
    } else {
        STATEMENT_PROVIDERS
            .scope(Arc::new(StatementProviders::default()), fut)
            .await
    }
}

/// What [`plan_sql`] and [`plan_parsed`] let DataFusion plan.
///
/// `SessionContext::sql` does not only plan: a DDL statement runs right
/// there, before the caller holds a plan. `SELECT ... INTO
/// datafusion.public.t FROM governed` therefore collected the governed table
/// raw into a session MemTable before the policy rewriter ever saw a plan,
/// and the next query read it back with no row filter or restriction
/// applied. Nothing may execute before policy enforcement sees the plan, so
/// DDL is refused here; the engine's own DDL never reaches DataFusion
/// through SQL text (scratch relations are registered through the API).
fn planning_options() -> datafusion::execution::context::SQLOptions {
    datafusion::execution::context::SQLOptions::new().with_allow_ddl(false)
}

/// Prepare the file table functions `sql` calls, then plan it: the one way
/// a planner entry point should reach `ctx.sql`. Refuses DDL
/// ([`planning_options`]).
pub async fn plan_sql(ctx: &SessionContext, sql: &str) -> DFResult<datafusion::dataframe::DataFrame> {
    in_statement(async {
        prepare_statement(ctx, sql).await?;
        ctx.sql_with_options(sql, planning_options()).await
    })
    .await
}

/// [`plan_sql`] for a session no policy governs (the embedded CLI: no auth,
/// no policy), whose users create tables through DataFusion's own DDL. DDL
/// runs while planning, so never call this where a policy enforcer exists.
pub async fn plan_sql_without_policy(
    ctx: &SessionContext,
    sql: &str,
) -> DFResult<datafusion::dataframe::DataFrame> {
    in_statement(async {
        prepare_statement(ctx, sql).await?;
        ctx.sql(sql).await
    })
    .await
}

/// [`plan_sql`] preparing from a statement the caller already parsed (the
/// engine's front door accepts syntax `GenericDialect` does not).
pub async fn plan_parsed(
    ctx: &SessionContext,
    statement: &sql::Statement,
    sql: &str,
) -> DFResult<datafusion::dataframe::DataFrame> {
    in_statement(async {
        prepare_parsed_statement(ctx, statement).await?;
        ctx.sql_with_options(sql, planning_options()).await
    })
    .await
}

/// [`plan_sql`] for callers that hold a `SessionState` and want the logical
/// plan.
pub async fn create_logical_plan(
    state: &datafusion::execution::SessionState,
    sql: &str,
) -> DFResult<datafusion_expr::LogicalPlan> {
    in_statement(async {
        if mentions_file_function(sql) {
            if let Some(registry) = state.config().get_extension::<FileTableFunctions>() {
                registry.prepare_statement(sql).await?;
            }
        }
        state.create_logical_plan(sql).await
    })
    .await
}

/// A file table function's handle on the current statement's prepared
/// providers. Holds only the function's name; the providers themselves live
/// in the statement scope ([`in_statement`]).
#[derive(Debug, Default)]
pub struct PreparedProviders {
    name: String,
}

impl PreparedProviders {
    /// The handle for the function registered as `name`.
    pub fn named(name: &str) -> Self {
        Self {
            name: name.to_ascii_lowercase(),
        }
    }

    fn slot(&self, exprs: &[Expr]) -> String {
        format!("{}\u{1e}{}", self.name, Self::key(exprs))
    }

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

    /// Store the provider built for `exprs` in the current statement's scope.
    /// Outside a statement scope there is nowhere to keep it: the following
    /// `call` then reports "not prepared", naming the entry point to fix.
    pub fn insert(&self, exprs: &[Expr], provider: Arc<dyn TableProvider>) {
        let slot = self.slot(exprs);
        let stored = STATEMENT_PROVIDERS.try_with(|scope| {
            scope
                .inner
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(slot, provider);
        });
        if stored.is_err() {
            tracing::debug!(function = %self.name, "file table function prepared outside a statement scope; dropped");
        }
    }

    /// The provider this statement prepared for `exprs`, if any.
    pub fn get(&self, exprs: &[Expr]) -> Option<Arc<dyn TableProvider>> {
        let slot = self.slot(exprs);
        STATEMENT_PROVIDERS
            .try_with(|scope| {
                scope
                    .inner
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(&slot)
                    .cloned()
            })
            .ok()
            .flatten()
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
             Every planner entry point must plan through \
             sqe_catalog::file_tvf_prepare::plan_sql (or prepare inside in_statement) \
             rather than calling ctx.sql directly \
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
        self.prepare_parsed(&statements).await
    }

    /// Prepare every call in statements the caller has already parsed.
    ///
    /// For a caller holding the engine's own parse of the user's text:
    /// `GenericDialect` rejects syntax the engine accepts (a CTAS with
    /// `PARTITIONED BY (month(d))`), and [`Self::prepare_statement`] then
    /// prepares nothing, so the write handler's later `ctx.sql` of the
    /// SELECT failed with "the table function was not prepared".
    pub async fn prepare_parsed(&self, statements: &[sql::Statement]) -> DFResult<()> {
        let mut calls = FileFunctionCalls::default();
        for statement in statements {
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
            // A statement prepares each call once: the front door prepares
            // from its own parse, and the write handler's `plan_sql` of the
            // CTAS SELECT would otherwise list the same prefix again.
            if PreparedProviders::named(&name).get(&exprs).is_some() {
                continue;
            }
            function.prepare(&exprs).await?;
        }
        Ok(())
    }
}

/// The file table functions this module knows how to prepare. A statement
/// that mentions none of them (case-insensitively) has nothing to prepare,
/// which lets callers skip the parse and the session-context build.
pub const FILE_TABLE_FUNCTIONS: [&str; 3] = ["read_parquet", "read_csv", "read_json"];

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

/// [`prepare_statement`] for a statement the caller already parsed (the
/// engine's front door accepts syntax `GenericDialect` does not).
pub async fn prepare_parsed_statement(
    ctx: &SessionContext,
    statement: &sql::Statement,
) -> DFResult<()> {
    let Some(registry) = ctx.state().config().get_extension::<FileTableFunctions>() else {
        return Ok(());
    };
    registry.prepare_parsed(std::slice::from_ref(statement)).await
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
        assert!(msg.contains("plan_sql"), "{msg}");
    }

    #[tokio::test]
    async fn prepare_statement_builds_the_provider_the_planner_then_finds() {
        let (ctx, f) = session_with("read_parquet");
        let sql = "SELECT x FROM read_parquet('s3://b/x.parquet', 'region=eu') WHERE x > 1";
        plan_sql(&ctx, sql).await.expect("planning finds the prepared provider");
        assert_eq!(f.prepares.load(Ordering::SeqCst), 1);
        let calls = f.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            PreparedProviders::key(&calls[0]),
            PreparedProviders::key(&[lit("s3://b/x.parquet"), lit("region=eu")]),
            "the planner's argument list and the prepared one key the same"
        );
    }

    #[tokio::test]
    async fn a_partitioned_ctas_prepares_from_the_engines_parse_not_the_text() {
        // The text form fails GenericDialect (`PARTITIONED BY (month(d))`),
        // so text-based preparation silently does nothing: the bug that
        // broke every partitioned TPC-H load. The engine's pre-parse
        // pipeline rewrites the clause; its parse must prepare the call.
        let (ctx, f) = session_with("read_parquet");
        in_statement(async {
        let sql = "CREATE TABLE t PARTITIONED BY (month(d)) \
                   AS SELECT * FROM read_parquet('s3://b/x.parquet', region => 'eu')";
        prepare_statement(&ctx, sql).await.expect("text path returns Ok");
        assert_eq!(f.prepares.load(Ordering::SeqCst), 0, "the text path cannot parse this");

        let rewritten = sqe_sql::pre_parse_pipeline(&sqe_sql::UserSql::from(sql)).expect("pipeline");
        let statements =
            Parser::parse_sql(&GenericDialect {}, rewritten.as_str()).expect("engine form parses");
        prepare_parsed_statement(&ctx, &statements[0]).await.expect("prepare");
        assert_eq!(f.prepares.load(Ordering::SeqCst), 1);
        assert!(f
            .prepared
            .get(&[lit("s3://b/x.parquet"), lit("region=eu")])
            .is_some());
        })
        .await;
    }

    #[tokio::test]
    async fn named_and_equals_arguments_prepare_as_positional_literals() {
        let (ctx, f) = session_with("read_parquet");
        in_statement(async {
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
        })
        .await;
    }

    /// The dbt race: the statement prepares at the front door on the cached
    /// context, another statement's commit invalidates the cache, and the
    /// write handler plans the CTAS SELECT on a freshly built context whose
    /// functions never ran `prepare`. Inside one statement that must still
    /// plan; a later statement must not see the earlier one's providers.
    #[tokio::test]
    async fn a_statement_finds_its_providers_on_a_rebuilt_context() {
        let (front_door, f1) = session_with("read_parquet");
        let (rebuilt, f2) = session_with("read_parquet");
        let sql = "SELECT * FROM read_parquet('s3://cbs-landing/raw/x/')";
        in_statement(async {
            prepare_statement(&front_door, sql).await.expect("prepare");
            rebuilt.sql(sql).await.expect("the rebuilt context finds this statement's provider");
        })
        .await;
        assert_eq!(f1.prepares.load(Ordering::SeqCst), 1);
        assert_eq!(f2.prepares.load(Ordering::SeqCst), 0);

        // Nothing outlives the statement.
        let err = rebuilt.sql(sql).await.expect_err("a new statement starts empty");
        assert!(err.to_string().contains("not prepared"), "{err}");
        assert!(f1.prepared.get(&[lit("s3://cbs-landing/raw/x/")]).is_none());
    }

    /// Four dbt threads on one token share one cached context. Each statement
    /// prepares and plans in its own scope; none loses another's providers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_statements_on_one_context_all_plan() {
        let (ctx, f) = session_with("read_parquet");
        let ctx = Arc::new(ctx);
        let tasks: Vec<_> = (0..16)
            .map(|i| {
                let ctx = Arc::clone(&ctx);
                tokio::spawn(async move {
                    let sql = format!("SELECT * FROM read_parquet('s3://b/part-{}/')", i % 4);
                    plan_sql(&ctx, &sql).await.map(|_| ())
                })
            })
            .collect();
        for task in tasks {
            task.await.expect("join").expect("every parallel statement plans");
        }
        assert_eq!(f.prepares.load(Ordering::SeqCst), 16, "one prepare per statement");
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

    /// `SELECT ... INTO` is DDL that `SessionContext::sql` would run while
    /// planning, before policy enforcement holds a plan; it must be refused,
    /// and nothing may be registered.
    #[tokio::test]
    async fn planning_refuses_ddl_and_registers_nothing() {
        let ctx = SessionContext::new();
        for sql in [
            "SELECT 1 AS a INTO into_t",
            "CREATE TABLE ctas_t AS SELECT 1 AS a",
            "CREATE VIEW view_t AS SELECT 1 AS a",
        ] {
            plan_sql(&ctx, sql).await.expect_err(sql);
            let statement = Parser::parse_sql(&GenericDialect {}, sql).unwrap().remove(0);
            plan_parsed(&ctx, &statement, sql).await.expect_err(sql);
        }
        for t in ["into_t", "ctas_t", "view_t"] {
            assert!(ctx.table(t).await.is_err(), "{t} was registered");
        }
        plan_sql(&ctx, "SELECT 1").await.expect("queries still plan");
    }
}
