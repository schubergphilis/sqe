//! Trino-compat AST rewrites that don't fit cleanly into a UDF.
//!
//! These rewrites operate on the parsed sqlparser AST before SQL is
//! handed to DataFusion. Each rewrite is a small, well-scoped fix for a
//! Trino syntax that DataFusion does not natively recognize.
//!
//! Currently:
//!
//! - `CAST(v AS JSON)` -> `to_json(v)`. Trino's JSON cast serializes a
//!   value to a JSON-formatted string; SQE already has a `to_json(v)` UDF
//!   that does exactly that. DataFusion's SQL planner does not recognize
//!   `JSON` as a target type for CAST, so without this rewrite users get
//!   `Error: Unsupported SQL type JSON` at planning time.
//!
//! - `SELECT * FROM "ns.t$snapshots"` -> `SELECT * FROM table_snapshots('ns', 't')`.
//!   Trino exposes Iceberg metadata tables as virtual tables with a `$`
//!   suffix on the table name (`$snapshots`, `$manifests`, `$history`,
//!   `$partitions`, `$files`, `$refs`). SQE already exposes the same data
//!   through TVF calls registered in `sqe-catalog::iceberg_metadata_tvf`;
//!   this rewriter translates the Trino-spelled FROM clause to the TVF
//!   call so `dbt-trino` macros that hard-code `$snapshots` work without
//!   modification.
//!
//! `CAST(json_col AS T)` (the inverse direction) is intentionally NOT
//! rewritten here. SQE represents JSON columns as `Utf8`, and DataFusion's
//! built-in `Utf8 -> T` coercion already parses numeric / boolean strings
//! into the target type. Users who need typed JSONPath extraction should
//! call `json_get_int(col, '$')`, `json_get_str(col, '$')`, etc. directly.
//!
//! The rewrite preserves the SQL string when no rewrite-eligible pattern
//! is present, so the cost is one parse + one serialize for every query
//! that hits a rewriter. Errors during parse fall through silently (the
//! rewriter returns the original SQL); DataFusion will surface the same
//! parse error itself.

use std::ops::ControlFlow;

use sqlparser::ast::{
    DataType as SqlDataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, GroupByExpr, GroupByWithModifier, Ident,
    LimitClause, ObjectName, Query, Select, SetExpr, Statement, TableFactor,
    TableFunctionArgs, Value, Visit, VisitMut, Visitor, VisitorMut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Maximum nesting depth permitted in any expression tree of attacker-supplied
/// SQL. sqlparser's infix-parse loop builds a depth-N left-leaning tree for a
/// flat chain like `a OR a OR ... OR a` WITHOUT consuming its recursion counter
/// (default 50), so a few thousand terms produce a tree deep enough that the
/// derived recursive `VisitMut` walk overflows the coordinator's stack — an
/// uncatchable OS-level abort that kills every concurrent query. 256 is far
/// above any genuine query and far below the ~16k-32k overflow threshold.
const MAX_EXPRESSION_DEPTH: usize = 256;

/// Visitor that tracks the live expression-nesting depth and bails the instant
/// it exceeds [`MAX_EXPRESSION_DEPTH`]. Because `pre_visit_expr` runs top-down
/// BEFORE the visitor descends into an expression's children, returning
/// `Break` here stops the recursive walk before it can go any deeper than the
/// cap — so this guard itself never recurses past the limit (plus the
/// parser-bounded query-nesting overhead), and detects the deep tree without
/// triggering the very overflow it prevents.
#[derive(Default)]
struct DepthGuard {
    current: usize,
    max_seen: usize,
}

impl Visitor for DepthGuard {
    type Break = ();

    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        self.current += 1;
        if self.current > self.max_seen {
            self.max_seen = self.current;
        }
        if self.current > MAX_EXPRESSION_DEPTH {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        self.current = self.current.saturating_sub(1);
        ControlFlow::Continue(())
    }
}

/// Reject SQL whose expression trees are nested deeper than
/// [`MAX_EXPRESSION_DEPTH`]. Run this BEFORE any recursive visitor (the
/// Trino-compat rewrite, and later DataFusion's analyzer) walks the AST, so a
/// crafted deep-chain query is turned into a clean error instead of a
/// stack-overflow process abort. Returns `Err` with a short message on
/// rejection.
pub fn check_expression_depth(statements: &[Statement]) -> Result<(), String> {
    let mut guard = DepthGuard::default();
    // sqlparser implements `Visit` for `Statement` (derived) but not for a
    // bare slice, so drive each statement individually. `guard.current` is
    // reset to a clean baseline between statements by the post-visit
    // decrements, so per-statement state does not leak.
    for stmt in statements {
        if let ControlFlow::Break(()) = stmt.visit(&mut guard) {
            return Err(format!(
                "expression nesting exceeds the maximum depth of {MAX_EXPRESSION_DEPTH}"
            ));
        }
    }
    Ok(())
}

/// Iceberg metadata-table suffixes Trino exposes via the `$<kind>` syntax
/// on a table name. Each maps to an SQE TVF registered in
/// `sqe-catalog::iceberg_metadata_tvf`. Listed lowercase; the rewriter
/// matches case-insensitively.
const METADATA_SUFFIXES: &[(&str, &str)] = &[
    ("snapshots", "table_snapshots"),
    ("manifests", "table_manifests"),
    ("history", "table_history"),
    ("partitions", "table_partitions"),
    ("files", "table_files"),
    ("refs", "table_refs"),
];

/// Rewrite Trino-compat AST patterns in `sql` that DataFusion does not
/// natively recognize. Returns the rewritten SQL as a new string. If the
/// input does not parse, returns the original string unchanged so the
/// downstream planner produces its own error message.
pub fn rewrite_trino_compat(sql: &str) -> String {
    let dialect = GenericDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return sql.to_string(),
    };

    // Cheap fast path: if the lowercased text contains none of the
    // rewriter triggers, no rewriter can fire. Skip the AST walk and the
    // re-serialization, which together cost more than the substring check
    // on the typical SQL string. Trigger words for each rewriter:
    //   `as json`              -> rewrite_cast_as_json
    //   `$`                    -> rewrite_metadata_dollar_table
    //   `rollup` / `cube` /
    //   `grouping sets`        -> wrap_rollup_for_empty_input
    let lower = sql.to_ascii_lowercase();
    let has_json_cast = lower.contains("as json");
    let has_dollar = lower.contains('$');
    let has_grouping_set = lower.contains("rollup")
        || lower.contains("cube")
        || lower.contains("grouping sets");
    // `current_schema` (bare keyword) -> rewrite_bare_current_schema. sqlparser
    // parses bare `current_schema` as a column identifier (unlike
    // `current_catalog`, which it treats as a reserved no-arg function), so we
    // rewrite it to the `current_schema()` call form the session UDF answers.
    let has_current_schema = lower.contains("current_schema");
    if !has_json_cast && !has_dollar && !has_grouping_set && !has_current_schema {
        return sql.to_string();
    }

    // Guard against a wire-crafted deep expression tree BEFORE the recursive
    // VisitMut walk below: a flat `a OR a OR ... OR a` chain parses into a
    // depth-N tree that overflows the stack inside `statements.visit(...)`.
    // The guard rides the (non-recursive-past-the-cap) visitor and bails
    // before the dangerous depth is reached. On rejection we skip the rewrite
    // and return the SQL untouched so the engine surfaces a normal error
    // rather than aborting the process; `parse_and_classify` rejects the same
    // input up front with a clean parse error.
    if check_expression_depth(&statements).is_err() {
        return sql.to_string();
    }

    let mut visitor = TrinoCompatVisitor::default();
    // Fully-qualified: `Visit` and `VisitMut` both expose a `visit` method, and
    // with both traits in scope (DepthGuard needs `Visit`) a bare
    // `statements.visit(..)` resolves to the `&self` `Visit::visit`, whose
    // `Visitor` bound `TrinoCompatVisitor` (a `VisitorMut`) does not satisfy.
    let _ = VisitMut::visit(&mut statements, &mut visitor);

    if visitor.rewrites == 0 {
        // Parsed cleanly but no rewrite fired. Avoid the round-trip
        // through Display: some SQL constructs do not round-trip exactly,
        // so leaving the original string alone preserves user formatting.
        return sql.to_string();
    }

    // Re-serialize each statement and join with `; `. sqlparser's Display
    // impl emits semicolon-separated statements without a trailing
    // semicolon, matching what `ctx.sql()` accepts.
    statements
        .iter()
        .map(Statement::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Combined visitor that runs every Trino-compat rewriter. One walk over
/// the AST, rewrites accumulate into the `rewrites` counter so the caller
/// knows whether to re-serialize.
///
/// `wrap_cte_depth` tracks how many enclosing `Query` nodes already carry
/// the empty-input ROLLUP wrap CTE.  When >0 we are inside an existing
/// wrap and must not wrap again — otherwise re-running the rewriter on
/// already-wrapped SQL would produce ever-more-nested wraps.
#[derive(Default)]
struct TrinoCompatVisitor {
    rewrites: usize,
    wrap_cte_depth: usize,
}

impl VisitorMut for TrinoCompatVisitor {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if rewrite_cast_as_json(expr) {
            self.rewrites += 1;
        }
        if rewrite_bare_current_schema(expr) {
            self.rewrites += 1;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(
        &mut self,
        factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if rewrite_metadata_dollar_table(factor) {
            self.rewrites += 1;
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        // Enter: if this Query already owns a wrap CTE, anything below it
        // (CTE body, outer-body subqueries) is "inside the wrap" for
        // re-wrap-suppression purposes.
        if has_wrap_cte(query) {
            self.wrap_cte_depth += 1;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        // Wrap only when we are NOT inside an existing wrap chain.  This
        // protects the inner CTE body (which still uses ROLLUP) from
        // being wrapped a second time when the rewriter runs on SQL that
        // came back through the same path.
        let was_wrap_owner = has_wrap_cte(query);
        if (self.wrap_cte_depth == 0 || was_wrap_owner)
            && wrap_rollup_for_empty_input(query)
        {
            self.rewrites += 1;
        }
        // Leave: pair the increment in pre_visit_query.
        if was_wrap_owner {
            self.wrap_cte_depth = self.wrap_cte_depth.saturating_sub(1);
        }
        ControlFlow::Continue(())
    }
}

/// True if `query` defines a CTE named `__sqe_rollup_q` (the wrap marker).
fn has_wrap_cte(query: &Query) -> bool {
    let Some(with) = &query.with else { return false };
    with.cte_tables
        .iter()
        .any(|cte| cte.alias.name.value == ROLLUP_WRAP_CTE)
}

/// Rewrite `Expr::Cast { data_type: JSON, expr }` to `to_json(expr)`.
/// Returns true if the rewrite fired.
fn rewrite_cast_as_json(expr: &mut Expr) -> bool {
    let Expr::Cast {
        kind: _,
        expr: inner,
        data_type,
        format: _,
        array: _,
    } = expr
    else {
        return false;
    };
    if !matches!(data_type, SqlDataType::JSON) {
        return false;
    }
    let inner_expr = std::mem::replace(
        inner.as_mut(),
        Expr::Identifier(Ident::new("__cast_as_json_placeholder__")),
    );
    *expr = Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new("to_json")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(inner_expr))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    });
    true
}

/// Rewrite a bare `current_schema` identifier into the `current_schema()`
/// call form. sqlparser parses bare `current_schema` as a column identifier
/// (so it reaches the planner as "No field named current_schema"), unlike
/// `current_catalog`, which it treats as a reserved no-arg function. Trino
/// clients send the bare keyword; the call form resolves to the session UDF.
/// Only unquoted identifiers are rewritten, so a quoted `"current_schema"`
/// column reference is left untouched. Returns true if the rewrite fired.
fn rewrite_bare_current_schema(expr: &mut Expr) -> bool {
    let Expr::Identifier(ident) = expr else {
        return false;
    };
    if ident.quote_style.is_some() || !ident.value.eq_ignore_ascii_case("current_schema") {
        return false;
    }
    *expr = Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new("current_schema")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    });
    true
}

/// Rewrite a `TableFactor::Table` whose name ends in `$<metadata_kind>`
/// (Trino's Iceberg metadata-table syntax) into a TVF call.
///
/// Examples:
///
/// - `FROM "ns.t$snapshots"` -> `FROM table_snapshots('ns', 't')`
/// - `FROM ns."t$manifests"` -> `FROM table_manifests('ns', 't')`
/// - `FROM "ns"."t$history"` -> `FROM table_history('ns', 't')`
///
/// The Trino quoted-identifier may collapse the namespace and the
/// `<table>$<kind>` part into a single identifier (when the user wrote
/// `"ns.t$snapshots"`) or split them across two idents (when each part
/// is quoted separately). We handle both shapes.
///
/// Returns true if the rewrite fired.
fn rewrite_metadata_dollar_table(factor: &mut TableFactor) -> bool {
    let TableFactor::Table {
        name,
        alias: _,
        args,
        version: _,
        ..
    } = factor
    else {
        return false;
    };

    // Already a TVF call? Don't double-rewrite.
    if args.is_some() {
        return false;
    }

    // The metadata suffix is on the LAST identifier component. Find it,
    // pull the suffix, and split into (bare_table, kind).
    let parts = &name.0;
    let Some(last) = parts.last().and_then(|p| p.as_ident()) else {
        return false;
    };
    let last_str = last.value.as_str();
    let dollar_pos = last_str.rfind('$');
    let Some(dollar_pos) = dollar_pos else {
        return false;
    };
    let bare_table_in_last = &last_str[..dollar_pos];
    let suffix = &last_str[dollar_pos + 1..];

    // Match the suffix case-insensitively against the known kinds.
    let tvf_name = METADATA_SUFFIXES
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(suffix))
        .map(|(_, tvf)| *tvf);
    let Some(tvf_name) = tvf_name else {
        return false;
    };

    // Extract namespace and table name. Two cases:
    //   1) Single-ident form: `"ns.tbl$snapshots"` -> last_str contains
    //      a `.` before the `$`. Split on the last `.`.
    //   2) Multi-ident form: `"ns"."tbl$snapshots"` -> parts.len() >= 2,
    //      take parts[0..len-1].join(".") as the namespace.
    let (namespace, table_name): (String, String) = if parts.len() >= 2 {
        let ns = parts[..parts.len() - 1]
            .iter()
            .filter_map(|p| p.as_ident())
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".");
        (ns, bare_table_in_last.to_string())
    } else if let Some(dot_in_last) = bare_table_in_last.rfind('.') {
        let ns = &bare_table_in_last[..dot_in_last];
        let tbl = &bare_table_in_last[dot_in_last + 1..];
        (ns.to_string(), tbl.to_string())
    } else {
        // Single-segment `"tbl$snapshots"` with no namespace. The TVFs
        // require both args, so we cannot rewrite without inventing a
        // namespace. Leave the FROM clause alone; DataFusion will produce
        // a "table not found" error which is the correct behaviour.
        return false;
    };

    // Build the TableFunctionArgs (two string-literal args).
    let args_vec = vec![
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
            Value::SingleQuotedString(namespace).into(),
        ))),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
            Value::SingleQuotedString(table_name).into(),
        ))),
    ];

    // Replace the table name and attach args.
    *name = ObjectName::from(vec![Ident::new(tvf_name)]);
    *args = Some(TableFunctionArgs {
        args: args_vec,
        settings: None,
    });

    true
}

/// CTE alias used by the empty-input ROLLUP wrap. Picked to be unlikely
/// to collide with a user-written identifier.
const ROLLUP_WRAP_CTE: &str = "__sqe_rollup_q";

/// Workaround for apache/datafusion#21570: `GROUP BY ROLLUP/CUBE/GROUPING
/// SETS` returns zero rows when the input is empty, where the SQL standard
/// requires the grand-total row.  Trino emits the grand-total row.
/// DataFusion does not.  Until the upstream fix lands, wrap every `Query`
/// whose top-level `Select` body uses grouping-set semantics so that an
/// empty result still produces a single all-NULL row.
///
/// Transformation:
///
/// ```sql
/// -- Before
/// SELECT a, SUM(b) FROM t GROUP BY ROLLUP(a) ORDER BY a LIMIT 10
///
/// -- After
/// WITH __sqe_rollup_q AS (SELECT a, SUM(b) FROM t GROUP BY ROLLUP(a))
/// SELECT __sqe_rollup_q.*
/// FROM (SELECT 1 AS __sqe_marker) AS __sqe_m
/// LEFT JOIN __sqe_rollup_q ON TRUE
/// ORDER BY a LIMIT 10
/// ```
///
/// `LEFT JOIN ... ON TRUE` against a 1-row left side produces a row of
/// NULLs when the right side is empty (`q` empty -> grand-total stand-in)
/// and the cross-product of `q`'s rows otherwise (`q` non-empty -> pass
/// through unchanged).  `ORDER BY` / `LIMIT` / `OFFSET` / `FETCH` are
/// lifted from the inner query to the outer so paging semantics are
/// preserved.
///
/// The wrap fires once per `Query` node visited; the visitor walks the
/// whole AST so nested ROLLUP subqueries (e.g. TPC-DS q67's
/// `(SELECT ... GROUP BY ROLLUP(...)) dw1`) are also covered.  An
/// idempotency guard skips queries that already carry the wrap CTE so a
/// second rewrite pass on rewritten SQL is a no-op.
///
/// Limitations:
/// - The synthetic row has NULL for every column, including
///   `GROUPING(col)` (would be `1` on Trino) and window functions like
///   `RANK()` (would be `1`).  The row counts match Trino for parity
///   tests; the value of the grand-total `GROUPING` and window columns
///   does not.  This is acceptable for the bench tool's row-count
///   comparison.  Drop the wrap once apache/datafusion#21570 lands and
///   SQE picks up a DataFusion release that includes the fix.
/// - Only fires when the immediate `Query.body` is a `Select` with a
///   grouping-set GROUP BY.  Set operations (`UNION ALL` of two
///   ROLLUP-using selects) are handled by the visitor recursing into
///   each side individually.
fn wrap_rollup_for_empty_input(query: &mut Query) -> bool {
    // Quick check: the body must be a plain SELECT (not a set operation,
    // parenthesised subquery, etc.) and that SELECT must have a
    // grouping-set GROUP BY clause.
    let body_uses_rollup = match query.body.as_ref() {
        SetExpr::Select(s) => select_uses_grouping_sets(s),
        _ => false,
    };
    if !body_uses_rollup {
        return false;
    }

    // Idempotency: if this Query already has our wrap CTE in its `with`,
    // don't re-wrap.  Prevents an unbounded blowup when this code path
    // runs more than once on the same SQL.
    if let Some(with) = &query.with {
        if with
            .cte_tables
            .iter()
            .any(|cte| cte.alias.name.value == ROLLUP_WRAP_CTE)
        {
            return false;
        }
    }

    // Lift outer clauses off the original query.  These ride on the
    // wrapper, not the inner CTE, so paging and ordering apply after the
    // empty-input row has been added (or not).
    let outer_order_by = query.order_by.take();
    // sqlparser 0.62 folds the old `limit` / `limit_by` / `offset` fields into
    // a single `limit_clause`. These three always rode together onto the same
    // target, so carrying the whole `Option<LimitClause>` is exactly
    // behaviour-preserving (and handles the MySQL `OFFSET,LIMIT` form for free).
    let outer_limit_clause = query.limit_clause.take();
    let outer_fetch = query.fetch.take();
    let outer_with = query.with.take();
    let outer_body = std::mem::replace(
        query.body.as_mut(),
        SetExpr::Select(Box::new(empty_placeholder_select())),
    );

    // Construct the inner Query that will become the CTE body.  Carries
    // forward any pre-existing WITH so previously-defined CTEs the user
    // wrote still resolve from inside __sqe_rollup_q.
    let inner_query = Query {
        with: outer_with,
        body: Box::new(outer_body),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    };

    // Build the wrapper by string-templating then re-parsing.  Trying to
    // construct the wrapper Query node by hand requires reproducing
    // sqlparser's exact AST layout for LEFT JOIN, AttachedToken spans,
    // empty Vecs, etc.; round-tripping through Parser is shorter, less
    // brittle, and inherits any future sqlparser changes for free.
    let inner_sql = inner_query.to_string();
    let wrap_sql = format!(
        "WITH {cte} AS ({inner}) \
         SELECT {cte}.* \
         FROM (SELECT 1 AS __sqe_marker) AS __sqe_m \
         LEFT JOIN {cte} ON TRUE",
        cte = ROLLUP_WRAP_CTE,
        inner = inner_sql,
    );

    let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, &wrap_sql) else {
        // Re-parse failed.  This should only happen if the inner SQL is
        // non-round-trip-safe (some sqlparser edge cases).  Restore the
        // original `Query` fields so the caller sees an untouched node
        // and the SQL goes to DataFusion unchanged.
        restore_query_fields(
            query,
            inner_query,
            outer_order_by,
            outer_limit_clause,
            outer_fetch,
        );
        return false;
    };
    if stmts.is_empty() {
        return false;
    }
    let Statement::Query(wrap_query) = stmts.remove(0) else {
        return false;
    };

    let mut new_q = *wrap_query;
    // Restore the outer clauses on the wrapper query so paging applies
    // to the unioned result, not the CTE.
    new_q.order_by = outer_order_by;
    new_q.limit_clause = outer_limit_clause;
    new_q.fetch = outer_fetch;

    *query = new_q;
    true
}

/// Helper: rebuild `query` from a saved inner query plus outer clauses.
/// Used to restore state when the wrap re-parse fails.
fn restore_query_fields(
    query: &mut Query,
    inner: Query,
    order_by: Option<sqlparser::ast::OrderBy>,
    limit_clause: Option<LimitClause>,
    fetch: Option<sqlparser::ast::Fetch>,
) {
    query.with = inner.with;
    *query.body = *inner.body;
    query.order_by = order_by;
    query.limit_clause = limit_clause;
    query.fetch = fetch;
}

/// Return true if `select.group_by` contains any grouping-set construct
/// (`ROLLUP(...)`, `CUBE(...)`, `GROUPING SETS (...)`, or the MySQL
/// `... WITH ROLLUP` / `... WITH CUBE` modifier).
fn select_uses_grouping_sets(select: &Select) -> bool {
    match &select.group_by {
        GroupByExpr::All(_) => false,
        GroupByExpr::Expressions(exprs, modifiers) => {
            if modifiers
                .iter()
                .any(|m| matches!(m, GroupByWithModifier::Rollup | GroupByWithModifier::Cube))
            {
                return true;
            }
            exprs.iter().any(|e| {
                matches!(
                    e,
                    Expr::Rollup(_) | Expr::Cube(_) | Expr::GroupingSets(_)
                )
            })
        }
    }
}

/// Build a throwaway empty `Select` used as a placeholder while the real
/// body is being moved out for wrapping.  Parsing an empty SELECT keeps
/// us from having to hand-construct an `AttachedToken`.
fn empty_placeholder_select() -> Select {
    let stmts = Parser::parse_sql(&GenericDialect {}, "SELECT 1")
        .expect("parse of literal `SELECT 1` cannot fail");
    let Statement::Query(q) = stmts.into_iter().next().expect("one statement") else {
        unreachable!("parsed `SELECT 1` is a Query");
    };
    let SetExpr::Select(s) = *q.body else {
        unreachable!("parsed `SELECT 1` is a Select");
    };
    *s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cast_as_json_rewritten_to_to_json() {
        let out = rewrite_trino_compat("SELECT CAST(123 AS JSON)");
        let lower = out.to_ascii_lowercase();
        assert!(
            lower.contains("to_json"),
            "expected to_json in rewritten SQL, got: {out}"
        );
        assert!(
            !lower.contains("as json"),
            "expected `AS JSON` to be removed, got: {out}"
        );
    }

    #[test]
    fn cast_as_json_with_complex_expr() {
        // The expression inside the CAST should round-trip intact.
        let out = rewrite_trino_compat("SELECT CAST(a + b * 2 AS JSON) FROM t");
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("to_json"));
        assert!(out.contains("a + b * 2"), "operands should survive: {out}");
    }

    #[test]
    fn nested_cast_as_json() {
        // Nested CASTs: outer is AS JSON, inner is AS BIGINT.
        let out = rewrite_trino_compat("SELECT CAST(CAST(x AS BIGINT) AS JSON) FROM t");
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("to_json"), "outer cast missing: {out}");
        assert!(lower.contains("cast"), "inner cast removed: {out}");
        assert!(lower.contains("bigint"), "BIGINT type removed: {out}");
    }

    #[test]
    fn no_cast_returns_input_unchanged() {
        let sql = "SELECT a, b FROM t WHERE c > 5";
        let out = rewrite_trino_compat(sql);
        assert_eq!(out, sql);
    }

    #[test]
    fn unparseable_input_returns_input_unchanged() {
        // Not even valid SQL; the rewriter must not panic and should
        // return the input verbatim so DataFusion produces its own error.
        let sql = "this is not SQL { @ }";
        let out = rewrite_trino_compat(sql);
        assert_eq!(out, sql);
    }

    #[test]
    fn other_cast_types_unchanged() {
        // Only AS JSON is rewritten. CAST AS BIGINT etc. must stay intact.
        let out = rewrite_trino_compat("SELECT CAST('123' AS BIGINT)");
        assert!(
            out.to_ascii_lowercase().contains("cast")
                && out.to_ascii_lowercase().contains("bigint"),
            "non-JSON CAST should be preserved: {out}"
        );
    }

    #[test]
    fn cast_as_json_in_where_clause() {
        let out = rewrite_trino_compat(
            "SELECT 1 FROM t WHERE CAST(payload AS JSON) IS NOT NULL",
        );
        assert!(
            out.to_ascii_lowercase().contains("to_json"),
            "WHERE-clause CAST should be rewritten: {out}"
        );
    }

    // ─── Metadata $-syntax rewriter ────────────────────────────────────────

    #[test]
    fn dollar_snapshots_single_quoted_full_name() {
        // Full name in one quoted ident: "ns.t$snapshots"
        let out = rewrite_trino_compat(r#"SELECT * FROM "ns.t$snapshots""#);
        let lower = out.to_ascii_lowercase();
        assert!(
            lower.contains("table_snapshots('ns', 't')"),
            "expected TVF call, got: {out}"
        );
        assert!(
            !lower.contains("$snapshots"),
            "$snapshots literal should be gone: {out}"
        );
    }

    #[test]
    fn dollar_snapshots_split_quoted_idents() {
        // Two quoted parts: "ns"."t$snapshots"
        let out = rewrite_trino_compat(r#"SELECT * FROM "ns"."t$snapshots""#);
        let lower = out.to_ascii_lowercase();
        assert!(
            lower.contains("table_snapshots('ns', 't')"),
            "expected TVF call, got: {out}"
        );
    }

    #[test]
    fn dollar_manifests_history_partitions_files_refs() {
        // Each suffix maps to a different TVF.
        let cases = [
            ("manifests", "table_manifests"),
            ("history", "table_history"),
            ("partitions", "table_partitions"),
            ("files", "table_files"),
            ("refs", "table_refs"),
        ];
        for (suffix, tvf) in cases {
            let sql = format!(r#"SELECT * FROM "ns.t${suffix}""#);
            let out = rewrite_trino_compat(&sql);
            let lower = out.to_ascii_lowercase();
            assert!(
                lower.contains(&format!("{tvf}('ns', 't')")),
                "expected {tvf}('ns', 't') for ${suffix}, got: {out}"
            );
        }
    }

    #[test]
    fn dollar_suffix_case_insensitive() {
        // Uppercase suffix should still match.
        let out = rewrite_trino_compat(r#"SELECT * FROM "ns.t$SNAPSHOTS""#);
        assert!(
            out.to_ascii_lowercase().contains("table_snapshots('ns', 't')"),
            "uppercase suffix should match, got: {out}"
        );
    }

    #[test]
    fn unknown_dollar_suffix_left_alone() {
        // `$wat` is not a known metadata table; the rewriter must not fire.
        // sqlparser may or may not parse this; either way the result must
        // not contain a TVF call.
        let out = rewrite_trino_compat(r#"SELECT * FROM "ns.t$wat""#);
        assert!(
            !out.to_ascii_lowercase().contains("table_wat("),
            "unknown $suffix should be left alone, got: {out}"
        );
    }

    #[test]
    fn dollar_in_normal_table_name_left_alone() {
        // No $<known_kind> suffix means no rewrite. `t$snapshots_archive`
        // does not match the suffix list and must be preserved.
        let out = rewrite_trino_compat(r#"SELECT * FROM "ns.t$snapshots_archive""#);
        assert!(
            !out.to_ascii_lowercase().contains("table_snapshots("),
            "non-suffix dollar should not trigger rewrite: {out}"
        );
    }

    #[test]
    fn no_namespace_dollar_table_left_alone() {
        // Single-segment `"t$snapshots"` cannot become a TVF call (no ns).
        // Leave it alone so DataFusion produces a table-not-found error.
        let out = rewrite_trino_compat(r#"SELECT * FROM "t$snapshots""#);
        assert!(
            !out.to_ascii_lowercase().contains("table_snapshots("),
            "single-segment $-table should not be rewritten: {out}"
        );
    }

    #[test]
    fn dollar_table_with_alias() {
        // Aliasing the metadata table should be preserved through rewrite.
        let out = rewrite_trino_compat(
            r#"SELECT s.committed_at FROM "ns.t$snapshots" AS s"#,
        );
        let lower = out.to_ascii_lowercase();
        assert!(
            lower.contains("table_snapshots('ns', 't')"),
            "expected TVF call, got: {out}"
        );
        // sqlparser preserves the AS alias through the rewrite.
        assert!(
            lower.contains(" s") || lower.contains("as s"),
            "alias should survive: {out}"
        );
    }

    #[test]
    fn dollar_table_combined_with_cast_as_json() {
        // Both rewriters should fire in one query.
        let out = rewrite_trino_compat(
            r#"SELECT CAST(snapshot_id AS JSON) FROM "ns.t$snapshots""#,
        );
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("to_json"), "JSON cast missing: {out}");
        assert!(
            lower.contains("table_snapshots('ns', 't')"),
            "TVF call missing: {out}"
        );
    }

    #[test]
    fn dollar_table_three_segment_namespace() {
        // catalog.schema.table format: three quoted parts.
        let out = rewrite_trino_compat(
            r#"SELECT * FROM "cat"."schema"."t$snapshots""#,
        );
        let lower = out.to_ascii_lowercase();
        // Namespace becomes "cat.schema"; the TVF takes the joined ns + bare table.
        assert!(
            lower.contains("table_snapshots('cat.schema', 't')"),
            "three-segment ns should join, got: {out}"
        );
    }

    // ── Empty-input ROLLUP wrap (DataFusion #21570 workaround) ──────────

    #[test]
    fn rollup_query_gets_wrapped() {
        let out = rewrite_trino_compat(
            "SELECT a, SUM(b) FROM t GROUP BY ROLLUP(a) ORDER BY a LIMIT 10",
        );
        let lower = out.to_ascii_lowercase();
        assert!(
            lower.contains("__sqe_rollup_q"),
            "wrap CTE missing: {out}"
        );
        assert!(
            lower.contains("left join __sqe_rollup_q"),
            "LEFT JOIN against wrap CTE missing: {out}"
        );
        assert!(
            lower.contains("order by a"),
            "ORDER BY should be lifted to outer: {out}"
        );
        assert!(
            lower.contains("limit 10"),
            "LIMIT should be lifted to outer: {out}"
        );
    }

    #[test]
    fn cube_query_gets_wrapped() {
        let out = rewrite_trino_compat(
            "SELECT a, b, SUM(c) FROM t GROUP BY CUBE(a, b)",
        );
        assert!(
            out.to_ascii_lowercase().contains("__sqe_rollup_q"),
            "CUBE should trigger wrap: {out}"
        );
    }

    #[test]
    fn grouping_sets_query_gets_wrapped() {
        let out = rewrite_trino_compat(
            "SELECT a, b, SUM(c) FROM t GROUP BY GROUPING SETS ((a), (b), ())",
        );
        assert!(
            out.to_ascii_lowercase().contains("__sqe_rollup_q"),
            "GROUPING SETS should trigger wrap: {out}"
        );
    }

    #[test]
    fn plain_group_by_is_not_wrapped() {
        let out = rewrite_trino_compat("SELECT a, SUM(b) FROM t GROUP BY a");
        assert!(
            !out.to_ascii_lowercase().contains("__sqe_rollup_q"),
            "plain GROUP BY must not wrap: {out}"
        );
    }

    #[test]
    fn select_without_group_by_is_not_wrapped() {
        let out = rewrite_trino_compat("SELECT a FROM t");
        assert!(
            !out.to_ascii_lowercase().contains("__sqe_rollup_q"),
            "no GROUP BY must not wrap: {out}"
        );
    }

    #[test]
    fn nested_rollup_subquery_gets_wrapped() {
        // TPC-DS q67 shape: ROLLUP inside an inner SELECT, outer wraps it
        // in another SELECT with RANK() and a filter.
        let out = rewrite_trino_compat(
            "SELECT * FROM (SELECT a, SUM(b) AS s FROM t GROUP BY ROLLUP(a)) dw \
             WHERE s IS NOT NULL ORDER BY a LIMIT 100",
        );
        assert!(
            out.to_ascii_lowercase().contains("__sqe_rollup_q"),
            "nested ROLLUP should trigger wrap somewhere: {out}"
        );
    }

    // ── Expression-depth guard (SQL-01) ────────────────────────────────

    #[test]
    fn shallow_expression_passes_depth_check() {
        let stmts = Parser::parse_sql(&GenericDialect {}, "SELECT a OR b OR c FROM t")
            .expect("parse");
        assert!(check_expression_depth(&stmts).is_ok());
    }

    #[test]
    fn deep_binary_chain_is_rejected_by_depth_check() {
        // A flat OR chain far past the cap parses cleanly (the parser's
        // recursion counter is not consumed by the infix loop) but builds a
        // very deep tree. The guard must reject it WITHOUT recursing deep
        // enough to overflow.
        let n = MAX_EXPRESSION_DEPTH + 200;
        let chain = std::iter::repeat_n("a", n).collect::<Vec<_>>().join(" OR ");
        let sql = format!("SELECT {chain} FROM t");
        let stmts = Parser::parse_sql(&GenericDialect {}, &sql).expect("parse");
        assert!(check_expression_depth(&stmts).is_err());
    }

    #[test]
    fn rewrite_trino_compat_does_not_overflow_on_deep_chain() {
        // The dollar fast-path trigger forces the rewriter past its skip, so
        // without the guard this would reach the recursive VisitMut and
        // overflow. With the guard it returns the SQL unchanged.
        let n = MAX_EXPRESSION_DEPTH + 500;
        let chain = std::iter::repeat_n("a", n).collect::<Vec<_>>().join(" OR ");
        // Embed a `$` so the cheap fast-path does not skip the walk.
        let sql = format!("SELECT {chain}, '$' AS marker FROM t");
        let out = rewrite_trino_compat(&sql);
        // No panic/abort; the deep input is returned untouched.
        assert_eq!(out, sql);
    }

    #[test]
    fn already_wrapped_is_not_double_wrapped() {
        // First pass wraps.  Second pass on the rewritten SQL must be a
        // no-op (no second nesting of __sqe_rollup_q).
        let once = rewrite_trino_compat(
            "SELECT a, SUM(b) FROM t GROUP BY ROLLUP(a)",
        );
        let twice = rewrite_trino_compat(&once);
        let count_once = once.matches("__sqe_rollup_q").count();
        let count_twice = twice.matches("__sqe_rollup_q").count();
        assert_eq!(
            count_once, count_twice,
            "idempotency violated:\n  once:  {once}\n  twice: {twice}"
        );
    }

    #[test]
    fn rewrites_bare_current_schema_to_call_form() {
        // sqlparser parses bare `current_schema` as a column identifier, so it
        // must be rewritten to `current_schema()` for the session UDF. (#1)
        let out = rewrite_trino_compat("SELECT current_schema");
        assert!(
            out.to_lowercase().contains("current_schema()"),
            "bare current_schema must become a call: {out}"
        );
    }

    #[test]
    fn quoted_current_schema_column_is_left_untouched() {
        // A quoted identifier is a real column reference, not the keyword.
        let out = rewrite_trino_compat(r#"SELECT "current_schema" FROM t"#);
        assert!(
            !out.contains("current_schema()"),
            "quoted column must not be rewritten: {out}"
        );
    }
}
