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
    LimitClause, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
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
    // `as uuid` / `as ipaddress` -> rewrite_cast_custom_to_varchar. Trino's
    // UUID and IPADDRESS types have no DataFusion equivalent (CAST -> a
    // NOT_SUPPORTED error); both are string-representable, so on the read path
    // we rewrite the cast to VARCHAR.
    let has_custom_cast = lower.contains("as uuid") || lower.contains("as ipaddress");
    // `json '...'` -> rewrite_json_typed_string: a Trino JSON typed-string
    // literal, which DataFusion rejects; rewritten to a plain string (SQE
    // stores JSON as Utf8).
    let has_json_literal = lower.contains("json '");
    // `row(` -> the Trino ROW(...) constructor and the `CAST(ROW(...) AS
    // ROW(...))` named-row cast. The bare constructor becomes `struct(...)`;
    // the constructor-then-cast idiom becomes `named_struct(...)`. A false
    // positive (e.g. a column literally named `arrow`) is harmless: it only
    // forces the AST walk, which rewrites nothing it should not.
    let has_row = lower.contains("row(");
    if !has_json_cast
        && !has_dollar
        && !has_grouping_set
        && !has_current_schema
        && !has_custom_cast
        && !has_json_literal
        && !has_row
    {
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

/// Give each unaliased expression column in the top-level projection Trino's
/// positional `_colN` name (N = 0-based position in the SELECT list).
///
/// Trino names an anonymous output column `_col0`, `_col1`, ... by its absolute
/// position in the select list (so `SELECT a, count(*)` yields `a`, `_col1`).
/// DataFusion instead names such a column after its expression text (e.g.
/// `Int64(1) + Int64(1)`), which BI clients display verbatim. This rewrite adds
/// the explicit alias so DataFusion emits the Trino-style name.
///
/// Scope and limits, all deliberate:
/// - Only the outermost query's output projection is rewritten. Anonymous
///   columns inside subqueries / CTEs never surface as output names, so they
///   keep DataFusion's naming. For a set operation (`UNION`, etc.) the output
///   names come from the leftmost SELECT, which is the one rewritten.
/// - Plain column references (`c`, `t.c`) and already-aliased items keep their
///   natural name; only genuine expressions are renamed.
/// - If the projection contains a `*` / `t.*` wildcard the rewrite is skipped
///   entirely: a wildcard's column count is unknown before planning, so the
///   absolute positions of any following expressions cannot be computed at the
///   AST level. A wrong `_colN` is worse than DataFusion's fallback name.
///
/// This is applied on the Trino wire path only (the HTTP server's pre-parse
/// chain), so native Flight SQL clients keep DataFusion's column names. Returns
/// the rewritten SQL, or the input unchanged when nothing was renamed or the
/// input does not parse.
pub fn alias_anonymous_select_columns(sql: &str) -> String {
    let dialect = GenericDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return sql.to_string(),
    };

    let mut changed = false;
    for stmt in &mut statements {
        if let Statement::Query(query) = stmt {
            if let Some(select) = leftmost_select_mut(query.body.as_mut()) {
                if alias_select_projection(select) {
                    changed = true;
                }
            }
        }
    }

    if !changed {
        // Preserve the user's exact text when no rename fired; the Display
        // round-trip does not reproduce every input verbatim.
        return sql.to_string();
    }

    statements
        .iter()
        .map(Statement::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Follow a set-expression tree to the leftmost `Select`, whose projection
/// determines the output column names (Trino, like SQL, takes set-operation
/// result names from the first branch). Returns `None` for bodies that have no
/// SELECT projection (`VALUES`, `INSERT`, ...).
fn leftmost_select_mut(body: &mut SetExpr) -> Option<&mut Select> {
    match body {
        SetExpr::Select(s) => Some(s.as_mut()),
        SetExpr::SetOperation { left, .. } => leftmost_select_mut(left.as_mut()),
        SetExpr::Query(q) => leftmost_select_mut(q.body.as_mut()),
        _ => None,
    }
}

/// Rewrite `select`'s projection in place, aliasing each unaliased expression
/// column to `_col<position>`. Returns true if any item was renamed. Skips the
/// whole projection when it contains a wildcard (see
/// [`alias_anonymous_select_columns`]).
fn alias_select_projection(select: &mut Select) -> bool {
    let has_wildcard = select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    });
    if has_wildcard {
        return false;
    }

    let mut changed = false;
    for (i, item) in select.projection.iter_mut().enumerate() {
        let SelectItem::UnnamedExpr(expr) = item else {
            // ExprWithAlias(es) already carry a name; wildcards are excluded above.
            continue;
        };
        // A bare column reference already has a usable name in both engines.
        if matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            continue;
        }
        let taken = std::mem::replace(expr, Expr::Identifier(Ident::new("__sqe_colN_tmp")));
        *item = SelectItem::ExprWithAlias {
            expr: taken,
            alias: Ident::new(format!("_col{i}")),
        };
        changed = true;
    }
    changed
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
        if rewrite_cast_custom_to_varchar(expr) {
            self.rewrites += 1;
        }
        if rewrite_cast_as_json(expr) {
            self.rewrites += 1;
        }
        if rewrite_bare_current_schema(expr) {
            self.rewrites += 1;
        }
        if rewrite_json_typed_string(expr) {
            self.rewrites += 1;
        }
        // ROW constructor rewrite runs before the cast rewrite at this node,
        // but post-order traversal means any *inner* ROW(...) has already
        // become struct(...) by the time we reach an enclosing Cast. The cast
        // rewrite accepts either name, so the order within this method does
        // not matter.
        if rewrite_row_constructor(expr) {
            self.rewrites += 1;
        }
        if rewrite_cast_as_row(expr) {
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

/// Rewrite `CAST(x AS UUID)` / `CAST(x AS IPADDRESS)` to `CAST(x AS VARCHAR)`.
///
/// Trino's UUID and IPADDRESS types have no DataFusion equivalent, so the cast
/// would surface as a NOT_SUPPORTED error. Both are string-representable, so on
/// the read path (BI clients over generic datasets) we map them to VARCHAR.
/// This is a display-level compatibility shim: it does not validate the UUID /
/// IP-address format the way Trino's native cast would. `ROW(...)` casts are
/// deliberately not handled here (named-field struct casts are out of scope).
/// Returns true if the rewrite fired.
fn rewrite_cast_custom_to_varchar(expr: &mut Expr) -> bool {
    let Expr::Cast { data_type, .. } = expr else {
        return false;
    };
    // sqlparser models UUID as a dedicated variant; IPADDRESS is unknown to it
    // and lands as a single-part Custom type.
    let is_target = match &*data_type {
        SqlDataType::Uuid => true,
        SqlDataType::Custom(name, _modifiers) => {
            name.0.len() == 1
                && name.0[0]
                    .as_ident()
                    .map(|i| i.value.eq_ignore_ascii_case("ipaddress"))
                    .unwrap_or(false)
        }
        _ => false,
    };
    if is_target {
        *data_type = SqlDataType::Varchar(None);
        true
    } else {
        false
    }
}

/// Rewrite `Expr::Cast { data_type: JSON, expr }` to `to_json(expr)`.
/// Returns true if the rewrite fired.
/// Rewrite a Trino `JSON '<text>'` typed-string literal to a plain string
/// literal. DataFusion's planner rejects the JSON typed string
/// ("Unsupported ... JSON"); SQE represents JSON columns as Utf8, and the
/// string value carries the same content. Returns true if the rewrite fired.
fn rewrite_json_typed_string(expr: &mut Expr) -> bool {
    if let Expr::TypedString(ts) = expr {
        if matches!(ts.data_type, SqlDataType::JSON) {
            *expr = Expr::Value(ts.value.clone());
            return true;
        }
    }
    false
}

/// Rewrite the Trino `ROW(...)` row constructor to DataFusion's `struct(...)`.
///
/// Trino spells an anonymous row value `ROW(1, 'a', true)`; DataFusion's
/// equivalent built-in is `struct(...)`, which produces a struct with
/// positional field names `c0`, `c1`, .... sqlparser parses `ROW(...)` as a
/// plain function call named `ROW`, so the rewrite is a single rename. Only
/// the single-part, unquoted, case-insensitive `ROW` name is matched, so a
/// quoted `"row"` column-returning function is left untouched. Returns true if
/// the rewrite fired.
fn rewrite_row_constructor(expr: &mut Expr) -> bool {
    let Expr::Function(func) = expr else {
        return false;
    };
    if !function_name_is(func, "row") {
        return false;
    }
    func.name = ObjectName::from(vec![Ident::new("struct")]);
    true
}

/// Rewrite `CAST(ROW(v1, v2, ...) AS ROW(n1 t1, n2 t2, ...))` to
/// `named_struct('n1', CAST(v1 AS t1), 'n2', CAST(v2 AS t2), ...)`.
///
/// This is Trino's exact named-row semantics: the cast labels each positional
/// field and coerces its value. sqlparser parses the target `ROW(...)` type as
/// a `Custom` type whose modifier list is the flattened `[name, type, name,
/// type, ...]` sequence (parameterized field types like `decimal(10,2)` do not
/// parse at all, so only single-token field types reach this rewrite).
///
/// Only the constructor-then-cast idiom is handled: the inner expression must
/// itself be a `ROW(...)` / `struct(...)` constructor with one argument per
/// declared field. `CAST(<struct column> AS ROW(...))` is intentionally left
/// alone -- reconstructing it would require positional `get_field('c0')`
/// access that breaks on a real column whose fields carry real names, so the
/// fragile case is surfaced as a normal DataFusion error rather than a wrong
/// answer. Returns true if the rewrite fired.
fn rewrite_cast_as_row(expr: &mut Expr) -> bool {
    let Expr::Cast {
        expr: inner,
        data_type,
        ..
    } = expr
    else {
        return false;
    };

    // Target type must be a single-part `ROW` custom type with a non-empty,
    // even modifier list (name/type pairs).
    let SqlDataType::Custom(type_name, modifiers) = data_type else {
        return false;
    };
    let is_row_type = type_name.0.len() == 1
        && type_name.0[0]
            .as_ident()
            .map(|i| i.value.eq_ignore_ascii_case("row"))
            .unwrap_or(false);
    if !is_row_type || modifiers.is_empty() || modifiers.len() % 2 != 0 {
        return false;
    }
    let field_count = modifiers.len() / 2;

    // Inner expression must be a ROW/struct constructor with one arg per field.
    let Expr::Function(func) = inner.as_ref() else {
        return false;
    };
    if !function_name_is(func, "row") && !function_name_is(func, "struct") {
        return false;
    }
    let FunctionArguments::List(arg_list) = &func.args else {
        return false;
    };
    if arg_list.args.len() != field_count {
        return false;
    }

    // Pair each constructor argument with its declared field name and type,
    // then build `named_struct('name', CAST(arg AS type), ...)` by templating
    // and re-parsing. Hand-constructing the named_struct call would mean
    // parsing each type token into a `SqlDataType` ourselves; round-tripping
    // through the parser is shorter and reuses sqlparser's type grammar.
    let mut parts: Vec<String> = Vec::with_capacity(field_count);
    for (i, arg) in arg_list.args.iter().enumerate() {
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = arg else {
            return false;
        };
        let field_name = modifiers[i * 2].replace('\'', "''");
        let field_type = &modifiers[i * 2 + 1];
        parts.push(format!("'{field_name}', CAST({arg_expr} AS {field_type})"));
    }
    let new_sql = format!("SELECT named_struct({})", parts.join(", "));

    let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, &new_sql) else {
        return false;
    };
    let Some(Statement::Query(q)) = stmts.drain(..).next() else {
        return false;
    };
    let SetExpr::Select(select) = *q.body else {
        return false;
    };
    let mut projection = select.projection;
    let Some(item) = projection.drain(..).next() else {
        return false;
    };
    let new_expr = match item {
        sqlparser::ast::SelectItem::UnnamedExpr(e) => e,
        _ => return false,
    };
    *expr = new_expr;
    true
}

/// True if `func` is a single-part, unquoted function name matching `want`
/// case-insensitively (e.g. distinguishes the `ROW` keyword-constructor from a
/// quoted `"row"` user function).
fn function_name_is(func: &Function, want: &str) -> bool {
    func.name.0.len() == 1
        && func.name.0[0]
            .as_ident()
            .map(|i| i.quote_style.is_none() && i.value.eq_ignore_ascii_case(want))
            .unwrap_or(false)
}

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

    // Flatten the reference into bare name segments, handling both the
    // multi-ident form (`"ns"."tbl$snapshots"`) and the single dotted ident
    // (`"ns.tbl$snapshots"`). The suffix has already been stripped from the
    // last segment.
    let segments: Vec<String> = if parts.len() >= 2 {
        let mut segs: Vec<String> = parts[..parts.len() - 1]
            .iter()
            .filter_map(|p| p.as_ident())
            .map(|i| i.value.clone())
            .collect();
        segs.push(bare_table_in_last.to_string());
        segs
    } else {
        bare_table_in_last.split('.').map(str::to_string).collect()
    };

    // The metadata TVFs take (namespace, table). SQE models a single-level
    // namespace plus an optional catalog prefix that handlers resolve against
    // the session's bound catalog regardless (the metadata TVFs use the
    // session's effective catalog). So the namespace is the segment
    // immediately before the table; any catalog (or higher) prefix is dropped.
    // Passing the catalog-qualified `catalog.schema` as the namespace made
    // Polaris fail to load the table (#317).
    let n = segments.len();
    if n < 2 {
        // Single-segment `"tbl$snapshots"` with no namespace. The TVFs require
        // both args, so we cannot rewrite without inventing a namespace. Leave
        // the FROM clause alone; DataFusion produces a "table not found" error,
        // which is the correct behaviour.
        return false;
    }
    let namespace = segments[n - 2].clone();
    let table_name = segments[n - 1].clone();

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
        // catalog.schema.table format: three quoted parts. The catalog prefix
        // is dropped -- the metadata TVF resolves against the session's bound
        // catalog (like every other table op in SQE), and Iceberg/Polaris wants
        // the bare schema as the namespace, not `catalog.schema`. Passing the
        // catalog-qualified namespace made Polaris fail with "table does not
        // exist" (#317).
        let out = rewrite_trino_compat(
            r#"SELECT * FROM "cat"."schema"."t$snapshots""#,
        );
        let lower = out.to_ascii_lowercase();
        assert!(
            lower.contains("table_snapshots('schema', 't')"),
            "catalog should be dropped, leaving the bare schema, got: {out}"
        );
    }

    #[test]
    fn dollar_table_three_segment_single_ident() {
        // The single-identifier dotted form must drop the catalog the same way.
        let out = rewrite_trino_compat(r#"SELECT * FROM "cat.schema.t$snapshots""#);
        let lower = out.to_ascii_lowercase();
        assert!(
            lower.contains("table_snapshots('schema', 't')"),
            "catalog should be dropped from the single-ident form, got: {out}"
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

    #[test]
    fn rewrites_cast_uuid_and_ipaddress_to_varchar() {
        // Trino UUID / IPADDRESS casts have no DataFusion equivalent; map to
        // VARCHAR so the read path succeeds. (#6)
        let out = rewrite_trino_compat("SELECT CAST(id AS UUID) FROM t").to_uppercase();
        assert!(out.contains("CAST(ID AS VARCHAR)"), "got: {out}");
        assert!(!out.contains("AS UUID"), "got: {out}");

        let out = rewrite_trino_compat("SELECT CAST(addr AS ipaddress) FROM t").to_uppercase();
        assert!(out.contains("CAST(ADDR AS VARCHAR)"), "got: {out}");
        assert!(!out.contains("IPADDRESS"), "got: {out}");
    }

    // ── _colN aliasing of unaliased expression columns (#8) ────────────

    #[test]
    fn anonymous_expression_gets_col0() {
        let out = alias_anonymous_select_columns("SELECT 1 + 1");
        assert!(out.contains("AS _col0"), "expected _col0 alias: {out}");
    }

    #[test]
    fn col_index_is_absolute_select_position() {
        // Trino numbers by absolute position: plain `a` keeps its name, the
        // expression at position 1 becomes _col1, the literal at 2 becomes
        // _col2.
        let out = alias_anonymous_select_columns("SELECT a, 2 + 2, 'h' FROM t");
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("2 + 2 as _col1"), "expr should be _col1: {out}");
        assert!(out.contains("'h' AS _col2"), "literal should be _col2: {out}");
        // The plain column `a` is not aliased.
        assert!(!out.contains("a AS _col0"), "plain column must keep its name: {out}");
    }

    #[test]
    fn aggregate_after_plain_column_is_col1() {
        // The canonical Trino case: SELECT a, count(*) -> a, _col1.
        let out = alias_anonymous_select_columns("SELECT a, count(*) FROM t GROUP BY a");
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("count(*) as _col1"), "count(*) should be _col1: {out}");
    }

    #[test]
    fn explicit_alias_is_preserved() {
        let out = alias_anonymous_select_columns("SELECT 1 + 1 AS total, 2 * 2");
        assert!(out.contains("AS total"), "explicit alias must survive: {out}");
        assert!(out.contains("AS _col1"), "second expr should be _col1: {out}");
        assert!(!out.to_ascii_lowercase().contains("as _col0"), "aliased item keeps name: {out}");
    }

    #[test]
    fn qualified_column_reference_is_not_aliased() {
        let out = alias_anonymous_select_columns("SELECT t.a, t.b FROM t");
        assert!(
            !out.to_ascii_lowercase().contains("_col"),
            "plain qualified columns must not be aliased: {out}"
        );
        // No rename fired -> input returned verbatim.
        assert_eq!(out, "SELECT t.a, t.b FROM t");
    }

    #[test]
    fn projection_with_wildcard_is_skipped() {
        // A wildcard makes following positions uncomputable, so the whole
        // projection is left alone.
        let sql = "SELECT *, 1 + 1 FROM t";
        let out = alias_anonymous_select_columns(sql);
        assert_eq!(out, sql, "wildcard projection must be left untouched: {out}");
    }

    #[test]
    fn union_aliases_leftmost_select() {
        // Output column names come from the first branch of a UNION.
        let out = alias_anonymous_select_columns("SELECT 1 + 1 UNION SELECT 2 + 2");
        assert!(out.contains("AS _col0"), "leftmost select should be aliased: {out}");
    }

    #[test]
    fn rewrites_json_literal_to_string() {
        // Trino `JSON '<text>'` -> a plain string literal (SQE stores JSON as
        // Utf8); DataFusion rejects the JSON typed string. (#7)
        let out = rewrite_trino_compat(r#"SELECT JSON '{"a": 1}' AS j"#);
        assert!(!out.to_uppercase().contains("JSON '"), "JSON literal kept: {out}");
        assert!(out.contains(r#"'{"a": 1}'"#), "string value lost: {out}");
    }

    #[test]
    fn row_constructor_rewritten_to_struct() {
        // Trino ROW(...) anonymous constructor -> DataFusion struct(...). (#7)
        let out = rewrite_trino_compat("SELECT ROW(1, 'a', true)");
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("struct("), "ROW must become struct(): {out}");
        assert!(!lower.contains("row("), "ROW( should be gone: {out}");
        assert!(out.contains('1') && out.contains("'a'"), "args must survive: {out}");
    }

    #[test]
    fn quoted_row_function_left_untouched() {
        // A quoted "row" identifier is a user function, not the keyword.
        let out = rewrite_trino_compat(r#"SELECT "row"(1, 2)"#);
        assert!(
            !out.to_ascii_lowercase().contains("struct("),
            "quoted row function must not be rewritten: {out}"
        );
    }

    #[test]
    fn cast_row_to_named_row_uses_named_struct() {
        // CAST(ROW(...) AS ROW(name type, ...)) -> named_struct with per-field
        // CAST. Trino's named-row semantics. (#7)
        let out = rewrite_trino_compat(
            "SELECT CAST(ROW(1, 'a') AS ROW(x int, y varchar))",
        );
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("named_struct("), "expected named_struct: {out}");
        assert!(lower.contains("'x'") && lower.contains("'y'"), "field names: {out}");
        assert!(
            lower.contains("cast(1 as int)"),
            "first field cast missing: {out}"
        );
        assert!(
            lower.contains("cast('a' as varchar)"),
            "second field cast missing: {out}"
        );
        // The outer ROW custom type must be gone (DataFusion rejects it).
        assert!(!lower.contains("as row("), "ROW cast type should be gone: {out}");
    }

    #[test]
    fn cast_struct_column_to_row_is_left_alone() {
        // CAST(<column> AS ROW(...)) is the fragile case we deliberately do
        // not reconstruct: the inner expr is not a ROW/struct constructor, so
        // named_struct must not fire.
        let out = rewrite_trino_compat("SELECT CAST(c AS ROW(x int, y varchar)) FROM t");
        assert!(
            !out.to_ascii_lowercase().contains("named_struct("),
            "column cast must not become named_struct: {out}"
        );
    }

    #[test]
    fn cast_row_arg_count_mismatch_left_alone() {
        // Field count != constructor arg count: do not produce a half-built
        // named_struct. The inner ROW still becomes struct() (harmless).
        let out = rewrite_trino_compat("SELECT CAST(ROW(1) AS ROW(x int, y varchar))");
        assert!(
            !out.to_ascii_lowercase().contains("named_struct("),
            "arg/field mismatch must not build named_struct: {out}"
        );
    }

    #[test]
    fn leaves_other_custom_casts_untouched() {
        // A non-UUID/ipaddress custom type (e.g. a v3 TIMESTAMP_NS) must not be
        // rewritten to VARCHAR.
        let out = rewrite_trino_compat("SELECT CAST(x AS BIGINT) FROM t");
        assert!(!out.to_uppercase().contains("VARCHAR"), "got: {out}");
    }
}
