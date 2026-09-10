// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! AST for the V2 SELECT-core slice plus the V3 DML/DDL slice (spec
//! 002-parser Requirements 2-4), plus the V4 join slice (#237), the V4
//! subquery-expression slice (#238), the V4 GROUP BY/HAVING slice
//! (#239), and the V6 non-recursive `WITH`/CTE slice (#375,
//! `WithClause`/`CommonTableExpr`).
//!
//! Scoped to `.openspec/grammar/sqlite.ebnf`'s `(* V2 *)`/`(* V3 *)`/
//! `(* V4 *)`-tagged rules: SELECT with an INNER/LEFT [OUTER]/CROSS join
//! chain (`FromClause`/`Join`/`JoinOp`/`JoinConstraint`, #237), WHERE,
//! GROUP BY, HAVING (#239), ORDER BY, LIMIT/OFFSET, the V2 expression
//! grammar, INSERT/UPDATE/DELETE, and CREATE/DROP TABLE/INDEX, plus the
//! V4 subquery-expression slice (#238, including correlated
//! subqueries): scalar subqueries (`ExprKind::Subquery`), `IN (SELECT
//! ...)` (`ExprKind::InSubquery`), and `EXISTS (SELECT ...)`
//! (`ExprKind::Exists`) — correlation is resolved at codegen time
//! (`Scope::with_outer`), not represented differently in the AST.
//! NATURAL/RIGHT/FULL joins, `USING`, comma-style joins, `ANY`/`ALL`/
//! `SOME` quantified comparisons, and multi-column `IN` do not exist here
//! at all, nor does FOREIGN KEY/REFERENCES (V8). Subqueries in FROM
//! (#257) are `TableRefKind::Subquery`.
//!
//! Every node carries a [`Span`] (Requirement 3: "AST completeness") and
//! parenthesized expressions are preserved explicitly via `ExprKind::Paren`
//! rather than discarded, so `SELECT (a + b) * c` round-trips its grouping.

use super::tokenizer::Span;

/// An `UPDATE table SET col = expr, ... [WHERE expr]` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    /// `OR REPLACE`/`OR IGNORE`/etc. conflict resolution, if given.
    pub or_action: Option<ConflictAction>,
    /// Name of the table being updated.
    pub table: String,
    /// The `SET` list — one entry per `col = expr` or expanded tuple
    /// assignment (see [`Assignment::columns`]).
    pub assignments: Vec<Assignment>,
    /// The `WHERE` clause, if given.
    pub where_clause: Option<Expr>,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// One `col = expr` (or expanded tuple-assignment) entry in an
/// [`Update`]'s `SET` list.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// One column for `col = expr`; the tuple form
    /// `(col1, col2) = (expr1, expr2)` is expanded into one [`Assignment`]
    /// per column, each carrying its paired expr from the RHS list.
    pub columns: Vec<String>,
    /// The RHS expression paired with `columns`.
    pub value: Expr,
}

/// A `SELECT` statement, including its `WITH` prefix and any `UNION`
/// compound arms.
#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    /// `WITH cte { , cte }` prefix (#375, non-recursive only —
    /// `WITH RECURSIVE` is out of scope here). #376's
    /// `codegen::expand_with_clause` rewrites this away before codegen
    /// proper runs: each CTE reference in `FROM`/`JOIN` becomes a
    /// `TableRefKind::Subquery`, materialized into an ephemeral table
    /// the same way #257's `FROM`-subqueries already are.
    pub with_clause: Option<WithClause>,
    /// `DISTINCT`/`ALL` qualifier on the result columns, if given.
    pub distinct: Option<Distinctness>,
    /// The result-column list (`SELECT a, b, ...`).
    pub columns: Vec<ResultColumn>,
    /// The `FROM` clause, if given.
    pub from: Option<FromClause>,
    /// The `WHERE` clause, if given.
    pub where_clause: Option<Expr>,
    /// The `GROUP BY` expression list.
    pub group_by: Vec<Expr>,
    /// The `HAVING` clause, if given.
    pub having: Option<Expr>,
    /// `UNION ALL` arms (#240) chained after this `Select`'s own
    /// core (`distinct`/`columns`/`from`/`where_clause`/`group_by`/
    /// `having`). `order_by`/`limit` below apply to the whole compound
    /// statement, not to any individual arm — matching SQLite's
    /// grammar, where only the outermost `select-stmt` carries a
    /// trailing ORDER BY/LIMIT.
    pub compound: Vec<CompoundSelect>,
    /// The `ORDER BY` term list, applying to the whole compound statement.
    pub order_by: Vec<OrderingTerm>,
    /// The `LIMIT [OFFSET]` clause, applying to the whole compound statement.
    pub limit: Option<Limit>,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// Non-recursive `WITH` clause (#375): `WITH cte { , cte }`, prefixing a
/// `select-stmt`. `WITH RECURSIVE` is not represented here — a bare
/// `WITH` is parsed, `WITH RECURSIVE` remains `unsupported(..)`.
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    /// The `cte { , cte }` definitions, in source order.
    pub ctes: Vec<CommonTableExpr>,
    /// The source span covering the whole clause.
    pub span: Span,
}

/// One `cte_name [(col, ...)] AS (select-stmt)` definition within a
/// `WithClause`.
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr {
    /// The CTE's name, referenced from `FROM`/`JOIN` in the enclosing query.
    pub name: String,
    /// Optional explicit column-name list `(col, ...)`.
    pub columns: Option<Vec<String>>,
    /// The CTE's own `select-stmt`, boxed since `Select` recurses.
    pub query: Box<Select>,
    /// The source span covering the whole definition.
    pub span: Span,
}

/// One `UNION ALL SELECT ...` arm of a compound `SELECT` (#240). Same
/// shape as `Select`'s own core, minus `order_by`/`limit` (see
/// [`Select::compound`]).
#[derive(Debug, Clone, PartialEq)]
pub struct CompoundSelect {
    /// `UNION`/`UNION ALL` operator joining this arm to the previous one.
    pub op: CompoundOp,
    /// `DISTINCT`/`ALL` qualifier on this arm's result columns, if given.
    pub distinct: Option<Distinctness>,
    /// This arm's result-column list.
    pub columns: Vec<ResultColumn>,
    /// This arm's `FROM` clause, if given.
    pub from: Option<FromClause>,
    /// This arm's `WHERE` clause, if given.
    pub where_clause: Option<Expr>,
    /// This arm's `GROUP BY` expression list.
    pub group_by: Vec<Expr>,
    /// This arm's `HAVING` clause, if given.
    pub having: Option<Expr>,
    /// The source span covering this arm.
    pub span: Span,
}

/// `UnionAll` (#240) and plain `Union` (#377/#378, dedup via a shared
/// ephemeral index across every arm — see
/// [`crate::codegen::select::compile_select_compound`]) are
/// implemented; `INTERSECT`/`EXCEPT` remain unsupported (deferred to
/// V7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompoundOp {
    /// `UNION ALL` — keeps duplicate rows.
    UnionAll,
    /// `UNION` — dedups rows across every arm.
    Union,
}

/// `EXPLAIN [QUERY PLAN] select-stmt` (#243) — pulled forward from its
/// original V7 slot (`.openspec/grammar/sqlite.ebnf`'s `explain-stmt`)
/// because the planner's join equality-index-selection work needs EQP
/// output to be observable now. Wraps only a `Select`: the acceptance
/// criterion this exists for ("EXPLAIN QUERY PLAN shows index usage")
/// is about the join planner, not `EXPLAIN`'s general opcode-dump form
/// over every statement kind — that broader form remains future scope.
#[derive(Debug, Clone, PartialEq)]
pub struct Explain {
    /// `true` for `EXPLAIN QUERY PLAN`, `false` for bare `EXPLAIN`.
    pub query_plan: bool,
    /// The wrapped `select-stmt`.
    pub select: Box<Select>,
}

/// `DISTINCT`/`ALL` qualifier on a `SELECT`'s result columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distinctness {
    /// `DISTINCT`.
    Distinct,
    /// `ALL`.
    All,
}

/// One entry in a `SELECT` result-column list.
#[derive(Debug, Clone, PartialEq)]
pub enum ResultColumn {
    /// Bare `*`.
    Star,
    /// `table.*`.
    TableStar {
        /// The table whose columns are expanded.
        table: String,
    },
    /// A single expression, optionally `AS alias`d.
    Expr {
        /// The projected expression.
        expr: Expr,
        /// The `AS alias`, if given.
        alias: Option<String>,
    },
}

/// A `FROM`-clause table entry (#237): either a real catalog table by
/// name, or (#257) a parenthesized `select-stmt` materialized at codegen
/// time into an ephemeral table — `SELECT * FROM (SELECT ...) AS sub`.
#[derive(Debug, Clone, PartialEq)]
pub enum TableRefKind {
    /// A real catalog table, by name.
    Name(String),
    /// A parenthesized `select-stmt`, materialized at codegen time.
    Subquery(Box<Select>),
}

/// One `FROM`-clause table entry, along with its optional alias.
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    /// The table itself: a catalog name or a materialized subquery.
    pub kind: TableRefKind,
    /// The `AS alias`, if given.
    pub alias: Option<String>,
    /// The source span covering this entry.
    pub span: Span,
}

impl TableRef {
    /// The catalog name to resolve this table against, or `None` for a
    /// subquery (which has no catalog entry — codegen materializes it
    /// instead).
    pub fn name(&self) -> Option<&str> {
        match &self.kind {
            TableRefKind::Name(name) => Some(name),
            TableRefKind::Subquery(_) => None,
        }
    }
}

/// A `FROM` clause (#237): the first table plus zero or more joins,
/// evaluated left-to-right — `a JOIN b ON .. JOIN c ON ..` joins `b`
/// against `a`, then `c` against that result. Bare `Option<TableRef>`
/// (V2 scope) was replaced by this once a second table entered scope;
/// the single-table case is simply `joins: vec![]`.
#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    /// The leftmost table.
    pub first: TableRef,
    /// Zero or more joins applied left-to-right against `first`.
    pub joins: Vec<Join>,
}

/// One `[NATURAL] <join_op> <table> [ON <expr> | USING (col, ...)]` step
/// of a [`FromClause`].
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    /// The join kind (`INNER`/`LEFT`/`CROSS`/`RIGHT`/`FULL`).
    pub op: JoinOp,
    /// The table being joined in.
    pub table: TableRef,
    /// `None` for [`JoinOp::Cross`] with no `ON`/`USING`, for
    /// `natural: true` joins (the matching columns are resolved from
    /// same-named columns in both tables — semantic resolution deferred
    /// to codegen), and for a bare `JOIN`/`INNER JOIN` with no `ON` —
    /// rejected by the parser, since this V4 slice requires an explicit
    /// condition for non-natural INNER/LEFT/RIGHT/FULL.
    pub constraint: Option<JoinConstraint>,
    /// `true` for `NATURAL [INNER|LEFT|RIGHT|FULL] JOIN` (#250).
    /// `NATURAL CROSS JOIN` is rejected by the parser (not legal SQLite
    /// grammar); comma-style joins (`FROM a, b`) are synthesized as
    /// `natural: false` `JoinOp::Cross` joins, per #250's design.
    pub natural: bool,
}

/// `INNER`/plain `JOIN`, `LEFT [OUTER] JOIN`, `CROSS JOIN` (#237), and
/// `RIGHT [OUTER] JOIN`/`FULL [OUTER] JOIN` (#250).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOp {
    /// `INNER JOIN`/plain `JOIN`.
    Inner,
    /// `LEFT [OUTER] JOIN`.
    Left,
    /// `CROSS JOIN`.
    Cross,
    /// `RIGHT [OUTER] JOIN`.
    Right,
    /// `FULL [OUTER] JOIN`.
    Full,
}

/// The join's matching condition: `ON <expr>` (#237) or
/// `USING (col, ...)` (#250, at least one column).
#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    /// `ON <expr>`.
    On(Expr),
    /// `USING (col, ...)` — at least one column.
    Using(Vec<String>),
}

/// One `ORDER BY` term.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderingTerm {
    /// The expression to order by.
    pub expr: Expr,
    /// `None` = no ASC/DESC given, `Some(true)` = DESC.
    pub desc: Option<bool>,
    /// `None` = no NULLS FIRST/LAST given, `Some(true)` = NULLS LAST.
    pub nulls_last: Option<bool>,
}

/// `LIMIT limit [OFFSET offset]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Limit {
    /// The `LIMIT` count expression.
    pub limit: Expr,
    /// The `OFFSET` expression, if given.
    pub offset: Option<Expr>,
}

/// A single expression node, carrying its source [`Span`].
#[derive(Debug, Clone, PartialEq)]
pub struct Expr {
    /// The expression's kind and payload.
    pub kind: ExprKind,
    /// The source span covering this expression.
    pub span: Span,
}

/// The kind of expression an [`Expr`] holds.
#[derive(Debug, Clone, PartialEq)]
pub enum ExprKind {
    /// A literal value.
    Literal(Literal),
    /// A bind parameter (`?`, `?NNN`, `:name`, `@name`, `$name`).
    Param(ParamKind),
    /// A column reference, optionally table- and/or catalog-qualified.
    Column {
        /// Optional `table.` qualifier.
        table: Option<String>,
        /// Optional `catalog.` qualifier.
        catalog: Option<String>,
        /// The column name.
        name: String,
    },
    /// A function call, e.g. `f(a, b)`, `f(DISTINCT a)`, `f(*)`.
    FunctionCall {
        /// The function name.
        name: String,
        /// `true` for `f(DISTINCT ...)`.
        distinct: bool,
        /// The argument list.
        args: FunctionArgs,
    },
    /// A unary operator applied to an expression, e.g. `-x`, `NOT x`.
    Unary {
        /// The unary operator.
        op: UnaryOp,
        /// The operand.
        expr: Box<Expr>,
    },
    /// A binary operator applied to two expressions, e.g. `a + b`.
    Binary {
        /// The binary operator.
        op: BinaryOp,
        /// The left-hand operand.
        lhs: Box<Expr>,
        /// The right-hand operand.
        rhs: Box<Expr>,
    },
    /// `lhs IS [NOT] rhs`.
    Is {
        /// The left-hand operand.
        lhs: Box<Expr>,
        /// The right-hand operand.
        rhs: Box<Expr>,
        /// `true` for `IS NOT`.
        negated: bool,
    },
    /// `expr IS [NOT] NULL`.
    IsNull {
        /// The tested expression.
        expr: Box<Expr>,
        /// `true` for `IS NOT NULL`.
        negated: bool,
    },
    /// `expr [NOT] BETWEEN lo AND hi`.
    Between {
        /// The tested expression.
        expr: Box<Expr>,
        /// The lower bound.
        lo: Box<Expr>,
        /// The upper bound.
        hi: Box<Expr>,
        /// `true` for `NOT BETWEEN`.
        negated: bool,
    },
    /// `expr [NOT] IN (list, ...)` — the literal-list form.
    In {
        /// The tested expression.
        expr: Box<Expr>,
        /// The literal list to test membership against.
        list: Vec<Expr>,
        /// `true` for `NOT IN`.
        negated: bool,
    },
    /// `expr [NOT] LIKE/GLOB pattern [ESCAPE escape]`.
    Like {
        /// The tested expression.
        expr: Box<Expr>,
        /// The pattern expression.
        pattern: Box<Expr>,
        /// `true` for `GLOB`, `false` for `LIKE`.
        glob: bool,
        /// `true` for `NOT LIKE`/`NOT GLOB`.
        negated: bool,
        /// The `ESCAPE` expression, if given.
        escape: Option<Box<Expr>>,
    },
    /// `CASE [operand] WHEN cond THEN result ... [ELSE else_] END`.
    Case {
        /// The `CASE operand` form's subject, absent for `CASE WHEN cond`.
        operand: Option<Box<Expr>>,
        /// `(condition, result)` pairs, in source order.
        whens: Vec<(Expr, Expr)>,
        /// The `ELSE` result, if given.
        else_: Option<Box<Expr>>,
    },
    /// `CAST(expr AS type_name)`.
    Cast {
        /// The expression being cast.
        expr: Box<Expr>,
        /// The target type name.
        type_name: String,
    },
    /// `expr COLLATE collation`.
    Collate {
        /// The expression being collated.
        expr: Box<Expr>,
        /// The collation sequence name.
        collation: String,
    },
    /// A parenthesized expression, preserved explicitly (Requirement 3's
    /// "preserve parentheses for precedence" scenario).
    Paren(Box<Expr>),
    /// A scalar subquery `(SELECT ...)` (#238) — usable anywhere an
    /// expression is, including correlated (a reference to an enclosing
    /// query's column).
    Subquery(Box<Select>),
    /// `EXISTS (SELECT ...)` / `NOT EXISTS (SELECT ...)` (#238).
    Exists {
        /// The subquery being tested for row existence.
        subquery: Box<Select>,
        /// `true` for `NOT EXISTS`.
        negated: bool,
    },
    /// `expr IN (SELECT ...)` / `expr NOT IN (SELECT ...)` (#238) — kept
    /// separate from [`ExprKind::In`]'s literal-list form rather than a
    /// union, so callers pattern-matching on `In` don't need to handle a
    /// subquery case.
    InSubquery {
        /// The tested expression.
        expr: Box<Expr>,
        /// The subquery to test membership against.
        subquery: Box<Select>,
        /// `true` for `NOT IN`.
        negated: bool,
    },
    /// `(a, b) IN (SELECT x, y FROM ...)` / `... NOT IN (...)` (#251) —
    /// the multi-column form of [`ExprKind::InSubquery`]. `exprs` is the
    /// LHS tuple (arity >= 2); the subquery's own result-column count
    /// must match it, checked at codegen time once the subquery's
    /// projection is known.
    InSubqueryMulti {
        /// The LHS tuple of tested expressions (arity >= 2).
        exprs: Vec<Expr>,
        /// The subquery to test membership against.
        subquery: Box<Select>,
        /// `true` for `NOT IN`.
        negated: bool,
    },
}

/// A function call's argument list.
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArgs {
    /// `f(*)`.
    Star,
    /// A plain (possibly empty) expression list.
    List(Vec<Expr>),
}

/// A literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// An integer literal.
    Integer(i64),
    /// A floating-point literal.
    Float(f64),
    /// A string literal.
    Str(String),
    /// A blob literal (`x'...'`).
    Blob(Vec<u8>),
    /// `NULL`.
    Null,
    /// `TRUE`.
    True,
    /// `FALSE`.
    False,
}

/// A bind parameter's form.
#[derive(Debug, Clone, PartialEq)]
pub enum ParamKind {
    /// Bare `?`.
    Anonymous,
    /// `?NNN`.
    Numbered(u32),
    /// `:name`.
    Colon(String),
    /// `@name`.
    At(String),
    /// `$name`.
    Dollar(String),
}

/// A unary prefix operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// `NOT x`.
    Not,
    /// `+x`.
    Plus,
    /// `-x`.
    Minus,
    /// `~x`.
    BitNot,
}

/// An `INSERT INTO table [(cols)] source` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    /// `OR REPLACE`/`OR IGNORE`/etc. conflict resolution, if given.
    pub or_action: Option<ConflictAction>,
    /// Name of the table being inserted into.
    pub table: String,
    /// Optional explicit target column list.
    pub columns: Option<Vec<String>>,
    /// The row source: `VALUES`, a `SELECT`, or `DEFAULT VALUES`.
    pub source: InsertSource,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// `INSERT OR <action>` / `ON CONFLICT` resolution strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictAction {
    /// `OR REPLACE`.
    Replace,
    /// `OR IGNORE`.
    Ignore,
    /// `OR ABORT`.
    Abort,
    /// `OR ROLLBACK`.
    Rollback,
    /// `OR FAIL`.
    Fail,
}

/// The row source of an [`Insert`].
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    /// `VALUES (row), (row), ...`.
    Values(Vec<Vec<Expr>>),
    /// `INSERT ... SELECT ...`.
    Select(Box<Select>),
    /// `DEFAULT VALUES`.
    DefaultValues,
}

/// A `DELETE FROM table [WHERE expr]` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Delete {
    /// Name of the table being deleted from.
    pub table: String,
    /// The `WHERE` clause, if given.
    pub where_clause: Option<Expr>,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// A `CREATE TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    /// `IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// The new table's name.
    pub name: String,
    /// The column definitions, in source order.
    pub columns: Vec<ColumnDef>,
    /// Table-level constraints (`PRIMARY KEY`/`UNIQUE`/`CHECK`).
    pub constraints: Vec<TableConstraint>,
    /// `WITHOUT ROWID`.
    pub without_rowid: bool,
    /// `STRICT`.
    pub strict: bool,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// A single column definition within a [`CreateTable`].
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// The column's name.
    pub name: String,
    /// The declared type name, if given.
    pub type_name: Option<String>,
    /// Column-level constraints (`NOT NULL`, `PRIMARY KEY`, etc.).
    pub constraints: Vec<ColumnConstraint>,
}

/// A column-level constraint on a [`ColumnDef`].
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    /// `NOT NULL`.
    NotNull,
    /// `PRIMARY KEY [ASC|DESC] [AUTOINCREMENT]`.
    PrimaryKey {
        /// `None` = no ASC/DESC given, `Some(true)` = DESC.
        desc: Option<bool>,
        /// `AUTOINCREMENT`.
        autoincrement: bool,
    },
    /// `UNIQUE`.
    Unique,
    /// `DEFAULT value`.
    Default(DefaultValue),
    /// `CHECK (expr)`.
    Check(Expr),
    /// `COLLATE collation`.
    Collate(String),
}

/// `DEFAULT` accepts either a bare literal or a parenthesized expression
/// (never a bare non-literal expression) — kept as separate variants so
/// the printer knows which form reparses correctly.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultValue {
    /// A bare literal, e.g. `DEFAULT 0`.
    Literal(Expr),
    /// A parenthesized expression, e.g. `DEFAULT (1 + 1)`.
    Paren(Expr),
}

/// A table-level constraint on a [`CreateTable`].
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    /// `PRIMARY KEY (col, ...)`.
    PrimaryKey(Vec<IndexedColumn>),
    /// `UNIQUE (col, ...)`.
    Unique(Vec<IndexedColumn>),
    /// `CHECK (expr)`.
    Check(Expr),
}

/// An indexed-column: an expression (bare column ref, `COLLATE`-qualified,
/// or a general expression for functional indexes), plus optional
/// ASC/DESC. Shared by `CREATE INDEX` and `PRIMARY KEY`/`UNIQUE` table
/// constraints — unlike [`OrderingTerm`], NULLS FIRST/LAST doesn't apply.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedColumn {
    /// The indexed expression (bare column ref, `COLLATE`-qualified, or a
    /// general expression for functional indexes).
    pub expr: Expr,
    /// `None` = no ASC/DESC given, `Some(true)` = DESC.
    pub desc: Option<bool>,
}

/// A `CREATE INDEX` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndex {
    /// `UNIQUE`.
    pub unique: bool,
    /// `IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// The new index's name.
    pub name: String,
    /// The table being indexed.
    pub table: String,
    /// The indexed columns/expressions.
    pub columns: Vec<IndexedColumn>,
    /// The partial-index `WHERE` clause, if given.
    pub where_clause: Option<Expr>,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// A `DROP TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropTable {
    /// `IF EXISTS`.
    pub if_exists: bool,
    /// The table being dropped.
    pub name: String,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// `CREATE VIEW view_name ['(' column_list ')'] AS select_stmt` (#379,
/// grammar V6 block). `query` is boxed for the same reason
/// [`CommonTableExpr::query`] is: it recurses through the full `Select`
/// AST, so an unboxed field would make [`super::ast`]'s types
/// infinitely-sized.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateView {
    /// `IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// The new view's name.
    pub name: String,
    /// Optional explicit column-name list.
    pub columns: Option<Vec<String>>,
    /// The view's defining `select-stmt`.
    pub query: Box<Select>,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// `DROP VIEW [IF EXISTS] view_name` (#379).
#[derive(Debug, Clone, PartialEq)]
pub struct DropView {
    /// `IF EXISTS`.
    pub if_exists: bool,
    /// The view being dropped.
    pub name: String,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// A `DROP INDEX` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndex {
    /// `IF EXISTS`.
    pub if_exists: bool,
    /// The index being dropped.
    pub name: String,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// `BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE]` locking mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// `DEFERRED`.
    Deferred,
    /// `IMMEDIATE`.
    Immediate,
    /// `EXCLUSIVE`.
    Exclusive,
}

/// A `BEGIN [mode] [TRANSACTION]` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Begin {
    /// The locking mode, if given.
    pub mode: Option<TransactionMode>,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// A `COMMIT [TRANSACTION]` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Commit {
    /// The source span covering the whole statement.
    pub span: Span,
}

/// A `ROLLBACK [TRANSACTION]` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Rollback {
    /// The source span covering the whole statement.
    pub span: Span,
}

/// The only two `journal_mode` values `pragma-stmt` (grammar V6
/// carve-out, #388) accepts — stock SQLite's `journal_mode` pragma also
/// takes MEMORY/OFF/TRUNCATE/PERSIST, all deferred to V7's general
/// PRAGMA support alongside every other pragma name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PragmaJournalMode {
    /// `PRAGMA journal_mode = WAL`.
    Wal,
    /// `PRAGMA journal_mode = DELETE`.
    Delete,
}

/// The three `synchronous` levels `pragma-stmt` (grammar V7 carve-out,
/// #645) accepts — stock SQLite's `synchronous` pragma also takes
/// `EXTRA` and the `ON`/boolean aliases, plus arbitrary integers beyond
/// 0-2 (with its own legacy masking quirks), all deferred to general
/// PRAGMA support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PragmaSynchronous {
    /// `PRAGMA synchronous = OFF` (or `= 0`).
    Off,
    /// `PRAGMA synchronous = NORMAL` (or `= 1`).
    Normal,
    /// `PRAGMA synchronous = FULL` (or `= 2`).
    Full,
}

/// A parsed `PRAGMA` statement: the narrow V6 `journal_mode` carve-out
/// (#388), the V7 `integrity_check`/`quick_check` carve-out (#540,
/// #541), and the V7 `synchronous` carve-out (#645). Every other pragma
/// name stays `Unsupported` at the parser (see `parse_pragma_stmt`).
#[derive(Debug, Clone, PartialEq)]
pub enum Pragma {
    /// `PRAGMA journal_mode = WAL|DELETE` (#388); see [`PragmaJournalMode`].
    JournalMode {
        /// The requested `journal_mode` value.
        journal_mode: PragmaJournalMode,
        /// The source span covering the whole statement.
        span: Span,
    },
    /// `PRAGMA integrity_check` / `PRAGMA quick_check` (#540, #541).
    /// `quick_check` (`quick: true`) skips the exhaustive index-vs-table
    /// cross-check pass that `integrity_check` performs.
    IntegrityCheck {
        /// `true` for `quick_check`, `false` for `integrity_check`.
        quick: bool,
        /// The source span covering the whole statement.
        span: Span,
    },
    /// `PRAGMA synchronous [= OFF|NORMAL|FULL|0|1|2]` (#645). `level =
    /// None` is the bare query form (`PRAGMA synchronous`, no `=`),
    /// which reports the connection's current level as a result row
    /// instead of changing it — unlike `journal_mode`, whose bare query
    /// form the parser still rejects as `Unsupported`.
    Synchronous {
        /// The requested level, or `None` to query the current one.
        level: Option<PragmaSynchronous>,
        /// The source span covering the whole statement.
        span: Span,
    },
}

impl Pragma {
    /// The source span covering the whole statement, whichever variant.
    pub fn span(&self) -> Span {
        match self {
            Pragma::JournalMode { span, .. }
            | Pragma::IntegrityCheck { span, .. }
            | Pragma::Synchronous { span, .. } => *span,
        }
    }
}

/// `ANALYZE` / `ANALYZE table-name` (#461, grammar V7 carve-out): `target
/// = None` analyzes every user table; `Some(name)` scopes to one table.
/// A qualified `schema-name.table-name` form, or an index name, is out
/// of this MVP's scope and rejected by the parser as `Unsupported`
/// before this struct is ever built.
#[derive(Debug, Clone, PartialEq)]
pub struct Analyze {
    /// The table to analyze, or `None` to analyze every user table.
    pub target: Option<String>,
    /// The source span covering the whole statement.
    pub span: Span,
}

/// A binary infix operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    /// `OR`.
    Or,
    /// `AND`.
    And,
    /// `=`/`==`.
    Eq,
    /// `!=`/`<>`.
    Ne,
    /// `<`.
    Lt,
    /// `<=`.
    Le,
    /// `>`.
    Gt,
    /// `>=`.
    Ge,
    /// `&`.
    BitAnd,
    /// `|`.
    BitOr,
    /// `<<`.
    Shl,
    /// `>>`.
    Shr,
    /// `+`.
    Add,
    /// `-`.
    Sub,
    /// `*`.
    Mul,
    /// `/`.
    Div,
    /// `%`.
    Mod,
    /// `||`.
    Concat,
}
