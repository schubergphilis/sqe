// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Recursive-descent parser for the SELECT-core V2 slice (spec
//! 002-parser Requirements 2-4; grammar `.openspec/grammar/sqlite.ebnf`
//! V2 block). Hand-written rather than pomelo/lemon-generated: spike
//! 006 (#57) picked pomelo as the long-term generator, but this ticket
//! ships the V2 slice directly against the tokenizer to keep the
//! surface small; a generator swap is future work and does not change
//! the AST or diagnostics contract.
//!
//! Operator precedence (lowest to highest) mirrors `parse.y`'s
//! `%left`/`%right` declarations exactly — see the grammar file's
//! "Expressions (V2 slice)" section for the full table. Descending-
//! precedence call order: `expr` (guarded) -> `bool_expr` (OR/AND,
//! precedence-climbing) -> `not_expr` (guarded) -> `equality_expr` ->
//! `binary_expr` (relational/bitwise/additive/multiplicative/concat,
//! precedence-climbing) -> `arrow_expr` -> `collate_expr` -> `unary_expr`
//! (guarded) -> `primary_expr`.
//!
//! `bool_expr` and `binary_expr` each collapse several historically
//! separate pass-through levels (OR+AND; relational/bitwise/additive/
//! multiplicative/concat) into one precedence-climbing function apiece —
//! one stack frame per nesting level instead of one per former level. That
//! collapse, plus `#118`'s narrower per-level cost, is what lets
//! `MAX_EXPR_DEPTH` actually be reached (rather than stack-overflowing
//! first) within a debug build's default thread stack.

use super::ast::*;
use super::error::{PResult, ParseFail};
use super::tokenizer::{Keyword, Param, Span, Token, TokenKind};

/// Recursive-descent parser state: the token stream, a cursor into it, and
/// the current expression-nesting depth (see [`MAX_EXPR_DEPTH`]).
pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    depth: usize,
}

/// Recursion-depth cap for `expr`/`not_expr`/`unary_expr`, so pathological
/// input (many nested `(`, or repeated `NOT`/unary operators) returns a
/// clean `ParseFail::Invalid` instead of overflowing the stack.
const MAX_EXPR_DEPTH: usize = 200;

fn join_span(a: Span, b: Span) -> Span {
    Span {
        line: a.line,
        column: a.column,
        offset: a.offset,
        len: b.offset.saturating_add(b.len).saturating_sub(a.offset),
    }
}

impl Parser {
    /// Creates a parser positioned at the start of `tokens`, with an
    /// empty expression-nesting depth.
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser {
            tokens,
            pos: 0,
            depth: 0,
        }
    }

    /// Guards a recursive-descent entry point: increments the depth
    /// counter, fails with `Invalid` past `MAX_EXPR_DEPTH` instead of
    /// recursing further, and always decrements again afterward
    /// (including on error) so sibling subtrees aren't penalized.
    fn with_depth_guard<T>(&mut self, f: impl FnOnce(&mut Self) -> PResult<T>) -> PResult<T> {
        self.depth = self.depth.saturating_add(1);
        if self.depth > MAX_EXPR_DEPTH {
            self.depth = self.depth.saturating_sub(1);
            return self.invalid("expression nesting too deep");
        }
        let result = f(self);
        self.depth = self.depth.saturating_sub(1);
        result
    }

    // ---- token stream helpers ----------------------------------------

    fn peek(&self) -> &Token {
        self.peek_at(0)
    }

    fn peek_at(&self, offset: usize) -> &Token {
        let idx = self.pos.saturating_add(offset);
        self.tokens.get(idx).unwrap_or_else(|| {
            // The tokenizer always terminates its stream with `Eof`, so
            // any in-range index resolves; out-of-range only happens by
            // peeking past `Eof`, which we handle by returning the last
            // (`Eof`) token itself.
            self.tokens.last().unwrap_or(&Token {
                kind: TokenKind::Eof,
                span: Span {
                    line: 1,
                    column: 1,
                    offset: 0,
                    len: 0,
                },
            })
        })
    }

    fn advance(&mut self) -> Token {
        let tok = self.peek().clone();
        if !matches!(tok.kind, TokenKind::Eof) {
            self.pos = self.pos.saturating_add(1);
        }
        tok
    }

    /// Consumes the current token and returns only its [`Span`], without
    /// cloning its `kind` — for the many call sites that only need the
    /// span (e.g. to anchor an AST node), this skips a heap-allocating
    /// clone of the token's payload (`String`/`Identifier`/boxed
    /// `Blob`/`Param`) that `advance().span` would otherwise pay for and
    /// immediately discard.
    fn advance_span(&mut self) -> Span {
        let span = self.peek().span;
        if !matches!(self.peek().kind, TokenKind::Eof) {
            self.pos = self.pos.saturating_add(1);
        }
        span
    }

    fn at_kw(&self, kw: Keyword) -> bool {
        matches!(&self.peek().kind, TokenKind::Keyword(k) if *k == kw)
    }

    fn eat_kw(&mut self, kw: Keyword) -> bool {
        if self.at_kw(kw) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: Keyword) -> PResult<Span> {
        if self.at_kw(kw) {
            Ok(self.advance_span())
        } else {
            let tok = self.peek().clone();
            Err(ParseFail::Invalid {
                message: format!("expected {kw:?}, found {:?}", tok.kind),
                span: tok.span,
            })
        }
    }

    fn eat_punct(&mut self, kind: &TokenKind) -> bool {
        if &self.peek().kind == kind {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_punct(&mut self, kind: TokenKind, what: &str) -> PResult<Span> {
        if self.peek().kind == kind {
            Ok(self.advance_span())
        } else {
            let tok = self.peek().clone();
            Err(ParseFail::Invalid {
                message: format!("expected {what}, found {:?}", tok.kind),
                span: tok.span,
            })
        }
    }

    fn invalid<T>(&self, message: impl Into<String>) -> PResult<T> {
        Err(ParseFail::Invalid {
            message: message.into(),
            span: self.peek().span,
        })
    }

    fn unsupported<T>(&self, message: impl Into<String>) -> PResult<T> {
        Err(ParseFail::Unsupported {
            message: message.into(),
            span: self.peek().span,
        })
    }

    /// After a full statement is parsed, only a trailing `;` (optionally
    /// repeated) and EOF are allowed.
    pub(super) fn expect_end(&mut self) -> PResult<()> {
        while self.eat_punct(&TokenKind::Semicolon) {}
        match &self.peek().kind {
            TokenKind::Eof => Ok(()),
            TokenKind::Keyword(Keyword::UNION)
            | TokenKind::Keyword(Keyword::INTERSECT)
            | TokenKind::Keyword(Keyword::EXCEPT) => {
                self.unsupported("compound SELECT (UNION/INTERSECT/EXCEPT) not yet supported")
            }
            other => {
                let tok = other.clone();
                Err(ParseFail::Invalid {
                    message: format!("unexpected trailing token {tok:?}"),
                    span: self.peek().span,
                })
            }
        }
    }

    fn identifier(&mut self) -> PResult<(String, Span)> {
        match self.peek().kind.clone() {
            TokenKind::Identifier(name) => {
                let span = self.advance_span();
                Ok((name, span))
            }
            _ => {
                let tok = self.peek().clone();
                Err(ParseFail::Invalid {
                    message: format!("expected identifier, found {:?}", tok.kind),
                    span: tok.span,
                })
            }
        }
    }

    // ---- statement -----------------------------------------------------

    pub(super) fn parse_insert_stmt(&mut self) -> PResult<Insert> {
        let start = self.expect_kw(Keyword::INSERT)?;
        let or_action = if self.eat_kw(Keyword::OR) {
            Some(self.conflict_action()?)
        } else {
            None
        };
        self.expect_kw(Keyword::INTO)?;
        let (table, _) = self.identifier()?;

        let columns = if self.eat_punct(&TokenKind::LParen) {
            let mut cols = vec![self.identifier()?.0];
            while self.eat_punct(&TokenKind::Comma) {
                cols.push(self.identifier()?.0);
            }
            self.expect_punct(TokenKind::RParen, "')'")?;
            Some(cols)
        } else {
            None
        };

        let (source, end) = if self.eat_kw(Keyword::DEFAULT) {
            let end = self.expect_kw(Keyword::VALUES)?;
            (InsertSource::DefaultValues, end)
        } else if self.eat_kw(Keyword::VALUES) {
            let first_row = self.value_row()?;
            let mut end = first_row.last().map_or(start, |e| e.span);
            let mut rows = vec![first_row];
            while self.eat_punct(&TokenKind::Comma) {
                let row = self.value_row()?;
                if let Some(last) = row.last() {
                    end = last.span;
                }
                rows.push(row);
            }
            (InsertSource::Values(rows), end)
        } else if self.at_kw(Keyword::SELECT) || self.at_kw(Keyword::WITH) {
            let select = self.parse_select_stmt()?;
            let end = select.span;
            (InsertSource::Select(Box::new(select)), end)
        } else {
            return self.invalid("expected VALUES, DEFAULT VALUES, or SELECT after INSERT INTO");
        };

        Ok(Insert {
            or_action,
            table,
            columns,
            source,
            span: join_span(start, end),
        })
    }

    pub(super) fn parse_delete_stmt(&mut self) -> PResult<Delete> {
        let start = self.expect_kw(Keyword::DELETE)?;
        self.expect_kw(Keyword::FROM)?;
        let (table, table_span) = self.identifier()?;

        let mut end = table_span;
        let where_clause = if self.eat_kw(Keyword::WHERE) {
            let expr = self.expr()?;
            end = expr.span;
            Some(expr)
        } else {
            None
        };

        Ok(Delete {
            table,
            where_clause,
            span: join_span(start, end),
        })
    }

    fn conflict_action(&mut self) -> PResult<ConflictAction> {
        if self.eat_kw(Keyword::REPLACE) {
            Ok(ConflictAction::Replace)
        } else if self.eat_kw(Keyword::IGNORE) {
            Ok(ConflictAction::Ignore)
        } else if self.eat_kw(Keyword::ABORT) {
            Ok(ConflictAction::Abort)
        } else if self.eat_kw(Keyword::ROLLBACK) {
            Ok(ConflictAction::Rollback)
        } else if self.eat_kw(Keyword::FAIL) {
            Ok(ConflictAction::Fail)
        } else {
            self.invalid("expected REPLACE, IGNORE, ABORT, ROLLBACK, or FAIL after OR")
        }
    }

    fn value_row(&mut self) -> PResult<Vec<Expr>> {
        self.expect_punct(TokenKind::LParen, "'('")?;
        let list = self.expr_list()?;
        self.expect_punct(TokenKind::RParen, "')'")?;
        Ok(list)
    }

    /// `update-stmt` (grammar V3 block): `UPDATE [OR conflict-action]
    /// table-name SET assignment { "," assignment } [ WHERE expr ]`, where
    /// `assignment` is either `column-name "=" expr` or the tuple form
    /// `"(" column-name { "," column-name } ")" "=" "(" expr-list ")"`.
    pub(super) fn parse_update_stmt(&mut self) -> PResult<Update> {
        let start = self.expect_kw(Keyword::UPDATE)?;

        let or_action = if self.eat_kw(Keyword::OR) {
            Some(self.conflict_action()?)
        } else {
            None
        };

        let (table, _) = self.identifier()?;

        self.expect_kw(Keyword::SET)?;

        let mut assignments = self.assignment()?;
        while self.eat_punct(&TokenKind::Comma) {
            assignments.extend(self.assignment()?);
        }

        let where_clause = if self.eat_kw(Keyword::WHERE) {
            Some(self.expr()?)
        } else {
            None
        };

        let end = self
            .tokens
            .get(self.pos.saturating_sub(1))
            .map_or(start, |t| t.span);
        Ok(Update {
            or_action,
            table,
            assignments,
            where_clause,
            span: join_span(start, end),
        })
    }

    /// One assignment "slot": `column-name "=" expr` (yields one
    /// [`Assignment`]), or the tuple form
    /// `"(" column-name { "," column-name } ")" "=" "(" expr-list ")"`,
    /// which requires a matching-arity parenthesized RHS expr-list (a
    /// scalar-subquery RHS is not yet supported) and expands into one
    /// [`Assignment`] per column, each paired with its RHS expr.
    fn assignment(&mut self) -> PResult<Vec<Assignment>> {
        if matches!(self.peek().kind, TokenKind::LParen) {
            self.advance();
            let mut columns = vec![self.identifier()?.0];
            while self.eat_punct(&TokenKind::Comma) {
                columns.push(self.identifier()?.0);
            }
            self.expect_punct(TokenKind::RParen, "')' to close column list")?;
            self.expect_punct(TokenKind::Eq, "'=' in tuple assignment")?;
            if !matches!(self.peek().kind, TokenKind::LParen) {
                return self.unsupported("tuple assignment RHS must be a parenthesized expr-list");
            }
            self.advance();
            if self.at_kw(Keyword::SELECT) {
                return self.unsupported("tuple assignment RHS subquery not yet supported");
            }
            let values = self.expr_list()?;
            self.expect_punct(TokenKind::RParen, "')' to close tuple assignment")?;
            if values.len() != columns.len() {
                return self.invalid("tuple assignment column/value count mismatch");
            }
            return Ok(columns
                .into_iter()
                .zip(values)
                .map(|(name, value)| Assignment {
                    columns: vec![name],
                    value,
                })
                .collect());
        }

        let (name, _) = self.identifier()?;
        self.expect_punct(TokenKind::Eq, "'=' in assignment")?;
        let value = self.expr()?;
        Ok(vec![Assignment {
            columns: vec![name],
            value,
        }])
    }

    // ---- DDL: CREATE/DROP TABLE, CREATE/DROP INDEX -----------------------

    fn opt_if_not_exists(&mut self) -> PResult<bool> {
        if self.eat_kw(Keyword::IF) {
            self.expect_kw(Keyword::NOT)?;
            self.expect_kw(Keyword::EXISTS)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn opt_if_exists(&mut self) -> PResult<bool> {
        if self.eat_kw(Keyword::IF) {
            self.expect_kw(Keyword::EXISTS)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// SQLite treats `ROWID`/`STRICT` as contextual keywords (unreserved
    /// words, not `Keyword` tokens) — matched case-insensitively against a
    /// bare identifier.
    fn eat_contextual_kw(&mut self, word: &str) -> bool {
        if matches!(&self.peek().kind, TokenKind::Identifier(id) if id.eq_ignore_ascii_case(word)) {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Bails with `Unsupported` (schema-qualified names not yet supported)
    /// if a `.` follows, mirroring `table_ref`'s existing behavior.
    fn check_no_schema_qualifier(&mut self) -> PResult<()> {
        if matches!(self.peek().kind, TokenKind::Dot) {
            return self.unsupported("schema-qualified names not yet supported");
        }
        Ok(())
    }

    /// Bails with `Unsupported` if an `ON CONFLICT` resolution clause
    /// follows — real SQLite allows one after NOT NULL/PRIMARY KEY/UNIQUE,
    /// but representing it isn't in this ticket's scope.
    fn check_no_conflict_clause(&mut self) -> PResult<()> {
        if self.at_kw(Keyword::ON)
            && matches!(self.peek_at(1).kind, TokenKind::Keyword(Keyword::CONFLICT))
        {
            return self.unsupported("ON CONFLICT resolution clause not yet supported");
        }
        Ok(())
    }

    pub(super) fn parse_create_table_stmt(&mut self) -> PResult<CreateTable> {
        let start = self.expect_kw(Keyword::CREATE)?;
        if self.at_kw(Keyword::TEMP) || self.at_kw(Keyword::TEMPORARY) {
            return self.unsupported("CREATE TEMP/TEMPORARY TABLE not yet supported");
        }
        if self.at_kw(Keyword::VIRTUAL) {
            return self.unsupported("CREATE VIRTUAL TABLE not yet supported");
        }
        self.expect_kw(Keyword::TABLE)?;
        let if_not_exists = self.opt_if_not_exists()?;
        let (name, _) = self.identifier()?;
        self.check_no_schema_qualifier()?;
        if self.at_kw(Keyword::AS) {
            return self.unsupported("CREATE TABLE ... AS select-stmt not yet supported");
        }
        self.expect_punct(TokenKind::LParen, "'(' after table name")?;

        let mut columns = vec![self.column_def()?];
        let mut constraints = Vec::new();
        while self.eat_punct(&TokenKind::Comma) {
            if self.at_table_constraint_start() {
                constraints.push(self.table_constraint()?);
                while self.eat_punct(&TokenKind::Comma) {
                    constraints.push(self.table_constraint()?);
                }
                break;
            }
            columns.push(self.column_def()?);
        }
        let mut end = self.expect_punct(TokenKind::RParen, "')' to close column list")?;

        let mut without_rowid = false;
        let mut strict = false;
        if self.eat_kw(Keyword::WITHOUT) {
            if !self.eat_contextual_kw("ROWID") {
                return self.invalid("expected ROWID after WITHOUT");
            }
            end = self
                .tokens
                .get(self.pos.saturating_sub(1))
                .map_or(end, |t| t.span);
            without_rowid = true;
        } else if matches!(&self.peek().kind, TokenKind::Identifier(id) if id.eq_ignore_ascii_case("STRICT"))
        {
            end = self.advance_span();
            strict = true;
        }

        Ok(CreateTable {
            if_not_exists,
            name,
            columns,
            constraints,
            without_rowid,
            strict,
            span: join_span(start, end),
        })
    }

    fn at_table_constraint_start(&self) -> bool {
        self.at_kw(Keyword::CONSTRAINT)
            || self.at_kw(Keyword::PRIMARY)
            || self.at_kw(Keyword::UNIQUE)
            || self.at_kw(Keyword::CHECK)
            || self.at_kw(Keyword::FOREIGN)
    }

    fn column_def(&mut self) -> PResult<ColumnDef> {
        let (name, _) = self.identifier()?;
        let type_name = if matches!(self.peek().kind, TokenKind::Identifier(_)) {
            Some(self.type_name()?)
        } else {
            None
        };
        let mut constraints = Vec::new();
        while let Some(c) = self.opt_column_constraint()? {
            constraints.push(c);
        }
        Ok(ColumnDef {
            name,
            type_name,
            constraints,
        })
    }

    fn opt_column_constraint(&mut self) -> PResult<Option<ColumnConstraint>> {
        let named = self.eat_kw(Keyword::CONSTRAINT);
        if named {
            self.identifier()?;
        }
        if self.eat_kw(Keyword::NOT) {
            self.expect_punct(TokenKind::Null, "NULL")?;
            self.check_no_conflict_clause()?;
            return Ok(Some(ColumnConstraint::NotNull));
        }
        if matches!(self.peek().kind, TokenKind::Null) {
            return self.unsupported("bare NULL column constraint not yet supported");
        }
        if self.eat_kw(Keyword::PRIMARY) {
            self.expect_kw(Keyword::KEY)?;
            let desc = if self.eat_kw(Keyword::ASC) {
                Some(false)
            } else if self.eat_kw(Keyword::DESC) {
                Some(true)
            } else {
                None
            };
            self.check_no_conflict_clause()?;
            let autoincrement = self.eat_kw(Keyword::AUTOINCREMENT);
            return Ok(Some(ColumnConstraint::PrimaryKey {
                desc,
                autoincrement,
            }));
        }
        if self.eat_kw(Keyword::UNIQUE) {
            self.check_no_conflict_clause()?;
            return Ok(Some(ColumnConstraint::Unique));
        }
        if self.eat_kw(Keyword::CHECK) {
            self.expect_punct(TokenKind::LParen, "'(' after CHECK")?;
            let expr = self.expr()?;
            self.expect_punct(TokenKind::RParen, "')' to close CHECK")?;
            return Ok(Some(ColumnConstraint::Check(expr)));
        }
        if self.eat_kw(Keyword::DEFAULT) {
            return Ok(Some(ColumnConstraint::Default(self.default_value()?)));
        }
        if self.eat_kw(Keyword::COLLATE) {
            let (name, _) = self.identifier()?;
            return Ok(Some(ColumnConstraint::Collate(name)));
        }
        if self.at_kw(Keyword::REFERENCES) {
            return self
                .unsupported("REFERENCES (foreign key) column constraint not yet supported");
        }
        if self.at_kw(Keyword::GENERATED)
            || (self.at_kw(Keyword::AS) && matches!(self.peek_at(1).kind, TokenKind::LParen))
        {
            return self.unsupported("GENERATED ALWAYS AS not yet supported");
        }
        if named {
            return self.invalid("expected column constraint after CONSTRAINT name");
        }
        Ok(None)
    }

    fn default_value(&mut self) -> PResult<DefaultValue> {
        if self.eat_punct(&TokenKind::LParen) {
            let expr = self.expr()?;
            self.expect_punct(TokenKind::RParen, "')' to close DEFAULT expression")?;
            return Ok(DefaultValue::Paren(expr));
        }
        if matches!(self.peek().kind, TokenKind::Plus | TokenKind::Minus) {
            let op = if matches!(self.peek().kind, TokenKind::Minus) {
                UnaryOp::Minus
            } else {
                UnaryOp::Plus
            };
            let start = self.advance_span();
            let inner = self.literal_value()?;
            let span = join_span(start, inner.span);
            return Ok(DefaultValue::Literal(Expr {
                kind: ExprKind::Unary {
                    op,
                    expr: Box::new(inner),
                },
                span,
            }));
        }
        Ok(DefaultValue::Literal(self.literal_value()?))
    }

    /// `literal-value` only (no columns, params, or general expressions) —
    /// the bare (non-parenthesized) form `DEFAULT` accepts.
    fn literal_value(&mut self) -> PResult<Expr> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::Integer(v) => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::Integer(v)),
                    span: tok.span,
                })
            }
            TokenKind::Float(v) => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::Float(v)),
                    span: tok.span,
                })
            }
            TokenKind::String(s) => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::Str(s)),
                    span: tok.span,
                })
            }
            TokenKind::Blob(b) => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::Blob(*b)),
                    span: tok.span,
                })
            }
            TokenKind::Null => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::Null),
                    span: tok.span,
                })
            }
            TokenKind::True => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::True),
                    span: tok.span,
                })
            }
            TokenKind::False => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::False),
                    span: tok.span,
                })
            }
            TokenKind::Keyword(Keyword::CURRENT_TIME)
            | TokenKind::Keyword(Keyword::CURRENT_DATE)
            | TokenKind::Keyword(Keyword::CURRENT_TIMESTAMP) => {
                self.unsupported("CURRENT_TIME/CURRENT_DATE/CURRENT_TIMESTAMP not yet supported")
            }
            _ => self.invalid("expected literal value after DEFAULT"),
        }
    }

    fn table_constraint(&mut self) -> PResult<TableConstraint> {
        if self.eat_kw(Keyword::CONSTRAINT) {
            self.identifier()?;
        }
        if self.eat_kw(Keyword::PRIMARY) {
            self.expect_kw(Keyword::KEY)?;
            let cols = self.indexed_column_list()?;
            self.check_no_conflict_clause()?;
            return Ok(TableConstraint::PrimaryKey(cols));
        }
        if self.eat_kw(Keyword::UNIQUE) {
            let cols = self.indexed_column_list()?;
            self.check_no_conflict_clause()?;
            return Ok(TableConstraint::Unique(cols));
        }
        if self.eat_kw(Keyword::CHECK) {
            self.expect_punct(TokenKind::LParen, "'(' after CHECK")?;
            let expr = self.expr()?;
            self.expect_punct(TokenKind::RParen, "')' to close CHECK")?;
            return Ok(TableConstraint::Check(expr));
        }
        if self.at_kw(Keyword::FOREIGN) {
            return self.unsupported("FOREIGN KEY table constraint not yet supported");
        }
        self.invalid("expected PRIMARY KEY, UNIQUE, CHECK, or FOREIGN KEY table constraint")
    }

    fn indexed_column_list(&mut self) -> PResult<Vec<IndexedColumn>> {
        self.expect_punct(TokenKind::LParen, "'(' after PRIMARY KEY/UNIQUE")?;
        let mut cols = vec![self.indexed_column()?];
        while self.eat_punct(&TokenKind::Comma) {
            cols.push(self.indexed_column()?);
        }
        self.expect_punct(TokenKind::RParen, "')' to close column list")?;
        Ok(cols)
    }

    fn indexed_column(&mut self) -> PResult<IndexedColumn> {
        let expr = self.expr()?;
        let desc = if self.eat_kw(Keyword::ASC) {
            Some(false)
        } else if self.eat_kw(Keyword::DESC) {
            Some(true)
        } else {
            None
        };
        Ok(IndexedColumn { expr, desc })
    }

    pub(super) fn parse_create_index_stmt(&mut self) -> PResult<CreateIndex> {
        let start = self.expect_kw(Keyword::CREATE)?;
        let unique = self.eat_kw(Keyword::UNIQUE);
        self.expect_kw(Keyword::INDEX)?;
        let if_not_exists = self.opt_if_not_exists()?;
        let (name, _) = self.identifier()?;
        self.check_no_schema_qualifier()?;
        self.expect_kw(Keyword::ON)?;
        let (table, _) = self.identifier()?;
        self.check_no_schema_qualifier()?;
        let columns = self.indexed_column_list()?;
        let mut end = self
            .tokens
            .get(self.pos.saturating_sub(1))
            .map_or(start, |t| t.span);
        let where_clause = if self.eat_kw(Keyword::WHERE) {
            let expr = self.expr()?;
            end = expr.span;
            Some(expr)
        } else {
            None
        };
        Ok(CreateIndex {
            unique,
            if_not_exists,
            name,
            table,
            columns,
            where_clause,
            span: join_span(start, end),
        })
    }

    /// `create_view_stmt` (#379, grammar V6): `CREATE VIEW view_name
    /// ['(' column_list ')'] AS select_stmt`.
    pub(super) fn parse_create_view_stmt(&mut self) -> PResult<CreateView> {
        let start = self.expect_kw(Keyword::CREATE)?;
        if self.at_kw(Keyword::TEMP) || self.at_kw(Keyword::TEMPORARY) {
            return self.unsupported("CREATE TEMP/TEMPORARY VIEW not yet supported");
        }
        self.expect_kw(Keyword::VIEW)?;
        let if_not_exists = self.opt_if_not_exists()?;
        let (name, _) = self.identifier()?;
        self.check_no_schema_qualifier()?;

        let columns = if self.eat_punct(&TokenKind::LParen) {
            let mut cols = vec![self.identifier()?.0];
            while self.eat_punct(&TokenKind::Comma) {
                cols.push(self.identifier()?.0);
            }
            self.expect_punct(TokenKind::RParen, "')'")?;
            Some(cols)
        } else {
            None
        };

        self.expect_kw(Keyword::AS)?;
        let query = self.parse_select_stmt()?;
        let end = query.span;

        Ok(CreateView {
            if_not_exists,
            name,
            columns,
            query: Box::new(query),
            span: join_span(start, end),
        })
    }

    /// `drop_view_stmt` (#379, grammar V6): `DROP VIEW [IF EXISTS]
    /// view_name`.
    pub(super) fn parse_drop_view_stmt(&mut self) -> PResult<DropView> {
        let start = self.expect_kw(Keyword::DROP)?;
        self.expect_kw(Keyword::VIEW)?;
        let if_exists = self.opt_if_exists()?;
        let (name, end) = self.identifier()?;
        self.check_no_schema_qualifier()?;
        Ok(DropView {
            if_exists,
            name,
            span: join_span(start, end),
        })
    }

    pub(super) fn parse_drop_table_stmt(&mut self) -> PResult<DropTable> {
        let start = self.expect_kw(Keyword::DROP)?;
        self.expect_kw(Keyword::TABLE)?;
        let if_exists = self.opt_if_exists()?;
        let (name, end) = self.identifier()?;
        self.check_no_schema_qualifier()?;
        Ok(DropTable {
            if_exists,
            name,
            span: join_span(start, end),
        })
    }

    pub(super) fn parse_drop_index_stmt(&mut self) -> PResult<DropIndex> {
        let start = self.expect_kw(Keyword::DROP)?;
        self.expect_kw(Keyword::INDEX)?;
        let if_exists = self.opt_if_exists()?;
        let (name, end) = self.identifier()?;
        self.check_no_schema_qualifier()?;
        Ok(DropIndex {
            if_exists,
            name,
            span: join_span(start, end),
        })
    }

    /// `begin-stmt` (#356, grammar V5): `BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE]
    /// [TRANSACTION]`.
    pub(super) fn parse_begin_stmt(&mut self) -> PResult<Begin> {
        let start = self.expect_kw(Keyword::BEGIN)?;
        let mut end = start;
        let mode = if let Some(span) = self.opt_kw_span(Keyword::DEFERRED) {
            end = span;
            Some(TransactionMode::Deferred)
        } else if let Some(span) = self.opt_kw_span(Keyword::IMMEDIATE) {
            end = span;
            Some(TransactionMode::Immediate)
        } else if let Some(span) = self.opt_kw_span(Keyword::EXCLUSIVE) {
            end = span;
            Some(TransactionMode::Exclusive)
        } else {
            None
        };
        if let Some(span) = self.opt_kw_span(Keyword::TRANSACTION) {
            end = span;
        }
        Ok(Begin {
            mode,
            span: join_span(start, end),
        })
    }

    /// `commit-stmt` (#356, grammar V5): `(COMMIT|END) [TRANSACTION]`.
    pub(super) fn parse_commit_stmt(&mut self) -> PResult<Commit> {
        let mut span = if self.at_kw(Keyword::COMMIT) {
            self.expect_kw(Keyword::COMMIT)?
        } else {
            self.expect_kw(Keyword::END)?
        };
        if let Some(end) = self.opt_kw_span(Keyword::TRANSACTION) {
            span = join_span(span, end);
        }
        Ok(Commit { span })
    }

    /// `rollback-stmt` (#356, grammar V5): `ROLLBACK [TRANSACTION]`.
    ///
    /// `ROLLBACK ... TO SAVEPOINT ...` is out of scope here (tracked
    /// separately for the SAVEPOINT/RELEASE follow-up).
    pub(super) fn parse_rollback_stmt(&mut self) -> PResult<Rollback> {
        let mut span = self.expect_kw(Keyword::ROLLBACK)?;
        if let Some(end) = self.opt_kw_span(Keyword::TRANSACTION) {
            span = join_span(span, end);
        }
        Ok(Rollback { span })
    }

    /// Consumes `kw` if present, returning its span.
    fn opt_kw_span(&mut self, kw: Keyword) -> Option<Span> {
        if self.at_kw(kw) {
            Some(self.advance_span())
        } else {
            None
        }
    }

    /// `pragma-stmt` (#388 `journal_mode`, #540/#541 `integrity_check`/
    /// `quick_check` grammar V6/V7 carve-outs). `PRAGMA` pragma names are
    /// identifiers in real SQLite (not a fixed keyword list), so the name
    /// is read via `identifier()` like any other name; any other pragma
    /// name, or any other value for `journal_mode`, parses far enough to
    /// report a clean `Unsupported` rather than a hard parse error --
    /// mirrors `parse_with_clause`'s `WITH RECURSIVE` precedent. General
    /// PRAGMA support stays deferred to V7 (grammar file's Future
    /// blocks).
    pub(super) fn parse_pragma_stmt(&mut self) -> PResult<Pragma> {
        let start = self.expect_kw(Keyword::PRAGMA)?;
        let (name, name_span) = self.identifier()?;
        if name.eq_ignore_ascii_case("integrity_check") || name.eq_ignore_ascii_case("quick_check")
        {
            let quick = name.eq_ignore_ascii_case("quick_check");
            // The `PRAGMA integrity_check(N)`/schema-qualified forms are
            // real SQLite syntax but stay out of this narrow carve-out --
            // only the bare query form is accepted.
            if self.eat_punct(&TokenKind::LParen) {
                return self.unsupported("PRAGMA integrity_check(N) form not yet supported");
            }
            return Ok(Pragma::IntegrityCheck {
                quick,
                span: join_span(start, name_span),
            });
        }
        if name.eq_ignore_ascii_case("synchronous") {
            // Unlike `journal_mode`, the bare query form (no `=`) *is*
            // implemented (#645) -- it reports the connection's current
            // level rather than changing it.
            if !self.eat_punct(&TokenKind::Eq) {
                return Ok(Pragma::Synchronous {
                    level: None,
                    span: join_span(start, name_span),
                });
            }
            let (level, end) = self.pragma_synchronous_value()?;
            return Ok(Pragma::Synchronous {
                level: Some(level),
                span: join_span(start, end),
            });
        }
        if !name.eq_ignore_ascii_case("journal_mode") {
            return self.unsupported(format!("pragma {name:?} not yet supported"));
        }
        // The bare `PRAGMA journal_mode` query form (no `=`) is real
        // SQLite syntax (queries the current mode) but stays out of this
        // narrow carve-out -- `Unsupported`, not `Invalid`, since it's
        // syntactically valid SQL this parser just doesn't implement.
        if !self.eat_punct(&TokenKind::Eq) {
            return self.unsupported("PRAGMA journal_mode query form (no '=') not yet supported");
        }
        let (journal_mode, end) = self.pragma_journal_mode_value()?;
        Ok(Pragma::JournalMode {
            journal_mode,
            span: join_span(start, end),
        })
    }

    /// `WAL`/`DELETE` only (#388). `DELETE` is a reserved keyword
    /// (`Keyword::DELETE`), not an identifier -- mirrors parse.y's own
    /// `nmnum` production (parse.y:1723), which folds `DELETE` in as a
    /// bare-keyword pragma value alongside a plain `nm` (identifier)
    /// value like `WAL` -- so it needs its own match arm here rather
    /// than falling out of a single `identifier()` call.
    fn pragma_journal_mode_value(&mut self) -> PResult<(PragmaJournalMode, Span)> {
        match self.peek().kind.clone() {
            TokenKind::Identifier(text) if text.eq_ignore_ascii_case("wal") => {
                let span = self.advance_span();
                Ok((PragmaJournalMode::Wal, span))
            }
            TokenKind::Keyword(Keyword::DELETE) => {
                let span = self.advance_span();
                Ok((PragmaJournalMode::Delete, span))
            }
            _ => self.unsupported("unsupported journal_mode value (only WAL/DELETE are supported)"),
        }
    }

    /// `OFF`/`NORMAL`/`FULL` (case-insensitive identifiers) or the
    /// equivalent `0`/`1`/`2` integer literal (#645). Stock SQLite also
    /// accepts `EXTRA`, `ON`/boolean aliases, and out-of-range integers
    /// (with its own legacy masking quirks) -- all deferred, same as
    /// `journal_mode`'s own narrower-than-stock carve-out.
    fn pragma_synchronous_value(&mut self) -> PResult<(PragmaSynchronous, Span)> {
        match self.peek().kind.clone() {
            TokenKind::Identifier(text) if text.eq_ignore_ascii_case("off") => {
                Ok((PragmaSynchronous::Off, self.advance_span()))
            }
            TokenKind::Identifier(text) if text.eq_ignore_ascii_case("normal") => {
                Ok((PragmaSynchronous::Normal, self.advance_span()))
            }
            // `FULL` is a reserved keyword (used in `FULL [OUTER] JOIN`),
            // never tokenized as a plain identifier -- same reason
            // `journal_mode`'s `DELETE` value needs its own keyword
            // match arm rather than falling out of `identifier()`.
            TokenKind::Keyword(Keyword::FULL) => Ok((PragmaSynchronous::Full, self.advance_span())),
            TokenKind::Integer(0) => Ok((PragmaSynchronous::Off, self.advance_span())),
            TokenKind::Integer(1) => Ok((PragmaSynchronous::Normal, self.advance_span())),
            TokenKind::Integer(2) => Ok((PragmaSynchronous::Full, self.advance_span())),
            _ => self.unsupported(
                "unsupported synchronous value (only OFF/NORMAL/FULL/0/1/2 are supported)",
            ),
        }
    }

    /// `analyze-stmt` (#461, grammar V7 carve-out): `ANALYZE` or `ANALYZE
    /// table-name`. Only a single bare identifier (a table name) is
    /// accepted; a qualified `schema-name.table-name` form parses far
    /// enough to report a clean `Unsupported` rather than a hard parse
    /// error, mirroring `parse_pragma_stmt`'s precedent (#388) for a
    /// narrow MVP carve-out of otherwise-valid SQL. Whether a bare name
    /// names a table or an index is a catalog question, not a grammar
    /// one, so it's resolved later by the codegen dispatch layer, not
    /// here.
    pub(super) fn parse_analyze_stmt(&mut self) -> PResult<Analyze> {
        let start = self.expect_kw(Keyword::ANALYZE)?;
        let mut end = start;
        let target = if matches!(self.peek().kind, TokenKind::Identifier(_)) {
            let (name, span) = self.identifier()?;
            end = span;
            if self.eat_punct(&TokenKind::Dot) {
                return self.unsupported("ANALYZE schema-name.table-name not yet supported");
            }
            Some(name)
        } else {
            None
        };
        Ok(Analyze {
            target,
            span: join_span(start, end),
        })
    }

    /// `explain-stmt` (#243, grammar V4): `EXPLAIN [QUERY PLAN]
    /// select-stmt`. Only a `SELECT` body is supported — wrapping any
    /// other statement kind is `Unsupported` rather than silently
    /// accepted. Bare `EXPLAIN` (#538) and `EXPLAIN QUERY PLAN` share
    /// this same parse; the caller distinguishes them via `query_plan`
    /// and renders bare `EXPLAIN` as an opcode/bytecode listing (spec
    /// 009 Requirement 10) rather than a query-plan summary.
    pub(super) fn parse_explain_stmt(&mut self) -> PResult<Explain> {
        self.expect_kw(Keyword::EXPLAIN)?;
        let query_plan = if self.eat_kw(Keyword::QUERY) {
            self.expect_kw(Keyword::PLAN)?;
            true
        } else {
            false
        };
        let select = self.parse_select_stmt()?;
        Ok(Explain {
            query_plan,
            select: Box::new(select),
        })
    }

    pub(super) fn parse_select_stmt(&mut self) -> PResult<Select> {
        let with_clause = if self.at_kw(Keyword::WITH) {
            Some(self.parse_with_clause()?)
        } else {
            None
        };
        if self.at_kw(Keyword::VALUES) {
            return self.unsupported("bare VALUES not yet supported");
        }
        // A `WITH` clause can also introduce `INSERT`/`UPDATE`/`DELETE`
        // (a CTE feeding a data-modifying statement) in real SQLite —
        // recognized syntax this grammar slice doesn't parse, so it's
        // surfaced as `Unsupported` rather than falling through to the
        // generic `SELECT` expectation below, which would misreport it
        // as malformed SQL.
        if with_clause.is_some()
            && (self.at_kw(Keyword::INSERT)
                || self.at_kw(Keyword::UPDATE)
                || self.at_kw(Keyword::DELETE))
        {
            return self.unsupported("WITH ... INSERT/UPDATE/DELETE not yet supported");
        }
        let select_start = self.expect_kw(Keyword::SELECT)?;
        let start = with_clause.as_ref().map_or(select_start, |w| w.span);

        let distinct = if self.eat_kw(Keyword::DISTINCT) {
            Some(Distinctness::Distinct)
        } else if self.eat_kw(Keyword::ALL) {
            Some(Distinctness::All)
        } else {
            None
        };

        let mut columns = vec![self.result_column()?];
        while self.eat_punct(&TokenKind::Comma) {
            columns.push(self.result_column()?);
        }

        let from = if self.eat_kw(Keyword::FROM) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        if self.at_kw(Keyword::WINDOW) {
            return self.unsupported("WINDOW clause not yet supported");
        }

        let where_clause = if self.eat_kw(Keyword::WHERE) {
            Some(self.expr()?)
        } else {
            None
        };

        let mut group_by = Vec::new();
        let mut having = None;
        if self.eat_kw(Keyword::GROUP) {
            self.expect_kw(Keyword::BY)?;
            group_by.push(self.expr()?);
            while self.eat_punct(&TokenKind::Comma) {
                group_by.push(self.expr()?);
            }
            if self.eat_kw(Keyword::HAVING) {
                having = Some(self.expr()?);
            }
        } else if self.eat_kw(Keyword::HAVING) {
            // #287: HAVING with no GROUP BY filters the single
            // implicit whole-table group's aggregate result — accepted
            // at the grammar level now that codegen supports it.
            having = Some(self.expr()?);
        }

        let mut compound = Vec::new();
        loop {
            if self.at_kw(Keyword::INTERSECT) || self.at_kw(Keyword::EXCEPT) {
                return self.unsupported("compound SELECT (INTERSECT/EXCEPT) not yet supported");
            }
            if !self.at_kw(Keyword::UNION) {
                break;
            }
            let union_start = self.advance_span();
            let op = if self.eat_kw(Keyword::ALL) {
                CompoundOp::UnionAll
            } else {
                CompoundOp::Union
            };
            compound.push(self.parse_compound_select_arm(union_start, op)?);
        }

        let mut order_by = Vec::new();
        if self.eat_kw(Keyword::ORDER) {
            self.expect_kw(Keyword::BY)?;
            order_by.push(self.ordering_term()?);
            while self.eat_punct(&TokenKind::Comma) {
                order_by.push(self.ordering_term()?);
            }
        }

        let limit = if self.eat_kw(Keyword::LIMIT) {
            let limit_expr = self.expr()?;
            let offset = if self.eat_kw(Keyword::OFFSET) || self.eat_punct(&TokenKind::Comma) {
                Some(self.expr()?)
            } else {
                None
            };
            Some(Limit {
                limit: limit_expr,
                offset,
            })
        } else {
            None
        };

        let end = self
            .tokens
            .get(self.pos.saturating_sub(1))
            .map_or(start, |t| t.span);
        Ok(Select {
            with_clause,
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            compound,
            order_by,
            limit,
            span: join_span(start, end),
        })
    }

    /// `with-clause` (#375, grammar V6): `WITH cte { , cte }`, where each
    /// `cte` is `cte_name [(col, ...)] AS (select-stmt)`. `WITH
    /// RECURSIVE` is not yet supported — only the non-recursive form.
    fn parse_with_clause(&mut self) -> PResult<WithClause> {
        let start = self.expect_kw(Keyword::WITH)?;
        if self.at_kw(Keyword::RECURSIVE) {
            return self.unsupported("WITH RECURSIVE not yet supported");
        }
        let mut ctes = vec![self.parse_common_table_expr()?];
        while self.eat_punct(&TokenKind::Comma) {
            ctes.push(self.parse_common_table_expr()?);
        }
        let end = ctes.last().map_or(start, |cte| cte.span);
        Ok(WithClause {
            ctes,
            span: join_span(start, end),
        })
    }

    /// `cte_name [(col, ...)] AS (select-stmt)`.
    fn parse_common_table_expr(&mut self) -> PResult<CommonTableExpr> {
        let (name, name_span) = self.identifier()?;

        let columns = if self.eat_punct(&TokenKind::LParen) {
            let mut cols = vec![self.identifier()?.0];
            while self.eat_punct(&TokenKind::Comma) {
                cols.push(self.identifier()?.0);
            }
            self.expect_punct(TokenKind::RParen, "')'")?;
            Some(cols)
        } else {
            None
        };

        self.expect_kw(Keyword::AS)?;
        // `[NOT] MATERIALIZED` (a query-planner hint, SQLite 3.35+) is
        // recognized syntax we don't yet act on — surfacing it as
        // `Unsupported` rather than falling through to the generic `'('`
        // expectation below, which would misreport it as malformed SQL.
        if self.at_kw(Keyword::MATERIALIZED)
            || (self.at_kw(Keyword::NOT)
                && matches!(
                    self.peek_at(1).kind,
                    TokenKind::Keyword(Keyword::MATERIALIZED)
                ))
        {
            return self.unsupported("[NOT] MATERIALIZED CTE hint not yet supported");
        }
        self.expect_punct(TokenKind::LParen, "'('")?;
        let query = self.parse_select_stmt()?;
        let end = self.expect_punct(TokenKind::RParen, "')'")?;

        Ok(CommonTableExpr {
            name,
            columns,
            query: Box::new(query),
            span: join_span(name_span, end),
        })
    }

    /// One `UNION [ALL] SELECT ...` arm (#240 for `UNION ALL`, #377 for
    /// plain `UNION`): same core shape as [`Self::parse_select_stmt`]
    /// minus ORDER BY/LIMIT, which bind to the whole compound statement
    /// rather than any one arm.
    fn parse_compound_select_arm(
        &mut self,
        union_start: Span,
        op: CompoundOp,
    ) -> PResult<CompoundSelect> {
        if self.at_kw(Keyword::VALUES) {
            return self.unsupported("UNION [ALL] VALUES (...) not yet supported");
        }
        let start = self.expect_kw(Keyword::SELECT)?;

        let distinct = if self.eat_kw(Keyword::DISTINCT) {
            Some(Distinctness::Distinct)
        } else if self.eat_kw(Keyword::ALL) {
            Some(Distinctness::All)
        } else {
            None
        };

        let mut columns = vec![self.result_column()?];
        while self.eat_punct(&TokenKind::Comma) {
            columns.push(self.result_column()?);
        }

        let from = if self.eat_kw(Keyword::FROM) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        if self.at_kw(Keyword::WINDOW) {
            return self.unsupported("WINDOW clause not yet supported");
        }

        let where_clause = if self.eat_kw(Keyword::WHERE) {
            Some(self.expr()?)
        } else {
            None
        };

        let mut group_by = Vec::new();
        let mut having = None;
        if self.eat_kw(Keyword::GROUP) {
            self.expect_kw(Keyword::BY)?;
            group_by.push(self.expr()?);
            while self.eat_punct(&TokenKind::Comma) {
                group_by.push(self.expr()?);
            }
            if self.eat_kw(Keyword::HAVING) {
                having = Some(self.expr()?);
            }
        } else if self.at_kw(Keyword::HAVING) {
            return self.unsupported("HAVING without GROUP BY not yet supported");
        }

        let end = self
            .tokens
            .get(self.pos.saturating_sub(1))
            .map_or(start, |t| t.span);
        Ok(CompoundSelect {
            op,
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            span: join_span(union_start, end),
        })
    }

    fn result_column(&mut self) -> PResult<ResultColumn> {
        if self.eat_punct(&TokenKind::Star) {
            return Ok(ResultColumn::Star);
        }
        // `table-name "." "*"` needs 2-token lookahead to distinguish
        // from a column-ref expression.
        if let TokenKind::Identifier(name) = self.peek().kind.clone() {
            if matches!(self.peek_at(1).kind, TokenKind::Dot)
                && matches!(self.peek_at(2).kind, TokenKind::Star)
            {
                self.advance();
                self.advance();
                self.advance();
                return Ok(ResultColumn::TableStar { table: name });
            }
        }
        let expr = self.expr()?;
        let alias = self.opt_alias()?;
        Ok(ResultColumn::Expr { expr, alias })
    }

    /// `[ [ "AS" ] identifier ]` — a bare identifier only counts as an
    /// alias, never a keyword (keywords are never `TokenKind::Identifier`).
    fn opt_alias(&mut self) -> PResult<Option<String>> {
        if self.eat_kw(Keyword::AS) {
            // Stock SQLite accepts a single-quoted string literal as an
            // alias too (a legacy compatibility quirk — `'m'` is treated
            // as if it were the identifier `m`), which our tokenizer
            // sees as a `TokenKind::String` rather than `Identifier`.
            // Recognized syntax we don't yet implement, not malformed
            // SQL: a plain `identifier()` call here would misreport it
            // as `Invalid` instead of `Unsupported`.
            if matches!(self.peek().kind, TokenKind::String(_)) {
                return self.unsupported(
                    "a quoted string literal as an alias (e.g. AS 'name') is not yet supported",
                );
            }
            let (name, _) = self.identifier()?;
            return Ok(Some(name));
        }
        if let TokenKind::Identifier(name) = self.peek().kind.clone() {
            self.advance();
            return Ok(Some(name));
        }
        Ok(None)
    }

    /// Parses `USING ( col { , col } )` (#250) — at least one
    /// parenthesized, comma-separated column name.
    fn using_columns(&mut self) -> PResult<Vec<String>> {
        self.expect_kw(Keyword::USING)?;
        self.expect_punct(TokenKind::LParen, "'(' after USING")?;
        let mut cols = vec![self.identifier()?.0];
        while self.eat_punct(&TokenKind::Comma) {
            cols.push(self.identifier()?.0);
        }
        self.expect_punct(TokenKind::RParen, "')' to close USING column list")?;
        Ok(cols)
    }

    /// Parses `FROM <table_ref> ( "," <table_ref>
    /// | [NATURAL] <join_op> <table_ref> [ON <expr> | USING (col, ...)]
    /// )*` (#237's INNER/LEFT/CROSS slice, extended by #250 with
    /// NATURAL, RIGHT/FULL, USING, and comma-style joins). Comma-joins
    /// are synthesized as constraint-less `JoinOp::Cross` steps (ANSI
    /// comma-join is definitionally an unconstrained cross join). A
    /// bare `JOIN`/`INNER JOIN`/`LEFT/RIGHT/FULL [OUTER] JOIN` with no
    /// `ON`/`USING` at all, and `NATURAL CROSS JOIN` (not legal SQLite
    /// grammar — NATURAL requires an implied constraint, which CROSS's
    /// definition-by-absence-of-constraint contradicts), stay explicit
    /// `unsupported(..)` errors.
    fn parse_from_clause(&mut self) -> PResult<FromClause> {
        let first = self.table_ref()?;
        let mut joins = Vec::new();
        loop {
            if self.eat_punct(&TokenKind::Comma) {
                // Real SQLite grammar treats `,` as a `joinop` just like
                // `JOIN` (`stl_prefix ::= seltablist joinop`, parse.y:758,
                // where `joinop ::= COMMA|JOIN`, parse.y:867) — so an
                // `ON`/`USING` clause is legal after a comma-joined table
                // too (`FROM a, b ON a.x = b.y`), not just constraint-less.
                let table = self.table_ref()?;
                let constraint = if self.eat_kw(Keyword::ON) {
                    Some(JoinConstraint::On(self.expr()?))
                } else if self.at_kw(Keyword::USING) {
                    Some(JoinConstraint::Using(self.using_columns()?))
                } else {
                    None
                };
                joins.push(Join {
                    op: JoinOp::Cross,
                    table,
                    constraint,
                    natural: false,
                });
                continue;
            }
            let natural = self.eat_kw(Keyword::NATURAL);
            // A bare `OUTER` only ever appears right after `LEFT`/
            // `RIGHT`/`FULL` (consumed together with those, below) —
            // seeing it here means some other/malformed join-operator
            // ordering. Reporting it as unsupported keeps it out of the
            // "unexpected trailing token" hard-error bucket, matching
            // this parser's convention of a graceful `unsupported(..)`
            // for anything recognizably join-shaped but out of this
            // slice's scope.
            if self.at_kw(Keyword::OUTER) {
                return self
                    .unsupported("OUTER without a preceding LEFT/RIGHT/FULL not yet supported");
            }
            if self.eat_kw(Keyword::CROSS) {
                self.expect_kw(Keyword::JOIN)?;
                if natural {
                    return self.unsupported("NATURAL CROSS JOIN is not valid SQLite grammar");
                }
                let table = self.table_ref()?;
                if self.at_kw(Keyword::ON) {
                    return self.unsupported("CROSS JOIN with an ON clause not yet supported");
                }
                let constraint = if self.at_kw(Keyword::USING) {
                    Some(JoinConstraint::Using(self.using_columns()?))
                } else {
                    None
                };
                joins.push(Join {
                    op: JoinOp::Cross,
                    table,
                    constraint,
                    natural: false,
                });
                continue;
            }
            let op = if self.eat_kw(Keyword::LEFT) {
                self.eat_kw(Keyword::OUTER);
                self.expect_kw(Keyword::JOIN)?;
                Some(JoinOp::Left)
            } else if self.eat_kw(Keyword::RIGHT) {
                self.eat_kw(Keyword::OUTER);
                self.expect_kw(Keyword::JOIN)?;
                Some(JoinOp::Right)
            } else if self.eat_kw(Keyword::FULL) {
                self.eat_kw(Keyword::OUTER);
                self.expect_kw(Keyword::JOIN)?;
                Some(JoinOp::Full)
            } else if self.eat_kw(Keyword::INNER) {
                self.expect_kw(Keyword::JOIN)?;
                Some(JoinOp::Inner)
            } else if self.eat_kw(Keyword::JOIN) {
                Some(JoinOp::Inner)
            } else {
                None
            };
            let Some(op) = op else {
                if natural {
                    return self.unsupported("NATURAL not followed by a recognized join operator");
                }
                break;
            };
            let table = self.table_ref()?;
            if natural {
                // NATURAL joins carry no explicit ON/USING clause — the
                // join columns are same-named columns in both tables,
                // resolved by codegen, not the parser.
                joins.push(Join {
                    op,
                    table,
                    constraint: None,
                    natural: true,
                });
                continue;
            }
            if self.at_kw(Keyword::USING) {
                let cols = self.using_columns()?;
                joins.push(Join {
                    op,
                    table,
                    constraint: Some(JoinConstraint::Using(cols)),
                    natural: false,
                });
                continue;
            }
            // A real `JOIN`/`INNER JOIN`/`LEFT/RIGHT/FULL [OUTER] JOIN`
            // with no `ON`/`USING` at all is valid SQL (equivalent to a
            // constraint-less cross join) — real SQLite accepts it, so
            // this stays a graceful `unsupported(..)` rather than the
            // hard parse error `expect_kw` would raise, which would
            // otherwise misclassify valid SQL as malformed (caught by
            // `tests/corpus/extracted_sql_test.rs`'s
            // `no_extracted_select_is_reported_invalid`). This bounded
            // MVP only compiles the `ON`-qualified form.
            if !self.at_kw(Keyword::ON) {
                return self.unsupported("JOIN without an ON/USING clause not yet supported");
            }
            self.expect_kw(Keyword::ON)?;
            let on_expr = self.expr()?;
            joins.push(Join {
                op,
                table,
                constraint: Some(JoinConstraint::On(on_expr)),
                natural: false,
            });
        }
        Ok(FromClause { first, joins })
    }

    /// A single `table-name [AS alias]` or (#257) `"(" select-stmt ")"
    /// [AS] identifier` — shared by the FROM clause's first table and
    /// every join's right-hand table. Schema-qualified names,
    /// table-valued functions, and `INDEXED BY`/`NOT INDEXED` stay
    /// explicit `unsupported(..)` errors. A subquery's alias is
    /// mandatory here (unlike a plain table's) — this codebase's column
    /// resolution needs a qualifier to refer to the subquery's columns,
    /// and SQLite itself always names one in practice.
    fn table_ref(&mut self) -> PResult<TableRef> {
        if matches!(self.peek().kind, TokenKind::LParen) {
            let start = self.peek().span;
            self.advance();
            if !self.at_kw(Keyword::SELECT) {
                return self.unsupported(
                    "table-valued functions in FROM not yet supported (only a SELECT subquery is)",
                );
            }
            let subquery = self.parse_select_stmt()?;
            self.expect_punct(TokenKind::RParen, "')' to close FROM subquery")?;
            let alias = self.opt_alias()?;
            let Some(alias) = alias else {
                return self.unsupported("a subquery in FROM requires an alias");
            };
            let end = self
                .tokens
                .get(self.pos.saturating_sub(1))
                .map_or(start, |t| t.span);
            return Ok(TableRef {
                kind: TableRefKind::Subquery(Box::new(subquery)),
                alias: Some(alias),
                span: join_span(start, end),
            });
        }
        let (name, start) = self.identifier()?;
        if matches!(self.peek().kind, TokenKind::Dot) {
            return self.unsupported("schema-qualified table names not yet supported");
        }
        let alias = self.opt_alias()?;
        let end = alias.is_some();
        let span = if end {
            join_span(
                start,
                self.tokens
                    .get(self.pos.saturating_sub(1))
                    .map_or(start, |t| t.span),
            )
        } else {
            start
        };

        if self.at_kw(Keyword::INDEXED) {
            return self.unsupported("INDEXED BY not yet supported");
        }
        if self.at_kw(Keyword::NOT)
            && matches!(self.peek_at(1).kind, TokenKind::Keyword(Keyword::INDEXED))
        {
            return self.unsupported("NOT INDEXED not yet supported");
        }
        if matches!(self.peek().kind, TokenKind::LParen) {
            return self.unsupported("table-valued functions in FROM not yet supported");
        }

        Ok(TableRef {
            kind: TableRefKind::Name(name),
            alias,
            span,
        })
    }

    fn ordering_term(&mut self) -> PResult<OrderingTerm> {
        let expr = self.expr()?;
        let desc = if self.eat_kw(Keyword::ASC) {
            Some(false)
        } else if self.eat_kw(Keyword::DESC) {
            Some(true)
        } else {
            None
        };
        let nulls_last = if self.eat_kw(Keyword::NULLS) {
            if self.eat_kw(Keyword::FIRST) {
                Some(false)
            } else if self.eat_kw(Keyword::LAST) {
                Some(true)
            } else {
                return self.invalid("expected FIRST or LAST after NULLS");
            }
        } else {
            None
        };
        Ok(OrderingTerm {
            expr,
            desc,
            nulls_last,
        })
    }

    // ---- expressions -----------------------------------------------------

    pub(super) fn expr(&mut self) -> PResult<Expr> {
        self.with_depth_guard(|this| this.bool_expr(0))
    }

    /// Precedence climb over `OR` (prec 0) and `AND` (prec 1, binds
    /// tighter) — one stack frame here instead of two separate
    /// pass-through functions (`or_expr`/`and_expr`) per nesting level.
    /// Collapsing these was one part of narrowing the debug/release stack
    /// gap that let a stack overflow pre-empt the `MAX_EXPR_DEPTH` guard
    /// (#118); see `binary_expr` for the larger half of that collapse.
    fn bool_expr(&mut self, min_prec: u8) -> PResult<Expr> {
        let mut lhs = self.not_expr()?;
        loop {
            // AND binds tighter than OR, so AND's rhs only ever needs
            // `not_expr` (nothing tighter exists in this pair); OR's rhs
            // must still climb through any following AND, but never a
            // sibling OR (left-associative).
            lhs = if self.at_kw(Keyword::AND) {
                if min_prec > 1 {
                    break;
                }
                self.advance();
                bin(BinaryOp::And, lhs, self.not_expr()?)
            } else if self.at_kw(Keyword::OR) {
                if min_prec > 0 {
                    break;
                }
                self.advance();
                bin(BinaryOp::Or, lhs, self.bool_expr(1)?)
            } else {
                break;
            };
        }
        Ok(lhs)
    }

    fn not_expr(&mut self) -> PResult<Expr> {
        self.with_depth_guard(|this| {
            if this.at_kw(Keyword::NOT) {
                let start = this.advance_span();
                if this.at_kw(Keyword::EXISTS) {
                    this.advance();
                    return this.exists_tail(start, true);
                }
                let inner = this.not_expr()?;
                let span = join_span(start, inner.span);
                return Ok(Expr {
                    kind: ExprKind::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(inner),
                    },
                    span,
                });
            }
            this.equality_expr()
        })
    }

    fn equality_expr(&mut self) -> PResult<Expr> {
        if let Some(multi_in) = self.try_tuple_in_subquery()? {
            return Ok(multi_in);
        }
        let mut lhs = self.binary_expr(1)?;
        loop {
            lhs = match self.peek().kind.clone() {
                TokenKind::Eq => {
                    self.advance();
                    let rhs = self.binary_expr(1)?;
                    bin(BinaryOp::Eq, lhs, rhs)
                }
                TokenKind::Ne => {
                    self.advance();
                    let rhs = self.binary_expr(1)?;
                    bin(BinaryOp::Ne, lhs, rhs)
                }
                TokenKind::Keyword(Keyword::IS) => {
                    self.advance();
                    let negated = self.eat_kw(Keyword::NOT);
                    let rhs = self.binary_expr(1)?;
                    let span = join_span(lhs.span, rhs.span);
                    Expr {
                        kind: ExprKind::Is {
                            lhs: Box::new(lhs),
                            rhs: Box::new(rhs),
                            negated,
                        },
                        span,
                    }
                }
                TokenKind::Keyword(Keyword::ISNULL) => {
                    let end = self.advance_span();
                    let span = join_span(lhs.span, end);
                    Expr {
                        kind: ExprKind::IsNull {
                            expr: Box::new(lhs),
                            negated: false,
                        },
                        span,
                    }
                }
                TokenKind::Keyword(Keyword::NOTNULL) => {
                    let end = self.advance_span();
                    let span = join_span(lhs.span, end);
                    Expr {
                        kind: ExprKind::IsNull {
                            expr: Box::new(lhs),
                            negated: true,
                        },
                        span,
                    }
                }
                TokenKind::Keyword(Keyword::BETWEEN) => {
                    self.advance();
                    let (lo, hi) = self.between_tail()?;
                    let span = join_span(lhs.span, hi.span);
                    Expr {
                        kind: ExprKind::Between {
                            expr: Box::new(lhs),
                            lo: Box::new(lo),
                            hi: Box::new(hi),
                            negated: false,
                        },
                        span,
                    }
                }
                TokenKind::Keyword(Keyword::IN) => {
                    self.advance();
                    self.in_tail(lhs, false)?
                }
                TokenKind::Keyword(Keyword::LIKE) | TokenKind::Keyword(Keyword::GLOB) => {
                    let glob = self.at_kw(Keyword::GLOB);
                    self.advance();
                    self.like_tail(lhs, glob, false)?
                }
                TokenKind::Keyword(Keyword::NOT) => match self.peek_at(1).kind.clone() {
                    TokenKind::Null => {
                        self.advance();
                        let end = self.advance_span();
                        let span = join_span(lhs.span, end);
                        Expr {
                            kind: ExprKind::IsNull {
                                expr: Box::new(lhs),
                                negated: true,
                            },
                            span,
                        }
                    }
                    TokenKind::Keyword(Keyword::BETWEEN) => {
                        self.advance();
                        self.advance();
                        let (lo, hi) = self.between_tail()?;
                        let span = join_span(lhs.span, hi.span);
                        Expr {
                            kind: ExprKind::Between {
                                expr: Box::new(lhs),
                                lo: Box::new(lo),
                                hi: Box::new(hi),
                                negated: true,
                            },
                            span,
                        }
                    }
                    TokenKind::Keyword(Keyword::IN) => {
                        self.advance();
                        self.advance();
                        self.in_tail(lhs, true)?
                    }
                    TokenKind::Keyword(Keyword::LIKE) | TokenKind::Keyword(Keyword::GLOB) => {
                        let glob =
                            matches!(self.peek_at(1).kind, TokenKind::Keyword(Keyword::GLOB));
                        self.advance();
                        self.advance();
                        self.like_tail(lhs, glob, true)?
                    }
                    _ => break,
                },
                _ => break,
            };
        }
        Ok(lhs)
    }

    fn between_tail(&mut self) -> PResult<(Expr, Expr)> {
        let lo = self.binary_expr(1)?;
        self.expect_kw(Keyword::AND)?;
        let hi = self.binary_expr(1)?;
        Ok((lo, hi))
    }

    /// `EXISTS (SELECT ...)` / `NOT EXISTS (SELECT ...)` — `start` is the
    /// span of the `EXISTS`/`NOT` token this tail follows, and anything
    /// after `EXISTS (` that isn't a `SELECT` is still `unsupported`
    /// (subqueries in FROM, `ANY`/`ALL`/`SOME`, etc. all parse a `SELECT`
    /// here so this stays narrow).
    fn exists_tail(&mut self, start: Span, negated: bool) -> PResult<Expr> {
        self.expect_punct(TokenKind::LParen, "'(' after EXISTS")?;
        if !self.at_kw(Keyword::SELECT) {
            return self.unsupported("EXISTS ( ... ) requires a SELECT subquery");
        }
        let subquery = self.parse_select_stmt()?;
        if matches!(
            self.peek().kind,
            TokenKind::Keyword(Keyword::UNION)
                | TokenKind::Keyword(Keyword::INTERSECT)
                | TokenKind::Keyword(Keyword::EXCEPT)
        ) {
            return self.unsupported("compound SELECT (UNION/INTERSECT/EXCEPT) not yet supported");
        }
        let end = self.expect_punct(TokenKind::RParen, "')' to close EXISTS subquery")?;
        let span = join_span(start, end);
        Ok(Expr {
            kind: ExprKind::Exists {
                subquery: Box::new(subquery),
                negated,
            },
            span,
        })
    }

    /// Cheap, non-allocating lookahead: does the parenthesized group
    /// starting at `self.pos` (a `(`) contain a top-level (depth-0
    /// relative to this group) comma, and is its matching `)`
    /// immediately followed by `IN`/`NOT IN`? Gates
    /// [`Self::try_tuple_in_subquery`]'s much more expensive speculative
    /// parse: without this, every `(` — including each level of a
    /// deeply nested plain grouping expression — would recursively
    /// parse its entire contents twice (once speculatively, once for
    /// real), turning `MAX_EXPR_DEPTH`'s already-bounded-but-nested
    /// pathological-input test (#118, 10,000 nested parens) quadratic.
    /// A token-index scan costs nothing next to that.
    fn looks_like_tuple_in(&self) -> bool {
        let mut idx = self.pos.saturating_add(1);
        let mut depth: u32 = 0;
        let mut saw_top_comma = false;
        loop {
            let kind = match self.tokens.get(idx) {
                Some(t) => &t.kind,
                None => return false,
            };
            match kind {
                TokenKind::Eof => return false,
                TokenKind::LParen => depth = depth.saturating_add(1),
                TokenKind::RParen => {
                    if depth == 0 {
                        let after_in = matches!(
                            self.tokens.get(idx.saturating_add(1)).map(|t| &t.kind),
                            Some(TokenKind::Keyword(Keyword::IN))
                        );
                        let after_not_in = matches!(
                            self.tokens.get(idx.saturating_add(1)).map(|t| &t.kind),
                            Some(TokenKind::Keyword(Keyword::NOT))
                        ) && matches!(
                            self.tokens.get(idx.saturating_add(2)).map(|t| &t.kind),
                            Some(TokenKind::Keyword(Keyword::IN))
                        );
                        return saw_top_comma && (after_in || after_not_in);
                    }
                    depth = depth.saturating_sub(1);
                }
                TokenKind::Comma if depth == 0 => saw_top_comma = true,
                _ => {}
            }
            idx = idx.saturating_add(1);
        }
    }

    /// `(a, b, ...) IN (SELECT ...)` / `... NOT IN (SELECT ...)` (#251) —
    /// the multi-column form. A bare parenthesized expr-list isn't valid
    /// SQLite expression syntax anywhere else, so this speculatively
    /// parses `"(" expr-list ")"` and only commits (returning `Some`) if
    /// the list has arity >= 2 *and* is immediately followed by `IN`/
    /// `NOT IN (SELECT ...)`; any other shape rewinds `self.pos` and
    /// returns `None`, leaving the normal single-expr/grouping-paren path
    /// (`primary_expr`'s `LParen` arm) to parse it as before.
    /// [`Self::looks_like_tuple_in`] gates entry so this expensive path
    /// only ever runs when a top-level comma + trailing `IN` are
    /// actually present.
    fn try_tuple_in_subquery(&mut self) -> PResult<Option<Expr>> {
        if !matches!(self.peek().kind, TokenKind::LParen) || !self.looks_like_tuple_in() {
            return Ok(None);
        }
        let start_pos = self.pos;
        let start_span = self.peek().span;
        self.advance();
        let list = match self.expr_list() {
            Ok(list) => list,
            Err(_) => {
                self.pos = start_pos;
                return Ok(None);
            }
        };
        if self.expect_punct(TokenKind::RParen, "')'").is_err() {
            self.pos = start_pos;
            return Ok(None);
        }
        let negated = if self.at_kw(Keyword::IN) {
            self.advance();
            false
        } else if self.at_kw(Keyword::NOT)
            && matches!(self.peek_at(1).kind, TokenKind::Keyword(Keyword::IN))
        {
            self.advance();
            self.advance();
            true
        } else {
            self.pos = start_pos;
            return Ok(None);
        };
        if list.len() < 2 {
            self.pos = start_pos;
            return Ok(None);
        }
        self.expect_punct(TokenKind::LParen, "'(' after IN")?;
        if !self.at_kw(Keyword::SELECT) {
            return self
                .unsupported("multi-column IN requires a SELECT subquery on the right-hand side");
        }
        let subquery = self.parse_select_stmt()?;
        if matches!(
            self.peek().kind,
            TokenKind::Keyword(Keyword::UNION)
                | TokenKind::Keyword(Keyword::INTERSECT)
                | TokenKind::Keyword(Keyword::EXCEPT)
        ) {
            return self.unsupported("compound SELECT (UNION/INTERSECT/EXCEPT) not yet supported");
        }
        let end = self.expect_punct(TokenKind::RParen, "')' to close IN subquery")?;
        let span = join_span(start_span, end);
        Ok(Some(Expr {
            kind: ExprKind::InSubqueryMulti {
                exprs: list,
                subquery: Box::new(subquery),
                negated,
            },
            span,
        }))
    }

    fn in_tail(&mut self, lhs: Expr, negated: bool) -> PResult<Expr> {
        if !matches!(self.peek().kind, TokenKind::LParen) {
            return self.unsupported("IN <table-name> not yet supported");
        }
        self.expect_punct(TokenKind::LParen, "'(' after IN")?;
        if self.at_kw(Keyword::SELECT) {
            let subquery = self.parse_select_stmt()?;
            if matches!(
                self.peek().kind,
                TokenKind::Keyword(Keyword::UNION)
                    | TokenKind::Keyword(Keyword::INTERSECT)
                    | TokenKind::Keyword(Keyword::EXCEPT)
            ) {
                return self
                    .unsupported("compound SELECT (UNION/INTERSECT/EXCEPT) not yet supported");
            }
            let end = self.expect_punct(TokenKind::RParen, "')' to close IN subquery")?;
            let span = join_span(lhs.span, end);
            return Ok(Expr {
                kind: ExprKind::InSubquery {
                    expr: Box::new(lhs),
                    subquery: Box::new(subquery),
                    negated,
                },
                span,
            });
        }
        let list = if matches!(self.peek().kind, TokenKind::RParen) {
            Vec::new()
        } else {
            self.expr_list()?
        };
        let end = self.expect_punct(TokenKind::RParen, "')' to close IN list")?;
        let span = join_span(lhs.span, end);
        Ok(Expr {
            kind: ExprKind::In {
                expr: Box::new(lhs),
                list,
                negated,
            },
            span,
        })
    }

    fn like_tail(&mut self, lhs: Expr, glob: bool, negated: bool) -> PResult<Expr> {
        let pattern = self.binary_expr(1)?;
        let mut span = join_span(lhs.span, pattern.span);
        let escape = if self.eat_kw(Keyword::ESCAPE) {
            let e = self.binary_expr(1)?;
            span = join_span(span, e.span);
            Some(Box::new(e))
        } else {
            None
        };
        Ok(Expr {
            kind: ExprKind::Like {
                expr: Box::new(lhs),
                pattern: Box::new(pattern),
                glob,
                negated,
                escape,
            },
            span,
        })
    }

    /// Precedence climb merging what used to be four separate pass-through
    /// levels — `relational_expr` (prec 1: `<`/`<=`/`>`/`>=`) ->
    /// `bitwise_expr` (prec 2: `&`/`|`/`<<`/`>>`) -> `additive_expr` (prec
    /// 3: `+`/`-`) -> `multiplicative_expr` (prec 4: `*`/`/`/`%`) ->
    /// `concat_expr` (prec 5: `||`) — into one stack frame per nesting
    /// level instead of five. All five operator groups are left-
    /// associative, so a run of same-precedence operators (`1+2+3+...`)
    /// stays iterative (the `loop`); recursion only climbs one level per
    /// *distinct* precedence step in the expression, exactly mirroring the
    /// original call chain's shape. `min_prec` is the lowest precedence
    /// this call is willing to consume; callers needing the full chain
    /// (what `relational_expr` used to mean) pass `1`. Narrows the debug
    /// stack-depth gap that let a stack overflow pre-empt the
    /// `MAX_EXPR_DEPTH` guard (#118).
    fn binary_expr(&mut self, min_prec: u8) -> PResult<Expr> {
        let mut lhs = self.arrow_expr()?;
        while let Some((op, prec)) = Self::binary_op(&self.peek().kind) {
            if prec < min_prec {
                break;
            }
            self.advance();
            let rhs = self.binary_expr(prec.saturating_add(1))?;
            lhs = bin(op, lhs, rhs);
        }
        Ok(lhs)
    }

    fn binary_op(kind: &TokenKind) -> Option<(BinaryOp, u8)> {
        Some(match kind {
            TokenKind::Lt => (BinaryOp::Lt, 1),
            TokenKind::Le => (BinaryOp::Le, 1),
            TokenKind::Gt => (BinaryOp::Gt, 1),
            TokenKind::Ge => (BinaryOp::Ge, 1),
            TokenKind::BitAnd => (BinaryOp::BitAnd, 2),
            TokenKind::BitOr => (BinaryOp::BitOr, 2),
            TokenKind::Shl => (BinaryOp::Shl, 2),
            TokenKind::Shr => (BinaryOp::Shr, 2),
            TokenKind::Plus => (BinaryOp::Add, 3),
            TokenKind::Minus => (BinaryOp::Sub, 3),
            TokenKind::Star => (BinaryOp::Mul, 4),
            TokenKind::Slash => (BinaryOp::Div, 4),
            TokenKind::Percent => (BinaryOp::Mod, 4),
            TokenKind::Concat => (BinaryOp::Concat, 5),
            _ => return None,
        })
    }

    /// `->` / `->>` (JSON extract operators, V11) are recognized here so
    /// they're reported `Unsupported` rather than falling through to a
    /// generic "unexpected trailing token" `Invalid`.
    fn arrow_expr(&mut self) -> PResult<Expr> {
        let lhs = self.collate_expr()?;
        if matches!(self.peek().kind, TokenKind::Arrow | TokenKind::ArrowArrow) {
            return self.unsupported("-> / ->> operators not yet supported");
        }
        Ok(lhs)
    }

    fn collate_expr(&mut self) -> PResult<Expr> {
        let mut lhs = self.unary_expr()?;
        while self.eat_kw(Keyword::COLLATE) {
            let (name, end) = self.identifier()?;
            let span = join_span(lhs.span, end);
            lhs = Expr {
                kind: ExprKind::Collate {
                    expr: Box::new(lhs),
                    collation: name,
                },
                span,
            };
        }
        Ok(lhs)
    }

    fn unary_expr(&mut self) -> PResult<Expr> {
        self.with_depth_guard(|this| {
            let op = match this.peek().kind {
                TokenKind::Plus => Some(UnaryOp::Plus),
                TokenKind::Minus => Some(UnaryOp::Minus),
                TokenKind::BitNot => Some(UnaryOp::BitNot),
                _ => None,
            };
            if let Some(op) = op {
                let start = this.advance_span();
                let inner = this.unary_expr()?;
                let span = join_span(start, inner.span);
                // `9223372036854775808` has no positive i64
                // representation, so the tokenizer folds it to a Float.
                // Negated, it is exactly i64::MIN — SQLite parses
                // `-9223372036854775808` as an INTEGER literal, not a
                // REAL (spike #59 finding).
                if matches!(op, UnaryOp::Minus) {
                    if let ExprKind::Literal(Literal::Float(f)) = inner.kind {
                        if f == 9_223_372_036_854_775_808.0 {
                            return Ok(Expr {
                                kind: ExprKind::Literal(Literal::Integer(i64::MIN)),
                                span,
                            });
                        }
                    }
                }
                return Ok(Expr {
                    kind: ExprKind::Unary {
                        op,
                        expr: Box::new(inner),
                    },
                    span,
                });
            }
            this.primary_expr()
        })
    }

    fn primary_expr(&mut self) -> PResult<Expr> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::Integer(v) => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::Integer(v)),
                    span: tok.span,
                })
            }
            TokenKind::Float(v) => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::Float(v)),
                    span: tok.span,
                })
            }
            TokenKind::String(s) => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::Str(s)),
                    span: tok.span,
                })
            }
            TokenKind::Blob(b) => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::Blob(*b)),
                    span: tok.span,
                })
            }
            TokenKind::Null => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::Null),
                    span: tok.span,
                })
            }
            TokenKind::True => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::True),
                    span: tok.span,
                })
            }
            TokenKind::False => {
                self.advance();
                Ok(Expr {
                    kind: ExprKind::Literal(Literal::False),
                    span: tok.span,
                })
            }
            TokenKind::Param(p) => {
                self.advance();
                let kind = match *p {
                    Param::Anonymous => ParamKind::Anonymous,
                    Param::Numbered(n) => ParamKind::Numbered(n),
                    Param::Colon(s) => ParamKind::Colon(s),
                    Param::At(s) => ParamKind::At(s),
                    Param::Dollar(s) => ParamKind::Dollar(s),
                };
                Ok(Expr {
                    kind: ExprKind::Param(kind),
                    span: tok.span,
                })
            }
            TokenKind::Keyword(Keyword::CURRENT_TIME)
            | TokenKind::Keyword(Keyword::CURRENT_DATE)
            | TokenKind::Keyword(Keyword::CURRENT_TIMESTAMP) => {
                self.unsupported("CURRENT_TIME/CURRENT_DATE/CURRENT_TIMESTAMP not yet supported")
            }
            TokenKind::Keyword(Keyword::CASE) => self.case_expr(),
            TokenKind::Keyword(Keyword::CAST) => self.cast_expr(),
            TokenKind::Keyword(Keyword::EXISTS) => {
                let start = tok.span;
                self.advance();
                self.exists_tail(start, false)
            }
            TokenKind::Identifier(name) => {
                self.advance();
                if matches!(self.peek().kind, TokenKind::LParen) {
                    return self.function_call(name, tok.span);
                }
                let mut parts = vec![name];
                while matches!(self.peek().kind, TokenKind::Dot) && parts.len() < 3 {
                    self.advance();
                    let (part, _) = self.identifier()?;
                    parts.push(part);
                }
                let end = self
                    .tokens
                    .get(self.pos.saturating_sub(1))
                    .map_or(tok.span, |t| t.span);
                let span = join_span(tok.span, end);
                let mut parts = parts.into_iter();
                let kind = match parts.len() {
                    1 => ExprKind::Column {
                        table: None,
                        catalog: None,
                        name: parts.next().unwrap_or_default(),
                    },
                    2 => ExprKind::Column {
                        catalog: None,
                        table: Some(parts.next().unwrap_or_default()),
                        name: parts.next().unwrap_or_default(),
                    },
                    _ => ExprKind::Column {
                        catalog: Some(parts.next().unwrap_or_default()),
                        table: Some(parts.next().unwrap_or_default()),
                        name: parts.next().unwrap_or_default(),
                    },
                };
                Ok(Expr { kind, span })
            }
            // SQLite treats most keywords as usable function names when
            // followed by `(` (e.g. `replace(...)`, `glob(...)`) — only
            // the handful matched above (CASE/CAST/EXISTS/CURRENT_*)
            // are true reserved words in expression position.
            TokenKind::Keyword(kw) if matches!(self.peek_at(1).kind, TokenKind::LParen) => {
                self.advance();
                self.function_call(format!("{kw:?}"), tok.span)
            }
            TokenKind::LParen => {
                self.advance();
                if self.at_kw(Keyword::SELECT) {
                    let subquery = self.parse_select_stmt()?;
                    if matches!(
                        self.peek().kind,
                        TokenKind::Keyword(Keyword::UNION)
                            | TokenKind::Keyword(Keyword::INTERSECT)
                            | TokenKind::Keyword(Keyword::EXCEPT)
                    ) {
                        return self.unsupported(
                            "compound SELECT (UNION/INTERSECT/EXCEPT) not yet supported",
                        );
                    }
                    let end = self.expect_punct(TokenKind::RParen, "')' to close subquery")?;
                    let span = join_span(tok.span, end);
                    return Ok(Expr {
                        kind: ExprKind::Subquery(Box::new(subquery)),
                        span,
                    });
                }
                let inner = self.expr()?;
                let end = self.expect_punct(TokenKind::RParen, "')' to close expression")?;
                let span = join_span(tok.span, end);
                Ok(Expr {
                    kind: ExprKind::Paren(Box::new(inner)),
                    span,
                })
            }
            other => Err(ParseFail::Invalid {
                message: format!("expected column or expression, found {other:?}"),
                span: tok.span,
            }),
        }
    }

    fn function_call(&mut self, name: String, start: Span) -> PResult<Expr> {
        self.expect_punct(TokenKind::LParen, "'(' after function name")?;
        let distinct = self.eat_kw(Keyword::DISTINCT);
        let args = if self.eat_punct(&TokenKind::Star) {
            FunctionArgs::Star
        } else if matches!(self.peek().kind, TokenKind::RParen) {
            FunctionArgs::List(Vec::new())
        } else {
            FunctionArgs::List(self.expr_list()?)
        };
        let mut end = self.expect_punct(TokenKind::RParen, "')' to close function call")?;
        if self.at_kw(Keyword::OVER) || self.at_kw(Keyword::FILTER) {
            return self.unsupported("window functions (OVER/FILTER) not yet supported");
        }
        let span = join_span(start, {
            end.len = end.len.max(1);
            end
        });
        Ok(Expr {
            kind: ExprKind::FunctionCall {
                name,
                distinct,
                args,
            },
            span,
        })
    }

    fn case_expr(&mut self) -> PResult<Expr> {
        let start = self.advance_span(); // CASE
        let operand = if self.at_kw(Keyword::WHEN) {
            None
        } else {
            Some(Box::new(self.expr()?))
        };
        let mut whens = Vec::new();
        while self.eat_kw(Keyword::WHEN) {
            let cond = self.expr()?;
            self.expect_kw(Keyword::THEN)?;
            let res = self.expr()?;
            whens.push((cond, res));
        }
        if whens.is_empty() {
            return self.invalid("expected WHEN in CASE expression");
        }
        let else_ = if self.eat_kw(Keyword::ELSE) {
            Some(Box::new(self.expr()?))
        } else {
            None
        };
        let end = self.expect_kw(Keyword::END)?;
        Ok(Expr {
            kind: ExprKind::Case {
                operand,
                whens,
                else_,
            },
            span: join_span(start, end),
        })
    }

    fn cast_expr(&mut self) -> PResult<Expr> {
        let start = self.advance_span(); // CAST
        self.expect_punct(TokenKind::LParen, "'(' after CAST")?;
        let inner = self.expr()?;
        self.expect_kw(Keyword::AS)?;
        let type_name = self.type_name()?;
        let end = self.expect_punct(TokenKind::RParen, "')' to close CAST")?;
        Ok(Expr {
            kind: ExprKind::Cast {
                expr: Box::new(inner),
                type_name,
            },
            span: join_span(start, end),
        })
    }

    /// `type-name ::= identifier { identifier } [ "(" NUMBER [ "," NUMBER ] ")" ]`
    fn type_name(&mut self) -> PResult<String> {
        let (first, _) = self.identifier()?;
        let mut parts = vec![first];
        while let TokenKind::Identifier(_) = self.peek().kind {
            let (part, _) = self.identifier()?;
            parts.push(part);
        }
        let mut name = parts.join(" ");
        if self.eat_punct(&TokenKind::LParen) {
            let n1 = self.number_literal()?;
            name.push('(');
            name.push_str(&n1);
            if self.eat_punct(&TokenKind::Comma) {
                let n2 = self.number_literal()?;
                name.push_str(", ");
                name.push_str(&n2);
            }
            self.expect_punct(TokenKind::RParen, "')' to close type size")?;
            name.push(')');
        }
        Ok(name)
    }

    fn number_literal(&mut self) -> PResult<String> {
        match self.peek().kind.clone() {
            TokenKind::Integer(v) => {
                self.advance();
                Ok(v.to_string())
            }
            other => {
                let span = self.peek().span;
                Err(ParseFail::Invalid {
                    message: format!("expected number, found {other:?}"),
                    span,
                })
            }
        }
    }

    fn expr_list(&mut self) -> PResult<Vec<Expr>> {
        let mut list = vec![self.expr()?];
        while self.eat_punct(&TokenKind::Comma) {
            list.push(self.expr()?);
        }
        Ok(list)
    }
}

fn bin(op: BinaryOp, lhs: Expr, rhs: Expr) -> Expr {
    let span = join_span(lhs.span, rhs.span);
    Expr {
        kind: ExprKind::Binary {
            op,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        },
        span,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::tokenizer::Tokenizer;
    use super::*;

    fn parser(sql: &str) -> Parser {
        Parser::new(Tokenizer::tokenize(sql))
    }

    /// #368 tagged MC/DC vector (obligation `grammar_262`, `parse_insert_stmt`'s
    /// decision `self.at_kw(SELECT) || self.at_kw(WITH)`): leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_262__v1_select_source() {
        assert!(parser("INSERT INTO t SELECT * FROM u")
            .parse_insert_stmt()
            .is_ok());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_262`): both leaves
    /// false — neither VALUES/DEFAULT VALUES nor SELECT/WITH follows.
    /// Independence pair for A against
    /// `mcdc__grammar_262__v1_select_source`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_262__v2_neither_select_nor_with() {
        assert!(parser("INSERT INTO t FROM u").parse_insert_stmt().is_err());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_262`): leaf B true,
    /// leaf A false. Independence pair for B against
    /// `mcdc__grammar_262__v2_neither_select_nor_with`. #375 landed
    /// non-recursive `WITH`, so this now parses successfully instead of
    /// erroring out on the (then-)unimplemented WITH clause — the leaf
    /// still exercises the WITH branch of `grammar_262`'s decision, just
    /// via an `Ok` result now.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_262__v3_with_source() {
        assert!(parser("INSERT INTO t WITH x AS (SELECT 1) SELECT 1")
            .parse_insert_stmt()
            .is_ok());
    }

    /// #409: the first VALUES row's end span used to come from
    /// `first_row.last().expect(...)`, now from a safe `map_or` fallback.
    /// Regression guard that the span computation is unchanged for the
    /// common case.
    #[test]
    fn insert_values_single_row_span_ends_at_last_value_expr() {
        let sql = "INSERT INTO t VALUES (1)";
        let insert = parser(sql).parse_insert_stmt().unwrap();
        // `end` comes from the last value expr's span, not the closing paren.
        assert_eq!(insert.span.len, (sql.len() - 1) as u32);
    }

    /// #368 tagged MC/DC vector (obligation `grammar_456`,
    /// `check_no_conflict_clause`'s decision `self.at_kw(ON) &&
    /// matches!(peek_at(1).kind, Keyword(CONFLICT))`): both leaves true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_456__v1_on_conflict() {
        assert!(parser("ON CONFLICT").check_no_conflict_clause().is_err());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_456`): both leaves
    /// false. Independence pair for A against
    /// `mcdc__grammar_456__v1_on_conflict`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_456__v2_no_on() {
        assert!(parser("NOT NULL").check_no_conflict_clause().is_ok());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_456`): leaf A true,
    /// leaf B false — `ON` not followed by `CONFLICT`. Independence pair
    /// for B against `mcdc__grammar_456__v1_on_conflict`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_456__v3_on_but_not_conflict() {
        assert!(parser("ON DELETE").check_no_conflict_clause().is_ok());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_466`,
    /// `parse_create_table_stmt`'s decision `self.at_kw(TEMP) ||
    /// self.at_kw(TEMPORARY)`): leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_466__v1_temp() {
        assert!(parser("CREATE TEMP TABLE t (a)")
            .parse_create_table_stmt()
            .is_err());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_466`): both leaves
    /// false. Independence pair for A against
    /// `mcdc__grammar_466__v1_temp`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_466__v2_neither() {
        assert!(parser("CREATE TABLE t (a INTEGER)")
            .parse_create_table_stmt()
            .is_ok());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_466`): leaf B true,
    /// leaf A false. Independence pair for B against
    /// `mcdc__grammar_466__v2_neither`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_466__v3_temporary() {
        assert!(parser("CREATE TEMPORARY TABLE t (a)")
            .parse_create_table_stmt()
            .is_err());
    }

    /// MC/DC vector (obligation `grammar_785`, `parse_create_view_stmt`'s
    /// decision `self.at_kw(TEMP) || self.at_kw(TEMPORARY)` — same shape
    /// as `grammar_466`'s, at a distinct call site for CREATE VIEW): leaf
    /// A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_785__v1_temp() {
        assert!(parser("CREATE TEMP VIEW v AS SELECT 1")
            .parse_create_view_stmt()
            .is_err());
    }

    /// MC/DC vector (obligation `grammar_785`): both leaves false.
    /// Independence pair for A against `mcdc__grammar_785__v1_temp`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_785__v2_neither() {
        assert!(parser("CREATE VIEW v AS SELECT 1")
            .parse_create_view_stmt()
            .is_ok());
    }

    /// MC/DC vector (obligation `grammar_785`): leaf B true, leaf A
    /// false. Independence pair for B against
    /// `mcdc__grammar_785__v2_neither`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_785__v3_temporary() {
        assert!(parser("CREATE TEMPORARY VIEW v AS SELECT 1")
            .parse_create_view_stmt()
            .is_err());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_599`,
    /// `opt_column_constraint`'s `GENERATED ALWAYS AS` decision, 3
    /// leaves / 4 required vectors): leaf A (`GENERATED`) true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_599__v1_generated() {
        assert!(parser("GENERATED ALWAYS AS (1)")
            .opt_column_constraint()
            .is_err());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_599`): leaves A and
    /// B (`AS`) both false — no recognized constraint at all.
    /// Independence pair for A against
    /// `mcdc__grammar_599__v1_generated`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_599__v2_neither_generated_nor_as() {
        assert_eq!(parser("").opt_column_constraint().unwrap(), None);
    }

    /// #368 tagged MC/DC vector (obligation `grammar_599`): leaf A
    /// false, leaf B true, leaf C (`LParen` follows `AS`) false.
    /// Independence pair for B against `mcdc__grammar_599__v2_neither_generated_nor_as`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_599__v3_as_without_paren() {
        assert_eq!(parser("AS 1").opt_column_constraint().unwrap(), None);
    }

    /// #368 tagged MC/DC vector (obligation `grammar_599`): leaf A
    /// false, leaves B and C both true. Independence pair for C against
    /// `mcdc__grammar_599__v2_neither_generated_nor_as`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_599__v4_as_with_paren() {
        assert!(parser("AS (1)").opt_column_constraint().is_err());
    }

    /// MC/DC vector (obligation `grammar_930`, `parse_pragma_stmt`'s
    /// decision `name.eq_ignore_ascii_case("integrity_check") ||
    /// name.eq_ignore_ascii_case("quick_check")`): leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_930__v1_integrity_check() {
        assert!(parser("PRAGMA integrity_check").parse_pragma_stmt().is_ok());
    }

    /// MC/DC vector (obligation `grammar_930`): both leaves false.
    /// Independence pair for A against
    /// `mcdc__grammar_930__v1_integrity_check`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_930__v2_neither() {
        assert!(parser("PRAGMA journal_mode = WAL")
            .parse_pragma_stmt()
            .is_ok());
    }

    /// MC/DC vector (obligation `grammar_930`): leaf B true, leaf A
    /// false. Independence pair for B against
    /// `mcdc__grammar_930__v2_neither`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_930__v3_quick_check() {
        assert!(parser("PRAGMA quick_check").parse_pragma_stmt().is_ok());
    }

    /// MC/DC vectors (obligation `grammar_1046`, `parse_select_stmt`'s
    /// `WITH ... INSERT/UPDATE/DELETE` decision `with_clause.is_some()
    /// && (self.at_kw(INSERT) || self.at_kw(UPDATE) ||
    /// self.at_kw(DELETE))`, 4 leaves / 5 required vectors): baseline,
    /// all four leaves false (`WITH` present, next token `SELECT`) —
    /// parses cleanly. Distinguishing `ParseFail::Unsupported` (this
    /// decision true) from `ParseFail::Invalid` (false, falling through
    /// to `expect_kw(SELECT)`) matters here: whenever the leading-token
    /// leaf (B/C/D) is true, the statement errors either way — as
    /// `Unsupported` if this decision is true, or as a generic `Invalid`
    /// "expected SELECT" if `with_clause` is `None` — so `is_err()`
    /// alone can't witness any leaf's effect; only the variant can.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1046__v1_with_then_select() {
        assert!(parser("WITH cte AS (SELECT 1) SELECT 1")
            .parse_select_stmt()
            .is_ok());
    }

    /// MC/DC vector (obligation `grammar_1046`): leaf B true, leaf A
    /// (`with_clause.is_some()`) false — no `WITH` at all, so this
    /// falls through to `expect_kw(SELECT)` and fails as `Invalid`
    /// (not `Unsupported`), unlike `mcdc__grammar_1046__v3_with_insert`
    /// below where only leaf A differs.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1046__v2_bare_insert_no_with() {
        assert!(matches!(
            parser("INSERT INTO t VALUES (1)").parse_select_stmt(),
            Err(ParseFail::Invalid { .. })
        ));
    }

    /// MC/DC vector (obligation `grammar_1046`): leaves A and B both
    /// true. Independence pair for A against
    /// `mcdc__grammar_1046__v2_bare_insert_no_with` (only A differs,
    /// `Invalid` -> `Unsupported`) and for B against
    /// `mcdc__grammar_1046__v1_with_then_select` (only B differs, `Ok`
    /// -> `Unsupported`).
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1046__v3_with_insert() {
        assert!(matches!(
            parser("WITH cte AS (SELECT 1) INSERT INTO t VALUES (1)").parse_select_stmt(),
            Err(ParseFail::Unsupported { .. })
        ));
    }

    /// MC/DC vector (obligation `grammar_1046`): leaves A and C both
    /// true. Independence pair for C against
    /// `mcdc__grammar_1046__v1_with_then_select` (only C differs, `Ok`
    /// -> `Unsupported`).
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1046__v4_with_update() {
        assert!(matches!(
            parser("WITH cte AS (SELECT 1) UPDATE t SET x = 1").parse_select_stmt(),
            Err(ParseFail::Unsupported { .. })
        ));
    }

    /// MC/DC vector (obligation `grammar_1046`): leaves A and D both
    /// true. Independence pair for D against
    /// `mcdc__grammar_1046__v1_with_then_select` (only D differs, `Ok`
    /// -> `Unsupported`).
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1046__v5_with_delete() {
        assert!(matches!(
            parser("WITH cte AS (SELECT 1) DELETE FROM t").parse_select_stmt(),
            Err(ParseFail::Unsupported { .. })
        ));
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1105`,
    /// `parse_select_stmt`'s compound-operator decision
    /// `self.at_kw(INTERSECT) || self.at_kw(EXCEPT)`): leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1105__v1_intersect() {
        assert!(parser("SELECT 1 INTERSECT SELECT 2")
            .parse_select_stmt()
            .is_err());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1105`): both leaves
    /// false — a non-compound SELECT. Independence pair for A against
    /// `mcdc__grammar_1105__v1_intersect`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1105__v2_neither() {
        assert!(parser("SELECT 1").parse_select_stmt().is_ok());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1105`): leaf B true,
    /// leaf A false. Independence pair for B against
    /// `mcdc__grammar_1105__v2_neither`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1105__v3_except() {
        assert!(parser("SELECT 1 EXCEPT SELECT 2")
            .parse_select_stmt()
            .is_err());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1131`,
    /// `parse_select_stmt`'s LIMIT-offset decision `self.eat_kw(OFFSET)
    /// || self.eat_punct(Comma)`): leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1131__v1_offset_keyword() {
        assert!(parser("SELECT 1 LIMIT 5 OFFSET 2")
            .parse_select_stmt()
            .is_ok());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1131`): both leaves
    /// false — a LIMIT with no offset at all. Independence pair for A
    /// against `mcdc__grammar_1131__v1_offset_keyword`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1131__v2_no_offset() {
        assert!(parser("SELECT 1 LIMIT 5").parse_select_stmt().is_ok());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1131`): leaf B true,
    /// leaf A false — the comma-form offset. Independence pair for B
    /// against `mcdc__grammar_1131__v2_no_offset`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1131__v3_comma_offset() {
        assert!(parser("SELECT 1 LIMIT 5, 2").parse_select_stmt().is_ok());
    }

    /// MC/DC vector (obligation `grammar_1202`,
    /// `parse_common_table_expr`'s `[NOT] MATERIALIZED` decision
    /// `self.at_kw(MATERIALIZED) || (self.at_kw(NOT) &&
    /// matches!(peek_at(1).kind, Keyword(MATERIALIZED)))`): leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1202__v1_materialized() {
        assert!(parser("cte AS MATERIALIZED (SELECT 1)")
            .parse_common_table_expr()
            .is_err());
    }

    /// MC/DC vector (obligation `grammar_1202`): all three leaves false
    /// (no MATERIALIZED hint at all). Independence pair for A against
    /// `mcdc__grammar_1202__v1_materialized`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1202__v2_neither_materialized_nor_not() {
        assert!(parser("cte AS (SELECT 1)")
            .parse_common_table_expr()
            .is_ok());
    }

    /// MC/DC vector (obligation `grammar_1202`): leaves B and C both
    /// true (`NOT MATERIALIZED`), leaf A false. `at_kw` only peeks the
    /// current token, so B (current == NOT) and C (the *next* token ==
    /// MATERIALIZED) are only jointly reachable together here — with A
    /// pinned false by construction (the current token is NOT, not
    /// MATERIALIZED), there is no reachable input isolating just B or
    /// just C from the other; this vector documents the one reachable
    /// true/true combination rather than claiming an unreachable
    /// independent split (same convention as `grammar_1647`/
    /// `mcdc__grammar_1898__v1_not_in`'s note elsewhere in this file).
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1202__v3_not_materialized() {
        assert!(parser("cte AS NOT MATERIALIZED (SELECT 1)")
            .parse_common_table_expr()
            .is_err());
    }

    /// MC/DC vector (obligation `grammar_1202`): leaf B true, leaf C
    /// false (`NOT` not followed by `MATERIALIZED`), leaf A false. The
    /// overall decision is false here too (same as v2's all-false case)
    /// — `self.expect_punct(LParen)` then fails on the un-consumed `NOT`
    /// token via a distinct, generic parse error, so this and v2 are
    /// not distinguishable purely by `is_err()`; recorded anyway as
    /// reachable-input coverage of the B=true/C=false combination,
    /// same "not independently observable, documented rather than
    /// defeated" spirit as v3 above.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1202__v4_not_without_materialized() {
        assert!(parser("cte AS NOT (SELECT 1)")
            .parse_common_table_expr()
            .is_err());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1304`,
    /// `result_column`'s table-star lookahead
    /// `matches!(peek_at(1).kind, Dot) && matches!(peek_at(2).kind, Star)`):
    /// both leaves true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1304__v1_table_star() {
        assert_eq!(
            parser("t.*").result_column().unwrap(),
            ResultColumn::TableStar {
                table: "t".to_string()
            }
        );
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1304`): leaf A
    /// false — a bare identifier, no dot. Independence pair for A
    /// against `mcdc__grammar_1304__v1_table_star`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1304__v2_no_dot() {
        assert!(matches!(
            parser("t").result_column().unwrap(),
            ResultColumn::Expr { .. }
        ));
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1304`): leaf A
    /// true, leaf B false — `table.column`, not `table.*`. Independence
    /// pair for B against `mcdc__grammar_1304__v1_table_star`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1304__v3_dot_but_not_star() {
        assert!(matches!(
            parser("t.a").result_column().unwrap(),
            ResultColumn::Expr { .. }
        ));
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1555`, `table_ref`'s
    /// `NOT INDEXED` decision `self.at_kw(NOT) &&
    /// matches!(peek_at(1).kind, Keyword(INDEXED))`): both leaves true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1555__v1_not_indexed() {
        assert!(parser("t NOT INDEXED").table_ref().is_err());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1555`): both leaves
    /// false — a plain table reference. Independence pair for A against
    /// `mcdc__grammar_1555__v1_not_indexed`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1555__v2_neither() {
        assert!(parser("t").table_ref().is_ok());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1555`): leaf A
    /// true, leaf B false — `NOT` not followed by `INDEXED`.
    /// Independence pair for B against
    /// `mcdc__grammar_1555__v1_not_indexed`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1555__v3_not_but_not_indexed() {
        assert!(parser("t NOT foo").table_ref().is_ok());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1878`,
    /// `try_tuple_in_subquery`'s entry gate `!matches!(peek().kind,
    /// LParen) || !self.looks_like_tuple_in()`): leaf A true (not even a
    /// paren).
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1878__v1_not_a_paren() {
        assert_eq!(parser("1").try_tuple_in_subquery().unwrap(), None);
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1878`): both leaves
    /// false — a real multi-column tuple-IN. Independence pair for A
    /// against `mcdc__grammar_1878__v1_not_a_paren`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1878__v2_looks_like_tuple_in() {
        assert!(parser("(1, 2) IN (SELECT 1)")
            .try_tuple_in_subquery()
            .unwrap()
            .is_some());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1878`): leaf A
    /// false (a paren), leaf B true — a single-element parenthesized
    /// expression, not a tuple-IN shape. Independence pair for B against
    /// `mcdc__grammar_1878__v2_looks_like_tuple_in`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1878__v3_paren_but_not_tuple_in_shape() {
        assert_eq!(
            parser("(1) IN (SELECT 1)").try_tuple_in_subquery().unwrap(),
            None
        );
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1898`,
    /// `try_tuple_in_subquery`'s `NOT IN` decision `self.at_kw(NOT) &&
    /// matches!(peek_at(1).kind, Keyword(IN))`). Note: `looks_like_tuple_in`'s
    /// own gate (see `grammar_1878`) guarantees that whenever this elif
    /// is reached, both leaves are already true together — there is no
    /// reachable input where they disagree, so all three tagged vectors
    /// exercise the same (only reachable) true/true combination via
    /// distinct call sites, documenting rather than defeating that
    /// invariant.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1898__v1_not_in() {
        assert!(parser("(1, 2) NOT IN (SELECT 1)")
            .try_tuple_in_subquery()
            .unwrap()
            .is_some());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1898`): see
    /// `mcdc__grammar_1898__v1_not_in`'s note — a second, distinct
    /// `NOT IN` call site.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1898__v2_not_in_three_columns() {
        assert!(parser("(1, 2, 3) NOT IN (SELECT 1)")
            .try_tuple_in_subquery()
            .unwrap()
            .is_some());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_1898`): see
    /// `mcdc__grammar_1898__v1_not_in`'s note — a third, distinct
    /// `NOT IN` call site.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_1898__v3_not_in_text_values() {
        assert!(parser("('a', 'b') NOT IN (SELECT 1)")
            .try_tuple_in_subquery()
            .unwrap()
            .is_some());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_2201`,
    /// `primary_expr`'s dotted-identifier-chain decision
    /// `matches!(peek().kind, Dot) && parts.len() < 3`): both leaves
    /// true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_2201__v1_chain_continues() {
        let expr = parser("a.b").primary_expr().unwrap();
        assert!(matches!(
            expr.kind,
            ExprKind::Column { table: Some(t), name, .. } if t == "a" && name == "b"
        ));
    }

    /// #368 tagged MC/DC vector (obligation `grammar_2201`): leaf A
    /// false — no dot at all. Independence pair for A against
    /// `mcdc__grammar_2201__v1_chain_continues`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_2201__v2_no_dot() {
        let expr = parser("a").primary_expr().unwrap();
        assert!(matches!(expr.kind, ExprKind::Column { table: None, name, .. } if name == "a"));
    }

    /// #368 tagged MC/DC vector (obligation `grammar_2201`): leaf A
    /// true, leaf B false — a 4th segment past the 3-part cap.
    /// Independence pair for B against
    /// `mcdc__grammar_2201__v1_chain_continues`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_2201__v3_capped_at_three_parts() {
        let expr = parser("a.b.c.d").primary_expr().unwrap();
        assert!(matches!(
            expr.kind,
            ExprKind::Column { catalog: Some(cat), table: Some(t), name }
                if cat == "a" && t == "b" && name == "c"
        ));
    }

    /// #368 tagged MC/DC vector (obligation `grammar_2286`,
    /// `function_call`'s window-function decision `self.at_kw(OVER) ||
    /// self.at_kw(FILTER)`): leaf A true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_2286__v1_over() {
        assert!(parser("() OVER")
            .function_call(
                "f".to_string(),
                Span {
                    line: 1,
                    column: 1,
                    offset: 0,
                    len: 1,
                }
            )
            .is_err());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_2286`): both leaves
    /// false — an ordinary function call. Independence pair for A
    /// against `mcdc__grammar_2286__v1_over`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_2286__v2_neither() {
        assert!(parser("()")
            .function_call(
                "f".to_string(),
                Span {
                    line: 1,
                    column: 1,
                    offset: 0,
                    len: 1,
                }
            )
            .is_ok());
    }

    /// #368 tagged MC/DC vector (obligation `grammar_2286`): leaf B
    /// true, leaf A false. Independence pair for B against
    /// `mcdc__grammar_2286__v2_neither`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__grammar_2286__v3_filter() {
        assert!(parser("() FILTER")
            .function_call(
                "f".to_string(),
                Span {
                    line: 1,
                    column: 1,
                    offset: 0,
                    len: 1,
                }
            )
            .is_err());
    }
}
