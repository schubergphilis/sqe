// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Pretty-printer for the V2 AST, used to verify the roundtrip
//! requirement (spec 002-parser Requirement 3: "parse -> print -> parse
//! gives identical AST"). Always emits explicit parentheses around
//! `ExprKind::Paren` nodes and normalizes whitespace/casing, so printer
//! output is not expected to match the original source text verbatim —
//! only to reparse to the same AST.

use super::ast::*;
use std::fmt;

impl fmt::Display for WithClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WITH ")?;
        for (i, cte) in self.ctes.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{cte}")?;
        }
        Ok(())
    }
}

impl fmt::Display for CommonTableExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(columns) = &self.columns {
            write!(f, " (")?;
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{col}")?;
            }
            write!(f, ")")?;
        }
        write!(f, " AS ({})", self.query)?;
        Ok(())
    }
}

impl fmt::Display for Select {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(with_clause) = &self.with_clause {
            write!(f, "{with_clause} ")?;
        }
        write!(f, "SELECT")?;
        match self.distinct {
            Some(Distinctness::Distinct) => write!(f, " DISTINCT")?,
            Some(Distinctness::All) => write!(f, " ALL")?,
            None => {}
        }
        for (i, col) in self.columns.iter().enumerate() {
            if i == 0 {
                write!(f, " ")?;
            } else {
                write!(f, ", ")?;
            }
            write!(f, "{col}")?;
        }
        if let Some(from) = &self.from {
            write!(f, " FROM {from}")?;
        }
        if let Some(w) = &self.where_clause {
            write!(f, " WHERE {w}")?;
        }
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY ")?;
            for (i, expr) in self.group_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{expr}")?;
            }
        }
        if let Some(having) = &self.having {
            write!(f, " HAVING {having}")?;
        }
        for arm in &self.compound {
            write!(f, " {arm}")?;
        }
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY ")?;
            for (i, term) in self.order_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{term}")?;
            }
        }
        if let Some(limit) = &self.limit {
            write!(f, " LIMIT {}", limit.limit)?;
            if let Some(offset) = &limit.offset {
                write!(f, " OFFSET {offset}")?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for CompoundSelect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.op {
            CompoundOp::UnionAll => write!(f, "UNION ALL SELECT")?,
            CompoundOp::Union => write!(f, "UNION SELECT")?,
        }
        match self.distinct {
            Some(Distinctness::Distinct) => write!(f, " DISTINCT")?,
            Some(Distinctness::All) => write!(f, " ALL")?,
            None => {}
        }
        for (i, col) in self.columns.iter().enumerate() {
            if i == 0 {
                write!(f, " ")?;
            } else {
                write!(f, ", ")?;
            }
            write!(f, "{col}")?;
        }
        if let Some(from) = &self.from {
            write!(f, " FROM {from}")?;
        }
        if let Some(w) = &self.where_clause {
            write!(f, " WHERE {w}")?;
        }
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY ")?;
            for (i, expr) in self.group_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{expr}")?;
            }
        }
        if let Some(having) = &self.having {
            write!(f, " HAVING {having}")?;
        }
        Ok(())
    }
}

impl fmt::Display for ResultColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResultColumn::Star => write!(f, "*"),
            ResultColumn::TableStar { table } => write!(f, "{table}.*"),
            ResultColumn::Expr { expr, alias } => {
                write!(f, "{expr}")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            TableRefKind::Name(name) => write!(f, "{name}")?,
            TableRefKind::Subquery(select) => write!(f, "({select})")?,
        }
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

impl fmt::Display for FromClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.first)?;
        for join in &self.joins {
            write!(f, " {join}")?;
        }
        Ok(())
    }
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op = match self.op {
            JoinOp::Inner => "JOIN",
            JoinOp::Left => "LEFT JOIN",
            JoinOp::Cross => "CROSS JOIN",
            JoinOp::Right => "RIGHT JOIN",
            JoinOp::Full => "FULL JOIN",
        };
        if self.natural {
            write!(f, "NATURAL ")?;
        }
        write!(f, "{op} {}", self.table)?;
        match &self.constraint {
            Some(JoinConstraint::On(expr)) => write!(f, " ON {expr}")?,
            Some(JoinConstraint::Using(cols)) => write!(f, " USING ({})", cols.join(", "))?,
            None => {}
        }
        Ok(())
    }
}

impl fmt::Display for OrderingTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        match self.desc {
            Some(false) => write!(f, " ASC")?,
            Some(true) => write!(f, " DESC")?,
            None => {}
        }
        match self.nulls_last {
            Some(false) => write!(f, " NULLS FIRST")?,
            Some(true) => write!(f, " NULLS LAST")?,
            None => {}
        }
        Ok(())
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ExprKind::Literal(lit) => write!(f, "{lit}"),
            ExprKind::Param(p) => write!(f, "{p}"),
            ExprKind::Column {
                catalog,
                table,
                name,
            } => {
                if let Some(catalog) = catalog {
                    write!(f, "{catalog}.")?;
                }
                if let Some(table) = table {
                    write!(f, "{table}.")?;
                }
                write!(f, "{name}")
            }
            ExprKind::FunctionCall {
                name,
                distinct,
                args,
            } => {
                write!(f, "{name}(")?;
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                match args {
                    FunctionArgs::Star => write!(f, "*")?,
                    FunctionArgs::List(list) => {
                        for (i, a) in list.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{a}")?;
                        }
                    }
                }
                write!(f, ")")
            }
            ExprKind::Unary { op, expr } => {
                let op = match op {
                    UnaryOp::Not => "NOT ",
                    UnaryOp::Plus => "+",
                    UnaryOp::Minus => "-",
                    UnaryOp::BitNot => "~",
                };
                write!(f, "{op}{expr}")
            }
            ExprKind::Binary { op, lhs, rhs } => {
                write!(f, "{lhs} {} {rhs}", binop_str(*op))
            }
            ExprKind::Is { lhs, rhs, negated } => {
                write!(f, "{lhs} IS {}{rhs}", if *negated { "NOT " } else { "" })
            }
            ExprKind::IsNull { expr, negated } => {
                write!(f, "{expr} {}", if *negated { "NOTNULL" } else { "ISNULL" })
            }
            ExprKind::Between {
                expr,
                lo,
                hi,
                negated,
            } => write!(
                f,
                "{expr} {}BETWEEN {lo} AND {hi}",
                if *negated { "NOT " } else { "" }
            ),
            ExprKind::In {
                expr,
                list,
                negated,
            } => {
                write!(f, "{expr} {}IN (", if *negated { "NOT " } else { "" })?;
                for (i, e) in list.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{e}")?;
                }
                write!(f, ")")
            }
            ExprKind::Like {
                expr,
                pattern,
                glob,
                negated,
                escape,
            } => {
                let op = if *glob { "GLOB" } else { "LIKE" };
                write!(
                    f,
                    "{expr} {}{op} {pattern}",
                    if *negated { "NOT " } else { "" }
                )?;
                if let Some(escape) = escape {
                    write!(f, " ESCAPE {escape}")?;
                }
                Ok(())
            }
            ExprKind::Case {
                operand,
                whens,
                else_,
            } => {
                write!(f, "CASE")?;
                if let Some(operand) = operand {
                    write!(f, " {operand}")?;
                }
                for (cond, res) in whens {
                    write!(f, " WHEN {cond} THEN {res}")?;
                }
                if let Some(else_) = else_ {
                    write!(f, " ELSE {else_}")?;
                }
                write!(f, " END")
            }
            ExprKind::Cast { expr, type_name } => write!(f, "CAST({expr} AS {type_name})"),
            ExprKind::Collate { expr, collation } => write!(f, "{expr} COLLATE {collation}"),
            ExprKind::Paren(inner) => write!(f, "({inner})"),
            ExprKind::Subquery(select) => write!(f, "({select})"),
            ExprKind::Exists { subquery, negated } => {
                if *negated {
                    write!(f, "NOT EXISTS ({subquery})")
                } else {
                    write!(f, "EXISTS ({subquery})")
                }
            }
            ExprKind::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                if *negated {
                    write!(f, "{expr} NOT IN ({subquery})")
                } else {
                    write!(f, "{expr} IN ({subquery})")
                }
            }
            ExprKind::InSubqueryMulti {
                exprs,
                subquery,
                negated,
            } => {
                write!(f, "(")?;
                for (i, e) in exprs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{e}")?;
                }
                write!(f, ")")?;
                if *negated {
                    write!(f, " NOT IN ({subquery})")
                } else {
                    write!(f, " IN ({subquery})")
                }
            }
        }
    }
}

fn binop_str(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Or => "OR",
        BinaryOp::And => "AND",
        BinaryOp::Eq => "=",
        BinaryOp::Ne => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::Le => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Ge => ">=",
        BinaryOp::BitAnd => "&",
        BinaryOp::BitOr => "|",
        BinaryOp::Shl => "<<",
        BinaryOp::Shr => ">>",
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Concat => "||",
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Integer(v) => write!(f, "{v}"),
            Literal::Float(v) => write!(f, "{v}"),
            Literal::Str(s) => write!(f, "'{}'", s.replace('\'', "''")),
            Literal::Blob(bytes) => {
                write!(f, "X'")?;
                for b in bytes {
                    write!(f, "{b:02X}")?;
                }
                write!(f, "'")
            }
            Literal::Null => write!(f, "NULL"),
            Literal::True => write!(f, "TRUE"),
            Literal::False => write!(f, "FALSE"),
        }
    }
}

impl fmt::Display for Insert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "INSERT")?;
        if let Some(action) = self.or_action {
            let action = match action {
                ConflictAction::Replace => "REPLACE",
                ConflictAction::Ignore => "IGNORE",
                ConflictAction::Abort => "ABORT",
                ConflictAction::Rollback => "ROLLBACK",
                ConflictAction::Fail => "FAIL",
            };
            write!(f, " OR {action}")?;
        }
        write!(f, " INTO {}", self.table)?;
        if let Some(columns) = &self.columns {
            write!(f, " (")?;
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{col}")?;
            }
            write!(f, ")")?;
        }
        match &self.source {
            InsertSource::DefaultValues => write!(f, " DEFAULT VALUES"),
            InsertSource::Values(rows) => {
                write!(f, " VALUES ")?;
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    for (j, expr) in row.iter().enumerate() {
                        if j > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{expr}")?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
            InsertSource::Select(select) => write!(f, " {select}"),
        }
    }
}

impl fmt::Display for Delete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DELETE FROM {}", self.table)?;
        if let Some(w) = &self.where_clause {
            write!(f, " WHERE {w}")?;
        }
        Ok(())
    }
}

impl fmt::Display for CreateTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TABLE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} (", self.name)?;
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{col}")?;
        }
        for constraint in &self.constraints {
            write!(f, ", {constraint}")?;
        }
        write!(f, ")")?;
        if self.without_rowid {
            write!(f, " WITHOUT ROWID")?;
        } else if self.strict {
            write!(f, " STRICT")?;
        }
        Ok(())
    }
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(type_name) = &self.type_name {
            write!(f, " {type_name}")?;
        }
        for constraint in &self.constraints {
            write!(f, " {constraint}")?;
        }
        Ok(())
    }
}

impl fmt::Display for ColumnConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnConstraint::NotNull => write!(f, "NOT NULL"),
            ColumnConstraint::PrimaryKey {
                desc,
                autoincrement,
            } => {
                write!(f, "PRIMARY KEY")?;
                match desc {
                    Some(false) => write!(f, " ASC")?,
                    Some(true) => write!(f, " DESC")?,
                    None => {}
                }
                if *autoincrement {
                    write!(f, " AUTOINCREMENT")?;
                }
                Ok(())
            }
            ColumnConstraint::Unique => write!(f, "UNIQUE"),
            ColumnConstraint::Default(value) => write!(f, "DEFAULT {value}"),
            ColumnConstraint::Check(expr) => write!(f, "CHECK ({expr})"),
            ColumnConstraint::Collate(name) => write!(f, "COLLATE {name}"),
        }
    }
}

impl fmt::Display for DefaultValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DefaultValue::Literal(expr) => write!(f, "{expr}"),
            DefaultValue::Paren(expr) => write!(f, "({expr})"),
        }
    }
}

impl fmt::Display for TableConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableConstraint::PrimaryKey(cols) => {
                write!(f, "PRIMARY KEY (")?;
                write_indexed_columns(f, cols)?;
                write!(f, ")")
            }
            TableConstraint::Unique(cols) => {
                write!(f, "UNIQUE (")?;
                write_indexed_columns(f, cols)?;
                write!(f, ")")
            }
            TableConstraint::Check(expr) => write!(f, "CHECK ({expr})"),
        }
    }
}

fn write_indexed_columns(f: &mut fmt::Formatter<'_>, cols: &[IndexedColumn]) -> fmt::Result {
    for (i, col) in cols.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{col}")?;
    }
    Ok(())
}

impl fmt::Display for IndexedColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        match self.desc {
            Some(false) => write!(f, " ASC")?,
            Some(true) => write!(f, " DESC")?,
            None => {}
        }
        Ok(())
    }
}

impl fmt::Display for CreateIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE ")?;
        if self.unique {
            write!(f, "UNIQUE ")?;
        }
        write!(f, "INDEX ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} ON {} (", self.name, self.table)?;
        write_indexed_columns(f, &self.columns)?;
        write!(f, ")")?;
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        Ok(())
    }
}

impl fmt::Display for CreateView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE VIEW ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        if let Some(columns) = &self.columns {
            write!(f, " (")?;
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{col}")?;
            }
            write!(f, ")")?;
        }
        write!(f, " AS {}", self.query)
    }
}

impl fmt::Display for DropView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP VIEW ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)
    }
}

impl fmt::Display for DropTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP TABLE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)
    }
}

impl fmt::Display for DropIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP INDEX ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)
    }
}

impl fmt::Display for TransactionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TransactionMode::Deferred => "DEFERRED",
            TransactionMode::Immediate => "IMMEDIATE",
            TransactionMode::Exclusive => "EXCLUSIVE",
        };
        write!(f, "{s}")
    }
}

impl fmt::Display for Begin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BEGIN")?;
        if let Some(mode) = self.mode {
            write!(f, " {mode}")?;
        }
        Ok(())
    }
}

impl fmt::Display for Commit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "COMMIT")
    }
}

impl fmt::Display for Rollback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ROLLBACK")
    }
}

impl fmt::Display for ParamKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParamKind::Anonymous => write!(f, "?"),
            ParamKind::Numbered(n) => write!(f, "?{n}"),
            ParamKind::Colon(s) => write!(f, ":{s}"),
            ParamKind::At(s) => write!(f, "@{s}"),
            ParamKind::Dollar(s) => write!(f, "${s}"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::super::error::{
        parse_begin, parse_commit, parse_create_index, parse_create_table, parse_create_view,
        parse_delete, parse_drop_index, parse_drop_table, parse_drop_view, parse_insert,
        parse_rollback, parse_select, ParseOutcome,
    };

    fn ok_select(sql: &str) -> String {
        match parse_select(sql) {
            ParseOutcome::Accepted(select) => select.to_string(),
            other => panic!("expected accepted select, got {other:?}"),
        }
    }

    #[test]
    fn select_distinct_all_group_having_order_limit_offset() {
        assert_eq!(
            ok_select(
                "SELECT DISTINCT a, b FROM t WHERE a > 1 GROUP BY a HAVING b < 2 \
                 ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST LIMIT 5 OFFSET 10"
            ),
            "SELECT DISTINCT a, b FROM t WHERE a > 1 GROUP BY a HAVING b < 2 \
             ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST LIMIT 5 OFFSET 10"
        );
        assert_eq!(ok_select("SELECT ALL a FROM t"), "SELECT ALL a FROM t");
    }

    #[test]
    fn select_with_clause_and_cte_columns() {
        assert_eq!(
            ok_select("WITH x (a, b) AS (SELECT 1, 2), y AS (SELECT 3) SELECT * FROM x"),
            "WITH x (a, b) AS (SELECT 1, 2), y AS (SELECT 3) SELECT * FROM x"
        );
    }

    #[test]
    fn compound_select_union_and_union_all() {
        assert_eq!(
            ok_select("SELECT a FROM t UNION SELECT b FROM u"),
            "SELECT a FROM t UNION SELECT b FROM u"
        );
        assert_eq!(
            ok_select("SELECT a FROM t UNION ALL SELECT DISTINCT b FROM u WHERE b > 0 GROUP BY b HAVING b < 9"),
            "SELECT a FROM t UNION ALL SELECT DISTINCT b FROM u WHERE b > 0 GROUP BY b HAVING b < 9"
        );
    }

    #[test]
    fn result_column_star_table_star_and_alias() {
        assert_eq!(ok_select("SELECT * FROM t"), "SELECT * FROM t");
        assert_eq!(ok_select("SELECT t.* FROM t"), "SELECT t.* FROM t");
        assert_eq!(ok_select("SELECT a AS x FROM t"), "SELECT a AS x FROM t");
    }

    #[test]
    fn from_clause_subquery_alias_and_joins() {
        assert_eq!(
            ok_select("SELECT * FROM (SELECT 1) AS sub"),
            "SELECT * FROM (SELECT 1) AS sub"
        );
        assert_eq!(
            ok_select("SELECT * FROM a NATURAL JOIN b"),
            "SELECT * FROM a NATURAL JOIN b"
        );
        assert_eq!(
            ok_select("SELECT * FROM a LEFT JOIN b ON a.x = b.x"),
            "SELECT * FROM a LEFT JOIN b ON a.x = b.x"
        );
        assert_eq!(
            ok_select("SELECT * FROM a CROSS JOIN b USING (x)"),
            "SELECT * FROM a CROSS JOIN b USING (x)"
        );
        assert_eq!(
            ok_select("SELECT * FROM a RIGHT JOIN b ON a.x = b.x"),
            "SELECT * FROM a RIGHT JOIN b ON a.x = b.x"
        );
        assert_eq!(
            ok_select("SELECT * FROM a FULL JOIN b ON a.x = b.x"),
            "SELECT * FROM a FULL JOIN b ON a.x = b.x"
        );
        assert_eq!(
            ok_select("SELECT * FROM a JOIN b ON a.x = b.x"),
            "SELECT * FROM a JOIN b ON a.x = b.x"
        );
    }

    #[test]
    fn expr_display_covers_all_kinds() {
        assert_eq!(ok_select("SELECT 1"), "SELECT 1");
        assert_eq!(ok_select("SELECT ?"), "SELECT ?");
        assert_eq!(ok_select("SELECT ?1"), "SELECT ?1");
        assert_eq!(ok_select("SELECT :name"), "SELECT :name");
        assert_eq!(ok_select("SELECT @name"), "SELECT @name");
        assert_eq!(ok_select("SELECT $name"), "SELECT $name");
        assert_eq!(ok_select("SELECT db.tbl.col"), "SELECT db.tbl.col");
        assert_eq!(ok_select("SELECT tbl.col"), "SELECT tbl.col");
        assert_eq!(ok_select("SELECT col"), "SELECT col");
        assert_eq!(
            ok_select("SELECT count(DISTINCT a, b)"),
            "SELECT count(DISTINCT a, b)"
        );
        assert_eq!(ok_select("SELECT count(*)"), "SELECT count(*)");
        assert_eq!(ok_select("SELECT NOT a"), "SELECT NOT a");
        assert_eq!(ok_select("SELECT +a"), "SELECT +a");
        assert_eq!(ok_select("SELECT -a"), "SELECT -a");
        assert_eq!(ok_select("SELECT ~a"), "SELECT ~a");
        assert_eq!(ok_select("SELECT a AND b"), "SELECT a AND b");
        assert_eq!(ok_select("SELECT a OR b"), "SELECT a OR b");
        assert_eq!(ok_select("SELECT a = b"), "SELECT a = b");
        assert_eq!(ok_select("SELECT a != b"), "SELECT a != b");
        assert_eq!(ok_select("SELECT a < b"), "SELECT a < b");
        assert_eq!(ok_select("SELECT a <= b"), "SELECT a <= b");
        assert_eq!(ok_select("SELECT a > b"), "SELECT a > b");
        assert_eq!(ok_select("SELECT a >= b"), "SELECT a >= b");
        assert_eq!(ok_select("SELECT a & b"), "SELECT a & b");
        assert_eq!(ok_select("SELECT a | b"), "SELECT a | b");
        assert_eq!(ok_select("SELECT a << b"), "SELECT a << b");
        assert_eq!(ok_select("SELECT a >> b"), "SELECT a >> b");
        assert_eq!(ok_select("SELECT a + b"), "SELECT a + b");
        assert_eq!(ok_select("SELECT a - b"), "SELECT a - b");
        assert_eq!(ok_select("SELECT a * b"), "SELECT a * b");
        assert_eq!(ok_select("SELECT a / b"), "SELECT a / b");
        assert_eq!(ok_select("SELECT a % b"), "SELECT a % b");
        assert_eq!(ok_select("SELECT a || b"), "SELECT a || b");
        assert_eq!(ok_select("SELECT a IS b"), "SELECT a IS b");
        assert_eq!(ok_select("SELECT a IS NOT b"), "SELECT a IS NOT b");
        assert_eq!(ok_select("SELECT a ISNULL"), "SELECT a ISNULL");
        assert_eq!(ok_select("SELECT a NOTNULL"), "SELECT a NOTNULL");
        assert_eq!(
            ok_select("SELECT a BETWEEN 1 AND 2"),
            "SELECT a BETWEEN 1 AND 2"
        );
        assert_eq!(
            ok_select("SELECT a NOT BETWEEN 1 AND 2"),
            "SELECT a NOT BETWEEN 1 AND 2"
        );
        assert_eq!(ok_select("SELECT a IN (1, 2)"), "SELECT a IN (1, 2)");
        assert_eq!(
            ok_select("SELECT a NOT IN (1, 2)"),
            "SELECT a NOT IN (1, 2)"
        );
        assert_eq!(ok_select("SELECT a LIKE 'x'"), "SELECT a LIKE 'x'");
        assert_eq!(ok_select("SELECT a NOT LIKE 'x'"), "SELECT a NOT LIKE 'x'");
        assert_eq!(ok_select("SELECT a GLOB 'x'"), "SELECT a GLOB 'x'");
        assert_eq!(
            ok_select("SELECT a LIKE 'x' ESCAPE '\\'"),
            "SELECT a LIKE 'x' ESCAPE '\\'"
        );
        assert_eq!(
            ok_select("SELECT CASE a WHEN 1 THEN 'x' ELSE 'y' END"),
            "SELECT CASE a WHEN 1 THEN 'x' ELSE 'y' END"
        );
        assert_eq!(
            ok_select("SELECT CASE WHEN a THEN 'x' END"),
            "SELECT CASE WHEN a THEN 'x' END"
        );
        assert_eq!(
            ok_select("SELECT CAST(a AS INTEGER)"),
            "SELECT CAST(a AS INTEGER)"
        );
        assert_eq!(ok_select("SELECT a COLLATE bin"), "SELECT a COLLATE bin");
        assert_eq!(ok_select("SELECT (a)"), "SELECT (a)");
        assert_eq!(ok_select("SELECT (SELECT 1)"), "SELECT (SELECT 1)");
        assert_eq!(
            ok_select("SELECT EXISTS (SELECT 1)"),
            "SELECT EXISTS (SELECT 1)"
        );
        assert_eq!(
            ok_select("SELECT NOT EXISTS (SELECT 1)"),
            "SELECT NOT EXISTS (SELECT 1)"
        );
        assert_eq!(
            ok_select("SELECT a IN (SELECT 1)"),
            "SELECT a IN (SELECT 1)"
        );
        assert_eq!(
            ok_select("SELECT a NOT IN (SELECT 1)"),
            "SELECT a NOT IN (SELECT 1)"
        );
        assert_eq!(
            ok_select("SELECT (a, b) IN (SELECT 1, 2)"),
            "SELECT (a, b) IN (SELECT 1, 2)"
        );
        assert_eq!(
            ok_select("SELECT (a, b) NOT IN (SELECT 1, 2)"),
            "SELECT (a, b) NOT IN (SELECT 1, 2)"
        );
    }

    #[test]
    fn literal_display_covers_all_kinds() {
        assert_eq!(ok_select("SELECT 1.5"), "SELECT 1.5");
        assert_eq!(ok_select("SELECT 'it''s'"), "SELECT 'it''s'");
        assert_eq!(ok_select("SELECT x'AB01'"), "SELECT X'AB01'");
        assert_eq!(ok_select("SELECT NULL"), "SELECT NULL");
        assert_eq!(ok_select("SELECT TRUE"), "SELECT TRUE");
        assert_eq!(ok_select("SELECT FALSE"), "SELECT FALSE");
    }

    #[test]
    fn insert_variants() {
        match parse_insert("INSERT OR REPLACE INTO t (a, b) VALUES (1, 2), (3, 4)") {
            ParseOutcome::Accepted(insert) => assert_eq!(
                insert.to_string(),
                "INSERT OR REPLACE INTO t (a, b) VALUES (1, 2), (3, 4)"
            ),
            other => panic!("{other:?}"),
        }
        for (action, sql) in [
            ("IGNORE", "INSERT OR IGNORE INTO t DEFAULT VALUES"),
            ("ABORT", "INSERT OR ABORT INTO t DEFAULT VALUES"),
            ("ROLLBACK", "INSERT OR ROLLBACK INTO t DEFAULT VALUES"),
            ("FAIL", "INSERT OR FAIL INTO t DEFAULT VALUES"),
        ] {
            match parse_insert(sql) {
                ParseOutcome::Accepted(insert) => {
                    assert!(insert.to_string().contains(action));
                    assert!(insert.to_string().contains("DEFAULT VALUES"));
                }
                other => panic!("{other:?}"),
            }
        }
        match parse_insert("INSERT INTO t SELECT * FROM u") {
            ParseOutcome::Accepted(insert) => {
                assert_eq!(insert.to_string(), "INSERT INTO t SELECT * FROM u");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn delete_display() {
        match parse_delete("DELETE FROM t WHERE a = 1") {
            ParseOutcome::Accepted(delete) => {
                assert_eq!(delete.to_string(), "DELETE FROM t WHERE a = 1");
            }
            other => panic!("{other:?}"),
        }
        match parse_delete("DELETE FROM t") {
            ParseOutcome::Accepted(delete) => assert_eq!(delete.to_string(), "DELETE FROM t"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn create_table_variants() {
        match parse_create_table(
            "CREATE TABLE IF NOT EXISTS t (a INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE DEFAULT (1) CHECK (a > 0) COLLATE bin, UNIQUE (a, b), PRIMARY KEY (a), CHECK (a > 0)) WITHOUT ROWID",
        ) {
            ParseOutcome::Accepted(ct) => {
                let s = ct.to_string();
                assert!(s.starts_with("CREATE TABLE IF NOT EXISTS t ("));
                assert!(s.contains("PRIMARY KEY ASC AUTOINCREMENT"));
                assert!(s.contains("NOT NULL"));
                assert!(s.contains("UNIQUE"));
                assert!(s.contains("DEFAULT (1)"));
                assert!(s.contains("CHECK (a > 0)"));
                assert!(s.contains("COLLATE bin"));
                assert!(s.contains("WITHOUT ROWID"));
            }
            other => panic!("{other:?}"),
        }
        match parse_create_table("CREATE TABLE t (a INTEGER, b TEXT) STRICT") {
            ParseOutcome::Accepted(ct) => {
                assert_eq!(ct.to_string(), "CREATE TABLE t (a INTEGER, b TEXT) STRICT");
            }
            other => panic!("{other:?}"),
        }
        match parse_create_table("CREATE TABLE t (a, b PRIMARY KEY DESC)") {
            ParseOutcome::Accepted(ct) => {
                assert_eq!(ct.to_string(), "CREATE TABLE t (a, b PRIMARY KEY DESC)");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn create_index_and_drop_variants() {
        match parse_create_index(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx ON t (a ASC, b DESC) WHERE a > 0",
        ) {
            ParseOutcome::Accepted(ci) => assert_eq!(
                ci.to_string(),
                "CREATE UNIQUE INDEX IF NOT EXISTS idx ON t (a ASC, b DESC) WHERE a > 0"
            ),
            other => panic!("{other:?}"),
        }
        match parse_create_index("CREATE INDEX idx ON t (a)") {
            ParseOutcome::Accepted(ci) => {
                assert_eq!(ci.to_string(), "CREATE INDEX idx ON t (a)");
            }
            other => panic!("{other:?}"),
        }
        match parse_drop_index("DROP INDEX IF EXISTS idx") {
            ParseOutcome::Accepted(di) => {
                assert_eq!(di.to_string(), "DROP INDEX IF EXISTS idx");
            }
            other => panic!("{other:?}"),
        }
        match parse_drop_index("DROP INDEX idx") {
            ParseOutcome::Accepted(di) => assert_eq!(di.to_string(), "DROP INDEX idx"),
            other => panic!("{other:?}"),
        }
        match parse_drop_table("DROP TABLE IF EXISTS t") {
            ParseOutcome::Accepted(dt) => assert_eq!(dt.to_string(), "DROP TABLE IF EXISTS t"),
            other => panic!("{other:?}"),
        }
        match parse_drop_table("DROP TABLE t") {
            ParseOutcome::Accepted(dt) => assert_eq!(dt.to_string(), "DROP TABLE t"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn create_view_and_drop_view_variants() {
        match parse_create_view("CREATE VIEW IF NOT EXISTS v (a, b) AS SELECT 1, 2") {
            ParseOutcome::Accepted(cv) => assert_eq!(
                cv.to_string(),
                "CREATE VIEW IF NOT EXISTS v (a, b) AS SELECT 1, 2"
            ),
            other => panic!("{other:?}"),
        }
        match parse_create_view("CREATE VIEW v AS SELECT 1") {
            ParseOutcome::Accepted(cv) => {
                assert_eq!(cv.to_string(), "CREATE VIEW v AS SELECT 1");
            }
            other => panic!("{other:?}"),
        }
        match parse_drop_view("DROP VIEW IF EXISTS v") {
            ParseOutcome::Accepted(dv) => assert_eq!(dv.to_string(), "DROP VIEW IF EXISTS v"),
            other => panic!("{other:?}"),
        }
        match parse_drop_view("DROP VIEW v") {
            ParseOutcome::Accepted(dv) => assert_eq!(dv.to_string(), "DROP VIEW v"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn transaction_statements() {
        match parse_begin("BEGIN") {
            ParseOutcome::Accepted(b) => assert_eq!(b.to_string(), "BEGIN"),
            other => panic!("{other:?}"),
        }
        for (mode, sql) in [
            ("DEFERRED", "BEGIN DEFERRED"),
            ("IMMEDIATE", "BEGIN IMMEDIATE"),
            ("EXCLUSIVE", "BEGIN EXCLUSIVE"),
        ] {
            match parse_begin(sql) {
                ParseOutcome::Accepted(b) => assert_eq!(b.to_string(), format!("BEGIN {mode}")),
                other => panic!("{other:?}"),
            }
        }
        match parse_commit("COMMIT") {
            ParseOutcome::Accepted(c) => assert_eq!(c.to_string(), "COMMIT"),
            other => panic!("{other:?}"),
        }
        match parse_rollback("ROLLBACK") {
            ParseOutcome::Accepted(r) => assert_eq!(r.to_string(), "ROLLBACK"),
            other => panic!("{other:?}"),
        }
    }
}
