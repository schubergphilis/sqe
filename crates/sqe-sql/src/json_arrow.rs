//! `->` is both the JSON arrow and the lambda arrow (#913).
//!
//! sqlparser's DuckDB dialect reads a bare word followed by `->` as a
//! one-parameter lambda wherever it stands, so `payload -> 'session'` became
//! `Lambda(payload, 'session')` and the planner refused it ("Unsupported ast
//! node in sqltorel: Lambda"). A qualified name (`e.payload -> 'k'`) and
//! `->>` were never affected.
//!
//! The planner therefore parses with [`ArrowExecutionDialect`], which has no
//! lambda syntax, so every `->` is a binary operator, and then
//! [`promote_lambda_arguments`] turns `x -> body` back into a lambda only
//! where one can stand: a later argument (never the first, which is the
//! array or map) of a higher-order function. A function argument is a whole
//! expression, so the promotion is exact: the lambda the DuckDB dialect
//! would have built has the argument as its body, with the `x -> R` at the
//! bottom of its left spine replaced by `R`. `->` binds looser than
//! everything except `NOT`, `AND` and `OR`, so only those, and other
//! operators of the same precedence, can sit above it on that spine.
//!
//! [`ArrowExecutionDialect`]: crate::front_door::ArrowExecutionDialect

use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, LambdaFunction,
    LambdaFunctionParameter, LambdaSyntax, OneOrManyWithParens, Value, VisitMut, VisitorMut,
};

/// Turn `x -> body`, `(x) -> body` and `(a, b) -> body` back into lambdas in
/// the second and later arguments of every function `is_higher_order`
/// names, anywhere in `node` (a statement, or a query inside DataFusion's
/// `COPY` / `EXPLAIN` wrappers). `is_higher_order` gets the planner's name
/// for the function: an unquoted name lowercased (ASCII, as DataFusion
/// does), a quoted one as written. Qualified function names are left alone.
pub fn promote_lambda_arguments<T: VisitMut>(node: &mut T, is_higher_order: &dyn Fn(&str) -> bool) {
    let _ = node.visit(&mut Promoter { is_higher_order });
}

struct Promoter<'a> {
    is_higher_order: &'a dyn Fn(&str) -> bool,
}

impl VisitorMut for Promoter<'_> {
    type Break = ();

    // Pre-order: the promoted lambda's body is then visited too, so a
    // higher-order call inside a lambda is handled.
    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        let [part] = function.name.0.as_slice() else {
            return ControlFlow::Continue(());
        };
        let Some(name) = part.as_ident() else {
            return ControlFlow::Continue(());
        };
        if !(self.is_higher_order)(&planner_name(name)) {
            return ControlFlow::Continue(());
        }
        let FunctionArguments::List(list) = &mut function.args else {
            return ControlFlow::Continue(());
        };
        for arg in list.args.iter_mut().skip(1) {
            let (FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(arg),
                ..
            }
            | FunctionArg::ExprNamed {
                arg: FunctionArgExpr::Expr(arg),
                ..
            }) = arg
            else {
                continue;
            };
            promote(arg);
        }
        ControlFlow::Continue(())
    }
}

fn planner_name(ident: &Ident) -> String {
    match ident.quote_style {
        Some(_) => ident.value.clone(),
        None => ident.value.to_ascii_lowercase(),
    }
}

/// `arg` as the lambda its leading `x ->` starts, if it has one.
fn promote(arg: &mut Expr) {
    let Some(params) = splice(arg) else {
        return;
    };
    let body = std::mem::replace(arg, Expr::value(Value::Null));
    *arg = Expr::Lambda(LambdaFunction {
        params,
        body: Box::new(body),
        syntax: LambdaSyntax::Arrow,
    });
}

/// Replace the `x -> R` at the bottom of `node`'s left spine with `R` and
/// return `x`. `None`, and `node` untouched, when the spine does not end in
/// an arrow from a parameter list.
fn splice(node: &mut Expr) -> Option<OneOrManyWithParens<LambdaFunctionParameter>> {
    let Expr::BinaryOp { left, op, right } = node else {
        return None;
    };
    if matches!(**left, Expr::BinaryOp { .. }) {
        return splice(left);
    }
    if *op != BinaryOperator::Arrow {
        return None;
    }
    let params = parameters(left)?;
    let body = std::mem::replace(right.as_mut(), Expr::value(Value::Null));
    *node = body;
    Some(params)
}

/// `x`, `(x)` or `(a, b)` as lambda parameters.
fn parameters(expr: &Expr) -> Option<OneOrManyWithParens<LambdaFunctionParameter>> {
    let param = |e: &Expr| match e {
        Expr::Identifier(name) => Some(LambdaFunctionParameter {
            name: name.clone(),
            data_type: None,
        }),
        _ => None,
    };
    match expr {
        Expr::Identifier(_) => param(expr).map(OneOrManyWithParens::One),
        Expr::Nested(inner) => param(inner).map(|p| OneOrManyWithParens::Many(vec![p])),
        Expr::Tuple(items) => items
            .iter()
            .map(param)
            .collect::<Option<Vec<_>>>()
            .map(OneOrManyWithParens::Many),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::parser::Parser;

    use sqlparser::ast::Statement;

    use super::*;
    use crate::front_door::{ArrowExecutionDialect, ExecutionDialect};

    const HIGHER_ORDER: &[&str] = &[
        "filter",
        "transform",
        "reduce",
        "any_match",
        "all_match",
        "none_match",
        "zip_with",
    ];

    fn higher_order(name: &str) -> bool {
        HIGHER_ORDER.contains(&name)
    }

    fn promoted(sql: &str) -> Statement {
        let mut statements =
            Parser::parse_sql(&ArrowExecutionDialect, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(statements.len(), 1, "{sql}");
        let mut statement = statements.remove(0);
        promote_lambda_arguments(&mut statement, &higher_order);
        statement
    }

    fn lambda_dialect(sql: &str) -> Statement {
        Parser::parse_sql(&ExecutionDialect, sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .remove(0)
    }

    /// Without lambda syntax, as the parser read these before !649: the
    /// oracle for every JSON arrow.
    fn no_lambdas(sql: &str) -> Statement {
        Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .remove(0)
    }

    fn lambdas(statement: &mut Statement) -> usize {
        struct Count(usize);
        impl VisitorMut for Count {
            type Break = ();
            fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
                self.0 += usize::from(matches!(expr, Expr::Lambda(_)));
                ControlFlow::Continue(())
            }
        }
        let mut count = Count(0);
        let _ = statement.visit(&mut count);
        count.0
    }

    /// The reported shape and every position a JSON arrow on a bare column
    /// can take parse as the parser without lambdas builds them, never as a
    /// lambda. Compared as trees: Display adds no parentheses, so a
    /// mis-nested tree would print the same.
    #[test]
    fn json_arrows_on_bare_columns_stay_json_arrows() {
        for sql in [
            "SELECT payload -> 'session' -> 'device' ->> 'type' AS device_type FROM events",
            "SELECT payload -> 'k' FROM t",
            "SELECT payload -> 0 -> 'k' FROM t",
            "SELECT coalesce(payload -> 'a', payload -> 'b') FROM t",
            "SELECT json_get_str(payload -> 'a', 'b') FROM t",
            "SELECT * FROM t WHERE payload -> 'k' ->> 'x' = 'v'",
            "SELECT * FROM t WHERE payload -> 'k' IS NULL",
            "SELECT * FROM t WHERE a AND payload -> 'k' ->> 'x' = 'v' OR b",
            "SELECT * FROM t WHERE NOT payload -> 'k' OR c",
            "SELECT CASE WHEN payload -> 'k' IS NOT NULL THEN payload ->> 'k' END FROM t",
            "SELECT count(*) FROM t GROUP BY payload -> 'k'",
            "CREATE VIEW v AS SELECT payload -> 's' ->> 't' AS ty FROM t",
            "CREATE TABLE c AS SELECT payload -> 's' ->> 't' AS ty FROM t",
        ] {
            let mut ours = promoted(sql);
            assert_eq!(ours, no_lambdas(sql), "{sql}");
            assert_eq!(lambdas(&mut ours), 0, "{sql}");
        }
    }

    /// Lambdas in higher-order calls come out as the DuckDB dialect builds
    /// them, including a JSON arrow inside a lambda body and a constant
    /// lambda.
    #[test]
    fn lambdas_in_higher_order_calls_are_unchanged() {
        for sql in [
            "SELECT filter(a, x -> x > 1) FROM t",
            "SELECT transform(a, x -> x + 1) FROM t",
            "SELECT transform(a, x -> 2 * x) FROM t",
            "SELECT transform(a, x -> 0) FROM t",
            "SELECT transform(a, n -> 'Mr ' || n) FROM t",
            "SELECT any_match(a, x -> x > 0 AND x < 5) FROM t",
            "SELECT all_match(a, x -> x IS NOT NULL OR x = 0) FROM t",
            "SELECT transform(a, x -> x ->> 'id') FROM t",
            "SELECT reduce(a, 0, (s, x) -> s + x, s -> s) FROM t",
            "SELECT zip_with(a, b, (x, y) -> x * y) FROM t",
            "SELECT transform(filter(a, x -> x > 1), y -> y * 2) FROM t",
            "SELECT transform(a, x -> transform(x, y -> y + 1)) FROM t",
            "SELECT TRANSFORM(a, x -> x + 1) FROM t",
        ] {
            assert_eq!(promoted(sql), lambda_dialect(sql), "{sql}");
        }
    }

    /// A JSON arrow on the parameter inside a lambda body stays a JSON
    /// arrow. The DuckDB dialect got this one wrong too: it read the inner
    /// `x -> 'k'` as a second, nested lambda.
    #[test]
    fn json_arrow_on_a_lambda_parameter_is_not_a_nested_lambda() {
        let sql = "SELECT transform(a, x -> x -> 'k' ->> 'id') FROM t";
        let mut statement = promoted(sql);
        assert_eq!(statement.to_string(), sql);
        assert_eq!(lambdas(&mut statement), 1, "{sql}");
        assert_eq!(lambdas(&mut lambda_dialect(sql)), 2, "{sql}");
    }

    /// A JSON arrow as the array argument of a higher-order call is not a
    /// lambda; the lambda after it is.
    #[test]
    fn first_argument_of_a_higher_order_call_is_never_a_lambda() {
        let sql = "SELECT filter(payload -> 'items', x -> x > 1) FROM t";
        let mut statement = promoted(sql);
        assert_eq!(statement.to_string(), sql);
        assert_eq!(lambdas(&mut statement), 1);
        let Statement::Query(query) = statement else {
            panic!("{sql}");
        };
        let sqlparser::ast::SetExpr::Select(select) = *query.body else {
            panic!("{sql}");
        };
        let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(filter)) = &select.projection[0]
        else {
            panic!("{sql}");
        };
        let FunctionArguments::List(list) = &filter.args else {
            panic!("{sql}");
        };
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(first)) = &list.args[0] else {
            panic!("{sql}");
        };
        let expected = no_lambdas("SELECT payload -> 'items'");
        let Statement::Query(expected) = expected else {
            panic!();
        };
        let sqlparser::ast::SetExpr::Select(expected) = *expected.body else {
            panic!();
        };
        assert_eq!(
            select_item_expr(&expected.projection[0]),
            Some(first),
            "{sql}"
        );
    }

    fn select_item_expr(item: &sqlparser::ast::SelectItem) -> Option<&Expr> {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => Some(expr),
            _ => None,
        }
    }

    /// Only higher-order functions take lambdas: a later argument of any
    /// other function stays a JSON arrow.
    #[test]
    fn later_arguments_of_other_functions_stay_json_arrows() {
        let sql = "SELECT coalesce(a, payload -> 'k') FROM t";
        assert_eq!(promoted(sql), no_lambdas(sql));
    }
}
