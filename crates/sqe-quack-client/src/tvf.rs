//! Table-valued function `quack_query(uri, [token,] sql)` for DataFusion.
//!
//! Symmetric to DuckDB's `quack_query()` built-in: any SQL session that
//! registers this TVF can pull data from a remote Quack endpoint inline.
//!
//! Two arity variants are accepted:
//!
//! ```sql
//! SELECT * FROM quack_query('quack:host:9495', 'SELECT 42');
//! SELECT * FROM quack_query('quack:host:9495', 'token', 'SELECT 42');
//! ```
//!
//! The 2-arg form sends an empty auth string; use the 3-arg form when the
//! remote server requires a bearer token. `uri` accepts the same shapes as
//! [`crate::QuackClient::connect`].

use std::sync::Arc;

use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::scalar::ScalarValue;
use datafusion_expr::Expr;

use crate::QuackTableProvider;

/// `quack_query()` TVF impl. The struct itself is stateless; per-call config
/// (URI + SQL + optional token) comes from the `Expr` args.
#[derive(Debug, Default)]
pub struct QuackQueryTvf;

impl QuackQueryTvf {
    pub fn new() -> Self {
        Self
    }
}

impl TableFunctionImpl for QuackQueryTvf {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let (uri, token, sql) = match exprs.len() {
            2 => {
                let uri = extract_string_arg("quack_query", exprs, 0, "uri")?;
                let sql = extract_string_arg("quack_query", exprs, 1, "sql")?;
                (uri, None, sql)
            }
            3 => {
                let uri = extract_string_arg("quack_query", exprs, 0, "uri")?;
                let token = extract_string_arg("quack_query", exprs, 1, "token")?;
                let sql = extract_string_arg("quack_query", exprs, 2, "sql")?;
                let token = if token.is_empty() { None } else { Some(token) };
                (uri, token, sql)
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "quack_query expects 2 args (uri, sql) or 3 args (uri, token, sql); got {other}"
                )));
            }
        };

        let provider = QuackTableProvider::new(&uri, token.as_deref(), &sql).map_err(|e| {
            DataFusionError::Execution(format!("quack_query: client error: {e}"))
        })?;
        Ok(Arc::new(provider))
    }
}

fn extract_string_arg(
    fn_name: &str,
    exprs: &[Expr],
    pos: usize,
    label: &str,
) -> DFResult<String> {
    match exprs.get(pos) {
        Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => Ok(s.clone()),
        Some(Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)) => Ok(s.clone()),
        Some(other) => Err(DataFusionError::Plan(format!(
            "{fn_name}: arg {pos} ({label}) must be a non-null string literal, got {other:?}"
        ))),
        None => Err(DataFusionError::Plan(format!(
            "{fn_name}: missing arg {pos} ({label})"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_wrong_arity() {
        let tvf = QuackQueryTvf::new();
        let err = tvf.call(&[]).unwrap_err();
        assert!(err.to_string().contains("quack_query expects"));
    }

    #[test]
    fn rejects_non_string_args() {
        let tvf = QuackQueryTvf::new();
        let err = tvf
            .call(&[
                Expr::Literal(ScalarValue::Int32(Some(1)), None),
                Expr::Literal(ScalarValue::Utf8(Some("x".into())), None),
            ])
            .unwrap_err();
        assert!(err.to_string().contains("non-null string literal"));
    }
}
