//! Table-valued function `quack_query(uri, sql)` for DataFusion.
//!
//! Symmetric to DuckDB's `quack_query()` built-in: a SQL session that has
//! this TVF registered can pull data from a remote Quack endpoint inline.
//!
//! ```sql
//! SELECT * FROM quack_query('quack:host:9495', 'SELECT 42');
//! ```
//!
//! The TVF is an outbound-HTTP primitive running from the coordinator's
//! network position, so it is constrained (review finding 604):
//!
//! - the destination must pass the [`QuackTargetPolicy`] allow-list before a
//!   socket is opened, and redirects are never followed;
//! - the credential sent to the remote is the *session's* own bearer token
//!   (or nothing), fixed at registration. The former three-argument form
//!   that took a token as a SQL literal is refused, so credentials no longer
//!   land in query text or audit logs;
//! - the decoded result is capped by `policy.max_result_bytes`;
//! - the synchronous round trip is announced to the tokio runtime with
//!   `block_in_place`, so it does not starve other tasks on that worker.
//!
//! Whether the TVF is registered at all, and for whom, is the coordinator's
//! decision (`[quack_client] enabled` and `allowed_roles`).

use std::sync::Arc;

use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::scalar::ScalarValue;
use datafusion_expr::Expr;

use crate::{QuackTableProvider, QuackTargetPolicy};

/// `quack_query()` TVF impl, bound to one session's credential and to the
/// deployment's destination policy.
#[derive(Debug, Default)]
pub struct QuackQueryTvf {
    policy: QuackTargetPolicy,
    session_token: Option<String>,
}

impl QuackQueryTvf {
    /// A TVF that can reach nothing: `QuackTargetPolicy::default()` has an
    /// empty allow-list. Use [`QuackQueryTvf::with_policy`] in production.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind the TVF to `policy` and to the credential forwarded on every
    /// call. `session_token` is typically the calling session's bearer
    /// token; `None` sends an empty auth string.
    pub fn with_policy(policy: QuackTargetPolicy, session_token: Option<String>) -> Self {
        Self {
            policy,
            session_token,
        }
    }

    fn provider(&self, uri: &str, sql: &str) -> DFResult<QuackTableProvider> {
        let build = || {
            QuackTableProvider::new_with_policy(
                uri,
                self.session_token.as_deref(),
                sql,
                self.policy.clone(),
            )
        };
        let result = match tokio::runtime::Handle::try_current() {
            // `call` is synchronous and runs on a runtime worker. Telling the
            // runtime this thread is about to block lets it hand the worker's
            // queue to another thread for the duration of the round trip.
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(build)
            }
            _ => build(),
        };
        result.map_err(|e| match e {
            crate::ClientError::DestinationDenied(_)
            | crate::ClientError::ResultTooLarge { .. } => {
                DataFusionError::Plan(format!("quack_query: {e}"))
            }
            other => DataFusionError::Execution(format!("quack_query: client error: {other}")),
        })
    }
}

impl TableFunctionImpl for QuackQueryTvf {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let (uri, sql) = match exprs.len() {
            2 => (
                extract_string_arg("quack_query", exprs, 0, "uri")?,
                extract_string_arg("quack_query", exprs, 1, "sql")?,
            ),
            3 => {
                return Err(DataFusionError::Plan(
                    "quack_query no longer accepts a token argument; the session's own \
                     credential is forwarded. Use quack_query(uri, sql)"
                        .to_string(),
                ));
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "quack_query expects 2 args (uri, sql); got {other}"
                )));
            }
        };
        // Destination check first, so a refused target never reaches the
        // network and the error is a plan error, not a transport one.
        Ok(Arc::new(self.provider(&uri, &sql)?))
    }
}

fn extract_string_arg(fn_name: &str, exprs: &[Expr], pos: usize, label: &str) -> DFResult<String> {
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
    // These tests exercise the legacy `TableFunctionImpl::call` entry point that
    // `QuackQueryTvf` implements. DF 54 deprecated `call` in favour of
    // `call_with_args` (which by default delegates back to `call`), so the
    // production path is unaffected; migrating the impls to `call_with_args` is a
    // separate follow-up. Allow the deprecation locally so the strict build stays clean.
    #![allow(deprecated)]
    use super::*;

    fn lit(s: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.into())), None)
    }

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
            .call(&[Expr::Literal(ScalarValue::Int32(Some(1)), None), lit("x")])
            .unwrap_err();
        assert!(err.to_string().contains("non-null string literal"));
    }

    /// 604: a token can no longer be smuggled in as a SQL literal.
    #[test]
    fn rejects_token_argument_form() {
        let tvf = QuackQueryTvf::with_policy(
            QuackTargetPolicy::new(vec!["quack-peer:9495".into()], 1024),
            None,
        );
        let err = tvf
            .call(&[
                lit("quack:quack-peer:9495"),
                lit("secret-token"),
                lit("SELECT 1"),
            ])
            .unwrap_err();
        assert!(
            err.to_string().contains("no longer accepts a token"),
            "{err}"
        );
    }

    /// 604: with the default (empty) policy nothing is reachable, and with an
    /// allow-list the metadata service is refused before any socket opens.
    /// Both errors are plan errors, so they contain no transport detail.
    #[test]
    fn refuses_disallowed_destinations_before_connecting() {
        let denied_all = QuackQueryTvf::new();
        let err = denied_all
            .call(&[lit("quack:localhost:9495"), lit("SELECT 1")])
            .unwrap_err();
        assert!(matches!(err, DataFusionError::Plan(_)), "{err}");
        assert!(
            err.to_string().contains("no quack_client.allowed_hosts"),
            "{err}"
        );

        let scoped = QuackQueryTvf::with_policy(
            QuackTargetPolicy::new(vec!["quack-peer:9495".into()], 1024),
            Some("session-token".into()),
        );
        let err = scoped
            .call(&[
                lit("http://169.254.169.254/latest/meta-data"),
                lit("SELECT 1"),
            ])
            .unwrap_err();
        assert!(matches!(err, DataFusionError::Plan(_)), "{err}");
        assert!(err.to_string().contains("internal address"), "{err}");
    }
}
