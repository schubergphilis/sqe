//! The door check for a signed catalog scope (issue #908).
//!
//! Every wire protocol that can carry a scope (Trino HTTP header, Flight SQL
//! metadata) calls [`attach`] on the per-request session right after
//! authentication. For the one configured client it verifies the scope and
//! attaches it to the session; for every other client it changes nothing.
//!
//! Protocols that cannot carry a scope (MCP, Quack) never call this, and do
//! not need to: the session keeps the bearer's [`TokenBinding`], and
//! `Session::effective_scope` refuses a session of the configured client
//! without a scope when its context is built. The gate here gives the early,
//! audited refusal; the core check is the one that cannot be skipped.
//!
//! [`TokenBinding`]: sqe_core::catalog_scope::TokenBinding

use std::sync::{Arc, RwLock};

use sqe_auth::catalog_scope_verifier::CatalogScopeVerifier;
use sqe_core::catalog_scope::{refused_in_scoped_session, ScopeReason};
use sqe_core::config::{CatalogScopeConfig, CatalogScopeMode};
use sqe_core::{Session, SqeError};
use sqe_metrics::audit::{Actor, AuditEvent, AuditKind, AuditLogger, Integrity, Outcome};
use sqe_sql::StatementKind;
use tracing::{debug, info, warn};

struct Gate {
    verifier: CatalogScopeVerifier,
    audit: Option<Arc<AuditLogger>>,
    /// `Binding` or `Union`; `Off` installs no gate.
    mode: CatalogScopeMode,
}

static GATE: RwLock<Option<Arc<Gate>>> = RwLock::new(None);

fn current() -> Option<Arc<Gate>> {
    GATE.read().ok().and_then(|g| g.clone())
}

/// Install (or, with `None`, remove) the gate. Called once at coordinator
/// startup from `[session.catalog_scope]`, and logs the active mode.
///
/// - `binding`: the gate verifies and attaches scopes, and the scoped client
///   is recorded in `sqe-core` so the fail-closed check at context build
///   knows it.
/// - `union`: the gate still refuses the client without a valid scope, but
///   attaches nothing, so the session sees every catalog the person can.
/// - `off`, or no section: no gate; the scope header is ignored.
pub fn install(
    config: Option<&CatalogScopeConfig>,
    audit: Option<Arc<AuditLogger>>,
) -> Result<(), SqeError> {
    let configured = config.is_some();
    let config = config.filter(|c| c.mode != CatalogScopeMode::Off);
    let gate = match config {
        Some(cfg) => {
            let verifier = CatalogScopeVerifier::new(cfg)
                .map_err(|e| SqeError::Config(format!("[session.catalog_scope]: {e}")))?;
            if cfg.allow_insecure_jwks {
                warn!("[session.catalog_scope] allow_insecure_jwks is set: development only");
            }
            info!(
                mode = cfg.mode.as_str(),
                client_id = %cfg.client_id,
                header = %verifier.header_name(),
                "catalog scope enforcement"
            );
            if cfg.mode == CatalogScopeMode::Union {
                warn!("[session.catalog_scope] mode = \"union\": scopes are verified but sessions are not narrowed");
            }
            Some(Arc::new(Gate {
                verifier,
                audit,
                mode: cfg.mode,
            }))
        }
        None if configured => {
            warn!("[session.catalog_scope] mode = \"off\": the scope header is ignored");
            None
        }
        None => {
            info!(mode = "off", "catalog scope enforcement");
            None
        }
    };
    sqe_core::catalog_scope::set_scoped_client(
        config
            .filter(|c| c.mode == CatalogScopeMode::Binding)
            .map(|c| c.client_id.clone()),
    );
    if let Ok(mut slot) = GATE.write() {
        *slot = gate;
    }
    Ok(())
}

/// The active mode: `Off` when no gate is installed.
pub fn mode() -> CatalogScopeMode {
    current().map_or(CatalogScopeMode::Off, |g| g.mode)
}

/// The header / Flight metadata key carrying the scope, lowercased; `None`
/// when the feature is off.
pub fn header_name() -> Option<String> {
    current().map(|g| g.verifier.header_name().to_string())
}

/// Whether `session` belongs to the scoped client, so [`attach`] has work to
/// do. False when the feature is off.
pub fn applies(session: &Session) -> bool {
    current().is_some_and(|g| g.verifier.applies_to(session.token_binding()))
}

/// Verify `scope` for `session` and attach it.
///
/// Sessions of other clients pass through unchanged, whatever they send.
/// A session of the configured client without a valid scope is refused with
/// the reason, and the refusal is audited (never the token itself).
pub async fn attach(
    session: Session,
    scope: Option<&str>,
    client_ip: Option<&str>,
) -> Result<Session, SqeError> {
    let Some(gate) = current() else {
        return Ok(session);
    };
    if !gate.verifier.applies_to(session.token_binding()) {
        return Ok(session);
    }
    let Some(binding) = session.token_binding().cloned() else {
        return Err(refuse(&gate, &session, client_ip, "no token binding"));
    };
    let Some(scope) = scope.map(str::trim).filter(|s| !s.is_empty()) else {
        let reason = format!(
            "a catalog scope is required for client '{}' (header '{}')",
            gate.verifier.client_id(),
            gate.verifier.header_name()
        );
        return Err(refuse(&gate, &session, client_ip, &reason));
    };
    match gate.verifier.verify(scope, &binding).await {
        Ok(scope) if gate.mode == CatalogScopeMode::Union => {
            debug!(
                user = %session.user.username,
                catalogs = ?scope.catalog_names().collect::<Vec<_>>(),
                "catalog scope verified, not attached (union mode)"
            );
            Ok(session)
        }
        Ok(scope) => {
            debug!(
                user = %session.user.username,
                catalogs = ?scope.catalog_names().collect::<Vec<_>>(),
                "catalog scope attached"
            );
            Ok(session.with_catalog_scope(Some(Arc::new(scope))))
        }
        Err(refusal) => Err(refuse(&gate, &session, client_ip, &refusal.to_string())),
    }
}

/// Refuse what a session limited to a catalog scope may not run (#908):
/// changing the catalogs or secrets of the process, and access control
/// statements. `USE` / `SET CATALOG` may only name a scoped catalog. Runs on
/// every classified statement; a no-op for an unscoped session.
pub fn check_statement(session: &Session, kind: &StatementKind) -> Result<(), SqeError> {
    if session.effective_scope()?.is_none() {
        return Ok(());
    }
    let refused = match kind {
        StatementKind::Attach(_) => "ATTACH",
        StatementKind::Detach(_) => "DETACH",
        StatementKind::CreateSecret(_) => "CREATE SECRET",
        StatementKind::DropSecret(_) => "DROP SECRET",
        StatementKind::ShowSecrets => "SHOW SECRETS",
        StatementKind::Grant(_) | StatementKind::GrantWithPolicy(_) => "GRANT",
        StatementKind::Revoke(_) => "REVOKE",
        StatementKind::Deny(_) => "DENY",
        StatementKind::PolicyDdl(_) => "CREATE / DROP POLICY",
        StatementKind::SetTags(_) | StatementKind::SetObjectTags(_) => "Setting tags",
        StatementKind::SetMaskingPolicy(_) => "Setting a masking policy",
        StatementKind::SetCatalog(target) => {
            if let Ok(catalog) = crate::query_handler::parse_catalog_target(target) {
                session.check_catalog_read(&catalog)?;
            }
            return Ok(());
        }
        StatementKind::Use(target) => {
            if let Ok((Some(catalog), _)) = crate::query_handler::parse_use_target(target) {
                session.check_catalog_read(&catalog)?;
            }
            return Ok(());
        }
        _ => return Ok(()),
    };
    Err(refused_in_scoped_session(refused))
}

/// For a statement that failed to plan in a scoped session: the scope's own
/// refusal when the statement names a catalog the scope hides, else `None`.
///
/// A hidden catalog is never registered, so DataFusion only says "table not
/// found". This puts the scope's answer in its place: the client still sees
/// "Catalog 'c' not found", and the audit record names the reason (#295).
/// Runs on the failure path only.
pub(crate) fn hidden_catalog_refusal(
    state: &datafusion::execution::SessionState,
    session: &Session,
    sql: &str,
) -> Option<SqeError> {
    session.catalog_scope()?;
    let statement = datafusion::sql::parser::DFParserBuilder::new(sql)
        .with_dialect(&sqe_sql::ExecutionDialect)
        .build()
        .ok()?
        .parse_statement()
        .ok()?;
    state
        .resolve_table_references(&statement)
        .ok()?
        .iter()
        .filter_map(|table| table.catalog())
        .find_map(|catalog| session.check_catalog_read(catalog).err())
        .filter(|e| ScopeReason::of(e).is_some())
}

fn refuse(gate: &Gate, session: &Session, client_ip: Option<&str>, reason: &str) -> SqeError {
    warn!(user = %session.user.username, reason, "catalog scope refused");
    if let Some(audit) = gate.audit.as_ref() {
        audit.log_event(AuditEvent {
            time: chrono::Utc::now(),
            kind: AuditKind::Auth,
            actor: Actor {
                username: session.user.username.clone(),
                subject: session.user.subject.clone(),
                email: session.user.email.clone(),
                roles: session.user.roles.clone(),
                groups: session.user.groups.clone(),
                app_name: Some(gate.verifier.client_id().to_string()),
            },
            outcome: Outcome::Failure {
                error_type: Some("catalog_scope".to_string()),
                error_code: Some("CATALOG_SCOPE_REFUSED".to_string()),
                message: Some(reason.to_string()),
            },
            resources: vec![],
            policy: None,
            timing: None,
            stats: None,
            query: None,
            session_id: None,
            client_ip: client_ip.map(str::to_string),
            trace_id: sqe_metrics::propagation::current_trace_id(),
            query_id: None,
            transaction: None,
            session: None,
            integrity: Integrity::default(),
        });
    }
    SqeError::Auth(reason.to_string())
}
