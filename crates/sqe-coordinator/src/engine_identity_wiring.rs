//! The coordinator's side of the engine credential identity (issue #934).
//!
//! A catalog with `credentials` set reads its tables with keys the ENGINE
//! fetched for them, after the policy engine said the person may `select`
//! the table (`sqe_catalog::engine_identity`). `sqe-catalog` cannot reach the
//! policy store, the token cache or the audit log, so this module supplies
//! the three pieces and binds them to one session:
//!
//! - [`PolicyReadGate`]: the same `resolve_effective_policy` +
//!   `policy_denies_all` answer the plan rewriter gives, for the person.
//! - [`ClientCredentialsToken`]: the engine's `client_credentials` token,
//!   cached by `sqe_auth::per_catalog`.
//! - [`SessionEngineAudit`]: one `AuditKind::Auth` event per key fetch, with
//!   the person as actor and the engine's client id as `app_name`.
//!
//! The policy store and the audit log are process-wide, installed once at
//! startup like the catalog scope gate ([`crate::catalog_scope_gate`]).

use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::TableIdent;
use sqe_catalog::engine_identity::{
    EngineAudit, EngineFetchEvent, EngineIdentity, EngineTokenSource, ReadGate,
    ENGINE_CREDENTIALS_ERROR_TYPE,
};
use sqe_core::config::{CatalogAuthConfig, MaskPrecedence};
use sqe_core::session::SessionUser;
use sqe_core::{Session, SqeError};
use sqe_metrics::audit::{
    Actor, AuditEvent, AuditKind, AuditLogger, Integrity, ObjectType, Outcome, Resource,
};
use sqe_policy::PolicyStore;

struct Installed {
    store: Arc<dyn PolicyStore>,
    mask_precedence: MaskPrecedence,
    audit: Option<Arc<AuditLogger>>,
}

static INSTALLED: RwLock<Option<Arc<Installed>>> = RwLock::new(None);

fn installed() -> Option<Arc<Installed>> {
    INSTALLED.read().ok().and_then(|g| g.clone())
}

/// Install (or, with `None` store, remove) the policy store and audit log the
/// engine identity binds to. Called once at coordinator startup, before the
/// first session context is built.
pub fn install(
    store: Option<Arc<dyn PolicyStore>>,
    mask_precedence: MaskPrecedence,
    audit: Option<Arc<AuditLogger>>,
) {
    let value = store.map(|store| {
        Arc::new(Installed {
            store,
            mask_precedence,
            audit,
        })
    });
    if let Ok(mut slot) = INSTALLED.write() {
        *slot = value;
    }
}

/// The engine identity for one session and one catalog, or `Ok(None)` when
/// the catalog does not use one. Fails closed: a catalog that sets
/// `credentials` while no policy store is installed cannot be built, so it
/// can never read without the gate.
pub(crate) fn engine_identity_for(
    credentials: Option<&CatalogAuthConfig>,
    catalog_name: &str,
    session: &Session,
) -> Result<Option<EngineIdentity>, SqeError> {
    let Some(auth) = credentials else {
        return Ok(None);
    };
    let client_id = match auth {
        CatalogAuthConfig::ClientCredentials { client_id, .. } => client_id.clone(),
        _ => {
            return Err(SqeError::Config(format!(
                "catalog '{catalog_name}': `credentials` must be client_credentials"
            )))
        }
    };
    let Some(wiring) = installed() else {
        return Err(SqeError::Config(format!(
            "catalog '{catalog_name}': `credentials` needs an active policy store; \
             none is installed, so no read can be gated"
        )));
    };
    let token = Arc::new(ClientCredentialsToken {
        auth: auth.clone(),
        client_id: client_id.clone(),
    });
    let gate = Arc::new(PolicyReadGate {
        store: Arc::clone(&wiring.store),
        user: session.user.clone(),
        catalog_name: catalog_name.to_string(),
        mask_precedence: wiring.mask_precedence,
    });
    let audit = Arc::new(SessionEngineAudit {
        audit: wiring.audit.clone(),
        user: session.user.clone(),
        catalog_name: catalog_name.to_string(),
    });
    Ok(Some(EngineIdentity::new(token, gate, audit)))
}

/// The select gate: what the plan rewriter would decide for this person.
struct PolicyReadGate {
    store: Arc<dyn PolicyStore>,
    user: SessionUser,
    catalog_name: String,
    mask_precedence: MaskPrecedence,
}

#[async_trait]
impl ReadGate for PolicyReadGate {
    async fn may_read(
        &self,
        table: &TableIdent,
        loaded: &Table,
        schema_tags: &[sqe_core::column_tags::ColumnTag],
    ) -> sqe_core::Result<bool> {
        // Malformed tags are UNKNOWN, not empty: fail the load, like the
        // rewriter does (never grant keys on a policy that cannot be read).
        let metadata = loaded.metadata();
        let col_tags = sqe_write::tag_source_impl::effective_column_tags(
            metadata.properties(),
            metadata.current_schema(),
            schema_tags,
        )
        .map_err(|e| {
            SqeError::Execution(format!(
                "engine credentials: malformed column or table tags on {table}: {e}"
            ))
        })?;
        let hive = sqe_policy::plan_rewriter::HiveCatalogs::new();
        let Some((policy_ns, policy_table)) =
            sqe_policy::plan_rewriter::resolve_policy_key_for_ident_scoped(
                Some(&self.catalog_name),
                table.namespace().as_ref(),
                table.name(),
                &hive,
                self.store.catalog_scoped(),
            )
        else {
            return Ok(false);
        };
        let policy = sqe_policy::plan_rewriter::resolve_effective_policy(
            self.store.as_ref(),
            &self.user,
            Some(&self.catalog_name),
            &policy_table,
            &policy_ns,
            &col_tags,
            self.mask_precedence,
        )
        .await;
        Ok(!sqe_policy::plan_rewriter::policy_denies_all(&policy))
    }
}

/// The engine's own catalog token, through the shared per-catalog cache.
struct ClientCredentialsToken {
    auth: CatalogAuthConfig,
    client_id: String,
}

#[async_trait]
impl EngineTokenSource for ClientCredentialsToken {
    fn client_id(&self) -> &str {
        &self.client_id
    }

    async fn token(&self) -> sqe_core::Result<String> {
        // No session bearer: the engine token never derives from the person's.
        sqe_auth::per_catalog::resolve_bearer(&self.auth, "").await
    }
}

/// One audit event per engine key fetch, for the person who caused it.
struct SessionEngineAudit {
    audit: Option<Arc<AuditLogger>>,
    user: SessionUser,
    catalog_name: String,
}

impl EngineAudit for SessionEngineAudit {
    fn record(&self, event: EngineFetchEvent) {
        let Some(audit) = self.audit.as_ref() else {
            return;
        };
        let outcome = match event.failure {
            None => Outcome::Success,
            Some(message) => Outcome::Failure {
                error_type: Some(ENGINE_CREDENTIALS_ERROR_TYPE.to_string()),
                error_code: Some("ENGINE_CREDENTIALS_FAILED".to_string()),
                message: Some(message),
            },
        };
        audit.log_event(AuditEvent {
            time: chrono::Utc::now(),
            kind: AuditKind::Auth,
            actor: Actor {
                username: self.user.username.clone(),
                subject: self.user.subject.clone(),
                email: self.user.email.clone(),
                roles: self.user.roles.clone(),
                groups: self.user.groups.clone(),
                app_name: Some(event.client_id),
            },
            outcome,
            resources: vec![Resource {
                catalog: Some(self.catalog_name.clone()),
                namespace: event.table.namespace().as_ref().to_vec(),
                name: event.table.name().to_string(),
                object_type: ObjectType::Table,
            }],
            policy: None,
            timing: None,
            stats: None,
            query: None,
            session_id: None,
            client_ip: None,
            trace_id: sqe_metrics::propagation::current_trace_id(),
            query_id: None,
            transaction: None,
            session: None,
            integrity: Integrity::default(),
        });
    }
}
