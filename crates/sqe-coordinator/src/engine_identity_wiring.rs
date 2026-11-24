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
    Actor, AuditEvent, AuditKind, AuditLogger, Integrity, ObjectType, Outcome, QueryInfo, Resource,
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
/// can never read without the gate. `discovered` marks a warehouse that is
/// not in config and passed the discovery proof; its key-fetch events say so.
pub(crate) fn engine_identity_for(
    credentials: Option<&CatalogAuthConfig>,
    catalog_name: &str,
    session: &Session,
    discovered: bool,
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
        catalog_name: catalog_name.to_string(),
        audit: wiring.audit.clone(),
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
        discovered,
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
        catalog_tags: &[sqe_core::column_tags::ColumnTag],
    ) -> sqe_core::Result<bool> {
        // Malformed tags are UNKNOWN, not empty: fail the load, like the
        // rewriter does (never grant keys on a policy that cannot be read).
        let metadata = loaded.metadata();
        let col_tags = sqe_write::tag_source_impl::effective_column_tags(
            metadata.properties(),
            metadata.current_schema(),
            schema_tags,
            catalog_tags,
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
    catalog_name: String,
    audit: Option<Arc<AuditLogger>>,
}

/// `statement_type` of the audit event for a rotated engine secret (#937).
const ENGINE_SECRET_ROTATED: &str = "engine_secret_rotated";

#[async_trait]
impl EngineTokenSource for ClientCredentialsToken {
    fn client_id(&self) -> &str {
        &self.client_id
    }

    async fn token(&self) -> sqe_core::Result<String> {
        // No session bearer: the engine token never derives from the person's.
        let resolved = sqe_auth::per_catalog::resolve_bearer_tracked(&self.auth, "").await?;
        if resolved.secret_rotated {
            self.record_rotation();
        }
        Ok(resolved.bearer)
    }
}

impl ClientCredentialsToken {
    /// One event when a rotated `client_secret_file` first mints a token.
    /// The actor is the system, not the person whose read triggered it; the
    /// event names the catalog and client id, never the secret.
    fn record_rotation(&self) {
        let text = format!(
            "engine client secret rotated: catalog {}, client {}",
            self.catalog_name, self.client_id
        );
        tracing::info!(catalog = %self.catalog_name, client_id = %self.client_id, "{text}");
        let Some(audit) = self.audit.as_ref() else {
            return;
        };
        let mut actor = Actor::from_parts("system".into(), None, None, vec![], vec![]);
        actor.app_name = Some(self.client_id.clone());
        audit.log_event(AuditEvent {
            time: chrono::Utc::now(),
            kind: AuditKind::Auth,
            actor,
            outcome: Outcome::Success,
            resources: vec![],
            policy: None,
            timing: None,
            stats: None,
            query: Some(QueryInfo {
                query_hash: sqe_metrics::audit::query_hash(&text),
                text: Some(text),
                statement_type: ENGINE_SECRET_ROTATED.to_string(),
                text_len: None,
                query_fingerprint: None,
            }),
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

/// One audit event per engine key fetch, for the person who caused it.
struct SessionEngineAudit {
    audit: Option<Arc<AuditLogger>>,
    user: SessionUser,
    catalog_name: String,
    discovered: bool,
}

/// `statement_type` of a key fetch for a discovered warehouse.
const DISCOVERED_KEY_FETCH: &str = "discovered_engine_key_fetch";
/// `statement_type` of a discovery proof outcome.
const DISCOVERY_PROOF: &str = "discovery_engine_proof";

/// The audit `query` block for an event with no SQL: a fixed kind and a
/// short text that names no token, key or prefix.
fn note(statement_type: &str, text: String) -> QueryInfo {
    QueryInfo {
        query_hash: sqe_metrics::audit::query_hash(&text),
        text: Some(text),
        statement_type: statement_type.to_string(),
        text_len: None,
        query_fingerprint: None,
    }
}

fn actor_for(user: &SessionUser, client_id: String) -> Actor {
    Actor {
        username: user.username.clone(),
        subject: user.subject.clone(),
        email: user.email.clone(),
        roles: user.roles.clone(),
        groups: user.groups.clone(),
        app_name: Some(client_id),
    }
}

/// One `AuditKind::Auth` event for the outcome of the discovery proof on
/// `warehouse`: `Ok` proved, `Err(step)` refused at that step. The person is
/// the actor and the engine's client id the app.
pub(crate) fn record_discovery_proof(
    session: &Session,
    client_id: &str,
    warehouse: &str,
    verdict: Result<(), &str>,
) {
    tracing::info!(
        warehouse,
        client_id,
        "{}",
        discovery_proof_text(warehouse, verdict)
    );
    let Some(audit) = installed().and_then(|w| w.audit.clone()) else {
        return;
    };
    audit.log_event(discovery_proof_event(
        session, client_id, warehouse, verdict,
    ));
}

fn discovery_proof_text(warehouse: &str, verdict: Result<(), &str>) -> String {
    match verdict {
        Ok(()) => format!("discovered warehouse {warehouse}: engine read proved"),
        Err(step) => format!("discovered warehouse {warehouse}: engine read refused at {step}"),
    }
}

/// The event [`record_discovery_proof`] writes.
fn discovery_proof_event(
    session: &Session,
    client_id: &str,
    warehouse: &str,
    verdict: Result<(), &str>,
) -> AuditEvent {
    let text = discovery_proof_text(warehouse, verdict);
    let outcome = match verdict {
        Ok(()) => Outcome::Success,
        Err(step) => Outcome::Failure {
            error_type: Some(DISCOVERY_PROOF.to_string()),
            error_code: Some(step.to_string()),
            message: None,
        },
    };
    AuditEvent {
        time: chrono::Utc::now(),
        kind: AuditKind::Auth,
        actor: actor_for(&session.user, client_id.to_string()),
        outcome,
        // The audit schema has no catalog object type; the text names the
        // warehouse.
        resources: vec![],
        policy: None,
        timing: None,
        stats: None,
        query: Some(note(DISCOVERY_PROOF, text)),
        session_id: None,
        client_ip: None,
        trace_id: sqe_metrics::propagation::current_trace_id(),
        query_id: None,
        transaction: None,
        session: None,
        integrity: Integrity::default(),
    }
}

/// The engine token for `credentials` through the shared cache, as a key
/// fetch would get it (a rotated secret is audited the same way).
pub(crate) async fn engine_token(
    credentials: &CatalogAuthConfig,
    catalog_name: &str,
) -> sqe_core::Result<String> {
    let CatalogAuthConfig::ClientCredentials { client_id, .. } = credentials else {
        return Err(SqeError::Config(format!(
            "catalog '{catalog_name}': `credentials` must be client_credentials"
        )));
    };
    ClientCredentialsToken {
        auth: credentials.clone(),
        client_id: client_id.clone(),
        catalog_name: catalog_name.to_string(),
        audit: installed().and_then(|w| w.audit.clone()),
    }
    .token()
    .await
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
            actor: actor_for(&self.user, event.client_id),
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
            query: self.discovered.then(|| {
                note(
                    DISCOVERED_KEY_FETCH,
                    format!(
                        "engine key fetch: discovered warehouse {}",
                        self.catalog_name
                    ),
                )
            }),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn alice() -> Session {
        Session::new(
            "alice".to_string(),
            sqe_core::SecretString::new("t".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        )
    }

    /// A proved warehouse: an Auth success naming the person, the engine
    /// client and the warehouse, with no resource.
    #[test]
    fn a_proved_warehouse_writes_an_auth_success() {
        let event = discovery_proof_event(&alice(), "sqe-engine", "ws_a", Ok(()));
        assert!(matches!(event.kind, AuditKind::Auth));
        assert!(matches!(event.outcome, Outcome::Success));
        assert_eq!(event.actor.username, "alice");
        assert_eq!(event.actor.app_name.as_deref(), Some("sqe-engine"));
        assert!(event.resources.is_empty());
        let query = event.query.expect("note");
        assert_eq!(query.statement_type, DISCOVERY_PROOF);
        assert_eq!(
            query.text.as_deref(),
            Some("discovered warehouse ws_a: engine read proved")
        );
    }

    /// A key fetch for a discovered warehouse says so; one for a configured
    /// catalog carries no marker. Read back from a real audit file.
    #[test]
    fn a_discovered_key_fetch_is_marked_in_the_audit_log() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("audit.jsonl");
        let audit = Arc::new(AuditLogger::new(path.to_str().expect("utf8")).expect("logger"));
        for (catalog, discovered) in [("ws_a", true), ("lake", false)] {
            SessionEngineAudit {
                audit: Some(Arc::clone(&audit)),
                user: alice().user,
                catalog_name: catalog.into(),
                discovered,
            }
            .record(EngineFetchEvent {
                table: TableIdent::from_strs(["ns", "t"]).expect("ident"),
                client_id: "sqe-engine".into(),
                failure: None,
            });
        }
        audit.flush();
        let lines: Vec<String> = std::fs::read_to_string(&path)
            .expect("audit file")
            .lines()
            .map(str::to_string)
            .collect();
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(lines[0].contains(DISCOVERED_KEY_FETCH), "{}", lines[0]);
        assert!(
            lines[0].contains("engine key fetch: discovered warehouse ws_a"),
            "{}",
            lines[0]
        );
        assert!(!lines[1].contains(DISCOVERED_KEY_FETCH), "{}", lines[1]);
        assert!(lines[1].contains("lake"), "{}", lines[1]);
    }

    /// A refusal carries the step that refused, in the text and the code.
    #[test]
    fn a_refused_warehouse_writes_the_refusing_step() {
        let event = discovery_proof_event(&alice(), "sqe-engine", "ws_a", Err("prefix_mismatch"));
        match event.outcome {
            Outcome::Failure {
                error_type,
                error_code,
                ..
            } => {
                assert_eq!(error_type.as_deref(), Some(DISCOVERY_PROOF));
                assert_eq!(error_code.as_deref(), Some("prefix_mismatch"));
            }
            Outcome::Success => panic!("a refusal is a failure"),
        }
        assert_eq!(
            event.query.and_then(|q| q.text).as_deref(),
            Some("discovered warehouse ws_a: engine read refused at prefix_mismatch")
        );
    }
}
