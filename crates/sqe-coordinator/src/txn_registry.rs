//! Open explicit transactions (#821, #824).
//!
//! A session in a transaction carries only its id: Flight SQL stores it on
//! the session after BEGIN, the Trino protocol replays it in
//! `X-Trino-Transaction-Id`. This registry maps that id to the
//! [`TxnScope`] every statement of the transaction runs in, so the second
//! INSERT finds the branch the first one created.
//!
//! Entries are created by the first statement of a transaction (the Trino
//! front door answers START TRANSACTION without the executor) and removed
//! by COMMIT or ROLLBACK. An id that ended is remembered for a day, so a
//! client replaying it gets an error instead of a fresh transaction that
//! silently lacks the earlier writes. A transaction idle for
//! [`IDLE_TIMEOUT`], or open for [`MAX_LIFETIME`], is rolled back on the
//! next registry access. The lifetime cap is what lets the maintenance
//! sweep drop an old transaction branch without asking every coordinator.
//!
//! The registry lives in this process only. An id minted by another
//! coordinator, or by this one before a restart, carries another
//! incarnation ([`sqe_core::txn_id`]) and is refused as not found here;
//! ROLLBACK of such an id succeeds, so the client can let go of it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sqe_catalog::txn::{TxnScope, MAX_LIFETIME};
use sqe_core::{SqeError, SqeErrorCode};
use sqe_metrics::audit::{
    Actor, AuditEvent, AuditKind, AuditLogger, Integrity, PublishedSnapshot, TransactionAudit,
};
use sqe_metrics::MetricsRegistry;

/// An open transaction nobody used for this long is rolled back.
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// How long an ended transaction id stays refused.
const ENDED_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// How long a shutdown waits for the open transactions' branches to drop.
const SHUTDOWN_DISCARD_LIMIT: Duration = Duration::from_secs(10);

struct Entry {
    owner: String,
    open: Open,
    last_used: Instant,
}

#[derive(Default)]
struct State {
    open: HashMap<String, Entry>,
    ended: HashMap<String, Instant>,
}

/// A transaction taken out of the registry by COMMIT or ROLLBACK.
pub(crate) struct Open {
    pub(crate) scope: Arc<TxnScope>,
    owner: String,
    opened: Instant,
    /// Run once when the transaction ends; see [`TxnRegistry::at_end`].
    at_end: Mutex<Vec<AtEnd>>,
}

/// Work held until a transaction ends, called with `None` when COMMIT
/// published its writes and with the end reason otherwise. Erased so the
/// registry, which `QueryHandler` holds, carries no plan types (the lineage
/// contexts it holds own a `LogicalPlan`; see `query_handler::LineageSink`).
pub(crate) type AtEnd = Box<dyn FnOnce(Option<&'static str>) + Send>;

impl Open {
    fn new(scope: Arc<TxnScope>, owner: &str) -> Self {
        Self {
            scope,
            owner: owner.to_string(),
            opened: Instant::now(),
            at_end: Mutex::new(Vec::new()),
        }
    }
}

/// What COMMIT or ROLLBACK finds for a transaction id.
pub(crate) enum Ended {
    /// The transaction ran statements: publish or discard this scope.
    Open(Open),
    /// No statement ran inside it: nothing to publish or discard.
    Unused,
    /// It already ended (a conflict or the idle sweep rolled it back), or
    /// it is not on this coordinator. ROLLBACK accepts that; COMMIT returns
    /// the error.
    Already(SqeError),
}

/// How a transaction ended, the `outcome` label of
/// `sqe_transactions_ended_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Committed,
    RolledBack,
    Conflict,
    IdleTimeout,
    Lifetime,
    SessionEnded,
    Shutdown,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RolledBack => "rolled_back",
            Self::Conflict => "conflict",
            Self::IdleTimeout => "idle_timeout",
            Self::Lifetime => "lifetime",
            Self::SessionEnded => "session_ended",
            Self::Shutdown => "shutdown",
        }
    }

    /// The audit `action`: what the transaction's end did to its writes.
    fn action(self) -> &'static str {
        match self {
            Self::Committed | Self::Conflict => "commit",
            _ => "rollback",
        }
    }
}

#[derive(Default)]
pub(crate) struct TxnRegistry {
    state: Mutex<State>,
    metrics: Option<Arc<MetricsRegistry>>,
    audit: Option<Arc<AuditLogger>>,
}

impl TxnRegistry {
    pub(crate) fn new(metrics: Option<Arc<MetricsRegistry>>) -> Self {
        Self {
            state: Mutex::default(),
            metrics,
            audit: None,
        }
    }

    /// Record every transaction's start and end as an
    /// [`AuditKind::Transaction`] event, the coordinator's own rollbacks
    /// (idle, lifetime, session end, shutdown) included.
    pub(crate) fn with_audit(mut self, audit: Arc<AuditLogger>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// The scope for transaction `id` of `owner`, created on first use.
    /// Errors for an id that is not a UUID (it becomes a branch name), one
    /// not minted by this process, one another user opened, or one that
    /// already ended.
    pub(crate) fn scope(&self, id: &str, owner: &str) -> sqe_core::Result<Arc<TxnScope>> {
        validate_id(id)?;
        if !sqe_core::txn_id::is_local_txn_id(id) {
            return Err(self.not_here(id));
        }
        let mut state = self.lock();
        let expired = state.sweep(Instant::now());
        let mut began = false;
        let result = if state.ended.contains_key(id) {
            Err(ended(id))
        } else {
            match state.open.get_mut(id) {
                Some(entry) if entry.owner != owner => Err(unknown(id)),
                Some(entry) => {
                    entry.last_used = Instant::now();
                    Ok(Arc::clone(&entry.open.scope))
                }
                None => {
                    let scope = TxnScope::new(id);
                    state.open.insert(
                        id.to_string(),
                        Entry {
                            owner: owner.to_string(),
                            open: Open::new(Arc::clone(&scope), owner),
                            last_used: Instant::now(),
                        },
                    );
                    began = true;
                    Ok(scope)
                }
            }
        };
        self.set_open_gauge(&state);
        drop(state);
        if began {
            self.audit_event(id, owner, "begin", None);
        }
        self.discard_all(expired);
        result
    }

    /// End transaction `id` of `owner` for COMMIT or ROLLBACK. The id is
    /// refused from now on; [`Self::reopen`] undoes that when a COMMIT
    /// fails in a way that keeps the transaction open. The caller reports
    /// how an [`Ended::Open`] transaction ended through [`Self::finish`].
    pub(crate) fn end(&self, id: &str, owner: &str) -> sqe_core::Result<Ended> {
        if let Err(e) = validate_id(id) {
            return Ok(Ended::Already(e));
        }
        if !sqe_core::txn_id::is_local_txn_id(id) {
            return Ok(Ended::Already(self.not_here(id)));
        }
        let mut state = self.lock();
        let expired = state.sweep(Instant::now());
        let result = match state.open.get(id) {
            Some(entry) if entry.owner != owner => Err(unknown(id)),
            Some(_) => {
                let open = state.open.remove(id).map(|e| e.open);
                state.ended.insert(id.to_string(), Instant::now());
                Ok(open.map_or(Ended::Unused, Ended::Open))
            }
            None if state.ended.contains_key(id) => Ok(Ended::Already(ended(id))),
            None => {
                state.ended.insert(id.to_string(), Instant::now());
                Ok(Ended::Unused)
            }
        };
        self.set_open_gauge(&state);
        drop(state);
        self.discard_all(expired);
        result
    }

    /// Hold `f` until transaction `id` ends. Hands it back when `id` is not
    /// open here, for the caller to run now.
    pub(crate) fn at_end(&self, id: &str, f: AtEnd) -> Result<(), AtEnd> {
        let state = self.lock();
        match state.open.get(id) {
            Some(entry) => {
                entry
                    .open
                    .at_end
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(f);
                Ok(())
            }
            None => Err(f),
        }
    }

    /// Record how a transaction [`Self::end`] returned as open ended.
    pub(crate) fn finish(&self, open: &Open, outcome: Outcome) {
        self.record_end(open, outcome);
    }

    /// Put back a transaction whose COMMIT failed without ending it.
    pub(crate) fn reopen(&self, owner: &str, open: Open) {
        let mut state = self.lock();
        state.ended.remove(open.scope.id());
        state.open.insert(
            open.scope.id().to_string(),
            Entry {
                owner: owner.to_string(),
                open,
                last_used: Instant::now(),
            },
        );
        self.set_open_gauge(&state);
    }

    /// Roll back transaction `id` of `owner` because the session holding
    /// it ended: its id went with the session, so nothing can commit it.
    pub(crate) fn abandon(&self, id: &str, owner: &str) {
        let mut state = self.lock();
        let taken = match state.open.get(id) {
            Some(entry) if entry.owner == owner => state.open.remove(id),
            _ => None,
        };
        if taken.is_some() {
            state.ended.insert(id.to_string(), Instant::now());
        }
        self.set_open_gauge(&state);
        drop(state);
        if let Some(entry) = taken {
            tracing::info!(transaction = %id, "rolling back transaction of an ended session");
            self.discard_all(vec![(entry.open, Outcome::SessionEnded)]);
        }
    }

    /// Roll back every open transaction, for a coordinator that is shutting
    /// down: once it exits, nothing can commit them. Waits at most
    /// [`SHUTDOWN_DISCARD_LIMIT`]; a branch not dropped by then holds no
    /// visible data.
    pub(crate) async fn abort_all(&self) {
        let open: Vec<Open> = {
            let mut state = self.lock();
            let now = Instant::now();
            let open: Vec<(String, Entry)> = state.open.drain().collect();
            for (id, _) in &open {
                state.ended.insert(id.clone(), now);
            }
            self.set_open_gauge(&state);
            open.into_iter().map(|(_, e)| e.open).collect()
        };
        if open.is_empty() {
            return;
        }
        tracing::info!(
            count = open.len(),
            "rolling back open transactions at shutdown"
        );
        let discards =
            open.into_iter().map(|open| {
                self.record_end(&open, Outcome::Shutdown);
                let metrics = self.metrics.clone();
                async move {
                    count_drop_failures(&metrics, sqe_catalog::txn::discard(&open.scope).await)
                }
            });
        if tokio::time::timeout(SHUTDOWN_DISCARD_LIMIT, futures::future::join_all(discards))
            .await
            .is_err()
        {
            tracing::warn!(
                limit_secs = SHUTDOWN_DISCARD_LIMIT.as_secs(),
                "transaction branches still dropping at the shutdown limit; leaving them"
            );
        }
    }

    /// Count a refused foreign id and build its error.
    fn not_here(&self, id: &str) -> SqeError {
        if let Some(m) = &self.metrics {
            m.transaction_not_found_total.inc();
        }
        not_found(id)
    }

    /// Every end of a transaction comes through here: COMMIT and ROLLBACK
    /// by way of [`Self::finish`], the coordinator's own rollbacks directly.
    fn record_end(&self, open: &Open, outcome: Outcome) {
        let elapsed = open.opened.elapsed();
        if let Some(m) = &self.metrics {
            m.transactions_ended_total
                .with_label_values(&[outcome.label()])
                .inc();
            m.transaction_duration_seconds.observe(elapsed.as_secs_f64());
        }
        let published = open
            .scope
            .published()
            .into_iter()
            .map(|(ident, snapshot_id)| PublishedSnapshot {
                table: ident.to_string(),
                snapshot_id,
            })
            .collect();
        self.audit_event(
            open.scope.id(),
            &open.owner,
            outcome.action(),
            Some(TransactionAudit {
                reason: Some(outcome.label().to_string()),
                duration_ms: Some(elapsed.as_millis() as u64),
                published,
                ..TransactionAudit::default()
            }),
        );
        let held = std::mem::take(&mut *open.at_end.lock().unwrap_or_else(|e| e.into_inner()));
        let unpublished = (outcome != Outcome::Committed).then(|| outcome.label());
        for f in held {
            f(unpublished);
        }
    }

    /// One [`AuditKind::Transaction`] event. `end` carries what
    /// [`Self::record_end`] knows about how it ended; `None` is a start.
    /// The actor is the owner's user name only: the registry never sees
    /// the session, and the coordinator's own rollbacks have none. The
    /// statement record of the `COMMIT` or `ROLLBACK` carries the session.
    fn audit_event(&self, id: &str, owner: &str, action: &str, end: Option<TransactionAudit>) {
        let Some(audit) = &self.audit else {
            return;
        };
        let conflict = end
            .as_ref()
            .and_then(|e| e.reason.as_deref())
            .is_some_and(|r| r == Outcome::Conflict.label());
        let outcome = if conflict {
            let code = SqeErrorCode::TransactionConflict;
            sqe_metrics::audit::Outcome::Failure {
                error_type: Some(code.trino_error_type().to_string()),
                error_code: Some(code.name().to_string()),
                message: None,
            }
        } else {
            sqe_metrics::audit::Outcome::Success
        };
        let transaction = TransactionAudit {
            id: id.to_string(),
            action: action.to_string(),
            ..end.unwrap_or_default()
        };
        audit.log_event(AuditEvent {
            time: chrono::Utc::now(),
            kind: AuditKind::Transaction,
            actor: Actor {
                username: owner.to_string(),
                ..Actor::default()
            },
            outcome,
            resources: Vec::new(),
            policy: None,
            timing: None,
            stats: None,
            query: None,
            session_id: None,
            client_ip: None,
            trace_id: sqe_metrics::propagation::current_trace_id(),
            query_id: None,
            transaction: Some(transaction),
            integrity: Integrity::default(),
        });
    }

    fn set_open_gauge(&self, state: &State) {
        if let Some(m) = &self.metrics {
            m.transactions_open.set(state.open.len() as i64);
        }
    }

    /// Roll back `expired` in the background. Outside any scope: the
    /// spawned task has no transaction task-local.
    fn discard_all(&self, expired: Vec<(Open, Outcome)>) {
        for (open, outcome) in expired {
            self.record_end(&open, outcome);
            match outcome {
                Outcome::IdleTimeout => {
                    tracing::info!(transaction = %open.scope.id(), "rolling back idle transaction")
                }
                Outcome::Lifetime => tracing::info!(
                    transaction = %open.scope.id(),
                    "rolling back transaction open past its lifetime"
                ),
                _ => {}
            }
            let metrics = self.metrics.clone();
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                rt.spawn(async move {
                    count_drop_failures(&metrics, sqe_catalog::txn::discard(&open.scope).await);
                });
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // Plain maps, never left half-updated by a panic.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn count_drop_failures(metrics: &Option<Arc<MetricsRegistry>>, failed: usize) {
    if let Some(m) = metrics {
        m.transaction_branch_drop_failures_total
            .inc_by(failed as u64);
    }
}

impl State {
    /// Remove transactions idle past [`IDLE_TIMEOUT`] or open past
    /// [`MAX_LIFETIME`] (returned for the caller to roll back outside the
    /// lock) and forget old ended ids.
    fn sweep(&mut self, now: Instant) -> Vec<(Open, Outcome)> {
        self.ended
            .retain(|_, at| now.duration_since(*at) < ENDED_RETENTION);
        let expired: Vec<(String, Outcome)> = self
            .open
            .iter()
            .filter_map(|(id, e)| {
                if now.duration_since(e.open.opened) >= MAX_LIFETIME {
                    Some((id.clone(), Outcome::Lifetime))
                } else if now.duration_since(e.last_used) >= IDLE_TIMEOUT {
                    Some((id.clone(), Outcome::IdleTimeout))
                } else {
                    None
                }
            })
            .collect();
        expired
            .into_iter()
            .filter_map(|(id, outcome)| {
                let entry = self.open.remove(&id)?;
                self.ended.insert(id, now);
                Some((entry.open, outcome))
            })
            .collect()
    }
}

fn validate_id(id: &str) -> sqe_core::Result<()> {
    uuid::Uuid::parse_str(id).map(|_| ()).map_err(|_| {
        SqeError::with_code(
            SqeErrorCode::InvalidArguments,
            format!("invalid transaction id {id:?}: expected a UUID"),
        )
    })
}

fn unknown(id: &str) -> SqeError {
    SqeError::with_code(
        SqeErrorCode::InvalidArguments,
        format!("unknown transaction {id}"),
    )
}

fn not_found(id: &str) -> SqeError {
    SqeError::with_code(
        SqeErrorCode::TransactionConflict,
        format!(
            "transaction {id} not found on this coordinator: it was started on \
             another coordinator, or before this one restarted, and its writes \
             are lost; run ROLLBACK and start a new one"
        ),
    )
}

fn ended(id: &str) -> SqeError {
    SqeError::with_code(
        SqeErrorCode::TransactionConflict,
        format!(
            "transaction {id} has ended (committed, rolled back, idle for \
             longer than {} minutes, or open for longer than {} hours); run \
             ROLLBACK and start a new one",
            IDLE_TIMEOUT.as_secs() / 60,
            MAX_LIFETIME.as_secs() / 3600
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_id() -> String {
        sqe_core::txn_id::new_txn_id()
    }

    /// A valid UUID whose incarnation is not this process's.
    fn foreign_id() -> String {
        let local = local_id();
        let flipped = u32::from_str_radix(&local[..8], 16).unwrap() ^ 1;
        format!("{flipped:08x}{}", &local[8..])
    }

    #[test]
    fn statements_of_one_transaction_share_its_scope() {
        let reg = TxnRegistry::default();
        let id = local_id();
        let a = reg.scope(&id, "alice").unwrap();
        let b = reg.scope(&id, "alice").unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn another_user_cannot_join_a_transaction() {
        let reg = TxnRegistry::default();
        let id = local_id();
        reg.scope(&id, "alice").unwrap();
        let err = reg.scope(&id, "mallory").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::InvalidArguments);
        let err = reg.end(&id, "mallory").err().expect("refused");
        assert_eq!(err.error_code(), SqeErrorCode::InvalidArguments);
    }

    #[test]
    fn an_ended_id_is_refused_not_restarted() {
        let reg = TxnRegistry::default();
        let id = local_id();
        reg.scope(&id, "alice").unwrap();
        let Ended::Open(open) = reg.end(&id, "alice").unwrap() else {
            panic!("expected an open transaction");
        };
        let err = reg.scope(&id, "alice").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::TransactionConflict);
        // A second COMMIT is told the transaction is gone.
        assert!(matches!(reg.end(&id, "alice").unwrap(), Ended::Already(_)));
        // A COMMIT that failed without ending the transaction puts it back.
        reg.reopen("alice", open);
        assert!(reg.scope(&id, "alice").is_ok());
    }

    #[test]
    fn every_start_and_end_is_audited() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("audit.jsonl");
        let audit = Arc::new(
            AuditLogger::with_config(
                path.to_str().unwrap(),
                sqe_metrics::audit::AuditFormat::Native,
            )
            .expect("logger"),
        );
        let reg = TxnRegistry::new(None).with_audit(Arc::clone(&audit));
        let committed = local_id();
        reg.scope(&committed, "alice").unwrap();
        // The second statement joins; it starts nothing.
        reg.scope(&committed, "alice").unwrap();
        let Ended::Open(open) = reg.end(&committed, "alice").unwrap() else {
            panic!("expected an open transaction");
        };
        reg.finish(&open, Outcome::Committed);
        let lost = local_id();
        reg.scope(&lost, "bob").unwrap();
        let Ended::Open(open) = reg.end(&lost, "bob").unwrap() else {
            panic!("expected an open transaction");
        };
        reg.finish(&open, Outcome::Conflict);
        // The coordinator's own rollback is recorded too.
        let abandoned = local_id();
        reg.scope(&abandoned, "carol").unwrap();
        reg.abandon(&abandoned, "carol");

        audit.flush();
        let events: Vec<serde_json::Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).expect("valid JSON line"))
            .filter(|v: &serde_json::Value| v["kind"] == "transaction")
            .collect();
        let summary: Vec<(String, String, String, Option<String>)> = events
            .iter()
            .map(|v| {
                let t = &v["transaction"];
                (
                    t["id"].as_str().unwrap().to_string(),
                    v["actor"]["username"].as_str().unwrap().to_string(),
                    t["action"].as_str().unwrap().to_string(),
                    t["reason"].as_str().map(str::to_string),
                )
            })
            .collect();
        let row = |id: &str, user: &str, action: &str, reason: Option<&str>| {
            (
                id.to_string(),
                user.to_string(),
                action.to_string(),
                reason.map(str::to_string),
            )
        };
        assert_eq!(
            summary,
            vec![
                row(&committed, "alice", "begin", None),
                row(&committed, "alice", "commit", Some("committed")),
                row(&lost, "bob", "begin", None),
                row(&lost, "bob", "commit", Some("conflict")),
                row(&abandoned, "carol", "begin", None),
                row(&abandoned, "carol", "rollback", Some("session_ended")),
            ]
        );
        assert_eq!(events[1]["status"], "success");
        assert_eq!(events[3]["status"], "failure");
        assert_eq!(events[3]["error_code"], "TRANSACTION_CONFLICT");
        assert!(events[1]["transaction"]["duration_ms"].is_u64());
        assert!(events[0]["transaction"].get("duration_ms").is_none());
    }

    #[test]
    fn held_work_runs_once_with_the_outcome() {
        let reg = TxnRegistry::default();
        let seen = Arc::new(Mutex::new(Vec::<Option<&'static str>>::new()));
        let hold = |id: &str| {
            let seen = Arc::clone(&seen);
            reg.at_end(id, Box::new(move |r| seen.lock().unwrap().push(r)))
        };

        // No open transaction: the caller gets its work back.
        assert!(hold(&local_id()).is_err());

        let committed = local_id();
        reg.scope(&committed, "alice").unwrap();
        assert!(hold(&committed).is_ok());
        assert!(hold(&committed).is_ok());
        let Ended::Open(open) = reg.end(&committed, "alice").unwrap() else {
            panic!("expected an open transaction");
        };
        reg.finish(&open, Outcome::Committed);

        let abandoned = local_id();
        reg.scope(&abandoned, "bob").unwrap();
        assert!(hold(&abandoned).is_ok());
        reg.abandon(&abandoned, "bob");

        assert_eq!(
            *seen.lock().unwrap(),
            vec![None, None, Some(Outcome::SessionEnded.label())]
        );
    }

    #[test]
    fn ending_an_unused_transaction_has_nothing_to_publish() {
        let reg = TxnRegistry::default();
        assert!(matches!(
            reg.end(&local_id(), "alice").unwrap(),
            Ended::Unused
        ));
    }

    #[test]
    fn ids_must_be_uuids() {
        let reg = TxnRegistry::default();
        let err = reg.scope("../main", "alice").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::InvalidArguments);
        // ROLLBACK lets the client drop it; COMMIT returns the error.
        let Ended::Already(err) = reg.end("../main", "alice").unwrap() else {
            panic!("a non-UUID id must not end cleanly");
        };
        assert_eq!(err.error_code(), SqeErrorCode::InvalidArguments);
    }

    #[test]
    fn another_coordinators_id_is_not_found_here() {
        let metrics = Arc::new(MetricsRegistry::new().unwrap());
        let reg = TxnRegistry::new(Some(Arc::clone(&metrics)));
        let id = foreign_id();
        let err = reg.scope(&id, "alice").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::TransactionConflict);
        assert!(
            err.to_string().contains("not found on this coordinator"),
            "{err}"
        );
        // Not silently started: COMMIT errors the same way.
        let Ended::Already(err) = reg.end(&id, "alice").unwrap() else {
            panic!("a foreign id must not end cleanly");
        };
        assert!(
            err.to_string().contains("not found on this coordinator"),
            "{err}"
        );
        assert!(reg.lock().open.is_empty());
        assert_eq!(metrics.transaction_not_found_total.get(), 2);
    }

    #[test]
    fn an_ended_session_rolls_its_transaction_back() {
        let metrics = Arc::new(MetricsRegistry::new().unwrap());
        let reg = TxnRegistry::new(Some(Arc::clone(&metrics)));
        let id = local_id();
        reg.scope(&id, "alice").unwrap();
        assert_eq!(metrics.transactions_open.get(), 1);
        // Only the owner's session ending counts.
        reg.abandon(&id, "mallory");
        assert_eq!(metrics.transactions_open.get(), 1);
        reg.abandon(&id, "alice");
        assert_eq!(metrics.transactions_open.get(), 0);
        assert_eq!(
            metrics
                .transactions_ended_total
                .with_label_values(&["session_ended"])
                .get(),
            1
        );
        let err = reg.scope(&id, "alice").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::TransactionConflict);
    }

    #[tokio::test]
    async fn shutdown_rolls_back_every_open_transaction() {
        let metrics = Arc::new(MetricsRegistry::new().unwrap());
        let reg = TxnRegistry::new(Some(Arc::clone(&metrics)));
        let (a, b) = (local_id(), local_id());
        reg.scope(&a, "alice").unwrap();
        reg.scope(&b, "bob").unwrap();
        reg.abort_all().await;
        assert_eq!(metrics.transactions_open.get(), 0);
        assert_eq!(
            metrics
                .transactions_ended_total
                .with_label_values(&["shutdown"])
                .get(),
            2
        );
        assert!(reg.scope(&a, "alice").is_err());
    }

    #[test]
    fn idle_transactions_are_swept() {
        let id = local_id();
        let mut state = State::default();
        state.open.insert(
            id.clone(),
            Entry {
                owner: "alice".into(),
                open: Open::new(TxnScope::new(id.as_str()), "alice"),
                last_used: Instant::now(),
            },
        );
        assert!(state.sweep(Instant::now()).is_empty());
        let later = Instant::now() + IDLE_TIMEOUT;
        let swept = state.sweep(later);
        assert_eq!(swept.len(), 1);
        assert_eq!(swept[0].1, Outcome::IdleTimeout);
        assert!(state.open.is_empty());
        assert!(state.ended.contains_key(&id));
    }

    #[test]
    fn busy_transactions_end_at_their_lifetime() {
        let id = local_id();
        let mut state = State::default();
        let opened = Instant::now();
        let at_lifetime = opened + MAX_LIFETIME;
        state.open.insert(
            id.clone(),
            Entry {
                owner: "alice".into(),
                open: Open {
                    opened,
                    ..Open::new(TxnScope::new(id.as_str()), "alice")
                },
                // Used a moment ago: never idle.
                last_used: at_lifetime - Duration::from_secs(1),
            },
        );
        assert!(state.sweep(at_lifetime - Duration::from_secs(1)).is_empty());
        let swept = state.sweep(at_lifetime);
        assert_eq!(swept.len(), 1);
        assert_eq!(swept[0].1, Outcome::Lifetime);
        assert!(state.ended.contains_key(&id));
    }
}
