//! Open explicit transactions (#821).
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
//! [`IDLE_TIMEOUT`] is rolled back on the next registry access.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sqe_catalog::txn::TxnScope;
use sqe_core::{SqeError, SqeErrorCode};

/// An open transaction nobody used for this long is rolled back.
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// How long an ended transaction id stays refused.
const ENDED_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

struct Entry {
    owner: String,
    scope: Arc<TxnScope>,
    last_used: Instant,
}

#[derive(Default)]
struct State {
    open: HashMap<String, Entry>,
    ended: HashMap<String, Instant>,
}

/// What COMMIT or ROLLBACK finds for a transaction id.
pub(crate) enum Ended {
    /// The transaction ran statements: publish or discard this scope.
    Open(Arc<TxnScope>),
    /// No statement ran inside it: nothing to publish or discard.
    Unused,
    /// It already ended (a conflict or the idle sweep rolled it back).
    /// ROLLBACK accepts that; COMMIT returns the error.
    Already(SqeError),
}

#[derive(Default)]
pub(crate) struct TxnRegistry {
    state: Mutex<State>,
}

impl TxnRegistry {
    /// The scope for transaction `id` of `owner`, created on first use.
    /// Errors for an id that is not a UUID (it becomes a branch name), one
    /// another user opened, or one that already ended.
    pub(crate) fn scope(&self, id: &str, owner: &str) -> sqe_core::Result<Arc<TxnScope>> {
        validate_id(id)?;
        let mut state = self.lock();
        let expired = state.sweep(Instant::now());
        let result = if state.ended.contains_key(id) {
            Err(ended(id))
        } else {
            match state.open.get_mut(id) {
                Some(entry) if entry.owner != owner => Err(unknown(id)),
                Some(entry) => {
                    entry.last_used = Instant::now();
                    Ok(Arc::clone(&entry.scope))
                }
                None => {
                    let scope = TxnScope::new(id);
                    state.open.insert(
                        id.to_string(),
                        Entry {
                            owner: owner.to_string(),
                            scope: Arc::clone(&scope),
                            last_used: Instant::now(),
                        },
                    );
                    Ok(scope)
                }
            }
        };
        drop(state);
        discard_all(expired);
        result
    }

    /// End transaction `id` of `owner` for COMMIT or ROLLBACK. The id is
    /// refused from now on; [`Self::reopen`] undoes that when a COMMIT
    /// fails in a way that keeps the transaction open.
    pub(crate) fn end(&self, id: &str, owner: &str) -> sqe_core::Result<Ended> {
        let mut state = self.lock();
        let expired = state.sweep(Instant::now());
        let result = match state.open.get(id) {
            Some(entry) if entry.owner != owner => Err(unknown(id)),
            Some(_) => {
                let scope = state.open.remove(id).map(|e| e.scope);
                state.ended.insert(id.to_string(), Instant::now());
                Ok(scope.map_or(Ended::Unused, Ended::Open))
            }
            None if state.ended.contains_key(id) => Ok(Ended::Already(ended(id))),
            None => {
                state.ended.insert(id.to_string(), Instant::now());
                Ok(Ended::Unused)
            }
        };
        drop(state);
        discard_all(expired);
        result
    }

    /// Put back a transaction whose COMMIT failed without ending it.
    pub(crate) fn reopen(&self, owner: &str, scope: Arc<TxnScope>) {
        let mut state = self.lock();
        state.ended.remove(scope.id());
        state.open.insert(
            scope.id().to_string(),
            Entry {
                owner: owner.to_string(),
                scope,
                last_used: Instant::now(),
            },
        );
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // Plain maps, never left half-updated by a panic.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl State {
    /// Remove transactions idle past [`IDLE_TIMEOUT`] (returned for the
    /// caller to roll back outside the lock) and forget old ended ids.
    fn sweep(&mut self, now: Instant) -> Vec<Arc<TxnScope>> {
        self.ended
            .retain(|_, at| now.duration_since(*at) < ENDED_RETENTION);
        let idle: Vec<String> = self
            .open
            .iter()
            .filter(|(_, e)| now.duration_since(e.last_used) >= IDLE_TIMEOUT)
            .map(|(id, _)| id.clone())
            .collect();
        idle.into_iter()
            .filter_map(|id| {
                let entry = self.open.remove(&id)?;
                self.ended.insert(id, now);
                Some(entry.scope)
            })
            .collect()
    }
}

/// Roll back idle transactions in the background. Outside any scope: the
/// spawned task has no transaction task-local.
fn discard_all(expired: Vec<Arc<TxnScope>>) {
    for scope in expired {
        tracing::info!(transaction = %scope.id(), "rolling back idle transaction");
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move { sqe_catalog::txn::discard(&scope).await });
        }
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

fn ended(id: &str) -> SqeError {
    SqeError::with_code(
        SqeErrorCode::TransactionConflict,
        format!(
            "transaction {id} has ended (committed, rolled back, or idle for \
             longer than {} minutes); run ROLLBACK and start a new one",
            IDLE_TIMEOUT.as_secs() / 60
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "0b9d6f3e-2f4a-4c3e-9a61-6f1b2c3d4e5f";

    #[test]
    fn statements_of_one_transaction_share_its_scope() {
        let reg = TxnRegistry::default();
        let a = reg.scope(ID, "alice").unwrap();
        let b = reg.scope(ID, "alice").unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn another_user_cannot_join_a_transaction() {
        let reg = TxnRegistry::default();
        reg.scope(ID, "alice").unwrap();
        let err = reg.scope(ID, "mallory").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::InvalidArguments);
        let err = reg.end(ID, "mallory").err().expect("refused");
        assert_eq!(err.error_code(), SqeErrorCode::InvalidArguments);
    }

    #[test]
    fn an_ended_id_is_refused_not_restarted() {
        let reg = TxnRegistry::default();
        let scope = reg.scope(ID, "alice").unwrap();
        assert!(matches!(reg.end(ID, "alice").unwrap(), Ended::Open(_)));
        let err = reg.scope(ID, "alice").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::TransactionConflict);
        // A second COMMIT is told the transaction is gone.
        assert!(matches!(reg.end(ID, "alice").unwrap(), Ended::Already(_)));
        // A COMMIT that failed without ending the transaction puts it back.
        reg.reopen("alice", scope);
        assert!(reg.scope(ID, "alice").is_ok());
    }

    #[test]
    fn ending_an_unused_transaction_has_nothing_to_publish() {
        let reg = TxnRegistry::default();
        assert!(matches!(reg.end(ID, "alice").unwrap(), Ended::Unused));
    }

    #[test]
    fn ids_must_be_uuids() {
        let reg = TxnRegistry::default();
        let err = reg.scope("../main", "alice").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::InvalidArguments);
    }

    #[test]
    fn idle_transactions_are_swept() {
        let mut state = State::default();
        state.open.insert(
            ID.to_string(),
            Entry {
                owner: "alice".into(),
                scope: TxnScope::new(ID),
                last_used: Instant::now(),
            },
        );
        assert!(state.sweep(Instant::now()).is_empty());
        let later = Instant::now() + IDLE_TIMEOUT;
        assert_eq!(state.sweep(later).len(), 1);
        assert!(state.open.is_empty());
        assert!(state.ended.contains_key(ID));
    }
}
