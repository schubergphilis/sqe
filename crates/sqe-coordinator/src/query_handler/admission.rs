//! Admission control: the gates a statement passes before it is parsed.
//!
//! `execute` and `execute_stream` used to carry their own copy of this
//! sequence (review 409). The two copies had drifted in the order of the
//! memory reservation and the global permit, so the error a client saw when
//! two gates failed at once depended on which entry point it used. One
//! function now, one order:
//!
//! 1. memory pressure: refuse outright when the coordinator pool is in the
//!    red band (the message names the ORDER BY columns because dropping the
//!    sort is the operator's first lever);
//! 2. the per-user concurrency permit, so one tenant cannot hold every
//!    global permit while under their own submission rate limit;
//! 3. the global concurrency permit;
//! 4. the per-user memory reservation.
//!
//! Permits and the reservation are owned values. The buffered path drops
//! them when the call returns; the streaming path moves them into the
//! result-stream wrapper so they are released when the client finishes
//! draining, not when planning returns.

use super::*;

/// What admission granted. Dropping it releases everything.
pub(crate) struct Admission {
    /// Per-user permit first, then the global one, when either cap is on.
    pub permits: Vec<tokio::sync::OwnedSemaphorePermit>,
    /// The per-user memory reservation, when the budget is on.
    pub memory_reservation: Option<crate::memory::PerUserReservation>,
}

impl QueryHandler {
    /// Run the admission gates for one statement. `what` names the entry
    /// point in the pressure-rejection log line ("query", "streaming query").
    pub(crate) fn admit(
        &self,
        session: &Session,
        sql: &str,
        what: &str,
    ) -> sqe_core::Result<Admission> {
        // Memory pressure admission control: reject new queries when the
        // coordinator's FairSpillPool is >95% utilized (Red).
        let pressure = crate::memory::check_pressure(&self.runtime.memory_pool);
        if let Some(ref metrics) = self.metrics {
            metrics.coordinator_memory_pressure.set(pressure.as_gauge());
            metrics
                .coordinator_memory_used_bytes
                .set(crate::memory::used_bytes(&self.runtime.memory_pool) as f64);
            metrics
                .coordinator_memory_limit_bytes
                .set(crate::memory::limit_bytes(&self.runtime.memory_pool) as f64);
        }
        if !pressure.admits_new_query() {
            warn!(
                pressure = %pressure,
                username = %session.user.username,
                "Rejecting {what} due to memory pressure"
            );
            let sort_cols = extract_order_by_columns(sql);
            return Err(SqeError::Execution(
                adaptive_sort::format_pressure_rejection(&sort_cols, pressure),
            ));
        }

        let mut permits = Vec::new();
        if self.config.query.max_concurrent_per_user > 0 {
            let username = session.user.username.clone();
            let sem = self
                .per_user_semaphores
                .entry(username.clone())
                .or_insert_with(|| {
                    Arc::new(tokio::sync::Semaphore::new(
                        self.config.query.max_concurrent_per_user,
                    ))
                })
                .clone();
            match sem.try_acquire_owned() {
                Ok(p) => permits.push(p),
                Err(_) => {
                    return Err(SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!(
                            "Too many concurrent queries for user '{}' ({} active). Please retry later.",
                            username, self.config.query.max_concurrent_per_user
                        ),
                    ));
                }
            }
        }
        if let Some(ref sem) = self.query_semaphore {
            match Arc::clone(sem).try_acquire_owned() {
                Ok(p) => permits.push(p),
                Err(_) => {
                    return Err(SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!(
                            "Too many concurrent queries ({} active). Please retry later.",
                            self.config.query.max_concurrent_queries
                        ),
                    ));
                }
            }
        }

        // Per-user memory reservation. Reject when this user's in-flight
        // queries would push past their share of the global pool, before
        // the global red-band check fires for everyone else.
        let memory_reservation =
            if self.per_user_memory_budget_bytes > 0 && self.per_query_memory_bytes > 0 {
                let username = session.user.username.clone();
                match self.per_user_memory.try_reserve(
                    &username,
                    self.per_query_memory_bytes,
                    self.per_user_memory_budget_bytes,
                ) {
                    Some(r) => Some(r),
                    None => {
                        let used = self.per_user_memory.used_bytes(&username);
                        return Err(SqeError::with_code(
                            SqeErrorCode::ExecutionFailed,
                            format!(
                                "Per-user memory budget exceeded for '{}': {} bytes reserved, \
                                 limit {} bytes. Wait for in-flight queries to complete.",
                                username, used, self.per_user_memory_budget_bytes
                            ),
                        ));
                    }
                }
            } else {
                None
            };

        Ok(Admission {
            permits,
            memory_reservation,
        })
    }
}
