//! Admission control: the gates a statement passes before it is parsed.
//!
//! `execute` and `execute_stream` used to carry their own copy of this
//! sequence (review 409). The two copies had drifted in the order of the
//! memory reservation and the global permit, so the error a client saw when
//! two gates failed at once depended on which entry point it used. One
//! function now, one order:
//!
//! 1. memory pressure: when the coordinator pool is in the red band,
//!    metadata statements pass, other statements run under the smaller
//!    `query.pressure_admission_memory` cap, and with that cap at 0 they are
//!    refused outright (the message names the ORDER BY columns because
//!    dropping the sort is the operator's first lever). The cap is enough
//!    for a light query; a heavy one fails at its first large reservation.
//!    Only reads get the cap, so `execute` refuses any other statement kind
//!    once it is classified ([`QueryHandler::refuse_under_pressure`]);
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
    /// The cap the statement executes under: `max_query_memory`, or the
    /// smaller pressure-admission cap when the pool was red.
    pub quota: crate::query_memory::QueryQuota,
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
        // Metadata statements (SHOW, DESCRIBE, EXPLAIN without ANALYZE) read
        // catalog state and plan; they never execute a scan. See below.
        let metadata = is_metadata_statement(sql);

        // Memory pressure admission control when the coordinator pool is
        // >95% utilized (Red). Refusing everything there also refused
        // `count(*) FROM nation` while heavy queries held the pool (#788).
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
        let mut quota = crate::query_memory::QueryQuota::normal(self.per_query_memory_bytes);
        if !pressure.admits_new_query() && !metadata {
            if self.pressure_admission_memory_bytes == 0 {
                warn!(
                    pressure = %pressure,
                    username = %session.user.username,
                    "Rejecting {what} due to memory pressure"
                );
                return Err(pressure_rejection(sql, pressure));
            }
            let bytes = match self.per_query_memory_bytes {
                0 => self.pressure_admission_memory_bytes,
                cap => cap.min(self.pressure_admission_memory_bytes),
            };
            warn!(
                pressure = %pressure,
                username = %session.user.username,
                quota_bytes = bytes,
                "Admitting {what} under memory pressure with a reduced quota"
            );
            quota = crate::query_memory::QueryQuota::under_pressure(bytes);
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
        //
        // Metadata statements take no share of the user's execution budget,
        // and pass the red band above. Without this, a crawler that
        // describes eight tables at once against the defaults (1GB budget,
        // 256MB per query) sees the fifth `SHOW COLUMNS` refused with a
        // memory error while the pool is idle. They still take the
        // concurrency permits above. A statement admitted under pressure
        // reserves only its reduced quota, so a user whose heavy queries
        // hold most of their budget can still run a light read.
        let reserves_memory = !metadata;
        let memory_reservation = if reserves_memory
            && self.per_user_memory_budget_bytes > 0
            && self.per_query_memory_bytes > 0
        {
            let username = session.user.username.clone();
            match self.per_user_memory.try_reserve(
                &username,
                quota.bytes,
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
            quota,
        })
    }

    /// Refuse a classified statement that is not a read when it was admitted
    /// under pressure. Only the read path runs under the reduced quota;
    /// writes, DDL and procedures would run uncapped on a red pool.
    pub(crate) fn refuse_under_pressure(
        &self,
        admission: &Admission,
        is_read: bool,
        sql: &str,
    ) -> sqe_core::Result<()> {
        if !admission.quota.under_pressure || is_read {
            return Ok(());
        }
        // Red as of admission: the statement was admitted only as a possible
        // read, even if the pool has drained since.
        Err(pressure_rejection(sql, crate::memory::MemoryPressure::Red))
    }
}

/// The refusal a statement gets at red without a pressure-admission cap.
fn pressure_rejection(sql: &str, pressure: crate::memory::MemoryPressure) -> SqeError {
    let sort_cols = extract_order_by_columns(sql);
    SqeError::Execution(adaptive_sort::format_pressure_rejection(
        &sort_cols, pressure,
    ))
}

/// True when the statement only reads catalog metadata or plans: `SHOW ...`,
/// `DESCRIBE`/`DESC ...`, and `EXPLAIN ...` other than `EXPLAIN ANALYZE`,
/// which executes. Admission runs before the parse (the parse itself is what
/// the size cap and the permits protect), so this looks at the leading
/// keyword after whitespace and `--` / `/* */` comments. A statement it
/// misclassifies as a query only pays the reservation it always paid; a
/// statement it misclassifies as metadata would have to begin with one of
/// these three keywords and then execute a scan, and only `EXPLAIN ANALYZE`
/// does, so that one is excluded.
pub(crate) fn is_metadata_statement(sql: &str) -> bool {
    let mut words = leading_keywords(sql, 2).into_iter();
    let Some(first) = words.next() else {
        return false;
    };
    match first.as_str() {
        "SHOW" | "DESCRIBE" | "DESC" => true,
        "EXPLAIN" => !words.next().is_some_and(|w| w == "ANALYZE"),
        _ => false,
    }
}

/// The first `n` keyword-like tokens of `sql`, upper-cased, skipping
/// whitespace, `--` line comments and `/* */` block comments. Stops at the
/// first token that is not a bare word, so `EXPLAIN (FORMAT ...)` yields
/// only `EXPLAIN`.
fn leading_keywords(sql: &str, n: usize) -> Vec<String> {
    let mut rest = sql;
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        rest = rest.trim_start();
        if let Some(after) = rest.strip_prefix("--") {
            rest = after.split_once('\n').map(|(_, r)| r).unwrap_or("");
            continue;
        }
        if let Some(after) = rest.strip_prefix("/*") {
            match after.split_once("*/") {
                Some((_, r)) => rest = r,
                None => return out,
            }
            continue;
        }
        let end = rest
            .find(|c: char| !c.is_ascii_alphabetic())
            .unwrap_or(rest.len());
        if end == 0 {
            return out;
        }
        out.push(rest[..end].to_ascii_uppercase());
        rest = &rest[end..];
    }
    out
}

#[cfg(test)]
mod tests {
    use super::is_metadata_statement;

    #[test]
    fn show_describe_and_explain_are_metadata() {
        for sql in [
            "SHOW COLUMNS FROM \"glue\".\"db\".\"t\"",
            "  show tables in glue.db",
            "SHOW STATS FOR glue.db.t",
            "DESCRIBE glue.db.t",
            "desc t",
            "EXPLAIN SELECT * FROM glue.db.t LIMIT 1",
            "explain\nselect 1",
            "-- crawler\nSHOW COLUMNS FROM t",
            "/* crawler */ EXPLAIN /* plan only */ SELECT 1",
        ] {
            assert!(is_metadata_statement(sql), "{sql}");
        }
    }

    #[test]
    fn queries_dml_and_explain_analyze_reserve_memory() {
        for sql in [
            "SELECT * FROM glue.db.t",
            "  select count(*) from t",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "INSERT INTO t SELECT * FROM s",
            "CREATE TABLE t AS SELECT * FROM s ORDER BY a",
            "EXPLAIN ANALYZE SELECT * FROM glue.db.t",
            "explain analyze verbose select 1",
            "",
            "   ",
            "-- only a comment",
            "/* unterminated SHOW",
            "(SELECT 1)",
        ] {
            assert!(!is_metadata_statement(sql), "{sql:?}");
        }
    }
}
