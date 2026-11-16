//! The admission queue for the global concurrency slot (#902).
//!
//! With every `query.max_concurrent_queries` slot taken, a statement used to
//! be refused at once. Now up to `query.queue_max` statements wait, for at
//! most `query.queue_timeout_secs`, and are served round robin over one lane
//! per user: a user with fifty queued statements and a user with one, the
//! second runs after at most one of the first's.
//!
//! The global permit only exists as a [`GlobalPermit`]. Its `Drop` hands the
//! slot to the next waiter instead of returning it to the semaphore, so every
//! way a statement ends (success, error, panic, a stream dropped mid-flight)
//! feeds the queue. The semaphore only gets a permit back when no one waits,
//! and a new arrival only takes one from the semaphore under the same lock,
//! so it cannot jump the queue.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use sqe_core::{RefusalReason, SqeError};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

/// The global slot a running statement holds. Dropping it admits the next
/// waiter, or returns the slot when no one waits.
pub(crate) struct GlobalPermit {
    permit: Option<OwnedSemaphorePermit>,
    queue: Arc<AdmissionQueue>,
}

impl Drop for GlobalPermit {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            self.queue.release(permit);
        }
    }
}

impl std::fmt::Debug for GlobalPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalPermit").finish_non_exhaustive()
    }
}

pub(crate) struct AdmissionQueue {
    sem: Arc<Semaphore>,
    /// `max_concurrent_queries`, for the refusal text.
    slots: usize,
    max_waiters: usize,
    timeout: Duration,
    state: Mutex<State>,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
}

#[derive(Default)]
struct State {
    lanes: VecDeque<Lane>,
    waiting: usize,
    next_id: u64,
}

struct Lane {
    user: String,
    waiters: VecDeque<Waiter>,
}

struct Waiter {
    id: u64,
    tx: oneshot::Sender<GlobalPermit>,
}

/// Removes a waiter whose future is dropped before it was admitted (the
/// client went away). Declared after the receiver in `acquire`, so it drops
/// first: a permit handed over in between still sits in the receiver, whose
/// drop then passes it on.
struct WaitGuard<'a> {
    queue: &'a AdmissionQueue,
    id: u64,
    started: Instant,
    armed: bool,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        if self.armed && self.queue.remove(self.id) {
            self.queue.observe("cancelled", self.started);
        }
    }
}

impl AdmissionQueue {
    pub(crate) fn new(
        slots: usize,
        max_waiters: usize,
        timeout: Duration,
        metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            sem: Arc::new(Semaphore::new(slots)),
            slots,
            max_waiters,
            timeout,
            state: Mutex::new(State::default()),
            metrics,
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_gauge(&self, state: &State) {
        if let Some(m) = &self.metrics {
            m.admission_queue_length.set(state.waiting as i64);
        }
    }

    fn observe(&self, outcome: &str, started: Instant) {
        if let Some(m) = &self.metrics {
            m.admission_queue_wait_seconds
                .with_label_values(&[outcome])
                .observe(started.elapsed().as_secs_f64());
        }
    }

    /// Statements waiting now.
    #[cfg(test)]
    pub(crate) fn waiting(&self) -> usize {
        self.lock().waiting
    }

    /// Take a global slot for `user`: at once when one is free, else after
    /// waiting in the user's lane. Refused with `queue_full` when
    /// `queue_max` statements already wait, `queue_timeout` when the wait
    /// runs out.
    pub(crate) async fn acquire(self: &Arc<Self>, user: &str) -> Result<GlobalPermit, SqeError> {
        let (id, mut rx) = {
            let mut st = self.lock();
            if let Ok(permit) = Arc::clone(&self.sem).try_acquire_owned() {
                return Ok(GlobalPermit {
                    permit: Some(permit),
                    queue: Arc::clone(self),
                });
            }
            if st.waiting >= self.max_waiters {
                let waiting = st.waiting;
                drop(st);
                return Err(SqeError::refused(
                    RefusalReason::QueueFull,
                    format!(
                        "Too many concurrent queries ({} active, {waiting} queued). \
                         Please retry later.",
                        self.slots
                    ),
                ));
            }
            let id = st.next_id;
            st.next_id += 1;
            let (tx, rx) = oneshot::channel();
            let waiter = Waiter { id, tx };
            match st.lanes.iter_mut().find(|l| l.user == user) {
                Some(lane) => lane.waiters.push_back(waiter),
                None => st.lanes.push_back(Lane {
                    user: user.to_string(),
                    waiters: VecDeque::from([waiter]),
                }),
            }
            st.waiting += 1;
            self.set_gauge(&st);
            (id, rx)
        };
        let mut guard = WaitGuard {
            queue: self,
            id,
            started: Instant::now(),
            armed: true,
        };
        match tokio::time::timeout(self.timeout, &mut rx).await {
            Ok(Ok(permit)) => {
                guard.armed = false;
                self.observe("admitted", guard.started);
                Ok(permit)
            }
            Ok(Err(_)) => {
                // The sender went without a permit: the queue was closed.
                guard.armed = false;
                self.observe("draining", guard.started);
                Err(SqeError::refused(
                    RefusalReason::Draining,
                    "The coordinator is shutting down. Retry on another coordinator.",
                ))
            }
            Err(_) => {
                guard.armed = false;
                if !self.remove(id) {
                    // Handed a slot between the timer and the lock: the send
                    // happened under the lock, so it is in the channel now.
                    if let Ok(permit) = rx.try_recv() {
                        self.observe("admitted", guard.started);
                        return Ok(permit);
                    }
                }
                self.observe("timeout", guard.started);
                Err(SqeError::refused(
                    RefusalReason::QueueTimeout,
                    format!(
                        "Waited {}s for a query slot ({} active). Please retry later.",
                        self.timeout.as_secs(),
                        self.slots
                    ),
                ))
            }
        }
    }

    /// Take waiter `id` out of its lane. False when it is no longer queued
    /// (it was admitted).
    fn remove(&self, id: u64) -> bool {
        let mut st = self.lock();
        let Some(li) = st
            .lanes
            .iter()
            .position(|l| l.waiters.iter().any(|w| w.id == id))
        else {
            return false;
        };
        let lane = &mut st.lanes[li];
        lane.waiters.retain(|w| w.id != id);
        if lane.waiters.is_empty() {
            st.lanes.remove(li);
        }
        st.waiting -= 1;
        self.set_gauge(&st);
        true
    }

    /// A slot came free: give it to the head of the front lane and move that
    /// lane to the back. A waiter whose receiver is gone is skipped. With no
    /// waiter left the slot goes back to the semaphore.
    fn release(self: &Arc<Self>, mut permit: OwnedSemaphorePermit) {
        let mut st = self.lock();
        loop {
            let Some(mut lane) = st.lanes.pop_front() else {
                // Under the lock, so an arrival that just failed
                // `try_acquire` is queued before this, not lost.
                drop(permit);
                return;
            };
            let waiter = lane.waiters.pop_front();
            if !lane.waiters.is_empty() {
                st.lanes.push_back(lane);
            }
            let Some(waiter) = waiter else { continue };
            st.waiting -= 1;
            self.set_gauge(&st);
            let handed = GlobalPermit {
                permit: Some(permit),
                queue: Arc::clone(self),
            };
            match waiter.tx.send(handed) {
                Ok(()) => return,
                Err(mut back) => {
                    permit = back.permit.take().expect("permit was just set");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    fn queue(slots: usize, max_waiters: usize, timeout_ms: u64) -> Arc<AdmissionQueue> {
        AdmissionQueue::new(slots, max_waiters, Duration::from_millis(timeout_ms), None)
    }

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn without_a_queue_a_full_engine_refuses_with_queue_full() {
        let q = queue(1, 0, 1_000);
        let _held = q.acquire("a").await.expect("free slot");
        let err = q.acquire("b").await.expect_err("no slot, no queue");
        assert_eq!(err.refusal_reason(), Some(RefusalReason::QueueFull));
        assert_eq!(err.error_code(), sqe_core::SqeErrorCode::ResourceExhausted);
    }

    #[tokio::test]
    async fn a_queued_statement_runs_when_the_slot_frees() {
        let q = queue(1, 4, 5_000);
        let held = q.acquire("a").await.expect("free slot");
        let q2 = Arc::clone(&q);
        let waiter = tokio::spawn(async move { q2.acquire("b").await });
        settle().await;
        assert_eq!(q.waiting(), 1);
        drop(held);
        let permit = waiter.await.expect("join").expect("admitted after drop");
        assert_eq!(q.waiting(), 0);
        drop(permit);
        // The slot went back to the semaphore: the next one is admitted at once.
        q.acquire("c")
            .now_or_never()
            .expect("immediate")
            .expect("free");
    }

    #[tokio::test]
    async fn the_queue_refuses_past_queue_max() {
        let q = queue(1, 1, 5_000);
        let _held = q.acquire("a").await.expect("free slot");
        let q2 = Arc::clone(&q);
        let _waiter = tokio::spawn(async move { q2.acquire("b").await });
        settle().await;
        let err = q.acquire("c").await.expect_err("queue is full");
        assert_eq!(err.refusal_reason(), Some(RefusalReason::QueueFull));
        assert!(
            err.client_message().contains("1 queued"),
            "{}",
            err.client_message()
        );
    }

    #[tokio::test]
    async fn a_wait_past_the_timeout_is_refused_and_leaves_the_queue() {
        let q = queue(1, 4, 50);
        let _held = q.acquire("a").await.expect("free slot");
        let err = q.acquire("b").await.expect_err("times out");
        assert_eq!(err.refusal_reason(), Some(RefusalReason::QueueTimeout));
        assert_eq!(q.waiting(), 0);
    }

    #[tokio::test]
    async fn a_cancelled_waiter_leaves_the_queue_and_does_not_take_the_slot() {
        let q = queue(1, 4, 5_000);
        let held = q.acquire("a").await.expect("free slot");
        let q2 = Arc::clone(&q);
        let cancelled = tokio::spawn(async move { q2.acquire("b").await });
        settle().await;
        assert_eq!(q.waiting(), 1);
        cancelled.abort();
        let _ = cancelled.await;
        assert_eq!(q.waiting(), 0);
        drop(held);
        q.acquire("c")
            .now_or_never()
            .expect("immediate")
            .expect("slot back");
    }

    #[tokio::test]
    async fn a_receiver_dropped_after_the_handoff_passes_the_slot_on() {
        let q = queue(1, 4, 5_000);
        let held = q.acquire("a").await.expect("free slot");
        // Two waiters; the first is dropped without being polled again after
        // its slot was sent, so its receiver drops holding the permit.
        let mut first = Box::pin(q.acquire("b"));
        assert!((&mut first).now_or_never().is_none());
        let q3 = Arc::clone(&q);
        let second = tokio::spawn(async move { q3.acquire("c").await });
        settle().await;
        assert_eq!(q.waiting(), 2);
        drop(held); // handed to "b", still sitting in its channel
        drop(first);
        let permit = second.await.expect("join").expect("slot passed on");
        drop(permit);
        assert_eq!(q.waiting(), 0);
    }

    #[tokio::test]
    async fn waiters_are_served_round_robin_per_user() {
        let q = queue(1, 16, 5_000);
        let held = q.acquire("seed").await.expect("free slot");
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        // "a" queues three statements before "b" queues one.
        for (user, n) in [("a", 1), ("a", 2), ("a", 3), ("b", 1)] {
            let q2 = Arc::clone(&q);
            let order = Arc::clone(&order);
            tasks.push(tokio::spawn(async move {
                let permit = q2.acquire(user).await.expect("admitted");
                order.lock().unwrap().push(format!("{user}{n}"));
                drop(permit);
            }));
            settle().await;
        }
        assert_eq!(q.waiting(), 4);
        drop(held);
        for t in tasks {
            t.await.expect("join");
        }
        assert_eq!(*order.lock().unwrap(), ["a1", "b1", "a2", "a3"]);
    }

    #[tokio::test]
    async fn one_users_waiters_run_in_fifo_order() {
        let q = queue(1, 16, 5_000);
        let held = q.acquire("seed").await.expect("free slot");
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for n in 0..5 {
            let q2 = Arc::clone(&q);
            let order = Arc::clone(&order);
            tasks.push(tokio::spawn(async move {
                let permit = q2.acquire("a").await.expect("admitted");
                order.lock().unwrap().push(n);
                drop(permit);
            }));
            settle().await;
        }
        drop(held);
        for t in tasks {
            t.await.expect("join");
        }
        assert_eq!(*order.lock().unwrap(), [0, 1, 2, 3, 4]);
    }
}
