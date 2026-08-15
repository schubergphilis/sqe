//! Async-from-sync bridges that work on both multi-thread and current-thread
//! tokio runtimes.
//!
//! DataFusion's [`TableFunctionImpl::call`] and several [`SchemaProvider`]
//! methods are synchronous. Bridging them into async catalog/object-store
//! work has historically used `tokio::task::block_in_place` plus
//! `Handle::current().block_on(...)`. `block_in_place` is only valid on the
//! multi-thread runtime: under `current_thread` it panics with
//! "can call blocking only when running on the multi-threaded runtime"
//! (issue #83).
//!
//! [`block_on_compat`] picks the right strategy based on
//! [`Handle::runtime_flavor`]:
//!
//! - `MultiThread`: `block_in_place` + `Handle::block_on`. The worker thread
//!   leaves the scheduler for the duration of the await, and the scheduler is
//!   free to move other tasks onto other workers. Every deployed SQE entry point
//!   is this flavor.
//! - `CurrentThread`: drive the future on a one-shot OS thread with a runtime of
//!   its own, and wait for the result on a channel with a hard deadline. Avoids
//!   the panic, and cannot hang forever.
//!
//! # Why the deadline is an OS-level wait
//!
//! A current-thread runtime has one core, and every real call site is a sync
//! DataFusion hook reached from inside a task poll. So the caller already holds
//! the core and keeps holding it while it waits here. Anything the bridged future
//! needs from THAT runtime cannot happen until this call returns, which is the
//! wedge this module has been bitten by twice (issue #195, and the
//! catalog-traversal case in
//! `docs/internal/research/2026-08-02-catalog-traversal-gate.md`).
//!
//! `tokio::time::timeout` is no guard against it: a timer cannot fire on a
//! runtime whose thread is synchronously blocked. Neither is `JoinHandle::join`,
//! which has no timeout at all. `Receiver::recv_timeout` is an OS-level wait, so
//! it fires regardless of what any runtime is doing, which makes it the only
//! guard that works here.
//!
//! On the deadline the worker thread is left running and detached. That leaks a
//! thread and a runtime, which is worth saying plainly, but the alternative is the
//! hang itself: nothing can safely cancel a future blocked in someone else's
//! reactor. The count is bounded by the number of timeouts, and a timeout means
//! something is already wrong.
//!
//! # What this still does not fix
//!
//! A resource created on the CALLER's current-thread runtime (a socket, a pooled
//! HTTP connection) is registered with that runtime's IO driver. Awaiting it from
//! anywhere else cannot make progress while the caller's runtime is parked, and no
//! choice of executor here changes that. The deadline turns that case from an
//! unkillable hang into a diagnosable error, which is all a bridge can do. The
//! real fix is not to call async catalog work from a sync hook on a
//! single-threaded runtime, and no deployed SQE binary does.

use std::fmt;
use std::future::Future;
use std::time::Duration;

use tokio::runtime::{Handle, RuntimeFlavor};
use tracing::error;

/// How long a current-thread bridge waits before giving up.
///
/// Generous on purpose: this bounds real catalog and object-store calls, and
/// turning a slow-but-working listNamespaces into a spurious error would be a
/// worse bug than the one being fixed. Anything past a minute in one of these
/// calls is not slow, it is stuck.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Why a bridged call produced no value.
///
/// Separate variants because the call sites turn this into a message a user
/// reads, and "no tokio runtime available" on a call that actually timed out sends
/// the reader somewhere there is nothing to find.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeError {
    /// Called from outside any tokio runtime.
    NoRuntime,
    /// The OS refused a thread for the bridge.
    ThreadSpawnFailed,
    /// The bridge's worker thread started but could not build a runtime to drive
    /// the future on.
    ///
    /// Distinct from `NoRuntime`, which is about the CALLER. Collapsing the two
    /// would report "no tokio runtime available" on a call made from a perfectly
    /// good runtime, which is the class of misleading message the `Result` return
    /// exists to remove.
    WorkerRuntimeUnavailable,
    /// The future did not finish in time. Usually means it is waiting on
    /// something owned by the runtime this call is blocking.
    TimedOut(Duration),
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRuntime => write!(f, "no tokio runtime available"),
            Self::ThreadSpawnFailed => write!(f, "could not start a thread to bridge async work"),
            Self::WorkerRuntimeUnavailable => write!(
                f,
                "the bridge worker thread could not build a runtime to drive async work"
            ),
            Self::TimedOut(d) => write!(
                f,
                "async work did not complete within {}s while bridged from a \
                 synchronous call on a current-thread runtime; it is most likely \
                 waiting on the runtime this call is blocking",
                d.as_secs()
            ),
        }
    }
}

impl std::error::Error for BridgeError {}

/// Drive `fut` to completion from a synchronous context, regardless of the
/// active tokio runtime flavor.
///
/// Errors rather than hanging. See the module docs for which failure modes are
/// fixed and which are only made visible.
pub fn block_on_compat<F>(fut: F) -> Result<F::Output, BridgeError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    block_on_compat_within(fut, DEFAULT_TIMEOUT)
}

/// [`block_on_compat`] with an explicit deadline, so a test can assert the
/// deadline behaviour without waiting a minute for it.
fn block_on_compat_within<F>(fut: F, timeout: Duration) -> Result<F::Output, BridgeError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let handle = Handle::try_current().map_err(|_| BridgeError::NoRuntime)?;
    if handle.runtime_flavor() == RuntimeFlavor::MultiThread {
        // No deadline here, deliberately. `block_in_place` hands the core back to
        // the scheduler, so the runtime keeps driving IO and timers while this
        // await runs and the deadlock this module guards against cannot arise.
        // Bounding it would need its own thread, on the hot path of every TVF
        // call, to protect against nothing.
        return Ok(tokio::task::block_in_place(|| handle.block_on(fut)));
    }

    // A runtime OF ITS OWN, and NOT the caller's handle.
    //
    // The caller's `handle` would be a deadlock: its single core is held by the
    // task that called us, and that task cannot release it until this returns.
    // Nor is the caller's context entered here. `Runtime::block_on` sets the
    // current handle to its own for the duration, so entering the parent's would
    // be inert; the comment that used to claim otherwise was wrong.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("sqe-block-on-compat".to_string())
        .spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                // Dropping `tx` without sending wakes the receiver, so the caller
                // gets a prompt error instead of sitting out the whole deadline.
                return;
            };
            let _ = tx.send(rt.block_on(fut));
        })
        .map_err(|_| BridgeError::ThreadSpawnFailed)?;

    match rx.recv_timeout(timeout) {
        Ok(v) => Ok(v),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            error!(
                timeout_secs = timeout.as_secs(),
                "bridged async work did not complete: it is most likely waiting on a \
                 resource owned by the current-thread runtime this synchronous call is \
                 blocking. Returning an error rather than hanging; the worker thread is \
                 left detached."
            );
            Err(BridgeError::TimedOut(timeout))
        }
        // The worker died without sending, which means its runtime would not build.
        // NOT `NoRuntime`: the caller's runtime is fine, the child's is what failed.
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            Err(BridgeError::WorkerRuntimeUnavailable)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn block_on_compat_works_on_multi_thread() {
        let result = tokio::task::spawn_blocking(|| block_on_compat(async { 42i32 }))
            .await
            .unwrap();
        assert_eq!(result, Ok(42));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn block_on_compat_works_on_current_thread() {
        // NOTE: this calls the bridge from a thread OUTSIDE the runtime, so the
        // runtime's core is free. That is not the shape of any real call site, and
        // it is why this test passed for months while production deadlocked. Kept
        // as the outside-a-task case; the two below cover the shapes that matter.
        let handle = Handle::current();
        let result = std::thread::spawn(move || {
            let _enter = handle.enter();
            block_on_compat(async { "ok".to_string() })
        })
        .join()
        .unwrap();
        assert_eq!(result.as_deref(), Ok("ok"));
    }

    /// The real shape: a synchronous call made while the current-thread runtime's
    /// only core is held by the task doing the calling.
    ///
    /// Every production caller is a sync DataFusion hook (`SchemaProvider::table`,
    /// `CatalogProvider::schema`, a TVF's `call`) reached from inside a task poll.
    /// An implementation that handed the future back to the CALLER's runtime could
    /// not make progress until the caller returned, so the two waited on each other
    /// forever.
    ///
    /// Driven from a scratch thread with a channel and `recv_timeout` rather than
    /// `#[tokio::test]`, deliberately: a regression here is a deadlock, and a
    /// deadlocking test hangs the whole suite with no message instead of failing.
    /// This one fails, and says what it means.
    #[test]
    fn does_not_deadlock_when_the_caller_holds_the_current_thread_core() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current-thread runtime");
            let out = rt.block_on(async {
                // Called INLINE from the task body: the runtime's core is busy
                // polling this future, exactly as it is when DataFusion calls a
                // sync provider hook.
                block_on_compat(async {
                    // Awaits a timer, so the future genuinely needs a runtime to be
                    // driven. A future that is ready on first poll would complete
                    // even under a broken implementation.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    7i32
                })
            });
            let _ = tx.send(out);
        });
        let out = rx.recv_timeout(Duration::from_secs(10)).expect(
            "block_on_compat deadlocked: it was called while the current-thread \
             runtime's core was held by the calling task, and handed the future \
             back to that same runtime",
        );
        assert_eq!(out, Ok(7));
    }

    /// A bridged future that cannot finish returns an error instead of blocking
    /// the caller forever.
    ///
    /// This is the guard that makes the module's promise true. Two others do not
    /// work here and both have been tried in this repo: `tokio::time::timeout`
    /// cannot fire while the runtime thread is synchronously blocked (learned at
    /// issue #195), and `JoinHandle::join` has no deadline at all.
    ///
    /// `pending()` stands in for the real cause, which is a future waiting on a
    /// resource registered with the caller's parked IO driver. It is the same
    /// observable behaviour (a future that never completes) and it is
    /// deterministic, where reproducing the driver case depends on whether a
    /// readiness event happened to be consumed before the bridge was entered.
    #[test]
    fn a_future_that_cannot_finish_fails_loudly_instead_of_hanging() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current-thread runtime");
            let out = rt.block_on(async {
                block_on_compat_within(std::future::pending::<()>(), Duration::from_millis(300))
            });
            let _ = tx.send(out);
        });
        let out = rx.recv_timeout(Duration::from_secs(10)).expect(
            "the bridge hung: a future that cannot complete must return \
             BridgeError::TimedOut, not block the calling thread forever",
        );
        assert_eq!(out, Err(BridgeError::TimedOut(Duration::from_millis(300))));
    }
}
