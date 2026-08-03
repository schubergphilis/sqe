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
//! - `MultiThread`: `block_in_place` + `Handle::block_on`. Same behaviour as
//!   before; the worker thread leaves the scheduler for the duration of the
//!   await, but the scheduler is free to move other tasks onto other workers.
//! - `CurrentThread`: ship the future to a one-shot OS thread that drives it
//!   through `Handle::block_on`. Avoids the panic. Cost: one
//!   `std::thread::spawn` per call. Acceptable because current-thread is
//!   only used in tests and the CLI embedded mode.
//!
//! Callers must already be inside a tokio runtime; outside a runtime the
//! result is `None` and the caller is expected to surface that.

use std::future::Future;

use tokio::runtime::{Handle, RuntimeFlavor};

/// Drive `fut` to completion from a synchronous context, regardless of the
/// active tokio runtime flavor.
///
/// Returns `None` when called outside any tokio runtime. Callers in
/// DataFusion's TVF / SchemaProvider hooks should treat that as a hard
/// error: every reachable call site is inside `tokio::runtime::Handle`.
pub fn block_on_compat<F>(fut: F) -> Option<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let handle = Handle::try_current().ok()?;
    match handle.runtime_flavor() {
        RuntimeFlavor::MultiThread => Some(tokio::task::block_in_place(|| handle.block_on(fut))),
        _ => {
            // Current-thread runtime: hand the future to a fresh OS thread with a
            // runtime OF ITS OWN.
            //
            // It must NOT be the caller's `handle`. A current-thread runtime has a
            // single core, and every real call site here is a synchronous
            // DataFusion hook invoked from inside a task being polled on that
            // runtime -- so by the time we get here the caller already holds the
            // core, and it then blocks in `join()` still holding it. A
            // `handle.block_on` on the spawned thread waits for that same core
            // forever: the parent cannot release it until the child returns, and
            // the child cannot start until the parent releases it. Deadlock, not
            // slowness. Observed as an unkillable `pthread_join`/`__ulock_wait`
            // on any query where the namespace cache missed and the catalog
            // provider had to reach the network.
            //
            // A private runtime has no such contention. The caller's runtime stays
            // parked for the duration, which is exactly the semantics
            // `block_in_place` gives on the multi-thread side.
            let join = std::thread::Builder::new()
                .name("sqe-block-on-compat".to_string())
                .spawn(move || {
                    // Keep the caller's handle in scope so anything in `fut` that
                    // asks for "the" runtime (rather than awaiting on ours) still
                    // resolves, while OUR runtime does the driving.
                    let _enter = handle.enter();
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .ok()
                        .map(|rt| rt.block_on(fut))
                })
                .ok()?;
            join.join().ok().flatten()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn block_on_compat_works_on_multi_thread() {
        let result = tokio::task::spawn_blocking(|| {
            block_on_compat(async { 42i32 })
        })
        .await
        .unwrap();
        assert_eq!(result, Some(42));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn block_on_compat_works_on_current_thread() {
        // NOTE: this calls the bridge from a thread OUTSIDE the runtime, so the
        // runtime's core is free. That is not the shape of any real call site, and
        // it is why this test passed for months while production deadlocked. Kept
        // as the outside-a-task case; `does_not_deadlock_when_the_caller_holds_the
        // _current_thread_core` covers the shape that matters.
        let handle = Handle::current();
        let result = std::thread::spawn(move || {
            let _enter = handle.enter();
            block_on_compat(async { "ok".to_string() })
        })
        .join()
        .unwrap();
        assert_eq!(result.as_deref(), Some("ok"));
    }

    /// The real shape: a synchronous call made while the current-thread runtime's
    /// only core is held by the task doing the calling.
    ///
    /// Every production caller is a sync DataFusion hook (`SchemaProvider::table`,
    /// `CatalogProvider::schema`, a TVF's `call`) reached from inside a task poll.
    /// The old implementation handed the future back to the CALLER's runtime, which
    /// cannot make progress until the caller returns, so the two waited on each
    /// other forever.
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
                    // even under the broken implementation.
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    7i32
                })
            });
            let _ = tx.send(out);
        });
        let out = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect(
                "block_on_compat deadlocked: it was called while the current-thread \
                 runtime's core was held by the calling task, and handed the future \
                 back to that same runtime",
            );
        assert_eq!(out, Some(7));
    }
}
