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
            // Current-thread runtime: hand the future to a fresh OS thread
            // that pins itself to the same Handle. The parent thread blocks
            // on the join, so semantics match block_in_place from the
            // caller's perspective.
            let join = std::thread::Builder::new()
                .name("sqe-block-on-compat".to_string())
                .spawn(move || handle.block_on(fut))
                .ok()?;
            join.join().ok()
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
        // The current-thread branch spawns an OS thread, so we must run the
        // bridge from a task that has the Handle available. Use a blocking
        // closure that captures the handle through Handle::current().
        let handle = Handle::current();
        let result = std::thread::spawn(move || {
            let _enter = handle.enter();
            block_on_compat(async { "ok".to_string() })
        })
        .join()
        .unwrap();
        assert_eq!(result.as_deref(), Some("ok"));
    }
}
