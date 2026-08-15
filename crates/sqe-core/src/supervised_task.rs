//! Supervised background tasks.
//!
//! Coordinator-side housekeeping tasks (health checks, credential refresh,
//! auth refresh) used to be raw `tokio::spawn` calls with their `JoinHandle`
//! dropped on the floor. The runtime would keep them alive until shutdown,
//! but tests that build and tear down a coordinator leaked the loop and any
//! `Arc`-owned state inside it. Three call sites carried identical
//! `TODO(security-hardening)` markers asking for a `JoinHandle` and a
//! `CancellationToken`.
//!
//! `spawn_supervised` is the one helper that consolidates the three TODOs:
//!
//! - The returned [`TaskGuard`] aborts the task on drop.
//! - The task receives a [`CancellationToken`] it can poll for cooperative
//!   shutdown.
//! - Panics inside the future are caught and logged with the task name
//!   instead of taking down the runtime.

use std::future::Future;
use std::panic::AssertUnwindSafe;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

/// Guard that owns the spawned task and cancels it on drop.
///
/// Operators can also call [`TaskGuard::shutdown`] to request graceful
/// shutdown and await termination, or [`TaskGuard::cancel`] to signal
/// without waiting.
pub struct TaskGuard {
    name: &'static str,
    token: CancellationToken,
    handle: Option<JoinHandle<()>>,
}

impl TaskGuard {
    /// Returns the cancellation token bound to this task. Useful for
    /// composing nested cancellation hierarchies.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Returns the task name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Signal cancellation without waiting for the task to finish.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// Signal cancellation and wait for the task to finish.
    pub async fn shutdown(mut self) {
        self.token.cancel();
        if let Some(handle) = self.handle.take() {
            if let Err(e) = handle.await {
                if !e.is_cancelled() {
                    warn!(task = %self.name, error = %e, "supervised task did not exit cleanly");
                }
            }
        }
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.token.cancel();
            handle.abort();
        }
    }
}

/// Spawn a supervised background task.
///
/// The closure receives a `CancellationToken` it can `select!` over. Panics
/// inside the future are caught and logged so a single misbehaving task does
/// not propagate panic-on-detach to the runtime.
pub fn spawn_supervised<F, Fut>(name: &'static str, build: F) -> TaskGuard
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let token = CancellationToken::new();
    let task_token = token.clone();
    let fut = build(task_token);
    let handle = tokio::spawn(async move {
        use futures::FutureExt;
        let result = AssertUnwindSafe(fut).catch_unwind().await;
        if let Err(payload) = result {
            let msg = panic_payload_message(&payload);
            error!(task = %name, panic = %msg, "supervised task panicked");
        }
    });
    TaskGuard {
        name,
        token,
        handle: Some(handle),
    }
}

fn panic_payload_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn drop_guard_cancels_task() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let guard = spawn_supervised("test-task", move |token| async move {
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                        c.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        drop(guard);
        let before = counter.load(Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let after = counter.load(Ordering::SeqCst);
        assert!(after <= before + 1, "task should have stopped after drop");
    }

    #[tokio::test]
    async fn shutdown_awaits_clean_exit() {
        let guard = spawn_supervised("test-task", |token| async move {
            token.cancelled().await;
        });
        guard.shutdown().await;
    }

    #[tokio::test]
    async fn panic_in_task_is_logged_not_propagated() {
        let guard = spawn_supervised("panicking-task", |_token| async move {
            panic!("intentional panic for test");
        });
        // The task panic is caught; dropping the guard should not propagate.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        drop(guard);
    }
}
