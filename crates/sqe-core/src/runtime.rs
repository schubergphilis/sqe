//! Shared tokio runtime builder for SQE server binaries, and the dedicated
//! large stack the recursive planning phases run on.
//!
//! The write path and the policy rewriter overflow the 2 MiB tokio default
//! stack. Every long-running binary (sqe-server coordinator, sqe-server
//! worker, sqe-worker) builds its runtime here so the stack size cannot
//! drift between entry points.
//!
//! Stack size is a correctness property here, not a tuning knob (#640): a
//! stack overflow in Rust is a process abort, so one deep `IN (...)` list or
//! one MERGE with a wide policy rewrite took down the coordinator and every
//! in-flight query with it. The planning phases (SQL to logical plan, the
//! policy rewrite, the optimizer, physical planning) are recursive tree
//! walks whose depth follows the query, and they used to run on whatever
//! request thread picked up the task. [`on_planning_stack`] moves them to a
//! thread with [`PLANNING_STACK_BYTES`] of stack, so the requirement is
//! bounded to those phases and the runtime threads no longer have to be
//! sized for the deepest plan anyone might send.

use std::future::Future;

use tokio::runtime::{Handle, RuntimeFlavor};

use crate::config::CoordinatorConfig;

/// Default request-thread stack in MiB when the operator has not set
/// `coordinator.runtime_stack_mib`.
pub const DEFAULT_RUNTIME_STACK_MIB: usize = 8;

/// Stack of the thread [`on_planning_stack`] runs a planning phase on.
///
/// 64 MiB: the deepest shape on record (the TPC-E `trade_result_update_holding`
/// OR chain of 34,496 disjuncts) overflowed 8 MiB in release and needed more
/// than that in debug; the integration suites run with 32 MiB. Virtual
/// reservation only: pages are committed as the recursion actually touches
/// them, so an ordinary plan costs the same few hundred kilobytes it always
/// did.
pub const PLANNING_STACK_BYTES: usize = 64 * 1024 * 1024;

/// Build the multi-thread tokio runtime used by sqe-server and sqe-worker.
pub fn build_server_runtime(
    cfg: &CoordinatorConfig,
    thread_name: &str,
) -> std::io::Result<tokio::runtime::Runtime> {
    let stack_mib = if cfg.runtime_stack_mib == 0 {
        DEFAULT_RUNTIME_STACK_MIB
    } else {
        cfg.runtime_stack_mib
    };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(stack_mib.saturating_mul(1024 * 1024))
        .thread_name(thread_name)
        .build()
}

/// Run `fut` to completion on a thread with [`PLANNING_STACK_BYTES`] of stack.
///
/// The future may borrow from the caller: it runs inside `std::thread::scope`,
/// so the borrows are proven to outlive the thread. The calling runtime worker
/// hands its core back to the scheduler for the duration
/// (`tokio::task::block_in_place`), and the planning thread drives the
/// future with `Handle::block_on` on the same runtime, so awaits inside it
/// (catalog loads, policy fetches) make progress on the other workers.
///
/// On a current-thread runtime, or outside any runtime, the future runs
/// inline on the caller's stack. There is no safe large-stack path there:
/// the caller holds the runtime's only core, and a second thread blocking on
/// the same handle cannot drive its IO or timers (the #195 wedge). Every
/// deployed SQE binary runs the multi-thread flavor; `#[tokio::test]` bodies
/// are the current-thread case, which is why the integration suites still
/// set `RUST_MIN_STACK`.
///
/// A panic inside `fut` is re-raised on the caller's task, exactly as if the
/// future had been awaited in place.
pub async fn on_planning_stack<F>(fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    let Ok(handle) = Handle::try_current() else {
        return fut.await;
    };
    if handle.runtime_flavor() != RuntimeFlavor::MultiThread {
        return fut.await;
    }
    tokio::task::block_in_place(move || {
        std::thread::scope(|scope| {
            let worker = std::thread::Builder::new()
                .name("sqe-planning".to_string())
                .stack_size(PLANNING_STACK_BYTES)
                .spawn_scoped(scope, move || handle.block_on(fut))
                // The closure owns `fut`, so a spawn failure cannot fall back
                // to running it here. Thread creation fails only when the
                // process is out of address space or threads, and a panic
                // fails this one request instead of aborting the process.
                .expect("spawn the planning thread (out of threads or address space?)");
            match worker.join() {
                Ok(output) => output,
                Err(payload) => std::panic::resume_unwind(payload),
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A recursion that needs about `depth * 4 KiB` of stack. The buffer is
    /// black-boxed and the addition follows the recursive call, so the
    /// optimizer can neither drop the frame nor turn it into a loop.
    #[inline(never)]
    fn burn(depth: usize) -> usize {
        let buf = [0u8; 4096];
        std::hint::black_box(&buf);
        if depth == 0 {
            buf[0] as usize
        } else {
            burn(depth - 1) + 1
        }
    }

    /// The builder honours the configured stack: a worker thread on the
    /// default 8 MiB runtime recurses through ~4 MiB, which would abort the
    /// process on tokio's 2 MiB default.
    #[test]
    fn server_runtime_threads_get_the_configured_stack() {
        let cfg: CoordinatorConfig = toml::from_str("").expect("all fields default");
        assert_eq!(cfg.runtime_stack_mib, DEFAULT_RUNTIME_STACK_MIB);
        let rt = build_server_runtime(&cfg, "sqe-test").expect("runtime");
        let depth = rt.block_on(async { tokio::spawn(async { burn(1000) }).await.expect("task") });
        assert_eq!(depth, 1000);
    }

    /// The property #640 is about: a planning phase that needs more stack
    /// than a runtime thread has completes instead of aborting the process.
    /// Runtime threads here get 1 MiB; the recursion needs about 12 MiB.
    #[test]
    fn planning_phase_deeper_than_the_runtime_stack_completes() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(1024 * 1024)
            .enable_all()
            .build()
            .expect("runtime");
        let depth = rt.block_on(async {
            tokio::spawn(async { on_planning_stack(async { burn(3000) }).await })
                .await
                .expect("task")
        });
        assert_eq!(depth, 3000);
    }

    /// Borrowed data and awaits inside the phase both work: the future runs
    /// on the same runtime, so timers fire, and `std::thread::scope` proves
    /// the borrows outlive the planning thread.
    #[test]
    fn planning_phase_can_borrow_and_await() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let joined = rt.block_on(async {
            tokio::spawn(async move {
                // Owned by the task, borrowed by the planning phase.
                let names = vec!["orders".to_string(), "customers".to_string()];
                let names_ref = &names;
                on_planning_stack(async {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    names_ref.join(",")
                })
                .await
            })
            .await
            .expect("task")
        });
        assert_eq!(joined, "orders,customers");
    }

    /// Current-thread runtimes run the phase inline: no second thread, no
    /// `block_in_place` panic, same result.
    #[tokio::test]
    async fn current_thread_runtime_runs_inline() {
        let value = on_planning_stack(async { burn(10) }).await;
        assert_eq!(value, 10);
    }

    /// A panic inside the phase reaches the caller as a panic, not as a
    /// silent hang or an aborted process.
    #[test]
    fn panic_inside_the_phase_propagates() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let result = rt.block_on(async {
            tokio::spawn(async {
                on_planning_stack(async {
                    if burn(1) == 1 {
                        panic!("planning failed loudly");
                    }
                    0
                })
                .await
            })
            .await
        });
        let err = result.expect_err("the task must report the panic");
        assert!(err.is_panic());
    }
}
