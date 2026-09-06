//! Shared tokio runtime builder for SQE server binaries.
//!
//! The write path and the policy rewriter overflow the 2 MiB tokio default
//! stack. Every long-running binary (sqe-server coordinator, sqe-server
//! worker, sqe-worker) builds its runtime here so the stack size cannot
//! drift between entry points.

use crate::config::CoordinatorConfig;

/// Default request-thread stack in MiB when the operator has not set
/// `coordinator.runtime_stack_mib`.
pub const DEFAULT_RUNTIME_STACK_MIB: usize = 8;

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
