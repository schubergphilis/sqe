//! Deterministic fault injection for spill tests.
//!
//! Production builds leave the injector inert. Tests install a fault plan
//! that forces typed failures (short write, corruption, cancel) without
//! needing real ENOSPC or disk faults.
//!
//! The fault queue is process-global, so tests that touch it must hold
//! [`serial_test_guard`] for the duration of the test (or use
//! [`FaultSession`]) to avoid racing other tests under cargo's parallel
//! runner.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

/// Kinds of faults the spill stack can inject under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillFault {
    /// Fail the next write_batch with a short-write style I/O error.
    ShortWrite,
    /// Fail finish/publish (rename) once.
    RenameFailure,
    /// Force the next open_reader to report corruption.
    CorruptOnRead,
    /// Cancel the next acquire-style wait (maps to BudgetError::Cancelled).
    Cancel,
    /// Fail the next quota/reserve check as disk full (ENOSPC-style).
    DiskFull,
    /// Fail the next create_writer / reserve as spill quota exceeded.
    QuotaExceeded,
}

/// Process-global (test) fault plan: a queue of faults to inject in order.
static FAULT_QUEUE: Mutex<Vec<SpillFault>> = Mutex::new(Vec::new());
static FAULTS_INJECTED: AtomicUsize = AtomicUsize::new(0);
/// Serialises tests that install or observe faults.
static TEST_SERIAL: Mutex<()> = Mutex::new(());

/// Hold this guard for the whole test body when installing faults (or when
/// running store I/O that must not observe another test's faults).
pub fn serial_test_guard() -> MutexGuard<'static, ()> {
    let guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    clear_faults();
    guard
}

/// RAII session: serialises tests and installs a fault plan; clears on drop.
pub struct FaultSession {
    _serial: MutexGuard<'static, ()>,
}

impl FaultSession {
    pub fn new(faults: Vec<SpillFault>) -> Self {
        let serial = TEST_SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        install_faults(faults);
        Self { _serial: serial }
    }
}

impl Drop for FaultSession {
    fn drop(&mut self) {
        clear_faults();
    }
}

/// Replace the fault queue (test only). Prefer [`FaultSession`].
pub fn install_faults(faults: Vec<SpillFault>) {
    *FAULT_QUEUE.lock().unwrap_or_else(|p| p.into_inner()) = faults;
    FAULTS_INJECTED.store(0, Ordering::Relaxed);
}

/// Clear all pending faults.
pub fn clear_faults() {
    FAULT_QUEUE.lock().unwrap_or_else(|p| p.into_inner()).clear();
}

/// Pop the next matching fault of the given kind, if it is at the head of the queue.
pub fn take_fault(kind: SpillFault) -> bool {
    let mut q = FAULT_QUEUE.lock().unwrap_or_else(|p| p.into_inner());
    if q.first() == Some(&kind) {
        q.remove(0);
        FAULTS_INJECTED.fetch_add(1, Ordering::Relaxed);
        true
    } else {
        false
    }
}

/// How many faults have been consumed since the last install.
pub fn faults_injected() -> usize {
    FAULTS_INJECTED.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_is_fifo_and_kind_matched() {
        let _g = serial_test_guard();
        install_faults(vec![SpillFault::ShortWrite, SpillFault::CorruptOnRead]);
        assert!(!take_fault(SpillFault::CorruptOnRead));
        assert!(take_fault(SpillFault::ShortWrite));
        assert!(take_fault(SpillFault::CorruptOnRead));
        assert!(!take_fault(SpillFault::ShortWrite));
        assert_eq!(faults_injected(), 2);
    }
}
