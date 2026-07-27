//! Deterministic fault injection for spill tests.
//!
//! Production builds leave the injector inert. Tests install a fault plan
//! that forces typed failures (short write, corruption, cancel) without
//! needing real ENOSPC or disk faults.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

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
}

/// Process-global (test) fault plan: a queue of faults to inject in order.
static FAULT_QUEUE: Mutex<Vec<SpillFault>> = Mutex::new(Vec::new());
static FAULTS_INJECTED: AtomicUsize = AtomicUsize::new(0);

/// Replace the fault queue (test only).
pub fn install_faults(faults: Vec<SpillFault>) {
    *FAULT_QUEUE.lock().unwrap() = faults;
    FAULTS_INJECTED.store(0, Ordering::Relaxed);
}

/// Clear all pending faults.
pub fn clear_faults() {
    FAULT_QUEUE.lock().unwrap().clear();
}

/// Pop the next matching fault of the given kind, if it is at the head of the queue.
pub fn take_fault(kind: SpillFault) -> bool {
    let mut q = FAULT_QUEUE.lock().unwrap();
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
        clear_faults();
        install_faults(vec![SpillFault::ShortWrite, SpillFault::CorruptOnRead]);
        assert!(!take_fault(SpillFault::CorruptOnRead));
        assert!(take_fault(SpillFault::ShortWrite));
        assert!(take_fault(SpillFault::CorruptOnRead));
        assert!(!take_fault(SpillFault::ShortWrite));
        assert_eq!(faults_injected(), 2);
        clear_faults();
    }
}
