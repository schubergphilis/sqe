//! Worker memory pool sizing: pool bytes minus spill-read headroom, resizable
//! under hot config reload.
//!
//! The admission machinery that used to live here (grants, workload classes,
//! reclaim) had one consumer, the feature-gated DoExchange shuffle intake,
//! which is gone. What remains is the arithmetic the bootstrap log and
//! `config_reload` actually read.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::budget::split_default_read_headroom;

/// Process-wide pool sizing for one worker.
pub struct MemoryGovernor {
    /// Total bytes managed. Atomic so hot config reload can resize without
    /// replacing the Arc.
    pool_bytes: AtomicUsize,
    /// Reserved spill-read / merge headroom not handed to operators.
    headroom_bytes: AtomicUsize,
    /// Distributable capacity (= pool - headroom).
    distributable: AtomicUsize,
}

impl MemoryGovernor {
    /// Create a governor over `pool_bytes`, carving default read headroom.
    pub fn new(pool_bytes: usize) -> Self {
        let pool_bytes = pool_bytes.max(1);
        let (distributable, headroom) = split_default_read_headroom(pool_bytes);
        Self {
            pool_bytes: AtomicUsize::new(pool_bytes),
            headroom_bytes: AtomicUsize::new(headroom),
            distributable: AtomicUsize::new(distributable),
        }
    }

    pub fn pool_bytes(&self) -> usize {
        self.pool_bytes.load(Ordering::Acquire)
    }

    pub fn headroom_bytes(&self) -> usize {
        self.headroom_bytes.load(Ordering::Acquire)
    }

    pub fn distributable_bytes(&self) -> usize {
        self.distributable.load(Ordering::Acquire)
    }

    /// Hot-reload: resize the pool. Nothing holds grants against it, so the
    /// resize always applies; the `Result` stays so `config_reload` keeps one
    /// code path for the memory-pool and governor windows.
    pub fn try_resize_pool(&self, new_pool_bytes: usize) -> Result<(), String> {
        let new_pool = new_pool_bytes.max(1);
        let (distributable, headroom) = split_default_read_headroom(new_pool);
        let prev = self.pool_bytes.swap(new_pool, Ordering::AcqRel);
        self.headroom_bytes.store(headroom, Ordering::Release);
        self.distributable.store(distributable, Ordering::Release);
        tracing::info!(
            previous_pool_bytes = prev,
            new_pool_bytes = new_pool,
            distributable,
            headroom,
            "MemoryGovernor pool resized (hot config reload)"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_resize_pool_grows_and_shrinks() {
        let gov = MemoryGovernor::new(16 * 1024 * 1024);
        assert_eq!(gov.pool_bytes(), 16 * 1024 * 1024);
        assert_eq!(gov.distributable_bytes() + gov.headroom_bytes(), gov.pool_bytes());
        gov.try_resize_pool(32 * 1024 * 1024).expect("grow");
        assert_eq!(gov.pool_bytes(), 32 * 1024 * 1024);
        gov.try_resize_pool(8 * 1024 * 1024).expect("shrink");
        assert_eq!(gov.pool_bytes(), 8 * 1024 * 1024);
    }
}
