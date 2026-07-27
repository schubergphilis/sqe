//! Fair spill pool with a **hot-resizable** limit.
//!
//! DataFusion's [`FairSpillPool`] fixes `pool_size` at construction. Worker
//! config hot-reload needs the same fairness rules with an atomic limit so
//! `worker.memory_limit` can change without rebuilding `SessionContext`.
//!
//! Semantics match FairSpillPool; shrinking the limit only affects **new**
//! `try_grow` decisions. Existing reservations may temporarily sit above the
//! new cap until operators free memory (same class of behaviour as a live
//! cgroup shrink relative to the DF pool).

use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{
    human_readable_size, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};

/// Fair division among spillable consumers with a mutable finite limit.
#[derive(Debug)]
pub struct ResizableFairSpillPool {
    pool_size: AtomicUsize,
    state: Mutex<PoolState>,
}

#[derive(Debug, Default)]
struct PoolState {
    num_spill: usize,
    spillable: usize,
    unspillable: usize,
}

impl ResizableFairSpillPool {
    pub fn new(pool_size: usize) -> Self {
        let pool_size = pool_size.max(1);
        tracing::debug!(pool_size, "Created ResizableFairSpillPool");
        Self {
            pool_size: AtomicUsize::new(pool_size),
            state: Mutex::new(PoolState::default()),
        }
    }

    pub fn pool_size(&self) -> usize {
        self.pool_size.load(Ordering::Acquire)
    }

    /// Update the finite limit used by subsequent `try_grow` calls.
    pub fn set_pool_size(&self, pool_size: usize) {
        let pool_size = pool_size.max(1);
        let prev = self.pool_size.swap(pool_size, Ordering::AcqRel);
        if prev != pool_size {
            tracing::info!(
                previous_pool_size = prev,
                new_pool_size = pool_size,
                reserved = self.reserved(),
                "ResizableFairSpillPool limit updated (hot config reload)"
            );
        }
    }
}

impl MemoryPool for ResizableFairSpillPool {
    fn name(&self) -> &str {
        "fair-resizable"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill() {
            if let Ok(mut state) = self.state.lock() {
                state.num_spill += 1;
            }
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill() {
            if let Ok(mut state) = self.state.lock() {
                state.num_spill = state.num_spill.saturating_sub(1);
            }
        }
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        if let Ok(mut state) = self.state.lock() {
            match reservation.consumer().can_spill() {
                true => state.spillable = state.spillable.saturating_add(additional),
                false => state.unspillable = state.unspillable.saturating_add(additional),
            }
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        if let Ok(mut state) = self.state.lock() {
            match reservation.consumer().can_spill() {
                true => state.spillable = state.spillable.saturating_sub(shrink),
                false => state.unspillable = state.unspillable.saturating_sub(shrink),
            }
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let pool_size = self.pool_size();
        let mut state = self.state.lock().map_err(|_| {
            DataFusionError::Internal("resizable memory pool lock poisoned".into())
        })?;

        match reservation.consumer().can_spill() {
            true => {
                let spill_available = pool_size.saturating_sub(state.unspillable);
                let available = if state.num_spill == 0 {
                    spill_available
                } else {
                    spill_available / state.num_spill
                };
                if reservation.size() + additional > available {
                    return Err(insufficient_capacity_err(
                        reservation,
                        additional,
                        available,
                        self,
                    ));
                }
                state.spillable = state.spillable.saturating_add(additional);
            }
            false => {
                let available = pool_size.saturating_sub(state.unspillable + state.spillable);
                if available < additional {
                    return Err(insufficient_capacity_err(
                        reservation,
                        additional,
                        available,
                        self,
                    ));
                }
                state.unspillable = state.unspillable.saturating_add(additional);
            }
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state
            .lock()
            .map(|s| s.spillable + s.unspillable)
            .unwrap_or(0)
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.pool_size())
    }
}

impl Display for ResizableFairSpillPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(pool_size: {})",
            self.name(),
            human_readable_size(self.pool_size()),
        )
    }
}

fn insufficient_capacity_err(
    reservation: &MemoryReservation,
    additional: usize,
    available: usize,
    pool: &impl MemoryPool,
) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!(
        "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total memory pool: {}",
        human_readable_size(additional),
        reservation.consumer().name(),
        human_readable_size(reservation.size()),
        human_readable_size(available),
        pool,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryConsumer;
    use std::sync::Arc;

    #[test]
    fn set_pool_size_affects_limit() {
        let pool = Arc::new(ResizableFairSpillPool::new(1024 * 1024));
        assert_eq!(pool.pool_size(), 1024 * 1024);
        pool.set_pool_size(2 * 1024 * 1024);
        assert_eq!(pool.pool_size(), 2 * 1024 * 1024);
        match pool.memory_limit() {
            MemoryLimit::Finite(n) => assert_eq!(n, 2 * 1024 * 1024),
            MemoryLimit::Infinite | MemoryLimit::Unknown => {
                panic!("expected finite limit")
            }
        }
    }

    #[test]
    fn try_grow_respects_resized_limit() {
        let concrete = Arc::new(ResizableFairSpillPool::new(64 * 1024));
        let pool: Arc<dyn MemoryPool> = concrete.clone();
        let consumer = MemoryConsumer::new("t").with_can_spill(true);
        let r = consumer.register(&pool);
        r.try_grow(32 * 1024).expect("half should fit");
        concrete.set_pool_size(16 * 1024);
        // Per-spiller fair share is now 16KiB; already at 32KiB so further grow fails.
        assert!(r.try_grow(1).is_err());
    }
}
