//! Ownership-based byte budgets backed by fixed-size accounting units and an
//! optional DataFusion [`MemoryPool`].
//!
//! Each [`BytePermit`] holds both a budget-unit grant and (when a pool is
//! configured) a live [`MemoryReservation`]. Dropping the permit releases both,
//! including on panic unwind. Moving a permit does not double-charge.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{BudgetError, Result};

/// Default accounting unit: 64 KiB. Every charge rounds up to a multiple of
/// this size so the semaphore can track capacity with a bounded permit count.
pub const DEFAULT_BUDGET_GRANULARITY: usize = 64 * 1024;

/// Default fraction of a shuffle/scan parent budget reserved for spill-read
/// and merge headroom (1/4 = 25%). Writers must not consume this slice so a
/// slow consumer can still stream spilled segments without waiting for
/// writers to free memory.
pub const DEFAULT_READ_HEADROOM_NUM: usize = 1;
pub const DEFAULT_READ_HEADROOM_DEN: usize = 4;

/// Split a parent capacity into `(writer_capacity, read_headroom)`.
///
/// `read_headroom` is at least one accounting unit when `capacity > 0`, and
/// `writer_capacity + read_headroom == capacity` (modulo zero-capacity).
pub fn split_read_headroom(
    capacity: usize,
    headroom_num: usize,
    headroom_den: usize,
) -> (usize, usize) {
    if capacity == 0 {
        return (0, 0);
    }
    let den = headroom_den.max(1);
    let num = headroom_num.min(den);
    let mut headroom = capacity.saturating_mul(num) / den;
    if headroom == 0 && num > 0 {
        headroom = 1;
    }
    if headroom >= capacity {
        // Always leave the writer at least 1 byte of capacity when possible.
        headroom = capacity.saturating_sub(1);
    }
    let writer = capacity.saturating_sub(headroom);
    (
        writer.max(1),
        headroom.max(1).min(capacity.saturating_sub(writer)),
    )
}

/// Convenience: default 25% read headroom.
pub fn split_default_read_headroom(capacity: usize) -> (usize, usize) {
    split_read_headroom(
        capacity,
        DEFAULT_READ_HEADROOM_NUM,
        DEFAULT_READ_HEADROOM_DEN,
    )
}

/// A named byte budget with a hard capacity.
///
/// Admission is two-layer:
/// 1. **Budget capacity** — a semaphore of fixed-size units. `acquire` waits
///    when the budget is full (backpressure). A single request larger than the
///    whole capacity fails with [`BudgetError::ItemTooLarge`].
/// 2. **Worker pool** (optional) — each permit owns a [`MemoryReservation`]
///    grown via `try_grow`. This keeps DataFusion operators aware of scan /
///    Flight / shuffle buffer residency under the same `worker.memory_limit`.
#[derive(Clone, Debug)]
pub struct ByteBudget {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    name: String,
    capacity_bytes: usize,
    unit_bytes: usize,
    capacity_units: u32,
    /// Free units available for acquire.
    units: Arc<Semaphore>,
    used_units: AtomicUsize,
    /// Optional shared DataFusion pool. When set, each permit reserves against it.
    pool: Option<Arc<dyn MemoryPool>>,
}

/// Owned grant of budget capacity (and optional pool reservation).
///
/// Drop releases units and shrinks the pool reservation exactly once.
#[derive(Debug)]
pub struct BytePermit {
    inner: Arc<Inner>,
    /// Rounded byte charge (multiple of unit size, ≥ requested).
    charged_bytes: usize,
    units: u32,
    /// Keeps the semaphore permit alive until drop.
    _sem: OwnedSemaphorePermit,
    /// Pool reservation; dropped to free pool bytes.
    reservation: Option<MemoryReservation>,
}

impl ByteBudget {
    /// Create a budget with the given capacity and default 64 KiB units.
    ///
    /// When `pool` is `Some`, every successful acquire also reserves against
    /// that pool so worker-wide accounting stays coherent.
    pub fn new(
        name: impl Into<String>,
        capacity_bytes: usize,
        pool: Option<Arc<dyn MemoryPool>>,
    ) -> Self {
        Self::with_granularity(name, capacity_bytes, DEFAULT_BUDGET_GRANULARITY, pool)
    }

    /// Create a budget with an explicit unit size (primarily for tests).
    pub fn with_granularity(
        name: impl Into<String>,
        capacity_bytes: usize,
        unit_bytes: usize,
        pool: Option<Arc<dyn MemoryPool>>,
    ) -> Self {
        assert!(unit_bytes > 0, "unit_bytes must be > 0");
        let name = name.into();
        // Capacity of 0 is allowed (every acquire fails as ItemTooLarge / Insufficient).
        let capacity_units = if capacity_bytes == 0 {
            0u32
        } else {
            // At least one unit so tiny budgets still work.
            units_for(capacity_bytes, unit_bytes).max(1) as u32
        };
        // Align reported capacity to whole units so capacity_bytes() matches what
        // can actually be granted.
        let aligned_capacity = capacity_units as usize * unit_bytes;
        Self {
            inner: Arc::new(Inner {
                name,
                capacity_bytes: aligned_capacity,
                unit_bytes,
                capacity_units,
                units: Arc::new(Semaphore::new(capacity_units as usize)),
                used_units: AtomicUsize::new(0),
                pool,
            }),
        }
    }

    /// Budget name (used in error messages and pool consumer labels).
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Total capacity in bytes (aligned to unit size).
    pub fn capacity_bytes(&self) -> usize {
        self.inner.capacity_bytes
    }

    /// Accounting unit size in bytes.
    pub fn unit_bytes(&self) -> usize {
        self.inner.unit_bytes
    }

    /// Bytes currently charged (sum of live permits, rounded).
    pub fn used_bytes(&self) -> usize {
        self.inner.used_units.load(Ordering::Relaxed) * self.inner.unit_bytes
    }

    /// Free capacity in bytes (approximate under concurrency).
    pub fn free_bytes(&self) -> usize {
        self.capacity_bytes().saturating_sub(self.used_bytes())
    }

    /// Round `bytes` up to a whole number of accounting units, then wait until
    /// the budget can grant them. Also reserves against the worker pool when
    /// configured.
    pub async fn acquire(&self, bytes: usize) -> Result<BytePermit> {
        let units = self.units_required(bytes)?;
        let sem = self
            .inner
            .units
            .clone()
            .acquire_many_owned(units)
            .await
            .map_err(|_| BudgetError::Cancelled {
                budget: self.inner.name.clone(),
            })?;
        // On pool failure, finish_acquire drops `sem` before returning Err so
        // budget units are not leaked.
        self.finish_acquire(bytes, units, sem)
    }

    /// Non-blocking acquire. Fails with [`BudgetError::InsufficientCapacity`]
    /// when the budget cannot grant immediately.
    pub fn try_acquire(&self, bytes: usize) -> Result<BytePermit> {
        let units = self.units_required(bytes)?;
        let sem = self
            .inner
            .units
            .clone()
            .try_acquire_many_owned(units)
            .map_err(|_| BudgetError::InsufficientCapacity {
                budget: self.inner.name.clone(),
                requested: rounded_bytes(bytes, self.inner.unit_bytes),
                capacity: self.inner.capacity_bytes,
                used: self.used_bytes(),
            })?;
        self.finish_acquire(bytes, units, sem)
    }

    fn units_required(&self, bytes: usize) -> Result<u32> {
        if bytes == 0 {
            // Zero-byte charge still takes one unit so Drop accounting is uniform
            // and empty batches do not bypass the budget entirely.
            if self.inner.capacity_units == 0 {
                return Err(BudgetError::ItemTooLarge {
                    budget: self.inner.name.clone(),
                    requested: 0,
                    capacity: 0,
                });
            }
            return Ok(1);
        }
        let units = units_for(bytes, self.inner.unit_bytes) as u32;
        if units > self.inner.capacity_units {
            return Err(BudgetError::ItemTooLarge {
                budget: self.inner.name.clone(),
                requested: bytes,
                capacity: self.inner.capacity_bytes,
            });
        }
        Ok(units)
    }

    fn finish_acquire(
        &self,
        requested: usize,
        units: u32,
        sem: OwnedSemaphorePermit,
    ) -> Result<BytePermit> {
        let charged_bytes = units as usize * self.inner.unit_bytes;
        let reservation = if let Some(ref pool) = self.inner.pool {
            let consumer = MemoryConsumer::new(format!("budget:{}", self.inner.name));
            let reservation = consumer.register(pool);
            if let Err(e) = reservation.try_grow(charged_bytes) {
                // Release semaphore by dropping `sem` when we return Err —
                // move sem into a local that drops.
                drop(sem);
                return Err(BudgetError::PoolExhausted {
                    budget: self.inner.name.clone(),
                    source: e,
                });
            }
            Some(reservation)
        } else {
            None
        };

        self.inner
            .used_units
            .fetch_add(units as usize, Ordering::Relaxed);

        // Silence unused-var for requested in non-debug builds.
        let _ = requested;

        Ok(BytePermit {
            inner: self.inner.clone(),
            charged_bytes,
            units,
            _sem: sem,
            reservation,
        })
    }
}

impl BytePermit {
    /// Bytes charged for this permit (rounded up to unit size).
    pub fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }

    /// Number of accounting units held.
    pub fn units(&self) -> u32 {
        self.units
    }
}

impl Drop for BytePermit {
    fn drop(&mut self) {
        self.inner
            .used_units
            .fetch_sub(self.units as usize, Ordering::Relaxed);
        // `_sem` and `reservation` drop automatically: semaphore units return
        // and the pool reservation shrinks to zero.
        if let Some(ref mut r) = self.reservation {
            // Explicit free is redundant with Drop but documents intent.
            let size = r.size();
            if size > 0 {
                r.shrink(size);
            }
        }
    }
}

/// Round `bytes` up to a multiple of `unit_bytes`.
pub fn rounded_bytes(bytes: usize, unit_bytes: usize) -> usize {
    units_for(bytes, unit_bytes) * unit_bytes
}

fn units_for(bytes: usize, unit_bytes: usize) -> usize {
    debug_assert!(unit_bytes > 0);
    bytes.div_ceil(unit_bytes).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::FairSpillPool;
    use std::sync::Arc;

    fn pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(FairSpillPool::new(limit))
    }

    #[tokio::test]
    async fn acquire_and_drop_releases_exactly_once() {
        let budget = ByteBudget::with_granularity("t", 1024, 256, None);
        assert_eq!(budget.capacity_bytes(), 1024);
        assert_eq!(budget.used_bytes(), 0);

        let p = budget.acquire(1).await.unwrap();
        // 1 byte rounds to 1 unit = 256.
        assert_eq!(p.charged_bytes(), 256);
        assert_eq!(budget.used_bytes(), 256);

        drop(p);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn try_acquire_fails_when_full() {
        let budget = ByteBudget::with_granularity("full", 512, 256, None);
        let a = budget.try_acquire(256).unwrap();
        let b = budget.try_acquire(256).unwrap();
        assert!(matches!(
            budget.try_acquire(1),
            Err(BudgetError::InsufficientCapacity { .. })
        ));
        drop(a);
        drop(b);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn item_too_large_does_not_wait() {
        let budget = ByteBudget::with_granularity("small", 512, 256, None);
        let err = budget.acquire(1024).await.unwrap_err();
        assert!(matches!(err, BudgetError::ItemTooLarge { .. }));
    }

    #[tokio::test]
    async fn pool_backed_acquire_shows_in_pool_reserved() {
        let p = pool(4096);
        let budget = ByteBudget::with_granularity("pool", 2048, 512, Some(p.clone()));
        assert_eq!(p.reserved(), 0);
        let permit = budget.acquire(100).await.unwrap();
        assert_eq!(permit.charged_bytes(), 512);
        assert_eq!(p.reserved(), 512);
        drop(permit);
        assert_eq!(p.reserved(), 0);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn pool_exhausted_releases_budget_units() {
        // Pool smaller than one unit charge after rounding.
        let p = pool(100);
        let budget = ByteBudget::with_granularity("pool-small", 4096, 512, Some(p.clone()));
        let err = budget.acquire(1).await.unwrap_err();
        assert!(matches!(err, BudgetError::PoolExhausted { .. }));
        // Budget units must be free again so a later acquire can retry.
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(p.reserved(), 0);
    }

    #[tokio::test]
    async fn acquire_waits_until_permit_released() {
        let budget = ByteBudget::with_granularity("wait", 256, 256, None);
        let held = budget.acquire(1).await.unwrap();

        let budget2 = budget.clone();
        let waiter = tokio::spawn(async move { budget2.acquire(1).await });

        // Give the waiter time to block on the semaphore.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        drop(held);
        let got = waiter.await.unwrap().unwrap();
        assert_eq!(got.charged_bytes(), 256);
    }

    #[tokio::test]
    async fn panic_unwind_releases_permit() {
        let budget = ByteBudget::with_granularity("panic", 1024, 256, None);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let p = budget.acquire(1).await.unwrap();
                assert_eq!(budget.used_bytes(), 256);
                // Move permit into a scope that panics.
                let _guard = p;
                panic!("boom");
            });
        }));
        assert!(result.is_err());
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn fairness_fifo_under_contention() {
        // Small budget: one unit. Two waiters; release order should match
        // acquire order (tokio Semaphore is fair).
        let budget = ByteBudget::with_granularity("fair", 64, 64, None);
        let first = budget.acquire(1).await.unwrap();

        let b1 = budget.clone();
        let b2 = budget.clone();
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let o1 = order.clone();
        let o2 = order.clone();

        let t1 = tokio::spawn(async move {
            let p = b1.acquire(1).await.unwrap();
            o1.lock().unwrap().push(1u8);
            drop(p);
        });
        tokio::task::yield_now().await;
        let t2 = tokio::spawn(async move {
            let p = b2.acquire(1).await.unwrap();
            o2.lock().unwrap().push(2u8);
            drop(p);
        });
        tokio::task::yield_now().await;

        drop(first);
        t1.await.unwrap();
        t2.await.unwrap();
        let seen = order.lock().unwrap().clone();
        assert_eq!(seen, vec![1, 2], "semaphore waiters should run FIFO");
    }

    #[test]
    fn split_read_headroom_default_quarter() {
        let (w, r) = split_default_read_headroom(1024);
        assert_eq!(w + r, 1024);
        assert_eq!(r, 256);
        assert_eq!(w, 768);
    }

    #[test]
    fn split_read_headroom_tiny_capacity() {
        // Prefer leaving the writer at least 1 byte; headroom may be 0.
        let (w, r) = split_default_read_headroom(1);
        assert_eq!(w, 1);
        assert_eq!(r, 0);
        let (w4, r4) = split_default_read_headroom(4);
        assert_eq!(w4 + r4, 4);
        assert_eq!(r4, 1);
        assert_eq!(w4, 3);
    }

    #[test]
    fn split_read_headroom_zero() {
        assert_eq!(split_default_read_headroom(0), (0, 0));
    }
}
