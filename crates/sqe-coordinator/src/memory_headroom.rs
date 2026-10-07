//! A share of a memory budget that only non-spillable consumers may use
//! (`coordinator.unspillable_headroom_percent`, #788).
//!
//! DataFusion operators that can spill (hash aggregates, sorts, repartition
//! buffers, nested-loop joins) spill only when their own reservation is
//! refused. Consumers that cannot spill (the scan's decode admission, a
//! sort's merge, a hash-join build) fail the query when refused. Under a
//! greedy budget a spillable aggregate grows to the limit and the scan
//! feeding it is refused first, so the query fails where it could have
//! spilled. Here spillable consumers are refused once they would hold more
//! than `limit - headroom`; they spill at that line and the headroom stays
//! available to the non-spillable ones.
//!
//! The refusal is `DataFusionError::ResourcesExhausted`, the error operators
//! treat as "spill now". Nothing waits: admission stays fail-fast (see the
//! `sqe_catalog::scan_memory` module docs for why a blocking scan admission
//! could deadlock).

use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};

/// Bytes charged against one budget, all and spillable, under one lock so a
/// refused spillable request never leaves the two counters out of step.
#[derive(Debug, Default)]
struct Charged {
    all: usize,
    spillable: usize,
}

/// A byte budget whose top `headroom_percent` only non-spillable consumers
/// may use.
#[derive(Debug)]
pub(crate) struct HeadroomBudget {
    /// The whole budget; `None` when the wrapped pool enforces it.
    limit: Option<usize>,
    /// The most spillable consumers may hold together.
    spillable_limit: usize,
    headroom_percent: u8,
    charged: Mutex<Charged>,
}

impl HeadroomBudget {
    /// `limit`: the budget's size. `enforce_total`: whether this budget also
    /// refuses past `limit` (the per-query cap does; the node pool leaves
    /// that to the pool it wraps).
    pub(crate) fn new(limit: usize, headroom_percent: u8, enforce_total: bool) -> Self {
        let headroom = limit / 100 * usize::from(headroom_percent)
            + limit % 100 * usize::from(headroom_percent) / 100;
        Self {
            limit: enforce_total.then_some(limit),
            spillable_limit: limit - headroom,
            headroom_percent,
            charged: Mutex::new(Charged::default()),
        }
    }

    pub(crate) fn charged(&self) -> usize {
        self.lock().all
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Charged> {
        self.charged.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Charge `additional` bytes for `consumer`, or refuse. `what` names the
    /// budget in the refusal.
    pub(crate) fn try_charge(
        &self,
        consumer: &MemoryConsumer,
        additional: usize,
        what: &str,
    ) -> Result<()> {
        let mut charged = self.lock();
        let all = charged.all.saturating_add(additional);
        if let Some(limit) = self.limit {
            if all > limit {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "{what}={limit} bytes: {} requests {additional} additional bytes with {} \
                     already reserved",
                    consumer.name(),
                    charged.all
                )));
            }
        }
        if consumer.can_spill() {
            let spillable = charged.spillable.saturating_add(additional);
            if spillable > self.spillable_limit {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "{what} headroom: spillable operators may hold {} of {} bytes ({}% kept for \
                     scans, merges and join builds); {} requests {additional} additional bytes \
                     with {} spillable bytes reserved",
                    self.spillable_limit,
                    self.limit
                        .map_or_else(|| "the pool's".to_string(), |l| l.to_string()),
                    self.headroom_percent,
                    consumer.name(),
                    charged.spillable
                )));
            }
            charged.spillable = spillable;
        }
        charged.all = all;
        Ok(())
    }

    /// Charge without refusing (DataFusion's infallible `grow`).
    pub(crate) fn charge(&self, consumer: &MemoryConsumer, additional: usize) {
        let mut charged = self.lock();
        charged.all = charged.all.saturating_add(additional);
        if consumer.can_spill() {
            charged.spillable = charged.spillable.saturating_add(additional);
        }
    }

    /// Release `bytes` previously charged for `consumer`. A spilling
    /// operator frees through here, which reopens the spillable budget.
    pub(crate) fn release(&self, consumer: &MemoryConsumer, bytes: usize) {
        let mut charged = self.lock();
        charged.all = charged.all.saturating_sub(bytes);
        if consumer.can_spill() {
            charged.spillable = charged.spillable.saturating_sub(bytes);
        }
    }
}

/// The node pool with a headroom: the wrapped pool still enforces the total
/// and tracks consumers for error reports; this layer only refuses spillable
/// growth past `limit - headroom`.
#[derive(Debug)]
pub(crate) struct HeadroomPool {
    inner: Arc<dyn MemoryPool>,
    budget: HeadroomBudget,
}

impl HeadroomPool {
    /// Wrap `inner` (of `limit` bytes). A zero headroom returns `inner`
    /// unchanged.
    pub(crate) fn wrap(
        inner: Arc<dyn MemoryPool>,
        limit: usize,
        headroom_percent: u8,
    ) -> Arc<dyn MemoryPool> {
        if headroom_percent == 0 {
            return inner;
        }
        Arc::new(Self {
            inner,
            budget: HeadroomBudget::new(limit, headroom_percent, false),
        })
    }
}

impl fmt::Display for HeadroomPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} with {}% unspillable headroom",
            self.inner, self.budget.headroom_percent
        )
    }
}

impl MemoryPool for HeadroomPool {
    fn name(&self) -> &str {
        "HeadroomPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.budget.charge(reservation.consumer(), additional);
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.budget.release(reservation.consumer(), shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.budget.try_charge(
            reservation.consumer(),
            additional,
            "coordinator.unspillable_headroom_percent",
        )?;
        if let Err(error) = self.inner.try_grow(reservation, additional) {
            self.budget.release(reservation.consumer(), additional);
            return Err(error);
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;

    fn node(limit: usize, percent: u8) -> Arc<dyn MemoryPool> {
        HeadroomPool::wrap(Arc::new(GreedyMemoryPool::new(limit)), limit, percent)
    }

    #[test]
    fn spillable_consumers_stop_at_the_headroom_line() {
        let pool = node(1000, 20);
        let agg = MemoryConsumer::new("agg")
            .with_can_spill(true)
            .register(&pool);
        agg.try_grow(800).unwrap();
        let err = agg.try_grow(1).unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "the refusal must be the error operators spill on: {err}"
        );
        assert!(err.to_string().contains("headroom"), "{err}");
    }

    #[test]
    fn non_spillable_consumers_use_the_headroom() {
        let pool = node(1000, 20);
        let agg = MemoryConsumer::new("agg")
            .with_can_spill(true)
            .register(&pool);
        let scan = MemoryConsumer::new("scan").register(&pool);
        agg.try_grow(800).unwrap();
        scan.try_grow(200).unwrap();
        // The wrapped pool still enforces the total.
        assert!(scan.try_grow(1).is_err());
    }

    #[test]
    fn a_spill_reopens_the_spillable_budget() {
        let pool = node(1000, 20);
        let agg = MemoryConsumer::new("agg")
            .with_can_spill(true)
            .register(&pool);
        agg.try_grow(800).unwrap();
        assert!(agg.try_grow(100).is_err());
        agg.shrink(500);
        agg.try_grow(400).unwrap();
        agg.try_resize(0).unwrap();
        assert_eq!(pool.reserved(), 0);
        agg.try_grow(800).unwrap();
    }

    #[test]
    fn infallible_grow_counts_without_refusing() {
        let pool = node(1000, 20);
        let sort = MemoryConsumer::new("sort")
            .with_can_spill(true)
            .register(&pool);
        sort.grow(900);
        assert!(sort.try_grow(1).is_err());
        sort.shrink(900);
        sort.try_grow(800).unwrap();
    }

    #[test]
    fn a_refused_inner_grow_rolls_back_the_charge() {
        let pool = node(1000, 20);
        let build = MemoryConsumer::new("build").register(&pool);
        build.try_grow(900).unwrap();
        let agg = MemoryConsumer::new("agg")
            .with_can_spill(true)
            .register(&pool);
        // The headroom admits 200, the full pool refuses it.
        assert!(agg.try_grow(200).is_err());
        build.shrink(900);
        agg.try_grow(800).unwrap();
    }

    #[test]
    fn zero_headroom_is_the_wrapped_pool() {
        let pool = node(1000, 0);
        assert_eq!(pool.name(), "greedy");
        let agg = MemoryConsumer::new("agg")
            .with_can_spill(true)
            .register(&pool);
        agg.try_grow(1000).unwrap();
    }

    #[test]
    fn a_query_budget_enforces_both_limits() {
        let budget = HeadroomBudget::new(1000, 20, true);
        let agg = MemoryConsumer::new("agg").with_can_spill(true);
        let scan = MemoryConsumer::new("scan");
        budget.try_charge(&agg, 800, "q").unwrap();
        assert!(budget.try_charge(&agg, 1, "q").is_err());
        budget.try_charge(&scan, 200, "q").unwrap();
        let err = budget.try_charge(&scan, 1, "q").unwrap_err();
        assert!(err.to_string().contains("q=1000 bytes"), "{err}");
        assert_eq!(budget.charged(), 1000);
        budget.release(&agg, 800);
        budget.try_charge(&agg, 800, "q").unwrap();
    }
}
