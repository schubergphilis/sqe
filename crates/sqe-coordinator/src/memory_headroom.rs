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
//!
//! Both shares are greedy by default: one consumer may take all of the
//! spillable bytes, and a non-spillable one everything else. A sort refused
//! while it holds nothing has nothing to spill and fails the query. A
//! budget built [`HeadroomBudget::with_fair_share`] caps the growth of each
//! consumer that spills or merges fewer files at an equal share of the
//! budget instead, so a sort that has not started yet still finds room (the
//! join fallback's retry, #784).

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
    /// Consumers registered on the budget's pool.
    consumers: usize,
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
    /// Whether each consumer's growth is capped at an equal share.
    fair_share: bool,
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
            fair_share: false,
            charged: Mutex::new(Charged::default()),
        }
    }

    /// Refuse a consumer that adapts to a refusal ([`adapts_when_refused`])
    /// growth past an equal share of the budget per registered consumer.
    /// Its first reservation is exempt (the other limits still apply), so a
    /// sort's merge pre-reservation is never refused for its size alone.
    ///
    /// Capping the consumers that adapt keeps a share free for every
    /// consumer that holds nothing yet, which is what a sort needs: refused
    /// while it holds nothing, it has nothing to spill and fails the query.
    /// The cost is earlier spills and more merge passes when one consumer
    /// could have used more than its share. Consumers that fail the query
    /// when refused (a scan's decode admission correcting its estimate, a
    /// top-k heap, an ungrouped aggregate) stay greedy.
    pub(crate) fn with_fair_share(mut self) -> Self {
        self.fair_share = true;
        self
    }

    pub(crate) fn charged(&self) -> usize {
        self.lock().all
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Charged> {
        self.charged.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Count `consumer` towards the fair share.
    pub(crate) fn register(&self, _consumer: &MemoryConsumer) {
        self.lock().consumers += 1;
    }

    pub(crate) fn unregister(&self, _consumer: &MemoryConsumer) {
        let mut charged = self.lock();
        charged.consumers = charged.consumers.saturating_sub(1);
    }

    /// The most one consumer may grow to under the fair share.
    fn share(&self, charged: &Charged) -> usize {
        self.limit.unwrap_or(self.spillable_limit) / charged.consumers.max(1)
    }

    /// Charge `additional` bytes for `consumer`, which already holds `held`,
    /// or refuse. `what` names the budget in the refusal.
    pub(crate) fn try_charge(
        &self,
        consumer: &MemoryConsumer,
        held: usize,
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
        if self.fair_share && held > 0 && adapts_when_refused(consumer) {
            let share = self.share(&charged);
            if held.saturating_add(additional) > share {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "{what} fair share: each of {} operators may grow to {share} bytes; {} \
                     requests {additional} additional bytes with {held} reserved",
                    charged.consumers,
                    consumer.name(),
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

/// Whether `consumer` continues when its growth is refused: it spills, or
/// it is a sort's merge reservation (DataFusion's `ExternalSorterMerge[n]`),
/// whose spill-file merge then merges fewer files per pass.
fn adapts_when_refused(consumer: &MemoryConsumer) -> bool {
    consumer.can_spill() || consumer.name().starts_with("ExternalSorterMerge[")
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
        self.budget.register(consumer);
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
        self.budget.unregister(consumer);
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
            reservation.size(),
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
        budget.try_charge(&agg, 0, 800, "q").unwrap();
        assert!(budget.try_charge(&agg, 800, 1, "q").is_err());
        budget.try_charge(&scan, 0, 200, "q").unwrap();
        let err = budget.try_charge(&scan, 200, 1, "q").unwrap_err();
        assert!(err.to_string().contains("q=1000 bytes"), "{err}");
        assert_eq!(budget.charged(), 1000);
        budget.release(&agg, 800);
        budget.try_charge(&agg, 0, 800, "q").unwrap();
    }

    /// A 1000-byte query budget with 20% headroom and `consumers`
    /// registered, fair or greedy.
    fn query_budget(fair: bool, consumers: &[&MemoryConsumer]) -> HeadroomBudget {
        let budget = HeadroomBudget::new(1000, 20, true);
        let budget = if fair {
            budget.with_fair_share()
        } else {
            budget
        };
        for consumer in consumers {
            budget.register(consumer);
        }
        budget
    }

    /// Grow `consumer` from `first` in `step`s until the budget refuses;
    /// return what it holds then.
    fn grow_until_refused(
        budget: &HeadroomBudget,
        consumer: &MemoryConsumer,
        first: usize,
        step: usize,
    ) -> usize {
        budget.try_charge(consumer, 0, first, "q").unwrap();
        let mut held = first;
        while budget.try_charge(consumer, held, step, "q").is_ok() {
            held += step;
        }
        held
    }

    #[test]
    fn a_sorter_that_holds_nothing_finds_room_beside_a_spillable_one() {
        // #784, CI job 5550766: one sorter grew to the spillable line and
        // the other, holding nothing it could spill, was refused.
        let a = MemoryConsumer::new("ExternalSorter[0]").with_can_spill(true);
        let b = MemoryConsumer::new("ExternalSorter[1]").with_can_spill(true);
        let greedy = query_budget(false, &[&a, &b]);
        assert_eq!(grow_until_refused(&greedy, &a, 100, 100), 800);
        assert!(greedy.try_charge(&b, 0, 100, "q").is_err());

        let fair = query_budget(true, &[&a, &b]);
        assert_eq!(grow_until_refused(&fair, &a, 100, 100), 500);
        let err = fair.try_charge(&a, 500, 1, "q").unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "the refusal must be the error operators spill on: {err}"
        );
        assert!(err.to_string().contains("fair share"), "{err}");
        // The spillable line still holds: 300 of 800 is left for b.
        fair.try_charge(&b, 0, 300, "q").unwrap();
        assert!(fair.try_charge(&b, 300, 1, "q").is_err());
    }

    #[test]
    fn a_growing_merge_leaves_room_for_the_sorters_that_have_not_started() {
        // The other #784 failure: the first join side's spill merge, which
        // cannot spill, grew to the whole budget before the second side's
        // sorters reserved anything.
        let merge = MemoryConsumer::new("ExternalSorterMerge[0]");
        let s1 = MemoryConsumer::new("ExternalSorter[1]").with_can_spill(true);
        let s2 = MemoryConsumer::new("ExternalSorter[2]").with_can_spill(true);
        let greedy = query_budget(false, &[&merge, &s1, &s2]);
        assert_eq!(grow_until_refused(&greedy, &merge, 100, 100), 1000);
        assert!(greedy.try_charge(&s1, 0, 100, "q").is_err());

        let fair = query_budget(true, &[&merge, &s1, &s2]);
        assert_eq!(grow_until_refused(&fair, &merge, 100, 100), 300);
        fair.try_charge(&s1, 0, 300, "q").unwrap();
        fair.try_charge(&s2, 0, 300, "q").unwrap();
        assert_eq!(fair.charged(), 900);
    }

    #[test]
    fn a_first_reservation_is_exempt_from_the_fair_share() {
        let consumers: Vec<_> = (0..4)
            .map(|i| MemoryConsumer::new(format!("ExternalSorterMerge[{i}]")))
            .collect();
        let budget = query_budget(true, &consumers.iter().collect::<Vec<_>>());
        // The share is 250; a merge pre-reservation arrives in one piece.
        budget.try_charge(&consumers[0], 0, 600, "q").unwrap();
        let err = budget.try_charge(&consumers[0], 600, 1, "q").unwrap_err();
        assert!(err.to_string().contains("fair share"), "{err}");
        // The total still applies to a first reservation.
        assert!(budget.try_charge(&consumers[1], 0, 401, "q").is_err());
        budget.try_charge(&consumers[1], 0, 400, "q").unwrap();
    }

    #[test]
    fn consumers_that_fail_when_refused_stay_greedy() {
        // A scan's decode admission corrects its compressed-size estimate
        // upwards once decoded; refused, the query fails.
        let scan = MemoryConsumer::new("scan-decode");
        let sorts: Vec<_> = (0..3)
            .map(|i| MemoryConsumer::new(format!("ExternalSorter[{i}]")).with_can_spill(true))
            .collect();
        let mut all: Vec<_> = sorts.iter().collect();
        all.push(&scan);
        let budget = query_budget(true, &all);
        budget.try_charge(&scan, 0, 100, "q").unwrap();
        budget.try_charge(&scan, 100, 800, "q").unwrap();
        // The total still applies.
        assert!(budget.try_charge(&scan, 900, 101, "q").is_err());
    }

    #[test]
    fn the_fair_share_follows_registrations() {
        let a = MemoryConsumer::new("a").with_can_spill(true);
        let b = MemoryConsumer::new("b").with_can_spill(true);
        let budget = query_budget(true, &[&a, &b]);
        budget.try_charge(&a, 0, 500, "q").unwrap();
        assert!(budget.try_charge(&a, 500, 1, "q").is_err());
        budget.unregister(&b);
        // Alone, a may grow to the spillable line.
        budget.try_charge(&a, 500, 300, "q").unwrap();
    }
}
