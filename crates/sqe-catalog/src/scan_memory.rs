//! Decode admission control for the Iceberg read path (issue #367).
//!
//! The scan/decode path used to be invisible to the DataFusion memory pool:
//! no `MemoryReservation` anywhere in this crate, while each of the scan's
//! output partitions built its own vendored reader with a `num_cpus`-wide
//! decode semaphore. With `parallel_scan` default-on that multiplied to
//! `target_partitions x num_cpus` concurrent row-group decompressions, and
//! an 8GB-pool coordinator was host-OOM-killed at 18GB RSS with ~4KB of
//! pool residue.
//!
//! [`ScanDecodeGate`] fixes both halves, mirroring the write path's
//! `TrackedBatchBuffer` (`sqe-coordinator/src/write_memory.rs`):
//!
//! - one semaphore per scan node, shared by every partition, bounds the
//!   scan's total in-flight decodes at `num_cpus` instead of
//!   `partitions x num_cpus`. The semaphore is deliberately per scan node,
//!   not process-global: a probe-side scan's decode workers can park on a
//!   full output channel until the join's build side finishes, and with a
//!   process-global semaphore those parked workers would starve the build
//!   side's own scan into a deadlock.
//! - each admitted decode reserves its estimated working set against the
//!   query's memory pool. A denial surfaces as
//!   `DataFusionError::ResourcesExhausted`: the query fails typed, the node
//!   and every other query keep running.
//!
//! Which pool refused decides whether a denial waits (#788):
//!
//! - The query's own quota: the bytes are held by this query, possibly by
//!   an operator waiting on this very scan, so waiting could deadlock. The
//!   denial is immediate, and the join fallback (#784) can retry the query
//!   with sort-merge joins.
//! - The node pool, while the quota still has room: other queries hold the
//!   bytes, and decodes, hash builds and merges release them as they
//!   finish. The admission polls with backoff for up to
//!   [`ScanDecodeGate::set_memory_wait`] before failing the same way. The
//!   wait is bounded, so queries waiting on each other's memory still end.
//!
//! Without a per-query quota the pool's own `reserved()` is the whole
//! node's charge, so every denial counts as the query's own and stays
//! immediate.

use std::any::Any;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use futures::future::BoxFuture;
use iceberg::arrow::DecodeGate;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Multiplier from compressed task bytes to the reserved decode estimate.
/// Parquet analytics data commonly decompresses 3-5x; the reservation is
/// corrected to the actual decoded size where the batches are visible (the
/// direct-read path), and released at subtask end on the vendored path.
pub const DECODE_MEMORY_ESTIMATE_FACTOR: u64 = 4;

const PPM: u32 = 1_000_000;

/// First and longest pause between retries of a decode admission waiting
/// for other queries' memory (#788).
const MEMORY_WAIT_FIRST_RETRY: Duration = Duration::from_millis(10);
const MEMORY_WAIT_MAX_RETRY: Duration = Duration::from_millis(200);

/// Escape hatch: set to `0`, `false`, or `off` to disable the pool
/// reservations (the concurrency bound always applies). Diagnostic only,
/// mirroring the write path's `write_buffer_tracking` flag.
const TRACKING_ENV: &str = "SQE_SCAN_DECODE_TRACKING";

/// Whether decode-memory pool tracking is enabled (read once per process).
pub fn decode_tracking_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var(TRACKING_ENV).as_deref(),
            Ok("0") | Ok("false") | Ok("off")
        )
    })
}

/// Admission gate for one scan node's decode work: a shared concurrency
/// bound plus a per-decode memory reservation. Implements the vendored
/// reader's [`DecodeGate`] hook and is used directly by the direct-read
/// fast path.
#[derive(Debug)]
pub struct ScanDecodeGate {
    permits: Arc<Semaphore>,
    /// Fetch-stage permits (sqe#scan-fetch-pipeline): bounds how many subtasks may sit in
    /// their I/O-latency-bound fetch stage concurrently, independent of
    /// the CPU-bound decode permits above. `None` = fetch staging off.
    fetch_permits: Option<Arc<Semaphore>>,
    pool: Arc<dyn MemoryPool>,
    /// Names the consumer in pool-denial errors, e.g.
    /// `iceberg-scan-decode:iceberg.tpch.lineitem`.
    label: String,
    /// `false` = permits only, no pool reservations (escape hatch).
    track_memory: bool,
    /// Share of a split's bytes the scan's projection actually reads, in
    /// parts per million (see [`Self::set_projected_share`]). Applied to the
    /// vendored reader's admissions only.
    projected_share_ppm: AtomicU32,
    /// How long a denial by the node pool waits for memory, in ms (see
    /// [`Self::set_memory_wait`]). 0 = fail fast.
    memory_wait_ms: AtomicU64,
    /// When this gate's first admission started waiting. Every later wait
    /// ends at the same deadline, so the wait bounds the whole scan
    /// partition, not each subtask.
    wait_started: std::sync::OnceLock<tokio::time::Instant>,
}

/// Floor for [`ScanDecodeGate::set_projected_share`]: page headers, the
/// footer and range coalescing still read some bytes outside the projected
/// column chunks.
pub const MIN_PROJECTED_SHARE: f64 = 1.0 / 32.0;

impl ScanDecodeGate {
    pub fn new(
        permits: Arc<Semaphore>,
        pool: Arc<dyn MemoryPool>,
        label: String,
        track_memory: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            permits,
            fetch_permits: None,
            pool,
            label,
            track_memory,
            projected_share_ppm: AtomicU32::new(PPM),
            memory_wait_ms: AtomicU64::new(0),
            wait_started: std::sync::OnceLock::new(),
        })
    }

    /// Gate with a separate fetch-stage bound (sqe#scan-fetch-pipeline). `fetch_permits`
    /// is shared across every partition of the scan node, like `permits`.
    pub fn new_with_fetch_permits(
        permits: Arc<Semaphore>,
        fetch_permits: Arc<Semaphore>,
        pool: Arc<dyn MemoryPool>,
        label: String,
        track_memory: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            permits,
            fetch_permits: Some(fetch_permits),
            pool,
            label,
            track_memory,
            projected_share_ppm: AtomicU32::new(PPM),
            memory_wait_ms: AtomicU64::new(0),
            wait_started: std::sync::OnceLock::new(),
        })
    }

    /// Scale the vendored reader's admissions to the share of each split the
    /// projection reads (sqe#801).
    ///
    /// The reader reports a subtask's whole byte range, every column, while it
    /// fetches only the projected column chunks. TPC-DS q09 SF10 runs 15
    /// store_sales scans projecting 1-2 of 23 columns, and their whole-split
    /// reservations reached 4 GiB within 80 ms. `share` is clamped to
    /// [`MIN_PROJECTED_SHARE`]..=1. The direct-read path reads whole files into
    /// memory and calls [`Self::admit`] directly, so it stays unscaled.
    pub fn set_projected_share(&self, share: f64) {
        let share = if share.is_finite() { share.clamp(MIN_PROJECTED_SHARE, 1.0) } else { 1.0 };
        self.projected_share_ppm
            .store((share * f64::from(PPM)).round() as u32, Ordering::Relaxed);
    }

    fn projected(&self, estimated_bytes: u64) -> u64 {
        let ppm = u64::from(self.projected_share_ppm.load(Ordering::Relaxed));
        (u128::from(estimated_bytes) * u128::from(ppm) / u128::from(PPM)) as u64
    }

    /// Let an admission the node pool refuses wait up to `wait` for other
    /// queries to release memory, as long as its own query's quota has room
    /// (#788; module docs). `Duration::ZERO`, the default, fails fast.
    pub fn set_memory_wait(&self, wait: Duration) {
        let ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
        self.memory_wait_ms.store(ms, Ordering::Relaxed);
    }

    /// Reserve `bytes` for one admission. A denial is retried with backoff
    /// until the memory wait runs out, but only while the bytes would fit
    /// the query's own limit: then the pool that refused was the shared
    /// one underneath. The wait is one budget for the gate, counted from
    /// its first wait, so a scan cannot stall for the wait once per
    /// subtask. The denial returned is the last `try_grow` error, so it
    /// names this consumer either way.
    async fn reserve(&self, bytes: usize) -> DFResult<MemoryReservation> {
        let reservation = MemoryConsumer::new(self.label.clone()).register(&self.pool);
        let wait = Duration::from_millis(self.memory_wait_ms.load(Ordering::Relaxed));
        let mut pause = MEMORY_WAIT_FIRST_RETRY;
        loop {
            let error = match reservation.try_grow(bytes) {
                Ok(()) => return Ok(reservation),
                Err(error) => error,
            };
            if wait.is_zero() || !self.fits_own_limit(bytes) {
                return Err(error);
            }
            let now = tokio::time::Instant::now();
            let deadline = *self.wait_started.get_or_init(|| now) + wait;
            if now >= deadline {
                return Err(error);
            }
            tokio::time::sleep(pause.min(deadline - now)).await;
            pause = (pause * 2).min(MEMORY_WAIT_MAX_RETRY);
        }
    }

    /// Whether `bytes` more would stay within the pool's own limit. For a
    /// per-query pool `reserved()` is the query's charge and the limit its
    /// quota, so a denial that still fits came from the node pool. For a
    /// plain pool it never fits after a denial.
    fn fits_own_limit(&self, bytes: usize) -> bool {
        match self.pool.memory_limit() {
            MemoryLimit::Finite(limit) => self.pool.reserved().saturating_add(bytes) <= limit,
            _ => false,
        }
    }

    /// Admit the FETCH stage of a subtask reading ~`estimated_bytes` of
    /// compressed input: wait for a fetch permit, then reserve the
    /// compressed size (1x — the fetch stage materializes compressed
    /// column chunks, not the decoded working set; see [`Self::reserve`]
    /// for when a denial waits). Falls back
    /// to a full decode admission when no fetch permits were configured,
    /// preserving single-stage semantics.
    pub async fn admit_fetch(&self, estimated_bytes: u64) -> DFResult<DecodeAdmission> {
        let Some(fetch_permits) = &self.fetch_permits else {
            return self.admit(estimated_bytes).await;
        };
        let permit = fetch_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let reservation = if self.track_memory {
            let estimate = usize::try_from(estimated_bytes).unwrap_or(usize::MAX);
            Some(self.reserve(estimate).await?)
        } else {
            None
        };
        Ok(DecodeAdmission {
            _permit: permit,
            reservation,
        })
    }

    /// Admit a decode of ~`estimated_bytes` compressed input: wait for a
    /// permit, then reserve the decoded estimate ([`Self::reserve`]). The
    /// permit is released on denial, so a rejected query never wedges the
    /// scan.
    pub async fn admit(&self, estimated_bytes: u64) -> DFResult<DecodeAdmission> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let reservation = if self.track_memory {
            let estimate =
                usize::try_from(estimated_bytes.saturating_mul(DECODE_MEMORY_ESTIMATE_FACTOR))
                    .unwrap_or(usize::MAX);
            Some(self.reserve(estimate).await?)
        } else {
            None
        };
        Ok(DecodeAdmission {
            _permit: permit,
            reservation,
        })
    }
}

impl DecodeGate for ScanDecodeGate {
    fn admit(&self, estimated_bytes: u64) -> BoxFuture<'_, iceberg::Result<Box<dyn Any + Send>>> {
        Box::pin(async move {
            let admission = ScanDecodeGate::admit(self, self.projected(estimated_bytes))
                .await
                .map_err(refused)?;
            Ok(Box::new(admission) as Box<dyn Any + Send>)
        })
    }

    fn admit_fetch(
        &self,
        estimated_bytes: u64,
    ) -> BoxFuture<'_, iceberg::Result<Box<dyn Any + Send>>> {
        Box::pin(async move {
            let admission = ScanDecodeGate::admit_fetch(self, self.projected(estimated_bytes))
                .await
                .map_err(refused)?;
            Ok(Box::new(admission) as Box<dyn Any + Send>)
        })
    }
}

/// A decode refusal crossing the vendored reader as an `iceberg::Error`.
/// The DataFusion error rides along as the source, not only as text: its
/// display keeps the "Resources exhausted" wording the coordinator
/// classifies by, and the typed source lets the join fallback (#784)
/// recognise the refusal and retry, which it could not while only the text
/// survived (#788).
fn refused(error: DataFusionError) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        "scan decode admission refused",
    )
    .with_source(error)
}

/// Guard for one admitted decode. Holds the concurrency permit and the
/// memory reservation; dropping it releases both.
#[derive(Debug)]
pub struct DecodeAdmission {
    _permit: OwnedSemaphorePermit,
    reservation: Option<MemoryReservation>,
}

impl DecodeAdmission {
    /// Correct the reservation to the actual decoded size once it is known
    /// (the estimate is compressed-size based). Growing past the pool budget
    /// fails typed, same as the initial admission.
    pub fn try_resize(&mut self, bytes: usize) -> DFResult<()> {
        match self.reservation.as_mut() {
            Some(reservation) => reservation.try_resize(bytes),
            None => Ok(()),
        }
    }

    /// Bytes currently reserved against the pool (0 when tracking is off).
    pub fn reserved_bytes(&self) -> usize {
        self.reservation.as_ref().map_or(0, |r| r.size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use futures::FutureExt;

    fn gate(
        pool_bytes: usize,
        permits: usize,
        track: bool,
    ) -> (Arc<ScanDecodeGate>, Arc<dyn MemoryPool>) {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
        let gate = ScanDecodeGate::new(
            Arc::new(Semaphore::new(permits)),
            pool.clone(),
            "iceberg-scan-decode:test".to_string(),
            track,
        );
        (gate, pool)
    }

    /// Gate with independent fetch-stage permits (sqe#scan-fetch-pipeline).
    fn staged_gate(
        pool_bytes: usize,
        permits: usize,
        fetch_permits: usize,
        track: bool,
    ) -> (Arc<ScanDecodeGate>, Arc<dyn MemoryPool>) {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
        let gate = ScanDecodeGate::new_with_fetch_permits(
            Arc::new(Semaphore::new(permits)),
            Arc::new(Semaphore::new(fetch_permits)),
            pool.clone(),
            "iceberg-scan-decode:test".to_string(),
            track,
        );
        (gate, pool)
    }

    #[tokio::test]
    async fn admit_fetch_reserves_compressed_size_and_drop_releases() {
        let (gate, pool) = staged_gate(1 << 30, 1, 4, true);
        let admission = gate.admit_fetch(1024).await.expect("fetch admits");
        // 1x compressed bytes, NOT the 4x decode estimate.
        assert_eq!(admission.reserved_bytes(), 1024);
        assert_eq!(pool.reserved(), 1024);
        drop(admission);
        assert_eq!(pool.reserved(), 0, "fetch reservation released on drop");
    }

    #[tokio::test]
    async fn fetch_permits_bound_independently_of_decode_permits() {
        // 1 decode permit, 2 fetch permits: two fetch admissions coexist
        // with a held decode admission, the third fetch parks.
        let (gate, _pool) = staged_gate(1 << 30, 1, 2, true);
        let _decode = gate.admit(8).await.expect("decode admit");
        let _f1 = gate.admit_fetch(8).await.expect("fetch 1");
        let f2 = gate.admit_fetch(8).await.expect("fetch 2");
        let mut f3 = Box::pin(gate.admit_fetch(8));
        assert!(
            f3.as_mut().now_or_never().is_none(),
            "third fetch admission must park on the fetch permits"
        );
        drop(f2);
        f3.await.expect("admitted after a fetch permit freed");
    }

    #[tokio::test]
    async fn admit_fetch_denies_over_budget_typed_and_frees_permit() {
        let (gate, pool) = staged_gate(64, 1, 1, true);
        let err = gate
            .admit_fetch(1 << 20)
            .await
            .expect_err("tiny pool denies");
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "typed pool denial, got: {err}"
        );
        assert_eq!(pool.reserved(), 0, "denied fetch reserves nothing");
        // The single fetch permit was returned on denial.
        let ok = gate.admit_fetch(1).await.expect("permit was released");
        assert!(ok.reserved_bytes() > 0);
    }

    #[tokio::test]
    async fn admit_fetch_without_fetch_permits_falls_back_to_decode_admission() {
        // Un-staged gate: admit_fetch == admit (single-stage semantics),
        // including the 4x decode estimate and the decode permit.
        let (gate, pool) = gate(1 << 30, 1, true);
        let admission = gate.admit_fetch(1024).await.expect("fallback admits");
        assert_eq!(
            admission.reserved_bytes(),
            1024 * DECODE_MEMORY_ESTIMATE_FACTOR as usize
        );
        assert_eq!(
            pool.reserved(),
            1024 * DECODE_MEMORY_ESTIMATE_FACTOR as usize
        );
        // It consumed the decode permit: a decode admit must park.
        assert!(Box::pin(gate.admit(1)).now_or_never().is_none());
    }

    #[tokio::test]
    async fn admit_reserves_estimate_and_drop_releases() {
        let (gate, pool) = gate(1 << 30, 4, true);
        let admission = gate.admit(1024).await.expect("large pool admits");
        let expected = 1024 * DECODE_MEMORY_ESTIMATE_FACTOR as usize;
        assert_eq!(admission.reserved_bytes(), expected);
        assert_eq!(pool.reserved(), expected);
        drop(admission);
        assert_eq!(pool.reserved(), 0, "reservation released on drop");
    }

    #[tokio::test]
    async fn admit_denies_over_budget_with_resources_exhausted_and_frees_permit() {
        let (gate, pool) = gate(64, 1, true);
        let err = gate.admit(1 << 20).await.expect_err("tiny pool denies");
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "typed pool denial, got: {err}"
        );
        assert!(
            err.to_string().contains("iceberg-scan-decode:test"),
            "error names the consumer: {err}"
        );
        assert_eq!(pool.reserved(), 0, "denied admit reserves nothing");
        // The single permit was returned on denial: a small admit succeeds
        // immediately (would hang forever if the permit leaked).
        let ok = gate.admit(1).await.expect("permit was released");
        assert!(ok.reserved_bytes() > 0);
    }

    #[tokio::test]
    async fn permits_bound_concurrent_admissions() {
        let (gate, _pool) = gate(1 << 30, 1, true);
        let first = gate.admit(8).await.expect("first admit");
        // Second admission must park on the semaphore, not resolve.
        let mut second = Box::pin(gate.admit(8));
        assert!(
            second.as_mut().now_or_never().is_none(),
            "second admit waits while the only permit is held"
        );
        drop(first);
        second.await.expect("admitted after the permit freed");
    }

    #[tokio::test]
    async fn untracked_gate_reserves_nothing_but_still_gates() {
        let (gate, pool) = gate(64, 1, false);
        // Far over the pool budget, but tracking is off: admit succeeds.
        let admission = gate.admit(1 << 20).await.expect("untracked never denies");
        assert_eq!(admission.reserved_bytes(), 0);
        assert_eq!(pool.reserved(), 0);
        // The permit is still honoured.
        assert!(Box::pin(gate.admit(1)).now_or_never().is_none());
    }

    #[tokio::test]
    async fn try_resize_corrects_reservation_to_actual() {
        let (gate, pool) = gate(1 << 30, 4, true);
        let mut admission = gate.admit(1024).await.expect("admit");
        admission.try_resize(100).expect("shrink to actual");
        assert_eq!(admission.reserved_bytes(), 100);
        assert_eq!(pool.reserved(), 100);
    }

    #[tokio::test]
    async fn decode_gate_trait_denial_carries_exhausted_wording() {
        let (gate, _pool) = gate(64, 1, true);
        let err = DecodeGate::admit(gate.as_ref(), 1 << 20)
            .await
            .expect_err("denied through the vendored trait");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("resources exhausted") || msg.contains("failed to allocate"),
            "wording survives the iceberg::Error wrap for classification: {msg}"
        );
        let source = std::error::Error::source(&err)
            .and_then(|s| s.downcast_ref::<DataFusionError>())
            .expect("the DataFusion refusal is the iceberg::Error's source");
        assert!(
            matches!(source, DataFusionError::ResourcesExhausted(_)),
            "typed refusal behind the wrap: {source}"
        );
    }

    #[tokio::test]
    async fn projected_share_scales_only_the_vendored_admissions() {
        let (gate, pool) = gate(1 << 30, 4, true);
        gate.set_projected_share(0.25);
        let vendored = DecodeGate::admit(gate.as_ref(), 4096).await.expect("admits");
        let scaled = 1024 * DECODE_MEMORY_ESTIMATE_FACTOR as usize;
        assert_eq!(pool.reserved(), scaled);
        // The direct-read path holds whole files and stays unscaled.
        let direct = gate.admit(4096).await.expect("admits");
        assert_eq!(pool.reserved(), scaled + 4096 * DECODE_MEMORY_ESTIMATE_FACTOR as usize);
        drop((vendored, direct));
        assert_eq!(pool.reserved(), 0);
    }

    /// A per-query pool over a shared node pool, like the coordinator's
    /// `QueryMemoryPool`: `reserved()` is the query's own charge and the
    /// limit its quota.
    #[derive(Debug)]
    struct QuotaPool {
        own: std::sync::atomic::AtomicUsize,
        limit: usize,
        shared: Arc<dyn MemoryPool>,
    }

    impl std::fmt::Display for QuotaPool {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("QuotaPool")
        }
    }

    impl MemoryPool for QuotaPool {
        fn name(&self) -> &str {
            "QuotaPool"
        }

        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.own.fetch_add(additional, Ordering::Relaxed);
            self.shared.grow(reservation, additional);
        }

        fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
            self.shared.shrink(reservation, shrink);
            self.own.fetch_sub(shrink, Ordering::Relaxed);
        }

        fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DFResult<()> {
            if self.reserved() + additional > self.limit {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "quota refused {}",
                    reservation.consumer().name()
                )));
            }
            self.shared.try_grow(reservation, additional)?;
            self.own.fetch_add(additional, Ordering::Relaxed);
            Ok(())
        }

        fn reserved(&self) -> usize {
            self.own.load(Ordering::Relaxed)
        }

        fn memory_limit(&self) -> MemoryLimit {
            MemoryLimit::Finite(self.limit)
        }
    }

    /// A gate on a `quota`-byte query pool over a 1000-byte node pool of
    /// which another query holds `held` bytes, returned as the reservation.
    fn quota_gate(
        quota: usize,
        held: usize,
        wait: Duration,
        staged: bool,
    ) -> (Arc<ScanDecodeGate>, Arc<dyn MemoryPool>, MemoryReservation) {
        let shared: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1000));
        let other = MemoryConsumer::new("other-query").register(&shared);
        other.try_grow(held).expect("other query fits");
        let pool: Arc<dyn MemoryPool> = Arc::new(QuotaPool {
            own: Default::default(),
            limit: quota,
            shared,
        });
        let label = "iceberg-scan-decode:test".to_string();
        let gate = if staged {
            ScanDecodeGate::new_with_fetch_permits(
                Arc::new(Semaphore::new(1)),
                Arc::new(Semaphore::new(1)),
                pool.clone(),
                label,
                true,
            )
        } else {
            ScanDecodeGate::new(Arc::new(Semaphore::new(1)), pool.clone(), label, true)
        };
        gate.set_memory_wait(wait);
        (gate, pool, other)
    }

    #[tokio::test]
    async fn a_node_pool_denial_waits_for_another_query_to_release() {
        for staged in [false, true] {
            // 200 bytes fit the 800-byte quota; the node pool has 100 left.
            let (gate, pool, other) = quota_gate(800, 900, Duration::from_secs(10), staged);
            let release = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                drop(other);
            });
            let admission = if staged {
                gate.admit_fetch(200).await
            } else {
                gate.admit(50).await
            }
            .expect("admitted once the other query released");
            assert_eq!(admission.reserved_bytes(), 200);
            assert_eq!(pool.reserved(), 200);
            release.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_node_pool_denial_fails_typed_when_the_wait_runs_out() {
        let wait = Duration::from_millis(50);
        let (gate, pool, _other) = quota_gate(800, 900, wait, false);
        let started = std::time::Instant::now();
        let err = gate.admit(50).await.expect_err("the node pool stays full");
        assert!(started.elapsed() >= wait, "waited before failing");
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "typed pool denial, got: {err}"
        );
        assert!(
            err.to_string().contains("iceberg-scan-decode:test"),
            "{err}"
        );
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn the_wait_is_one_budget_for_the_gate_not_one_per_admission() {
        let wait = Duration::from_millis(100);
        let (gate, _pool, _other) = quota_gate(800, 900, wait, false);
        let started = std::time::Instant::now();
        gate.admit(50)
            .await
            .expect_err("first admission waits it out");
        // A later subtask's denial finds the budget spent and fails at once.
        let again = std::time::Instant::now();
        gate.admit(50).await.expect_err("budget spent");
        assert!(again.elapsed() < wait, "no second wait");
        assert!(started.elapsed() >= wait);
    }

    #[tokio::test]
    async fn a_denial_by_the_query_quota_fails_without_waiting() {
        // 200 bytes exceed the 100-byte quota: the query's own bytes, so
        // waiting could deadlock and the join fallback wants it fast.
        let (gate, _pool, _other) = quota_gate(100, 0, Duration::from_secs(10), false);
        let started = std::time::Instant::now();
        let err = gate.admit(50).await.expect_err("over the quota");
        assert!(started.elapsed() < Duration::from_secs(1), "no wait");
        assert!(err.to_string().contains("quota refused"), "{err}");
    }

    #[tokio::test]
    async fn a_pool_without_a_quota_fails_without_waiting() {
        let (gate, _pool) = gate(64, 1, true);
        gate.set_memory_wait(Duration::from_secs(10));
        let started = std::time::Instant::now();
        gate.admit(1 << 20).await.expect_err("tiny pool denies");
        assert!(started.elapsed() < Duration::from_secs(1), "no wait");
    }

    #[tokio::test]
    async fn projected_share_is_clamped() {
        let (gate, pool) = gate(1 << 30, 4, true);
        gate.set_projected_share(0.0);
        let low = DecodeGate::admit(gate.as_ref(), 3200).await.expect("admits");
        assert_eq!(pool.reserved(), 100 * DECODE_MEMORY_ESTIMATE_FACTOR as usize);
        drop(low);
        for share in [2.0, f64::NAN] {
            gate.set_projected_share(share);
            let full = DecodeGate::admit(gate.as_ref(), 1000).await.expect("admits");
            assert_eq!(pool.reserved(), 1000 * DECODE_MEMORY_ESTIMATE_FACTOR as usize);
            drop(full);
        }
    }
}
