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
//!   query's memory pool (fail-fast `try_grow`, never a blocking wait, so
//!   pressure cannot deadlock against operators waiting on this scan).
//!   A denial surfaces as `DataFusionError::ResourcesExhausted`: the query
//!   fails typed, the node and every other query keep running.

use std::any::Any;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use futures::future::BoxFuture;
use iceberg::arrow::DecodeGate;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Multiplier from compressed task bytes to the reserved decode estimate.
/// Parquet analytics data commonly decompresses 3-5x; the reservation is
/// corrected to the actual decoded size where the batches are visible (the
/// direct-read path), and released at subtask end on the vendored path.
pub const DECODE_MEMORY_ESTIMATE_FACTOR: u64 = 4;

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
    pool: Arc<dyn MemoryPool>,
    /// Names the consumer in pool-denial errors, e.g.
    /// `iceberg-scan-decode:iceberg.tpch.lineitem`.
    label: String,
    /// `false` = permits only, no pool reservations (escape hatch).
    track_memory: bool,
}

impl ScanDecodeGate {
    pub fn new(
        permits: Arc<Semaphore>,
        pool: Arc<dyn MemoryPool>,
        label: String,
        track_memory: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            permits,
            pool,
            label,
            track_memory,
        })
    }

    /// Admit a decode of ~`estimated_bytes` compressed input: wait for a
    /// permit, then reserve the decoded estimate fail-fast. The permit is
    /// released on denial, so a rejected query never wedges the scan.
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
            let reservation = MemoryConsumer::new(self.label.clone()).register(&self.pool);
            reservation.try_grow(estimate)?;
            Some(reservation)
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
            let admission = ScanDecodeGate::admit(self, estimated_bytes)
                .await
                // Carry the DataFusion message verbatim: it contains the
                // "Resources exhausted"/"Failed to allocate" wording the
                // coordinator classifies as RESOURCE_EXHAUSTED.
                .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;
            Ok(Box::new(admission) as Box<dyn Any + Send>)
        })
    }
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
    }
}
