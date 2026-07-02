//! Pool-tracked write buffers for the Iceberg write sink.
//!
//! The DataFusion memory pool tracks operator memory (joins, aggregates,
//! sorts) but not the write sink's own buffers. Every byte a MERGE
//! copy-on-write, a Flight ingest collect, an UPDATE/DELETE file decode, or a
//! partitioned fanout writer buffers is invisible to the pool, so it can only
//! OOM-kill the process instead of failing cleanly or spilling.
//!
//! These wrappers register write-side allocations against the same shared pool
//! the query operators use, mirroring the scan-path precedent in
//! `sqe-worker/src/executor.rs` (`MemoryConsumer::new(...).register(&pool)`
//! then `reservation.try_grow(...)`). A denied grow becomes a typed
//! `ResourceExhausted` error naming the buffer: one query fails, the node and
//! every other query keep running. Dropping a buffer releases its reservation,
//! so early returns, `?` short-circuits, and panics all reclaim to zero.
//!
//! See `docs/internal/specs/2026-07-02-write-path-memory-safety-design.md`.

use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use sqe_core::{SqeError, SqeErrorCode};

/// Build the typed resource-exhausted error a denied write reservation
/// surfaces. The message names the buffer so operators can act on the specific
/// path (for example: switch a large MERGE to merge-on-read).
fn exhausted(
    label: &str,
    requested: usize,
    source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
) -> SqeError {
    SqeError::sourced(
        SqeErrorCode::ResourceExhausted,
        format!(
            "write buffer '{label}' exceeded the query memory pool while requesting \
             {requested} bytes; reduce the write size or, for a MERGE, use a \
             merge-on-read table"
        ),
        source,
    )
}

/// A `Vec<RecordBatch>` whose memory is reserved against the shared pool.
///
/// `push` grows the reservation by the batch's array memory size before
/// appending; a denial returns [`SqeError`] with [`SqeErrorCode::ResourceExhausted`]
/// and appends nothing. The whole reservation releases on drop.
pub struct TrackedBatchBuffer {
    label: String,
    batches: Vec<RecordBatch>,
    reservation: MemoryReservation,
}

impl TrackedBatchBuffer {
    /// Register a new tracked buffer against `pool`. `label` names the buffer
    /// in exhaustion errors and metrics (for example `merge-target-buffer`).
    pub fn new(pool: &Arc<dyn MemoryPool>, label: impl Into<String>) -> Self {
        let label = label.into();
        let reservation = MemoryConsumer::new(label.clone()).register(pool);
        Self {
            label,
            batches: Vec::new(),
            reservation,
        }
    }

    /// Reserve room for `batch` and append it. On denial the batch is dropped
    /// and a typed resource-exhausted error is returned; the buffer is
    /// unchanged.
    pub fn push(&mut self, batch: RecordBatch) -> Result<(), SqeError> {
        let requested = batch.get_array_memory_size();
        self.reservation
            .try_grow(requested)
            .map_err(|e| exhausted(&self.label, requested, e))?;
        self.batches.push(batch);
        Ok(())
    }

    /// The buffered batches by reference. Callers that write by reference keep
    /// the buffer (and its reservation) alive across the write.
    pub fn as_slice(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Number of buffered batches.
    pub fn len(&self) -> usize {
        self.batches.len()
    }

    /// Whether the buffer holds no batches.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Bytes currently reserved against the pool.
    pub fn reserved_bytes(&self) -> usize {
        self.reservation.size()
    }

    /// The buffer's label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Take ownership of the batches. The reservation releases when the buffer
    /// is dropped, so callers that need the batches to outlive tracking should
    /// prefer [`as_slice`](Self::as_slice) and keep the buffer alive across the
    /// write instead.
    pub fn into_inner(self) -> Vec<RecordBatch> {
        self.batches
    }
}

/// A bare resizable reservation for write-side allocations that are not a
/// `Vec<RecordBatch>`: the per-file compressed `Bytes` read on the
/// copy-on-write path, and the estimated total buffered bytes across open
/// partition-fanout writers. Releases on drop.
pub struct WriteReservation {
    label: String,
    reservation: MemoryReservation,
}

impl WriteReservation {
    /// Register a new bare reservation against `pool`.
    pub fn new(pool: &Arc<dyn MemoryPool>, label: impl Into<String>) -> Self {
        let label = label.into();
        let reservation = MemoryConsumer::new(label.clone()).register(pool);
        Self { label, reservation }
    }

    /// Grow the reservation by `bytes`. Returns a typed resource-exhausted
    /// error on denial without changing the reservation.
    pub fn try_grow(&mut self, bytes: usize) -> Result<(), SqeError> {
        self.reservation
            .try_grow(bytes)
            .map_err(|e| exhausted(&self.label, bytes, e))
    }

    /// Set the reservation to exactly `bytes`. Returns a typed resource-exhausted
    /// error on denial without changing the reservation.
    pub fn try_resize(&mut self, bytes: usize) -> Result<(), SqeError> {
        self.reservation
            .try_resize(bytes)
            .map_err(|e| exhausted(&self.label, bytes, e))
    }

    /// Shrink the reservation by `bytes` (saturating at zero). Always succeeds.
    pub fn shrink(&mut self, bytes: usize) {
        let bytes = bytes.min(self.reservation.size());
        self.reservation.shrink(bytes);
    }

    /// Bytes currently reserved.
    pub fn size(&self) -> usize {
        self.reservation.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::GreedyMemoryPool;

    fn pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(limit))
    }

    /// An Int32 batch of `n` rows. Array memory size is well above `n * 4`
    /// bytes, so a tiny pool denies it and a large pool admits it.
    fn batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let array = Int32Array::from_iter_values(0..n as i32);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("valid batch")
    }

    #[test]
    fn tracked_buffer_grows_and_reports_reserved_bytes() {
        let pool = pool(1 << 30);
        let mut buf = TrackedBatchBuffer::new(&pool, "test-buffer");
        assert!(buf.is_empty());
        assert_eq!(buf.reserved_bytes(), 0);

        let b = batch(1000);
        let sz = b.get_array_memory_size();
        buf.push(b).expect("large pool admits");
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.reserved_bytes(), sz);
        assert_eq!(buf.as_slice().len(), 1);
    }

    #[test]
    fn tracked_buffer_denies_over_budget_with_resource_exhausted() {
        let pool = pool(64); // far smaller than one batch
        let mut buf = TrackedBatchBuffer::new(&pool, "merge-target-buffer");
        let err = buf.push(batch(1000)).expect_err("tiny pool must deny");
        assert_eq!(err.error_code(), SqeErrorCode::ResourceExhausted);
        assert!(
            err.to_string().contains("merge-target-buffer"),
            "error names the buffer: {err}"
        );
        // Nothing was appended and nothing was reserved on denial.
        assert!(buf.is_empty());
        assert_eq!(buf.reserved_bytes(), 0);
    }

    #[test]
    fn dropping_tracked_buffer_releases_reservation_to_pool() {
        let pool = pool(1 << 20);
        {
            let mut buf = TrackedBatchBuffer::new(&pool, "drop-test");
            buf.push(batch(100)).expect("admit");
            assert!(pool.reserved() > 0);
        }
        assert_eq!(pool.reserved(), 0, "reservation released on drop");
    }

    #[test]
    fn write_reservation_grows_resizes_and_shrinks() {
        let pool = pool(1 << 20);
        let mut r = WriteReservation::new(&pool, "fanout-buffer");
        r.try_grow(1000).expect("grow");
        assert_eq!(r.size(), 1000);
        r.try_grow(500).expect("grow more");
        assert_eq!(r.size(), 1500);
        r.shrink(500);
        assert_eq!(r.size(), 1000);
        r.try_resize(200).expect("resize down");
        assert_eq!(r.size(), 200);
    }

    #[test]
    fn write_reservation_denies_over_budget() {
        let pool = pool(1024);
        let mut r = WriteReservation::new(&pool, "cow-file-bytes");
        let err = r.try_grow(4096).expect_err("over budget");
        assert_eq!(err.error_code(), SqeErrorCode::ResourceExhausted);
        assert_eq!(r.size(), 0, "denied grow leaves reservation unchanged");
    }

    #[test]
    fn write_reservation_releases_on_drop() {
        let pool = pool(1 << 20);
        {
            let mut r = WriteReservation::new(&pool, "drop-res");
            r.try_grow(4096).expect("grow");
            assert_eq!(pool.reserved(), 4096);
        }
        assert_eq!(pool.reserved(), 0);
    }
}
