//! Spillable partition buffer for distributed shuffle (Phase 4).
//!
//! Appends accounted in-memory batches until a soft watermark, then spills
//! immutable Arrow segments through the shared [`SpillManager`]. Readers
//! drain residual memory batches first, then stream committed segments.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use sqe_metrics::WorkerMetricsRegistry;
use sqe_spill::{
    Accounted, ByteBudget, SpillManager, SpillScope, SpillScopeGuard, SpillSegment,
};
use tracing::{debug, warn};

/// Soft watermark: spill when in-memory bytes reach this fraction of the
/// partition budget (default 75%).
const SOFT_WATERMARK_NUM: usize = 3;
const SOFT_WATERMARK_DEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionBufferState {
    Open,
    Finished,
    Failed,
    Cancelled,
}

/// Completion manifest for one partition exchange.
#[derive(Debug, Clone)]
pub struct PartitionManifest {
    pub rows: u64,
    pub batches: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub segments: usize,
}

/// Per-partition buffer that spills at a soft watermark.
pub struct SpillablePartitionBuffer {
    scope: SpillScope,
    schema: SchemaRef,
    budget: ByteBudget,
    soft_limit: usize,
    manager: Arc<SpillManager>,
    /// Live in-memory batches (permits held via Accounted).
    memory: Vec<Accounted<RecordBatch>>,
    memory_bytes: usize,
    segments: Vec<SpillSegment>,
    state: PartitionBufferState,
    rows: u64,
    batches: u64,
    logical_bytes: u64,
    metrics: Option<Arc<WorkerMetricsRegistry>>,
    _guard: Option<SpillScopeGuard>,
    fail_msg: Option<String>,
}

impl SpillablePartitionBuffer {
    pub fn new(
        manager: Arc<SpillManager>,
        scope: SpillScope,
        schema: SchemaRef,
        budget: ByteBudget,
        metrics: Option<Arc<WorkerMetricsRegistry>>,
    ) -> Self {
        let soft_limit =
            (budget.capacity_bytes().saturating_mul(SOFT_WATERMARK_NUM) / SOFT_WATERMARK_DEN)
                .max(1);
        let guard = SpillScopeGuard::new(manager.clone(), scope.clone());
        Self {
            scope,
            schema,
            budget,
            soft_limit: soft_limit.max(1),
            manager,
            memory: Vec::new(),
            memory_bytes: 0,
            segments: Vec::new(),
            state: PartitionBufferState::Open,
            rows: 0,
            batches: 0,
            logical_bytes: 0,
            metrics,
            _guard: Some(guard),
            fail_msg: None,
        }
    }

    pub fn state(&self) -> PartitionBufferState {
        self.state
    }

    pub fn resident_bytes(&self) -> usize {
        self.memory_bytes
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Append one batch. Spills automatically when the soft watermark is hit.
    pub async fn append(&mut self, batch: RecordBatch) -> anyhow::Result<()> {
        if self.state != PartitionBufferState::Open {
            anyhow::bail!(
                "cannot append to partition buffer in state {:?}",
                self.state
            );
        }
        let logical = batch.get_array_memory_size();
        // Spill resident batches BEFORE reserving budget for this one when the
        // addition would cross the soft watermark. The budget permits held by
        // buffered batches are released only inside `spill_memory` (permit drop
        // after `write_batch`). Acquiring first and spilling second deadlocks:
        // a batch between half and the watermark fraction of capacity waits in
        // `acquire` for budget that only a spill can release, but the spill is
        // downstream of the acquire and never runs. Flushing first frees this
        // buffer's permits so the acquire can always succeed for any batch that
        // individually fits the budget.
        if !self.memory.is_empty()
            && self.memory_bytes.saturating_add(logical) >= self.soft_limit
        {
            self.spill_memory().await?;
        }
        let permit = self
            .budget
            .acquire(logical)
            .await
            .map_err(|e| anyhow::anyhow!("shuffle partition budget: {e}"))?;
        let charged = permit.charged_bytes();
        self.memory_bytes = self.memory_bytes.saturating_add(charged);
        self.rows += batch.num_rows() as u64;
        self.batches += 1;
        self.logical_bytes += logical as u64;
        self.memory
            .push(Accounted::new(batch, permit, logical));
        self.publish_metrics();

        // A single batch at or above the watermark spills immediately so the
        // next batch never stacks on top of an already-over-watermark buffer.
        if self.memory_bytes >= self.soft_limit {
            self.spill_memory().await?;
        }
        Ok(())
    }

    /// Force-spill all in-memory batches into one segment.
    pub async fn spill_memory(&mut self) -> anyhow::Result<()> {
        if self.memory.is_empty() {
            return Ok(());
        }
        let mut writer = self
            .manager
            .create_writer(&self.scope, self.schema.clone())
            .await
            .map_err(|e| anyhow::anyhow!("spill writer: {e}"))?;

        let batches: Vec<Accounted<RecordBatch>> = std::mem::take(&mut self.memory);
        self.memory_bytes = 0;
        for accounted in batches {
            let (batch, _permit) = accounted.into_parts();
            // Permit drops after write_batch returns → budget freed.
            writer
                .write_batch(&batch)
                .await
                .map_err(|e| anyhow::anyhow!("spill write: {e}"))?;
        }
        let seg = writer
            .finish()
            .await
            .map_err(|e| anyhow::anyhow!("spill finish: {e}"))?;
        if let Some(ref m) = self.metrics {
            m.spill_bytes_written.inc_by(seg.physical_bytes as f64);
            m.spill_files.set(self.segments.len() as f64 + 1.0);
        }
        debug!(
            scope = %self.scope,
            path = %seg.path.display(),
            rows = seg.row_count,
            physical = seg.physical_bytes,
            "Spilled shuffle partition segment"
        );
        self.segments.push(seg);
        self.publish_metrics();
        Ok(())
    }

    /// Mark the partition finished and spill any residual memory.
    pub async fn finish(&mut self) -> anyhow::Result<PartitionManifest> {
        if self.state == PartitionBufferState::Open {
            self.spill_memory().await?;
            self.state = PartitionBufferState::Finished;
        }
        // Successful completion: disarm guard so segments stay until reader
        // drains, then caller deletes scope.
        if let Some(g) = self._guard.take() {
            g.disarm();
        }
        let physical: u64 = self.segments.iter().map(|s| s.physical_bytes).sum();
        Ok(PartitionManifest {
            rows: self.rows,
            batches: self.batches,
            logical_bytes: self.logical_bytes,
            physical_bytes: physical,
            segments: self.segments.len(),
        })
    }

    pub fn fail(&mut self, msg: impl Into<String>) {
        self.state = PartitionBufferState::Failed;
        self.fail_msg = Some(msg.into());
        // Guard remains armed → cleanup on drop.
    }

    pub fn cancel(&mut self) {
        self.state = PartitionBufferState::Cancelled;
        self.memory.clear();
        self.memory_bytes = 0;
    }

    /// Drain remaining in-memory batches and spill segments into a Vec of
    /// batches (for tests / small partitions). Production readers should
    /// stream segments.
    pub async fn collect_all(&mut self) -> anyhow::Result<Vec<RecordBatch>> {
        if self.state == PartitionBufferState::Failed {
            anyhow::bail!(
                "partition failed: {}",
                self.fail_msg.as_deref().unwrap_or("unknown")
            );
        }
        let mut out = Vec::new();
        for accounted in std::mem::take(&mut self.memory) {
            let (batch, _) = accounted.into_parts();
            out.push(batch);
        }
        self.memory_bytes = 0;
        for seg in &self.segments {
            let reader = self
                .manager
                .open_reader(seg)
                .await
                .map_err(|e| anyhow::anyhow!("spill read: {e}"))?;
            let mut stream = reader.into_stream();
            while let Some(item) = stream.next().await {
                let batch = item.map_err(|e| anyhow::anyhow!("spill stream: {e}"))?;
                if let Some(ref m) = self.metrics {
                    m.spill_bytes_read
                        .inc_by(batch.get_array_memory_size() as f64);
                }
                out.push(batch);
            }
        }
        self.publish_metrics();
        Ok(out)
    }

    /// Delete spilled segments after a successful read (or abandon).
    pub async fn cleanup(&mut self) -> anyhow::Result<()> {
        self.manager
            .delete_scope(&self.scope)
            .await
            .map_err(|e| anyhow::anyhow!("spill cleanup: {e}"))?;
        self.segments.clear();
        if let Some(ref m) = self.metrics {
            m.spill_files.set(0.0);
        }
        Ok(())
    }

    fn publish_metrics(&self) {
        if let Some(ref m) = self.metrics {
            m.shuffle_resident_bytes.set(self.memory_bytes as f64);
            m.spill_files.set(self.segments.len() as f64);
        }
    }
}

impl Drop for SpillablePartitionBuffer {
    fn drop(&mut self) {
        if !self.memory.is_empty() {
            warn!(
                scope = %self.scope,
                batches = self.memory.len(),
                "SpillablePartitionBuffer dropped with in-memory batches"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::FairSpillPool;
    use sqe_spill::LocalSegmentStore;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(n: i64, rows: usize) -> RecordBatch {
        let vals: Vec<i64> = (0..rows as i64).map(|i| n + i).collect();
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    async fn setup(
        soft_budget: usize,
    ) -> (Arc<SpillManager>, ByteBudget, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            LocalSegmentStore::open(tmp.path(), 1 << 30, 0, 4, 4).unwrap(),
        );
        let manager = Arc::new(SpillManager::new(
            store,
            std::time::Duration::from_secs(0),
        ));
        let pool = Arc::new(FairSpillPool::new(soft_budget.max(1024 * 1024)));
        let budget = ByteBudget::new("shuffle-part", soft_budget, Some(pool));
        (manager, budget, tmp)
    }

    #[tokio::test]
    async fn spills_at_soft_watermark_and_roundtrips() {
        // Tiny budget forces spill after a few batches.
        let (manager, budget, _tmp) = setup(64 * 1024).await;
        let scope = SpillScope::new("q-spill", "stage0", "shuffle", 0, 0);
        let mut buf = SpillablePartitionBuffer::new(
            manager,
            scope,
            schema(),
            budget,
            None,
        );

        // Append enough rows that array memory exceeds soft limit.
        for i in 0..50 {
            buf.append(batch(i * 1000, 256)).await.unwrap();
        }
        assert!(
            !buf.segments.is_empty() || buf.memory_bytes < 64 * 1024,
            "expected spill or drained memory; segments={} mem={}",
            buf.segments.len(),
            buf.memory_bytes
        );

        let manifest = buf.finish().await.unwrap();
        assert_eq!(manifest.rows, 50 * 256);
        assert!(manifest.batches >= 50);

        let collected = buf.collect_all().await.unwrap();
        let total_rows: usize = collected.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows as u64, manifest.rows);

        buf.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn append_does_not_deadlock_between_half_and_watermark() {
        // Regression for the acquire-before-spill deadlock. A batch sized
        // between half and the soft-watermark fraction of the per-partition
        // budget previously hung `append`: it acquired budget before the
        // watermark spill, so the second such batch waited forever for budget
        // that only a spill (downstream of the acquire) could release.
        let probe = batch(0, 200_000);
        let bytes = probe.get_array_memory_size();
        // Budget in ((4/3)·bytes, 2·bytes): one batch fits and stays under the
        // 3/4 watermark, but two batches cannot be co-resident.
        let capacity = bytes * 8 / 5;
        assert!(bytes < capacity, "single batch must fit the budget");
        assert!(
            bytes * SOFT_WATERMARK_DEN < capacity * SOFT_WATERMARK_NUM,
            "one batch must stay under the soft watermark"
        );
        assert!(bytes * 2 > capacity, "two batches must not be co-resident");

        let (manager, budget, _tmp) = setup(capacity).await;
        let scope = SpillScope::new("q-dl", "s", "sh", 0, 0);
        let mut buf =
            SpillablePartitionBuffer::new(manager, scope, schema(), budget, None);

        let run = async {
            for i in 0..4i64 {
                buf.append(batch(i * 1_000_000, 200_000)).await.unwrap();
            }
            buf.finish().await.unwrap()
        };
        let manifest =
            tokio::time::timeout(std::time::Duration::from_secs(10), run)
                .await
                .expect("append must not deadlock between half and watermark");
        assert_eq!(manifest.rows, 4 * 200_000);
        buf.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn ten_x_budget_completes_via_spill() {
        // 256 KiB budget; append ~3 MiB of batches (≈12x).
        let budget_bytes = 256 * 1024;
        let (manager, budget, _tmp) = setup(budget_bytes).await;
        let scope = SpillScope::new("q-10x", "s", "sh", 0, 0);
        let mut buf =
            SpillablePartitionBuffer::new(manager, scope, schema(), budget, None);

        let mut appended = 0usize;
        // Each batch of 4096 i64 ≈ 32 KiB+; 100 batches ≈ 3+ MiB.
        for i in 0..100 {
            let b = batch(i * 10_000, 4096);
            appended += b.get_array_memory_size();
            buf.append(b).await.unwrap();
            // Live memory must stay near the budget (soft spill).
            assert!(
                buf.resident_bytes() <= budget_bytes + 128 * 1024,
                "resident {} exceeded budget headroom",
                buf.resident_bytes()
            );
        }
        assert!(
            appended >= 10 * budget_bytes,
            "test input only {} bytes, need 10x {}",
            appended,
            budget_bytes
        );
        let m = buf.finish().await.unwrap();
        let got = buf.collect_all().await.unwrap();
        let rows: usize = got.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows as u64, m.rows);
        assert_eq!(rows, 100 * 4096);
        buf.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_releases_memory() {
        let (manager, budget, _tmp) = setup(1024 * 1024).await;
        let scope = SpillScope::new("q-cancel", "s", "sh", 0, 0);
        let mut buf =
            SpillablePartitionBuffer::new(manager, scope, schema(), budget.clone(), None);
        buf.append(batch(0, 100)).await.unwrap();
        assert!(buf.resident_bytes() > 0);
        buf.cancel();
        assert_eq!(buf.resident_bytes(), 0);
        assert_eq!(buf.state(), PartitionBufferState::Cancelled);
        // Budget fully free after cancel.
        assert_eq!(budget.used_bytes(), 0);
    }
}
