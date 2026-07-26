//! Spillable partition buffer for distributed shuffle (Phase 4).
//!
//! Appends accounted in-memory batches until a soft watermark, then spills
//! immutable Arrow segments through the shared [`SpillManager`]. Readers
//! drain residual memory batches first, then stream committed segments.
//!
//! DoExchange intake creates one buffer per accepted partition attempt,
//! appends decoded batches under the shuffle byte budget, finishes (forcing
//! residual spill), then streams segments back to the caller without
//! re-materializing the full exchange volume in RAM.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::{Stream, StreamExt};
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

    /// Cancel intake: drop resident memory and leave the scope guard armed so
    /// spilled segments are deleted on drop. Safe to call from DoExchange when
    /// the client disconnects or a superseding attempt wins mid-stream.
    pub fn cancel(&mut self) {
        self.state = PartitionBufferState::Cancelled;
        self.memory.clear();
        self.memory_bytes = 0;
        // Segments stay listed until Drop of `_guard` deletes the scope; clear
        // the local list so collect/drain cannot observe cancelled data.
        self.segments.clear();
        self.fail_msg = Some("cancelled".to_string());
        if let Some(ref m) = self.metrics {
            m.shuffle_resident_bytes.set(0.0);
        }
    }

    /// Drain remaining in-memory batches and spill segments into a Vec of
    /// batches (for tests / small partitions). Production readers should
    /// prefer [`Self::into_drain_stream`].
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

    /// Finish the partition (if still open) and return a streaming drain that
    /// yields residual memory batches then spill segments one batch at a time.
    ///
    /// The drain deletes the spill scope when fully consumed or dropped, so
    /// callers do not need a separate `cleanup` after a successful stream.
    pub async fn into_drain_stream(mut self) -> anyhow::Result<PartitionDrainStream> {
        if self.state == PartitionBufferState::Failed {
            anyhow::bail!(
                "partition failed: {}",
                self.fail_msg.as_deref().unwrap_or("unknown")
            );
        }
        if self.state == PartitionBufferState::Cancelled {
            anyhow::bail!("partition cancelled");
        }
        // Ensure residuals are spilled so the drain only needs segment I/O
        // (keeps post-finish resident memory near zero).
        if self.state == PartitionBufferState::Open {
            let _ = self.finish().await?;
        }

        let mut memory = VecDeque::new();
        for accounted in std::mem::take(&mut self.memory) {
            let (batch, _) = accounted.into_parts();
            memory.push_back(batch);
        }
        self.memory_bytes = 0;

        // Disarm the buffer guard: PartitionDrainStream owns cleanup.
        if let Some(g) = self._guard.take() {
            g.disarm();
        }

        Ok(PartitionDrainStream {
            manager: self.manager.clone(),
            scope: self.scope.clone(),
            metrics: self.metrics.clone(),
            memory,
            segments: std::mem::take(&mut self.segments),
            segment_idx: 0,
            current: None,
            cleaned: false,
        })
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

/// Streaming reader over a finished [`SpillablePartitionBuffer`].
///
/// Yields residual in-memory batches first, then each spill segment's batches.
/// Deletes the spill scope on full consumption or drop (best-effort async
/// cleanup via `tokio::spawn` on drop when the runtime is available).
pub struct PartitionDrainStream {
    manager: Arc<SpillManager>,
    scope: SpillScope,
    metrics: Option<Arc<WorkerMetricsRegistry>>,
    memory: VecDeque<RecordBatch>,
    segments: Vec<SpillSegment>,
    segment_idx: usize,
    current: Option<futures::stream::BoxStream<'static, sqe_spill::Result<RecordBatch>>>,
    cleaned: bool,
}

impl PartitionDrainStream {
    /// Explicit cleanup (also runs on drop if not already cleaned).
    pub async fn cleanup(&mut self) -> anyhow::Result<()> {
        if self.cleaned {
            return Ok(());
        }
        self.cleaned = true;
        self.manager
            .delete_scope(&self.scope)
            .await
            .map_err(|e| anyhow::anyhow!("spill cleanup: {e}"))?;
        self.segments.clear();
        self.memory.clear();
        self.current = None;
        if let Some(ref m) = self.metrics {
            m.spill_files.set(0.0);
            m.shuffle_resident_bytes.set(0.0);
        }
        Ok(())
    }
}

impl Stream for PartitionDrainStream {
    type Item = anyhow::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // 1. Residual memory batches (usually empty after finish).
        if let Some(batch) = self.memory.pop_front() {
            if let Some(ref m) = self.metrics {
                m.spill_bytes_read
                    .inc_by(batch.get_array_memory_size() as f64);
            }
            return Poll::Ready(Some(Ok(batch)));
        }

        // 2. Current segment stream.
        loop {
            if let Some(ref mut cur) = self.current {
                match Pin::new(cur).poll_next(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        if let Some(ref m) = self.metrics {
                            m.spill_bytes_read
                                .inc_by(batch.get_array_memory_size() as f64);
                        }
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Some(Err(anyhow::anyhow!("spill stream: {e}"))));
                    }
                    Poll::Ready(None) => {
                        self.current = None;
                        self.segment_idx += 1;
                        // fall through to open next segment
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // 3. Open next segment, or finish.
            if self.segment_idx >= self.segments.len() {
                // Schedule cleanup without blocking the poll.
                if !self.cleaned {
                    self.cleaned = true;
                    let manager = self.manager.clone();
                    let scope = self.scope.clone();
                    let metrics = self.metrics.clone();
                    if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        handle.spawn(async move {
                            if let Err(e) = manager.delete_scope(&scope).await {
                                warn!(%scope, error = %e, "drain stream cleanup failed");
                            }
                            if let Some(m) = metrics {
                                m.spill_files.set(0.0);
                                m.shuffle_resident_bytes.set(0.0);
                            }
                        });
                    }
                }
                return Poll::Ready(None);
            }

            let seg = &self.segments[self.segment_idx];
            // open_reader is async; we cannot await inside poll_next.
            // Use a one-shot future stored in `current` via a small helper stream.
            let manager = self.manager.clone();
            let seg = seg.clone();
            let fut_stream = futures::stream::once(async move {
                match manager.open_reader(&seg).await {
                    Ok(reader) => Ok(reader),
                    Err(e) => Err(e),
                }
            })
            .map(|res| match res {
                Ok(reader) => reader.into_stream(),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            })
            .flatten()
            .boxed();
            self.current = Some(fut_stream);
        }
    }
}

impl Drop for PartitionDrainStream {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        self.cleaned = true;
        let manager = self.manager.clone();
        let scope = self.scope.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(e) = manager.delete_scope(&scope).await {
                    warn!(%scope, error = %e, "PartitionDrainStream drop cleanup failed");
                }
            });
        } else {
            warn!(
                %scope,
                "PartitionDrainStream dropped without runtime; spill scope may leak until orphan cleanup"
            );
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

    #[tokio::test]
    async fn drain_stream_yields_all_rows_and_cleans_up() {
        let budget_bytes = 64 * 1024;
        let (manager, budget, tmp) = setup(budget_bytes).await;
        let scope = SpillScope::new("q-drain", "s", "sh", 0, 0);
        let mut buf =
            SpillablePartitionBuffer::new(manager.clone(), scope.clone(), schema(), budget, None);

        let mut peak = 0usize;
        for i in 0..40 {
            buf.append(batch(i * 1000, 512)).await.unwrap();
            peak = peak.max(buf.resident_bytes());
        }
        // Soft spill should have kept residency near the budget.
        assert!(
            peak <= budget_bytes + 128 * 1024,
            "peak resident {peak} far above budget {budget_bytes}"
        );

        let mut drain = buf.into_drain_stream().await.unwrap();
        let mut rows = 0usize;
        while let Some(item) = drain.next().await {
            rows += item.unwrap().num_rows();
        }
        assert_eq!(rows, 40 * 512);

        // Scope directory should be gone after stream completion.
        let scope_dir = tmp.path().join(scope.relative_dir());
        // Give the spawned cleanup task a tick.
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !scope_dir.exists(),
            "expected spill scope cleaned after drain: {}",
            scope_dir.display()
        );
    }

    /// Mirrors DoExchange spill intake: append under budget, finish, stream.
    #[tokio::test]
    async fn cancel_after_spill_drops_resident_and_blocks_drain() {
        let (manager, budget, tmp) = setup(32 * 1024).await;
        let scope = SpillScope::new("q-cancel-spill", "s", "sh", 0, 0);
        let mut buf =
            SpillablePartitionBuffer::new(manager, scope.clone(), schema(), budget.clone(), None);
        for i in 0..30 {
            buf.append(batch(i * 1000, 256)).await.unwrap();
        }
        // Likely spilled; cancel must still zero resident and refuse drain.
        buf.cancel();
        assert_eq!(buf.state(), PartitionBufferState::Cancelled);
        assert_eq!(buf.resident_bytes(), 0);
        assert_eq!(budget.used_bytes(), 0);
        let drain = buf.into_drain_stream().await;
        assert!(drain.is_err(), "cancelled buffer must not open a drain");
        drop(drain);
        // Guard cleanup is async on drop of buffer... buffer already moved/dropped
        // after into_drain_stream failed? into_drain_stream takes self by value only
        // on Ok path — on Err, self is not consumed. Wait, into_drain_stream takes mut self
        // by value, so on Err early return before finish... looking at code:
        // into_drain_stream takes mut self by value, so on Cancelled it returns Err
        // and drops self (and armed guard). Good.
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let scope_dir = tmp.path().join(scope.relative_dir());
        assert!(
            !scope_dir.exists(),
            "cancelled scope should be cleaned: {}",
            scope_dir.display()
        );
    }

    #[tokio::test]
    async fn concurrent_producers_slow_consumer() {
        // Four partitions append concurrently under a shared-style budget each;
        // a slow drain of each partition still sees full row counts.
        let budget_bytes = 128 * 1024;
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            LocalSegmentStore::open(tmp.path(), 1 << 30, 0, 8, 8).unwrap(),
        );
        let manager = Arc::new(SpillManager::new(
            store,
            std::time::Duration::from_secs(0),
        ));

        let mut handles = Vec::new();
        for p in 0..4u32 {
            let manager = manager.clone();
            handles.push(tokio::spawn(async move {
                let pool = Arc::new(FairSpillPool::new(budget_bytes.max(1024 * 1024)));
                let budget = ByteBudget::new(format!("p{p}"), budget_bytes, Some(pool));
                let scope = SpillScope::new("q-conc", "s", "sh", p, 0);
                let mut buf =
                    SpillablePartitionBuffer::new(manager, scope, schema(), budget, None);
                let mut peak = 0usize;
                for i in 0..40 {
                    buf.append(batch((p as i64) * 100_000 + i * 1000, 512))
                        .await
                        .unwrap();
                    peak = peak.max(buf.resident_bytes());
                    assert!(peak <= budget_bytes + 128 * 1024);
                }
                let m = buf.finish().await.unwrap();
                // Slow consumer: yield between batches.
                let mut drain = buf.into_drain_stream().await.unwrap();
                let mut rows = 0usize;
                while let Some(item) = drain.next().await {
                    rows += item.unwrap().num_rows();
                    tokio::task::yield_now().await;
                }
                (m.rows, rows, peak)
            }));
        }

        let mut total_rows = 0u64;
        for h in handles {
            let (manifest_rows, drained, peak) = h.await.unwrap();
            assert_eq!(manifest_rows, drained as u64);
            assert_eq!(manifest_rows, 40 * 512);
            assert!(peak <= budget_bytes + 128 * 1024);
            total_rows += manifest_rows;
        }
        assert_eq!(total_rows, 4 * 40 * 512);
    }

    #[tokio::test]
    async fn do_exchange_style_ten_x_intake_stays_bounded() {
        let budget_bytes = 256 * 1024;
        let (manager, budget, _tmp) = setup(budget_bytes).await;
        let scope = SpillScope::new("q-dx", "stage0", "do_exchange", 0, 1);
        let mut buf =
            SpillablePartitionBuffer::new(manager, scope, schema(), budget, None);

        let mut appended = 0usize;
        let mut peak = 0usize;
        for i in 0..120 {
            let b = batch(i * 10_000, 4096);
            appended += b.get_array_memory_size();
            buf.append(b).await.unwrap();
            peak = peak.max(buf.resident_bytes());
            assert!(
                buf.resident_bytes() <= budget_bytes + 128 * 1024,
                "resident {} exceeded budget headroom during intake",
                buf.resident_bytes()
            );
        }
        assert!(
            appended >= 10 * budget_bytes,
            "need ≥10x budget input, got {appended} vs {budget_bytes}"
        );

        let manifest = buf.finish().await.unwrap();
        assert_eq!(manifest.rows, 120 * 4096);
        assert!(
            manifest.segments > 0,
            "expected at least one spill segment for 10x intake"
        );

        let mut drain = buf.into_drain_stream().await.unwrap();
        let mut rows = 0usize;
        while let Some(item) = drain.next().await {
            rows += item.unwrap().num_rows();
        }
        assert_eq!(rows as u64, manifest.rows);
        assert!(
            peak <= budget_bytes + 128 * 1024,
            "peak resident {peak} must stay near budget {budget_bytes}"
        );
    }
}
