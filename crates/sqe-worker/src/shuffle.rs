//! Shuffle infrastructure for distributed computation via Arrow Flight DoExchange.
//!
//! This module provides:
//! - [`ExchangeDescriptor`]: Describes the type of data exchange (hash or range partition).
//! - [`ShuffleReceiver`]: Per-stage partition buffers backed by bounded mpsc channels.
//! - [`ShuffleManager`]: Registry of active shuffle receivers across queries/stages.
//! - [`HashPartitioner`]: Splits a RecordBatch by hashing key columns modulo partition count.
//! - [`RangePartitioner`]: Splits a RecordBatch using sort-key boundaries for range partitioning.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use datafusion::common::hash_utils::create_hashes;
use serde::{Deserialize, Serialize};
use sqe_metrics::WorkerMetricsRegistry;
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::debug;

// ───────────────────────────── ExchangeDescriptor ─────────────────────────────

/// Describes the type of data exchange for a DoExchange call.
///
/// Serialized as JSON in the first FlightData message's descriptor `cmd` field.
///
/// Phase 4 adds optional attempt / producer identity fields (default 0 / empty
/// for older producers) so late data from a losing task attempt can be rejected.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExchangeDescriptor {
    /// Receive hash-partitioned data for a join or aggregate.
    HashPartition {
        query_id: String,
        stage_id: String,
        partition_id: u32,
        /// Task attempt id. Late data from a lower attempt is rejected once a
        /// higher attempt is committed for this partition.
        #[serde(default)]
        attempt_id: u32,
        /// Opaque producer task id (optional; empty for older clients).
        #[serde(default)]
        producer_task_id: String,
    },
    /// Receive range-partitioned data for a distributed sort.
    RangePartition {
        query_id: String,
        stage_id: String,
        range_bounds: Vec<String>,
        #[serde(default)]
        attempt_id: u32,
        #[serde(default)]
        producer_task_id: String,
    },
}

impl ExchangeDescriptor {
    /// Serialize to JSON bytes for Flight descriptor cmd field.
    pub fn to_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }

    /// Extract the (query_id, stage_id) key for this descriptor.
    pub fn stage_key(&self) -> (String, String) {
        match self {
            ExchangeDescriptor::HashPartition {
                query_id, stage_id, ..
            } => (query_id.clone(), stage_id.clone()),
            ExchangeDescriptor::RangePartition {
                query_id, stage_id, ..
            } => (query_id.clone(), stage_id.clone()),
        }
    }

    /// Extract the partition_id for hash-partitioned exchanges.
    /// For range partitions, returns 0 (all data goes to a single receiver initially).
    pub fn partition_id(&self) -> u32 {
        match self {
            ExchangeDescriptor::HashPartition { partition_id, .. } => *partition_id,
            ExchangeDescriptor::RangePartition { .. } => 0,
        }
    }

    /// Task attempt id (0 when omitted by older producers).
    pub fn attempt_id(&self) -> u32 {
        match self {
            ExchangeDescriptor::HashPartition { attempt_id, .. } => *attempt_id,
            ExchangeDescriptor::RangePartition { attempt_id, .. } => *attempt_id,
        }
    }

    /// Producer task id string (empty when omitted).
    pub fn producer_task_id(&self) -> &str {
        match self {
            ExchangeDescriptor::HashPartition {
                producer_task_id, ..
            } => producer_task_id.as_str(),
            ExchangeDescriptor::RangePartition {
                producer_task_id, ..
            } => producer_task_id.as_str(),
        }
    }
}

// ───────────────────────────── AttemptGate ────────────────────────────────────

/// Tracks the highest committed attempt id per (query, stage, partition).
///
/// Late data from a lower attempt is rejected so a retried producer cannot
/// poison results after a winner has already been accepted.
/// (query_id, stage_id, partition) -> highest accepted attempt id.
type WinnerMap = HashMap<(String, String, u32), u32>;

#[derive(Clone, Default)]
pub struct AttemptGate {
    winners: Arc<Mutex<WinnerMap>>,
}

impl AttemptGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if `attempt_id` is still admissible (equal to or greater
    /// than the current winner). Updates the winner when `attempt_id` is higher.
    pub async fn admit(
        &self,
        query_id: &str,
        stage_id: &str,
        partition_id: u32,
        attempt_id: u32,
    ) -> bool {
        let key = (query_id.to_string(), stage_id.to_string(), partition_id);
        let mut map = self.winners.lock().await;
        match map.get(&key).copied() {
            Some(winner) if attempt_id < winner => false,
            Some(winner) if attempt_id == winner => true,
            _ => {
                map.insert(key, attempt_id);
                true
            }
        }
    }

    /// Current winner for a partition, if any.
    pub async fn winner(&self, query_id: &str, stage_id: &str, partition_id: u32) -> Option<u32> {
        let key = (query_id.to_string(), stage_id.to_string(), partition_id);
        self.winners.lock().await.get(&key).copied()
    }
}

// ───────────────────────────── ShuffleReceiver ────────────────────────────────

/// Default bounded channel capacity per partition.
///
/// A batch-count bound does **not** bound resident bytes. `send_batch`
/// also waits on [`DEFAULT_MAX_RESIDENT_BYTES`] (issue #406). DoExchange
/// prefers `SpillablePartitionBuffer` when a SpillManager is configured.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 64;

/// Default resident-byte cap across all partitions of one `ShuffleReceiver`.
///
/// One 64-slot channel of 8 MiB batches is 512 MiB *outside* the DataFusion
/// pool. 64 MiB backpressures the producer before that cliff. A single batch
/// larger than the cap is still admitted so a wide row is not wedged.
pub const DEFAULT_MAX_RESIDENT_BYTES: usize = 64 * 1024 * 1024;

/// Holds per-partition mpsc channels for receiving shuffled RecordBatches.
///
/// The sender side is used by the DoExchange handler when data arrives.
/// The receiver side is consumed by the downstream operator (e.g., ShuffleReaderExec).
///
/// Resident bytes across all partitions are tracked in `resident_bytes` and
/// published to [`WorkerMetricsRegistry::shuffle_resident_bytes`] when metrics
/// are attached. `send_batch` waits when adding a batch would exceed
/// `max_resident_bytes` (issue #406). Disk spill remains the DoExchange
/// SpillManager path; this cap is the legacy mpsc safety net.
pub struct ShuffleReceiver {
    /// Per-partition senders — DoExchange handler writes here.
    senders: HashMap<u32, mpsc::Sender<RecordBatch>>,
    /// Per-partition receivers — consuming operators read from here.
    receivers: Mutex<HashMap<u32, mpsc::Receiver<RecordBatch>>>,
    /// Schema of the data being shuffled.
    schema: SchemaRef,
    /// Sum of `get_array_memory_size()` across batches currently in any
    /// partition channel. Incremented on successful send, decremented on recv.
    resident_bytes: Arc<AtomicUsize>,
    /// Byte cap for [`Self::send_batch`]. 0 disables the wait (item bound only).
    max_resident_bytes: usize,
    /// Wakes `send_batch` waiters when a recv frees bytes.
    space: Arc<Notify>,
    /// Optional worker metrics for the shuffle_resident_bytes gauge.
    metrics: Option<Arc<WorkerMetricsRegistry>>,
}

impl ShuffleReceiver {
    /// Create a new ShuffleReceiver with the given number of partitions and schema.
    ///
    /// Each partition gets a bounded mpsc channel with `capacity` buffer slots.
    pub fn new(num_partitions: u32, schema: SchemaRef, capacity: usize) -> Self {
        Self::new_with_metrics(num_partitions, schema, capacity, None)
    }

    /// Create a ShuffleReceiver that publishes resident-byte gauges.
    pub fn new_with_metrics(
        num_partitions: u32,
        schema: SchemaRef,
        capacity: usize,
        metrics: Option<Arc<WorkerMetricsRegistry>>,
    ) -> Self {
        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();

        for partition_id in 0..num_partitions {
            let (tx, rx) = mpsc::channel(capacity);
            senders.insert(partition_id, tx);
            receivers.insert(partition_id, rx);
        }

        Self {
            senders,
            receivers: Mutex::new(receivers),
            schema,
            resident_bytes: Arc::new(AtomicUsize::new(0)),
            max_resident_bytes: DEFAULT_MAX_RESIDENT_BYTES,
            space: Arc::new(Notify::new()),
            metrics,
        }
    }

    /// Override the resident-byte cap. `0` disables the wait (item bound only).
    pub fn with_max_resident_bytes(mut self, max_resident_bytes: usize) -> Self {
        self.max_resident_bytes = max_resident_bytes;
        self
    }

    /// Create a ShuffleReceiver with default channel capacity.
    pub fn with_defaults(num_partitions: u32, schema: SchemaRef) -> Self {
        Self::new(num_partitions, schema, DEFAULT_CHANNEL_CAPACITY)
    }

    /// Get a sender for a given partition. Used by the DoExchange handler.
    ///
    /// Prefer [`Self::send_batch`] when resident-byte tracking is required:
    /// raw sends bypass the gauge.
    pub fn sender(&self, partition_id: u32) -> Option<&mpsc::Sender<RecordBatch>> {
        self.senders.get(&partition_id)
    }

    /// Send a batch into a partition, updating the resident-byte gauge.
    ///
    /// Waits while `resident + batch` would exceed `max_resident_bytes`,
    /// except when the receiver is empty: a single oversized batch is still
    /// admitted so a wide row cannot deadlock. Returns `Err(batch)` if the
    /// channel is closed (receiver dropped).
    pub async fn send_batch(
        &self,
        partition_id: u32,
        batch: RecordBatch,
    ) -> Result<(), RecordBatch> {
        let sender = match self.senders.get(&partition_id) {
            Some(s) => s,
            None => return Err(batch),
        };
        let bytes = batch.get_array_memory_size();
        if self.max_resident_bytes > 0 {
            loop {
                let cur = self.resident_bytes.load(Ordering::Relaxed);
                if cur == 0 || cur.saturating_add(bytes) <= self.max_resident_bytes {
                    break;
                }
                self.space.notified().await;
            }
        }
        match sender.send(batch).await {
            Ok(()) => {
                self.resident_bytes.fetch_add(bytes, Ordering::Relaxed);
                self.publish_resident();
                Ok(())
            }
            Err(e) => Err(e.0),
        }
    }

    /// Take the receiver for a given partition. This can only be called once per partition.
    ///
    /// Returns `None` if the receiver was already taken or the partition doesn't exist.
    /// The returned stream decrements resident-byte accounting as batches are pulled.
    pub async fn take_receiver(&self, partition_id: u32) -> Option<TrackedPartitionReceiver> {
        let rx = self.receivers.lock().await.remove(&partition_id)?;
        Some(TrackedPartitionReceiver {
            inner: rx,
            resident_bytes: self.resident_bytes.clone(),
            space: self.space.clone(),
            metrics: self.metrics.clone(),
        })
    }

    /// Current resident bytes across all partition channels.
    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes.load(Ordering::Relaxed)
    }

    /// Get the schema of the shuffled data.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn publish_resident(&self) {
        if let Some(ref m) = self.metrics {
            m.shuffle_resident_bytes
                .set(self.resident_bytes.load(Ordering::Relaxed) as f64);
        }
    }
}

/// Partition receiver that decrements shuffle resident-byte accounting on
/// every successful `recv`. Dropping the receiver does **not** automatically
/// drain remaining batches; call sites that abandon a partition should drain
/// or accept a residual gauge until the `ShuffleReceiver` is dropped.
pub struct TrackedPartitionReceiver {
    inner: mpsc::Receiver<RecordBatch>,
    resident_bytes: Arc<AtomicUsize>,
    space: Arc<Notify>,
    metrics: Option<Arc<WorkerMetricsRegistry>>,
}

impl TrackedPartitionReceiver {
    /// Receive the next batch, updating resident-byte accounting.
    pub async fn recv(&mut self) -> Option<RecordBatch> {
        let batch = self.inner.recv().await?;
        let bytes = batch.get_array_memory_size();
        let _ = self
            .resident_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(bytes))
            });
        if let Some(ref m) = self.metrics {
            m.shuffle_resident_bytes
                .set(self.resident_bytes.load(Ordering::Relaxed) as f64);
        }
        self.space.notify_waiters();
        Some(batch)
    }
}

// ───────────────────────────── ShuffleManager ─────────────────────────────────

/// Key for looking up shuffle receivers: (query_id, stage_id).
type StageKey = (String, String);

/// Manages ShuffleReceivers across queries and stages.
///
/// The coordinator pre-registers receivers before dispatching stages.
/// Workers look up receivers when DoExchange calls arrive.
#[derive(Clone)]
pub struct ShuffleManager {
    receivers: Arc<Mutex<HashMap<StageKey, Arc<ShuffleReceiver>>>>,
    /// Attempt admission for late-data rejection (Phase 4).
    attempts: AttemptGate,
}

impl ShuffleManager {
    pub fn new() -> Self {
        Self {
            receivers: Arc::new(Mutex::new(HashMap::new())),
            attempts: AttemptGate::new(),
        }
    }

    /// Shared attempt gate for DoExchange intake.
    pub fn attempts(&self) -> &AttemptGate {
        &self.attempts
    }

    /// Register a new ShuffleReceiver for a (query_id, stage_id).
    pub async fn register(&self, query_id: &str, stage_id: &str, receiver: Arc<ShuffleReceiver>) {
        let key = (query_id.to_string(), stage_id.to_string());
        debug!(
            query_id = %query_id,
            stage_id = %stage_id,
            "Registering shuffle receiver"
        );
        self.receivers.lock().await.insert(key, receiver);
    }

    /// Look up a ShuffleReceiver by (query_id, stage_id).
    pub async fn get(&self, query_id: &str, stage_id: &str) -> Option<Arc<ShuffleReceiver>> {
        let key = (query_id.to_string(), stage_id.to_string());
        self.receivers.lock().await.get(&key).cloned()
    }

    /// Remove a ShuffleReceiver when a stage completes.
    pub async fn remove(&self, query_id: &str, stage_id: &str) -> Option<Arc<ShuffleReceiver>> {
        let key = (query_id.to_string(), stage_id.to_string());
        debug!(
            query_id = %query_id,
            stage_id = %stage_id,
            "Removing shuffle receiver"
        );
        self.receivers.lock().await.remove(&key)
    }
}

impl Default for ShuffleManager {
    fn default() -> Self {
        Self::new()
    }
}

// ───────────────────────────── Bounded partition groups ───────────────────────

/// Default cap on simultaneous materialised partition-slice bytes when grouping
/// (`partition_grouped`). Tuned for laptop 64 MiB workers; callers with a
/// known shuffle sub-budget should pass that instead.
pub const DEFAULT_PARTITION_GROUP_BYTES: usize = 4 * 1024 * 1024;

/// Approximate logical size of a partition slice with `n_rows` out of
/// `batch.num_rows()` total (pro-rata of `get_array_memory_size`).
fn approx_slice_bytes(batch: &RecordBatch, n_rows: usize) -> usize {
    if batch.num_rows() == 0 || n_rows == 0 {
        return 0;
    }
    let total = batch.get_array_memory_size();
    // Ceiling division so a non-empty slice is never estimated as zero.
    total
        .saturating_mul(n_rows)
        .div_ceil(batch.num_rows())
        .max(1)
}

/// Materialise non-empty partition slices for the given partition IDs only.
fn take_partitions(
    batch: &RecordBatch,
    partition_indices: &[Vec<u32>],
    partition_ids: &[usize],
) -> anyhow::Result<Vec<(u32, RecordBatch)>> {
    let mut result = Vec::with_capacity(partition_ids.len());
    for &pid in partition_ids {
        let indices = &partition_indices[pid];
        if indices.is_empty() {
            continue;
        }
        let indices_array = UInt32Array::from(indices.clone());
        let taken_columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|col| {
                arrow::compute::take(col.as_ref(), &indices_array, None)
                    .map_err(|e| anyhow::anyhow!("take failed: {e}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let partition_batch = RecordBatch::try_new(batch.schema(), taken_columns)?;
        result.push((pid as u32, partition_batch));
    }
    Ok(result)
}

/// Pack non-empty partition IDs into groups whose approximate materialised
/// size stays under `max_group_bytes`. A single partition that alone exceeds
/// the cap still forms its own group (cannot split further without row
/// slicing).
fn group_partition_ids(
    batch: &RecordBatch,
    partition_indices: &[Vec<u32>],
    max_group_bytes: usize,
) -> Vec<Vec<usize>> {
    let cap = max_group_bytes.max(1);
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut current: Vec<usize> = Vec::new();
    let mut current_bytes = 0usize;

    for (pid, indices) in partition_indices.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let slice_bytes = approx_slice_bytes(batch, indices.len());
        if !current.is_empty() && current_bytes.saturating_add(slice_bytes) > cap {
            groups.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current.push(pid);
        current_bytes = current_bytes.saturating_add(slice_bytes);
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

/// Materialise partition slices in bounded groups so peak concurrent output
/// is approximately `max_group_bytes` rather than the full fan-out of all
/// partitions at once.
fn materialise_partition_groups(
    batch: &RecordBatch,
    partition_indices: Vec<Vec<u32>>,
    max_group_bytes: usize,
) -> anyhow::Result<Vec<Vec<(u32, RecordBatch)>>> {
    let id_groups = group_partition_ids(batch, &partition_indices, max_group_bytes);
    let mut out = Vec::with_capacity(id_groups.len());
    for ids in id_groups {
        let group = take_partitions(batch, &partition_indices, &ids)?;
        if !group.is_empty() {
            out.push(group);
        }
    }
    Ok(out)
}

// ───────────────────────────── HashPartitioner ────────────────────────────────

/// Splits a RecordBatch by hashing key columns modulo the number of partitions.
///
/// Uses DataFusion's `create_hashes()` for consistent hashing, then
/// `arrow::compute::take()` to extract rows for each partition.
///
/// Prefer [`Self::partition_grouped`] when the fan-out would materialise more
/// partition slices than the shuffle byte budget can hold at once.
pub struct HashPartitioner {
    /// Column names to hash on.
    key_columns: Vec<String>,
    /// Number of output partitions.
    num_partitions: usize,
}

impl HashPartitioner {
    pub fn new(key_columns: Vec<String>, num_partitions: usize) -> Self {
        assert!(num_partitions > 0, "num_partitions must be > 0");
        Self {
            key_columns,
            num_partitions,
        }
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Partition a RecordBatch by hashing the key columns.
    ///
    /// Returns a Vec of (partition_id, RecordBatch) pairs. Empty partitions
    /// are omitted from the result. Equivalent to a single group from
    /// [`Self::partition_grouped`] with an unbounded group size.
    pub fn partition(&self, batch: &RecordBatch) -> anyhow::Result<Vec<(u32, RecordBatch)>> {
        let groups = self.partition_grouped(batch, usize::MAX)?;
        Ok(groups.into_iter().flatten().collect())
    }

    /// Hash-partition one input batch, materialising output slices in groups
    /// whose approximate combined size stays under `max_group_bytes`.
    ///
    /// Assignment (hashes + row indices) is computed once for the whole batch;
    /// only `take()` materialisation is grouped so peak concurrent output
    /// memory is bounded.
    pub fn partition_grouped(
        &self,
        batch: &RecordBatch,
        max_group_bytes: usize,
    ) -> anyhow::Result<Vec<Vec<(u32, RecordBatch)>>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        if self.num_partitions == 1 {
            return Ok(vec![vec![(0, batch.clone())]]);
        }

        let partition_indices = self.assign_rows(batch)?;
        materialise_partition_groups(batch, partition_indices, max_group_bytes)
    }

    /// Assign each row to a partition id (no materialisation).
    fn assign_rows(&self, batch: &RecordBatch) -> anyhow::Result<Vec<Vec<u32>>> {
        let key_arrays: Vec<ArrayRef> = self
            .key_columns
            .iter()
            .map(|name| {
                batch
                    .column_by_name(name)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Key column '{}' not found in batch", name))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        // DF 54 switched the hash backend from ahash to foldhash; `RandomState` is now
        // an alias for `foldhash::fast::FixedState`. Fixed seed-0 state keeps the
        // shuffle deterministic across nodes (DF REPARTITION_RANDOM_STATE).
        let mut hashes = vec![0u64; batch.num_rows()];
        create_hashes(
            &key_arrays,
            &datafusion::common::hash_utils::RandomState::default(),
            &mut hashes,
        )?;

        let num_partitions = self.num_partitions as u64;
        let mut partition_indices: Vec<Vec<u32>> = vec![Vec::new(); self.num_partitions];
        for (row_idx, h) in hashes.iter().enumerate() {
            let partition_id = (h % num_partitions) as usize;
            partition_indices[partition_id].push(row_idx as u32);
        }
        Ok(partition_indices)
    }
}

// ───────────────────────────── RangePartitioner ──────────────────────────────

/// Splits a RecordBatch using sort-key boundaries for range partitioning.
///
/// Given P-1 boundaries for P partitions, each row is assigned to a partition
/// by binary searching the key column value against the boundaries.
pub struct RangePartitioner {
    /// Boundary values between partitions (sorted ascending). For P partitions,
    /// there are P-1 boundaries. Semantics:
    ///   partition 0: key < boundaries[0]
    ///   partition i: boundaries[i-1] <= key < boundaries[i]
    ///   partition P-1: key >= boundaries[P-2]
    boundaries: Vec<i64>,
    /// Column name to partition on.
    key_column: String,
    /// Total number of partitions (boundaries.len() + 1).
    num_partitions: usize,
}

impl RangePartitioner {
    /// Create a range partitioner.
    ///
    /// `boundaries` must be sorted in ascending order. The number of output
    /// partitions will be `boundaries.len() + 1`.
    pub fn new(key_column: String, boundaries: Vec<i64>) -> Self {
        let num_partitions = boundaries.len() + 1;
        Self {
            boundaries,
            key_column,
            num_partitions,
        }
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Partition a RecordBatch by range on the key column.
    ///
    /// Returns a Vec of (partition_id, RecordBatch) pairs. Empty partitions
    /// are omitted. Equivalent to unbounded [`Self::partition_grouped`].
    pub fn partition(&self, batch: &RecordBatch) -> anyhow::Result<Vec<(u32, RecordBatch)>> {
        let groups = self.partition_grouped(batch, usize::MAX)?;
        Ok(groups.into_iter().flatten().collect())
    }

    /// Range-partition one input batch with bounded materialisation groups
    /// (same contract as [`HashPartitioner::partition_grouped`]).
    pub fn partition_grouped(
        &self,
        batch: &RecordBatch,
        max_group_bytes: usize,
    ) -> anyhow::Result<Vec<Vec<(u32, RecordBatch)>>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        if self.num_partitions == 1 {
            return Ok(vec![vec![(0, batch.clone())]]);
        }

        let partition_indices = self.assign_rows(batch)?;
        materialise_partition_groups(batch, partition_indices, max_group_bytes)
    }

    fn assign_rows(&self, batch: &RecordBatch) -> anyhow::Result<Vec<Vec<u32>>> {
        let key_col = batch.column_by_name(&self.key_column).ok_or_else(|| {
            anyhow::anyhow!("Key column '{}' not found in batch", self.key_column)
        })?;

        // Int64 covers the common sort keys (timestamps, IDs). Wider type
        // support can land without changing the grouping contract.
        let key_array = key_col
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "RangePartitioner currently supports Int64 key columns, got {:?}",
                    key_col.data_type()
                )
            })?;

        let mut partition_indices: Vec<Vec<u32>> = vec![Vec::new(); self.num_partitions];
        for row_idx in 0..batch.num_rows() {
            let value = key_array.value(row_idx);
            let partition_id = self.boundaries.partition_point(|b| *b <= value);
            partition_indices[partition_id].push(row_idx as u32);
        }
        Ok(partition_indices)
    }
}

// ───────────────────────────── Tests ──────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn make_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
        let schema = test_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    // ─── ExchangeDescriptor tests ───

    #[test]
    fn test_exchange_descriptor_roundtrip_hash() {
        let desc = ExchangeDescriptor::HashPartition {
            query_id: "q1".to_string(),
            stage_id: "s1".to_string(),
            partition_id: 3,
            attempt_id: 2,
            producer_task_id: "prod-a".to_string(),
        };
        let bytes = desc.to_bytes().unwrap();
        let decoded = ExchangeDescriptor::from_bytes(&bytes).unwrap();
        assert_eq!(desc, decoded);
        assert_eq!(decoded.attempt_id(), 2);
        assert_eq!(decoded.producer_task_id(), "prod-a");
    }

    #[test]
    fn test_exchange_descriptor_roundtrip_range() {
        let desc = ExchangeDescriptor::RangePartition {
            query_id: "q2".to_string(),
            stage_id: "s2".to_string(),
            range_bounds: vec!["10".to_string(), "20".to_string()],
            attempt_id: 0,
            producer_task_id: String::new(),
        };
        let bytes = desc.to_bytes().unwrap();
        let decoded = ExchangeDescriptor::from_bytes(&bytes).unwrap();
        assert_eq!(desc, decoded);
    }

    #[test]
    fn test_exchange_descriptor_legacy_json_defaults_attempt() {
        // Older producers omit attempt_id / producer_task_id.
        let json = br#"{"HashPartition":{"query_id":"q","stage_id":"s","partition_id":1}}"#;
        let decoded = ExchangeDescriptor::from_bytes(json).unwrap();
        assert_eq!(decoded.partition_id(), 1);
        assert_eq!(decoded.attempt_id(), 0);
        assert_eq!(decoded.producer_task_id(), "");
    }

    #[test]
    fn test_exchange_descriptor_stage_key() {
        let desc = ExchangeDescriptor::HashPartition {
            query_id: "q1".to_string(),
            stage_id: "s1".to_string(),
            partition_id: 0,
            attempt_id: 0,
            producer_task_id: String::new(),
        };
        assert_eq!(desc.stage_key(), ("q1".to_string(), "s1".to_string()));
    }

    #[tokio::test]
    async fn attempt_gate_rejects_late_data() {
        let gate = AttemptGate::new();
        assert!(gate.admit("q", "s", 0, 1).await);
        assert!(gate.admit("q", "s", 0, 1).await); // same attempt ok
        assert!(!gate.admit("q", "s", 0, 0).await); // older rejected
        assert!(gate.admit("q", "s", 0, 2).await); // newer wins
        assert!(!gate.admit("q", "s", 0, 1).await);
        assert_eq!(gate.winner("q", "s", 0).await, Some(2));
    }

    // ─── ShuffleReceiver tests ───

    #[tokio::test]
    async fn test_shuffle_receiver_send_recv() {
        let schema = test_schema();
        let receiver = ShuffleReceiver::with_defaults(2, schema);

        let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);

        // Send to partition 0
        let sender = receiver.sender(0).unwrap();
        sender.send(batch.clone()).await.unwrap();

        // Receive from partition 0
        let mut rx = receiver.take_receiver(0).await.unwrap();
        let received = rx.recv().await.unwrap();
        assert_eq!(received.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_shuffle_receiver_take_once() {
        let schema = test_schema();
        let receiver = ShuffleReceiver::with_defaults(1, schema);

        // First take succeeds
        assert!(receiver.take_receiver(0).await.is_some());
        // Second take returns None
        assert!(receiver.take_receiver(0).await.is_none());
    }

    #[tokio::test]
    async fn send_batch_waits_when_over_byte_cap() {
        let schema = test_schema();
        let receiver = ShuffleReceiver::new(1, schema, 8).with_max_resident_bytes(1);
        let first = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        let second = make_batch(vec![4, 5, 6], vec!["d", "e", "f"]);
        assert!(first.get_array_memory_size() > 1);
        let mut rx = receiver.take_receiver(0).await.expect("rx");
        receiver
            .send_batch(0, first)
            .await
            .expect("first batch admitted on empty receiver");
        assert!(receiver.resident_bytes() > 1);

        let drain = async {
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            rx.recv().await
        };
        let send = receiver.send_batch(0, second);
        let (got, sent) = tokio::join!(drain, send);
        assert!(got.is_some(), "first batch drained");
        sent.expect("second send after drain");
    }

    // ─── ShuffleManager tests ───

    #[tokio::test]
    async fn test_shuffle_manager_register_get_remove() {
        let manager = ShuffleManager::new();
        let schema = test_schema();
        let receiver = Arc::new(ShuffleReceiver::with_defaults(4, schema));

        manager.register("q1", "s1", receiver.clone()).await;

        assert!(manager.get("q1", "s1").await.is_some());
        assert!(manager.get("q1", "s2").await.is_none());

        manager.remove("q1", "s1").await;
        assert!(manager.get("q1", "s1").await.is_none());
    }

    // ─── HashPartitioner tests ───

    #[test]
    fn test_hash_partitioner_4_partitions_distributes() {
        let batch = make_batch(
            (0..100).collect(),
            (0..100)
                .map(|i| format!("name_{i}"))
                .collect::<Vec<_>>()
                .iter()
                .map(|s| s.as_str())
                .collect(),
        );

        let partitioner = HashPartitioner::new(vec!["id".to_string()], 4);
        let result = partitioner.partition(&batch).unwrap();

        // All partitions should have some rows (probabilistically)
        let total_rows: usize = result.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total_rows, 100, "All rows must be accounted for");

        // Check that partition IDs are in range
        for (pid, _) in &result {
            assert!(*pid < 4, "Partition ID must be < 4");
        }

        // With 100 distinct int IDs hashed to 4 partitions, each should have rows
        assert!(
            result.len() >= 2,
            "With 100 rows and 4 partitions, at least 2 should be non-empty"
        );
    }

    #[test]
    fn test_hash_partitioner_deterministic() {
        let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
        let partitioner = HashPartitioner::new(vec!["id".to_string()], 4);

        let result1 = partitioner.partition(&batch).unwrap();
        let result2 = partitioner.partition(&batch).unwrap();

        // Same input should produce same partitioning
        assert_eq!(result1.len(), result2.len());
        for ((p1, b1), (p2, b2)) in result1.iter().zip(result2.iter()) {
            assert_eq!(p1, p2);
            assert_eq!(b1.num_rows(), b2.num_rows());
        }
    }

    #[test]
    fn test_hash_partitioner_empty_batch() {
        let schema = test_schema();
        let batch = RecordBatch::new_empty(schema);

        let partitioner = HashPartitioner::new(vec!["id".to_string()], 4);
        let result = partitioner.partition(&batch).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_hash_partitioner_single_partition() {
        let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        let partitioner = HashPartitioner::new(vec!["id".to_string()], 1);

        let result = partitioner.partition(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, 0);
        assert_eq!(result[0].1.num_rows(), 3);
    }

    #[test]
    fn test_hash_partitioner_preserves_schema() {
        let batch = make_batch(vec![1, 2, 3, 4], vec!["a", "b", "c", "d"]);
        let partitioner = HashPartitioner::new(vec!["id".to_string()], 2);

        let result = partitioner.partition(&batch).unwrap();
        for (_, partition_batch) in &result {
            assert_eq!(partition_batch.schema(), batch.schema());
        }
    }

    // ─── RangePartitioner tests ───

    fn make_range_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_range_partitioner_3_partitions() {
        // Boundaries: [10, 20] → 3 partitions:
        //   partition 0: key < 10        (strictly less than first boundary)
        //   partition 1: 10 <= key < 20  (between boundaries)
        //   partition 2: key >= 20       (at or above last boundary)
        let batch = make_range_batch(vec![5, 10, 15, 20, 25], vec!["a", "b", "c", "d", "e"]);

        let partitioner = RangePartitioner::new("id".to_string(), vec![10, 20]);
        let result = partitioner.partition(&batch).unwrap();

        // Verify partitions
        let mut partition_map: HashMap<u32, Vec<i64>> = HashMap::new();
        for (pid, b) in &result {
            let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let vals: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
            partition_map.insert(*pid, vals);
        }

        // partition 0: values < 10 → [5]
        assert_eq!(partition_map.get(&0).unwrap(), &vec![5]);
        // partition 1: 10 <= values < 20 → [10, 15]
        assert_eq!(partition_map.get(&1).unwrap(), &vec![10, 15]);
        // partition 2: values >= 20 → [20, 25]
        assert_eq!(partition_map.get(&2).unwrap(), &vec![20, 25]);
    }

    #[test]
    fn test_range_partitioner_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::new_empty(schema);

        let partitioner = RangePartitioner::new("id".to_string(), vec![10, 20]);
        let result = partitioner.partition(&batch).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_range_partitioner_single_partition_no_boundaries() {
        let batch = make_range_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        let partitioner = RangePartitioner::new("id".to_string(), vec![]);

        let result = partitioner.partition(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, 0);
        assert_eq!(result[0].1.num_rows(), 3);
    }

    #[test]
    fn test_range_partitioner_all_in_one_partition() {
        // All values < first boundary
        let batch = make_range_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        let partitioner = RangePartitioner::new("id".to_string(), vec![100, 200]);

        let result = partitioner.partition(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, 0);
        assert_eq!(result[0].1.num_rows(), 3);
    }

    #[test]
    fn test_range_partitioner_preserves_total_rows() {
        let batch = make_range_batch(
            vec![1, 5, 10, 15, 20, 25, 30],
            vec!["a", "b", "c", "d", "e", "f", "g"],
        );
        let partitioner = RangePartitioner::new("id".to_string(), vec![10, 20]);

        let result = partitioner.partition(&batch).unwrap();
        let total_rows: usize = result.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total_rows, 7);
    }

    // ─── DoExchange round-trip test ───

    #[tokio::test]
    async fn test_shuffle_receiver_10_batches_in_order() {
        let schema = test_schema();
        let receiver = ShuffleReceiver::new(1, schema, 16);

        // Send 10 batches through partition 0
        let sender = receiver.sender(0).unwrap().clone();

        let send_handle = tokio::spawn(async move {
            for i in 0..10 {
                let batch = make_batch(vec![i], vec!["batch"]);
                sender.send(batch).await.unwrap();
            }
            // Drop sender to signal completion
        });

        let mut rx = receiver.take_receiver(0).await.unwrap();

        // Receive all 10 and verify order
        let mut received = Vec::new();
        // Wait for sender to finish, then drain
        send_handle.await.unwrap();
        // Close the sender side by dropping remaining senders
        drop(receiver);

        while let Some(batch) = rx.recv().await {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            received.push(ids.value(0));
        }

        assert_eq!(received, (0..10).collect::<Vec<i32>>());
    }

    // ─── Additional HashPartitioner tests (Task 17) ───

    #[test]
    fn test_hash_partitioner_multi_column_key() {
        // Hash on both id and name columns
        let batch = make_batch(vec![1, 1, 2, 2], vec!["a", "b", "a", "b"]);
        let partitioner = HashPartitioner::new(vec!["id".to_string(), "name".to_string()], 4);
        let result = partitioner.partition(&batch).unwrap();

        let total_rows: usize = result.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(
            total_rows, 4,
            "All rows accounted for with multi-column key"
        );

        // Rows with same (id, name) must land in same partition
        // (1, "a") and (2, "a") have different id, may differ
        // (1, "a") appears once, so nothing to pair-check here,
        // but total must be preserved.
        for (pid, _) in &result {
            assert!(*pid < 4);
        }
    }

    #[test]
    fn test_hash_partitioner_missing_column_errors() {
        let batch = make_batch(vec![1, 2], vec!["a", "b"]);
        let partitioner = HashPartitioner::new(vec!["nonexistent".to_string()], 2);
        let result = partitioner.partition(&batch);
        assert!(result.is_err(), "Should error when key column is missing");
    }

    #[test]
    fn test_hash_partitioner_same_key_same_partition() {
        // All rows have the same key value — they must all land in one partition
        let batch = make_batch(vec![42, 42, 42, 42, 42], vec!["a", "b", "c", "d", "e"]);
        let partitioner = HashPartitioner::new(vec!["id".to_string()], 4);
        let result = partitioner.partition(&batch).unwrap();

        assert_eq!(result.len(), 1, "All identical keys → single partition");
        assert_eq!(result[0].1.num_rows(), 5);
    }

    // ─── Additional RangePartitioner tests (Task 17) ───

    #[test]
    fn test_range_partitioner_negative_values() {
        // Boundaries: [-10, 0, 10] → 4 partitions
        let batch = make_range_batch(
            vec![-20, -10, -5, 0, 5, 10, 20],
            vec!["a", "b", "c", "d", "e", "f", "g"],
        );
        let partitioner = RangePartitioner::new("id".to_string(), vec![-10, 0, 10]);
        let result = partitioner.partition(&batch).unwrap();

        let mut partition_map: HashMap<u32, Vec<i64>> = HashMap::new();
        for (pid, b) in &result {
            let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let vals: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
            partition_map.insert(*pid, vals);
        }

        // partition 0: key < -10 → [-20]
        assert_eq!(partition_map.get(&0).unwrap(), &vec![-20]);
        // partition 1: -10 <= key < 0 → [-10, -5]
        assert_eq!(partition_map.get(&1).unwrap(), &vec![-10, -5]);
        // partition 2: 0 <= key < 10 → [0, 5]
        assert_eq!(partition_map.get(&2).unwrap(), &vec![0, 5]);
        // partition 3: key >= 10 → [10, 20]
        assert_eq!(partition_map.get(&3).unwrap(), &vec![10, 20]);
    }

    #[test]
    fn test_range_partitioner_missing_column_errors() {
        let batch = make_range_batch(vec![1], vec!["a"]);
        let partitioner = RangePartitioner::new("nonexistent".to_string(), vec![10]);
        let result = partitioner.partition(&batch);
        assert!(result.is_err(), "Should error when key column is missing");
    }

    #[test]
    fn test_range_partitioner_preserves_schema() {
        let batch = make_range_batch(vec![1, 10, 20], vec!["a", "b", "c"]);
        let partitioner = RangePartitioner::new("id".to_string(), vec![5, 15]);
        let result = partitioner.partition(&batch).unwrap();

        for (_, partition_batch) in &result {
            assert_eq!(partition_batch.schema(), batch.schema());
        }
    }

    #[test]
    fn test_range_partitioner_all_in_last_partition() {
        // All values >= last boundary
        let batch = make_range_batch(vec![100, 200, 300], vec!["a", "b", "c"]);
        let partitioner = RangePartitioner::new("id".to_string(), vec![10, 20]);

        let result = partitioner.partition(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, 2); // Last partition
        assert_eq!(result[0].1.num_rows(), 3);
    }

    // ─── Bounded partition groups (Phase 4) ───

    #[test]
    fn hash_partition_grouped_preserves_all_rows() {
        let batch = make_batch((0..64).collect(), vec!["x"; 64]);
        let partitioner = HashPartitioner::new(vec!["id".to_string()], 16);
        let flat = partitioner.partition(&batch).unwrap();
        let groups = partitioner
            .partition_grouped(&batch, 256) // tiny cap forces multiple groups
            .unwrap();
        assert!(
            groups.len() > 1,
            "expected multiple groups under tiny cap, got {}",
            groups.len()
        );
        let grouped_rows: usize = groups
            .iter()
            .flat_map(|g| g.iter().map(|(_, b)| b.num_rows()))
            .sum();
        let flat_rows: usize = flat.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(grouped_rows, 64);
        assert_eq!(grouped_rows, flat_rows);

        // Groups are non-empty and cover every non-empty partition exactly once.
        let mut seen = std::collections::HashSet::new();
        for g in &groups {
            assert!(!g.is_empty());
            for (pid, _) in g {
                assert!(
                    seen.insert(*pid),
                    "partition {pid} appears in multiple groups"
                );
            }
        }
        assert_eq!(seen.len(), flat.len());
    }

    #[test]
    fn hash_partition_grouped_unbounded_is_single_group() {
        let batch = make_batch(vec![1, 2, 3, 4], vec!["a", "b", "c", "d"]);
        let partitioner = HashPartitioner::new(vec!["id".to_string()], 4);
        let groups = partitioner.partition_grouped(&batch, usize::MAX).unwrap();
        assert_eq!(groups.len(), 1);
        let flat_from_groups: Vec<_> = groups.into_iter().flatten().collect();
        let flat = partitioner.partition(&batch).unwrap();
        assert_eq!(flat_from_groups.len(), flat.len());
    }

    #[test]
    fn range_partition_grouped_preserves_rows() {
        let ids: Vec<i64> = (0..100).collect();
        let names: Vec<&str> = vec!["n"; 100];
        let batch = make_range_batch(ids, names);
        let partitioner = RangePartitioner::new("id".to_string(), vec![25, 50, 75]);
        let groups = partitioner.partition_grouped(&batch, 512).unwrap();
        let rows: usize = groups
            .iter()
            .flat_map(|g| g.iter().map(|(_, b)| b.num_rows()))
            .sum();
        assert_eq!(rows, 100);
        // With 4 partitions and a small cap we usually get >1 group.
        assert!(!groups.is_empty());
    }

    #[test]
    fn group_partition_ids_respects_byte_cap() {
        // Two equal non-empty partitions; tiny cap forces one partition per group.
        let batch = make_batch(vec![0, 1], vec!["a", "b"]);
        // Force assignments: use enough partitions that each row likely lands alone.
        let indices = vec![vec![0u32], vec![1u32], vec![]];
        let groups = group_partition_ids(&batch, &indices, 1);
        assert_eq!(
            groups.len(),
            2,
            "each non-empty partition should be its own group"
        );
        assert_eq!(groups[0], vec![0]);
        assert_eq!(groups[1], vec![1]);
    }
}
