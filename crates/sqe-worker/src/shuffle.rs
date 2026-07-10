//! Shuffle infrastructure for distributed computation via Arrow Flight DoExchange.
//!
//! This module provides:
//! - [`ExchangeDescriptor`]: Describes the type of data exchange (hash or range partition).
//! - [`ShuffleReceiver`]: Per-stage partition buffers backed by bounded mpsc channels.
//! - [`ShuffleManager`]: Registry of active shuffle receivers across queries/stages.
//! - [`HashPartitioner`]: Splits a RecordBatch by hashing key columns modulo partition count.
//! - [`RangePartitioner`]: Splits a RecordBatch using sort-key boundaries for range partitioning.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use datafusion::common::hash_utils::create_hashes;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

// ───────────────────────────── ExchangeDescriptor ─────────────────────────────

/// Describes the type of data exchange for a DoExchange call.
///
/// Serialized as JSON in the first FlightData message's descriptor `cmd` field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExchangeDescriptor {
    /// Receive hash-partitioned data for a join or aggregate.
    HashPartition {
        query_id: String,
        stage_id: String,
        partition_id: u32,
    },
    /// Receive range-partitioned data for a distributed sort.
    RangePartition {
        query_id: String,
        stage_id: String,
        range_bounds: Vec<String>,
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
                query_id,
                stage_id,
                ..
            } => (query_id.clone(), stage_id.clone()),
            ExchangeDescriptor::RangePartition {
                query_id,
                stage_id,
                ..
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
}

// ───────────────────────────── ShuffleReceiver ────────────────────────────────

/// Default bounded channel capacity per partition.
const DEFAULT_CHANNEL_CAPACITY: usize = 64;

/// Holds per-partition mpsc channels for receiving shuffled RecordBatches.
///
/// The sender side is used by the DoExchange handler when data arrives.
/// The receiver side is consumed by the downstream operator (e.g., ShuffleReaderExec).
pub struct ShuffleReceiver {
    /// Per-partition senders — DoExchange handler writes here.
    senders: HashMap<u32, mpsc::Sender<RecordBatch>>,
    /// Per-partition receivers — consuming operators read from here.
    receivers: Mutex<HashMap<u32, mpsc::Receiver<RecordBatch>>>,
    /// Schema of the data being shuffled.
    schema: SchemaRef,
}

impl ShuffleReceiver {
    /// Create a new ShuffleReceiver with the given number of partitions and schema.
    ///
    /// Each partition gets a bounded mpsc channel with `capacity` buffer slots.
    pub fn new(num_partitions: u32, schema: SchemaRef, capacity: usize) -> Self {
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
        }
    }

    /// Create a ShuffleReceiver with default channel capacity.
    pub fn with_defaults(num_partitions: u32, schema: SchemaRef) -> Self {
        Self::new(num_partitions, schema, DEFAULT_CHANNEL_CAPACITY)
    }

    /// Get a sender for a given partition. Used by the DoExchange handler.
    pub fn sender(&self, partition_id: u32) -> Option<&mpsc::Sender<RecordBatch>> {
        self.senders.get(&partition_id)
    }

    /// Take the receiver for a given partition. This can only be called once per partition.
    ///
    /// Returns `None` if the receiver was already taken or the partition doesn't exist.
    pub async fn take_receiver(&self, partition_id: u32) -> Option<mpsc::Receiver<RecordBatch>> {
        self.receivers.lock().await.remove(&partition_id)
    }

    /// Get the schema of the shuffled data.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
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
}

impl ShuffleManager {
    pub fn new() -> Self {
        Self {
            receivers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a new ShuffleReceiver for a (query_id, stage_id).
    pub async fn register(
        &self,
        query_id: &str,
        stage_id: &str,
        receiver: Arc<ShuffleReceiver>,
    ) {
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

// ───────────────────────────── HashPartitioner ────────────────────────────────

/// Splits a RecordBatch by hashing key columns modulo the number of partitions.
///
/// Uses DataFusion's `create_hashes()` for consistent hashing, then
/// `arrow::compute::take()` to extract rows for each partition.
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

    /// Partition a RecordBatch by hashing the key columns.
    ///
    /// Returns a Vec of (partition_id, RecordBatch) pairs. Empty partitions
    /// are omitted from the result.
    pub fn partition(&self, batch: &RecordBatch) -> anyhow::Result<Vec<(u32, RecordBatch)>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        if self.num_partitions == 1 {
            return Ok(vec![(0, batch.clone())]);
        }

        // Extract key column arrays as ArrayRef
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

        // Compute hashes for all rows using DataFusion's hasher.
        // DF 54 switched the hash backend from ahash to foldhash; `RandomState` is now
        // an alias for `foldhash::fast::FixedState`. We use a fixed (seed-0) state so the
        // shuffle is deterministic across all nodes, matching DF's own REPARTITION_RANDOM_STATE.
        let mut hashes = vec![0u64; batch.num_rows()];
        create_hashes(
            &key_arrays,
            &datafusion::common::hash_utils::RandomState::default(),
            &mut hashes,
        )?;

        // Assign rows to partitions: hash % num_partitions
        let num_partitions = self.num_partitions as u64;
        let partition_assignments: Vec<u32> = hashes
            .iter()
            .map(|h| (h % num_partitions) as u32)
            .collect();

        // Build per-partition row indices
        let mut partition_indices: Vec<Vec<u32>> = vec![Vec::new(); self.num_partitions];
        for (row_idx, &partition_id) in partition_assignments.iter().enumerate() {
            partition_indices[partition_id as usize].push(row_idx as u32);
        }

        // Split the batch by partition using take()
        let mut result = Vec::new();
        for (partition_id, indices) in partition_indices.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let indices_array = UInt32Array::from(indices);

            // Use arrow::compute::take on each column
            let taken_columns: Vec<_> = batch
                .columns()
                .iter()
                .map(|col| {
                    arrow::compute::take(col.as_ref(), &indices_array, None)
                        .map_err(|e| anyhow::anyhow!("take failed: {e}"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;

            let partition_batch = RecordBatch::try_new(batch.schema(), taken_columns)?;
            result.push((partition_id as u32, partition_batch));
        }

        Ok(result)
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

    /// Partition a RecordBatch by range on the key column.
    ///
    /// Returns a Vec of (partition_id, RecordBatch) pairs. Empty partitions
    /// are omitted.
    pub fn partition(&self, batch: &RecordBatch) -> anyhow::Result<Vec<(u32, RecordBatch)>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        if self.num_partitions == 1 {
            return Ok(vec![(0, batch.clone())]);
        }

        // Extract the key column
        let key_col = batch
            .column_by_name(&self.key_column)
            .ok_or_else(|| {
                anyhow::anyhow!("Key column '{}' not found in batch", self.key_column)
            })?;

        // Downcast to Int64Array for binary search against i64 boundaries.
        // For a production system, we'd support more types; for now Int64 covers
        // the most common sort keys (timestamps, IDs, etc.).
        let key_array = key_col
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "RangePartitioner currently supports Int64 key columns, got {:?}",
                    key_col.data_type()
                )
            })?;

        // Assign each row to a partition via binary search
        let mut partition_indices: Vec<Vec<u32>> = vec![Vec::new(); self.num_partitions];
        for row_idx in 0..batch.num_rows() {
            let value = key_array.value(row_idx);
            // Binary search: find first boundary > value
            let partition_id = self
                .boundaries
                .partition_point(|b| *b <= value);
            partition_indices[partition_id].push(row_idx as u32);
        }

        // Split the batch by partition
        let mut result = Vec::new();
        for (partition_id, indices) in partition_indices.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let indices_array = UInt32Array::from(indices);
            let taken_columns: Vec<_> = batch
                .columns()
                .iter()
                .map(|col| {
                    arrow::compute::take(col.as_ref(), &indices_array, None)
                        .map_err(|e| anyhow::anyhow!("take failed: {e}"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;

            let partition_batch = RecordBatch::try_new(batch.schema(), taken_columns)?;
            result.push((partition_id as u32, partition_batch));
        }

        Ok(result)
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
        };
        let bytes = desc.to_bytes().unwrap();
        let decoded = ExchangeDescriptor::from_bytes(&bytes).unwrap();
        assert_eq!(desc, decoded);
    }

    #[test]
    fn test_exchange_descriptor_roundtrip_range() {
        let desc = ExchangeDescriptor::RangePartition {
            query_id: "q2".to_string(),
            stage_id: "s2".to_string(),
            range_bounds: vec!["10".to_string(), "20".to_string()],
        };
        let bytes = desc.to_bytes().unwrap();
        let decoded = ExchangeDescriptor::from_bytes(&bytes).unwrap();
        assert_eq!(desc, decoded);
    }

    #[test]
    fn test_exchange_descriptor_stage_key() {
        let desc = ExchangeDescriptor::HashPartition {
            query_id: "q1".to_string(),
            stage_id: "s1".to_string(),
            partition_id: 0,
        };
        assert_eq!(desc.stage_key(), ("q1".to_string(), "s1".to_string()));
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
            (0..100).map(|i| format!("name_{i}")).collect::<Vec<_>>().iter().map(|s| s.as_str()).collect(),
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
        let batch = make_range_batch(
            vec![5, 10, 15, 20, 25],
            vec!["a", "b", "c", "d", "e"],
        );

        let partitioner = RangePartitioner::new("id".to_string(), vec![10, 20]);
        let result = partitioner.partition(&batch).unwrap();

        // Verify partitions
        let mut partition_map: HashMap<u32, Vec<i64>> = HashMap::new();
        for (pid, b) in &result {
            let ids = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
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
        let batch = make_batch(
            vec![1, 1, 2, 2],
            vec!["a", "b", "a", "b"],
        );
        let partitioner = HashPartitioner::new(
            vec!["id".to_string(), "name".to_string()],
            4,
        );
        let result = partitioner.partition(&batch).unwrap();

        let total_rows: usize = result.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "All rows accounted for with multi-column key");

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
}
