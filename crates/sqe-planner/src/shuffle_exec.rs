//! DataFusion `ExecutionPlan` nodes for distributed shuffle via Arrow Flight DoExchange.
//!
//! This module provides two plan nodes that together form the shuffle boundary
//! between stages in a distributed query:
//!
//! - [`ShuffleWriterExec`]: Reads from an input plan, partitions each batch (hash or
//!   range), and sends the partitioned data to remote executors via Flight DoExchange.
//!   Returns an empty stream (data was sent, not returned locally).
//!
//! - [`ShuffleReaderExec`]: Receives shuffled `RecordBatch`es from a bounded mpsc
//!   channel (populated by the DoExchange handler on the receiving side) and
//!   presents them as a `SendableRecordBatchStream` to downstream operators.
//!
//! These nodes are inserted into the physical plan by the stage planner
//! ([`super::stage_planner`]) at shuffle boundaries (joins, distributed sorts,
//! repartitioning aggregates).

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

// ─────────────────────────── Partitioner trait ───────────────────────────────

/// Describes how data should be partitioned for a shuffle exchange.
///
/// This is a planner-side descriptor that is serialized into the
/// `ExchangeDescriptor` sent as the first DoExchange message. The actual
/// partitioning logic lives in `sqe-worker::shuffle::{HashPartitioner, RangePartitioner}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ShufflePartitioning {
    /// Hash-partition on the given key columns into `num_partitions` buckets.
    Hash {
        key_columns: Vec<String>,
        num_partitions: usize,
    },
    /// Range-partition on a single key column using boundary values.
    /// Produces `boundaries.len() + 1` partitions.
    Range {
        key_column: String,
        /// Boundary values encoded as strings (decoded by the executor).
        boundaries: Vec<String>,
    },
}

impl fmt::Display for ShufflePartitioning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShufflePartitioning::Hash {
                key_columns,
                num_partitions,
            } => write!(
                f,
                "Hash(keys=[{}], partitions={})",
                key_columns.join(", "),
                num_partitions
            ),
            ShufflePartitioning::Range {
                key_column,
                boundaries,
            } => write!(
                f,
                "Range(key={}, boundaries={})",
                key_column,
                boundaries.len()
            ),
        }
    }
}

// ─────────────────────────── ShuffleWriterExec ───────────────────────────────

/// DataFusion `ExecutionPlan` that partitions its input and sends batches to
/// remote executors via Arrow Flight DoExchange.
///
/// This node sits at the output boundary of a query stage. When executed:
/// 1. Runs the input plan to produce `RecordBatch` streams.
/// 2. For each batch, partitions it according to the configured [`ShufflePartitioning`].
/// 3. Sends each partition's batches to the corresponding target executor.
/// 4. Returns an empty stream (all data was shipped out, not returned locally).
///
/// The `query_id` and `stage_id` are included in the `ExchangeDescriptor`
/// serialized as the first FlightData message, so the receiving executor can
/// route incoming data to the correct [`ShuffleReceiver`](sqe_worker::shuffle::ShuffleReceiver).
#[derive(Debug)]
pub struct ShuffleWriterExec {
    /// The source plan to read from.
    input: Arc<dyn ExecutionPlan>,
    /// How to partition the data.
    partitioning: ShufflePartitioning,
    /// Flight URLs of target executors (one per output partition).
    target_endpoints: Vec<String>,
    /// Query identifier (for exchange descriptor routing).
    query_id: String,
    /// Stage identifier (for exchange descriptor routing).
    stage_id: String,
    /// Cached plan properties.
    properties: Arc<PlanProperties>,
}

impl ShuffleWriterExec {
    /// Create a new `ShuffleWriterExec`.
    ///
    /// # Arguments
    /// - `input`: The child plan whose output will be partitioned and shipped.
    /// - `partitioning`: Hash or range partitioning configuration.
    /// - `target_endpoints`: Flight URLs of target executors (one per output partition).
    /// - `query_id`: Identifies the query for exchange descriptor routing.
    /// - `stage_id`: Identifies the stage for exchange descriptor routing.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: ShufflePartitioning,
        target_endpoints: Vec<String>,
        query_id: String,
        stage_id: String,
    ) -> Self {
        // The writer itself has as many output partitions as the input plan.
        // Each input partition independently partitions and sends its data.
        let input_partitions = input.properties().partitioning.partition_count();
        let schema = input.schema();

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            // Output partitioning matches the input — each input partition
            // independently writes to ALL target partitions.
            Partitioning::UnknownPartitioning(input_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Self {
            input,
            partitioning,
            target_endpoints,
            query_id,
            stage_id,
            properties,
        }
    }

    /// Returns the shuffle partitioning configuration.
    pub fn partitioning_config(&self) -> &ShufflePartitioning {
        &self.partitioning
    }

    /// Returns the target executor endpoints.
    pub fn target_endpoints(&self) -> &[String] {
        &self.target_endpoints
    }

    /// Returns the query ID.
    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    /// Returns the stage ID.
    pub fn stage_id(&self) -> &str {
        &self.stage_id
    }
}

impl DisplayAs for ShuffleWriterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ShuffleWriterExec: partitioning={}, targets={}, query_id={}, stage_id={}",
            self.partitioning,
            self.target_endpoints.len(),
            self.query_id,
            self.stage_id,
        )
    }
}

impl ExecutionPlan for ShuffleWriterExec {
    fn name(&self) -> &str {
        "ShuffleWriterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ShuffleWriterExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(ShuffleWriterExec::new(
            Arc::clone(&children[0]),
            self.partitioning.clone(),
            self.target_endpoints.clone(),
            self.query_id.clone(),
            self.stage_id.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_partitions = self.input.properties().partitioning.partition_count();
        if partition >= input_partitions {
            return Err(DataFusionError::Internal(format!(
                "ShuffleWriterExec partition {partition} out of range (max {input_partitions})"
            )));
        }

        debug!(
            query_id = %self.query_id,
            stage_id = %self.stage_id,
            partition = partition,
            partitioning = %self.partitioning,
            targets = self.target_endpoints.len(),
            "Executing ShuffleWriterExec"
        );

        // Execute the input plan for this partition
        let input_stream = self.input.execute(partition, context)?;
        let schema = self.input.schema();

        // In the full implementation, this would:
        // 1. Read batches from input_stream
        // 2. Partition each batch using HashPartitioner/RangePartitioner
        // 3. Send each partition's data to the corresponding target via Flight DoExchange
        // 4. Return an empty stream once all data has been sent
        //
        // For now, we return an empty stream. The actual Flight DoExchange
        // send logic will be wired in when the distributed execution pipeline
        // is integrated (Streams 7-8).

        // Drop the input stream reference — in real execution we'd consume it
        drop(input_stream);

        Ok(Box::pin(EmptyRecordBatchStream::new(schema)))
    }
}

// ─────────────────────────── ShuffleReaderExec ───────────────────────────────

/// DataFusion `ExecutionPlan` that reads shuffled `RecordBatch`es from a
/// bounded mpsc channel.
///
/// This node sits at the input boundary of a query stage. The receiving
/// executor's DoExchange handler deposits batches into the mpsc channel;
/// this plan node reads them out and presents them as a standard
/// `SendableRecordBatchStream` to downstream operators.
///
/// The channel receiver is passed in at construction time and can only be
/// taken once (via interior `Mutex`). This enforces that `execute()` is
/// called at most once per partition, which is the DataFusion contract.
#[derive(Debug)]
pub struct ShuffleReaderExec {
    /// Expected output schema.
    schema: SchemaRef,
    /// Number of partitions this reader has.
    num_partitions: usize,
    /// Per-partition mpsc receivers, wrapped in Mutex for take-once semantics.
    /// Index = partition number.
    receivers: Vec<Arc<Mutex<Option<mpsc::Receiver<RecordBatch>>>>>,
    /// Query identifier (for tracing/debugging).
    query_id: String,
    /// Stage identifier (for tracing/debugging).
    stage_id: String,
    /// Cached plan properties.
    properties: Arc<PlanProperties>,
}

impl ShuffleReaderExec {
    /// Create a new `ShuffleReaderExec` with multiple partition receivers.
    ///
    /// # Arguments
    /// - `schema`: The expected output schema of the shuffled data.
    /// - `receivers`: One mpsc receiver per partition. Each receiver will be
    ///   taken exactly once when `execute(partition)` is called.
    /// - `query_id`: Query identifier for tracing.
    /// - `stage_id`: Stage identifier for tracing.
    pub fn new(
        schema: SchemaRef,
        receivers: Vec<mpsc::Receiver<RecordBatch>>,
        query_id: String,
        stage_id: String,
    ) -> Self {
        let num_partitions = receivers.len();
        let wrapped: Vec<Arc<Mutex<Option<mpsc::Receiver<RecordBatch>>>>> = receivers
            .into_iter()
            .map(|rx| Arc::new(Mutex::new(Some(rx))))
            .collect();

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Self {
            schema,
            num_partitions,
            receivers: wrapped,
            query_id,
            stage_id,
            properties,
        }
    }

    /// Create a reader with a single partition.
    pub fn new_single(
        schema: SchemaRef,
        receiver: mpsc::Receiver<RecordBatch>,
        query_id: String,
        stage_id: String,
    ) -> Self {
        Self::new(schema, vec![receiver], query_id, stage_id)
    }

    /// Returns the query ID.
    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    /// Returns the stage ID.
    pub fn stage_id(&self) -> &str {
        &self.stage_id
    }
}

impl DisplayAs for ShuffleReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ShuffleReaderExec: partitions={}, query_id={}, stage_id={}",
            self.num_partitions, self.query_id, self.stage_id,
        )
    }
}

impl ExecutionPlan for ShuffleReaderExec {
    fn name(&self) -> &str {
        "ShuffleReaderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Leaf node — data arrives from the network, not from a child plan.
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "ShuffleReaderExec has no children".to_string(),
            ));
        }
        // Cannot clone receivers, so return self unchanged.
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.num_partitions {
            return Err(DataFusionError::Internal(format!(
                "ShuffleReaderExec partition {partition} out of range (max {})",
                self.num_partitions
            )));
        }

        debug!(
            query_id = %self.query_id,
            stage_id = %self.stage_id,
            partition = partition,
            "Executing ShuffleReaderExec"
        );

        // Take the receiver (can only be done once per partition).
        // We use try_lock here because execute() is called from a sync context.
        // The mutex is uncontended in practice because each partition is
        // executed at most once.
        let receiver = {
            let mut guard = self.receivers[partition]
                .try_lock()
                .map_err(|_| {
                    DataFusionError::Internal(format!(
                        "ShuffleReaderExec partition {partition} receiver lock contention"
                    ))
                })?;
            guard.take().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "ShuffleReaderExec partition {partition} receiver already taken"
                ))
            })?
        };

        let schema = self.schema.clone();
        Ok(Box::pin(ChannelRecordBatchStream::new(schema, receiver)))
    }
}

// ─────────────────────── EmptyRecordBatchStream ──────────────────────────────

/// A `RecordBatchStream` that immediately yields `None` (empty).
///
/// Used by `ShuffleWriterExec` since all output data is sent to remote
/// executors rather than returned locally.
pub(crate) struct EmptyRecordBatchStream {
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl Stream for EmptyRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl datafusion::physical_plan::RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ─────────────────────── ChannelRecordBatchStream ────────────────────────────

/// A `RecordBatchStream` backed by a `tokio::sync::mpsc::Receiver<RecordBatch>`.
///
/// Used by `ShuffleReaderExec` to present incoming shuffled data as a
/// standard DataFusion stream.
struct ChannelRecordBatchStream {
    schema: SchemaRef,
    receiver: mpsc::Receiver<RecordBatch>,
}

impl ChannelRecordBatchStream {
    fn new(schema: SchemaRef, receiver: mpsc::Receiver<RecordBatch>) -> Self {
        Self { schema, receiver }
    }
}

impl Stream for ChannelRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for ChannelRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ─────────────────────────────── Tests ───────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;

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

    fn make_memory_plan(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(LazyMemoryExec::try_new(schema, vec![]).unwrap())
    }

    /// A trivial `ExecutionPlan` with exactly 1 partition that returns an empty stream.
    /// Used for execute tests where `LazyMemoryExec::try_new(_, vec![])` gives 0 partitions.
    #[derive(Debug)]
    struct SinglePartitionPlan {
        schema: SchemaRef,
        properties: Arc<PlanProperties>,
    }

    impl SinglePartitionPlan {
        fn new(schema: SchemaRef) -> Self {
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self { schema, properties }
        }
    }

    impl DisplayAs for SinglePartitionPlan {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "SinglePartitionPlan")
        }
    }

    impl ExecutionPlan for SinglePartitionPlan {
        fn name(&self) -> &str {
            "SinglePartitionPlan"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DFResult<SendableRecordBatchStream> {
            Ok(Box::pin(EmptyRecordBatchStream::new(self.schema.clone())))
        }
    }

    fn make_single_partition_plan(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(SinglePartitionPlan::new(schema))
    }

    // ─── ShuffleWriterExec tests ───

    #[test]
    fn test_writer_schema_matches_input() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let writer = ShuffleWriterExec::new(
            input,
            ShufflePartitioning::Hash {
                key_columns: vec!["id".to_string()],
                num_partitions: 4,
            },
            vec![
                "grpc://host1:50051".to_string(),
                "grpc://host2:50051".to_string(),
                "grpc://host3:50051".to_string(),
                "grpc://host4:50051".to_string(),
            ],
            "q1".to_string(),
            "s1".to_string(),
        );

        assert_eq!(writer.schema(), schema);
        assert_eq!(writer.name(), "ShuffleWriterExec");
    }

    #[test]
    fn test_writer_children() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let writer = ShuffleWriterExec::new(
            input,
            ShufflePartitioning::Hash {
                key_columns: vec!["id".to_string()],
                num_partitions: 2,
            },
            vec![
                "grpc://host1:50051".to_string(),
                "grpc://host2:50051".to_string(),
            ],
            "q1".to_string(),
            "s1".to_string(),
        );

        assert_eq!(writer.children().len(), 1);
    }

    #[test]
    fn test_writer_properties() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let writer = ShuffleWriterExec::new(
            input,
            ShufflePartitioning::Hash {
                key_columns: vec!["id".to_string()],
                num_partitions: 4,
            },
            vec![
                "grpc://h1:50051".to_string(),
                "grpc://h2:50051".to_string(),
                "grpc://h3:50051".to_string(),
                "grpc://h4:50051".to_string(),
            ],
            "q1".to_string(),
            "s1".to_string(),
        );

        let props = writer.properties();
        assert_eq!(props.emission_type, EmissionType::Incremental);
        assert_eq!(props.boundedness, Boundedness::Bounded);
    }

    #[test]
    fn test_writer_with_new_children() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let writer = Arc::new(ShuffleWriterExec::new(
            input,
            ShufflePartitioning::Hash {
                key_columns: vec!["id".to_string()],
                num_partitions: 2,
            },
            vec![
                "grpc://h1:50051".to_string(),
                "grpc://h2:50051".to_string(),
            ],
            "q1".to_string(),
            "s1".to_string(),
        ));

        let new_input = make_memory_plan(schema);
        let new_writer = writer.with_new_children(vec![new_input]).unwrap();
        assert_eq!(new_writer.name(), "ShuffleWriterExec");
        assert_eq!(new_writer.children().len(), 1);
    }

    #[test]
    fn test_writer_with_new_children_wrong_count_errors() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let writer = Arc::new(ShuffleWriterExec::new(
            input,
            ShufflePartitioning::Hash {
                key_columns: vec!["id".to_string()],
                num_partitions: 2,
            },
            vec![
                "grpc://h1:50051".to_string(),
                "grpc://h2:50051".to_string(),
            ],
            "q1".to_string(),
            "s1".to_string(),
        ));

        // Too many children
        let result = writer.with_new_children(vec![
            make_memory_plan(schema.clone()),
            make_memory_plan(schema),
        ]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_writer_execute_returns_empty_stream() {
        let schema = test_schema();
        let input = make_single_partition_plan(schema);
        let writer = ShuffleWriterExec::new(
            input,
            ShufflePartitioning::Hash {
                key_columns: vec!["id".to_string()],
                num_partitions: 2,
            },
            vec![
                "grpc://h1:50051".to_string(),
                "grpc://h2:50051".to_string(),
            ],
            "q1".to_string(),
            "s1".to_string(),
        );

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let mut stream = writer.execute(0, task_ctx).unwrap();

        // Writer returns an empty stream (data is sent, not returned)
        let next = stream.next().await;
        assert!(next.is_none(), "ShuffleWriterExec should return empty stream");
    }

    #[test]
    fn test_writer_display() {
        let schema = test_schema();
        let input = make_memory_plan(schema);
        let writer = ShuffleWriterExec::new(
            input,
            ShufflePartitioning::Hash {
                key_columns: vec!["id".to_string()],
                num_partitions: 4,
            },
            vec![
                "grpc://h1:50051".to_string(),
                "grpc://h2:50051".to_string(),
                "grpc://h3:50051".to_string(),
                "grpc://h4:50051".to_string(),
            ],
            "q1".to_string(),
            "s1".to_string(),
        );

        // Verify DisplayAs and Debug don't panic
        let debug_str = format!("{:?}", writer);
        assert!(debug_str.contains("ShuffleWriterExec"));

        // Verify DisplayAs via the datafusion display mechanism
        let display_str = datafusion::physical_plan::displayable(&writer)
            .one_line()
            .to_string();
        assert!(display_str.contains("ShuffleWriterExec"));
        assert!(display_str.contains("partitions=4"));
    }

    // ─── ShuffleReaderExec tests ───

    #[tokio::test]
    async fn test_reader_single_partition_receives_batches() {
        let schema = test_schema();
        let (tx, rx) = mpsc::channel(16);
        let reader = ShuffleReaderExec::new_single(
            schema.clone(),
            rx,
            "q1".to_string(),
            "s1".to_string(),
        );

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        // Send some batches
        let batch1 = make_batch(vec![1, 2], vec!["a", "b"]);
        let batch2 = make_batch(vec![3, 4], vec!["c", "d"]);
        tx.send(batch1).await.unwrap();
        tx.send(batch2).await.unwrap();
        drop(tx); // Signal end of stream

        // Execute and collect
        let mut stream = reader.execute(0, task_ctx).unwrap();
        let mut batches = vec![];
        while let Some(result) = stream.next().await {
            batches.push(result.unwrap());
        }

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_reader_multi_partition() {
        let schema = test_schema();
        let (tx0, rx0) = mpsc::channel(16);
        let (tx1, rx1) = mpsc::channel(16);

        let reader = ShuffleReaderExec::new(
            schema.clone(),
            vec![rx0, rx1],
            "q1".to_string(),
            "s1".to_string(),
        );

        // Send to partition 0
        let batch0 = make_batch(vec![1], vec!["a"]);
        tx0.send(batch0).await.unwrap();
        drop(tx0);

        // Send to partition 1
        let batch1a = make_batch(vec![2], vec!["b"]);
        let batch1b = make_batch(vec![3], vec!["c"]);
        tx1.send(batch1a).await.unwrap();
        tx1.send(batch1b).await.unwrap();
        drop(tx1);

        let ctx = SessionContext::new();

        // Read partition 0
        let mut stream0 = reader.execute(0, ctx.task_ctx()).unwrap();
        let mut count0 = 0;
        while let Some(result) = stream0.next().await {
            count0 += result.unwrap().num_rows();
        }
        assert_eq!(count0, 1);

        // Read partition 1
        let mut stream1 = reader.execute(1, ctx.task_ctx()).unwrap();
        let mut count1 = 0;
        while let Some(result) = stream1.next().await {
            count1 += result.unwrap().num_rows();
        }
        assert_eq!(count1, 2);
    }

    #[tokio::test]
    async fn test_reader_partition_taken_once() {
        let schema = test_schema();
        let (_tx, rx) = mpsc::channel::<RecordBatch>(16);
        let reader = ShuffleReaderExec::new_single(
            schema,
            rx,
            "q1".to_string(),
            "s1".to_string(),
        );

        let ctx = SessionContext::new();

        // First execute succeeds
        let _stream = reader.execute(0, ctx.task_ctx()).unwrap();

        // Second execute on same partition fails
        let result = reader.execute(0, ctx.task_ctx());
        assert!(result.is_err(), "Second execute on same partition should fail");
    }

    #[test]
    fn test_reader_out_of_range_partition() {
        let schema = test_schema();
        let (_tx, rx) = mpsc::channel::<RecordBatch>(16);
        let reader = ShuffleReaderExec::new_single(
            schema,
            rx,
            "q1".to_string(),
            "s1".to_string(),
        );

        let ctx = SessionContext::new();
        let result = reader.execute(5, ctx.task_ctx());
        assert!(result.is_err(), "Out-of-range partition should fail");
    }

    #[test]
    fn test_reader_schema() {
        let schema = test_schema();
        let (_tx, rx) = mpsc::channel::<RecordBatch>(16);
        let reader = ShuffleReaderExec::new_single(
            schema.clone(),
            rx,
            "q1".to_string(),
            "s1".to_string(),
        );

        assert_eq!(reader.schema(), schema);
        assert_eq!(reader.name(), "ShuffleReaderExec");
    }

    #[test]
    fn test_reader_is_leaf_node() {
        let schema = test_schema();
        let (_tx, rx) = mpsc::channel::<RecordBatch>(16);
        let reader = ShuffleReaderExec::new_single(
            schema,
            rx,
            "q1".to_string(),
            "s1".to_string(),
        );

        assert!(reader.children().is_empty(), "ShuffleReaderExec is a leaf node");
    }

    #[test]
    fn test_reader_properties() {
        let schema = test_schema();
        let (_tx, rx) = mpsc::channel::<RecordBatch>(16);
        let reader = ShuffleReaderExec::new_single(
            schema,
            rx,
            "q1".to_string(),
            "s1".to_string(),
        );

        let props = reader.properties();
        assert_eq!(props.emission_type, EmissionType::Incremental);
        assert_eq!(props.boundedness, Boundedness::Bounded);
        assert_eq!(props.partitioning.partition_count(), 1);
    }

    #[test]
    fn test_reader_with_new_children_rejects_children() {
        let schema = test_schema();
        let (_tx, rx) = mpsc::channel::<RecordBatch>(16);
        let reader = Arc::new(ShuffleReaderExec::new_single(
            schema.clone(),
            rx,
            "q1".to_string(),
            "s1".to_string(),
        ));

        // Empty children is fine (returns self)
        let result = reader.clone().with_new_children(vec![]);
        assert!(result.is_ok());

        // Non-empty children is an error
        let result = reader.with_new_children(vec![make_memory_plan(schema)]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_reader_empty_channel() {
        let schema = test_schema();
        let (tx, rx) = mpsc::channel::<RecordBatch>(16);
        let reader = ShuffleReaderExec::new_single(
            schema,
            rx,
            "q1".to_string(),
            "s1".to_string(),
        );

        // Drop sender immediately — channel closed
        drop(tx);

        let ctx = SessionContext::new();
        let mut stream = reader.execute(0, ctx.task_ctx()).unwrap();

        let next = stream.next().await;
        assert!(next.is_none(), "Empty channel should yield None immediately");
    }

    // ─── ShufflePartitioning tests ───

    #[test]
    fn test_shuffle_partitioning_display() {
        let hash = ShufflePartitioning::Hash {
            key_columns: vec!["id".to_string(), "name".to_string()],
            num_partitions: 4,
        };
        assert_eq!(
            format!("{hash}"),
            "Hash(keys=[id, name], partitions=4)"
        );

        let range = ShufflePartitioning::Range {
            key_column: "ts".to_string(),
            boundaries: vec!["10".to_string(), "20".to_string()],
        };
        assert_eq!(
            format!("{range}"),
            "Range(key=ts, boundaries=2)"
        );
    }

    #[test]
    fn test_shuffle_partitioning_serde() {
        let hash = ShufflePartitioning::Hash {
            key_columns: vec!["id".to_string()],
            num_partitions: 8,
        };
        let json = serde_json::to_string(&hash).unwrap();
        let deserialized: ShufflePartitioning = serde_json::from_str(&json).unwrap();
        assert_eq!(hash, deserialized);

        let range = ShufflePartitioning::Range {
            key_column: "ts".to_string(),
            boundaries: vec!["100".to_string(), "200".to_string()],
        };
        let json = serde_json::to_string(&range).unwrap();
        let deserialized: ShufflePartitioning = serde_json::from_str(&json).unwrap();
        assert_eq!(range, deserialized);
    }
}
