//! Abstract spill segment store (local disk, S3, tiered).

use crate::error::Result;
use crate::scope::SpillScope;
use crate::segment::SpillSegment;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use std::path::PathBuf;

/// Storage backend for immutable spill segments.
///
/// Operator spill and durable exchange share this one abstraction. Implementations
/// must write via attempt-local `.partial` then publish atomically, never follow
/// symlinks out of the configured root, and never embed credentials in paths.
#[async_trait]
pub trait SegmentStore: Send + Sync {
    /// Ensure the scope directory/prefix exists with restrictive permissions.
    async fn ensure_scope(&self, scope: &SpillScope) -> Result<PathBuf>;

    /// Begin a new segment write. Returns a writer that buffers Arrow IPC.
    async fn create_writer(
        &self,
        scope: &SpillScope,
        sequence: u64,
        schema: SchemaRef,
    ) -> Result<Box<dyn SegmentWriter>>;

    /// Open a published segment for streaming reads.
    async fn open_reader(&self, segment: &SpillSegment) -> Result<Box<dyn SegmentReader>>;

    /// Delete all segments under a scope (completion or terminal failure).
    async fn delete_scope(&self, scope: &SpillScope) -> Result<()>;

    /// List orphaned scopes older than `max_age` for startup cleanup.
    async fn list_orphan_scopes(&self, max_age: std::time::Duration) -> Result<Vec<SpillScope>>;

    /// Bytes currently used under the store quota (best effort).
    async fn used_bytes(&self) -> Result<u64>;
}

/// Streaming writer for one spill segment.
#[async_trait]
pub trait SegmentWriter: Send {
    /// Append one record batch (IPC-encoded with per-batch CRC).
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;

    /// Atomically publish the segment (rename `.partial` → final).
    async fn finish(self: Box<Self>) -> Result<SpillSegment>;

    /// Abandon the partial write and delete staging data.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Streaming reader for one spill segment.
#[async_trait]
pub trait SegmentReader: Send {
    /// Schema of the segment.
    fn schema(&self) -> SchemaRef;

    /// Stream batches without materializing the whole segment.
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>>;
}
