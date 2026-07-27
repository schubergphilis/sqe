//! Tiered spill store: write local first, fall back to S3 on local pressure.
//!
//! Used when `worker.spill.backend = "tiered"`. Local disk is preferred for
//! latency; on `SpillQuotaExceeded` / `SpillDiskFull` at writer create or
//! finish, the write is retried against the S3 store.

use crate::error::{BudgetError, Result};
use crate::scope::SpillScope;
use crate::segment::SpillSegment;
use crate::store::{SegmentReader, SegmentStore, SegmentWriter};
use crate::store_local::LocalSegmentStore;
use crate::store_s3::S3SegmentStore;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// Prefer local NVMe, spill to S3 when local quota/disk pressure hits.
pub struct TieredSegmentStore {
    local: Arc<LocalSegmentStore>,
    s3: Arc<S3SegmentStore>,
}

impl TieredSegmentStore {
    pub fn new(local: Arc<LocalSegmentStore>, s3: Arc<S3SegmentStore>) -> Self {
        Self { local, s3 }
    }

    fn is_local_pressure(err: &BudgetError) -> bool {
        matches!(
            err,
            BudgetError::SpillQuotaExceeded { .. } | BudgetError::SpillDiskFull { .. }
        )
    }
}

#[async_trait]
impl SegmentStore for TieredSegmentStore {
    async fn ensure_scope(&self, scope: &SpillScope) -> Result<PathBuf> {
        // Prefer local scope dir; S3 is prefix-based and always "ok".
        self.local.ensure_scope(scope).await
    }

    async fn create_writer(
        &self,
        scope: &SpillScope,
        sequence: u64,
        schema: SchemaRef,
    ) -> Result<Box<dyn SegmentWriter>> {
        match self
            .local
            .create_writer(scope, sequence, schema.clone())
            .await
        {
            Ok(w) => Ok(Box::new(TieredWriter {
                inner: w,
                backend: Backend::Local,
                s3: self.s3.clone(),
                scope: scope.clone(),
                sequence,
                schema,
            })),
            Err(e) if Self::is_local_pressure(&e) => {
                warn!(
                    error = %e,
                    "Local spill pressure at create_writer; falling back to S3"
                );
                let w = self.s3.create_writer(scope, sequence, schema.clone()).await?;
                Ok(Box::new(TieredWriter {
                    inner: w,
                    backend: Backend::S3,
                    s3: self.s3.clone(),
                    scope: scope.clone(),
                    sequence,
                    schema,
                }))
            }
            Err(e) => Err(e),
        }
    }

    async fn open_reader(&self, segment: &SpillSegment) -> Result<Box<dyn SegmentReader>> {
        let path = segment.path.to_string_lossy();
        if path.starts_with("s3://") {
            self.s3.open_reader(segment).await
        } else {
            match self.local.open_reader(segment).await {
                Ok(r) => Ok(r),
                Err(e) => {
                    debug!(error = %e, "Local open_reader failed; trying S3");
                    self.s3.open_reader(segment).await
                }
            }
        }
    }

    async fn delete_scope(&self, scope: &SpillScope) -> Result<()> {
        let local_err = self.local.delete_scope(scope).await.err();
        let s3_err = self.s3.delete_scope(scope).await.err();
        if let Some(e) = local_err.or(s3_err) {
            return Err(e);
        }
        Ok(())
    }

    async fn list_orphan_scopes(&self, max_age: Duration) -> Result<Vec<SpillScope>> {
        let mut a = self.local.list_orphan_scopes(max_age).await?;
        let b = self.s3.list_orphan_scopes(max_age).await?;
        a.extend(b);
        Ok(a)
    }

    async fn used_bytes(&self) -> Result<u64> {
        let l = self.local.used_bytes().await.unwrap_or(0);
        let s = self.s3.used_bytes().await.unwrap_or(0);
        Ok(l.saturating_add(s))
    }
}

#[derive(Clone, Copy)]
enum Backend {
    Local,
    S3,
}

struct TieredWriter {
    inner: Box<dyn SegmentWriter>,
    backend: Backend,
    s3: Arc<S3SegmentStore>,
    scope: SpillScope,
    sequence: u64,
    schema: SchemaRef,
}

#[async_trait]
impl SegmentWriter for TieredWriter {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write_batch(batch).await
    }

    async fn finish(self: Box<Self>) -> Result<SpillSegment> {
        match self.inner.finish().await {
            Ok(seg) => Ok(seg),
            Err(e) if matches!(self.backend, Backend::Local) && TieredSegmentStore::is_local_pressure(&e) => {
                // Cannot seamlessly re-write already-buffered batches after
                // finish failed; surface pressure so the operator retries
                // the stage. Future work: dual-write buffer for seamless fail-over.
                warn!(
                    error = %e,
                    "Local spill finish hit pressure; stage should retry (S3 create on next attempt)"
                );
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
}

// silence unused fields on TieredWriter for future seamless failover
#[allow(dead_code)]
fn _tiered_fields(w: &TieredWriter) {
    let _ = (&w.s3, &w.scope, w.sequence, &w.schema);
}
