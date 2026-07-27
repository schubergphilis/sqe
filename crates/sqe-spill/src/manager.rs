//! Spill manager: quota, cleanup, and store routing.

use crate::error::Result;
use crate::scope::SpillScope;
use crate::segment::SpillSegment;
use crate::store::{SegmentReader, SegmentStore, SegmentWriter};
use arrow_schema::SchemaRef;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// Process-wide spill coordinator for one worker.
pub struct SpillManager {
    store: Arc<dyn SegmentStore>,
    next_sequence: AtomicU64,
    orphan_age: Duration,
}

impl SpillManager {
    pub fn new(store: Arc<dyn SegmentStore>, orphan_age: Duration) -> Self {
        Self {
            store,
            next_sequence: AtomicU64::new(0),
            orphan_age,
        }
    }

    /// Allocate the next segment sequence number (process-local).
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence.fetch_add(1, Ordering::Relaxed)
    }

    /// Create a writer for a new segment under `scope`.
    pub async fn create_writer(
        &self,
        scope: &SpillScope,
        schema: SchemaRef,
    ) -> Result<Box<dyn SegmentWriter>> {
        let seq = self.next_sequence();
        self.store.create_writer(scope, seq, schema).await
    }

    pub async fn open_reader(&self, segment: &SpillSegment) -> Result<Box<dyn SegmentReader>> {
        self.store.open_reader(segment).await
    }

    pub async fn delete_scope(&self, scope: &SpillScope) -> Result<()> {
        self.store.delete_scope(scope).await
    }

    /// Startup orphan cleanup: delete abandoned scopes older than configured age.
    pub async fn cleanup_orphans_on_start(&self) -> Result<usize> {
        let orphans = self.store.list_orphan_scopes(self.orphan_age).await?;
        let mut n = 0usize;
        for scope in orphans {
            match self.store.delete_scope(&scope).await {
                Ok(()) => {
                    n += 1;
                    info!(%scope, "Cleaned orphan spill scope");
                }
                Err(e) => warn!(%scope, error = %e, "Failed to clean orphan spill scope"),
            }
        }
        Ok(n)
    }

    pub async fn used_bytes(&self) -> Result<u64> {
        self.store.used_bytes().await
    }

    pub fn store(&self) -> &Arc<dyn SegmentStore> {
        &self.store
    }
}

/// RAII guard that deletes a spill scope on drop (success, error, or panic).
pub struct SpillScopeGuard {
    manager: Arc<SpillManager>,
    scope: SpillScope,
    armed: bool,
}

impl SpillScopeGuard {
    pub fn new(manager: Arc<SpillManager>, scope: SpillScope) -> Self {
        Self {
            manager,
            scope,
            armed: true,
        }
    }

    /// Disarm so successful hand-off keeps segments (caller owns cleanup).
    pub fn disarm(mut self) {
        self.armed = false;
    }

    pub fn scope(&self) -> &SpillScope {
        &self.scope
    }
}

impl Drop for SpillScopeGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let manager = self.manager.clone();
        let scope = self.scope.clone();
        // Best-effort async cleanup on a detached task.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(e) = manager.delete_scope(&scope).await {
                    warn!(%scope, error = %e, "SpillScopeGuard cleanup failed");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_local::LocalSegmentStore;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;

    #[tokio::test]
    async fn manager_roundtrip_and_guard_cleanup() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            LocalSegmentStore::open(tmp.path(), 1 << 30, 0, 2, 2).unwrap(),
        );
        let manager = Arc::new(SpillManager::new(store, Duration::from_secs(0)));
        let scope = SpillScope::new("q-guard", "s", "agg", 0, 0);
        let guard = SpillScopeGuard::new(manager.clone(), scope.clone());

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let mut w = manager.create_writer(&scope, schema).await.unwrap();
        w.write_batch(&batch).await.unwrap();
        let seg = w.finish().await.unwrap();
        assert!(seg.path.exists());

        let r = manager.open_reader(&seg).await.unwrap();
        let mut s = r.into_stream();
        let got = s.next().await.unwrap().unwrap();
        assert_eq!(got.num_rows(), 3);

        drop(guard);
        // Allow cleanup task to run.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !seg.path.exists(),
            "guard should delete scope on drop"
        );
    }
}
