//! Durable exchange attempt manifests (Phase 8).
//!
//! After every spill segment for a shuffle/exchange task attempt is durable,
//! the producer publishes an [`AttemptManifest`]. The coordinator (or
//! receiving worker) commits one **winning** attempt per task; late data from
//! losing attempts is rejected. On retry, completed upstream segments listed
//! in a committed manifest are reused rather than re-shuffled.
//!
//! Storage is intentionally backend-agnostic: manifests are JSON files under
//! a scope directory of any [`crate::SegmentStore`]. Local and (future) S3
//! backends share this format.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::error::{BudgetError, Result};
use crate::scope::SpillScope;
use crate::segment::SpillSegment;

/// Manifest format version.
pub const ATTEMPT_MANIFEST_VERSION: u32 = 1;

/// Completion state of one producer task attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    /// Segments still being written.
    Open,
    /// All segments published; waiting for winner commit.
    Published,
    /// Selected as the winning attempt for this task.
    Committed,
    /// Superseded by a higher attempt or cancelled.
    Lost,
    /// Failed mid-write; segments must not be reused.
    Failed,
}

/// One durable exchange task attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttemptManifest {
    pub version: u32,
    pub query_id: String,
    pub stage_id: String,
    pub task_id: String,
    pub partition_id: u32,
    pub attempt_id: u32,
    pub producer_task_id: String,
    pub state: AttemptState,
    pub rows: u64,
    pub batches: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    /// Relative segment paths (or absolute for local tests).
    pub segments: Vec<String>,
    /// CRC32 of concatenated segment checksums (best-effort integrity).
    pub checksum: u32,
}

impl AttemptManifest {
    pub fn new(
        query_id: impl Into<String>,
        stage_id: impl Into<String>,
        task_id: impl Into<String>,
        partition_id: u32,
        attempt_id: u32,
    ) -> Self {
        Self {
            version: ATTEMPT_MANIFEST_VERSION,
            query_id: query_id.into(),
            stage_id: stage_id.into(),
            task_id: task_id.into(),
            partition_id,
            attempt_id,
            producer_task_id: String::new(),
            state: AttemptState::Open,
            rows: 0,
            batches: 0,
            logical_bytes: 0,
            physical_bytes: 0,
            segments: Vec::new(),
            checksum: 0,
        }
    }

    pub fn with_producer(mut self, producer_task_id: impl Into<String>) -> Self {
        self.producer_task_id = producer_task_id.into();
        self
    }

    pub fn add_segment(&mut self, segment: &SpillSegment) {
        self.rows += segment.row_count;
        self.batches += 1;
        self.logical_bytes += segment.logical_bytes;
        self.physical_bytes += segment.physical_bytes;
        self.segments
            .push(segment.path.display().to_string());
        self.checksum = self.checksum.wrapping_add(segment.checksum);
    }

    pub fn mark_published(&mut self) {
        self.state = AttemptState::Published;
    }

    pub fn mark_committed(&mut self) {
        self.state = AttemptState::Committed;
    }

    pub fn mark_lost(&mut self) {
        self.state = AttemptState::Lost;
    }

    pub fn mark_failed(&mut self) {
        self.state = AttemptState::Failed;
    }

    pub fn is_reusable(&self) -> bool {
        matches!(self.state, AttemptState::Committed | AttemptState::Published)
            && !self.segments.is_empty()
    }

    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self).map_err(|e| BudgetError::Config(format!("manifest json: {e}")))
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| BudgetError::Config(format!("manifest parse: {e}")))
    }

    /// File name for this attempt under a scope directory.
    pub fn file_name(&self) -> String {
        format!(
            "manifest-p{}-a{}-task{}.json",
            self.partition_id, self.attempt_id, sanitize(&self.task_id)
        )
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Key for the winning attempt of one exchange task.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskKey {
    pub query_id: String,
    pub stage_id: String,
    pub task_id: String,
    pub partition_id: u32,
}

impl TaskKey {
    pub fn new(
        query_id: impl Into<String>,
        stage_id: impl Into<String>,
        task_id: impl Into<String>,
        partition_id: u32,
    ) -> Self {
        Self {
            query_id: query_id.into(),
            stage_id: stage_id.into(),
            task_id: task_id.into(),
            partition_id,
        }
    }
}

/// In-memory durable exchange coordinator (process-local).
///
/// Production will persist manifests via SegmentStore; this registry is the
/// authority for winner selection and late-data rejection on a single worker
/// or test harness.
#[derive(Default)]
pub struct ExchangeAttemptStore {
    /// Published/committed manifests by task key + attempt.
    manifests: Mutex<HashMap<(TaskKey, u32), AttemptManifest>>,
    /// Winning attempt id per task.
    winners: Mutex<HashMap<TaskKey, u32>>,
}

impl ExchangeAttemptStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish a completed attempt manifest. Does not yet commit a winner.
    pub fn publish(&self, mut manifest: AttemptManifest) -> Result<()> {
        manifest.mark_published();
        let key = TaskKey::new(
            &manifest.query_id,
            &manifest.stage_id,
            &manifest.task_id,
            manifest.partition_id,
        );
        let attempt = manifest.attempt_id;
        // Reject if a higher winner already committed.
        if let Some(w) = self.winners.lock().unwrap_or_else(|p| p.into_inner()).get(&key) {
            if attempt < *w {
                return Err(BudgetError::Config(format!(
                    "rejecting late attempt {attempt} for {:?}: winner is {w}",
                    key
                )));
            }
        }
        self.manifests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert((key, attempt), manifest);
        Ok(())
    }

    /// Commit `attempt_id` as the winner for the task. Higher attempts may
    /// still supersede later; lower attempts are marked lost.
    pub fn commit_winner(&self, key: &TaskKey, attempt_id: u32) -> Result<AttemptManifest> {
        let mut winners = self.winners.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(existing) = winners.get(key) {
            if *existing > attempt_id {
                return Err(BudgetError::Config(format!(
                    "cannot commit attempt {attempt_id}: higher winner {existing} exists"
                )));
            }
        }
        winners.insert(key.clone(), attempt_id);
        drop(winners);

        let mut manifests = self.manifests.lock().unwrap_or_else(|p| p.into_inner());
        let m = manifests
            .get_mut(&(key.clone(), attempt_id))
            .ok_or_else(|| {
                BudgetError::Config(format!(
                    "no published manifest for attempt {attempt_id} of {:?}",
                    key
                ))
            })?;
        m.mark_committed();
        let committed = m.clone();

        // Mark older attempts lost.
        for ((k, a), man) in manifests.iter_mut() {
            if k == key && *a < attempt_id && man.state != AttemptState::Failed {
                man.mark_lost();
            }
        }
        Ok(committed)
    }

    /// True if this attempt is still admissible (no higher winner).
    pub fn admit(&self, key: &TaskKey, attempt_id: u32) -> bool {
        !matches!(
            self.winners.lock().unwrap_or_else(|p| p.into_inner()).get(key),
            Some(w) if attempt_id < *w
        )
    }

    /// Fetch a reusable committed/published manifest for retry.
    pub fn reusable(&self, key: &TaskKey) -> Option<AttemptManifest> {
        let winners = self.winners.lock().unwrap_or_else(|p| p.into_inner());
        let attempt = *winners.get(key)?;
        drop(winners);
        let manifests = self.manifests.lock().unwrap_or_else(|p| p.into_inner());
        manifests
            .get(&(key.clone(), attempt))
            .filter(|m| m.is_reusable())
            .cloned()
    }

    pub fn winner(&self, key: &TaskKey) -> Option<u32> {
        self.winners
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
            .copied()
    }

    pub fn get(&self, key: &TaskKey, attempt_id: u32) -> Option<AttemptManifest> {
        self.manifests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(key.clone(), attempt_id))
            .cloned()
    }
}

pub type SharedExchangeAttemptStore = Arc<ExchangeAttemptStore>;

/// Write a manifest atomically to a directory (`.partial` then rename).
pub fn write_manifest_atomic(dir: &Path, manifest: &AttemptManifest) -> Result<PathBuf> {
    std::fs::create_dir_all(dir).map_err(|e| BudgetError::SpillIo {
        path: dir.display().to_string(),
        source: e,
    })?;
    let final_path = dir.join(manifest.file_name());
    let partial = final_path.with_extension("json.partial");
    let bytes = manifest.to_json()?;
    std::fs::write(&partial, &bytes).map_err(|e| BudgetError::SpillIo {
        path: partial.display().to_string(),
        source: e,
    })?;
    std::fs::rename(&partial, &final_path).map_err(|e| BudgetError::SpillIo {
        path: final_path.display().to_string(),
        source: e,
    })?;
    Ok(final_path)
}

/// Read a manifest from disk.
pub fn read_manifest(path: &Path) -> Result<AttemptManifest> {
    let bytes = std::fs::read(path).map_err(|e| BudgetError::SpillIo {
        path: path.display().to_string(),
        source: e,
    })?;
    AttemptManifest::from_json(&bytes)
}

/// Derive a spill scope for durable exchange segments.
pub fn exchange_scope(
    query_id: &str,
    stage_id: &str,
    partition_id: u32,
    attempt_id: u32,
) -> SpillScope {
    SpillScope::new(query_id, stage_id, "exchange", partition_id, attempt_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::{SEGMENT_FORMAT_VERSION, SEGMENT_MAGIC};

    fn dummy_segment(path: &str, rows: u64, checksum: u32) -> SpillSegment {
        SpillSegment {
            scope: SpillScope::new("q", "s", "exchange", 0, 0),
            sequence: 0,
            path: PathBuf::from(path),
            schema_fingerprint: 0,
            row_count: rows,
            logical_bytes: rows * 8,
            physical_bytes: rows * 8,
            checksum,
            format_version: SEGMENT_FORMAT_VERSION,
        }
    }

    #[test]
    fn publish_commit_winner_rejects_late() {
        let store = ExchangeAttemptStore::new();
        let key = TaskKey::new("q1", "stage0", "task-a", 0);

        let mut m0 = AttemptManifest::new("q1", "stage0", "task-a", 0, 0);
        m0.add_segment(&dummy_segment("/tmp/seg0", 10, 1));
        store.publish(m0).unwrap();
        store.commit_winner(&key, 0).unwrap();
        assert_eq!(store.winner(&key), Some(0));
        assert!(store.reusable(&key).unwrap().is_reusable());

        // Late lower attempt rejected on publish after higher winner? We have winner 0.
        // Higher attempt can still publish and supersede.
        let mut m1 = AttemptManifest::new("q1", "stage0", "task-a", 0, 1);
        m1.add_segment(&dummy_segment("/tmp/seg1", 10, 2));
        store.publish(m1).unwrap();
        store.commit_winner(&key, 1).unwrap();
        assert_eq!(store.winner(&key), Some(1));

        // Attempt 0 is no longer admissible.
        assert!(!store.admit(&key, 0));
        assert!(store.admit(&key, 1));
        assert!(store.admit(&key, 2));

        // Publishing attempt 0 after winner 1 fails.
        let mut late = AttemptManifest::new("q1", "stage0", "task-a", 0, 0);
        late.add_segment(&dummy_segment("/tmp/late", 1, 3));
        assert!(store.publish(late).is_err());
    }

    #[test]
    fn atomic_manifest_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let mut m = AttemptManifest::new("q", "s", "t1", 3, 2).with_producer("prod-x");
        m.add_segment(&dummy_segment("/data/seg-1", 100, 42));
        m.mark_published();
        let path = write_manifest_atomic(tmp.path(), &m).unwrap();
        assert!(path.exists());
        let loaded = read_manifest(&path).unwrap();
        assert_eq!(loaded.attempt_id, 2);
        assert_eq!(loaded.rows, 100);
        assert_eq!(loaded.producer_task_id, "prod-x");
        assert_eq!(loaded.state, AttemptState::Published);
        // Magic constant sanity (not embedded in manifest).
        assert_eq!(SEGMENT_MAGIC.len(), 8);
    }

    #[test]
    fn lost_attempts_marked_on_commit() {
        let store = ExchangeAttemptStore::new();
        let key = TaskKey::new("q", "s", "t", 0);
        for a in 0..3u32 {
            let mut m = AttemptManifest::new("q", "s", "t", 0, a);
            m.add_segment(&dummy_segment(&format!("/s{a}"), 1, a));
            store.publish(m).unwrap();
        }
        store.commit_winner(&key, 2).unwrap();
        assert_eq!(store.get(&key, 0).unwrap().state, AttemptState::Lost);
        assert_eq!(store.get(&key, 1).unwrap().state, AttemptState::Lost);
        assert_eq!(store.get(&key, 2).unwrap().state, AttemptState::Committed);
    }
}
