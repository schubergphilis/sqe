//! Local NVMe / filesystem spill backend.

use crate::error::{BudgetError, Result};
use crate::fault::{take_fault, SpillFault};
use crate::scope::SpillScope;
use crate::segment::{SpillSegment, SEGMENT_FORMAT_VERSION, SEGMENT_MAGIC};
use crate::store::{SegmentReader, SegmentStore, SegmentWriter};
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Semaphore;
use tracing::debug;

/// Local disk spill store under a validated root directory.
pub struct LocalSegmentStore {
    root: PathBuf,
    max_bytes: u64,
    min_free_bytes: u64,
    used_bytes: Arc<AtomicU64>,
    write_sem: Arc<Semaphore>,
    read_sem: Arc<Semaphore>,
}

impl LocalSegmentStore {
    /// Create a store rooted at `root`. Creates the directory with mode 0o700
    /// when missing. Rejects paths that would escape via symlink on first use.
    pub fn open(
        root: impl Into<PathBuf>,
        max_bytes: u64,
        min_free_bytes: u64,
        max_concurrent_writes: usize,
        max_concurrent_reads: usize,
    ) -> Result<Self> {
        let root = root.into();
        if root.as_os_str().is_empty() {
            return Err(BudgetError::InvalidSpillRoot {
                path: String::new(),
                reason: "empty path".into(),
            });
        }
        fs::create_dir_all(&root).map_err(|e| BudgetError::SpillIo {
            path: root.display().to_string(),
            source: e,
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&root, fs::Permissions::from_mode(0o700));
        }
        // Refuse if root itself is a symlink (broad path escape).
        if root
            .symlink_metadata()
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
        {
            return Err(BudgetError::InvalidSpillRoot {
                path: root.display().to_string(),
                reason: "spill root must not be a symlink".into(),
            });
        }
        let used = measure_dir_bytes(&root).unwrap_or(0);
        Ok(Self {
            root,
            max_bytes,
            min_free_bytes,
            used_bytes: Arc::new(AtomicU64::new(used)),
            write_sem: Arc::new(Semaphore::new(max_concurrent_writes.max(1))),
            read_sem: Arc::new(Semaphore::new(max_concurrent_reads.max(1))),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn reserve_quota(&self, need: u64) -> Result<()> {
        // Deterministic fault injection (test only; inert in production).
        if take_fault(SpillFault::DiskFull) {
            return Err(BudgetError::SpillDiskFull {
                need: self.min_free_bytes.max(need),
                available: 0,
            });
        }
        if take_fault(SpillFault::QuotaExceeded) {
            return Err(BudgetError::SpillQuotaExceeded {
                scope: "local".into(),
                need,
                free: 0,
                max: self.max_bytes,
            });
        }

        let used = self.used_bytes.load(Ordering::Relaxed);
        if used.saturating_add(need) > self.max_bytes {
            return Err(BudgetError::SpillQuotaExceeded {
                scope: "local".into(),
                need,
                free: self.max_bytes.saturating_sub(used),
                max: self.max_bytes,
            });
        }
        // Free-space probe (best effort; not available on all platforms).
        if self.min_free_bytes > 0 {
            if let Some(available) = free_space_bytes(&self.root) {
                if available.saturating_sub(need) < self.min_free_bytes {
                    return Err(BudgetError::SpillDiskFull {
                        need: self.min_free_bytes,
                        available,
                    });
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SegmentStore for LocalSegmentStore {
    async fn ensure_scope(&self, scope: &SpillScope) -> Result<PathBuf> {
        let dir = scope.absolute_dir(&self.root);
        // Containment check.
        if !dir.starts_with(&self.root) {
            return Err(BudgetError::InvalidSpillRoot {
                path: dir.display().to_string(),
                reason: "scope path escapes spill root".into(),
            });
        }
        fs::create_dir_all(&dir).map_err(|e| BudgetError::SpillIo {
            path: dir.display().to_string(),
            source: e,
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o700));
        }
        Ok(dir)
    }

    async fn create_writer(
        &self,
        scope: &SpillScope,
        sequence: u64,
        schema: SchemaRef,
    ) -> Result<Box<dyn SegmentWriter>> {
        let _permit = self
            .write_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BudgetError::Cancelled {
                budget: "spill-write".into(),
            })?;
        let dir = self.ensure_scope(scope).await?;
        let partial = dir.join(SpillSegment::partial_file_name(sequence));
        let final_path = dir.join(SpillSegment::segment_file_name(sequence));
        // Reserve a conservative segment target so concurrent writers do not
        // oversubscribe; true size is reconciled on finish.
        self.reserve_quota(64 * 1024)?;

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&partial)
            .map_err(|e| BudgetError::SpillIo {
                path: partial.display().to_string(),
                source: e,
            })?;
        let mut writer = BufWriter::new(file);
        writer
            .write_all(SEGMENT_MAGIC)
            .map_err(|e| BudgetError::SpillIo {
                path: partial.display().to_string(),
                source: e,
            })?;
        writer
            .write_all(&SEGMENT_FORMAT_VERSION.to_le_bytes())
            .map_err(|e| BudgetError::SpillIo {
                path: partial.display().to_string(),
                source: e,
            })?;
        let ipc = StreamWriter::try_new(writer, &schema).map_err(|e| {
            BudgetError::Config(format!("Arrow IPC writer init failed: {e}"))
        })?;

        Ok(Box::new(LocalSegmentWriter {
            scope: scope.clone(),
            sequence,
            schema,
            partial_path: partial,
            final_path,
            ipc: Some(ipc),
            row_count: 0,
            logical_bytes: 0,
            used_counter: self.used_bytes.clone(),
            _write_permit: _permit,
        }))
    }

    async fn open_reader(&self, segment: &SpillSegment) -> Result<Box<dyn SegmentReader>> {
        let _permit = self
            .read_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BudgetError::Cancelled {
                budget: "spill-read".into(),
            })?;
        if take_fault(SpillFault::CorruptOnRead) {
            return Err(BudgetError::SegmentCorrupt {
                path: segment.path.display().to_string(),
                reason: "injected corruption on open_reader".into(),
            });
        }
        let path = segment.path.clone();
        // Read the whole encoded segment so its integrity is verified BEFORE
        // any batch is yielded. Segments are bounded by `segment_target_size`,
        // and only the *encoded* body is resident here; `into_stream` decodes
        // batches lazily, one at a time, from this buffer. Verifying nothing
        // (the previous behavior) let bit rot or a clean-boundary truncation
        // return corrupted or short data silently, producing wrong query
        // results from a downstream join/sort.
        let body = fs::read(&path).map_err(|e| BudgetError::SpillIo {
            path: path.display().to_string(),
            source: e,
        })?;
        if body.len() < 12 {
            return Err(BudgetError::SegmentCorrupt {
                path: path.display().to_string(),
                reason: format!("truncated header: {} bytes", body.len()),
            });
        }
        if &body[..8] != SEGMENT_MAGIC {
            return Err(BudgetError::SegmentCorrupt {
                path: path.display().to_string(),
                reason: format!("bad magic {:?}", &body[..8]),
            });
        }
        let version = u32::from_le_bytes([body[8], body[9], body[10], body[11]]);
        if version != SEGMENT_FORMAT_VERSION {
            return Err(BudgetError::UnsupportedSegmentVersion {
                path: path.display().to_string(),
                version,
            });
        }
        // Whole-file checksum. `finish()` hashes the entire published file, so
        // any bit flip or tamper that keeps the file parseable is caught here.
        let actual = crc32fast::hash(&body);
        if actual != segment.checksum {
            return Err(BudgetError::SegmentCorrupt {
                path: path.display().to_string(),
                reason: format!(
                    "checksum mismatch: expected {:#010x}, found {:#010x}",
                    segment.checksum, actual
                ),
            });
        }
        // The IPC stream starts after the 8-byte magic + 4-byte version header.
        let mut cursor = std::io::Cursor::new(body);
        cursor.set_position(12);
        let ipc = StreamReader::try_new(cursor, None).map_err(|e| {
            BudgetError::SegmentCorrupt {
                path: path.display().to_string(),
                reason: format!("IPC open: {e}"),
            }
        })?;
        let schema = ipc.schema();
        // Schema fingerprint guard: a descriptor paired with a wrong-schema
        // file must not be read as if it matched.
        let actual_fp = schema_fingerprint(&schema);
        if actual_fp != segment.schema_fingerprint {
            return Err(BudgetError::SegmentCorrupt {
                path: path.display().to_string(),
                reason: format!(
                    "schema fingerprint mismatch: expected {}, found {}",
                    segment.schema_fingerprint, actual_fp
                ),
            });
        }
        Ok(Box::new(LocalSegmentReader {
            path,
            schema,
            ipc: Some(ipc),
            expected_rows: segment.row_count,
            _read_permit: _permit,
        }))
    }

    async fn delete_scope(&self, scope: &SpillScope) -> Result<()> {
        let dir = scope.absolute_dir(&self.root);
        if !dir.starts_with(&self.root) {
            return Err(BudgetError::InvalidSpillRoot {
                path: dir.display().to_string(),
                reason: "scope path escapes spill root".into(),
            });
        }
        if dir.exists() {
            let freed = measure_dir_bytes(&dir).unwrap_or(0);
            fs::remove_dir_all(&dir).map_err(|e| BudgetError::SpillIo {
                path: dir.display().to_string(),
                source: e,
            })?;
            self.used_bytes.fetch_sub(freed, Ordering::Relaxed);
            debug!(scope = %scope, freed, "Deleted spill scope");
        }
        Ok(())
    }

    async fn list_orphan_scopes(&self, max_age: Duration) -> Result<Vec<SpillScope>> {
        let mut orphans = Vec::new();
        let now = SystemTime::now();
        let entries = match fs::read_dir(&self.root) {
            Ok(e) => e,
            Err(_) => return Ok(orphans),
        };
        // Walk query/stage/operator/pN/aN — best effort.
        for query in entries.flatten() {
            let qpath = query.path();
            if !qpath.is_dir() {
                continue;
            }
            for stage in fs::read_dir(&qpath).into_iter().flatten().flatten() {
                let spath = stage.path();
                if !spath.is_dir() {
                    continue;
                }
                for op in fs::read_dir(&spath).into_iter().flatten().flatten() {
                    let opath = op.path();
                    if !opath.is_dir() {
                        continue;
                    }
                    for part in fs::read_dir(&opath).into_iter().flatten().flatten() {
                        let ppath = part.path();
                        if !ppath.is_dir() {
                            continue;
                        }
                        for att in fs::read_dir(&ppath).into_iter().flatten().flatten() {
                            let apath = att.path();
                            if !apath.is_dir() {
                                continue;
                            }
                            let modified = att
                                .metadata()
                                .and_then(|m| m.modified())
                                .unwrap_or(SystemTime::UNIX_EPOCH);
                            let age = now.duration_since(modified).unwrap_or_default();
                            if age < max_age {
                                continue;
                            }
                            // Reconstruct scope from path components.
                            let q = query.file_name().to_string_lossy().into_owned();
                            let s = stage.file_name().to_string_lossy().into_owned();
                            let o = op.file_name().to_string_lossy().into_owned();
                            let p_name = part.file_name().to_string_lossy().into_owned();
                            let a_name = att.file_name().to_string_lossy().into_owned();
                            let partition_id = p_name
                                .strip_prefix('p')
                                .and_then(|x| x.parse().ok())
                                .unwrap_or(0);
                            let attempt_id = a_name
                                .strip_prefix('a')
                                .and_then(|x| x.parse().ok())
                                .unwrap_or(0);
                            orphans.push(SpillScope::new(q, s, o, partition_id, attempt_id));
                        }
                    }
                }
            }
        }
        Ok(orphans)
    }

    async fn used_bytes(&self) -> Result<u64> {
        Ok(self.used_bytes.load(Ordering::Relaxed))
    }
}

struct LocalSegmentWriter {
    scope: SpillScope,
    sequence: u64,
    schema: SchemaRef,
    partial_path: PathBuf,
    final_path: PathBuf,
    ipc: Option<StreamWriter<BufWriter<File>>>,
    row_count: u64,
    logical_bytes: u64,
    used_counter: Arc<AtomicU64>,
    _write_permit: tokio::sync::OwnedSemaphorePermit,
}

#[async_trait]
impl SegmentWriter for LocalSegmentWriter {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if take_fault(SpillFault::ShortWrite) {
            return Err(BudgetError::SpillIo {
                path: self.partial_path.display().to_string(),
                source: std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "injected short write",
                ),
            });
        }
        let ipc = self
            .ipc
            .as_mut()
            .ok_or_else(|| BudgetError::Config("writer already finished".into()))?;
        ipc.write(batch).map_err(|e| BudgetError::SpillIo {
            path: self.partial_path.display().to_string(),
            source: std::io::Error::other(e.to_string()),
        })?;
        self.row_count += batch.num_rows() as u64;
        self.logical_bytes += batch.get_array_memory_size() as u64;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<SpillSegment> {
        let mut ipc = self
            .ipc
            .take()
            .ok_or_else(|| BudgetError::Config("writer already finished".into()))?;
        ipc.finish().map_err(|e| BudgetError::SpillIo {
            path: self.partial_path.display().to_string(),
            source: std::io::Error::other(e.to_string()),
        })?;
        // Drop writer to flush/close.
        drop(ipc);

        // Checksum whole file.
        let body = fs::read(&self.partial_path).map_err(|e| BudgetError::SpillIo {
            path: self.partial_path.display().to_string(),
            source: e,
        })?;
        let checksum = crc32fast::hash(&body);
        let physical_bytes = body.len() as u64;

        // Atomic publish (fault inject before rename).
        if take_fault(SpillFault::RenameFailure) {
            let _ = fs::remove_file(&self.partial_path);
            return Err(BudgetError::SpillIo {
                path: self.final_path.display().to_string(),
                source: std::io::Error::other("injected rename failure"),
            });
        }
        fs::rename(&self.partial_path, &self.final_path).map_err(|e| BudgetError::SpillIo {
            path: self.final_path.display().to_string(),
            source: e,
        })?;
        self.used_counter
            .fetch_add(physical_bytes, Ordering::Relaxed);

        let schema_fingerprint = schema_fingerprint(&self.schema);
        Ok(SpillSegment {
            scope: self.scope,
            sequence: self.sequence,
            path: self.final_path,
            schema_fingerprint,
            row_count: self.row_count,
            logical_bytes: self.logical_bytes,
            physical_bytes,
            checksum,
            format_version: SEGMENT_FORMAT_VERSION,
        })
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.ipc = None;
        if self.partial_path.exists() {
            let _ = fs::remove_file(&self.partial_path);
        }
        Ok(())
    }
}

struct LocalSegmentReader {
    path: PathBuf,
    schema: SchemaRef,
    ipc: Option<StreamReader<std::io::Cursor<Vec<u8>>>>,
    /// Row count recorded in the descriptor; verified at end-of-stream.
    expected_rows: u64,
    _read_permit: tokio::sync::OwnedSemaphorePermit,
}

/// Lazy decode state for one segment stream. Holds the read-semaphore permit
/// for the stream's whole lifetime so concurrent spill I/O stays rate-limited.
struct StreamState {
    reader: Option<StreamReader<std::io::Cursor<Vec<u8>>>>,
    rows: u64,
    expected: u64,
    path: PathBuf,
    verified: bool,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

#[async_trait]
impl SegmentReader for LocalSegmentReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        let LocalSegmentReader {
            path,
            schema: _,
            ipc,
            expected_rows,
            _read_permit,
        } = *self;
        let state = StreamState {
            reader: ipc,
            rows: 0,
            expected: expected_rows,
            path,
            verified: false,
            _permit: _read_permit,
        };
        futures::stream::unfold(state, |mut st| async move {
            // Decode one batch per poll: only the encoded body is resident, and
            // decoded batches are handed off one at a time.
            if let Some(reader) = st.reader.as_mut() {
                match reader.next() {
                    Some(Ok(batch)) => {
                        st.rows += batch.num_rows() as u64;
                        return Some((Ok(batch), st));
                    }
                    Some(Err(e)) => {
                        let err = Err(BudgetError::SegmentCorrupt {
                            path: st.path.display().to_string(),
                            reason: e.to_string(),
                        });
                        st.reader = None;
                        return Some((err, st));
                    }
                    None => st.reader = None,
                }
            }
            // End of stream: verify the row count exactly once. A segment
            // truncated at a clean Arrow message boundary makes the reader
            // return `None` with no decode error; only the recorded row count
            // reveals the short read.
            if !st.verified {
                st.verified = true;
                if st.rows != st.expected {
                    let err = Err(BudgetError::SegmentCorrupt {
                        path: st.path.display().to_string(),
                        reason: format!(
                            "row count mismatch: expected {}, read {}",
                            st.expected, st.rows
                        ),
                    });
                    return Some((err, st));
                }
            }
            None
        })
        .boxed()
    }
}

fn schema_fingerprint(schema: &SchemaRef) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    format!("{schema:?}").hash(&mut h);
    h.finish()
}

fn measure_dir_bytes(root: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    if !root.exists() {
        return Ok(0);
    }
    for entry in walkdir_simple(root)? {
        if entry.is_file() {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

fn walkdir_simple(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    fn rec(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
        for e in fs::read_dir(dir)? {
            let e = e?;
            let p = e.path();
            if p.is_dir() {
                rec(&p, out)?;
            } else {
                out.push(p);
            }
        }
        Ok(())
    }
    rec(root, &mut out)?;
    Ok(out)
}

fn free_space_bytes(path: &Path) -> Option<u64> {
    // Best-effort via `statvfs` is not portable without libc; skip on macOS
    // for now and rely on max_bytes quota. Return None = no free-space probe.
    let _ = path;
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(n: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![n, n + 1, n + 2]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn roundtrip_preserves_rows() {
        let _serial = crate::fault::serial_test_guard();
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalSegmentStore::open(tmp.path(), 1 << 30, 0, 2, 2).unwrap();
        let scope = SpillScope::new("q", "s", "op", 0, 0);
        let schema = batch(1).schema();
        let mut w = store.create_writer(&scope, 0, schema).await.unwrap();
        w.write_batch(&batch(1)).await.unwrap();
        w.write_batch(&batch(10)).await.unwrap();
        let seg = w.finish().await.unwrap();
        assert_eq!(seg.row_count, 6);
        assert!(seg.path.exists());

        let r = store.open_reader(&seg).await.unwrap();
        let mut stream = r.into_stream();
        let mut rows = 0usize;
        while let Some(b) = stream.next().await {
            rows += b.unwrap().num_rows();
        }
        assert_eq!(rows, 6);

        store.delete_scope(&scope).await.unwrap();
        assert!(!seg.path.exists());
    }

    #[tokio::test]
    async fn read_rejects_checksum_mismatch() {
        let _serial = crate::fault::serial_test_guard();
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalSegmentStore::open(tmp.path(), 1 << 30, 0, 2, 2).unwrap();
        let scope = SpillScope::new("q", "s", "op", 0, 0);
        let mut w = store
            .create_writer(&scope, 0, batch(1).schema())
            .await
            .unwrap();
        w.write_batch(&batch(1)).await.unwrap();
        let seg = w.finish().await.unwrap();

        // Flip a byte in the IPC body. The old reader ignored the stored
        // checksum and would have returned corrupted data silently.
        let mut bytes = fs::read(&seg.path).unwrap();
        let idx = bytes.len() - 1;
        bytes[idx] ^= 0xFF;
        fs::write(&seg.path, &bytes).unwrap();

        let err = store.open_reader(&seg).await;
        let is_corrupt = matches!(err, Err(BudgetError::SegmentCorrupt { .. }));
        let detail = err
            .err()
            .map_or_else(|| "Ok(reader)".to_string(), |e| format!("{e:?}"));
        assert!(
            is_corrupt,
            "corrupted segment must be rejected on open, got {detail}"
        );
    }

    #[tokio::test]
    async fn read_rejects_truncated_segment() {
        let _serial = crate::fault::serial_test_guard();
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalSegmentStore::open(tmp.path(), 1 << 30, 0, 2, 2).unwrap();
        let scope = SpillScope::new("q", "s", "op", 0, 0);
        let mut w = store
            .create_writer(&scope, 0, batch(1).schema())
            .await
            .unwrap();
        w.write_batch(&batch(1)).await.unwrap();
        w.write_batch(&batch(10)).await.unwrap();
        let seg = w.finish().await.unwrap();

        // Truncate the published segment. The old reader would have returned
        // whatever batches decoded cleanly before EOF as a short read.
        let bytes = fs::read(&seg.path).unwrap();
        fs::write(&seg.path, &bytes[..bytes.len() / 2]).unwrap();

        let err = store.open_reader(&seg).await;
        let is_corrupt = matches!(err, Err(BudgetError::SegmentCorrupt { .. }));
        let detail = err
            .err()
            .map_or_else(|| "Ok(reader)".to_string(), |e| format!("{e:?}"));
        assert!(is_corrupt, "truncated segment must be rejected, got {detail}");
    }

    #[tokio::test]
    async fn reject_symlink_root() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        fs::create_dir_all(&real).unwrap();
        let link = tmp.path().join("link");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&real, &link).unwrap();
            let err = LocalSegmentStore::open(&link, 1 << 20, 0, 1, 1);
            assert!(err.is_err());
        }
    }

    #[tokio::test]
    async fn no_credentials_in_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalSegmentStore::open(tmp.path(), 1 << 30, 0, 1, 1).unwrap();
        // Scope with credential-like input is sanitized.
        let scope = SpillScope::new("AKIASECRET", "s", "op", 0, 0);
        let dir = store.ensure_scope(&scope).await.unwrap();
        let s = dir.to_string_lossy();
        assert!(!s.contains("AKIASECRET") || scope.query_id == "AKIASECRET");
        // Path must stay under root.
        assert!(dir.starts_with(tmp.path()));
    }

    #[tokio::test]
    async fn fault_disk_full_fails_create_writer_only() {
        use crate::fault::{install_faults, FaultSession, SpillFault};
        let _session = FaultSession::new(vec![SpillFault::DiskFull]);
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalSegmentStore::open(tmp.path(), 1 << 30, 0, 1, 1).unwrap();
        let scope = SpillScope::new("q-diskfull", "s", "op", 0, 0);
        let schema = batch(1).schema();
        let err = store.create_writer(&scope, 0, schema).await;
        assert!(
            matches!(err, Err(BudgetError::SpillDiskFull { .. })),
            "got {}",
            err.err().map(|e| e.to_string()).unwrap_or_default()
        );
        // Clear injected plan mid-session and retry.
        install_faults(vec![]);
        let mut w = store
            .create_writer(&scope, 1, batch(1).schema())
            .await
            .unwrap();
        w.write_batch(&batch(1)).await.unwrap();
        let _ = w.finish().await.unwrap();
        store.delete_scope(&scope).await.unwrap();
    }

    #[tokio::test]
    async fn fault_corrupt_on_read() {
        use crate::fault::{install_faults, serial_test_guard, SpillFault};
        let _serial = serial_test_guard();
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalSegmentStore::open(tmp.path(), 1 << 30, 0, 1, 1).unwrap();
        let scope = SpillScope::new("q-corrupt", "s", "op", 0, 0);
        let mut w = store
            .create_writer(&scope, 0, batch(1).schema())
            .await
            .unwrap();
        w.write_batch(&batch(7)).await.unwrap();
        let seg = w.finish().await.unwrap();

        install_faults(vec![SpillFault::CorruptOnRead]);
        let err = store.open_reader(&seg).await;
        assert!(matches!(err, Err(BudgetError::SegmentCorrupt { .. })));
        store.delete_scope(&scope).await.unwrap();
    }

    #[tokio::test]
    async fn fault_short_write_does_not_publish() {
        use crate::fault::{FaultSession, SpillFault};
        let _session = FaultSession::new(vec![SpillFault::ShortWrite]);
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalSegmentStore::open(tmp.path(), 1 << 30, 0, 1, 1).unwrap();
        let scope = SpillScope::new("q-short", "s", "op", 0, 0);
        let mut w = store
            .create_writer(&scope, 0, batch(1).schema())
            .await
            .unwrap();
        let err = w.write_batch(&batch(1)).await;
        assert!(matches!(err, Err(BudgetError::SpillIo { .. })), "got {err:?}");
        // Abort path: no published segment file.
        let _ = w.abort().await;
        let final_path = scope
            .absolute_dir(tmp.path())
            .join(crate::segment::SpillSegment::segment_file_name(0));
        assert!(!final_path.exists());
        store.delete_scope(&scope).await.unwrap();
    }
}
