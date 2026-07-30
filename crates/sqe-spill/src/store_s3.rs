//! S3 (or S3-compatible) spill segment backend.
//!
//! Uses a dedicated bucket/prefix and credential, never table-vended STS
//! credentials or a warehouse path. Writes stage under `*.partial` then
//! publish via copy (atomic from the reader's point of view). Objects are
//! tagged for bucket lifecycle expiry so orphaned segments are reaped even
//! if process cleanup fails.
//!
//! Tests inject an [`object_store::memory::InMemory`] store; production builds
//! an AmazonS3 client from [`S3SpillConfig`].

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
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{
    Attribute, AttributeValue, Attributes, GetOptions, GetRange, ObjectStore, ObjectStoreExt,
    PutOptions, PutPayload, TagSet,
};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use tracing::debug;

// ───────────────────────────── Lifecycle tags ────────────────────────────────

/// Object tag key identifying SQE spill objects for S3 lifecycle rules.
///
/// Example bucket lifecycle filter:
/// `Tag: sqe-spill = true` with expiration after N days.
pub const LIFECYCLE_TAG_KEY: &str = "sqe-spill";
pub const LIFECYCLE_TAG_VALUE: &str = "true";
/// Distinguishes spill segments from durable-exchange manifests.
pub const LIFECYCLE_PURPOSE_KEY: &str = "sqe-purpose";
pub const LIFECYCLE_PURPOSE_SEGMENT: &str = "spill-segment";
pub const LIFECYCLE_PURPOSE_PARTIAL: &str = "spill-partial";
/// Opaque query id (sanitized) for operator filtering / cost allocation.
pub const LIFECYCLE_QUERY_KEY: &str = "sqe-query";

/// Build the standard lifecycle [`TagSet`] for a published segment.
pub fn lifecycle_tags_for_segment(scope: &SpillScope) -> TagSet {
    let mut tags = TagSet::default();
    tags.push(LIFECYCLE_TAG_KEY, LIFECYCLE_TAG_VALUE);
    tags.push(LIFECYCLE_PURPOSE_KEY, LIFECYCLE_PURPOSE_SEGMENT);
    tags.push(LIFECYCLE_QUERY_KEY, &scope.query_id);
    tags
}

/// Tags for staging (partial) objects — shorter expiry can target these.
pub fn lifecycle_tags_for_partial(scope: &SpillScope) -> TagSet {
    let mut tags = TagSet::default();
    tags.push(LIFECYCLE_TAG_KEY, LIFECYCLE_TAG_VALUE);
    tags.push(LIFECYCLE_PURPOSE_KEY, LIFECYCLE_PURPOSE_PARTIAL);
    tags.push(LIFECYCLE_QUERY_KEY, &scope.query_id);
    tags
}

/// User metadata attributes mirrored for clients that do not surface tags.
pub fn lifecycle_attributes(scope: &SpillScope, purpose: &str) -> Attributes {
    let mut attrs = Attributes::new();
    attrs.insert(
        Attribute::Metadata(LIFECYCLE_TAG_KEY.into()),
        AttributeValue::from(LIFECYCLE_TAG_VALUE),
    );
    attrs.insert(
        Attribute::Metadata(LIFECYCLE_PURPOSE_KEY.into()),
        AttributeValue::from(purpose.to_string()),
    );
    attrs.insert(
        Attribute::Metadata(LIFECYCLE_QUERY_KEY.into()),
        AttributeValue::from(scope.query_id.clone()),
    );
    attrs.insert(
        Attribute::ContentType,
        AttributeValue::from("application/octet-stream"),
    );
    attrs
}

// ───────────────────────────── Config ────────────────────────────────────────

/// Dedicated S3 credentials and location for spill (not table STS).
///
/// `Debug` is hand-written so `secret_access_key` never renders through
/// `{:?}`, a panic, or an error chain. `sqe-spill` does not depend on
/// `sqe-core`, so this mirrors the `SecretString` redaction guarantee that
/// guards the config-side `WorkerSpillS3Config` (CORE-01).
#[derive(Clone)]
pub struct S3SpillConfig {
    pub bucket: String,
    /// Key prefix under the bucket (e.g. `sqe-spill/`). Must not overlap
    /// table warehouse prefixes.
    pub prefix: String,
    pub region: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub allow_http: bool,
    pub path_style: bool,
    pub max_bytes: u64,
    pub max_objects: u64,
    pub max_concurrent_writes: usize,
    pub max_concurrent_reads: usize,
}

impl std::fmt::Debug for S3SpillConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3SpillConfig")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("allow_http", &self.allow_http)
            .field("path_style", &self.path_style)
            .field("max_bytes", &self.max_bytes)
            .field("max_objects", &self.max_objects)
            .field("max_concurrent_writes", &self.max_concurrent_writes)
            .field("max_concurrent_reads", &self.max_concurrent_reads)
            .finish()
    }
}

impl S3SpillConfig {
    /// Reject empty bucket/prefix, credential-like path injection, and
    /// common warehouse prefixes that must never hold spill data.
    pub fn validate(&self) -> Result<()> {
        if self.bucket.trim().is_empty() {
            return Err(BudgetError::Config(
                "worker.spill.s3.bucket must be set for s3 backend".into(),
            ));
        }
        if self.prefix.trim().is_empty() {
            return Err(BudgetError::Config(
                "worker.spill.s3.prefix must be set (dedicated spill prefix, not table data)"
                    .into(),
            ));
        }
        let p = self.prefix.to_ascii_lowercase();
        for forbidden in ["warehouse", "iceberg", "data/", "table/"] {
            if p.contains(forbidden) {
                return Err(BudgetError::Config(format!(
                    "worker.spill.s3.prefix must not look like table data (contains '{forbidden}')"
                )));
            }
        }
        // Reject empty static credentials that would fall through to IMDS /
        // ambient env (table-vended STS risk). Require explicit keys.
        if self.access_key_id.trim().is_empty() || self.secret_access_key.trim().is_empty() {
            return Err(BudgetError::Config(
                "worker.spill.s3 requires dedicated access_key_id and secret_access_key \
                 (do not use table-vended STS credentials)"
                    .into(),
            ));
        }
        // Session tokens look like ASIA* temporary keys when only ambient
        // chain is used; if the access key starts with ASIA without an
        // explicit session path we still allow but warn via config note —
        // hard reject ASIA keys to force long-lived spill IAM user/role keys.
        if self.access_key_id.starts_with("ASIA") {
            return Err(BudgetError::Config(
                "worker.spill.s3.access_key_id looks like temporary STS (ASIA*); \
                 use a dedicated long-lived spill credential"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Normalize prefix to end with `/` and not start with `/`.
    pub fn normalized_prefix(&self) -> String {
        let mut p = self.prefix.trim().trim_start_matches('/').to_string();
        if !p.is_empty() && !p.ends_with('/') {
            p.push('/');
        }
        p
    }
}

// ───────────────────────────── Store ─────────────────────────────────────────

/// S3-backed [`SegmentStore`].
pub struct S3SegmentStore {
    store: Arc<dyn ObjectStore>,
    /// Bucket name (for path display only when using real S3).
    bucket: String,
    prefix: String,
    max_bytes: u64,
    max_objects: u64,
    used_bytes: Arc<AtomicU64>,
    used_objects: Arc<AtomicU64>,
    write_sem: Arc<Semaphore>,
    read_sem: Arc<Semaphore>,
}

impl S3SegmentStore {
    /// Open against a pre-built object store (production S3 or test InMemory).
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: &S3SpillConfig,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            store,
            bucket: config.bucket.clone(),
            prefix: config.normalized_prefix(),
            max_bytes: config.max_bytes.max(1),
            max_objects: config.max_objects.max(1),
            used_bytes: Arc::new(AtomicU64::new(0)),
            used_objects: Arc::new(AtomicU64::new(0)),
            write_sem: Arc::new(Semaphore::new(config.max_concurrent_writes.max(1))),
            read_sem: Arc::new(Semaphore::new(config.max_concurrent_reads.max(1))),
        })
    }

    /// Build an AmazonS3 client from config and wrap it.
    pub fn from_config(config: &S3SpillConfig) -> Result<Self> {
        config.validate()?;
        let store = build_amazon_s3(config)?;
        Self::new(store, config)
    }

    /// Object key for a scope-relative file name.
    fn key(&self, scope: &SpillScope, file_name: &str) -> ObjectPath {
        let rel = scope.relative_dir();
        let joined = format!(
            "{}{}/{}",
            self.prefix,
            rel.to_string_lossy().replace('\\', "/"),
            file_name
        );
        ObjectPath::from(joined)
    }

    fn key_prefix_for_scope(&self, scope: &SpillScope) -> ObjectPath {
        let rel = scope.relative_dir();
        let joined = format!(
            "{}{}/",
            self.prefix,
            rel.to_string_lossy().replace('\\', "/")
        );
        ObjectPath::from(joined)
    }

    fn reserve_quota(&self, need: u64) -> Result<()> {
        if take_fault(SpillFault::DiskFull) {
            return Err(BudgetError::SpillDiskFull {
                need,
                available: 0,
            });
        }
        if take_fault(SpillFault::QuotaExceeded) {
            return Err(BudgetError::SpillQuotaExceeded {
                scope: "s3".into(),
                need,
                free: 0,
                max: self.max_bytes,
            });
        }
        let used = self.used_bytes.load(Ordering::Relaxed);
        if used.saturating_add(need) > self.max_bytes {
            return Err(BudgetError::SpillQuotaExceeded {
                scope: "s3".into(),
                need,
                free: self.max_bytes.saturating_sub(used),
                max: self.max_bytes,
            });
        }
        let objs = self.used_objects.load(Ordering::Relaxed);
        if objs.saturating_add(1) > self.max_objects {
            return Err(BudgetError::SpillQuotaExceeded {
                scope: "s3-objects".into(),
                need: 1,
                free: self.max_objects.saturating_sub(objs),
                max: self.max_objects,
            });
        }
        Ok(())
    }

}

fn build_amazon_s3(config: &S3SpillConfig) -> Result<Arc<dyn ObjectStore>> {
    use object_store::aws::AmazonS3Builder;
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key);
    if !config.region.is_empty() {
        builder = builder.with_region(&config.region);
    }
    if !config.endpoint.is_empty() {
        builder = builder.with_endpoint(&config.endpoint);
    }
    if config.allow_http {
        builder = builder.with_allow_http(true);
    }
    if config.path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }
    let s3 = builder
        .build()
        .map_err(|e| BudgetError::Config(format!("S3 spill client: {e}")))?;
    Ok(Arc::new(s3))
}

fn map_os_err(path: &str, e: object_store::Error) -> BudgetError {
    BudgetError::SpillIo {
        path: path.to_string(),
        source: std::io::Error::other(e.to_string()),
    }
}

#[async_trait]
impl SegmentStore for S3SegmentStore {
    async fn ensure_scope(&self, scope: &SpillScope) -> Result<PathBuf> {
        // Object stores are prefix-based; no mkdir. Return the logical prefix.
        Ok(PathBuf::from(format!(
            "s3://{}/{}{}",
            self.bucket,
            self.prefix,
            scope.relative_dir().to_string_lossy()
        )))
    }

    async fn create_writer(
        &self,
        scope: &SpillScope,
        sequence: u64,
        schema: SchemaRef,
    ) -> Result<Box<dyn SegmentWriter>> {
        let permit = self
            .write_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BudgetError::Cancelled {
                budget: "spill-write-s3".into(),
            })?;
        self.reserve_quota(64 * 1024)?;

        let partial_key = self.key(scope, &SpillSegment::partial_file_name(sequence));
        let final_key = self.key(scope, &SpillSegment::segment_file_name(sequence));

        // Buffer IPC into memory; segment_target_size bounds this in production.
        // Prepend magic+version, then seek past them so StreamWriter appends.
        let mut body = Vec::with_capacity(64 * 1024);
        body.extend_from_slice(SEGMENT_MAGIC);
        body.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
        let mut cursor = Cursor::new(body);
        cursor.set_position(cursor.get_ref().len() as u64);
        let ipc = StreamWriter::try_new(cursor, &schema).map_err(|e| {
            BudgetError::Config(format!("Arrow IPC writer init failed: {e}"))
        })?;

        Ok(Box::new(S3SegmentWriter {
            store: self.store.clone(),
            scope: scope.clone(),
            sequence,
            schema,
            partial_key,
            final_key,
            bucket: self.bucket.clone(),
            ipc: Some(ipc),
            row_count: 0,
            logical_bytes: 0,
            used_bytes: self.used_bytes.clone(),
            used_objects: self.used_objects.clone(),
            _write_permit: permit,
        }))
    }

    async fn open_reader(&self, segment: &SpillSegment) -> Result<Box<dyn SegmentReader>> {
        let permit = self
            .read_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BudgetError::Cancelled {
                budget: "spill-read-s3".into(),
            })?;
        if take_fault(SpillFault::CorruptOnRead) {
            return Err(BudgetError::SegmentCorrupt {
                path: segment.path.display().to_string(),
                reason: "injected corruption on open_reader".into(),
            });
        }

        // Path stored as s3://bucket/key — extract key after bucket/.
        let key = object_key_from_display(&segment.path, &self.bucket)?;
        let display = segment.path.display().to_string();

        // Range-GET the header first (plan: range GETs).
        let header_opts = GetOptions {
            range: Some(GetRange::Bounded(0..12)),
            ..Default::default()
        };
        let header_res = self
            .store
            .get_opts(&key, header_opts)
            .await
            .map_err(|e| map_os_err(&display, e))?;
        let header = header_res
            .bytes()
            .await
            .map_err(|e| map_os_err(&display, e))?;
        if header.len() < 12 {
            return Err(BudgetError::SegmentCorrupt {
                path: display,
                reason: format!("truncated header: {} bytes", header.len()),
            });
        }
        if &header[..8] != SEGMENT_MAGIC {
            return Err(BudgetError::SegmentCorrupt {
                path: display,
                reason: format!("bad magic {:?}", &header[..8]),
            });
        }
        let version = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        if version != SEGMENT_FORMAT_VERSION {
            return Err(BudgetError::UnsupportedSegmentVersion {
                path: display,
                version,
            });
        }

        // Full body via get (segment-bounded). Could multi-range; single get
        // is correct for segment_target_size objects and lets us CRC-check.
        let get_result = self
            .store
            .get(&key)
            .await
            .map_err(|e| map_os_err(&display, e))?;
        let body: Bytes = get_result
            .bytes()
            .await
            .map_err(|e| map_os_err(&display, e))?;
        let actual = crc32fast::hash(body.as_ref());
        if actual != segment.checksum {
            return Err(BudgetError::SegmentCorrupt {
                path: display,
                reason: format!(
                    "checksum mismatch: expected {:#010x}, found {:#010x}",
                    segment.checksum, actual
                ),
            });
        }

        let mut cursor = Cursor::new(body.to_vec());
        // Skip magic+version for IPC.
        std::io::Read::read_exact(&mut cursor, &mut [0u8; 12]).map_err(|e| {
            BudgetError::SegmentCorrupt {
                path: segment.path.display().to_string(),
                reason: format!("seek past header: {e}"),
            }
        })?;
        let ipc = StreamReader::try_new(cursor, None).map_err(|e| {
            BudgetError::SegmentCorrupt {
                path: segment.path.display().to_string(),
                reason: format!("IPC open: {e}"),
            }
        })?;
        let schema = ipc.schema();
        Ok(Box::new(S3SegmentReader {
            path: segment.path.clone(),
            schema,
            ipc: Some(ipc),
            expected_rows: segment.row_count,
            _read_permit: permit,
        }))
    }

    async fn delete_scope(&self, scope: &SpillScope) -> Result<()> {
        let prefix = self.key_prefix_for_scope(scope);
        let mut list = self.store.list(Some(&prefix));
        let mut keys = Vec::new();
        while let Some(item) = list.next().await {
            let meta = item.map_err(|e| map_os_err(prefix.as_ref(), e))?;
            keys.push((meta.location, meta.size));
        }
        for (loc, size) in keys {
            if let Err(e) = self.store.delete(&loc).await {
                debug!(key = %loc, error = %e, "S3 spill delete failed");
            } else {
                self.used_bytes.fetch_sub(size, Ordering::Relaxed);
                self.used_objects
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                        Some(c.saturating_sub(1))
                    })
                    .ok();
            }
        }
        Ok(())
    }

    async fn list_orphan_scopes(
        &self,
        max_age: Duration,
    ) -> Result<Vec<SpillScope>> {
        let prefix = ObjectPath::from(self.prefix.as_str());
        let mut list = self.store.list(Some(&prefix));
        let now = SystemTime::now();
        let mut orphans = Vec::new();
        let mut seen = std::collections::HashSet::new();
        while let Some(item) = list.next().await {
            let meta = item.map_err(|e| map_os_err(self.prefix.as_str(), e))?;
            let lm_secs = meta.last_modified.timestamp().max(0) as u64;
            let lm_sys = UNIX_EPOCH + Duration::from_secs(lm_secs);
            let age = now.duration_since(lm_sys).unwrap_or_default();
            if age < max_age {
                continue;
            }
            // Parse scope from key relative to prefix: query/stage/op/pN/aN/file
            let full = meta.location.as_ref();
            let rel = full.strip_prefix(&self.prefix).unwrap_or(full);
            let parts: Vec<&str> = rel.split('/').filter(|p| !p.is_empty()).collect();
            if parts.len() < 5 {
                continue;
            }
            let partition_id = parts[3]
                .strip_prefix('p')
                .and_then(|x| x.parse().ok())
                .unwrap_or(0);
            let attempt_id = parts[4]
                .strip_prefix('a')
                .and_then(|x| x.parse().ok())
                .unwrap_or(0);
            let scope = SpillScope::new(parts[0], parts[1], parts[2], partition_id, attempt_id);
            if seen.insert(scope.clone()) {
                orphans.push(scope);
            }
        }
        Ok(orphans)
    }

    async fn used_bytes(&self) -> Result<u64> {
        Ok(self.used_bytes.load(Ordering::Relaxed))
    }
}

fn object_key_from_display(path: &std::path::Path, bucket: &str) -> Result<ObjectPath> {
    let s = path.to_string_lossy();
    let prefix = format!("s3://{bucket}/");
    let key = s.strip_prefix(&prefix).ok_or_else(|| {
        BudgetError::Config(format!(
            "S3 segment path '{s}' does not start with {prefix}"
        ))
    })?;
    Ok(ObjectPath::from(key))
}

// ───────────────────────────── Writer ────────────────────────────────────────

struct S3SegmentWriter {
    store: Arc<dyn ObjectStore>,
    scope: SpillScope,
    sequence: u64,
    schema: SchemaRef,
    partial_key: ObjectPath,
    final_key: ObjectPath,
    bucket: String,
    ipc: Option<StreamWriter<Cursor<Vec<u8>>>>,
    row_count: u64,
    logical_bytes: u64,
    used_bytes: Arc<AtomicU64>,
    used_objects: Arc<AtomicU64>,
    _write_permit: tokio::sync::OwnedSemaphorePermit,
}

#[async_trait]
impl SegmentWriter for S3SegmentWriter {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if take_fault(SpillFault::ShortWrite) {
            return Err(BudgetError::SpillIo {
                path: self.partial_key.to_string(),
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
            path: self.partial_key.to_string(),
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
            path: self.partial_key.to_string(),
            source: std::io::Error::other(e.to_string()),
        })?;
        let cursor = ipc.into_inner().map_err(|e| BudgetError::SpillIo {
            path: self.partial_key.to_string(),
            source: std::io::Error::other(e.to_string()),
        })?;
        let body = cursor.into_inner();
        let checksum = crc32fast::hash(&body);
        let physical_bytes = body.len() as u64;

        if take_fault(SpillFault::RenameFailure) {
            return Err(BudgetError::SpillIo {
                path: self.final_key.to_string(),
                source: std::io::Error::other("injected rename failure"),
            });
        }

        // Stage partial with lifecycle tags (short-lived purpose).
        let mut partial_opts = PutOptions::from(lifecycle_tags_for_partial(&self.scope));
        partial_opts.attributes =
            lifecycle_attributes(&self.scope, LIFECYCLE_PURPOSE_PARTIAL);
        self.store
            .put_opts(
                &self.partial_key,
                PutPayload::from(Bytes::from(body.clone())),
                partial_opts,
            )
            .await
            .map_err(|e| map_os_err(self.partial_key.as_ref(), e))?;

        // Publish: put final with segment tags (copy not always tagged on all backends).
        let mut final_opts = PutOptions::from(lifecycle_tags_for_segment(&self.scope));
        final_opts.attributes =
            lifecycle_attributes(&self.scope, LIFECYCLE_PURPOSE_SEGMENT);
        self.store
            .put_opts(
                &self.final_key,
                PutPayload::from(Bytes::from(body)),
                final_opts,
            )
            .await
            .map_err(|e| map_os_err(self.final_key.as_ref(), e))?;

        // Delete partial staging object.
        let _ = self.store.delete(&self.partial_key).await;

        self.used_bytes
            .fetch_add(physical_bytes, Ordering::Relaxed);
        self.used_objects.fetch_add(1, Ordering::Relaxed);

        let schema_fingerprint = schema_fingerprint(&self.schema);
        let path = PathBuf::from(format!("s3://{}/{}", self.bucket, self.final_key));
        Ok(SpillSegment {
            scope: self.scope,
            sequence: self.sequence,
            path,
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
        let _ = self.store.delete(&self.partial_key).await;
        Ok(())
    }
}

// ───────────────────────────── Reader ────────────────────────────────────────

struct S3SegmentReader {
    path: PathBuf,
    schema: SchemaRef,
    ipc: Option<StreamReader<Cursor<Vec<u8>>>>,
    expected_rows: u64,
    _read_permit: tokio::sync::OwnedSemaphorePermit,
}

#[async_trait]
impl SegmentReader for S3SegmentReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn into_stream(mut self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        let path = self.path.display().to_string();
        let expected = self.expected_rows;
        let mut rows = 0u64;
        let mut batches = Vec::new();
        if let Some(reader) = self.ipc.take() {
            for item in reader {
                match item {
                    Ok(batch) => {
                        rows += batch.num_rows() as u64;
                        batches.push(Ok(batch));
                    }
                    Err(e) => {
                        batches.push(Err(BudgetError::SegmentCorrupt {
                            path: path.clone(),
                            reason: e.to_string(),
                        }));
                        break;
                    }
                }
            }
            if rows != expected && batches.iter().all(|b| b.is_ok()) {
                batches.push(Err(BudgetError::SegmentCorrupt {
                    path,
                    reason: format!("row count mismatch: expected {expected}, got {rows}"),
                }));
            }
        }
        futures::stream::iter(batches).boxed()
    }
}

fn schema_fingerprint(schema: &SchemaRef) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    format!("{schema:?}").hash(&mut h);
    h.finish()
}

// ───────────────────────────── Tests ─────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use object_store::memory::InMemory;

    fn test_config() -> S3SpillConfig {
        S3SpillConfig {
            bucket: "sqe-spill-test".into(),
            prefix: "sqe-spill/".into(),
            region: "us-east-1".into(),
            endpoint: String::new(),
            access_key_id: "AKIA_TEST_KEY".into(),
            secret_access_key: "secret".into(),
            allow_http: true,
            path_style: true,
            max_bytes: 1 << 30,
            max_objects: 10_000,
            max_concurrent_writes: 4,
            max_concurrent_reads: 4,
        }
    }

    fn batch(n: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![n, n + 1, n + 2]))],
        )
        .unwrap()
    }

    fn memory_store() -> Arc<S3SegmentStore> {
        let cfg = test_config();
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Arc::new(S3SegmentStore::new(mem, &cfg).unwrap())
    }

    #[test]
    fn validate_rejects_warehouse_prefix() {
        let mut cfg = test_config();
        cfg.prefix = "warehouse/iceberg/".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_asia_sts_keys() {
        let mut cfg = test_config();
        cfg.access_key_id = "ASIAEXAMPLE".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_credentials() {
        let mut cfg = test_config();
        cfg.access_key_id.clear();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn lifecycle_tags_include_spill_marker() {
        let scope = SpillScope::new("q1", "s", "op", 0, 0);
        let tags = lifecycle_tags_for_segment(&scope);
        let enc = tags.encoded();
        assert!(enc.contains("sqe-spill"));
        assert!(enc.contains("true"));
        assert!(enc.contains("spill-segment"));
        assert!(enc.contains("q1"));
    }

    #[tokio::test]
    async fn s3_roundtrip_preserves_rows() {
        let store = memory_store();
        let scope = SpillScope::new("q-s3", "stage0", "shuffle", 0, 0);
        let schema = batch(1).schema();
        let mut w = store.create_writer(&scope, 0, schema).await.unwrap();
        w.write_batch(&batch(1)).await.unwrap();
        w.write_batch(&batch(10)).await.unwrap();
        let seg = w.finish().await.unwrap();
        assert_eq!(seg.row_count, 6);
        assert!(seg.path.to_string_lossy().starts_with("s3://sqe-spill-test/"));

        let r = store.open_reader(&seg).await.unwrap();
        let mut stream = r.into_stream();
        let mut rows = 0usize;
        while let Some(b) = stream.next().await {
            rows += b.unwrap().num_rows();
        }
        assert_eq!(rows, 6);

        store.delete_scope(&scope).await.unwrap();
        assert!(store.open_reader(&seg).await.is_err());
    }

    #[tokio::test]
    async fn s3_object_quota() {
        let mut cfg = test_config();
        cfg.max_objects = 1;
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(S3SegmentStore::new(mem, &cfg).unwrap());
        let scope = SpillScope::new("q", "s", "op", 0, 0);
        let mut w = store
            .create_writer(&scope, 0, batch(1).schema())
            .await
            .unwrap();
        w.write_batch(&batch(1)).await.unwrap();
        let _ = w.finish().await.unwrap();
        // Second object should fail quota at create (reserve counts +1).
        let err = store
            .create_writer(&scope, 1, batch(1).schema())
            .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn no_credentials_in_object_keys() {
        let store = memory_store();
        let scope = SpillScope::new("AKIASECRET", "s", "op", 0, 0);
        let mut w = store
            .create_writer(&scope, 0, batch(1).schema())
            .await
            .unwrap();
        w.write_batch(&batch(0)).await.unwrap();
        let seg = w.finish().await.unwrap();
        let key = seg.path.to_string_lossy();
        // Sanitized scope may keep alphanumeric AKIASECRET but must not embed
        // secret keys from config.
        assert!(!key.contains("secret"));
        assert!(!key.contains("SECRET_KEY"));
    }
}
