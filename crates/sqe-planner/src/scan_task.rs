use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;

use crate::scan_morsel::{
    SCAN_TASK_VERSION_CURRENT, SCAN_TASK_VERSION_V1, SCAN_TASK_VERSION_V2, SCAN_TASK_VERSION_V3,
};

/// File format of a Hive external table's data files.
///
/// Deliberately a wire-local enum rather than a re-export of
/// `sqe_catalog::hive::serde_map::HiveFileFormat`. `sqe-planner` does not
/// depend on `sqe-catalog`, and more importantly a wire type should not move
/// every time an internal enum gains a variant: a format the worker cannot
/// read must fail the fallback check at planning time, not deserialize into
/// something a new worker guesses at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HiveScanFormat {
    Csv,
    Json,
    Parquet,
}

/// Everything a worker needs to open a Hive external table's files that an
/// Iceberg task does not carry. Present ONLY on a Hive task; `None` keeps the
/// ticket byte-identical to v2 (see `hive_task_absent_is_byte_identical_to_v2`).
///
/// Why the partition data is shaped this way, rather than just a value map:
/// `HiveTableProvider::attach_partition_values` builds a `ProjectionExec` that
/// emits a column reference for indices `< file_schema.len()` and a typed
/// LITERAL above that, indexing `partition_columns` BY POSITION. So the worker
/// needs the declared order (a `BTreeMap` has its own, unrelated) and the file
/// schema (to know the split point, and to type each literal from the field's
/// own data type). A value map alone would silently reorder columns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HiveScanDescriptor {
    /// How to open the files. `Parquet` uses the same reader as an Iceberg task.
    pub format: HiveScanFormat,
    /// CSV field delimiter. Ignored for `Json` and `Parquet`.
    #[serde(default)]
    pub delimiter: u8,
    /// CSV quote character, if the SerDe declared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<u8>,
    /// CSV escape character, if the SerDe declared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escape: Option<u8>,
    /// Whether the first line of each CSV file is a header to skip.
    #[serde(default)]
    pub has_header: bool,
    /// Directory-encoded partition values for this task's files, keyed by
    /// partition column name. A task carries ONE map, so a task must not span
    /// partitions.
    #[serde(default)]
    pub partition_values: BTreeMap<String, String>,
    /// Declared partition key order. Load-bearing: the projection indexes by
    /// position, and `partition_values` is a map with no meaningful order.
    #[serde(default)]
    pub partition_columns: Vec<String>,
    /// The FULL Arrow schema, JSON-serialized: file columns followed by
    /// partition columns, in that order.
    ///
    /// It must be the full schema, not the file schema. `attach_partition_values`
    /// types each partition literal from `full_schema.field(i)`, and a partition
    /// column does not appear in the data file at all, so a file-only schema
    /// carries no type for it and the worker would have to guess (in practice:
    /// default everything to Utf8, changing an integer partition key's type
    /// between coordinator-local and distributed execution of the same query).
    ///
    /// The split point is derived, not carried: file columns are everything
    /// before `len() - partition_columns.len()`.
    #[serde(default)]
    pub full_schema_json: String,
}

/// A secret carried inside a scan ticket. The ticket is the transport, so it
/// serializes as the plain value; `Debug` never prints it (finding 401).
#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct TicketSecret(String);

impl TicketSecret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Debug for TicketSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_empty() {
            "[empty]"
        } else {
            "[REDACTED]"
        })
    }
}

/// Where a scan ticket's storage credentials come from (finding 401).
///
/// `WorkerDefault` is the serde default so a ticket with no credential block
/// never implies the engine's static key pair.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScanCredentials {
    /// No credentials shipped; the worker resolves its own chain (IRSA,
    /// instance profile, environment). Single-tenant deployments only.
    #[default]
    WorkerDefault,
    /// Short-lived credentials the catalog vended for this table and this
    /// user. `expires_at` is unix seconds when known.
    Vended {
        access_key: TicketSecret,
        secret_key: TicketSecret,
        #[serde(default)]
        session_token: TicketSecret,
        #[serde(default)]
        expires_at: Option<i64>,
    },
    /// The engine's static `[storage]` key pair. Requires the coordinator to
    /// opt in via `coordinator.distributed_credentials`.
    Static {
        access_key: TicketSecret,
        secret_key: TicketSecret,
        #[serde(default)]
        session_token: TicketSecret,
    },
}

impl ScanCredentials {
    pub fn static_keys(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: impl Into<String>,
    ) -> Self {
        Self::Static {
            access_key: TicketSecret::new(access_key),
            secret_key: TicketSecret::new(secret_key),
            session_token: TicketSecret::new(session_token),
        }
    }

    pub fn vended(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: impl Into<String>,
        expires_at: Option<i64>,
    ) -> Self {
        Self::Vended {
            access_key: TicketSecret::new(access_key),
            secret_key: TicketSecret::new(secret_key),
            session_token: TicketSecret::new(session_token),
            expires_at,
        }
    }

    /// Label for logs, metrics and audit (`worker_default`, `vended`, `static`).
    pub fn source(&self) -> &'static str {
        match self {
            Self::WorkerDefault => "worker_default",
            Self::Vended { .. } => "vended",
            Self::Static { .. } => "static",
        }
    }

    pub fn access_key(&self) -> &str {
        match self {
            Self::WorkerDefault => "",
            Self::Vended { access_key, .. } | Self::Static { access_key, .. } => {
                access_key.expose()
            }
        }
    }

    pub fn secret_key(&self) -> &str {
        match self {
            Self::WorkerDefault => "",
            Self::Vended { secret_key, .. } | Self::Static { secret_key, .. } => {
                secret_key.expose()
            }
        }
    }

    pub fn session_token(&self) -> &str {
        match self {
            Self::WorkerDefault => "",
            Self::Vended { session_token, .. } | Self::Static { session_token, .. } => {
                session_token.expose()
            }
        }
    }

    /// Unix-seconds expiry of vended credentials, if the catalog told us.
    pub fn expires_at(&self) -> Option<i64> {
        match self {
            Self::Vended { expires_at, .. } => *expires_at,
            _ => None,
        }
    }

    /// True when the ticket carries key material of any kind.
    pub fn carries_keys(&self) -> bool {
        !matches!(self, Self::WorkerDefault)
    }
}

/// Lightweight message sent from coordinator to worker describing
/// which Parquet files to scan and how to access them.
///
/// Workers receive this as a JSON-encoded Flight Ticket body. Storage
/// credentials ride inside `credentials` so workers need no catalog access;
/// the ticket is signed (finding 601) and should travel over TLS.
#[derive(Clone, Serialize, Deserialize)]
pub struct ScanTask {
    /// Ticket encoding version. `1` = whole-file (pre-morsel); `2` = optional
    /// row-group / byte-range morsel fields; `3` = `hive`. Workers reject
    /// unsupported versions. Missing field deserializes as v1 for old
    /// coordinators.
    ///
    /// Finding 109: do not hand-set this. Build tasks with
    /// [`ScanTask::builder`], which derives it from the fields present
    /// ([`ScanTask::required_version`]); [`ScanTask::to_bytes`] refuses a task
    /// that declares less than its fields need. Still `pub` so existing struct
    /// literals compile while they migrate to the builder.
    #[serde(default = "default_scan_task_version")]
    pub version: u32,
    /// Unique identifier for this fragment.
    pub fragment_id: String,
    /// Optional stable morsel id for retry / dedup (Phase 2).
    #[serde(default)]
    pub morsel_id: Option<String>,
    /// S3 URLs of Parquet data files to scan.
    pub data_file_paths: Vec<String>,
    /// Size in bytes per file, parallel to `data_file_paths`.
    /// Empty if sizes were not available at planning time — callers should
    /// treat an empty vec as "unknown" and fall back to file-count cost.
    #[serde(default)]
    pub file_sizes_bytes: Vec<u64>,
    /// Inclusive start row-group for the first file when this task is a
    /// sub-file morsel. `None` = whole file(s).
    #[serde(default)]
    pub row_group_start: Option<u32>,
    /// Exclusive end row-group for the first file. `None` with a start means
    /// "through the last group".
    #[serde(default)]
    pub row_group_end: Option<u32>,
    /// Optional compressed byte range start (inclusive) for the first file.
    #[serde(default)]
    pub start_byte: Option<u64>,
    /// Optional compressed byte range end (exclusive) for the first file.
    #[serde(default)]
    pub end_byte: Option<u64>,
    /// Column names to project (empty = all columns). Used as a fallback when
    /// the parquet file lacks PARQUET:field_id metadata or when the
    /// coordinator did not supply field IDs.
    pub projected_columns: Vec<String>,
    /// Iceberg field IDs to project, parallel to `projected_columns` (#43).
    ///
    /// When non-empty and the parquet file has PARQUET:field_id metadata on
    /// each field, the worker projects by field ID so RENAME COLUMN and
    /// ADD COLUMN survive schema evolution. Older coordinators may not send
    /// this field; readers fall back to `projected_columns` matching in that
    /// case.
    #[serde(default)]
    pub projected_field_ids: Vec<i32>,
    /// S3 endpoint URL.
    pub s3_endpoint: String,
    /// S3 region.
    pub s3_region: String,
    /// Storage credentials for this scan (finding 401). Typed so the ticket
    /// records where they came from, `Debug` never prints them, and a
    /// worker can be told to resolve its own instead.
    #[serde(default)]
    pub credentials: ScanCredentials,
    /// Worker URL this ticket was issued to (finding 601). Part of the signed
    /// bytes, so a ticket captured from one worker cannot be replayed to
    /// another. Empty only on legacy tickets.
    #[serde(default)]
    pub audience: String,
    /// Unix seconds after which the worker refuses the ticket (finding 601).
    /// `0` only on legacy tickets.
    #[serde(default)]
    pub expires_at: i64,
    /// Whether to use path-style S3 access (required for most S3-compatible endpoints).
    pub s3_path_style: bool,
    /// Allow plaintext HTTP for S3 endpoints. Only enable for dev/test (e.g., MinIO).
    pub s3_allow_http: bool,
    /// Pushed-down filter predicate (#233), serialized as a DataFusion logical
    /// `Expr` via datafusion-proto (`Expr::to_bytes`). Columns are referenced by
    /// name, so the worker can rebuild a `PhysicalExpr` against the file schema
    /// without depending on projection order.
    ///
    /// Applying this on the worker is a pure optimization: the coordinator keeps
    /// the authoritative `FilterExec` above `DistributedScanExec`, so pushing the
    /// predicate only changes how many rows ship over Flight, never correctness.
    /// `None` for old coordinators or when no predicate exists.
    ///
    /// Rides the wire base64-encoded under the key `predicate_proto_b64`
    /// (#432). A `Vec<u8>` inside a JSON document serializes as an array of
    /// decimal numbers, which measured **2.9x** inflation on real predicates:
    /// a 1.5 MB serialized `Expr` became a 4.5 MB ticket and the worker
    /// refused it at tonic's 4 MiB decode ceiling. base64 costs 1.34x instead.
    ///
    /// The reader also accepts the legacy `predicate_proto` array form, so an
    /// old coordinator still gets its predicate applied. The other direction
    /// (new coordinator, old worker) degrades rather than breaks: the old
    /// worker does not know the `_b64` key, sees no predicate, and ships
    /// unfiltered rows, which the doc above licenses because the coordinator
    /// keeps the authoritative `FilterExec`. No version bump is needed.
    #[serde(
        rename = "predicate_proto_b64",
        alias = "predicate_proto",
        default,
        skip_serializing_if = "Option::is_none",
        with = "predicate_wire"
    )]
    pub predicate_proto: Option<Vec<u8>>,
    /// Per-fragment row LIMIT hint (#233). When set, the worker may stop
    /// emitting rows early once this many have been produced for the fragment.
    ///
    /// The value is an over-approximation of the global LIMIT (global_skip +
    /// global_fetch). The coordinator still enforces the authoritative global
    /// limit above `DistributedScanExec`, so a per-fragment over-count is safe.
    /// Only populated when the path between the limit node and the scan is
    /// row-count-monotonic and order-insensitive. `None` otherwise.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Present only for a Hive external table scan.
    ///
    /// `skip_serializing_if` is what keeps every Iceberg ticket byte-identical
    /// to its v2 encoding, which matters because this field sits on the path
    /// every distributed Iceberg query already uses. That property is asserted
    /// by a test rather than trusted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hive: Option<HiveScanDescriptor>,
}

/// Wire codec for [`ScanTask::predicate_proto`]: writes base64, reads either
/// base64 (current) or a JSON byte array (pre-#432 coordinators).
mod predicate_wire {
    use base64::Engine as _;
    use serde::de::{self, Deserializer};
    use serde::{Deserialize, Serializer};

    const B64: base64::engine::general_purpose::GeneralPurpose =
        base64::engine::general_purpose::STANDARD;

    pub fn serialize<S: Serializer>(
        value: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => serializer.serialize_str(&B64.encode(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            B64(String),
            Raw(Vec<u8>),
        }

        match Option::<Wire>::deserialize(deserializer)? {
            None => Ok(None),
            Some(Wire::B64(text)) => B64
                .decode(text.as_bytes())
                .map(Some)
                .map_err(de::Error::custom),
            Some(Wire::Raw(bytes)) => Ok(Some(bytes)),
        }
    }
}

fn default_scan_task_version() -> u32 {
    // Old tickets without a version field are whole-file (v1).
    SCAN_TASK_VERSION_V1
}

/// Storage connection parameters a scan ticket carries (finding 109).
///
/// Groups the fields every task needs to open its files so the builder takes
/// one argument instead of five. Credentials default to
/// [`ScanCredentials::WorkerDefault`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanStorage {
    pub s3_endpoint: String,
    pub s3_region: String,
    pub s3_path_style: bool,
    pub s3_allow_http: bool,
    pub credentials: ScanCredentials,
}

/// Builder for [`ScanTask`] (finding 109 / #552).
///
/// The point of the builder is that `version` is DERIVED from the fields
/// present, never hand-set: [`ScanTaskBuilder::build`] computes it as the
/// maximum version any populated field requires
/// ([`ScanTask::required_version`]). A producer therefore cannot fill a
/// byte-range morsel field and leave `version: 1`, which is the mixed-fleet
/// case where an old worker silently scans the whole file per morsel.
///
/// The struct-literal surface of `ScanTask` stays public for now so existing
/// construction sites keep compiling; [`ScanTask::to_bytes`] runs
/// [`ScanTask::validate_payload`] so a literal that under-declares its
/// version fails at encode time rather than on the worker.
#[derive(Clone, Debug)]
pub struct ScanTaskBuilder {
    task: ScanTask,
}

impl ScanTaskBuilder {
    /// Row-group sub-range of the first file: `start` inclusive, `end`
    /// exclusive (`None` = through the last group). Requires v2.
    pub fn with_row_groups(mut self, start: u32, end: Option<u32>) -> Self {
        self.task.row_group_start = Some(start);
        self.task.row_group_end = end;
        self
    }

    /// Compressed byte sub-range of the first file: `start` inclusive, `end`
    /// exclusive. Requires v2.
    pub fn with_byte_range(mut self, start: u64, end: u64) -> Self {
        self.task.start_byte = Some(start);
        self.task.end_byte = Some(end);
        self
    }

    /// Stable morsel id for retry / dedup. Requires v2.
    pub fn with_morsel_id(mut self, morsel_id: impl Into<String>) -> Self {
        self.task.morsel_id = Some(morsel_id.into());
        self
    }

    /// Hive external-table descriptor. Requires v3.
    pub fn with_hive(mut self, desc: HiveScanDescriptor) -> Self {
        self.task.hive = Some(desc);
        self
    }

    /// Per-file sizes, parallel to the file list. Empty = unknown.
    pub fn with_file_sizes(mut self, sizes: Vec<u64>) -> Self {
        self.task.file_sizes_bytes = sizes;
        self
    }

    /// Column projection by name and, when known, by Iceberg field id
    /// (parallel vectors; an empty `field_ids` means name-based fallback).
    pub fn with_projection(mut self, columns: Vec<String>, field_ids: Vec<i32>) -> Self {
        self.task.projected_columns = columns;
        self.task.projected_field_ids = field_ids;
        self
    }

    /// Pushed-down predicate as datafusion-proto `Expr` bytes (#233).
    pub fn with_predicate_proto(mut self, predicate: Option<Vec<u8>>) -> Self {
        self.task.predicate_proto = predicate;
        self
    }

    /// Per-fragment row LIMIT hint (#233).
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.task.limit = limit;
        self
    }

    /// Ticket audience and expiry (finding 601). Both are part of the signed
    /// bytes; the signer normally fills them, so this is for tests and for
    /// producers that sign inline.
    pub fn with_audience(mut self, audience: impl Into<String>, expires_at: i64) -> Self {
        self.task.audience = audience.into();
        self.task.expires_at = expires_at;
        self
    }

    /// Finish the task, deriving `version` from the fields present.
    pub fn build(mut self) -> ScanTask {
        self.task.version = self.task.required_version();
        self.task
    }
}

impl std::fmt::Debug for ScanTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total_bytes: u64 = self.file_sizes_bytes.iter().sum();
        f.debug_struct("ScanTask")
            .field("version", &self.version)
            .field("fragment_id", &self.fragment_id)
            .field("morsel_id", &self.morsel_id)
            .field("data_file_paths", &self.data_file_paths)
            .field("total_bytes", &total_bytes)
            .field("row_group_start", &self.row_group_start)
            .field("row_group_end", &self.row_group_end)
            .field("projected_columns", &self.projected_columns)
            .field("s3_endpoint", &self.s3_endpoint)
            .field("s3_region", &self.s3_region)
            .field("credentials", &self.credentials)
            .field("audience", &self.audience)
            .field("expires_at", &self.expires_at)
            .field("s3_path_style", &self.s3_path_style)
            .field("s3_allow_http", &self.s3_allow_http)
            .field("has_predicate", &self.predicate_proto.is_some())
            .field("limit", &self.limit)
            .field("hive", &self.hive.as_ref().map(|h| h.format))
            .finish()
    }
}

/// Wire key the pre-#432 predicate rode under. It is a serde `alias`, so it
/// never appears in re-serialised output and must be whitelisted by the
/// unknown-field check.
const LEGACY_PREDICATE_KEY: &str = "predicate_proto";

impl ScanTask {
    /// Start building a task. `version` is derived at [`ScanTaskBuilder::build`].
    pub fn builder(
        fragment_id: impl Into<String>,
        data_file_paths: Vec<String>,
        storage: ScanStorage,
    ) -> ScanTaskBuilder {
        ScanTaskBuilder {
            task: ScanTask {
                version: SCAN_TASK_VERSION_V1,
                fragment_id: fragment_id.into(),
                morsel_id: None,
                data_file_paths,
                file_sizes_bytes: Vec::new(),
                row_group_start: None,
                row_group_end: None,
                start_byte: None,
                end_byte: None,
                projected_columns: Vec::new(),
                projected_field_ids: Vec::new(),
                s3_endpoint: storage.s3_endpoint,
                s3_region: storage.s3_region,
                credentials: storage.credentials,
                audience: String::new(),
                expires_at: 0,
                s3_path_style: storage.s3_path_style,
                s3_allow_http: storage.s3_allow_http,
                predicate_proto: None,
                limit: None,
                hive: None,
            },
        }
    }

    /// The lowest ticket version a worker must understand to honour every
    /// field this task populates. This is the single source of truth the
    /// builder and both validators share:
    ///
    /// - v2: any morsel field (`morsel_id`, `row_group_*`, `start_byte`,
    ///   `end_byte`);
    /// - v3: `hive`.
    ///
    /// `predicate_proto`, `limit`, `projected_field_ids`, `credentials`,
    /// `audience` and `expires_at` do not raise the version: an old worker
    /// that ignores them still returns correct rows (the coordinator keeps the
    /// authoritative Filter/Limit) or fails loudly (signature / credentials),
    /// never silently wrong data.
    pub fn required_version(&self) -> u32 {
        let mut required = SCAN_TASK_VERSION_V1;
        if self.is_morsel() {
            required = required.max(SCAN_TASK_VERSION_V2);
        }
        if self.hive.is_some() {
            required = required.max(SCAN_TASK_VERSION_V3);
        }
        required
    }

    /// Producer-side invariant: `version` must be at least what the populated
    /// fields require. Called from [`ScanTask::to_bytes`] so a hand-built
    /// literal that under-declares fails at encode time on the coordinator.
    pub fn validate_payload(&self) -> Result<(), String> {
        let required = self.required_version();
        if self.version < required {
            let fields = self.fields_requiring_version(required);
            return Err(format!(
                "ScanTask {} declares version {} but carries v{required} fields [{}]; \
                 build it with ScanTask::builder so the version is derived",
                self.fragment_id,
                self.version,
                fields.join(", ")
            ));
        }
        Ok(())
    }

    /// Names of the populated fields that need `version >= required`.
    fn fields_requiring_version(&self, required: u32) -> Vec<&'static str> {
        let mut fields = Vec::new();
        if required >= SCAN_TASK_VERSION_V2 {
            if self.morsel_id.is_some() {
                fields.push("morsel_id");
            }
            if self.row_group_start.is_some() {
                fields.push("row_group_start");
            }
            if self.row_group_end.is_some() {
                fields.push("row_group_end");
            }
            if self.start_byte.is_some() {
                fields.push("start_byte");
            }
            if self.end_byte.is_some() {
                fields.push("end_byte");
            }
        }
        if required >= SCAN_TASK_VERSION_V3 && self.hive.is_some() {
            fields.push("hive");
        }
        fields
    }

    /// Consumer-side check a worker runs after decoding, given the newest
    /// version it implements (normally [`SCAN_TASK_VERSION_CURRENT`]).
    ///
    /// Rejects, in order: a version above `supported_max` (a newer
    /// coordinator; `validate_version` semantics), a version below v1, and a
    /// payload whose fields need a higher version than declared (a producer
    /// that hand-set `version`, which on an OLDER worker would have meant
    /// silently duplicated rows). `unknown_fields` are keys serde dropped on
    /// decode ([`ScanTask::from_bytes_with_unknown`]); any present means the
    /// coordinator filled fields this build does not know, so the task is
    /// refused rather than executed with data missing.
    pub fn validate_for_worker(
        &self,
        supported_max: u32,
        unknown_fields: &[String],
    ) -> Result<(), String> {
        if self.version > supported_max || self.version < SCAN_TASK_VERSION_V1 {
            return Err(format!(
                "unsupported ScanTask version {}; this worker supports \
                 {SCAN_TASK_VERSION_V1}..={supported_max}",
                self.version
            ));
        }
        self.validate_payload()?;
        if !unknown_fields.is_empty() {
            return Err(format!(
                "ScanTask {} (version {}) carries fields this worker does not know [{}]; \
                 refusing rather than scanning with them dropped",
                self.fragment_id,
                self.version,
                unknown_fields.join(", ")
            ));
        }
        Ok(())
    }

    /// Decode and report the keys serde silently dropped.
    ///
    /// Design choice (finding 109 option 3): `ScanTask` cannot grow a
    /// `#[serde(flatten)]` catch-all field without breaking every struct
    /// literal in the coordinator and worker, and `deny_unknown_fields` is a
    /// static attribute that cannot sit behind a runtime flag. So the check
    /// is done outside the struct: the input's top-level keys minus the keys
    /// of the re-serialised task (plus the legacy `predicate_proto` alias) are
    /// the ones no field consumed. Re-serialising is cheap relative to a scan
    /// and cannot drift from the struct definition the way a static key list
    /// would.
    pub fn from_bytes_with_unknown(bytes: &[u8]) -> serde_json::Result<(Self, Vec<String>)> {
        let task: Self = serde_json::from_slice(bytes)?;
        let input: serde_json::Value = serde_json::from_slice(bytes)?;
        let known = serde_json::to_value(&task)?;
        let unknown = match (input.as_object(), known.as_object()) {
            (Some(input), Some(known)) => input
                .keys()
                .filter(|k| !known.contains_key(*k) && k.as_str() != LEGACY_PREDICATE_KEY)
                .cloned()
                .collect(),
            _ => Vec::new(),
        };
        Ok((task, unknown))
    }

    /// One-call worker entry point: decode, detect dropped keys, and run
    /// [`ScanTask::validate_for_worker`] against `supported_max`.
    pub fn decode_for_worker(bytes: &[u8], supported_max: u32) -> Result<Self, String> {
        let (task, unknown) = Self::from_bytes_with_unknown(bytes)
            .map_err(|e| format!("Invalid ScanTask ticket: {e}"))?;
        task.validate_for_worker(supported_max, &unknown)?;
        Ok(task)
    }

    /// Serialize to JSON bytes for Flight Ticket body.
    ///
    /// Runs [`ScanTask::validate_payload`] first: a task whose fields need a
    /// higher version than it declares is refused here, on the producer.
    pub fn to_bytes(&self) -> serde_json::Result<Vec<u8>> {
        self.validate_payload()
            .map_err(<serde_json::Error as serde::ser::Error>::custom)?;
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }

    /// Reject tickets whose version this worker does not understand.
    pub fn validate_version(&self) -> Result<(), String> {
        match self.version {
            SCAN_TASK_VERSION_V1 | SCAN_TASK_VERSION_V2 | SCAN_TASK_VERSION_V3 => Ok(()),
            other => Err(format!(
                "unsupported ScanTask version {other}; this worker supports \
                 {SCAN_TASK_VERSION_V1}..={SCAN_TASK_VERSION_CURRENT}"
            )),
        }
    }

    /// True when this task is restricted to a row-group or byte sub-range of
    /// the first file (a Phase 2 morsel).
    pub fn is_morsel(&self) -> bool {
        self.row_group_start.is_some()
            || self.row_group_end.is_some()
            || self.start_byte.is_some()
            || self.end_byte.is_some()
            || self.morsel_id.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_with_predicate(predicate: Option<Vec<u8>>) -> ScanTask {
        ScanTask {
            version: SCAN_TASK_VERSION_V2,
            morsel_id: None,
            row_group_start: None,
            row_group_end: None,
            start_byte: None,
            end_byte: None,
            fragment_id: "frag-b64".to_string(),
            data_file_paths: vec!["s3://bucket/data/file1.parquet".to_string()],
            file_sizes_bytes: vec![1024],
            projected_columns: vec!["lo_partkey".to_string()],
            projected_field_ids: vec![1],
            s3_endpoint: "http://localhost:9000".to_string(),
            s3_region: "us-east-1".to_string(),
            credentials: ScanCredentials::static_keys(
                "key".to_string(),
                "secret".to_string(),
                String::new(),
            ),
            audience: String::new(),
            expires_at: 0,
            s3_path_style: true,
            s3_allow_http: true,
            predicate_proto: predicate,
            limit: None,
            hive: None,
        }
    }

    fn hive_descriptor() -> HiveScanDescriptor {
        HiveScanDescriptor {
            format: HiveScanFormat::Csv,
            delimiter: b',',
            quote: Some(b'"'),
            escape: None,
            has_header: true,
            partition_values: BTreeMap::from([
                ("dt".to_string(), "2024-01-01".to_string()),
                ("region".to_string(), "eu".to_string()),
            ]),
            // Deliberately NOT alphabetical: this is the order the projection
            // indexes by, and a BTreeMap would sort it to dt,region.
            partition_columns: vec!["region".to_string(), "dt".to_string()],
            full_schema_json: "{\"fields\":[]}".to_string(),
        }
    }

    /// The guard the whole of cycle 5 rests on. Adding an optional field to a
    /// message every distributed Iceberg query already sends must not change
    /// one byte of those messages.
    #[test]
    fn an_iceberg_task_is_byte_identical_to_its_v2_encoding() {
        let task = task_with_predicate(None);
        let bytes = task.to_bytes().expect("serialize");
        let json = String::from_utf8(bytes).expect("utf8");
        assert!(
            !json.contains("hive"),
            "an Iceberg ticket must not carry the hive key at all, got: {json}"
        );
        // And it still declares v2: the version bumps only when a descriptor
        // is actually present, so an old worker keeps accepting these.
        assert!(json.contains("\"version\":2"), "got: {json}");
    }

    #[test]
    fn a_hive_descriptor_round_trips_and_keeps_the_declared_column_order() {
        let mut task = task_with_predicate(None);
        task.version = SCAN_TASK_VERSION_V3;
        task.hive = Some(hive_descriptor());

        let bytes = task.to_bytes().expect("serialize");
        let back = ScanTask::from_bytes(&bytes).expect("deserialize");
        let hive = back.hive.expect("descriptor survives the round trip");

        assert_eq!(hive, hive_descriptor());
        assert_eq!(
            hive.partition_columns,
            vec!["region".to_string(), "dt".to_string()],
            "declared order is load-bearing: the projection indexes by position, \
             so sorting it silently swaps two partition columns"
        );
    }

    #[test]
    fn version_three_is_accepted_and_four_is_not() {
        let mut task = task_with_predicate(None);
        for v in [
            SCAN_TASK_VERSION_V1,
            SCAN_TASK_VERSION_V2,
            SCAN_TASK_VERSION_V3,
        ] {
            task.version = v;
            assert!(task.validate_version().is_ok(), "v{v} must be accepted");
        }
        task.version = 4;
        assert!(
            task.validate_version().is_err(),
            "an unknown version must be refused, not read as the newest known one"
        );
    }

    fn storage() -> ScanStorage {
        ScanStorage {
            s3_endpoint: "http://localhost:9000".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_path_style: true,
            s3_allow_http: true,
            credentials: ScanCredentials::static_keys("key", "secret", ""),
        }
    }

    fn built(builder: ScanTaskBuilder) -> ScanTask {
        builder.build()
    }

    // ---- finding 109: version is derived, not set ----

    #[test]
    fn builder_whole_file_task_is_v1() {
        let task = built(ScanTask::builder(
            "f",
            vec!["s3://b/f.parquet".to_string()],
            storage(),
        ));
        assert_eq!(task.version, SCAN_TASK_VERSION_V1);
        assert_eq!(task.required_version(), SCAN_TASK_VERSION_V1);
        assert!(task.validate_payload().is_ok());
        assert_eq!(task.s3_endpoint, "http://localhost:9000");
        assert!(task.s3_path_style);
    }

    #[test]
    fn builder_byte_range_reports_v2() {
        let task = built(
            ScanTask::builder("f", vec!["s3://b/f.parquet".to_string()], storage())
                .with_byte_range(0, 4096),
        );
        assert_eq!(task.version, SCAN_TASK_VERSION_V2);
        assert_eq!(task.start_byte, Some(0));
        assert_eq!(task.end_byte, Some(4096));
        assert!(task.validate_payload().is_ok());
    }

    #[test]
    fn builder_row_groups_and_morsel_id_report_v2() {
        let rg = built(
            ScanTask::builder("f", vec!["s3://b/f.parquet".to_string()], storage())
                .with_row_groups(2, None),
        );
        assert_eq!(rg.version, SCAN_TASK_VERSION_V2);
        let id = built(
            ScanTask::builder("f", vec!["s3://b/f.parquet".to_string()], storage())
                .with_morsel_id("m-1"),
        );
        assert_eq!(id.version, SCAN_TASK_VERSION_V2);
    }

    #[test]
    fn builder_hive_reports_v3_even_with_morsel_fields() {
        let task = built(
            ScanTask::builder("f", vec!["s3://b/f.csv".to_string()], storage())
                .with_byte_range(0, 10)
                .with_hive(hive_descriptor()),
        );
        assert_eq!(task.version, SCAN_TASK_VERSION_V3);
        assert!(task.validate_payload().is_ok());
    }

    /// Optional fields that an old worker can ignore without returning wrong
    /// rows must not raise the version, or every Iceberg ticket would stop
    /// being accepted by v1 workers for no correctness gain.
    #[test]
    fn builder_predicate_limit_and_field_ids_stay_v1() {
        let task = built(
            ScanTask::builder("f", vec!["s3://b/f.parquet".to_string()], storage())
                .with_projection(vec!["id".to_string()], vec![1])
                .with_predicate_proto(Some(vec![1, 2, 3]))
                .with_limit(Some(10))
                .with_file_sizes(vec![1024])
                .with_audience("http://w1:50052", 1_700_000_000),
        );
        assert_eq!(task.version, SCAN_TASK_VERSION_V1);
        assert_eq!(task.projected_field_ids, vec![1]);
        assert_eq!(task.limit, Some(10));
        assert_eq!(task.audience, "http://w1:50052");
    }

    /// The acceptance criterion from the review: a hand-set `version: 1`
    /// with a byte range is exactly the mixed-fleet duplicate-rows case.
    #[test]
    fn hand_set_v1_with_byte_range_fails_validate_payload_and_to_bytes() {
        let mut task = task_with_predicate(None);
        task.version = SCAN_TASK_VERSION_V1;
        task.start_byte = Some(0);
        let err = task
            .validate_payload()
            .expect_err("v1 with start_byte must fail");
        assert!(
            err.contains("start_byte"),
            "names the offending field: {err}"
        );
        assert!(
            err.contains("version 1"),
            "names the declared version: {err}"
        );
        assert!(
            task.to_bytes().is_err(),
            "to_bytes must refuse to encode an under-declared task"
        );
    }

    #[test]
    fn hand_set_v2_with_hive_fails_validate_payload() {
        let mut task = task_with_predicate(None);
        task.version = SCAN_TASK_VERSION_V2;
        task.hive = Some(hive_descriptor());
        let err = task.validate_payload().expect_err("v2 with hive must fail");
        assert!(err.contains("hive"), "{err}");
        task.version = SCAN_TASK_VERSION_V3;
        assert!(task.validate_payload().is_ok());
    }

    /// Over-declaring is fine: a v2 ticket with no morsel fields is what
    /// today's coordinator sends for whole-file scans on the morsel path.
    #[test]
    fn over_declared_version_is_accepted() {
        let task = task_with_predicate(None);
        assert_eq!(task.version, SCAN_TASK_VERSION_V2);
        assert_eq!(task.required_version(), SCAN_TASK_VERSION_V1);
        assert!(task.validate_payload().is_ok());
        assert!(task
            .validate_for_worker(SCAN_TASK_VERSION_CURRENT, &[])
            .is_ok());
    }

    // ---- finding 109: consumer side ----

    #[test]
    fn worker_rejects_under_declared_ticket_from_the_wire() {
        // Bypass to_bytes (which would refuse) to forge what a buggy or older
        // coordinator puts on the wire.
        let mut value: serde_json::Value =
            serde_json::from_slice(&task_with_predicate(None).to_bytes().unwrap()).unwrap();
        value["version"] = serde_json::json!(1);
        value["start_byte"] = serde_json::json!(0);
        value["end_byte"] = serde_json::json!(100);
        let wire = serde_json::to_vec(&value).unwrap();

        let err = ScanTask::decode_for_worker(&wire, SCAN_TASK_VERSION_CURRENT)
            .expect_err("worker must refuse a v1 ticket carrying byte-range fields");
        assert!(err.contains("start_byte"), "{err}");
    }

    #[test]
    fn worker_rejects_version_above_its_supported_max() {
        let task = task_with_predicate(None); // v2
        let err = task
            .validate_for_worker(SCAN_TASK_VERSION_V1, &[])
            .expect_err("a v1-only worker must refuse a v2 ticket");
        assert!(err.contains("unsupported ScanTask version 2"), "{err}");
        assert!(err.contains("1..=1"), "{err}");
    }

    #[test]
    fn worker_rejects_version_zero() {
        let mut task = task_with_predicate(None);
        task.version = 0;
        assert!(task
            .validate_for_worker(SCAN_TASK_VERSION_CURRENT, &[])
            .is_err());
    }

    #[test]
    fn unknown_keys_are_reported_and_refused_by_the_worker() {
        let mut value: serde_json::Value =
            serde_json::from_slice(&task_with_predicate(None).to_bytes().unwrap()).unwrap();
        value["future_field"] = serde_json::json!({"x": 1});
        let wire = serde_json::to_vec(&value).unwrap();

        let (task, unknown) = ScanTask::from_bytes_with_unknown(&wire).unwrap();
        assert_eq!(unknown, vec!["future_field".to_string()]);
        assert_eq!(task.fragment_id, "frag-b64");

        let err = ScanTask::decode_for_worker(&wire, SCAN_TASK_VERSION_CURRENT)
            .expect_err("unknown keys mean the coordinator is newer; refuse");
        assert!(err.contains("future_field"), "{err}");
    }

    /// Keys that a field consumed under a different wire name (the legacy
    /// `predicate_proto` alias) and keys omitted by `skip_serializing_if`
    /// when present must not be reported as unknown.
    #[test]
    fn known_aliases_and_optional_keys_are_not_unknown() {
        let mut task = task_with_predicate(Some(vec![1, 2]));
        task.version = SCAN_TASK_VERSION_V3;
        task.hive = Some(hive_descriptor());
        let mut value: serde_json::Value =
            serde_json::from_slice(&task.to_bytes().unwrap()).unwrap();
        // Rewrite the predicate to the legacy array form under the alias key.
        value.as_object_mut().unwrap().remove("predicate_proto_b64");
        value["predicate_proto"] = serde_json::json!([1, 2]);
        let wire = serde_json::to_vec(&value).unwrap();

        let (decoded, unknown) = ScanTask::from_bytes_with_unknown(&wire).unwrap();
        assert!(unknown.is_empty(), "got unknown keys: {unknown:?}");
        assert_eq!(decoded.predicate_proto, Some(vec![1, 2]));
        assert!(decoded.hive.is_some());
        assert!(ScanTask::decode_for_worker(&wire, SCAN_TASK_VERSION_CURRENT).is_ok());
    }

    #[test]
    fn decode_for_worker_accepts_a_builder_task_round_trip() {
        let task = built(
            ScanTask::builder("f", vec!["s3://b/f.parquet".to_string()], storage())
                .with_byte_range(0, 4096)
                .with_morsel_id("m-1"),
        );
        let wire = task.to_bytes().unwrap();
        let back = ScanTask::decode_for_worker(&wire, SCAN_TASK_VERSION_CURRENT).unwrap();
        assert_eq!(back.version, SCAN_TASK_VERSION_V2);
        assert_eq!(back.morsel_id.as_deref(), Some("m-1"));
    }

    /// A v2 ticket from an older coordinator has no `hive` key at all. It must
    /// deserialize as a non-Hive task rather than failing.
    #[test]
    fn a_v2_ticket_without_the_key_deserializes_as_non_hive() {
        let json = r#"{"version":2,"fragment_id":"f","data_file_paths":[],
            "projected_columns":[],"s3_endpoint":"","s3_region":"",
            "s3_access_key":"","s3_secret_key":"","s3_session_token":"",
            "s3_path_style":false,"s3_allow_http":false}"#;
        let task = ScanTask::from_bytes(json.as_bytes()).expect("v2 ticket still parses");
        assert!(task.hive.is_none());
    }

    /// The predicate must ride as base64, not as a JSON array of decimal
    /// numbers. Measured expansion for the array form is 2.9x, which put a
    /// 1.5 MB predicate over tonic's 4 MiB decode ceiling (#432). base64 is
    /// 1.34x, so the encoded ticket must stay well under 1.5x of the payload.
    #[test]
    fn predicate_rides_the_wire_as_base64() {
        // Bytes spanning the whole range: the array form is most expensive for
        // three-digit values, which is what real proto payloads are full of.
        let raw: Vec<u8> = (0..64_000usize).map(|i| (i % 256) as u8).collect();
        let task = task_with_predicate(Some(raw.clone()));
        let bytes = task.to_bytes().unwrap();
        let json = String::from_utf8(bytes.clone()).unwrap();

        assert!(
            json.contains("\"predicate_proto_b64\""),
            "predicate must be written under the base64 key"
        );
        assert!(
            !json.contains("predicate_proto\":["),
            "predicate must not be written as a JSON number array"
        );
        let ratio = bytes.len() as f64 / raw.len() as f64;
        assert!(
            ratio < 1.5,
            "encoded ticket is {ratio:.2}x the predicate; base64 should cost ~1.34x"
        );

        let decoded = ScanTask::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.predicate_proto, Some(raw));
    }

    /// A pre-#432 coordinator writes `predicate_proto` as a number array. A
    /// current worker must still apply that predicate rather than silently
    /// scanning unfiltered during a rolling upgrade.
    #[test]
    fn legacy_number_array_predicate_still_decodes() {
        let mut value: serde_json::Value =
            serde_json::from_slice(&task_with_predicate(None).to_bytes().unwrap()).unwrap();
        value["predicate_proto"] = serde_json::json!([10, 20, 30, 255]);
        let legacy = serde_json::to_vec(&value).unwrap();

        let decoded = ScanTask::from_bytes(&legacy).unwrap();
        assert_eq!(decoded.predicate_proto, Some(vec![10, 20, 30, 255]));
    }

    /// No predicate means no key at all, so an old worker's `serde(default)`
    /// sees None instead of failing on a null of the wrong shape.
    #[test]
    fn absent_predicate_writes_no_key() {
        let bytes = task_with_predicate(None).to_bytes().unwrap();
        let json = String::from_utf8(bytes.clone()).unwrap();
        assert!(!json.contains("predicate_proto"));
        assert!(ScanTask::from_bytes(&bytes)
            .unwrap()
            .predicate_proto
            .is_none());
    }

    #[test]
    fn test_scan_task_roundtrip() {
        let task = ScanTask {
            version: SCAN_TASK_VERSION_V1,
            morsel_id: None,
            row_group_start: None,
            row_group_end: None,
            start_byte: None,
            end_byte: None,
            fragment_id: "frag-001".to_string(),
            data_file_paths: vec![
                "s3://bucket/data/file1.parquet".to_string(),
                "s3://bucket/data/file2.parquet".to_string(),
            ],
            file_sizes_bytes: vec![],
            projected_columns: vec!["id".to_string(), "name".to_string()],
            projected_field_ids: vec![1, 2],
            s3_endpoint: "http://localhost:9000".to_string(),
            s3_region: "us-east-1".to_string(),
            credentials: ScanCredentials::static_keys(
                "testadmin".to_string(),
                "testadmin".to_string(),
                String::new(),
            ),
            audience: String::new(),
            expires_at: 0,
            s3_path_style: true,
            s3_allow_http: true,
            predicate_proto: None,
            limit: None,
            hive: None,
        };

        let bytes = task.to_bytes().unwrap();
        let decoded = ScanTask::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.fragment_id, "frag-001");
        assert_eq!(decoded.data_file_paths.len(), 2);
        assert_eq!(decoded.projected_columns, vec!["id", "name"]);
        assert_eq!(decoded.projected_field_ids, vec![1, 2]);
        assert!(decoded.s3_path_style);
        assert!(decoded.s3_allow_http);
    }

    #[test]
    fn test_scan_task_empty_projection_means_all_columns() {
        let task = ScanTask {
            version: SCAN_TASK_VERSION_V1,
            morsel_id: None,
            row_group_start: None,
            row_group_end: None,
            start_byte: None,
            end_byte: None,
            fragment_id: "frag-002".to_string(),
            data_file_paths: vec!["s3://bucket/data/file1.parquet".to_string()],
            file_sizes_bytes: vec![],
            projected_columns: vec![],
            projected_field_ids: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            credentials: ScanCredentials::default(),
            audience: String::new(),
            expires_at: 0,
            s3_path_style: false,
            s3_allow_http: true,
            predicate_proto: None,
            limit: None,
            hive: None,
        };

        let bytes = task.to_bytes().unwrap();
        let decoded = ScanTask::from_bytes(&bytes).unwrap();
        assert!(decoded.projected_columns.is_empty());
        assert!(decoded.projected_field_ids.is_empty());
    }

    #[test]
    fn test_scan_task_old_coordinator_omits_field_ids() {
        // Old coordinators that predate #43 send ScanTask JSON without the
        // projected_field_ids key. Serde's default makes the decode succeed
        // with an empty vector; the worker falls back to name-based
        // projection in that case.
        let json = r#"{
            "fragment_id": "frag-old",
            "data_file_paths": ["s3://b/f.parquet"],
            "file_sizes_bytes": [],
            "projected_columns": ["id"],
            "s3_endpoint": "",
            "s3_region": "",
            "s3_access_key": "",
            "s3_secret_key": "",
            "s3_session_token": "",
            "s3_path_style": false,
            "s3_allow_http": false
        }"#;
        let decoded: ScanTask = serde_json::from_str(json).unwrap();
        assert_eq!(decoded.projected_columns, vec!["id"]);
        assert!(decoded.projected_field_ids.is_empty());
    }

    #[test]
    fn test_debug_redacts_credentials() {
        let task = ScanTask {
            version: SCAN_TASK_VERSION_V1,
            morsel_id: None,
            row_group_start: None,
            row_group_end: None,
            start_byte: None,
            end_byte: None,
            fragment_id: "frag-001".to_string(),
            data_file_paths: vec![],
            file_sizes_bytes: vec![],
            projected_columns: vec![],
            projected_field_ids: vec![],
            s3_endpoint: "http://localhost:9000".to_string(),
            s3_region: "us-east-1".to_string(),
            // nosemgrep: opt.aikido.semgrep-rules.generic.secrets.gitleaks.aws-access-token -- AWS's documented example key, test fixture
            credentials: ScanCredentials::static_keys(
                "AKIAIOSFODNN7EXAMPLE".to_string(),
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                "session-token-value".to_string(),
            ),
            audience: String::new(),
            expires_at: 0,
            s3_path_style: true,
            s3_allow_http: false,
            predicate_proto: None,
            limit: None,
            hive: None,
        };
        let debug_output = format!("{task:?}");
        // nosemgrep: opt.aikido.semgrep-rules.generic.secrets.gitleaks.aws-access-token -- AWS's documented example key, asserting it is redacted
        assert!(!debug_output.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!debug_output.contains("wJalrXUtnFEMI"));
        assert!(!debug_output.contains("session-token-value"));
        assert!(debug_output.contains("[REDACTED]"));
    }

    #[test]
    fn test_scan_task_predicate_and_limit_roundtrip() {
        // #233: the pushed-down predicate (serialized Expr bytes) and the
        // per-fragment limit hint must survive the JSON encode/decode that
        // backs the Flight ticket.
        let predicate_bytes = vec![1u8, 2, 3, 4, 5];
        let task = ScanTask {
            version: SCAN_TASK_VERSION_V1,
            morsel_id: None,
            row_group_start: None,
            row_group_end: None,
            start_byte: None,
            end_byte: None,
            fragment_id: "frag-pred".to_string(),
            data_file_paths: vec!["s3://bucket/data/file1.parquet".to_string()],
            file_sizes_bytes: vec![1024],
            projected_columns: vec!["id".to_string(), "payload".to_string()],
            projected_field_ids: vec![1, 2],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            credentials: ScanCredentials::default(),
            audience: String::new(),
            expires_at: 0,
            s3_path_style: false,
            s3_allow_http: false,
            predicate_proto: Some(predicate_bytes.clone()),
            limit: Some(42),
            hive: None,
        };

        let bytes = task.to_bytes().unwrap();
        let decoded = ScanTask::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.predicate_proto, Some(predicate_bytes));
        assert_eq!(decoded.limit, Some(42));
    }

    #[test]
    fn test_scan_task_old_coordinator_omits_predicate_and_limit() {
        // Coordinators predating #233 send no predicate_proto / limit keys.
        // Serde defaults both to None so the worker simply ships every row
        // (correct: the coordinator's authoritative Filter/Limit still apply).
        let json = r#"{
            "fragment_id": "frag-old233",
            "data_file_paths": ["s3://b/f.parquet"],
            "file_sizes_bytes": [],
            "projected_columns": ["id"],
            "projected_field_ids": [1],
            "s3_endpoint": "",
            "s3_region": "",
            "s3_access_key": "",
            "s3_secret_key": "",
            "s3_session_token": "",
            "s3_path_style": false,
            "s3_allow_http": false
        }"#;
        let decoded: ScanTask = serde_json::from_str(json).unwrap();
        assert!(decoded.predicate_proto.is_none());
        assert!(decoded.limit.is_none());
    }
}
