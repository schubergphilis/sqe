use serde::{Deserialize, Serialize};

use crate::scan_morsel::{SCAN_TASK_VERSION_CURRENT, SCAN_TASK_VERSION_V1, SCAN_TASK_VERSION_V2};

/// Lightweight message sent from coordinator to worker describing
/// which Parquet files to scan and how to access them.
///
/// Workers receive this as a JSON-encoded Flight Ticket body.
/// S3 credentials are included so workers don't need Polaris access.
#[derive(Clone, Serialize, Deserialize)]
pub struct ScanTask {
    /// Ticket encoding version. `1` = whole-file (pre-morsel); `2` = optional
    /// row-group / byte-range morsel fields. Workers reject unsupported
    /// versions. Missing field deserializes as v1 for old coordinators.
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
    /// S3 access key (vended or static).
    pub s3_access_key: String,
    /// S3 secret key.
    pub s3_secret_key: String,
    /// S3 session token (from credential vending, empty if static).
    pub s3_session_token: String,
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
            Some(Wire::B64(text)) => B64.decode(text.as_bytes()).map(Some).map_err(de::Error::custom),
            Some(Wire::Raw(bytes)) => Ok(Some(bytes)),
        }
    }
}

fn default_scan_task_version() -> u32 {
    // Old tickets without a version field are whole-file (v1).
    SCAN_TASK_VERSION_V1
}

impl std::fmt::Debug for ScanTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let session_token_display = if self.s3_session_token.is_empty() {
            "[empty]"
        } else {
            "[REDACTED]"
        };
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
            .field("s3_access_key", &"[REDACTED]")
            .field("s3_secret_key", &"[REDACTED]")
            .field("s3_session_token", &session_token_display)
            .field("s3_path_style", &self.s3_path_style)
            .field("s3_allow_http", &self.s3_allow_http)
            .field("has_predicate", &self.predicate_proto.is_some())
            .field("limit", &self.limit)
            .finish()
    }
}

impl ScanTask {
    /// Serialize to JSON bytes for Flight Ticket body.
    pub fn to_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }

    /// Reject tickets whose version this worker does not understand.
    pub fn validate_version(&self) -> Result<(), String> {
        match self.version {
            SCAN_TASK_VERSION_V1 | SCAN_TASK_VERSION_V2 => Ok(()),
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
            s3_access_key: "key".to_string(),
            s3_secret_key: "secret".to_string(),
            s3_session_token: String::new(),
            s3_path_style: true,
            s3_allow_http: true,
            predicate_proto: predicate,
            limit: None,
        }
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
        assert!(ScanTask::from_bytes(&bytes).unwrap().predicate_proto.is_none());
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
            s3_access_key: "testadmin".to_string(),
            s3_secret_key: "testadmin".to_string(),
            s3_session_token: String::new(),
            s3_path_style: true,
            s3_allow_http: true,
            predicate_proto: None,
            limit: None,
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
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: false,
            s3_allow_http: true,
            predicate_proto: None,
            limit: None,
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
            s3_access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            s3_secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            s3_session_token: "session-token-value".to_string(),
            s3_path_style: true,
            s3_allow_http: false,
            predicate_proto: None,
            limit: None,
        };
        let debug_output = format!("{task:?}");
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
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: false,
            s3_allow_http: false,
            predicate_proto: Some(predicate_bytes.clone()),
            limit: Some(42),
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
