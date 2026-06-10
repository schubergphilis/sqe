use serde::{Deserialize, Serialize};

/// Lightweight message sent from coordinator to worker describing
/// which Parquet files to scan and how to access them.
///
/// Workers receive this as a JSON-encoded Flight Ticket body.
/// S3 credentials are included so workers don't need Polaris access.
#[derive(Clone, Serialize, Deserialize)]
pub struct ScanTask {
    /// Unique identifier for this fragment.
    pub fragment_id: String,
    /// S3 URLs of Parquet data files to scan.
    pub data_file_paths: Vec<String>,
    /// Size in bytes per file, parallel to `data_file_paths`.
    /// Empty if sizes were not available at planning time — callers should
    /// treat an empty vec as "unknown" and fall back to file-count cost.
    #[serde(default)]
    pub file_sizes_bytes: Vec<u64>,
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
    #[serde(default)]
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

impl std::fmt::Debug for ScanTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let session_token_display = if self.s3_session_token.is_empty() {
            "[empty]"
        } else {
            "[REDACTED]"
        };
        let total_bytes: u64 = self.file_sizes_bytes.iter().sum();
        f.debug_struct("ScanTask")
            .field("fragment_id", &self.fragment_id)
            .field("data_file_paths", &self.data_file_paths)
            .field("total_bytes", &total_bytes)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_task_roundtrip() {
        let task = ScanTask {
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
