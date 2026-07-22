//! Wire types + signing for the distributed `compact_file_group` worker RPC
//! (Phase 4c Task 2).
//!
//! Later tasks build on this: Task 4 (coordinator job runner) constructs and
//! signs a [`CompactGroupRequest`] per file group and sends it to a worker's
//! `do_action("compact_file_group")`; Task 3 (worker action) decodes,
//! [`verify`]s the signature, executes the rewrite, and streams back
//! [`CompactGroupFrame`]s. Both sides depend on `sqe-compaction`, so the
//! types live here rather than in `sqe-coordinator` or `sqe-worker` (a
//! dependency from the worker to the coordinator, or vice versa, would be a
//! cycle).
//!
//! Serialization mirrors `sqe_planner::ScanTask`: JSON via `to_bytes` /
//! `from_bytes`.
//!
//! ## Signing
//!
//! `sign`/`verify` implement the same scheme already used for scan tickets
//! (issue #206, see `sqe_coordinator::distributed_scan::sign_ticket` and
//! `sqe_worker::flight_service::FlightServiceImpl::verify_scan_signature`):
//! HMAC-SHA256 over the raw wire bytes, hex-encoded, checked with a
//! constant-time comparison. Signing the wire bytes directly (rather than a
//! re-serialized struct) means the tag covers exactly what gets decoded,
//! including the embedded S3 credentials.
//!
//! This is a **parallel implementation**, not a refactor of the existing
//! scan-ticket functions: those two functions are private to their crates
//! and are not exported for reuse, and copying their few lines here avoids
//! any risk of changing already-shipped scan-ticket signing behavior. Task 3
//! and Task 4 are expected to call `wire::sign` / `wire::verify` for the
//! compaction RPC; the scan-ticket path is untouched.

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::rewrite::SortSpec;

/// S3 connection details a worker needs to build a `FileIO` / object store
/// for a compaction group. Field set mirrors `sqe_planner::ScanTask`'s
/// flattened `s3_*` fields exactly (same names, minus the `s3_` prefix since
/// this struct is already nested under `CompactGroupRequest::s3`), so a
/// coordinator that already has those values on hand (e.g. from
/// credential vending) can construct this with a straight field-for-field
/// copy.
///
/// `sqe_planner::ScanTask` embeds its S3 fields flat on the struct itself
/// (not as a nested reusable type), so there is nothing to import here --
/// this is a parallel definition with the identical field set, not a
/// refactor of `ScanTask`.
#[derive(Clone, Serialize, Deserialize)]
pub struct S3Conn {
    /// S3 endpoint URL.
    pub endpoint: String,
    /// S3 region.
    pub region: String,
    /// S3 access key (vended or static).
    pub access_key: String,
    /// S3 secret key.
    pub secret_key: String,
    /// S3 session token (from credential vending, empty if static).
    pub session_token: String,
    /// Whether to use path-style S3 access (required for most S3-compatible
    /// endpoints).
    pub path_style: bool,
    /// Allow plaintext HTTP for S3 endpoints. Only enable for dev/test (e.g.
    /// MinIO).
    pub allow_http: bool,
}

impl std::fmt::Debug for S3Conn {
    /// Redacts credentials, matching `ScanTask`'s `Debug` impl: secrets must
    /// never land in a worker/coordinator log line via a stray `{:?}`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let session_token_display = if self.session_token.is_empty() {
            "[empty]"
        } else {
            "[REDACTED]"
        };
        f.debug_struct("S3Conn")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key", &"[REDACTED]")
            .field("secret_key", &"[REDACTED]")
            .field("session_token", &session_token_display)
            .field("path_style", &self.path_style)
            .field("allow_http", &self.allow_http)
            .finish()
    }
}

/// Serializable form of [`SortSpec`] for the wire. Mirrors the enum exactly
/// (`Columns` as `(column, ascending)` pairs applied in order, `ZOrder` as a
/// column list) since `SortSpec` itself does not derive `Serialize`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SortSpecWire {
    /// (column, ascending) pairs, applied in order.
    Columns(Vec<(String, bool)>),
    /// Z-order clustering across these columns.
    ZOrder(Vec<String>),
}

impl SortSpecWire {
    /// Rebuilds the in-process [`SortSpec`] a worker needs to drive
    /// `sort_group_stream` / `rewrite_group`.
    pub fn to_sort_spec(&self) -> SortSpec {
        match self {
            SortSpecWire::Columns(cols) => SortSpec::Columns(cols.clone()),
            SortSpecWire::ZOrder(cols) => SortSpec::ZOrder(cols.clone()),
        }
    }
}

impl From<&SortSpec> for SortSpecWire {
    fn from(spec: &SortSpec) -> Self {
        match spec {
            SortSpec::Columns(cols) => SortSpecWire::Columns(cols.clone()),
            SortSpec::ZOrder(cols) => SortSpecWire::ZOrder(cols.clone()),
        }
    }
}

impl From<SortSpec> for SortSpecWire {
    fn from(spec: SortSpec) -> Self {
        match spec {
            SortSpec::Columns(cols) => SortSpecWire::Columns(cols),
            SortSpec::ZOrder(cols) => SortSpecWire::ZOrder(cols),
        }
    }
}

impl From<SortSpecWire> for SortSpec {
    fn from(wire: SortSpecWire) -> Self {
        wire.to_sort_spec()
    }
}

/// Request sent from the coordinator to a worker for one file group of a
/// `compact_file_group` job. One job (a single `CALL
/// system.rewrite_data_files` invocation, or an active-mode maintenance
/// tick) fans out into one `CompactGroupRequest` per bin-packed group.
#[derive(Clone, Serialize, Deserialize)]
pub struct CompactGroupRequest {
    /// Identifier for the parent job, shared across all groups in the job.
    pub job_id: String,
    /// Identifier for this group within the job.
    pub group_id: u32,
    /// Fully qualified table identifier (e.g. `catalog.namespace.table`).
    pub table_ident: String,
    /// Iceberg table metadata location the worker should load to resolve
    /// schema / partition spec for the rewrite.
    pub metadata_location: String,
    /// Snapshot ID this group's files were read from. Pins the read to a
    /// consistent view even if the table advances during the job.
    pub snapshot_id: i64,
    /// Data file paths (with any covering delete files applied by the
    /// worker) that make up this group.
    pub group_file_paths: Vec<String>,
    /// Target size in bytes for each rewritten output file.
    pub target_file_size_bytes: u64,
    /// Output Parquet compression codec (e.g. `"zstd"`, `"snappy"`).
    pub compression: String,
    /// Optional sort to apply before writing the group back out.
    pub sort: Option<SortSpecWire>,
    /// S3 connection details for reading the input files and writing the
    /// rewritten output.
    pub s3: S3Conn,
}

impl std::fmt::Debug for CompactGroupRequest {
    /// Delegates credential redaction to `S3Conn`'s `Debug` impl so a stray
    /// `{:?}` on the whole request still never leaks secrets.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactGroupRequest")
            .field("job_id", &self.job_id)
            .field("group_id", &self.group_id)
            .field("table_ident", &self.table_ident)
            .field("metadata_location", &self.metadata_location)
            .field("snapshot_id", &self.snapshot_id)
            .field("group_file_paths", &self.group_file_paths)
            .field("target_file_size_bytes", &self.target_file_size_bytes)
            .field("compression", &self.compression)
            .field("sort", &self.sort)
            .field("s3", &self.s3)
            .finish()
    }
}

impl CompactGroupRequest {
    /// Serialize to JSON bytes for the `do_action` request body. These are
    /// the exact bytes `sign`/`verify` operate over.
    pub fn to_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }
}

/// One frame of the worker's streamed response to a `compact_file_group`
/// action. Workers emit zero or more `Progress` frames followed by exactly
/// one `Done` frame.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CompactGroupFrame {
    /// Best-effort progress heartbeat while a group is being read/rewritten.
    Progress {
        /// Which group this progress update is for.
        group_id: u32,
        /// Cumulative rows read so far for this group.
        rows_read: u64,
    },
    /// Terminal frame: the group finished (successfully committed its
    /// rewrite at the worker's local stage) and produced output files.
    Done(CompactGroupResponse),
}

impl CompactGroupFrame {
    /// Serialize to JSON bytes for one `do_action` response frame.
    pub fn to_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }
}

/// Result of rewriting one file group, carried in the terminal
/// [`CompactGroupFrame::Done`] frame.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactGroupResponse {
    /// Which group this response is for.
    pub group_id: u32,
    /// The new Iceberg `DataFile` entries produced by the rewrite, Avro
    /// encoded so the coordinator can decode them without depending on the
    /// worker's in-memory `DataFile` representation directly.
    pub new_data_files_avro: Vec<u8>,
    /// Total rows written across all output files for this group.
    pub rows_written: u64,
    /// Total bytes written across all output files for this group.
    pub bytes_written: u64,
    /// S3 paths of the uploaded output files, for the coordinator's commit
    /// bookkeeping / cleanup on failure.
    pub uploaded_paths: Vec<String>,
}

/// Compute the HMAC-SHA256 tag (lowercase hex) over `bytes` keyed by
/// `secret`.
///
/// See the module docs for why this mirrors, rather than reuses, the
/// scan-ticket signing helpers.
///
/// Unlike `sign_ticket` in `sqe-coordinator`, this does **not** special-case
/// an empty `secret` by skipping signing (there is no `Option` return here).
/// An empty secret still produces a tag (`HMAC-SHA256("")`), and `verify`
/// will only accept that exact tag, not any value. If Task 3/4 want to
/// preserve the scan-ticket convention where an empty `worker_secret` means
/// "dev mode, skip verification entirely," that check belongs in the
/// caller (e.g. the worker action bypasses `verify` up front when its
/// configured secret is empty), not in this function.
pub fn sign(bytes: &[u8], secret: &str) -> String {
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts keys of any length");
    mac.update(bytes);
    hex_encode(&mac.finalize().into_bytes())
}

/// Constant-time verification of `sig` (lowercase hex) against the
/// HMAC-SHA256 tag of `bytes` keyed by `secret`.
///
/// Returns `false` on any mismatch, including a length mismatch (checked
/// before the constant-time comparison, as `subtle::ConstantTimeEq` requires
/// equal-length inputs).
pub fn verify(bytes: &[u8], sig: &str, secret: &str) -> bool {
    let expected = sign(bytes, secret);
    let provided_bytes = sig.as_bytes();
    let expected_bytes = expected.as_bytes();
    provided_bytes.len() == expected_bytes.len()
        && bool::from(provided_bytes.ct_eq(expected_bytes))
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_s3() -> S3Conn {
        S3Conn {
            endpoint: "http://localhost:9000".to_string(),
            region: "us-east-1".to_string(),
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: "session-token-value".to_string(),
            path_style: true,
            allow_http: true,
        }
    }

    fn sample_request() -> CompactGroupRequest {
        CompactGroupRequest {
            job_id: "job-001".to_string(),
            group_id: 3,
            table_ident: "catalog.ns.orders".to_string(),
            metadata_location: "s3://bucket/warehouse/orders/metadata/v3.metadata.json"
                .to_string(),
            snapshot_id: 42,
            group_file_paths: vec![
                "s3://bucket/data/f1.parquet".to_string(),
                "s3://bucket/data/f2.parquet".to_string(),
            ],
            target_file_size_bytes: 128 * 1024 * 1024,
            compression: "zstd".to_string(),
            sort: Some(SortSpecWire::Columns(vec![
                ("order_ts".to_string(), true),
                ("order_id".to_string(), false),
            ])),
            s3: sample_s3(),
        }
    }

    #[test]
    fn compact_group_request_roundtrips() {
        let req = sample_request();
        let bytes = req.to_bytes().unwrap();
        let decoded = CompactGroupRequest::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.job_id, "job-001");
        assert_eq!(decoded.group_id, 3);
        assert_eq!(decoded.table_ident, "catalog.ns.orders");
        assert_eq!(decoded.snapshot_id, 42);
        assert_eq!(decoded.group_file_paths, req.group_file_paths);
        assert_eq!(decoded.target_file_size_bytes, req.target_file_size_bytes);
        assert_eq!(decoded.compression, "zstd");
        assert_eq!(decoded.sort, req.sort);
        assert_eq!(decoded.s3.endpoint, req.s3.endpoint);
        assert_eq!(decoded.s3.access_key, req.s3.access_key);
        assert_eq!(decoded.s3.secret_key, req.s3.secret_key);
    }

    #[test]
    fn compact_group_request_roundtrips_with_zorder_and_no_sort() {
        let mut req = sample_request();
        req.sort = Some(SortSpecWire::ZOrder(vec![
            "lat".to_string(),
            "lon".to_string(),
        ]));
        let bytes = req.to_bytes().unwrap();
        let decoded = CompactGroupRequest::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.sort, req.sort);

        let mut req_none = sample_request();
        req_none.sort = None;
        let bytes_none = req_none.to_bytes().unwrap();
        let decoded_none = CompactGroupRequest::from_bytes(&bytes_none).unwrap();
        assert_eq!(decoded_none.sort, None);
    }

    #[test]
    fn compact_group_frame_progress_roundtrips() {
        let frame = CompactGroupFrame::Progress {
            group_id: 7,
            rows_read: 12345,
        };
        let bytes = frame.to_bytes().unwrap();
        let decoded = CompactGroupFrame::from_bytes(&bytes).unwrap();
        match decoded {
            CompactGroupFrame::Progress { group_id, rows_read } => {
                assert_eq!(group_id, 7);
                assert_eq!(rows_read, 12345);
            }
            CompactGroupFrame::Done(_) => panic!("expected Progress frame"),
        }
    }

    #[test]
    fn compact_group_frame_done_roundtrips() {
        let response = CompactGroupResponse {
            group_id: 9,
            new_data_files_avro: vec![1, 2, 3, 4, 5],
            rows_written: 99_000,
            bytes_written: 1024 * 1024,
            uploaded_paths: vec!["s3://bucket/data/out-1.parquet".to_string()],
        };
        let frame = CompactGroupFrame::Done(response.clone());
        let bytes = frame.to_bytes().unwrap();
        let decoded = CompactGroupFrame::from_bytes(&bytes).unwrap();
        match decoded {
            CompactGroupFrame::Done(decoded_response) => {
                assert_eq!(decoded_response, response);
            }
            CompactGroupFrame::Progress { .. } => panic!("expected Done frame"),
        }
    }

    #[test]
    fn sign_and_verify_accept_a_valid_signature() {
        let bytes = sample_request().to_bytes().unwrap();
        let secret = "shared-worker-secret";
        let sig = sign(&bytes, secret);
        assert!(verify(&bytes, &sig, secret));
    }

    #[test]
    fn verify_rejects_a_tampered_payload() {
        let mut bytes = sample_request().to_bytes().unwrap();
        let secret = "shared-worker-secret";
        let sig = sign(&bytes, secret);

        // Flip a byte in the middle of the payload to simulate tampering.
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;

        assert!(!verify(&bytes, &sig, secret));
    }

    #[test]
    fn verify_rejects_a_wrong_secret() {
        let bytes = sample_request().to_bytes().unwrap();
        let sig = sign(&bytes, "correct-secret");
        assert!(!verify(&bytes, &sig, "wrong-secret"));
    }

    #[test]
    fn verify_rejects_a_signature_of_different_length() {
        let bytes = sample_request().to_bytes().unwrap();
        let secret = "shared-worker-secret";
        assert!(!verify(&bytes, "not-a-valid-hex-sig", secret));
    }

    /// Pins the empty-secret behavior described in `sign`'s doc comment:
    /// unlike the scan-ticket path's "empty secret means dev mode, skip
    /// verification" convention, `wire::verify` never blanket-accepts. An
    /// empty secret still produces a real (if predictable) tag, and only an
    /// exact match against it passes. A future edit that special-cases an
    /// empty secret into an automatic `true` here would be a security
    /// regression for any deployment that forgot to set the worker secret;
    /// this test fails first.
    #[test]
    fn empty_secret_does_not_blanket_accept_but_is_self_consistent() {
        let bytes = sample_request().to_bytes().unwrap();

        // An arbitrary signature is not accepted just because the secret is
        // empty.
        assert!(!verify(&bytes, "anything", ""));
        assert!(!verify(&bytes, "", ""));

        // But signing and verifying with the same empty secret is still
        // internally consistent (it is a real HMAC tag, just a
        // predictable/weak one -- callers must not rely on an empty secret
        // for real security, matching the scan-ticket path's own framing of
        // an empty secret as "allow_unauthenticated dev mode").
        let sig = sign(&bytes, "");
        assert!(verify(&bytes, &sig, ""));
    }

    #[test]
    fn sort_spec_wire_roundtrips_columns() {
        let spec = SortSpec::Columns(vec![
            ("a".to_string(), true),
            ("b".to_string(), false),
        ]);
        let wire: SortSpecWire = (&spec).into();
        let back: SortSpec = wire.to_sort_spec();
        assert_eq!(back, spec);

        let wire_owned: SortSpecWire = spec.clone().into();
        let back_owned: SortSpec = wire_owned.into();
        assert_eq!(back_owned, spec);
    }

    #[test]
    fn sort_spec_wire_roundtrips_zorder() {
        let spec = SortSpec::ZOrder(vec!["x".to_string(), "y".to_string()]);
        let wire: SortSpecWire = (&spec).into();
        let back: SortSpec = wire.to_sort_spec();
        assert_eq!(back, spec);
    }

    #[test]
    fn s3_conn_debug_redacts_secrets() {
        let s3 = sample_s3();
        let debug_output = format!("{s3:?}");
        assert!(!debug_output.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!debug_output.contains("wJalrXUtnFEMI"));
        assert!(!debug_output.contains("session-token-value"));
        assert!(debug_output.contains("[REDACTED]"));
    }

    #[test]
    fn compact_group_request_debug_redacts_secrets() {
        let req = sample_request();
        let debug_output = format!("{req:?}");
        assert!(!debug_output.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!debug_output.contains("wJalrXUtnFEMI"));
        assert!(!debug_output.contains("session-token-value"));
    }
}
