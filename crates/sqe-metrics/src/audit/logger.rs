use std::sync::mpsc::{self, Sender};
use std::thread::JoinHandle;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::chain::HashChain;
use super::event::{
    Actor, AuditEvent, AuditKind, Integrity, Outcome, PolicyAudit, QueryInfo, QueryStats, Timing,
};
use super::redact_pii;
use super::sink::{AuditFormat, AuditSink, NativeJsonlSink, OcsfJsonlSink};

#[derive(Debug, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: String,
    pub username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// SHA-256 hash of normalised SQL (whitespace-collapsed, uppercase keywords).
    pub query_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_text: Option<String>,
    pub statement_type: String,
    pub duration_ms: u64,
    pub rows_returned: usize,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables_touched: Vec<String>,

    // --- Policy-decision fields (issue: no-policy-decision-audit) ---
    // Populated from the `PolicySummary` the enforcer returns. `serde(default)`
    // so existing call sites and log consumers that don't set them keep working,
    // and a deny-all (zero rows) is no longer indistinguishable from a
    // legitimate empty result.
    /// Count of row-filter expressions injected by policy (excludes the
    /// deny-all sentinel; a deny is reflected in `policy_denied`).
    #[serde(default)]
    pub row_filters_applied: usize,
    /// Columns masked by policy (sorted names).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns_masked: Vec<String>,
    /// Columns restricted/dropped by policy (sorted names).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns_restricted: Vec<String>,
    /// True when at least one scanned table was denied (deny-all row filter:
    /// resolve failure, breaker-open, unknown-tag state, or fully-restricted).
    #[serde(default)]
    pub policy_denied: bool,
}

/// Compute SHA-256 hash of normalised SQL (whitespace-collapsed, uppercase keywords).
pub fn query_hash(sql: &str) -> String {
    use sha2::{Digest, Sha256};
    let normalised: String = sql
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_uppercase();
    let hash = Sha256::digest(normalised.as_bytes());
    format!("{hash:x}")
}

/// Compute a chain hash over raw bytes: `sha256(prev_hash || body_bytes)`.
///
/// Used to chain legacy `AuditEntry` records without converting them to `AuditEvent`.
fn chain_hash_raw(prev_hash: &str, body: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(prev_hash.as_bytes());
    hasher.update(body);
    format!("{:x}", hasher.finalize())
}

/// Inject an `Integrity` block into a serialized JSON object string.
///
/// Expects `json` to be a `{...}` object. Strips the closing `}`, appends
/// `,"integrity":{...}}`. If `json` is empty or malformed, appends the
/// integrity as a standalone object (best-effort, never panics).
fn inject_integrity(json: &str, integrity: &Integrity) -> String {
    let integrity_json = match serde_json::to_string(integrity) {
        Ok(s) => s,
        Err(_) => return json.to_string(),
    };
    let trimmed = json.trim_end();
    if let Some(body) = trimmed.strip_suffix('}') {
        format!("{body},\"integrity\":{integrity_json}}}")
    } else {
        // Malformed JSON: append best-effort.
        format!("{json},\"integrity\":{integrity_json}")
    }
}

/// Convert a (pre-redacted) `AuditEntry` to the canonical `AuditEvent` type.
///
/// Used by `log_event` paths that need the structured event. The legacy `log()`
/// path preserves the flat `AuditEntry` JSON format.
///
/// Mapping notes:
/// - `timestamp: String` parsed as RFC-3339; falls back to `Utc::now()` on failure.
/// - `status: String` "success" -> `Outcome::Success`; anything else -> `Outcome::Failure`.
/// - `tables_touched: Vec<String>` has no exact target in the structured event model.
///   The field is noted but currently dropped from the AuditEvent representation.
///   It will be wired into `resources` in a follow-up task.
impl From<AuditEntry> for AuditEvent {
    fn from(e: AuditEntry) -> Self {
        let time = DateTime::parse_from_rfc3339(&e.timestamp)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now());

        let outcome = if e.status.eq_ignore_ascii_case("success") {
            Outcome::Success
        } else {
            Outcome::Failure {
                error_type: Some(e.status.clone()),
                error_code: None,
                message: None,
            }
        };

        let policy = if e.row_filters_applied > 0
            || !e.columns_masked.is_empty()
            || !e.columns_restricted.is_empty()
            || e.policy_denied
        {
            Some(PolicyAudit {
                row_filters_applied: e.row_filters_applied,
                columns_masked: e.columns_masked,
                columns_restricted: e.columns_restricted,
                denied: e.policy_denied,
            })
        } else {
            None
        };

        AuditEvent {
            time,
            kind: AuditKind::Query,
            actor: Actor {
                username: e.username,
                ..Default::default()
            },
            outcome,
            resources: Vec::new(),
            policy,
            timing: Some(Timing {
                duration_ms: e.duration_ms,
                ..Default::default()
            }),
            stats: Some(QueryStats {
                rows_returned: e.rows_returned,
                ..Default::default()
            }),
            query: Some(QueryInfo {
                text: e.query_text,
                query_hash: e.query_hash,
                statement_type: e.statement_type,
            }),
            session_id: e.session_id,
            client_ip: e.client_ip,
            integrity: Integrity::default(),
        }
    }
}

/// Message type sent to the audit writer thread.
enum AuditMsg {
    /// Legacy `AuditEntry` path from `log()`. Already PII-redacted.
    /// Written as flat `AuditEntry` JSON + an appended `integrity` block,
    /// preserving the on-disk format for consumers that parse the flat shape.
    Legacy(Box<AuditEntry>),
    /// Canonical event from `log_event()`. Fan-out to all sinks with chain stamp.
    Event(Box<AuditEvent>),
    Flush(Sender<()>),
}

pub struct AuditLogger {
    tx: Option<Sender<AuditMsg>>,
    worker: std::sync::Mutex<Option<JoinHandle<()>>>,
}

/// Derive the OCSF file path from the native path.
///
/// Given `audit.jsonl`, produces `audit.ocsf.jsonl`.
/// Given `audit` (no extension), produces `audit.ocsf.jsonl`.
/// This keeps both files in the same directory with clear naming.
fn ocsf_path_from(native_path: &std::path::Path) -> std::path::PathBuf {
    let stem = native_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();
    let dir = native_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    dir.join(format!("{stem}.ocsf.jsonl"))
}

fn open_sink_file(path: &std::path::Path) -> Result<std::fs::File, String> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("Failed to open audit log file '{}': {e}", path.display()))
}

impl AuditLogger {
    /// Create a logger writing native JSONL to `path`.
    ///
    /// An empty path disables logging (no-op logger).
    pub fn new(path: &str) -> Result<Self, String> {
        Self::with_config(path, AuditFormat::Native)
    }

    /// Create a logger with a configurable output format.
    ///
    /// - `Native`: writes to `path`.
    /// - `Ocsf`: writes OCSF JSON to `path`.
    /// - `Both`: writes native JSON to `path` and OCSF JSON to the derived
    ///   path (see [`ocsf_path_from`]).
    ///
    /// The legacy `log(&AuditEntry)` path always writes flat `AuditEntry` JSON
    /// (with an appended `integrity` block) to the native/ocsf sink's underlying
    /// file. `log_event` fans out to all sinks via the `AuditSink` trait.
    ///
    /// An empty `path` disables logging regardless of format.
    pub fn with_config(path: &str, format: AuditFormat) -> Result<Self, String> {
        if path.is_empty() {
            return Ok(Self {
                tx: None,
                worker: std::sync::Mutex::new(None),
            });
        }

        let native_path = std::path::Path::new(path);

        let sinks: Vec<Box<dyn AuditSink>> = match format {
            AuditFormat::Native => {
                let file = open_sink_file(native_path)?;
                vec![Box::new(NativeJsonlSink::from_writer(Box::new(file)))]
            }
            AuditFormat::Ocsf => {
                let file = open_sink_file(native_path)?;
                vec![Box::new(OcsfJsonlSink::from_writer(Box::new(file)))]
            }
            AuditFormat::Both => {
                let native_file = open_sink_file(native_path)?;
                let ocsf_p = ocsf_path_from(native_path);
                let ocsf_file = open_sink_file(&ocsf_p)?;
                vec![
                    Box::new(NativeJsonlSink::from_writer(Box::new(native_file))),
                    Box::new(OcsfJsonlSink::from_writer(Box::new(ocsf_file))),
                ]
            }
        };

        // For the legacy `log()` path we need a direct-write file handle so we
        // can write the flat AuditEntry + integrity JSON bypassing the sink trait.
        // We open the same native file path a second time in append mode.
        let legacy_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(native_path)
            .map_err(|e| format!("Failed to open legacy audit log file '{path}': {e}"))?;

        let path_owned = path.to_string();
        let (tx, rx) = mpsc::channel::<AuditMsg>();
        let worker = std::thread::Builder::new()
            .name("sqe-audit-writer".to_string())
            .spawn(move || {
                let mut sinks = sinks;
                let mut chain = HashChain::new();
                let mut legacy_writer = std::io::BufWriter::new(legacy_file);
                while let Ok(msg) = rx.recv() {
                    match msg {
                        AuditMsg::Legacy(entry) => {
                            let mut batch = vec![*entry];
                            while let Ok(more) = rx.try_recv() {
                                match more {
                                    AuditMsg::Legacy(e) => batch.push(*e),
                                    AuditMsg::Event(e) => {
                                        Self::flush_legacy(&mut legacy_writer, &path_owned);
                                        flush_sinks(&mut sinks, &path_owned);
                                        // Process this event immediately.
                                        Self::write_events(&mut sinks, &mut chain, &mut [*e], &path_owned);
                                        flush_sinks(&mut sinks, &path_owned);
                                    }
                                    AuditMsg::Flush(ack) => {
                                        Self::write_legacy_batch(&mut legacy_writer, &mut chain, &batch, &path_owned);
                                        batch.clear();
                                        Self::flush_legacy(&mut legacy_writer, &path_owned);
                                        flush_sinks(&mut sinks, &path_owned);
                                        let _ = ack.send(());
                                    }
                                }
                            }
                            if !batch.is_empty() {
                                Self::write_legacy_batch(&mut legacy_writer, &mut chain, &batch, &path_owned);
                                Self::flush_legacy(&mut legacy_writer, &path_owned);
                                flush_sinks(&mut sinks, &path_owned);
                            }
                        }
                        AuditMsg::Event(event) => {
                            let mut batch = vec![*event];
                            while let Ok(more) = rx.try_recv() {
                                match more {
                                    AuditMsg::Event(e) => batch.push(*e),
                                    AuditMsg::Legacy(e) => {
                                        Self::write_events(&mut sinks, &mut chain, &mut batch, &path_owned);
                                        batch.clear();
                                        flush_sinks(&mut sinks, &path_owned);
                                        // Process this legacy entry immediately.
                                        Self::write_legacy_batch(&mut legacy_writer, &mut chain, &[*e], &path_owned);
                                        Self::flush_legacy(&mut legacy_writer, &path_owned);
                                    }
                                    AuditMsg::Flush(ack) => {
                                        Self::write_events(&mut sinks, &mut chain, &mut batch, &path_owned);
                                        batch.clear();
                                        Self::flush_legacy(&mut legacy_writer, &path_owned);
                                        flush_sinks(&mut sinks, &path_owned);
                                        let _ = ack.send(());
                                    }
                                }
                            }
                            if !batch.is_empty() {
                                Self::write_events(&mut sinks, &mut chain, &mut batch, &path_owned);
                                Self::flush_legacy(&mut legacy_writer, &path_owned);
                                flush_sinks(&mut sinks, &path_owned);
                            }
                        }
                        AuditMsg::Flush(ack) => {
                            Self::flush_legacy(&mut legacy_writer, &path_owned);
                            flush_sinks(&mut sinks, &path_owned);
                            let _ = ack.send(());
                        }
                    }
                }
                Self::flush_legacy(&mut legacy_writer, &path_owned);
                flush_sinks(&mut sinks, &path_owned);
            })
            .map_err(|e| format!("Failed to start audit writer thread: {e}"))?;

        info!(path = path, "Audit log initialized");
        Ok(Self {
            tx: Some(tx),
            worker: std::sync::Mutex::new(Some(worker)),
        })
    }

    /// Write a batch of legacy `AuditEntry` records as flat JSON + integrity block.
    fn write_legacy_batch(
        writer: &mut std::io::BufWriter<std::fs::File>,
        chain: &mut HashChain,
        batch: &[AuditEntry],
        path: &str,
    ) {
        use std::io::Write;
        for entry in batch {
            let body_json = match serde_json::to_string(entry) {
                Ok(j) => j,
                Err(e) => {
                    tracing::error!("AUDIT: serialization failed: {e}");
                    continue;
                }
            };
            // Build integrity: seq + prev_hash are set by the chain state;
            // hash = sha256(prev_hash || body_json).
            let seq = chain.next_seq();
            let prev = chain.current_prev_hash().to_string();
            let hash = chain_hash_raw(&prev, body_json.as_bytes());
            chain.advance(hash.clone());
            let integrity = Integrity { seq, prev_hash: prev, hash };
            let line = inject_integrity(&body_json, &integrity);
            if let Err(e) = writeln!(writer, "{line}") {
                tracing::error!("AUDIT: write failed for '{path}': {e}");
            }
        }
    }

    fn flush_legacy(writer: &mut std::io::BufWriter<std::fs::File>, path: &str) {
        use std::io::Write;
        if let Err(e) = writer.flush() {
            tracing::error!("AUDIT: flush failed for '{path}': {e}");
        }
    }

    /// Write a batch of canonical `AuditEvent` records to all sinks with chain stamping.
    fn write_events(
        sinks: &mut Vec<Box<dyn AuditSink>>,
        chain: &mut HashChain,
        batch: &mut [AuditEvent],
        path: &str,
    ) {
        for event in batch.iter_mut() {
            chain.stamp(event);
            for sink in sinks.iter_mut() {
                if let Err(e) = sink.write_line(event) {
                    tracing::error!("AUDIT: write failed for '{path}': {e}");
                }
            }
        }
    }

    /// Log an `AuditEntry` (legacy path). Redaction runs here before the entry
    /// is sent to the worker. The worker writes the flat `AuditEntry` JSON format
    /// plus an appended `integrity` block, preserving backward compatibility with
    /// consumers that parse the flat shape.
    pub fn log(&self, entry: &AuditEntry) {
        let Some(ref tx) = self.tx else {
            return;
        };

        // Redaction runs on the caller side, before the entry is queued.
        // Task 7 will add redaction for the `log_event` path.
        let redacted = if entry.query_text.is_some() {
            AuditEntry {
                query_text: entry.query_text.as_deref().map(redact_pii),
                timestamp: entry.timestamp.clone(),
                username: entry.username.clone(),
                session_id: entry.session_id.clone(),
                query_hash: entry.query_hash.clone(),
                statement_type: entry.statement_type.clone(),
                duration_ms: entry.duration_ms,
                rows_returned: entry.rows_returned,
                status: entry.status.clone(),
                client_ip: entry.client_ip.clone(),
                tables_touched: entry.tables_touched.clone(),
                row_filters_applied: entry.row_filters_applied,
                columns_masked: entry.columns_masked.clone(),
                columns_restricted: entry.columns_restricted.clone(),
                policy_denied: entry.policy_denied,
            }
        } else {
            AuditEntry {
                timestamp: entry.timestamp.clone(),
                username: entry.username.clone(),
                session_id: entry.session_id.clone(),
                query_hash: entry.query_hash.clone(),
                query_text: None,
                statement_type: entry.statement_type.clone(),
                duration_ms: entry.duration_ms,
                rows_returned: entry.rows_returned,
                status: entry.status.clone(),
                client_ip: entry.client_ip.clone(),
                tables_touched: entry.tables_touched.clone(),
                row_filters_applied: entry.row_filters_applied,
                columns_masked: entry.columns_masked.clone(),
                columns_restricted: entry.columns_restricted.clone(),
                policy_denied: entry.policy_denied,
            }
        };

        if let Err(e) = tx.send(AuditMsg::Legacy(Box::new(redacted))) {
            tracing::error!("AUDIT: writer dropped, entry not recorded: {e}");
        }
    }

    /// Log a canonical `AuditEvent` directly. Redaction (Task 7) is not yet
    /// applied here; callers are responsible for sanitising PII before calling
    /// this method.
    pub fn log_event(&self, event: AuditEvent) {
        let Some(ref tx) = self.tx else {
            return;
        };
        if let Err(e) = tx.send(AuditMsg::Event(Box::new(event))) {
            tracing::error!("AUDIT: writer dropped, event not recorded: {e}");
        }
    }

    /// Block until every entry sent before this call has been flushed to disk.
    /// Intended for shutdown paths and tests that read the file synchronously.
    pub fn flush(&self) {
        let Some(ref tx) = self.tx else {
            return;
        };
        let (ack_tx, ack_rx) = mpsc::channel();
        if tx.send(AuditMsg::Flush(ack_tx)).is_err() {
            return;
        }
        let _ = ack_rx.recv();
    }
}

fn flush_sinks(sinks: &mut Vec<Box<dyn AuditSink>>, path: &str) {
    for sink in sinks.iter_mut() {
        if let Err(e) = sink.flush() {
            tracing::error!("AUDIT: flush failed for '{path}': {e}");
        }
    }
}

impl Drop for AuditLogger {
    fn drop(&mut self) {
        self.flush();
        // Drop the sender so the writer thread exits, then join.
        self.tx.take();
        if let Ok(mut guard) = self.worker.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }
    }
}

impl std::fmt::Debug for AuditLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLogger")
            .field("active", &self.tx.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::TempDir;

    fn fresh_audit_path(name: &str) -> (TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join(name);
        (dir, path)
    }

    fn test_entry() -> AuditEntry {
        AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            username: "test".to_string(),
            session_id: Some("sess-123".to_string()),
            query_hash: query_hash("SELECT 1"),
            query_text: Some("SELECT 1".to_string()),
            statement_type: "query".to_string(),
            duration_ms: 42,
            rows_returned: 1,
            status: "success".to_string(),
            client_ip: Some("127.0.0.1".to_string()),
            tables_touched: Vec::new(),
            row_filters_applied: 0,
            columns_masked: Vec::new(),
            columns_restricted: Vec::new(),
            policy_denied: false,
        }
    }

    #[test]
    fn test_noop_logger() {
        let logger = AuditLogger::new("").unwrap();
        logger.log(&test_entry());
    }

    #[test]
    fn test_audit_entry_serialization() {
        let entry = test_entry();
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"username\":\"test\""));
        assert!(json.contains("\"query_hash\":"));
        assert!(json.contains("\"session_id\":\"sess-123\""));
    }

    #[test]
    fn test_audit_entry_omits_none_fields() {
        let mut entry = test_entry();
        entry.query_text = None;
        entry.client_ip = None;
        entry.session_id = None;
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("query_text"));
        assert!(!json.contains("client_ip"));
        assert!(!json.contains("session_id"));
    }

    #[test]
    fn test_audit_entry_policy_fields_serialize() {
        let mut entry = test_entry();
        entry.row_filters_applied = 2;
        entry.columns_masked = vec!["salary".to_string(), "ssn".to_string()];
        entry.columns_restricted = vec!["notes".to_string()];
        entry.policy_denied = false;
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"row_filters_applied\":2"));
        assert!(json.contains("\"columns_masked\":[\"salary\",\"ssn\"]"));
        assert!(json.contains("\"columns_restricted\":[\"notes\"]"));
        assert!(json.contains("\"policy_denied\":false"));
    }

    #[test]
    fn test_audit_entry_policy_denied_serialize() {
        let mut entry = test_entry();
        entry.policy_denied = true;
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"policy_denied\":true"));
    }

    #[test]
    fn test_audit_entry_deserializes_legacy_without_policy_fields() {
        // A pre-existing log line (no policy fields) must still parse, defaulting
        // the new fields. Guards the `serde(default)` contract for log consumers.
        let legacy = r#"{
            "timestamp": "2026-01-01T00:00:00Z",
            "username": "bob",
            "query_hash": "abc",
            "statement_type": "query",
            "duration_ms": 1,
            "rows_returned": 0,
            "status": "success"
        }"#;
        let entry: AuditEntry = serde_json::from_str(legacy).unwrap();
        assert_eq!(entry.row_filters_applied, 0);
        assert!(entry.columns_masked.is_empty());
        assert!(entry.columns_restricted.is_empty());
        assert!(!entry.policy_denied);
    }

    #[test]
    fn test_query_hash_normalises() {
        let h1 = query_hash("select  1  from   t");
        let h2 = query_hash("SELECT 1 FROM T");
        assert_eq!(h1, h2, "Hashes should match after normalisation");
    }

    #[test]
    fn test_query_hash_differs_for_different_sql() {
        let h1 = query_hash("SELECT 1");
        let h2 = query_hash("SELECT 2");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_file_logger_writes() {
        let (_dir, path) = fresh_audit_path("sqe-audit-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str).unwrap();
        logger.log(&test_entry());
        logger.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1, "exactly one entry written, got: {content}");
        assert!(lines[0].contains("\"username\":\"test\""));
        assert!(lines[0].contains("query_hash"));
    }

    #[test]
    fn test_flush_blocks_until_written() {
        let (_dir, path) = fresh_audit_path("sqe-audit-flush-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str).unwrap();
        for _ in 0..50 {
            logger.log(&test_entry());
        }
        logger.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines = content.lines().count();
        assert_eq!(lines, 50, "all entries visible after flush");
    }

    #[test]
    fn test_audit_log_does_not_contain_create_secret_literal() {
        let (_dir, path) = fresh_audit_path("sqe-audit-secret-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str).unwrap();
        let mut entry = test_entry();
        entry.statement_type = "create_secret".to_string();
        entry.query_text = Some(
            "CREATE SECRET prod_token (TYPE bearer, TOKEN 'eyJSECRETJWTPAYLOAD')".to_string(),
        );
        logger.log(&entry);
        logger.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(
            lines.len(),
            1,
            "exactly one entry must exist so the negative assertion below is meaningful, got: {content}"
        );
        assert!(
            !content.contains("eyJSECRETJWTPAYLOAD"),
            "secret literal leaked to audit log: {content}"
        );
        assert!(content.contains("[REDACTED]"));
    }

    #[test]
    fn test_log_redacts_pii_in_written_file() {
        let (_dir, path) = fresh_audit_path("sqe-audit-pii-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str).unwrap();
        let mut entry = test_entry();
        entry.query_text = Some(
            "SELECT * FROM users WHERE email = 'carol@example.com'".to_string(),
        );
        logger.log(&entry);
        logger.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(
            lines.len(),
            1,
            "exactly one entry must exist so the negative assertion below is meaningful, got: {content}"
        );
        assert!(!content.contains("carol@example.com"), "PII must not appear in audit log");
        assert!(content.contains("[EMAIL]"));
    }

    #[test]
    fn both_format_writes_native_and_ocsf_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let logger = AuditLogger::with_config(
            path.to_str().unwrap(),
            crate::audit::AuditFormat::Both,
        )
        .unwrap();
        logger.log_event(crate::audit::sample_query_event());
        logger.flush();
        let native = std::fs::read_to_string(&path).unwrap();
        let ocsf = std::fs::read_to_string(dir.path().join("audit.ocsf.jsonl")).unwrap();
        assert!(native.contains("\"kind\":\"query\""));
        assert!(ocsf.contains("\"class_uid\":6005"));
        // Every native record carries an integrity hash.
        assert!(native.contains("\"hash\":"));
    }
}
