use std::io::Write;
use std::sync::mpsc::{self, Sender};
use std::thread::JoinHandle;

use serde::{Deserialize, Serialize};
use tracing::info;

use super::redact_pii;

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
    let normalised: String = sql.split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase();
    let hash = Sha256::digest(normalised.as_bytes());
    format!("{hash:x}")
}

enum AuditMsg {
    Entry(Box<AuditEntry>),
    Flush(Sender<()>),
}

pub struct AuditLogger {
    tx: Option<Sender<AuditMsg>>,
    worker: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl AuditLogger {
    pub fn new(path: &str) -> Result<Self, String> {
        if path.is_empty() {
            return Ok(Self {
                tx: None,
                worker: std::sync::Mutex::new(None),
            });
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("Failed to open audit log file '{path}': {e}"))?;

        let path_owned = path.to_string();
        let (tx, rx) = mpsc::channel::<AuditMsg>();
        let worker = std::thread::Builder::new()
            .name("sqe-audit-writer".to_string())
            .spawn(move || {
                let mut writer = std::io::BufWriter::new(file);
                while let Ok(msg) = rx.recv() {
                    match msg {
                        AuditMsg::Entry(entry) => {
                            // Drain whatever else has piled up so we batch
                            // multiple entries between flushes.
                            let mut batch = vec![*entry];
                            while let Ok(more) = rx.try_recv() {
                                match more {
                                    AuditMsg::Entry(e) => batch.push(*e),
                                    AuditMsg::Flush(ack) => {
                                        Self::write_batch(&mut writer, &batch, &path_owned);
                                        batch.clear();
                                        if let Err(e) = writer.flush() {
                                            tracing::error!("AUDIT: flush failed for '{path_owned}': {e}");
                                        }
                                        let _ = ack.send(());
                                    }
                                }
                            }
                            if !batch.is_empty() {
                                Self::write_batch(&mut writer, &batch, &path_owned);
                                if let Err(e) = writer.flush() {
                                    tracing::error!("AUDIT: flush failed for '{path_owned}': {e}");
                                }
                            }
                        }
                        AuditMsg::Flush(ack) => {
                            if let Err(e) = writer.flush() {
                                tracing::error!("AUDIT: flush failed for '{path_owned}': {e}");
                            }
                            let _ = ack.send(());
                        }
                    }
                }
                let _ = writer.flush();
            })
            .map_err(|e| format!("Failed to start audit writer thread: {e}"))?;

        info!(path = path, "Audit log initialized");
        Ok(Self {
            tx: Some(tx),
            worker: std::sync::Mutex::new(Some(worker)),
        })
    }

    fn write_batch(
        writer: &mut std::io::BufWriter<std::fs::File>,
        batch: &[AuditEntry],
        path: &str,
    ) {
        for entry in batch {
            let json = match serde_json::to_string(entry) {
                Ok(j) => j,
                Err(e) => {
                    tracing::error!("AUDIT: serialization failed: {e}");
                    continue;
                }
            };
            if let Err(e) = writeln!(writer, "{json}") {
                tracing::error!("AUDIT: write failed for '{path}': {e}");
            }
        }
    }

    pub fn log(&self, entry: &AuditEntry) {
        let Some(ref tx) = self.tx else {
            return;
        };

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

        if let Err(e) = tx.send(AuditMsg::Entry(Box::new(redacted))) {
            tracing::error!("AUDIT: writer dropped, entry not recorded: {e}");
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
}
