use std::io::Write;
use std::sync::mpsc::{self, Sender};
use std::thread::JoinHandle;

use serde::Serialize;
use tracing::info;

/// Redact common PII patterns and secret literals from SQL text for audit
/// log safety.
///
/// Replaces:
/// - Email addresses -> [EMAIL]
/// - Phone numbers (US/intl) -> [PHONE]
/// - SSN patterns (XXX-XX-XXXX) -> [SSN]
/// - Credit card-like numbers (13-19 digits) -> [CARD]
/// - Quoted secret literals (`TOKEN '...'`, `PASSWORD '...'`,
///   `ACCESS_KEY_ID '...'`, `SECRET_ACCESS_KEY '...'`, `SESSION_TOKEN '...'`,
///   `SECRET '...'`) -> [REDACTED]
///
/// The secret-literal pass is the belt-and-suspenders guard for issue #4:
/// without it, `CREATE SECRET ... TOKEN '<jwt>'` lands verbatim in the audit
/// JSONL, OTel/Loki sinks, and any debug-level trace, exfiltrating every
/// long-lived bearer ever created in the cluster.
pub fn redact_pii(sql: &str) -> String {
    use std::sync::OnceLock;

    static EMAIL_RE: OnceLock<regex_lite::Regex> = OnceLock::new();
    static SSN_RE: OnceLock<regex_lite::Regex> = OnceLock::new();
    static PHONE_RE: OnceLock<regex_lite::Regex> = OnceLock::new();
    static CARD_RE: OnceLock<regex_lite::Regex> = OnceLock::new();
    static SECRET_RE: OnceLock<regex_lite::Regex> = OnceLock::new();

    let email_re = EMAIL_RE.get_or_init(|| {
        regex_lite::Regex::new(
            r"'[^']*[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}[^']*'",
        )
        .unwrap()
    });
    let ssn_re = SSN_RE.get_or_init(|| {
        regex_lite::Regex::new(r"'\d{3}-\d{2}-\d{4}'").unwrap()
    });
    let phone_re = PHONE_RE.get_or_init(|| {
        regex_lite::Regex::new(
            r"'(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'",
        )
        .unwrap()
    });
    let card_re = CARD_RE.get_or_init(|| {
        regex_lite::Regex::new(r"'\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{1,7}'").unwrap()
    });
    let secret_re = SECRET_RE.get_or_init(|| {
        regex_lite::Regex::new(
            r"(?i)\b(TOKEN|PASSWORD|PASSWD|SECRET|ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN|API_KEY|CLIENT_SECRET|BEARER)\b(\s*=\s*|\s+|\s*\(\s*)'[^']*'",
        )
        .unwrap()
    });

    let mut result = sql.to_string();
    result = email_re.replace_all(&result, "'[EMAIL]'").to_string();
    result = ssn_re.replace_all(&result, "'[SSN]'").to_string();
    result = phone_re.replace_all(&result, "'[PHONE]'").to_string();
    result = card_re.replace_all(&result, "'[CARD]'").to_string();
    result = secret_re.replace_all(&result, "$1$2'[REDACTED]'").to_string();
    result
}

#[derive(Debug, Serialize)]
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

    // --- PII redaction tests ---

    #[test]
    fn redact_email_in_where_clause() {
        let sql = "SELECT * FROM users WHERE email = 'alice@example.com'";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("alice@example.com"));
        assert!(redacted.contains("[EMAIL]"));
    }

    #[test]
    fn redact_ssn() {
        let sql = "SELECT * FROM records WHERE ssn = '123-45-6789'";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("123-45-6789"));
        assert!(redacted.contains("[SSN]"));
    }

    #[test]
    fn redact_phone() {
        let sql = "SELECT * FROM contacts WHERE phone = '(555) 123-4567'";
        let redacted = redact_pii(sql);
        assert!(redacted.contains("[PHONE]"));
    }

    #[test]
    fn no_redaction_for_normal_sql() {
        let sql = "SELECT id, name FROM products WHERE category = 'electronics'";
        let redacted = redact_pii(sql);
        assert_eq!(redacted, sql);
    }

    #[test]
    fn redact_multiple_patterns() {
        let sql = "INSERT INTO users (email, ssn) VALUES ('bob@test.com', '987-65-4321')";
        let redacted = redact_pii(sql);
        assert!(redacted.contains("[EMAIL]"));
        assert!(redacted.contains("[SSN]"));
        assert!(!redacted.contains("bob@test.com"));
    }

    // --- Secret-literal redaction (issue #4 regression tests) ---

    #[test]
    fn redact_create_secret_bearer_token() {
        let sql = "CREATE SECRET my_token (TYPE bearer, TOKEN 'eyJhbGciOiJSUzI1NiJ9.payload.sig')";
        let redacted = redact_pii(sql);
        assert!(
            !redacted.contains("eyJhbGciOiJSUzI1NiJ9.payload.sig"),
            "bearer token literal must not survive: {redacted}"
        );
        assert!(redacted.contains("[REDACTED]"));
        assert!(redacted.to_uppercase().contains("TOKEN"));
    }

    #[test]
    fn redact_create_secret_password() {
        let sql = "CREATE SECRET my_pw (TYPE password, PASSWORD 'hunter2!correct horse')";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("hunter2!correct horse"));
        assert!(redacted.contains("[REDACTED]"));
    }

    #[test]
    fn redact_create_secret_aws_keys() {
        let sql = "CREATE SECRET aws (\
            TYPE aws, \
            ACCESS_KEY_ID 'AKIAIOSFODNN7EXAMPLE', \
            SECRET_ACCESS_KEY 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', \
            SESSION_TOKEN 'FQoDYXdzEPv...EXAMPLE')";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("AKIAIOSFODNN7EXAMPLE"), "{redacted}");
        assert!(!redacted.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"), "{redacted}");
        assert!(!redacted.contains("FQoDYXdzEPv...EXAMPLE"), "{redacted}");
        assert!(redacted.matches("[REDACTED]").count() >= 3);
    }

    #[test]
    fn redact_secret_kv_equals_style() {
        let sql = "CREATE SECRET s WITH (token = 'abc.def.ghi', password = 'hunter2')";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("abc.def.ghi"));
        assert!(!redacted.contains("hunter2"));
    }

    #[test]
    fn redact_secret_case_insensitive() {
        let sql = "CREATE SECRET x (token 'abc', Password 'def', api_key 'ghi')";
        let redacted = redact_pii(sql);
        assert!(!redacted.contains("'abc'"));
        assert!(!redacted.contains("'def'"));
        assert!(!redacted.contains("'ghi'"));
    }

    #[test]
    fn redact_does_not_touch_column_named_token() {
        let sql = "SELECT token FROM creds WHERE id = 1";
        let redacted = redact_pii(sql);
        assert_eq!(redacted, sql);
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
