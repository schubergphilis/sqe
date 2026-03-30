use std::io::Write;
use std::sync::Mutex;

use serde::Serialize;
use tracing::info;

/// Redact common PII patterns from SQL text for audit log safety.
///
/// Replaces:
/// - Email addresses → [EMAIL]
/// - Phone numbers (US/intl) → [PHONE]
/// - SSN patterns (XXX-XX-XXXX) → [SSN]
/// - Credit card-like numbers (13-19 digits) → [CARD]
/// - Quoted string literals that look like identifiers → preserved
pub fn redact_pii(sql: &str) -> String {
    let mut result = sql.to_string();

    // Email: word@word.tld pattern inside single quotes
    let email_re = regex_lite::Regex::new(
        r"'[^']*[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}[^']*'",
    )
    .unwrap();
    result = email_re.replace_all(&result, "'[EMAIL]'").to_string();

    // SSN: XXX-XX-XXXX
    let ssn_re = regex_lite::Regex::new(r"'\d{3}-\d{2}-\d{4}'").unwrap();
    result = ssn_re.replace_all(&result, "'[SSN]'").to_string();

    // Phone: various formats (10-15 digits with optional separators)
    let phone_re = regex_lite::Regex::new(
        r"'(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'",
    )
    .unwrap();
    result = phone_re.replace_all(&result, "'[PHONE]'").to_string();

    // Credit card: 4-groups of digits (possibly with spaces/dashes)
    let card_re =
        regex_lite::Regex::new(r"'\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{1,7}'").unwrap();
    result = card_re.replace_all(&result, "'[CARD]'").to_string();

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
}

/// Compute SHA-256 hash of normalised SQL (whitespace-collapsed, uppercase keywords).
pub fn query_hash(sql: &str) -> String {
    use sha2::{Digest, Sha256};
    let normalised: String = sql.split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase();
    let hash = Sha256::digest(normalised.as_bytes());
    format!("{hash:x}")
}

pub struct AuditLogger {
    writer: Option<Mutex<std::io::BufWriter<std::fs::File>>>,
}

impl AuditLogger {
    pub fn new(path: &str) -> Result<Self, String> {
        if path.is_empty() {
            return Ok(Self { writer: None });
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("Failed to open audit log file '{path}': {e}"))?;

        info!(path = path, "Audit log initialized");
        Ok(Self {
            writer: Some(Mutex::new(std::io::BufWriter::new(file))),
        })
    }

    pub fn log(&self, entry: &AuditEntry) {
        if let Some(ref writer) = self.writer {
            let mut w = match writer.lock() {
                Ok(w) => w,
                Err(e) => {
                    eprintln!("AUDIT: mutex poisoned, audit entry lost: {e}");
                    return;
                }
            };
            // Redact PII from query_text before writing to the audit log.
            // query_hash is a non-reversible hash and is left untouched.
            let redacted_entry;
            let entry = if entry.query_text.is_some() {
                redacted_entry = AuditEntry {
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
                };
                &redacted_entry
            } else {
                entry
            };
            let json = match serde_json::to_string(entry) {
                Ok(j) => j,
                Err(e) => {
                    eprintln!("AUDIT: serialization failed: {e}");
                    return;
                }
            };
            if let Err(e) = writeln!(w, "{json}") {
                eprintln!("AUDIT: write failed: {e}");
            }
            if let Err(e) = w.flush() {
                eprintln!("AUDIT: flush failed: {e}");
            }
        }
    }
}

impl std::fmt::Debug for AuditLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLogger")
            .field("active", &self.writer.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

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
        let dir = std::env::temp_dir();
        let path = dir.join("sqe-audit-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str).unwrap();
        logger.log(&test_entry());

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("\"username\":\"test\""));
        assert!(content.contains("query_hash"));

        let _ = std::fs::remove_file(&path);
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

    #[test]
    fn test_log_redacts_pii_in_written_file() {
        let dir = std::env::temp_dir();
        let path = dir.join("sqe-audit-pii-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str).unwrap();
        let mut entry = test_entry();
        entry.query_text = Some(
            "SELECT * FROM users WHERE email = 'carol@example.com'".to_string(),
        );
        logger.log(&entry);

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(!content.contains("carol@example.com"), "PII must not appear in audit log");
        assert!(content.contains("[EMAIL]"));

        let _ = std::fs::remove_file(&path);
    }
}
