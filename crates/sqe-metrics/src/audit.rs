use std::io::Write;
use std::sync::Mutex;

use serde::Serialize;
use tracing::{error, info};

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
    pub fn new(path: &str) -> Self {
        if path.is_empty() {
            return Self { writer: None };
        }

        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(file) => {
                info!(path = path, "Audit log initialized");
                Self {
                    writer: Some(Mutex::new(std::io::BufWriter::new(file))),
                }
            }
            Err(e) => {
                error!(path = path, error = %e, "Failed to open audit log file");
                Self { writer: None }
            }
        }
    }

    pub fn log(&self, entry: &AuditEntry) {
        if let Some(ref writer) = self.writer {
            if let Ok(mut w) = writer.lock() {
                if let Ok(json) = serde_json::to_string(entry) {
                    let _ = writeln!(w, "{json}");
                    let _ = w.flush();
                }
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
        let logger = AuditLogger::new("");
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

        let logger = AuditLogger::new(path_str);
        logger.log(&test_entry());

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("\"username\":\"test\""));
        assert!(content.contains("query_hash"));

        let _ = std::fs::remove_file(&path);
    }
}
