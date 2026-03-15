use std::io::Write;
use std::sync::Mutex;

use serde::Serialize;
use tracing::{error, info};

#[derive(Debug, Serialize)]
pub struct AuditEntry {
    pub timestamp: String,
    pub username: String,
    pub query_text: String,
    pub statement_type: String,
    pub duration_ms: u64,
    pub rows_returned: usize,
    pub status: String,
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

    #[test]
    fn test_noop_logger() {
        let logger = AuditLogger::new("");
        let entry = AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            username: "test".to_string(),
            query_text: "SELECT 1".to_string(),
            statement_type: "query".to_string(),
            duration_ms: 42,
            rows_returned: 1,
            status: "success".to_string(),
        };
        logger.log(&entry);
    }

    #[test]
    fn test_audit_entry_serialization() {
        let entry = AuditEntry {
            timestamp: "2026-03-15T00:00:00Z".to_string(),
            username: "root".to_string(),
            query_text: "SELECT * FROM t".to_string(),
            statement_type: "query".to_string(),
            duration_ms: 100,
            rows_returned: 5,
            status: "success".to_string(),
        };

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"username\":\"root\""));
        assert!(json.contains("\"duration_ms\":100"));
    }

    #[test]
    fn test_file_logger_writes() {
        let dir = std::env::temp_dir();
        let path = dir.join("sqe-audit-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str);
        let entry = AuditEntry {
            timestamp: "2026-03-15T00:00:00Z".to_string(),
            username: "testuser".to_string(),
            query_text: "SELECT 1".to_string(),
            statement_type: "query".to_string(),
            duration_ms: 10,
            rows_returned: 1,
            status: "success".to_string(),
        };
        logger.log(&entry);

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("testuser"));
        assert!(content.contains("SELECT 1"));

        let _ = std::fs::remove_file(&path);
    }
}
