use std::io::Write;
use std::sync::mpsc::{self, Sender};
use std::thread::JoinHandle;

use serde::{Deserialize, Serialize};
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
///
/// IMPORTANT (SQL-07): `redact_pii` is **best-effort pattern matching**, not a
/// guarantee. It catches email / SSN / phone / card / secret-keyword *shapes*;
/// it does NOT catch free-form sensitive literals such as
/// `WHERE patient_id = 'P-998877'` or `WHERE diagnosis = 'HIV positive'`. For
/// sinks at a different trust boundary (lineage), prefer [`strip_sql_literals`]
/// (which removes ALL literals) plus the SQL hash.
pub fn redact_pii(sql: &str) -> String {
    use std::sync::OnceLock;

    static EMAIL_RE: OnceLock<regex::Regex> = OnceLock::new();
    static SSN_RE: OnceLock<regex::Regex> = OnceLock::new();
    static PHONE_RE: OnceLock<regex::Regex> = OnceLock::new();
    static CARD_RE: OnceLock<regex::Regex> = OnceLock::new();
    static SECRET_RE: OnceLock<regex::Regex> = OnceLock::new();

    let email_re = EMAIL_RE.get_or_init(|| {
        regex::Regex::new(
            r"'[^']*[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}[^']*'",
        )
        .unwrap()
    });
    let ssn_re = SSN_RE.get_or_init(|| {
        regex::Regex::new(r"'\d{3}-\d{2}-\d{4}'").unwrap()
    });
    let phone_re = PHONE_RE.get_or_init(|| {
        regex::Regex::new(
            r"'(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'",
        )
        .unwrap()
    });
    let card_re = CARD_RE.get_or_init(|| {
        regex::Regex::new(r"'\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{1,7}'").unwrap()
    });
    let secret_re = SECRET_RE.get_or_init(|| {
        regex::Regex::new(
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

/// SQL-07: replace every string and numeric literal in `sql` with a `?`
/// placeholder, leaving structure (keywords, identifiers, operators) intact.
///
/// Unlike [`redact_pii`] (pattern-only, best-effort), this removes ALL literal
/// values, so free-form sensitive data in predicates
/// (`WHERE patient_id = 'P-998877'`, `WHERE diagnosis = 'HIV positive'`,
/// `WHERE balance > 50000`) cannot reach a sink. Use it for sinks that sit at
/// a different trust boundary than the SQL client (lineage). The query shape is
/// preserved for debugging; correlate exact text via the SQL hash if needed.
///
/// Single-quoted strings (with `''` escapes) become `'?'`; standalone numeric
/// literals become `?`. A best-effort lexer, not a full SQL parser, but it is
/// total (never panics) and fail-closed (an unterminated quote consumes to EOL).
pub fn strip_sql_literals(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' {
            // String literal: consume until the closing quote, handling the
            // doubled-quote ('') escape. Emit a single placeholder.
            out.push_str("'?'");
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    // Doubled quote -> escaped quote, stay in the string.
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1; // closing quote
                    break;
                }
                i += 1;
            }
        } else if c.is_ascii_digit()
            && (i == 0 || !is_ident_byte(bytes[i - 1]))
        {
            // Numeric literal not part of an identifier (e.g. not `col1`).
            // Consume digits, decimal point, and exponent.
            out.push('?');
            i += 1;
            while i < bytes.len()
                && (bytes[i].is_ascii_digit()
                    || bytes[i] == b'.'
                    || bytes[i] == b'e'
                    || bytes[i] == b'E'
                    || bytes[i] == b'+'
                    || bytes[i] == b'-')
            {
                // Stop a trailing +/- that is an operator, not an exponent sign.
                if (bytes[i] == b'+' || bytes[i] == b'-')
                    && !(i > 0 && (bytes[i - 1] == b'e' || bytes[i - 1] == b'E'))
                {
                    break;
                }
                i += 1;
            }
        } else {
            // Push this UTF-8 character whole (i is at a char boundary here
            // because string/number branches only advance over ASCII bytes).
            let ch_len = utf8_char_len(c);
            let end = (i + ch_len).min(bytes.len());
            out.push_str(&sql[i..end]);
            i = end;
        }
    }
    out
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn utf8_char_len(first: u8) -> usize {
    if first < 0x80 {
        1
    } else if first >> 5 == 0b110 {
        2
    } else if first >> 4 == 0b1110 {
        3
    } else if first >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

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

    // --- SQL-07: literal stripping for lineage sinks ---

    #[test]
    fn strip_literals_removes_freeform_pii() {
        // The exact case redact_pii misses: a non-pattern sensitive literal.
        let sql = "SELECT * FROM patients WHERE patient_id = 'P-998877'";
        let stripped = strip_sql_literals(sql);
        assert!(!stripped.contains("P-998877"), "freeform literal leaked: {stripped}");
        assert!(stripped.contains("'?'"), "string literal must become a placeholder");
        // Structure (table + column) is preserved for debugging.
        assert!(stripped.contains("patients"));
        assert!(stripped.contains("patient_id"));
    }

    #[test]
    fn strip_literals_removes_numbers_but_keeps_identifiers() {
        let sql = "SELECT col1, col2 FROM t WHERE balance > 50000 AND year = 2026";
        let stripped = strip_sql_literals(sql);
        assert!(!stripped.contains("50000"), "numeric literal leaked: {stripped}");
        assert!(!stripped.contains("2026"), "numeric literal leaked: {stripped}");
        // `col1`/`col2` are identifiers with trailing digits, not literals.
        assert!(stripped.contains("col1"), "identifier must survive: {stripped}");
        assert!(stripped.contains("col2"), "identifier must survive: {stripped}");
    }

    #[test]
    fn strip_literals_handles_escaped_quotes() {
        let sql = "SELECT * FROM t WHERE name = 'O''Brien'";
        let stripped = strip_sql_literals(sql);
        assert!(!stripped.contains("Brien"), "escaped-quote literal leaked: {stripped}");
        assert!(stripped.contains("'?'"));
    }

    #[test]
    fn strip_literals_total_on_unterminated_quote() {
        // Must not panic; consumes to end of input.
        let sql = "SELECT * FROM t WHERE x = 'oops";
        let stripped = strip_sql_literals(sql);
        assert!(!stripped.contains("oops"));
    }

    #[test]
    fn strip_literals_preserves_non_ascii_structure() {
        // Non-ASCII identifier/comment bytes must pass through without panic.
        let sql = "SELECT * FROM t -- café WHERE x = 'sécret'";
        let stripped = strip_sql_literals(sql);
        assert!(!stripped.contains("sécret"));
        assert!(stripped.contains("café"));
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
