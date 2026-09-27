//! Verify a native audit log file, or a contiguous extract of one, and
//! produce the facts a chain-of-custody record needs.
//!
//! The file holds two line kinds on one chain: canonical [`AuditEvent`]s,
//! hashed as `sha256(prev_hash || event JSON with hash blanked)`, and legacy
//! `AuditEntry` lines, hashed as `sha256(prev_hash || entry JSON)` with the
//! integrity block appended after hashing. Both are recomputed here.

use sha2::{Digest, Sha256};

use super::chain::{compute_hash, GENESIS};
use super::event::{AuditEvent, Integrity};
use super::logger::chain_hash_raw;

/// What a verified file contains. Everything a custody record should carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditVerifyReport {
    /// sha256 of the file bytes, hex.
    pub file_sha256: String,
    pub records: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    /// `prev_hash` of the first record. GENESIS means the extract starts at
    /// the very first record ever written; anything else must match the
    /// `hash` of the record before it in the source log.
    pub first_prev_hash: String,
    /// `hash` of the last record: the anchor the next record must chain to.
    pub last_hash: String,
    /// Line numbers (1-based) where a `BROKEN:` segment starts: the writer
    /// found a torn tail after a crash and restarted the chain on purpose.
    pub broken_segments: Vec<(u64, String)>,
}

impl AuditVerifyReport {
    pub fn starts_at_genesis(&self) -> bool {
        self.first_prev_hash == GENESIS
    }
}

/// Verify `bytes` (the whole file). `Err` names the first bad line.
pub fn verify_audit_bytes(bytes: &[u8]) -> Result<AuditVerifyReport, String> {
    let text = std::str::from_utf8(bytes).map_err(|e| format!("not UTF-8: {e}"))?;
    let mut report = AuditVerifyReport {
        file_sha256: hex::encode(Sha256::digest(bytes)),
        records: 0,
        first_seq: 0,
        last_seq: 0,
        first_prev_hash: String::new(),
        last_hash: String::new(),
        broken_segments: Vec::new(),
    };
    if !text.is_empty() && !text.ends_with('\n') {
        let n = text.lines().count();
        return Err(format!("line {n}: torn tail (no trailing newline)"));
    }
    for (i, line) in text.lines().enumerate() {
        let n = i as u64 + 1;
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value =
            serde_json::from_str(line).map_err(|e| format!("line {n}: not JSON: {e}"))?;
        let integrity: Integrity = value
            .get("integrity")
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
            .ok_or_else(|| format!("line {n}: no integrity block"))?;

        let expected = match serde_json::from_value::<AuditEvent>(value) {
            Ok(event) => compute_hash(&event),
            Err(_) => {
                // Legacy AuditEntry: the hashed body is the line minus the
                // appended `,"integrity":{...}` plus the closing brace.
                let cut = line
                    .rfind(",\"integrity\":")
                    .ok_or_else(|| format!("line {n}: no integrity block"))?;
                chain_hash_raw(
                    &integrity.prev_hash,
                    format!("{}}}", &line[..cut]).as_bytes(),
                )
            }
        };
        if expected != integrity.hash {
            return Err(format!(
                "line {n} (seq {}): hash mismatch, record altered",
                integrity.seq
            ));
        }

        if report.records == 0 {
            report.first_seq = integrity.seq;
            report.first_prev_hash = integrity.prev_hash.clone();
        } else if let Some(reason) = integrity.prev_hash.strip_prefix("BROKEN:") {
            report.broken_segments.push((n, reason.to_string()));
        } else {
            if integrity.seq != report.last_seq + 1 {
                return Err(format!(
                    "line {n}: seq {} follows {}, records missing",
                    integrity.seq, report.last_seq
                ));
            }
            if integrity.prev_hash != report.last_hash {
                return Err(format!(
                    "line {n} (seq {}): prev_hash does not chain",
                    integrity.seq
                ));
            }
        }
        report.records += 1;
        report.last_seq = integrity.seq;
        report.last_hash = integrity.hash;
    }
    if report.records == 0 {
        return Err("no audit records".into());
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{sample_query_event, AuditEntry, AuditFormat, AuditLogger};

    /// A real log with both line kinds, as the writer produces it.
    fn real_log() -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let logger = AuditLogger::with_config(path.to_str().unwrap(), AuditFormat::Native).unwrap();
        for _ in 0..3 {
            logger.log_event(sample_query_event());
        }
        logger.log(&serde_json::from_str::<AuditEntry>(LEGACY).unwrap());
        logger.log_event(sample_query_event());
        logger.flush();
        drop(logger);
        std::fs::read(&path).unwrap()
    }

    const LEGACY: &str = r#"{"timestamp":"2026-09-25T10:00:00Z","username":"bob","query_hash":"abc","statement_type":"query","duration_ms":1,"rows_returned":0,"status":"success"}"#;

    #[test]
    fn full_log_verifies_from_genesis() {
        let r = verify_audit_bytes(&real_log()).unwrap();
        assert_eq!(r.records, 5);
        assert_eq!((r.first_seq, r.last_seq), (0, 4));
        assert!(r.starts_at_genesis());
        assert!(r.broken_segments.is_empty());
    }

    #[test]
    fn contiguous_extract_verifies_and_anchors() {
        let full = real_log();
        let text = String::from_utf8(full).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        let extract = format!("{}\n{}\n", lines[1], lines[2]);
        let r = verify_audit_bytes(extract.as_bytes()).unwrap();
        assert_eq!((r.records, r.first_seq, r.last_seq), (2, 1, 2));
        assert!(!r.starts_at_genesis());
        let first_of_full: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(r.first_prev_hash, first_of_full["integrity"]["hash"]);
    }

    #[test]
    fn deleted_altered_and_torn_records_fail() {
        let text = String::from_utf8(real_log()).unwrap();
        let lines: Vec<&str> = text.lines().collect();

        let deleted = format!("{}\n{}\n", lines[0], lines[2]);
        assert!(verify_audit_bytes(deleted.as_bytes())
            .unwrap_err()
            .contains("records missing"));

        // Alter one canonical record and the legacy one.
        let altered = text.replacen("alice", "mallory", 1);
        assert!(verify_audit_bytes(altered.as_bytes())
            .unwrap_err()
            .contains("hash mismatch"));
        let altered = text.replacen("\"bob\"", "\"eve\"", 1);
        assert!(verify_audit_bytes(altered.as_bytes())
            .unwrap_err()
            .contains("hash mismatch"));

        let torn = &text[..text.len() - 5];
        assert!(verify_audit_bytes(torn.as_bytes())
            .unwrap_err()
            .contains("torn tail"));
    }
}
