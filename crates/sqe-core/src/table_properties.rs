//! Iceberg write-mode table properties.
//!
//! Phase E introduced `write.delete.mode` dispatch at DELETE time. Phase H
//! adds the sibling properties `write.update.mode` and `write.merge.mode`.
//! All three accept `copy-on-write` or `merge-on-read` and default to
//! `copy-on-write` for backward compatibility.
//!
//! Upstream spec: <https://iceberg.apache.org/docs/latest/configuration/#write-properties>
//!
//! The parser is intentionally narrow. We reject typos like `"mor"` or
//! `"MoR"` at dispatch time so silent mode mismatches never happen.

use std::collections::HashMap;

use crate::error::{Result, SqeError};

/// Table property key: write mode for DELETE statements.
pub const WRITE_DELETE_MODE: &str = "write.delete.mode";

/// Table property key: write mode for UPDATE statements.
pub const WRITE_UPDATE_MODE: &str = "write.update.mode";

/// Table property key: write mode for MERGE statements.
pub const WRITE_MERGE_MODE: &str = "write.merge.mode";

/// Write mode for a DML statement.
///
/// - `CopyOnWrite`: read affected files, rewrite them in place, swap via
///   `RewriteFiles` action. Existing behaviour.
/// - `MergeOnRead`: emit equality-delete or position-delete files plus new
///   data files, commit via `RowDeltaAction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    #[default]
    CopyOnWrite,
    MergeOnRead,
}

impl WriteMode {
    /// String form used in Iceberg table properties.
    pub fn as_str(self) -> &'static str {
        match self {
            WriteMode::CopyOnWrite => "copy-on-write",
            WriteMode::MergeOnRead => "merge-on-read",
        }
    }

    /// Parse a table property value. Returns a typed error for invalid
    /// strings so users notice typos at the dispatch point.
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "copy-on-write" => Ok(WriteMode::CopyOnWrite),
            "merge-on-read" => Ok(WriteMode::MergeOnRead),
            other => Err(SqeError::Execution(format!(
                "unsupported write mode '{other}'; expected 'copy-on-write' or 'merge-on-read'"
            ))),
        }
    }
}

/// Resolve a write mode from a property map under a given key.
///
/// Missing keys default to `CopyOnWrite`. Invalid values return an error.
pub fn resolve_mode(properties: &HashMap<String, String>, key: &str) -> Result<WriteMode> {
    match properties.get(key) {
        None => Ok(WriteMode::CopyOnWrite),
        Some(value) => WriteMode::parse(value),
    }
}

/// Resolve the DELETE write mode. Convenience wrapper.
pub fn resolve_delete_mode(properties: &HashMap<String, String>) -> Result<WriteMode> {
    resolve_mode(properties, WRITE_DELETE_MODE)
}

/// Resolve the UPDATE write mode. Convenience wrapper.
pub fn resolve_update_mode(properties: &HashMap<String, String>) -> Result<WriteMode> {
    resolve_mode(properties, WRITE_UPDATE_MODE)
}

/// Resolve the MERGE write mode. Convenience wrapper.
pub fn resolve_merge_mode(properties: &HashMap<String, String>) -> Result<WriteMode> {
    resolve_mode(properties, WRITE_MERGE_MODE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_copy_on_write() {
        assert_eq!(
            WriteMode::parse("copy-on-write").unwrap(),
            WriteMode::CopyOnWrite
        );
    }

    #[test]
    fn parse_accepts_merge_on_read() {
        assert_eq!(
            WriteMode::parse("merge-on-read").unwrap(),
            WriteMode::MergeOnRead
        );
    }

    #[test]
    fn parse_rejects_camelcase_typos() {
        for bad in ["MoR", "mor", "cow", "COPY-ON-WRITE", "rewrite", ""] {
            assert!(
                WriteMode::parse(bad).is_err(),
                "expected {bad:?} to be rejected",
            );
        }
    }

    #[test]
    fn resolve_defaults_to_cow_when_missing() {
        let props = HashMap::new();
        assert_eq!(
            resolve_mode(&props, WRITE_UPDATE_MODE).unwrap(),
            WriteMode::CopyOnWrite,
        );
    }

    #[test]
    fn resolve_reads_each_property_key_independently() {
        let mut props = HashMap::new();
        props.insert(WRITE_DELETE_MODE.to_string(), "merge-on-read".to_string());
        props.insert(WRITE_UPDATE_MODE.to_string(), "copy-on-write".to_string());

        assert_eq!(resolve_delete_mode(&props).unwrap(), WriteMode::MergeOnRead);
        assert_eq!(resolve_update_mode(&props).unwrap(), WriteMode::CopyOnWrite);
        assert_eq!(resolve_merge_mode(&props).unwrap(), WriteMode::CopyOnWrite);
    }

    #[test]
    fn resolve_propagates_invalid_value() {
        let mut props = HashMap::new();
        props.insert(WRITE_MERGE_MODE.to_string(), "MoR".to_string());
        let err = resolve_merge_mode(&props).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("MoR"),
            "error should mention the bad value: {msg}"
        );
    }

    #[test]
    fn property_keys_match_iceberg_spec_names() {
        // These constants are in the Iceberg spec verbatim. Drift here
        // would silently make MoR tables created elsewhere take the CoW
        // path, so lock them in with a test.
        assert_eq!(WRITE_DELETE_MODE, "write.delete.mode");
        assert_eq!(WRITE_UPDATE_MODE, "write.update.mode");
        assert_eq!(WRITE_MERGE_MODE, "write.merge.mode");
    }
}
