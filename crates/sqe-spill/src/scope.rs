//! Spill scope identifiers: opaque query / stage / operator / partition / attempt.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};

/// Hierarchical ownership of spill segments. Paths under the spill root are
/// derived only from these opaque IDs — never from user SQL, table names, or
/// credentials.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpillScope {
    pub query_id: String,
    pub stage_id: String,
    pub operator_id: String,
    pub partition_id: u32,
    pub attempt_id: u32,
}

impl SpillScope {
    pub fn new(
        query_id: impl Into<String>,
        stage_id: impl Into<String>,
        operator_id: impl Into<String>,
        partition_id: u32,
        attempt_id: u32,
    ) -> Self {
        Self {
            query_id: sanitize_id(query_id.into()),
            stage_id: sanitize_id(stage_id.into()),
            operator_id: sanitize_id(operator_id.into()),
            partition_id,
            attempt_id,
        }
    }

    /// Relative directory under the spill root for this scope.
    pub fn relative_dir(&self) -> PathBuf {
        PathBuf::from(&self.query_id)
            .join(&self.stage_id)
            .join(&self.operator_id)
            .join(format!("p{}", self.partition_id))
            .join(format!("a{}", self.attempt_id))
    }

    /// Absolute directory for this scope under `root`.
    pub fn absolute_dir(&self, root: &Path) -> PathBuf {
        root.join(self.relative_dir())
    }
}

impl fmt::Display for SpillScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/p{}/a{}",
            self.query_id, self.stage_id, self.operator_id, self.partition_id, self.attempt_id
        )
    }
}

/// Keep path components free of `..`, separators, and empty segments.
fn sanitize_id(raw: String) -> String {
    let cleaned: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        "unknown".to_string()
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_rejects_path_escape() {
        let s = SpillScope::new("../evil", "s", "op", 0, 0);
        assert!(!s.query_id.contains(".."));
        assert!(!s.query_id.contains('/'));
    }

    #[test]
    fn relative_dir_layout() {
        let s = SpillScope::new("q1", "stage-a", "join", 3, 1);
        assert_eq!(s.relative_dir(), PathBuf::from("q1/stage-a/join/p3/a1"));
    }
}
