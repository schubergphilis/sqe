//! Immutable spill segment descriptors.

use crate::scope::SpillScope;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Current on-disk segment format version (Arrow IPC payloads + CRC trailer).
pub const SEGMENT_FORMAT_VERSION: u32 = 1;

/// Magic bytes identifying an SQE spill segment file.
pub const SEGMENT_MAGIC: &[u8; 8] = b"SQESPILL";

/// Descriptor for a published (immutable) spill segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpillSegment {
    pub scope: SpillScope,
    /// Monotonic sequence within the scope.
    pub sequence: u64,
    /// Absolute path of the published segment (local backend) or object key.
    pub path: PathBuf,
    /// Schema fingerprint (stable hash of Arrow schema IPC bytes).
    pub schema_fingerprint: u64,
    /// Logical rows stored in the segment.
    pub row_count: u64,
    /// Sum of Arrow `get_array_memory_size` over written batches.
    pub logical_bytes: u64,
    /// On-disk / object size in bytes.
    pub physical_bytes: u64,
    /// Whole-segment CRC32C of the file body (excluding trailer).
    pub checksum: u32,
    pub format_version: u32,
}

impl SpillSegment {
    pub fn segment_file_name(sequence: u64) -> String {
        format!("seg-{sequence:08}.spill")
    }

    pub fn partial_file_name(sequence: u64) -> String {
        format!("seg-{sequence:08}.spill.partial")
    }
}
