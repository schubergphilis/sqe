//! Scan morsels: sub-file work units for distributed scans.
//!
//! Phase 2 of the bounded-memory plan. A morsel is a stable, retryable unit of
//! scan work — typically adjacent Parquet row groups (or a byte range that the
//! reader resolves to row groups) sized to a target of 64–128 MiB compressed.
//!
//! See `docs/superpowers/plans/2026-07-25-bounded-memory-spill-execution.md`.

use serde::{Deserialize, Serialize};

/// Current encoding version for morsel-aware [`crate::ScanTask`] tickets.
///
/// Workers must reject unsupported versions. Version 1 is the pre-morsel
/// whole-file ticket; version 2 adds optional row-group / byte ranges.
pub const SCAN_TASK_VERSION_V1: u32 = 1;
pub const SCAN_TASK_VERSION_V2: u32 = 2;
pub const SCAN_TASK_VERSION_CURRENT: u32 = SCAN_TASK_VERSION_V2;

/// Default target compressed size for a morsel (128 MiB).
pub const DEFAULT_MORSEL_TARGET_BYTES: u64 = 128 * 1024 * 1024;
/// Default maximum compressed size for a morsel (256 MiB).
pub const DEFAULT_MORSEL_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// A versioned sub-file scan unit.
///
/// Carries either explicit row-group indices or a byte range (which the
/// Parquet reader resolves to row groups at open time). Delete-aware scans
/// that cannot safely restrict deletes to a sub-file range must keep a
/// file-level morsel (`row_group_start = 0`, `row_group_end = None`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanMorsel {
    /// Stable identifier for retry / dedup (opaque string).
    pub morsel_id: String,
    /// Absolute file path (S3 URL or object-store key URL).
    pub file_path: String,
    /// Full file size in bytes when known.
    pub file_size_bytes: u64,
    /// Inclusive start row-group index.
    pub row_group_start: u32,
    /// Exclusive end row-group index. `None` means "through the last group".
    pub row_group_end: Option<u32>,
    /// Optional compressed byte offset start (inclusive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_byte: Option<u64>,
    /// Optional compressed byte offset end (exclusive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_byte: Option<u64>,
    /// Estimated compressed bytes covered by this morsel.
    pub compressed_bytes_estimate: u64,
    /// Estimated decoded Arrow bytes (heuristic; used for admission only).
    pub decoded_bytes_estimate: u64,
}

/// One Parquet row group’s location and size, used to build morsels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowGroupSlice {
    pub index: u32,
    /// Compressed size of the row group in bytes.
    pub compressed_bytes: u64,
    /// Byte offset of the row group in the file (if known).
    pub offset: Option<u64>,
}

/// Group adjacent row groups into morsels of approximately `target_bytes`,
/// never exceeding `max_bytes` for a multi-group morsel. A single row group
/// larger than `max_bytes` becomes its own morsel (cannot split further
/// without page-level work).
pub fn group_row_groups_into_morsels(
    file_path: &str,
    file_size_bytes: u64,
    row_groups: &[RowGroupSlice],
    target_bytes: u64,
    max_bytes: u64,
    decoded_factor: u64,
) -> Vec<ScanMorsel> {
    if row_groups.is_empty() {
        // Whole-file fallback morsel.
        return vec![ScanMorsel {
            morsel_id: format!("{file_path}#0"),
            file_path: file_path.to_string(),
            file_size_bytes,
            row_group_start: 0,
            row_group_end: None,
            start_byte: None,
            end_byte: None,
            compressed_bytes_estimate: file_size_bytes,
            decoded_bytes_estimate: file_size_bytes.saturating_mul(decoded_factor),
        }];
    }

    let target = target_bytes.max(1);
    let max_b = max_bytes.max(target);
    let mut out = Vec::new();
    let mut i = 0usize;

    while i < row_groups.len() {
        let start = i;
        let mut compressed = row_groups[i].compressed_bytes;
        i += 1;
        while i < row_groups.len() {
            let next = row_groups[i].compressed_bytes;
            if compressed >= target {
                break;
            }
            if compressed.saturating_add(next) > max_b {
                break;
            }
            compressed += next;
            i += 1;
        }
        let rg_start = row_groups[start].index;
        let rg_end = row_groups[i - 1].index + 1;
        let start_byte = row_groups[start].offset;
        let end_byte = match (row_groups[i - 1].offset, row_groups[i - 1].compressed_bytes) {
            (Some(off), sz) => Some(off.saturating_add(sz)),
            _ => None,
        };
        out.push(ScanMorsel {
            morsel_id: format!("{file_path}#{rg_start}-{rg_end}"),
            file_path: file_path.to_string(),
            file_size_bytes,
            row_group_start: rg_start,
            row_group_end: Some(rg_end),
            start_byte,
            end_byte,
            compressed_bytes_estimate: compressed,
            decoded_bytes_estimate: compressed.saturating_mul(decoded_factor),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rgs(sizes: &[u64]) -> Vec<RowGroupSlice> {
        let mut off = 0u64;
        sizes
            .iter()
            .enumerate()
            .map(|(i, &sz)| {
                let s = RowGroupSlice {
                    index: i as u32,
                    compressed_bytes: sz,
                    offset: Some(off),
                };
                off += sz;
                s
            })
            .collect()
    }

    #[test]
    fn groups_adjacent_row_groups_to_target() {
        // 4 x 40 MiB → two morsels at 80 MiB with target 64 MiB / max 128 MiB.
        let mb = 1024 * 1024u64;
        let slices = rgs(&[40 * mb, 40 * mb, 40 * mb, 40 * mb]);
        let morsels = group_row_groups_into_morsels(
            "s3://b/f.parquet",
            160 * mb,
            &slices,
            64 * mb,
            128 * mb,
            4,
        );
        assert_eq!(morsels.len(), 2);
        assert_eq!(morsels[0].row_group_start, 0);
        assert_eq!(morsels[0].row_group_end, Some(2));
        assert_eq!(morsels[1].row_group_start, 2);
        assert_eq!(morsels[1].row_group_end, Some(4));
        assert_eq!(morsels[0].compressed_bytes_estimate, 80 * mb);
    }

    #[test]
    fn single_oversized_row_group_is_own_morsel() {
        let mb = 1024 * 1024u64;
        let slices = rgs(&[300 * mb, 10 * mb]);
        let morsels = group_row_groups_into_morsels(
            "s3://b/f.parquet",
            310 * mb,
            &slices,
            64 * mb,
            128 * mb,
            4,
        );
        assert_eq!(morsels.len(), 2);
        assert_eq!(morsels[0].compressed_bytes_estimate, 300 * mb);
        assert_eq!(morsels[0].row_group_end, Some(1));
        assert_eq!(morsels[1].compressed_bytes_estimate, 10 * mb);
    }

    #[test]
    fn empty_row_groups_yield_whole_file_morsel() {
        let morsels =
            group_row_groups_into_morsels("s3://b/f.parquet", 1000, &[], 128, 256, 4);
        assert_eq!(morsels.len(), 1);
        assert_eq!(morsels[0].row_group_start, 0);
        assert!(morsels[0].row_group_end.is_none());
        assert_eq!(morsels[0].compressed_bytes_estimate, 1000);
    }
}
