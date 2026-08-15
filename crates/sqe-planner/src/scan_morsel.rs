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

/// Split a single file into byte-range morsels without reading the Parquet
/// footer. The worker resolves each range to row groups at open time (same
/// idea as the vendored `split_file_scan_task` byte-range path).
///
/// Files at or below `target_bytes` yield a single whole-file morsel.
/// Larger files are sliced into consecutive ranges of about `target_bytes`,
/// never exceeding `max_bytes` except for the final partial slice.
///
/// **Deletes:** callers must pass `allow_subfile_split = false` when the
/// snapshot has delete files; the result is then always one whole-file morsel.
pub fn plan_file_byte_morsels(
    file_path: &str,
    file_size_bytes: u64,
    target_bytes: u64,
    max_bytes: u64,
    decoded_factor: u64,
    allow_subfile_split: bool,
) -> Vec<ScanMorsel> {
    let target = target_bytes.max(1);
    let max_b = max_bytes.max(target);

    if !allow_subfile_split || file_size_bytes <= target {
        return vec![ScanMorsel {
            morsel_id: format!("{file_path}#file"),
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

    let chunk = target.min(max_b);
    let mut out = Vec::new();
    let mut start = 0u64;
    let mut idx = 0u32;
    while start < file_size_bytes {
        let end = (start + chunk).min(file_size_bytes);
        let compressed = end - start;
        out.push(ScanMorsel {
            morsel_id: format!("{file_path}#b{start}-{end}"),
            file_path: file_path.to_string(),
            file_size_bytes,
            row_group_start: 0,
            row_group_end: None,
            start_byte: Some(start),
            end_byte: Some(end),
            compressed_bytes_estimate: compressed,
            decoded_bytes_estimate: compressed.saturating_mul(decoded_factor),
        });
        start = end;
        idx += 1;
        let _ = idx;
    }
    out
}

/// Expand a list of whole files into scan morsels for distributed assignment.
///
/// When `allow_subfile_split` is false (delete-bearing snapshots), every file
/// stays as one morsel. Otherwise files larger than `target_bytes` are
/// byte-range split so a multi-gigabyte file becomes many worker tasks.
pub fn plan_scan_morsels(
    files: &[(String, u64)],
    target_bytes: u64,
    max_bytes: u64,
    allow_subfile_split: bool,
) -> Vec<ScanMorsel> {
    const DECODED_FACTOR: u64 = 4;
    let mut out = Vec::with_capacity(files.len());
    for (path, size) in files {
        out.extend(plan_file_byte_morsels(
            path,
            *size,
            target_bytes,
            max_bytes,
            DECODED_FACTOR,
            allow_subfile_split,
        ));
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
        let morsels = group_row_groups_into_morsels("s3://b/f.parquet", 1000, &[], 128, 256, 4);
        assert_eq!(morsels.len(), 1);
        assert_eq!(morsels[0].row_group_start, 0);
        assert!(morsels[0].row_group_end.is_none());
        assert_eq!(morsels[0].compressed_bytes_estimate, 1000);
    }

    #[test]
    fn byte_morsels_split_large_file() {
        let mb = 1024 * 1024u64;
        let morsels =
            plan_file_byte_morsels("s3://b/big.parquet", 500 * mb, 128 * mb, 256 * mb, 4, true);
        assert!(
            morsels.len() >= 3,
            "expected multiple morsels, got {}",
            morsels.len()
        );
        let total: u64 = morsels.iter().map(|m| m.compressed_bytes_estimate).sum();
        assert_eq!(total, 500 * mb);
        assert!(morsels
            .iter()
            .all(|m| m.start_byte.is_some() && m.end_byte.is_some()));
        // Ranges must be contiguous and non-overlapping.
        let mut cursor = 0u64;
        for m in &morsels {
            assert_eq!(m.start_byte.unwrap(), cursor);
            cursor = m.end_byte.unwrap();
        }
        assert_eq!(cursor, 500 * mb);
    }

    #[test]
    fn deletes_force_whole_file_morsel() {
        let mb = 1024 * 1024u64;
        let morsels = plan_file_byte_morsels(
            "s3://b/del.parquet",
            500 * mb,
            128 * mb,
            256 * mb,
            4,
            false, // has deletes
        );
        assert_eq!(morsels.len(), 1);
        assert!(morsels[0].start_byte.is_none());
        assert!(morsels[0].end_byte.is_none());
    }

    #[test]
    fn plan_scan_morsels_mixes_small_and_large() {
        let mb = 1024 * 1024u64;
        let files = vec![
            ("s3://b/small.parquet".into(), 10 * mb),
            ("s3://b/large.parquet".into(), 300 * mb),
        ];
        let morsels = plan_scan_morsels(&files, 128 * mb, 256 * mb, true);
        // small = 1, large = at least 2
        assert!(morsels.len() >= 3);
        assert_eq!(
            morsels
                .iter()
                .filter(|m| m.file_path.contains("small"))
                .count(),
            1
        );
    }
}
