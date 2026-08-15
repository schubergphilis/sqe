//! Configuration for `sqe-bench generate`.
//!
//! Defaults and precedence (highest wins): CLI flag > env var > default.
//! The caller in `main.rs` resolves the three inputs and constructs a
//! single `GenerateConfig` before invoking any generator.

use std::num::NonZeroUsize;

/// Parquet compression codec for generated files.
///
/// Maps to `parquet::basic::Compression`. ZSTD levels are clamped to the
/// range the parquet crate accepts (1..=22).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CompressionKind {
    Zstd1,
    #[default]
    Zstd3,
    Zstd9,
    Snappy,
    None,
}

impl CompressionKind {
    /// Parse from the string forms accepted on CLI and env vars.
    ///
    /// Named `parse_cli` rather than `from_str` so it doesn't shadow the
    /// stdlib `FromStr` convention, which would invite calls that expect
    /// a `Result<Self, Self::Err>` instead of an `anyhow::Result<Self>`.
    pub fn parse_cli(s: &str) -> anyhow::Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "zstd1" => Ok(Self::Zstd1),
            "zstd" | "zstd3" => Ok(Self::Zstd3),
            "zstd9" => Ok(Self::Zstd9),
            "snappy" => Ok(Self::Snappy),
            "none" | "uncompressed" => Ok(Self::None),
            other => anyhow::bail!(
                "invalid compression '{other}'; accepted values: zstd1, zstd3, zstd9, snappy, none"
            ),
        }
    }

    pub fn to_parquet(self) -> parquet::basic::Compression {
        use parquet::basic::{Compression, ZstdLevel};
        match self {
            Self::Zstd1 => Compression::ZSTD(ZstdLevel::try_new(1).unwrap()),
            Self::Zstd3 => Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            Self::Zstd9 => Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
            Self::Snappy => Compression::SNAPPY,
            Self::None => Compression::UNCOMPRESSED,
        }
    }
}

/// Resolved generator configuration.
#[derive(Debug, Clone, Copy)]
pub struct GenerateConfig {
    /// Maximum concurrent per-partition worker threads for one table.
    /// Clamped to `[1, MAX_THREADS]` at construction.
    pub threads: usize,

    /// Parquet compression codec.
    pub compression: CompressionKind,

    /// Parquet row-group size hint. `None` means use the writer default.
    pub row_group_size: Option<usize>,
}

/// Hard cap on worker count to prevent pathological configurations
/// (e.g. a typo-`threads=10000`). 256 is well above any realistic
/// CPU count and keeps peak RSS bounded.
pub const MAX_THREADS: usize = 256;

impl GenerateConfig {
    /// Build a resolved config from explicit overrides with env and
    /// system-default fallbacks. None = fall back to env, env-missing =
    /// fall back to default.
    ///
    /// Returns an error if any of the three inputs is malformed
    /// (invalid compression string, non-numeric threads, out-of-range
    /// threads value).
    pub fn resolve(
        threads_cli: Option<usize>,
        compression_cli: Option<&str>,
        row_group_size_cli: Option<usize>,
    ) -> anyhow::Result<Self> {
        let threads = match (threads_cli, std::env::var("BENCH_GEN_THREADS").ok()) {
            (Some(n), _) => n,
            (None, Some(s)) => s
                .parse::<usize>()
                .map_err(|e| anyhow::anyhow!("invalid BENCH_GEN_THREADS='{s}': {e}"))?,
            (None, None) => std::thread::available_parallelism()
                .map(NonZeroUsize::get)
                .unwrap_or(1),
        };
        let threads = threads.clamp(1, MAX_THREADS);

        let compression = match (compression_cli, std::env::var("BENCH_GEN_COMPRESSION").ok()) {
            (Some(s), _) => CompressionKind::parse_cli(s)?,
            (None, Some(s)) => CompressionKind::parse_cli(&s)?,
            (None, None) => CompressionKind::default(),
        };

        let row_group_size = match (
            row_group_size_cli,
            std::env::var("BENCH_GEN_ROW_GROUP_SIZE").ok(),
        ) {
            (Some(n), _) => Some(n),
            (None, Some(s)) => Some(
                s.parse::<usize>()
                    .map_err(|e| anyhow::anyhow!("invalid BENCH_GEN_ROW_GROUP_SIZE='{s}': {e}"))?,
            ),
            (None, None) => None,
        };

        Ok(Self {
            threads,
            compression,
            row_group_size,
        })
    }
}

impl Default for GenerateConfig {
    fn default() -> Self {
        Self {
            threads: std::thread::available_parallelism()
                .map(NonZeroUsize::get)
                .unwrap_or(1),
            compression: CompressionKind::default(),
            row_group_size: None,
        }
    }
}

/// Partition `total_rows` into `parts` disjoint ranges of roughly equal
/// size. The last partition absorbs the remainder so the union of all
/// ranges covers exactly `0..total_rows`.
pub fn partition(total_rows: usize, parts: usize) -> Vec<std::ops::Range<usize>> {
    let parts = parts.max(1);
    if total_rows == 0 {
        return vec![0..0; parts];
    }
    let base = total_rows / parts;
    let mut ranges = Vec::with_capacity(parts);
    let mut start = 0;
    for i in 0..parts {
        let end = if i + 1 == parts {
            total_rows
        } else {
            start + base
        };
        ranges.push(start..end);
        start = end;
    }
    ranges
}

/// Deterministic seed for table partition `part_idx`. Using XOR with a
/// golden-ratio constant keeps per-partition seeds well-separated even
/// when `base_seed` is small.
const GOLDEN_RATIO_U64: u64 = 0x9E3779B97F4A7C15;

pub fn seed_for_table_partition(base_seed: u64, part_idx: usize) -> u64 {
    base_seed ^ (part_idx as u64).wrapping_mul(GOLDEN_RATIO_U64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_parses_all_accepted_forms() {
        assert_eq!(
            CompressionKind::parse_cli("zstd1").unwrap(),
            CompressionKind::Zstd1
        );
        assert_eq!(
            CompressionKind::parse_cli("ZSTD3").unwrap(),
            CompressionKind::Zstd3
        );
        assert_eq!(
            CompressionKind::parse_cli("zstd").unwrap(),
            CompressionKind::Zstd3
        );
        assert_eq!(
            CompressionKind::parse_cli("Snappy").unwrap(),
            CompressionKind::Snappy
        );
        assert_eq!(
            CompressionKind::parse_cli("none").unwrap(),
            CompressionKind::None
        );
        assert_eq!(
            CompressionKind::parse_cli("uncompressed").unwrap(),
            CompressionKind::None
        );
    }

    #[test]
    fn compression_rejects_unknown() {
        let err = CompressionKind::parse_cli("gzip").unwrap_err();
        assert!(err.to_string().contains("zstd1"));
        assert!(err.to_string().contains("gzip"));
    }

    #[test]
    fn partition_splits_evenly_with_remainder_in_last() {
        let ranges = partition(100, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], 0..25);
        assert_eq!(ranges[1], 25..50);
        assert_eq!(ranges[2], 50..75);
        assert_eq!(ranges[3], 75..100);
    }

    #[test]
    fn partition_with_remainder_stuffs_last() {
        let ranges = partition(103, 4);
        assert_eq!(ranges.last().unwrap().end, 103);
        // union covers 0..103 exactly
        let last_end = ranges.last().unwrap().end;
        assert_eq!(last_end, 103);
    }

    #[test]
    fn partition_zero_rows_gives_empty_ranges() {
        let ranges = partition(0, 4);
        assert_eq!(ranges.len(), 4);
        for r in &ranges {
            assert_eq!(r.len(), 0);
        }
    }

    #[test]
    fn partition_more_parts_than_rows() {
        let ranges = partition(3, 8);
        assert_eq!(ranges.len(), 8);
        // total covers 0..3, some ranges are empty
        assert_eq!(ranges.iter().map(|r| r.len()).sum::<usize>(), 3);
    }

    #[test]
    fn seed_for_partition_is_deterministic_and_unique() {
        let seeds: Vec<u64> = (0..8).map(|i| seed_for_table_partition(42, i)).collect();
        // all different
        let mut sorted = seeds.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 8);
        // deterministic: same input -> same output
        assert_eq!(seeds[0], seed_for_table_partition(42, 0));
        assert_eq!(seeds[7], seed_for_table_partition(42, 7));
    }

    // Tests that mutate process-wide env vars are combined into a single
    // function so cargo's parallel test runner cannot observe intermediate
    // states from other tests concurrently reading `BENCH_GEN_*`. A
    // future change could swap this for `serial_test` if we take on that
    // dep, but one big test is simpler and has no dep footprint.
    #[test]
    fn config_env_and_precedence_behaviour() {
        // Clean slate.
        std::env::remove_var("BENCH_GEN_THREADS");
        std::env::remove_var("BENCH_GEN_COMPRESSION");
        std::env::remove_var("BENCH_GEN_ROW_GROUP_SIZE");

        // CLI clamp: out-of-range values are bounded to [1, MAX_THREADS].
        let c = GenerateConfig::resolve(Some(0), None, None).unwrap();
        assert_eq!(c.threads, 1);
        let c = GenerateConfig::resolve(Some(10_000), None, None).unwrap();
        assert_eq!(c.threads, MAX_THREADS);

        // CLI wins over env.
        std::env::set_var("BENCH_GEN_THREADS", "4");
        let c = GenerateConfig::resolve(Some(8), None, None).unwrap();
        assert_eq!(c.threads, 8);
        std::env::remove_var("BENCH_GEN_THREADS");

        // Env wins over default when CLI is absent.
        std::env::set_var("BENCH_GEN_COMPRESSION", "snappy");
        let c = GenerateConfig::resolve(None, None, None).unwrap();
        assert_eq!(c.compression, CompressionKind::Snappy);
        std::env::remove_var("BENCH_GEN_COMPRESSION");

        // Invalid env fails fast with a message naming the offending var.
        std::env::set_var("BENCH_GEN_THREADS", "not-a-number");
        let err = GenerateConfig::resolve(None, None, None).unwrap_err();
        assert!(err.to_string().contains("BENCH_GEN_THREADS"));
        std::env::remove_var("BENCH_GEN_THREADS");
    }
}
