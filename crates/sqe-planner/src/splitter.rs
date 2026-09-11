use tracing::debug;

/// Bin-pack data files into tasks targeting a configurable size.
///
/// Small files are grouped together until reaching `target_size_bytes`.
/// Uses first-fit-decreasing: sort files largest-first, assign each to
/// the smallest bin that won't exceed the target. If all bins exceed
/// the target, add a new bin (up to `max_bins`). If `max_bins` is reached,
/// add to the smallest bin anyway.
///
/// Returns a Vec of file groups, where each group is a Vec of (path, size) tuples.
pub fn bin_pack_files(
    files: Vec<(String, u64)>,
    target_size_bytes: u64,
    max_bins: usize,
) -> Vec<Vec<(String, u64)>> {
    if files.is_empty() || max_bins == 0 {
        return vec![];
    }

    // Sort largest first (first-fit-decreasing heuristic)
    let mut sorted = files;
    sorted.sort_by_key(|f| std::cmp::Reverse(f.1));

    let mut bins: Vec<(u64, Vec<(String, u64)>)> = vec![]; // (total_size, files)

    for (path, size) in sorted {
        // Find the smallest bin where adding this file stays under target
        let best_fit = bins
            .iter()
            .enumerate()
            .filter(|(_, (total, _))| *total + size <= target_size_bytes)
            .min_by_key(|(_, (total, _))| *total)
            .map(|(i, _)| i);

        if let Some(idx) = best_fit {
            bins[idx].0 += size;
            bins[idx].1.push((path, size));
        } else if bins.len() < max_bins {
            // Start a new bin
            bins.push((size, vec![(path, size)]));
        } else {
            // All bins full — add to the smallest (least overloaded)
            let min_idx = bins
                .iter()
                .enumerate()
                .min_by_key(|(_, (total, _))| *total)
                .map(|(i, _)| i)
                .unwrap();
            bins[min_idx].0 += size;
            bins[min_idx].1.push((path, size));
        }
    }

    debug!(
        bin_count = bins.len(),
        total_files = bins.iter().map(|(_, f)| f.len()).sum::<usize>(),
        bin_sizes_mb = ?bins.iter().map(|(s, _)| s / (1024 * 1024)).collect::<Vec<_>>(),
        "Bin-packed files"
    );

    bins.into_iter().map(|(_, files)| files).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1024 * 1024;

    #[test]
    fn test_bin_pack_single_large_file() {
        let files = vec![("big.parquet".to_string(), 500 * MB)];
        let bins = bin_pack_files(files, 256 * MB, 10);
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].len(), 1);
    }

    #[test]
    fn test_bin_pack_groups_small_files() {
        // 10 files of 50MB each, target 256MB → should pack into 2 bins
        let files: Vec<_> = (0..10)
            .map(|i| (format!("file{i}.parquet"), 50 * MB))
            .collect();
        let bins = bin_pack_files(files, 256 * MB, 10);
        assert!(bins.len() <= 3, "expected 2-3 bins, got {}", bins.len());
        // Total files should be preserved
        let total: usize = bins.iter().map(|b| b.len()).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_bin_pack_respects_max_bins() {
        let files: Vec<_> = (0..20)
            .map(|i| (format!("file{i}.parquet"), 100 * MB))
            .collect();
        let bins = bin_pack_files(files, 256 * MB, 3);
        assert_eq!(bins.len(), 3, "should not exceed max_bins");
        let total: usize = bins.iter().map(|b| b.len()).sum();
        assert_eq!(total, 20, "all files must be assigned");
    }

    #[test]
    fn test_bin_pack_empty_input() {
        let bins = bin_pack_files(vec![], 256 * MB, 10);
        assert!(bins.is_empty());
    }

    #[test]
    fn test_bin_pack_zero_max_bins() {
        let files = vec![("f.parquet".to_string(), 100 * MB)];
        let bins = bin_pack_files(files, 256 * MB, 0);
        assert!(bins.is_empty());
    }

    #[test]
    fn test_bin_pack_largest_first_balancing() {
        // Files: 200MB, 150MB, 100MB, 50MB. Target: 256MB, max: 2 bins
        // Optimal: bin1=[200,50]=250MB, bin2=[150,100]=250MB
        let files = vec![
            ("a.parquet".to_string(), 200 * MB),
            ("b.parquet".to_string(), 150 * MB),
            ("c.parquet".to_string(), 100 * MB),
            ("d.parquet".to_string(), 50 * MB),
        ];
        let bins = bin_pack_files(files, 256 * MB, 2);
        assert_eq!(bins.len(), 2);
        // Both bins should be roughly balanced (within 50MB)
        let sizes: Vec<u64> = bins
            .iter()
            .map(|b| b.iter().map(|(_, s)| s).sum::<u64>())
            .collect();
        let diff = sizes[0].abs_diff(sizes[1]);
        assert!(
            diff <= 50 * MB,
            "bins should be balanced, diff={}MB",
            diff / MB
        );
    }

    #[test]
    fn test_bin_pack_preserves_all_files() {
        let files: Vec<_> = (0..100)
            .map(|i| (format!("file{i}.parquet"), (i as u64 + 1) * MB))
            .collect();
        let bins = bin_pack_files(files, 256 * MB, 50);
        let total: usize = bins.iter().map(|b| b.len()).sum();
        assert_eq!(total, 100, "all files must be assigned");
    }

    #[test]
    fn test_bin_pack_single_file_per_bin_when_large() {
        // 5 files of 300MB each, target 256MB → each in its own bin
        let files: Vec<_> = (0..5)
            .map(|i| (format!("file{i}.parquet"), 300 * MB))
            .collect();
        let bins = bin_pack_files(files, 256 * MB, 10);
        assert_eq!(bins.len(), 5, "each large file gets its own bin");
    }
}
