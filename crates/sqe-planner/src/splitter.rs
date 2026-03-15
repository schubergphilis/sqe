use tracing::debug;

/// Distributes data file paths across N workers using round-robin assignment.
///
/// Returns a Vec of length `num_workers`, where each element is the list
/// of file paths assigned to that worker. Empty workers get empty Vecs.
///
/// If `num_workers` is 0 or `files` is empty, returns an empty Vec.
pub fn split_files(files: Vec<String>, num_workers: usize) -> Vec<Vec<String>> {
    if num_workers == 0 || files.is_empty() {
        return vec![];
    }

    let mut groups: Vec<Vec<String>> = (0..num_workers).map(|_| Vec::new()).collect();

    for (i, file) in files.into_iter().enumerate() {
        groups[i % num_workers].push(file);
    }

    debug!(
        num_workers,
        files_per_worker = ?groups.iter().map(|g| g.len()).collect::<Vec<_>>(),
        "Split files across workers"
    );

    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_files_even() {
        let files: Vec<String> = (0..6).map(|i| format!("file{i}.parquet")).collect();
        let groups = split_files(files, 3);

        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0], vec!["file0.parquet", "file3.parquet"]);
        assert_eq!(groups[1], vec!["file1.parquet", "file4.parquet"]);
        assert_eq!(groups[2], vec!["file2.parquet", "file5.parquet"]);
    }

    #[test]
    fn test_split_files_uneven() {
        let files: Vec<String> = (0..5).map(|i| format!("file{i}.parquet")).collect();
        let groups = split_files(files, 3);

        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].len(), 2); // file0, file3
        assert_eq!(groups[1].len(), 2); // file1, file4
        assert_eq!(groups[2].len(), 1); // file2
    }

    #[test]
    fn test_split_files_more_workers_than_files() {
        let files = vec!["file0.parquet".to_string()];
        let groups = split_files(files, 5);

        assert_eq!(groups.len(), 5);
        assert_eq!(groups[0], vec!["file0.parquet"]);
        assert!(groups[1].is_empty());
        assert!(groups[4].is_empty());
    }

    #[test]
    fn test_split_files_single_worker() {
        let files: Vec<String> = (0..3).map(|i| format!("file{i}.parquet")).collect();
        let groups = split_files(files, 1);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 3);
    }

    #[test]
    fn test_split_files_empty() {
        let groups = split_files(vec![], 3);
        assert!(groups.is_empty());
    }

    #[test]
    fn test_split_files_zero_workers() {
        let files = vec!["file0.parquet".to_string()];
        let groups = split_files(files, 0);
        assert!(groups.is_empty());
    }
}
