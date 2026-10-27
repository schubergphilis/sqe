//! Position deletes and deletion vectors on the worker scan path.
//!
//! The coordinator ships a ticket's delete-bearing data files as Iceberg
//! [`FileScanTask`]s (`ScanTask::delete_tasks`). Before a data file opens, the
//! worker loads its position-delete files and deletion vector into one
//! [`DeleteVector`] and hands the Parquet reader a [`RowSelection`] that skips
//! those rows. Equality deletes are not positions: the coordinator keeps such
//! snapshots local, and a ticket that carries one anyway is refused here.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use sqe_planner::ScanTask;

use crate::credential_channel::RefreshableCredentials;

/// The delete files of one scan task, plus the `FileIO` that reads them.
#[derive(Clone)]
pub struct DeleteFiles {
    tasks: Arc<HashMap<String, FileScanTask>>,
    file_io: FileIO,
}

impl DeleteFiles {
    /// Decode the ticket's delete tasks. `Ok(None)` when it carries none.
    pub fn from_task(task: &ScanTask) -> anyhow::Result<Option<Self>> {
        let Some(bytes) = task.delete_tasks.as_deref() else {
            return Ok(None);
        };
        let decoded = sqe_catalog::delete_tasks::decode(bytes)
            .map_err(|e| anyhow::anyhow!("undecodable delete tasks: {e}"))?;
        if let Some(t) = decoded
            .iter()
            .find(|t| sqe_catalog::delete_tasks::has_equality_deletes(t))
        {
            anyhow::bail!(
                "data file {} has equality deletes, which a worker does not apply",
                t.data_file_path()
            );
        }
        let tasks = decoded
            .into_iter()
            .map(|t| (t.data_file_path().to_string(), t))
            .collect();
        Ok(Some(Self {
            tasks: Arc::new(tasks),
            file_io: file_io(
                task,
                task.credentials.access_key(),
                task.credentials.secret_key(),
                task.credentials.session_token(),
            )?,
        }))
    }

    /// The same delete files, read with refreshed credentials.
    pub fn with_credentials(
        &self,
        task: &ScanTask,
        creds: &RefreshableCredentials,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            tasks: Arc::clone(&self.tasks),
            file_io: file_io(
                task,
                &creds.access_key_id,
                &creds.secret_access_key,
                &creds.session_token,
            )?,
        })
    }

    /// Every deleted row position of `data_file`, or `None` when nothing in
    /// it is deleted.
    pub async fn load(&self, data_file: &str) -> anyhow::Result<Option<DeleteVector>> {
        let Some(task) = self.tasks.get(data_file) else {
            return Ok(None);
        };
        let dv = iceberg::arrow::load_position_deletes(self.file_io.clone(), task).await?;
        Ok((!dv.is_empty()).then_some(dv))
    }
}

fn file_io(
    task: &ScanTask,
    access_key: &str,
    secret_key: &str,
    session_token: &str,
) -> anyhow::Result<FileIO> {
    crate::compaction::build_file_io(&sqe_compaction::wire::S3Conn {
        endpoint: task.s3_endpoint.clone(),
        region: task.s3_region.clone(),
        access_key: access_key.to_string(),
        secret_key: secret_key.to_string(),
        session_token: session_token.to_string(),
        path_style: task.s3_path_style,
        allow_http: task.s3_allow_http,
    })
    .map_err(|e| anyhow::anyhow!("building FileIO for delete files: {e}"))
}

/// The rows to read from the row groups `groups` (in read order) of a file
/// whose row groups hold `group_rows` rows each, skipping every position in
/// `deletes`.
///
/// Parquet applies a selection to the concatenation of the selected row
/// groups, while delete positions count from the start of the file. Each
/// selected group's positions are translated from its file offset.
pub fn row_selection(deletes: &DeleteVector, group_rows: &[i64], groups: &[usize]) -> RowSelection {
    let deleted: Vec<u64> = deletes.iter().collect();
    let mut offsets = Vec::with_capacity(group_rows.len());
    let mut at = 0u64;
    for rows in group_rows {
        offsets.push(at);
        at += (*rows).max(0) as u64;
    }

    let mut selectors = Vec::new();
    for &g in groups {
        let start = offsets[g];
        let end = start + group_rows[g].max(0) as u64;
        let mut cursor = start;
        let first = deleted.partition_point(|&p| p < start);
        for &pos in deleted[first..].iter().take_while(|&&p| p < end) {
            if pos > cursor {
                selectors.push(RowSelector::select((pos - cursor) as usize));
            }
            selectors.push(RowSelector::skip(1));
            cursor = pos + 1;
        }
        if end > cursor {
            selectors.push(RowSelector::select((end - cursor) as usize));
        }
    }
    // `From<Vec<RowSelector>>` merges adjacent runs of the same kind.
    selectors.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows_read(selection: &RowSelection, total: usize) -> Vec<usize> {
        let mut out = Vec::new();
        let mut at = 0;
        for s in selection.iter() {
            if !s.skip {
                out.extend(at..at + s.row_count);
            }
            at += s.row_count;
        }
        assert_eq!(at, total, "the selection must cover every selected row");
        out
    }

    #[test]
    fn positions_are_translated_to_the_selected_row_groups() {
        // Three groups of 4 rows: file rows 0..4, 4..8, 8..12. Rows 1, 5, 6
        // and 11 are deleted; only groups 1 and 2 are read.
        let dv: DeleteVector = [1u64, 5, 6, 11].into_iter().collect();
        let selection = row_selection(&dv, &[4, 4, 4], &[1, 2]);
        // Concatenated rows 0..8 are file rows 4..12.
        assert_eq!(rows_read(&selection, 8), vec![0, 3, 4, 5, 6]);
    }

    #[test]
    fn a_group_without_deletes_is_read_whole() {
        let dv: DeleteVector = [0u64].into_iter().collect();
        let selection = row_selection(&dv, &[3, 5], &[1]);
        assert_eq!(rows_read(&selection, 5), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn a_fully_deleted_group_reads_nothing() {
        let dv: DeleteVector = [2u64, 3].into_iter().collect();
        let selection = row_selection(&dv, &[2, 2], &[0, 1]);
        assert_eq!(rows_read(&selection, 4), vec![0, 1]);
    }
}
