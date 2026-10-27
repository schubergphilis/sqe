//! Wire form of `ScanTask::delete_tasks`: the Iceberg file scan tasks of the
//! data files in a distributed scan ticket that have delete files.
//!
//! The coordinator plans the scan with iceberg-rust, which already decided
//! which delete files apply to which data file (sequence numbers, partition
//! scope). Shipping that decision as the planner's own `FileScanTask` means the
//! worker re-derives nothing: it hands the task to
//! `iceberg::arrow::load_position_deletes` and gets the deleted row positions.

use iceberg::scan::FileScanTask;
use iceberg::spec::DataContentType;

/// Encode the tasks for the ticket.
pub fn encode(tasks: &[FileScanTask]) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(tasks)
}

/// Decode the tasks a ticket carries.
pub fn decode(bytes: &[u8]) -> serde_json::Result<Vec<FileScanTask>> {
    serde_json::from_slice(bytes)
}

/// True when any delete file of `task` is an equality delete. Workers apply
/// only position deletes and deletion vectors, so the coordinator scans a
/// snapshot with equality deletes itself.
pub fn has_equality_deletes(task: &FileScanTask) -> bool {
    task.deletes()
        .iter()
        .any(|d| d.file_type == DataContentType::EqualityDeletes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::scan::FileScanTaskDeleteFile;
    use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};

    use super::*;

    fn task(deletes: Vec<FileScanTaskDeleteFile>) -> FileScanTask {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        FileScanTask::builder()
            .with_file_size_in_bytes(1024)
            .with_start(0)
            .with_length(1024)
            .with_data_file_path("s3://b/t/data/f.parquet".to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(Arc::new(schema))
            .with_project_field_ids(vec![1])
            .with_deletes(deletes)
            .with_case_sensitive(false)
            .build()
            .unwrap()
    }

    fn deletion_vector() -> FileScanTaskDeleteFile {
        FileScanTaskDeleteFile::builder()
            .with_file_path("s3://b/t/data/dv.puffin".to_string())
            .with_file_size_in_bytes(256)
            .with_file_type(DataContentType::PositionDeletes)
            .with_file_format(DataFileFormat::Puffin)
            .with_partition_spec_id(0)
            .with_referenced_data_file(Some("s3://b/t/data/f.parquet".to_string()))
            .with_content_offset(Some(4))
            .with_content_size_in_bytes(Some(40))
            .with_record_count(Some(2))
            .with_sequence_number(3)
            .build()
    }

    #[test]
    fn a_deletion_vector_survives_the_round_trip() {
        let tasks = vec![task(vec![deletion_vector()])];
        let back = decode(&encode(&tasks).unwrap()).unwrap();
        assert_eq!(back, tasks);
        let dv = &back[0].deletes()[0];
        assert_eq!(dv.content_offset, Some(4));
        assert_eq!(dv.content_size_in_bytes, Some(40));
        assert_eq!(
            dv.referenced_data_file.as_deref(),
            Some("s3://b/t/data/f.parquet")
        );
        assert!(!has_equality_deletes(&back[0]));
    }

    #[test]
    fn an_equality_delete_is_reported() {
        let eq = FileScanTaskDeleteFile::builder()
            .with_file_path("s3://b/t/data/eq.parquet".to_string())
            .with_file_size_in_bytes(256)
            .with_file_type(DataContentType::EqualityDeletes)
            .with_file_format(DataFileFormat::Parquet)
            .with_partition_spec_id(0)
            .with_equality_ids(Some(vec![1]))
            .build();
        assert!(has_equality_deletes(&task(vec![deletion_vector(), eq])));
    }
}
