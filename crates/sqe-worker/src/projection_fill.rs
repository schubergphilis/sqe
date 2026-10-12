//! Worker-side conformance of decoded batches to the scan's declared
//! projection (#831).
//!
//! The parquet reader projects by field id and simply drops a projected id
//! the file lacks, and its `ProjectionMask` emits columns in FILE order. A
//! fragment whose files predate an `ADD COLUMN` therefore produced batches
//! narrower than the stream schema taken from its first file, and the Flight
//! encoder refused them. A renamed column also arrived under its old name, in
//! file order, which the coordinator could only realign by position.
//!
//! When the ticket carries a projection fill, every batch is rebuilt here into
//! the fill's schema: one slot per projected column, taken from the file by
//! field id (by name for a file without field ids), cast when the type was
//! promoted, and otherwise filled from the fill row -- the column's
//! `initial_default`, or NULL. This matches what the coordinator's own scan
//! does in `sqe_catalog::iceberg_scan::conform_batch_to_projection`.

use std::sync::Arc;

use arrow::array::{new_null_array, ArrayRef, UInt32Array};
use arrow::compute::{cast, take};
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::SchemaRef;
use sqe_planner::ScanTask;

/// Iceberg's parquet field-id metadata key, as the parquet reader stamps it on
/// each Arrow field.
const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

/// A decoded projection fill: the declared schema and one value per column.
#[derive(Debug)]
pub struct ProjectionFill {
    schema: SchemaRef,
    fill: Vec<ArrayRef>,
    field_ids: Vec<i32>,
    names: Vec<String>,
}

impl ProjectionFill {
    /// Decode the task's fill, or `None` when it carries none. An undecodable
    /// fill, or one whose width disagrees with the projection, is an error:
    /// scanning without it would bring back the narrow batches it exists for.
    pub fn from_task(task: &ScanTask) -> anyhow::Result<Option<Arc<Self>>> {
        let Some(bytes) = task.projection_fill.as_deref() else {
            return Ok(None);
        };
        let batch = sqe_catalog::iceberg_scan::decode_projection_fill(bytes)
            .map_err(|e| anyhow::anyhow!("invalid projection fill: {e}"))?;
        let width = batch.num_columns();
        let ids_ok = task.projected_field_ids.is_empty() || task.projected_field_ids.len() == width;
        if task.projected_columns.len() != width || !ids_ok {
            anyhow::bail!(
                "projection fill has {width} columns but the task projects {} (ids: {})",
                task.projected_columns.len(),
                task.projected_field_ids.len()
            );
        }
        Ok(Some(Arc::new(Self {
            schema: batch.schema(),
            fill: batch.columns().to_vec(),
            field_ids: task.projected_field_ids.clone(),
            names: task.projected_columns.clone(),
        })))
    }

    /// The schema every emitted batch has.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Rebuild `batch` (one file's projected output) in the declared schema.
    pub fn conform(&self, batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
        let rows = batch.num_rows();
        let file_schema = batch.schema();
        let file_ids: Vec<Option<i32>> = file_schema
            .fields()
            .iter()
            .map(|f| {
                f.metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|v| v.parse().ok())
            })
            .collect();
        let by_id = !self.field_ids.is_empty() && file_ids.iter().all(Option::is_some);

        let mut columns = Vec::with_capacity(self.fill.len());
        for (i, field) in self.schema.fields().iter().enumerate() {
            let found = if by_id {
                let id = self.field_ids[i];
                file_ids.iter().position(|f| *f == Some(id))
            } else {
                file_schema.index_of(&self.names[i]).ok()
            };
            let column = match found {
                Some(idx) => {
                    let column = batch.column(idx);
                    if column.data_type() == field.data_type() {
                        Arc::clone(column)
                    } else {
                        cast(column, field.data_type())?
                    }
                }
                None if self.fill[i].is_null(0) => new_null_array(field.data_type(), rows),
                None => take(&self.fill[i], &UInt32Array::from(vec![0u32; rows]), None)?,
            };
            columns.push(column);
        }
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::util::display::array_value_to_string;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use std::collections::HashMap;

    fn with_id(name: &str, ty: DataType, id: i32) -> Field {
        Field::new(name, ty, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    /// Declared projection: `note` (id 3, default 'eu'), `id` (id 1, BIGINT),
    /// `tag` (id 4, no default) -- deliberately not in file order.
    fn fill() -> ProjectionFill {
        let schema = Arc::new(Schema::new(vec![
            Field::new("note", DataType::Utf8, true),
            Field::new("id", DataType::Int64, true),
            Field::new("tag", DataType::Utf8, true),
        ]));
        let defaults = vec![Some(ScalarValue::Utf8(Some("eu".into()))), None, None];
        let bytes = sqe_catalog::iceberg_scan::encode_projection_fill(&schema, &defaults).unwrap();
        let task = sqe_planner::ScanTask::builder(
            "f",
            vec!["s3://b/f.parquet".to_string()],
            sqe_planner::ScanStorage::default(),
        )
        .with_projection(
            vec!["note".into(), "id".into(), "tag".into()],
            vec![3, 1, 4],
        )
        .with_projection_fill(Some(bytes))
        .build();
        Arc::try_unwrap(ProjectionFill::from_task(&task).unwrap().unwrap()).unwrap()
    }

    fn rows(batch: &RecordBatch) -> Vec<String> {
        (0..batch.num_rows())
            .map(|r| {
                batch
                    .columns()
                    .iter()
                    .map(|c| {
                        if c.is_null(r) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(c, r).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect()
    }

    #[test]
    fn a_file_that_predates_columns_reads_their_default_or_null() {
        // Written before `note` and `tag` existed, and with `id` still INT.
        let file = RecordBatch::try_new(
            Arc::new(Schema::new(vec![with_id("id", DataType::Int32, 1)])),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let out = fill().conform(&file).unwrap();
        assert_eq!(out.schema().field(1).data_type(), &DataType::Int64);
        assert_eq!(rows(&out), vec!["eu|1|NULL", "eu|2|NULL"]);
    }

    #[test]
    fn columns_are_matched_by_field_id_not_name_or_position() {
        // A pre-RENAME file: `note` was called `old_note`, and the file stores
        // id before it. Same types, so a positional match would swap nothing
        // visibly wrong in the schema while swapping the values.
        let file = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                with_id("id", DataType::Int64, 1),
                with_id("old_note", DataType::Utf8, 3),
                with_id("tag", DataType::Utf8, 4),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(StringArray::from(vec!["t"])),
            ],
        )
        .unwrap();
        let out = fill().conform(&file).unwrap();
        assert_eq!(out.schema().field(0).name(), "note");
        assert_eq!(rows(&out), vec!["x|7|t"]);
    }

    #[test]
    fn a_file_without_field_ids_is_matched_by_name() {
        let file = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("tag", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![5])),
                Arc::new(StringArray::from(vec!["t"])),
            ],
        )
        .unwrap();
        assert_eq!(rows(&fill().conform(&file).unwrap()), vec!["eu|5|t"]);
    }

    #[test]
    fn a_fill_narrower_than_the_projection_is_refused() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let bytes = sqe_catalog::iceberg_scan::encode_projection_fill(&schema, &[]).unwrap();
        let task = sqe_planner::ScanTask::builder(
            "f",
            vec!["s3://b/f.parquet".to_string()],
            sqe_planner::ScanStorage::default(),
        )
        .with_projection(vec!["id".into(), "note".into()], vec![1, 3])
        .with_projection_fill(Some(bytes))
        .build();
        assert!(ProjectionFill::from_task(&task).is_err());
    }
}
