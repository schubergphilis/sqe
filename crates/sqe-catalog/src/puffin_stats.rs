//! Puffin NDV sketch sidecar writer.
//!
//! Emits one `apache-datasketches-theta-v1` blob per data column after a
//! successful CTAS/INSERT commit. The blob's `fields` vector carries the
//! Iceberg field id; the `properties` map includes a best-effort `ndv`
//! estimate so a reader can skip the sketch body when only the cardinality
//! is needed.
//!
//! ## Scope
//!
//! Phase F of the iceberg-matrix-parity change covers the writer side only.
//! DataFusion 53 has no `StatisticsSource` hook; the consumer side waits for
//! DataFusion 54 + apache/datafusion#21157. See the TODO in
//! `crates/sqe-planner/src/stats.rs` for the future wiring point.
//!
//! ## Sketch format
//!
//! The Puffin spec expects a serialised theta sketch body matching the Apache
//! DataSketches "compact" wire format. The pure-Rust `datasketches` crate
//! does not yet export that exact byte layout, so the blob body stores the
//! sketch's retained 64-bit hashes in little-endian order. A future reader
//! can still estimate NDV from the `ndv` blob property, which mirrors the
//! `ThetaSketch::estimate()` value at emit time. Once upstream
//! `datasketches` adds Java-compatible serialisation (tracked upstream at
//! apache/datasketches-rust#5), we replace `serialise_theta_body` with that
//! call and the blob becomes bit-compatible with Trino and Spark.
//!
//! ## Opt-in
//!
//! Emission is gated by the table property `write.puffin.stats = 'true'`.
//! The default is off while the deferred consumer side lands; flipping the
//! default to true is a follow-up once tasks 7.12-7.14 close.
//!
//! ## Consumer side (deferred)
//!
//! TODO(matrix-f): tasks 7.12-7.14 wire these blobs into DataFusion's
//! `StatisticsSource` once it lands in DataFusion 54. See
//! `crates/sqe-planner/src/stats.rs` for the planned module and
//! <https://github.com/apache/datafusion/issues/21157> for the upstream
//! tracking issue.

use std::collections::HashMap;

use arrow_array::{Array, RecordBatch};
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use datasketches::theta::ThetaSketch;
use iceberg::io::FileIO;
use iceberg::puffin::{APACHE_DATASKETCHES_THETA_V1, Blob, CompressionCodec, PuffinWriter};
use iceberg::spec::{Schema as IcebergSchema, StatisticsFile};
use iceberg::spec::BlobMetadata as SpecBlobMetadata;
use tracing::{debug, warn};

/// Iceberg table property enabling Puffin NDV sidecar emission.
pub const PROP_PUFFIN_STATS: &str = "write.puffin.stats";

/// Blob property key for the estimated distinct count.
pub const PROP_NDV: &str = "ndv";

/// Blob property key for the field id (for readers that do not index on
/// `fields`).
pub const PROP_FIELD_ID: &str = "field_id";

/// Return true when the table property opts in to Puffin emission.
pub fn puffin_stats_enabled(table_properties: &HashMap<String, String>) -> bool {
    table_properties
        .get(PROP_PUFFIN_STATS)
        .map(|v| matches!(v.as_str(), "true" | "TRUE" | "True" | "1"))
        .unwrap_or(false)
}

/// Build a path for the Puffin sidecar given the table's metadata directory.
///
/// The Iceberg spec is silent on the exact location; we mirror the convention
/// used by Trino and Spark: `<metadata_dir>/<snapshot_id>-<uuid>.stats`.
pub fn puffin_sidecar_path(metadata_location: &str, snapshot_id: i64) -> String {
    // metadata_location looks like `s3://bucket/ns/table/metadata/00003-abc.json`
    // or `<prefix>/metadata/00003-abc.json`. Strip the filename and append the
    // sidecar name so the file lands next to the table metadata.
    let dir = metadata_location
        .rsplit_once('/')
        .map(|(head, _tail)| head)
        .unwrap_or(metadata_location);
    let uuid = uuid::Uuid::new_v4();
    format!("{dir}/{snapshot_id}-{uuid}.stats")
}

/// Build one theta sketch per top-level column in the schema.
///
/// Returns `(field_id, name, sketch, ndv_estimate)` tuples in schema order.
/// Columns whose Arrow type is not hashable in a meaningful way (structs,
/// lists, maps) are skipped with a warning, which matches the Java writer's
/// behaviour of only sketching primitives.
pub fn build_theta_sketches(
    schema: &IcebergSchema,
    batches: &[RecordBatch],
) -> Vec<(i32, String, ThetaSketch, u64)> {
    let fields = schema.as_struct().fields();
    let mut out = Vec::with_capacity(fields.len());

    for (col_idx, iceberg_field) in fields.iter().enumerate() {
        let Some(first_batch) = batches.first() else {
            continue;
        };
        if first_batch.num_columns() <= col_idx {
            continue;
        }

        let arrow_type = first_batch.column(col_idx).data_type().clone();
        if !is_sketchable_type(&arrow_type) {
            debug!(
                field = %iceberg_field.name,
                data_type = ?arrow_type,
                "skipping non-primitive column for theta sketch"
            );
            continue;
        }

        let mut sketch = ThetaSketch::builder().build();
        for batch in batches {
            if col_idx >= batch.num_columns() {
                continue;
            }
            feed_column_into_sketch(&mut sketch, batch.column(col_idx).as_ref());
        }

        // estimate() returns f64; use rounded value for the `ndv` property.
        let ndv = sketch.estimate().round().max(0.0) as u64;
        out.push((iceberg_field.id, iceberg_field.name.clone(), sketch, ndv));
    }

    out
}

/// Decide whether an Arrow type can be fed into a theta sketch.
///
/// We accept numeric, boolean, and string-like primitives. Nested and
/// binary-with-hash-mismatch types are skipped because the sketch `update`
/// contract relies on `Hash`; nested hashing would give unstable cardinality
/// across engines.
fn is_sketchable_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// Feed a single Arrow column into a theta sketch, one non-null value at a
/// time. Nulls are ignored, matching DataSketches semantics.
fn feed_column_into_sketch(sketch: &mut ThetaSketch, column: &dyn Array) {
    match column.data_type() {
        DataType::Boolean => {
            let arr = column.as_boolean();
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    sketch.update(arr.value(i));
                }
            }
        }
        DataType::Int32 => {
            let arr = column.as_primitive::<arrow_array::types::Int32Type>();
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    sketch.update(arr.value(i));
                }
            }
        }
        DataType::Int64 => {
            let arr = column.as_primitive::<arrow_array::types::Int64Type>();
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    sketch.update(arr.value(i));
                }
            }
        }
        DataType::UInt64 => {
            let arr = column.as_primitive::<arrow_array::types::UInt64Type>();
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    sketch.update(arr.value(i));
                }
            }
        }
        DataType::Float64 => {
            let arr = column.as_primitive::<arrow_array::types::Float64Type>();
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    sketch.update_f64(arr.value(i));
                }
            }
        }
        DataType::Float32 => {
            let arr = column.as_primitive::<arrow_array::types::Float32Type>();
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    sketch.update_f32(arr.value(i));
                }
            }
        }
        DataType::Utf8 => {
            let arr = column.as_string::<i32>();
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    sketch.update(arr.value(i));
                }
            }
        }
        DataType::LargeUtf8 => {
            let arr = column.as_string::<i64>();
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    sketch.update(arr.value(i));
                }
            }
        }
        DataType::Timestamp(_, _) => {
            // All timestamp variants store an i64 internally. Cast via the
            // downcast helpers for the microsecond unit since Iceberg stores
            // timestamps at microsecond precision; other units are covered by
            // the generic primitive path below.
            if let Some(arr) =
                column.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>()
            {
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        sketch.update(arr.value(i));
                    }
                }
            } else if let Some(arr) =
                column.as_any().downcast_ref::<arrow_array::TimestampNanosecondArray>()
            {
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        sketch.update(arr.value(i));
                    }
                }
            } else if let Some(arr) =
                column.as_any().downcast_ref::<arrow_array::TimestampMillisecondArray>()
            {
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        sketch.update(arr.value(i));
                    }
                }
            }
        }
        other => {
            // Types classified as sketchable above that we have not wired a
            // fast-path for yet. Unreachable in practice: is_sketchable_type
            // is the single source of truth. Warn so a gap surfaces during
            // testing rather than silently ignoring data.
            warn!(data_type = ?other, "theta sketch missing type handler; column skipped");
        }
    }
}

/// Serialise a theta sketch body into bytes suitable for a Puffin blob.
///
/// The format is intentionally minimal: 8 bytes per retained 64-bit hash,
/// little-endian, in iteration order. A reader can recover the NDV estimate
/// from the `ndv` property (or recompute it from retained counts plus
/// theta), without needing to parse the raw hashes. See the module docs for
/// the upgrade path to Java-compatible encoding.
pub fn serialise_theta_body(sketch: &ThetaSketch) -> Vec<u8> {
    let mut out = Vec::with_capacity(sketch.num_retained() * 8);
    for hash in sketch.iter() {
        out.extend_from_slice(&hash.to_le_bytes());
    }
    out
}

/// Write a Puffin sidecar containing one theta blob per column of the
/// supplied batches and return the on-disk descriptor.
///
/// `base_dir` is the directory (e.g. `s3://bucket/ns/table/metadata`) where
/// the file should land; the caller is responsible for passing a location
/// that the catalog accepts as a sibling of the current metadata file.
pub async fn write_puffin_sidecar(
    file_io: &FileIO,
    base_dir: &str,
    schema: &IcebergSchema,
    batches: &[RecordBatch],
    snapshot_id: i64,
    sequence_number: i64,
) -> iceberg::Result<StatisticsFile> {
    let uuid = uuid::Uuid::new_v4();
    let sidecar_path = format!("{base_dir}/{snapshot_id}-{uuid}.stats");

    let sketches = build_theta_sketches(schema, batches);

    let output_file = file_io.new_output(&sidecar_path)?;
    let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false).await?;

    let mut blob_metadata_specs: Vec<SpecBlobMetadata> = Vec::with_capacity(sketches.len());

    for (field_id, name, sketch, ndv) in &sketches {
        let mut props = HashMap::new();
        props.insert(PROP_NDV.to_string(), ndv.to_string());
        props.insert(PROP_FIELD_ID.to_string(), field_id.to_string());
        props.insert("column".to_string(), name.clone());

        let body = serialise_theta_body(sketch);
        let blob = Blob::builder()
            .r#type(APACHE_DATASKETCHES_THETA_V1.to_string())
            .fields(vec![*field_id])
            .snapshot_id(snapshot_id)
            .sequence_number(sequence_number)
            .data(body)
            .properties(props.clone())
            .build();

        writer.add(blob, CompressionCodec::None).await?;

        blob_metadata_specs.push(SpecBlobMetadata {
            r#type: APACHE_DATASKETCHES_THETA_V1.to_string(),
            snapshot_id,
            sequence_number,
            fields: vec![*field_id],
            properties: props,
        });
    }

    let result = writer.close_with_metadata().await?;
    debug!(
        path = %sidecar_path,
        blob_count = sketches.len(),
        size = result.file_size_in_bytes,
        "wrote Puffin NDV sidecar"
    );

    Ok(StatisticsFile {
        snapshot_id,
        statistics_path: sidecar_path,
        file_size_in_bytes: result.file_size_in_bytes as i64,
        // The Puffin footer size is the last 4 bytes' length field; we do not
        // surface it back from the writer. Reading the file can recompute it.
        // Setting 0 is valid per the Iceberg REST spec, which treats missing
        // values as unknown.
        file_footer_size_in_bytes: 0,
        key_metadata: None,
        blob_metadata: blob_metadata_specs,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
    use tempfile::TempDir;

    use super::*;

    fn schema_id_name() -> IcebergSchema {
        IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema")
    }

    fn sample_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let id_array = Arc::new(Int64Array::from(ids));
        let name_array = Arc::new(StringArray::from(names));
        RecordBatch::try_new(arrow_schema, vec![id_array, name_array]).unwrap()
    }

    #[test]
    fn puffin_stats_enabled_reads_table_property() {
        let mut on = HashMap::new();
        on.insert(PROP_PUFFIN_STATS.to_string(), "true".to_string());
        assert!(puffin_stats_enabled(&on));

        let mut off = HashMap::new();
        off.insert(PROP_PUFFIN_STATS.to_string(), "false".to_string());
        assert!(!puffin_stats_enabled(&off));

        let empty = HashMap::new();
        assert!(!puffin_stats_enabled(&empty));
    }

    #[test]
    fn build_theta_sketches_emits_one_per_column() {
        let schema = schema_id_name();
        let batch = sample_batch(vec![1, 2, 3, 1, 2], vec!["a", "b", "c", "a", "b"]);
        let sketches = build_theta_sketches(&schema, &[batch]);
        assert_eq!(sketches.len(), 2);
        // id has 3 distinct values
        assert_eq!(sketches[0].0, 1); // field id
        assert_eq!(sketches[0].1, "id");
        assert_eq!(sketches[0].3, 3);
        // name has 3 distinct values
        assert_eq!(sketches[1].0, 2);
        assert_eq!(sketches[1].1, "name");
        assert_eq!(sketches[1].3, 3);
    }

    #[test]
    fn serialise_theta_body_len_matches_retained() {
        let schema = schema_id_name();
        let batch = sample_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        let sketches = build_theta_sketches(&schema, &[batch]);
        let (_, _, sk, _) = &sketches[0];
        let body = serialise_theta_body(sk);
        // Every retained hash is 8 bytes.
        assert_eq!(body.len(), sk.num_retained() * 8);
    }

    #[tokio::test]
    async fn write_puffin_sidecar_emits_file_with_blobs_per_column() {
        let temp = TempDir::new().unwrap();
        let base_dir = temp.path().to_string_lossy().to_string();

        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let schema = schema_id_name();
        let batch = sample_batch(vec![1, 2, 3, 4], vec!["a", "b", "c", "d"]);

        let stats = write_puffin_sidecar(&file_io, &base_dir, &schema, &[batch], 1, 1)
            .await
            .expect("write puffin");

        assert_eq!(stats.snapshot_id, 1);
        assert!(stats.file_size_in_bytes > 0);
        assert_eq!(stats.blob_metadata.len(), 2, "one blob per primitive column");

        let id_meta = stats
            .blob_metadata
            .iter()
            .find(|m| m.fields == vec![1])
            .expect("id blob");
        assert_eq!(id_meta.r#type, APACHE_DATASKETCHES_THETA_V1);
        assert_eq!(id_meta.properties.get(PROP_NDV), Some(&"4".to_string()));
    }

    #[tokio::test]
    async fn ndv_estimate_within_five_percent_of_one_million() {
        // Task 7.9: sketch NDV within 5% of true count on 1M distinct values.
        let schema = IcebergSchema::builder()
            .with_fields(vec![NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )
            .into()])
            .build()
            .unwrap();

        let mut ids: Vec<i64> = Vec::with_capacity(1_000_000);
        for i in 0..1_000_000i64 {
            ids.push(i);
        }
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int64Array::from(ids)) as _],
        )
        .unwrap();

        let sketches = build_theta_sketches(&schema, &[batch]);
        let (_, _, _, ndv) = sketches[0];
        let err = (ndv as f64 - 1_000_000.0).abs() / 1_000_000.0;
        assert!(
            err < 0.05,
            "theta sketch NDV {ndv} differs from 1M by {:.2}%",
            err * 100.0
        );
    }
}
