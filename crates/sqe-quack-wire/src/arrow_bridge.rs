//! Convert Arrow `RecordBatch` to our `DataChunk` so query results from
//! DataFusion (or any Arrow-producing engine) can be serialised for Quack
//! clients.
//!
//! Only the column types covered by `LogicalTypeId`'s MVP set are supported.
//! Unknown Arrow types return `UnsupportedArrowType` so the server can surface
//! a clear `SQE-EXEC` error rather than silently emitting wrong bytes.
//!
//! Reverse direction (DataChunk -> RecordBatch) is needed for the
//! `AppendRequest` write path; not implemented yet.

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::DataType;

use crate::data_chunk::{DataChunk, LogicalType, LogicalTypeId, Vector, VectorData};

pub fn record_batch_to_data_chunk(batch: &RecordBatch) -> crate::Result<DataChunk> {
    let row_count = batch.num_rows();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        columns.push(column_to_vector(column.as_ref(), row_count)?);
    }
    Ok(DataChunk {
        row_count: row_count as u32,
        columns,
    })
}

fn column_to_vector(array: &dyn Array, row_count: usize) -> crate::Result<Vector> {
    let validity = null_buffer_to_validity(array, row_count);
    let (logical_type, data) = match array.data_type() {
        DataType::Int8 => fixed_from_array::<Int8Array>(array, LogicalTypeId::TinyInt, row_count)?,
        DataType::Int16 => {
            fixed_from_array::<Int16Array>(array, LogicalTypeId::SmallInt, row_count)?
        }
        DataType::Int32 => {
            fixed_from_array::<Int32Array>(array, LogicalTypeId::Integer, row_count)?
        }
        DataType::Int64 => fixed_from_array::<Int64Array>(array, LogicalTypeId::BigInt, row_count)?,
        DataType::UInt8 => {
            fixed_from_array::<UInt8Array>(array, LogicalTypeId::UTinyInt, row_count)?
        }
        DataType::UInt16 => {
            fixed_from_array::<UInt16Array>(array, LogicalTypeId::USmallInt, row_count)?
        }
        DataType::UInt32 => {
            fixed_from_array::<UInt32Array>(array, LogicalTypeId::UInteger, row_count)?
        }
        DataType::UInt64 => {
            fixed_from_array::<UInt64Array>(array, LogicalTypeId::UBigInt, row_count)?
        }
        DataType::Float32 => {
            fixed_from_array::<Float32Array>(array, LogicalTypeId::Float, row_count)?
        }
        DataType::Float64 => {
            fixed_from_array::<Float64Array>(array, LogicalTypeId::Double, row_count)?
        }
        DataType::Boolean => boolean_to_fixed(array, row_count)?,
        DataType::Utf8 => string_to_strings::<StringArray>(array)?,
        DataType::LargeUtf8 => string_to_strings::<LargeStringArray>(array)?,
        other => return Err(crate::WireError::UnsupportedArrowType(format!("{other:?}"))),
    };
    Ok(Vector {
        logical_type,
        validity,
        data,
    })
}

fn null_buffer_to_validity(array: &dyn Array, row_count: usize) -> Option<Vec<bool>> {
    if array.null_count() == 0 {
        return None;
    }
    let mut bits = Vec::with_capacity(row_count);
    for i in 0..row_count {
        bits.push(array.is_valid(i));
    }
    Some(bits)
}

trait FixedWidthArrowArray: 'static {
    fn write_le(&self, row: usize, out: &mut Vec<u8>);
}

macro_rules! fixed_width_impl {
    ($arr_ty:ty, $value_ty:ty) => {
        impl FixedWidthArrowArray for $arr_ty {
            fn write_le(&self, row: usize, out: &mut Vec<u8>) {
                let v: $value_ty = if self.is_valid(row) {
                    self.value(row)
                } else {
                    <$value_ty>::default()
                };
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
    };
}

fixed_width_impl!(Int8Array, i8);
fixed_width_impl!(Int16Array, i16);
fixed_width_impl!(Int32Array, i32);
fixed_width_impl!(Int64Array, i64);
fixed_width_impl!(UInt8Array, u8);
fixed_width_impl!(UInt16Array, u16);
fixed_width_impl!(UInt32Array, u32);
fixed_width_impl!(UInt64Array, u64);
fixed_width_impl!(Float32Array, f32);
fixed_width_impl!(Float64Array, f64);

fn fixed_from_array<A: FixedWidthArrowArray + Array>(
    array: &dyn Array,
    id: LogicalTypeId,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| crate::WireError::UnsupportedArrowType("downcast failed".to_string()))?;
    let mut bytes = Vec::with_capacity(row_count * id.fixed_width().unwrap_or(0));
    for i in 0..row_count {
        typed.write_le(i, &mut bytes);
    }
    Ok((LogicalType::new(id), VectorData::Fixed(bytes)))
}

fn boolean_to_fixed(
    array: &dyn Array,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("boolean downcast failed".to_string())
        })?;
    let mut bytes = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if typed.is_valid(i) {
            typed.value(i)
        } else {
            false
        };
        bytes.push(v as u8);
    }
    Ok((
        LogicalType::new(LogicalTypeId::Boolean),
        VectorData::Fixed(bytes),
    ))
}

trait StringLikeArray: Array + 'static {
    fn value_at(&self, idx: usize) -> &str;
}

impl StringLikeArray for StringArray {
    fn value_at(&self, idx: usize) -> &str {
        self.value(idx)
    }
}

impl StringLikeArray for LargeStringArray {
    fn value_at(&self, idx: usize) -> &str {
        self.value(idx)
    }
}

fn string_to_strings<A: StringLikeArray>(
    array: &dyn Array,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array.as_any().downcast_ref::<A>().ok_or_else(|| {
        crate::WireError::UnsupportedArrowType("string downcast failed".to_string())
    })?;
    let len = typed.len();
    let mut values = Vec::with_capacity(len);
    for i in 0..len {
        if typed.is_null(i) {
            values.push(None);
        } else {
            values.push(Some(typed.value_at(i).to_string()));
        }
    }
    Ok((
        LogicalType::new(LogicalTypeId::Varchar),
        VectorData::Strings(values),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::data_chunk::{LogicalTypeId, VectorData};

    fn batch_with(columns: Vec<(&str, DataType, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, dt, _)| Field::new(*name, dt.clone(), true))
            .collect();
        let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|(_, _, a)| a).collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    #[test]
    fn int32_column_without_nulls_round_trips_bytes() {
        let arr: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let batch = batch_with(vec![("x", DataType::Int32, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.row_count, 3);
        assert_eq!(chunk.columns.len(), 1);
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Integer);
        assert!(chunk.columns[0].validity.is_none());
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                let expected: Vec<u8> = [1i32, 2, 3].iter().flat_map(|v| v.to_le_bytes()).collect();
                assert_eq!(bytes, &expected);
            }
            other => panic!("unexpected variant {other:?}"),
        }
    }

    #[test]
    fn int64_column_with_nulls_captures_validity() {
        let arr: Arc<dyn Array> = Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]));
        let batch = batch_with(vec![("y", DataType::Int64, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::BigInt);
        assert_eq!(chunk.columns[0].validity, Some(vec![true, false, true]));
    }

    #[test]
    fn float64_column_round_trips() {
        let arr: Arc<dyn Array> = Arc::new(Float64Array::from(vec![1.5, 2.25, 3.0]));
        let batch = batch_with(vec![("f", DataType::Float64, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Double);
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                let expected: Vec<u8> = [1.5f64, 2.25, 3.0]
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect();
                assert_eq!(bytes, &expected);
            }
            other => panic!("unexpected variant {other:?}"),
        }
    }

    #[test]
    fn utf8_column_with_nulls_round_trips_strings() {
        let arr: Arc<dyn Array> =
            Arc::new(StringArray::from(vec![Some("alice"), None, Some("bob")]));
        let batch = batch_with(vec![("name", DataType::Utf8, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Varchar);
        assert_eq!(chunk.columns[0].validity, Some(vec![true, false, true]));
        match &chunk.columns[0].data {
            VectorData::Strings(values) => assert_eq!(
                values,
                &vec![Some("alice".to_string()), None, Some("bob".to_string()),]
            ),
            other => panic!("unexpected variant {other:?}"),
        }
    }

    #[test]
    fn multi_column_batch_preserves_row_count() {
        let ints: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let names: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let batch = batch_with(vec![
            ("id", DataType::Int32, ints),
            ("name", DataType::Utf8, names),
        ]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.row_count, 3);
        assert_eq!(chunk.columns.len(), 2);
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Integer);
        assert_eq!(chunk.columns[1].logical_type.id, LogicalTypeId::Varchar);
    }

    #[test]
    fn unsupported_arrow_type_returns_clear_error() {
        use arrow_array::Date32Array;
        let arr: Arc<dyn Array> = Arc::new(Date32Array::from(vec![100, 200]));
        let batch = batch_with(vec![("d", DataType::Date32, arr)]);
        let err = record_batch_to_data_chunk(&batch).unwrap_err();
        match err {
            crate::WireError::UnsupportedArrowType(msg) => assert!(msg.contains("Date32")),
            other => panic!("expected UnsupportedArrowType, got {other:?}"),
        }
    }

    #[test]
    fn record_batch_to_chunk_then_encode_decode_round_trips() {
        let ints: Arc<dyn Array> = Arc::new(Int32Array::from(vec![Some(10), None, Some(30)]));
        let names: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some("x"), Some("y"), None]));
        let batch = batch_with(vec![
            ("id", DataType::Int32, ints),
            ("name", DataType::Utf8, names),
        ]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();

        let mut s = crate::codec::BinarySerializer::new();
        s.begin_object();
        chunk.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = crate::codec::BinaryDeserializer::new(&bytes);
        let decoded = DataChunk::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, chunk);
    }
}
