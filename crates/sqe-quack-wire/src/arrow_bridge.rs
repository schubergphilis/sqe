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
    cast::AsArray, types as arrow_types, Array, BinaryArray, BinaryViewArray, BooleanArray,
    Date32Array, Date64Array, Decimal128Array, DictionaryArray, FixedSizeBinaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, RecordBatch, StringArray,
    StringViewArray, StructArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};

use crate::data_chunk::{
    decimal_physical_width, enum_physical_width, DataChunk, ExtraTypeInfo, LogicalType,
    LogicalTypeId, Vector, VectorData,
};

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

/// Map an Arrow [`DataType`] to the `LogicalType` that `column_to_vector`
/// would stamp on a vector of that type. This is the *type-only* mirror of the
/// encode path: a server has to fill `PrepareResponse.result_types` from the
/// result **schema**, not from the first batch, otherwise a query that returns
/// zero rows reports zero columns.
///
/// Everything except `Dictionary` (DuckDB `ENUM`) is derivable from the Arrow
/// type alone. `ENUM` carries its dictionary values in `ExtraTypeInfo::Enum`,
/// and those live in the *array*, not the type, so this function rejects
/// `Dictionary`; a caller holding an encoded chunk should read the types off
/// the chunk instead.
pub fn arrow_type_to_logical(dt: &DataType) -> crate::Result<LogicalType> {
    let id = match dt {
        DataType::Int8 => LogicalTypeId::TinyInt,
        DataType::Int16 => LogicalTypeId::SmallInt,
        DataType::Int32 => LogicalTypeId::Integer,
        DataType::Int64 => LogicalTypeId::BigInt,
        DataType::UInt8 => LogicalTypeId::UTinyInt,
        DataType::UInt16 => LogicalTypeId::USmallInt,
        DataType::UInt32 => LogicalTypeId::UInteger,
        DataType::UInt64 => LogicalTypeId::UBigInt,
        DataType::Float32 => LogicalTypeId::Float,
        DataType::Float64 => LogicalTypeId::Double,
        DataType::Boolean => LogicalTypeId::Boolean,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => LogicalTypeId::Varchar,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => LogicalTypeId::Blob,
        // Date64 narrows to DuckDB's 4-byte DATE on the value path, so the
        // declared type has to narrow with it.
        DataType::Date32 | DataType::Date64 => LogicalTypeId::Date,
        DataType::Timestamp(TimeUnit::Second, _) => LogicalTypeId::TimestampSec,
        DataType::Timestamp(TimeUnit::Millisecond, _) => LogicalTypeId::TimestampMs,
        DataType::Timestamp(TimeUnit::Microsecond, _) => LogicalTypeId::Timestamp,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => LogicalTypeId::TimestampNs,
        // Time32(s|ms) widen into DuckDB TIME (i64 micros-of-day).
        DataType::Time32(TimeUnit::Second)
        | DataType::Time32(TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond) => LogicalTypeId::Time,
        DataType::Time64(TimeUnit::Nanosecond) => LogicalTypeId::TimeNs,
        DataType::FixedSizeBinary(16) => LogicalTypeId::Uuid,
        DataType::Interval(_) => LogicalTypeId::Interval,
        DataType::Decimal128(precision, scale) => {
            let scale_u8 = u8::try_from(*scale).map_err(|_| {
                crate::WireError::UnsupportedArrowType(format!(
                    "Decimal128 with negative scale {scale} is not representable in \
                     DuckDB DecimalTypeInfo"
                ))
            })?;
            return Ok(LogicalType::with_extra(
                LogicalTypeId::Decimal,
                ExtraTypeInfo::Decimal {
                    precision: *precision,
                    scale: scale_u8,
                },
            ));
        }
        DataType::List(field) | DataType::LargeList(field) => {
            let child = arrow_type_to_logical(field.data_type())?;
            return Ok(LogicalType::with_extra(
                LogicalTypeId::List,
                ExtraTypeInfo::List {
                    child: Box::new(child),
                },
            ));
        }
        DataType::Struct(fields) => {
            let mut out = Vec::with_capacity(fields.len());
            for f in fields {
                out.push((f.name().clone(), arrow_type_to_logical(f.data_type())?));
            }
            return Ok(LogicalType::with_extra(
                LogicalTypeId::Struct,
                ExtraTypeInfo::Struct { fields: out },
            ));
        }
        // Arrow MAP is `Map(entries: Struct<key, value>)`; DuckDB stores it as
        // LIST<STRUCT<key, value>> with the parent id stamped MAP — the same
        // shape `map_array_to_map` produces on the value path.
        DataType::Map(entries, _) => {
            let child = arrow_type_to_logical(entries.data_type())?;
            return Ok(LogicalType::with_extra(
                LogicalTypeId::Map,
                ExtraTypeInfo::List {
                    child: Box::new(child),
                },
            ));
        }
        DataType::FixedSizeList(field, size) => {
            let child = arrow_type_to_logical(field.data_type())?;
            let size = u32::try_from(*size).map_err(|_| {
                crate::WireError::UnsupportedArrowType(format!(
                    "FixedSizeList with negative size {size}"
                ))
            })?;
            return Ok(LogicalType::with_extra(
                LogicalTypeId::Array,
                ExtraTypeInfo::Array {
                    child: Box::new(child),
                    size,
                },
            ));
        }
        other => return Err(crate::WireError::UnsupportedArrowType(format!("{other:?}"))),
    };
    Ok(LogicalType::new(id))
}

/// Column types for a whole Arrow [`Schema`], in field order. See
/// [`arrow_type_to_logical`] for why the schema (rather than the first batch)
/// is the right source for `PrepareResponse.result_types`.
pub fn arrow_schema_to_logical(schema: &arrow_schema::Schema) -> crate::Result<Vec<LogicalType>> {
    schema
        .fields()
        .iter()
        .map(|f| arrow_type_to_logical(f.data_type()))
        .collect()
}

/// Map a single `LogicalType` to an Arrow [`DataType`]. Mirrors the
/// type-level choices made by `column_to_vector` so a server's
/// `PrepareResponse.result_types` can be turned into an Arrow [`Schema`]
/// even before the first `DataChunk` arrives.
///
/// Nested types (LIST/STRUCT/MAP/ARRAY) are not yet supported on the reverse
/// schema path; they surface as `UnsupportedLogicalType` until the recursive
/// `vector_to_array` body exists.
pub fn logical_type_to_arrow(t: &LogicalType) -> crate::Result<arrow_schema::DataType> {
    use arrow_schema::DataType as A;
    Ok(match t.id {
        LogicalTypeId::Boolean => A::Boolean,
        LogicalTypeId::TinyInt => A::Int8,
        LogicalTypeId::SmallInt => A::Int16,
        LogicalTypeId::Integer => A::Int32,
        LogicalTypeId::BigInt => A::Int64,
        LogicalTypeId::UTinyInt => A::UInt8,
        LogicalTypeId::USmallInt => A::UInt16,
        LogicalTypeId::UInteger => A::UInt32,
        LogicalTypeId::UBigInt => A::UInt64,
        LogicalTypeId::Float => A::Float32,
        LogicalTypeId::Double => A::Float64,
        LogicalTypeId::Varchar => A::Utf8,
        LogicalTypeId::Blob => A::Binary,
        LogicalTypeId::Date => A::Date32,
        LogicalTypeId::Time => A::Time64(TimeUnit::Microsecond),
        LogicalTypeId::TimeNs => A::Time64(TimeUnit::Nanosecond),
        LogicalTypeId::Timestamp => A::Timestamp(TimeUnit::Microsecond, None),
        LogicalTypeId::TimestampSec => A::Timestamp(TimeUnit::Second, None),
        LogicalTypeId::TimestampMs => A::Timestamp(TimeUnit::Millisecond, None),
        LogicalTypeId::TimestampNs => A::Timestamp(TimeUnit::Nanosecond, None),
        LogicalTypeId::Uuid => A::FixedSizeBinary(16),
        LogicalTypeId::Interval => A::Interval(IntervalUnit::MonthDayNano),
        LogicalTypeId::Decimal => {
            let (precision, scale) = match &t.extra {
                Some(crate::data_chunk::ExtraTypeInfo::Decimal { precision, scale }) => {
                    (*precision, *scale)
                }
                _ => {
                    return Err(crate::WireError::UnsupportedLogicalType(
                        LogicalTypeId::Decimal,
                    ))
                }
            };
            A::Decimal128(precision, scale as i8)
        }
        LogicalTypeId::List => {
            let child = match &t.extra {
                Some(crate::data_chunk::ExtraTypeInfo::List { child }) => child,
                _ => {
                    return Err(crate::WireError::UnsupportedLogicalType(
                        LogicalTypeId::List,
                    ))
                }
            };
            let item_dt = logical_type_to_arrow(child)?;
            A::List(std::sync::Arc::new(arrow_schema::Field::new(
                "item", item_dt, true,
            )))
        }
        LogicalTypeId::Struct => {
            let fields = match &t.extra {
                Some(crate::data_chunk::ExtraTypeInfo::Struct { fields }) => fields,
                _ => {
                    return Err(crate::WireError::UnsupportedLogicalType(
                        LogicalTypeId::Struct,
                    ))
                }
            };
            let mut arrow_fields = Vec::with_capacity(fields.len());
            for (name, ty) in fields {
                let dt = logical_type_to_arrow(ty)?;
                arrow_fields.push(arrow_schema::Field::new(name.clone(), dt, true));
            }
            A::Struct(arrow_fields.into())
        }
        LogicalTypeId::Map => {
            // DuckDB MAP is LogicalTypeId::Map + ExtraTypeInfo::List { child: STRUCT(key, value) }.
            // Arrow's Map type is Map(Field { name: "entries", type: Struct(key, value) }, keys_sorted=false).
            let entries_struct = match &t.extra {
                Some(crate::data_chunk::ExtraTypeInfo::List { child }) => child,
                _ => return Err(crate::WireError::UnsupportedLogicalType(LogicalTypeId::Map)),
            };
            let entries_dt = logical_type_to_arrow(entries_struct)?;
            // The Arrow spec requires the key field to be non-nullable and
            // `MapArray::try_new` enforces it; the generic STRUCT mapping
            // above marks every field nullable, so rebuild the entries type.
            let entries_dt = map_entries_type(entries_dt)?;
            let entries_field = arrow_schema::Field::new("entries", entries_dt, false);
            A::Map(std::sync::Arc::new(entries_field), false)
        }
        LogicalTypeId::Array => {
            let (child, size) = match &t.extra {
                Some(crate::data_chunk::ExtraTypeInfo::Array { child, size }) => (child, *size),
                _ => {
                    return Err(crate::WireError::UnsupportedLogicalType(
                        LogicalTypeId::Array,
                    ))
                }
            };
            let item_dt = logical_type_to_arrow(child)?;
            // Arrow carries the list size as i32; a wire size past that can
            // never be materialised, so refuse it at the schema step already.
            let size = i32::try_from(size).map_err(|_| {
                crate::WireError::UnsupportedArrowType(format!(
                    "ARRAY size {size} exceeds Arrow's i32 FixedSizeList limit"
                ))
            })?;
            A::FixedSizeList(
                std::sync::Arc::new(arrow_schema::Field::new("item", item_dt, true)),
                size,
            )
        }
        LogicalTypeId::Enum => {
            // ENUM round-trips as Arrow Dictionary(key, Utf8). Key width follows
            // the same tier table as the wire payload (1/2/4 bytes).
            let values = match &t.extra {
                Some(crate::data_chunk::ExtraTypeInfo::Enum { values }) => values,
                _ => {
                    return Err(crate::WireError::UnsupportedLogicalType(
                        LogicalTypeId::Enum,
                    ))
                }
            };
            let key_dt = match crate::data_chunk::enum_physical_width(values.len()) {
                1 => A::UInt8,
                2 => A::UInt16,
                4 => A::UInt32,
                _ => {
                    return Err(crate::WireError::UnsupportedLogicalType(
                        LogicalTypeId::Enum,
                    ))
                }
            };
            A::Dictionary(Box::new(key_dt), Box::new(A::Utf8))
        }
        other => return Err(crate::WireError::UnsupportedLogicalType(other)),
    })
}

/// Shape a MAP's `STRUCT(key, value)` entries type the way Arrow requires:
/// exactly two fields, key non-nullable. Anything else is a malformed type
/// from the wire and surfaces as an error rather than an assert later in
/// `MapArray::try_new`.
fn map_entries_type(entries_dt: DataType) -> crate::Result<DataType> {
    let fields = match &entries_dt {
        DataType::Struct(fields) if fields.len() == 2 => fields,
        other => {
            return Err(crate::WireError::UnsupportedArrowType(format!(
                "MAP entries must be STRUCT(key, value), got {other:?}"
            )))
        }
    };
    let key = fields[0].as_ref().clone().with_nullable(false);
    let value = fields[1].clone();
    Ok(DataType::Struct(
        vec![std::sync::Arc::new(key), value].into(),
    ))
}

/// Build an Arrow [`Schema`] from a parallel `(names, types)` pair, suitable
/// for `RecordBatch::schema()` and DataFusion's `TableProvider::schema()` even
/// when no batches have been received yet.
pub fn logical_schema_to_arrow(
    names: &[String],
    types: &[LogicalType],
) -> crate::Result<arrow_schema::SchemaRef> {
    if names.len() != types.len() {
        return Err(crate::WireError::UnexpectedField {
            expected: names.len() as u16,
            actual: types.len() as u16,
        });
    }
    let mut fields = Vec::with_capacity(names.len());
    for (name, ty) in names.iter().zip(types.iter()) {
        let dt = logical_type_to_arrow(ty)?;
        fields.push(arrow_schema::Field::new(name.clone(), dt, true));
    }
    Ok(std::sync::Arc::new(arrow_schema::Schema::new(fields)))
}

/// Reverse direction: take a decoded `DataChunk` plus the column names from
/// `PrepareResponse.result_names` and rebuild an Arrow [`RecordBatch`]. Used by
/// `sqe-quack-client` to surface server responses as Arrow.
///
/// Scope: covers the scalar/varchar/blob/decimal/temporal/uuid/interval paths
/// needed for the bulk of DuckDB query results. Nested types (LIST, STRUCT,
/// MAP, ARRAY) surface as `UnsupportedLogicalType` until the recursive
/// vector_to_array logic is added in a follow-up.
pub fn data_chunk_to_record_batch(
    names: &[String],
    chunk: &DataChunk,
) -> crate::Result<RecordBatch> {
    if names.len() != chunk.columns.len() {
        return Err(crate::WireError::UnexpectedField {
            expected: chunk.columns.len() as u16,
            actual: names.len() as u16,
        });
    }
    let row_count = chunk.row_count as usize;
    let mut fields = Vec::with_capacity(chunk.columns.len());
    let mut arrays: Vec<std::sync::Arc<dyn Array>> = Vec::with_capacity(chunk.columns.len());
    for (name, column) in names.iter().zip(chunk.columns.iter()) {
        let array = vector_to_array(column, row_count)?;
        fields.push(arrow_schema::Field::new(
            name.clone(),
            array.data_type().clone(),
            true,
        ));
        arrays.push(array);
    }
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
    RecordBatch::try_new(schema, arrays).map_err(|e| {
        crate::WireError::UnsupportedArrowType(format!("RecordBatch::try_new failed: {e}"))
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
        // `Utf8View` is Arrow 53's compact string representation; DataFusion 53
        // emits this by default for string outputs. Convert to owned `String`s
        // the same way we do for `Utf8` — the view layout is collapsed at the
        // codec boundary.
        DataType::Utf8View => string_view_to_strings(array)?,
        // Binary types map to DuckDB's BLOB. Same shape as VARCHAR: per-row
        // list with empty sentinels at null positions.
        DataType::Binary => binary_to_blobs::<BinaryArray>(array)?,
        DataType::LargeBinary => binary_to_blobs::<LargeBinaryArray>(array)?,
        DataType::BinaryView => binary_view_to_blobs(array)?,
        // Arrow `Date32` is i32 days since the 1970-01-01 UNIX epoch, which
        // is the same convention DuckDB's `DATE` uses on the wire (logical
        // type id 15, internal width 4 bytes). Pass-through is correct.
        DataType::Date32 => fixed_from_array::<Date32Array>(array, LogicalTypeId::Date, row_count)?,
        // Arrow `Date64` is i64 ms since 1970-01-01. DuckDB has no 8-byte
        // date variant, so we narrow to a `Date` (i32 days) by dividing
        // out ms-per-day.
        DataType::Date64 => date64_to_date(array, row_count)?,
        // Arrow timestamps share i64 storage with DuckDB's TIMESTAMP_*
        // variants. Map by precision; timezone info on the Arrow side is
        // discarded for now (timestamp_tz support is a separate
        // follow-up — it would need the `TimestampTz` LogicalType plus
        // timezone forwarding).
        DataType::Timestamp(TimeUnit::Second, _) => {
            fixed_from_array::<TimestampSecondArray>(array, LogicalTypeId::TimestampSec, row_count)?
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            fixed_from_array::<TimestampMillisecondArray>(
                array,
                LogicalTypeId::TimestampMs,
                row_count,
            )?
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            fixed_from_array::<TimestampMicrosecondArray>(
                array,
                LogicalTypeId::Timestamp,
                row_count,
            )?
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            fixed_from_array::<TimestampNanosecondArray>(
                array,
                LogicalTypeId::TimestampNs,
                row_count,
            )?
        }
        // Arrow `Time32` is i32 seconds- or millisecond-of-day; DuckDB's `TIME`
        // is i64 microseconds-of-day. Widen + scale into 8-byte LE.
        DataType::Time32(TimeUnit::Second) => time32_second_to_time(array, row_count)?,
        DataType::Time32(TimeUnit::Millisecond) => time32_millisecond_to_time(array, row_count)?,
        // Arrow `Time64(Microsecond)` already matches DuckDB `TIME` exactly.
        DataType::Time64(TimeUnit::Microsecond) => {
            fixed_from_array::<Time64MicrosecondArray>(array, LogicalTypeId::Time, row_count)?
        }
        // Arrow `Time64(Nanosecond)` maps to DuckDB `TIME_NS` (i64 ns of day).
        DataType::Time64(TimeUnit::Nanosecond) => {
            fixed_from_array::<Time64NanosecondArray>(array, LogicalTypeId::TimeNs, row_count)?
        }
        // FixedSizeBinary(16) is the canonical UUID encoding in Arrow. DuckDB's
        // UUID is also 16 bytes (a `uhugeint` on disk, but byte-for-byte we
        // copy the raw 16 bytes).
        DataType::FixedSizeBinary(16) => fixed_size_binary_to_uuid(array, row_count)?,
        DataType::FixedSizeBinary(n) => {
            return Err(crate::WireError::UnsupportedArrowType(format!(
                "FixedSizeBinary({n}) — only width 16 (UUID) is supported"
            )))
        }
        // Arrow `Decimal128(p, s)` always stores i128 per row. DuckDB's
        // physical width is precision-tiered (i16/i32/i64/i128), so we
        // narrow on encode and attach `ExtraTypeInfo::Decimal { precision,
        // scale }` so the wire carries the parameters.
        DataType::Decimal128(precision, scale) => {
            decimal128_to_decimal(array, row_count, *precision, *scale)?
        }
        // LIST<T> / LARGELIST<T>: build (offset, length) entries from the
        // Arrow offsets array, then recurse into the values child.
        DataType::List(_) => list_array_to_list::<ListArray>(array, row_count)?,
        DataType::LargeList(_) => list_array_to_list::<LargeListArray>(array, row_count)?,
        // STRUCT(...): each field becomes a parallel child vector sharing
        // the parent row count.
        DataType::Struct(_) => struct_array_to_struct(array, row_count)?,
        // Arrow `Map` is physically `List<Struct<keys, values>>`. DuckDB also
        // stores MAP as LIST<STRUCT<key, value>>, so we reuse the LIST wire
        // payload and just stamp the parent LogicalTypeId as `Map`.
        DataType::Map(_, _) => map_array_to_map(array, row_count)?,
        // FixedSizeList<T, N>: DuckDB ARRAY. Flat child vector with N elements
        // per parent row, no per-row offsets needed.
        DataType::FixedSizeList(_, _) => fixed_size_list_to_array(array, row_count)?,
        // Arrow `Dictionary(Key, Utf8)` with a string value type maps cleanly
        // to DuckDB ENUM. Keys narrow to u8/u16/u32 per the dict-size tier.
        // Non-string value types (Binary, Int, etc.) aren't valid ENUM payloads
        // and fall through to the unsupported branch below.
        DataType::Dictionary(key_dt, value_dt)
            if matches!(value_dt.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            dictionary_to_enum(array, row_count, key_dt.as_ref())?
        }
        // Arrow interval types all widen into DuckDB's 16-byte `interval_t`
        // { months: i32, days: i32, micros: i64 }.
        DataType::Interval(IntervalUnit::YearMonth) => {
            interval_yearmonth_to_interval(array, row_count)?
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            interval_daytime_to_interval(array, row_count)?
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            interval_monthdaynano_to_interval(array, row_count)?
        }
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
fixed_width_impl!(Date32Array, i32);
fixed_width_impl!(TimestampSecondArray, i64);
fixed_width_impl!(TimestampMillisecondArray, i64);
fixed_width_impl!(TimestampMicrosecondArray, i64);
fixed_width_impl!(TimestampNanosecondArray, i64);
fixed_width_impl!(Time64MicrosecondArray, i64);
fixed_width_impl!(Time64NanosecondArray, i64);

/// Arrow's `Date64` is `i64` ms since UNIX epoch. DuckDB's `DATE` is a
/// 4-byte day count, so we divide out 86_400_000 to convert.
fn date64_to_date(array: &dyn Array, row_count: usize) -> crate::Result<(LogicalType, VectorData)> {
    const MS_PER_DAY: i64 = 86_400_000;
    let typed = array
        .as_any()
        .downcast_ref::<Date64Array>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("date64 downcast failed".to_string())
        })?;
    let mut bytes = Vec::with_capacity(row_count * 4);
    for i in 0..row_count {
        let v: i32 = if typed.is_valid(i) {
            (typed.value(i) / MS_PER_DAY) as i32
        } else {
            0
        };
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    Ok((
        LogicalType::new(LogicalTypeId::Date),
        VectorData::Fixed(bytes),
    ))
}

/// Arrow `Time32(Second)` is i32 seconds-of-day. DuckDB `TIME` is i64
/// microseconds-of-day, so we widen and multiply by 1_000_000.
fn time32_second_to_time(
    array: &dyn Array,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<Time32SecondArray>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("time32(second) downcast failed".to_string())
        })?;
    let mut bytes = Vec::with_capacity(row_count * 8);
    for i in 0..row_count {
        let v: i64 = if typed.is_valid(i) {
            (typed.value(i) as i64) * 1_000_000
        } else {
            0
        };
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    Ok((
        LogicalType::new(LogicalTypeId::Time),
        VectorData::Fixed(bytes),
    ))
}

/// Arrow `Time32(Millisecond)` is i32 ms-of-day. Widen ×1_000 into i64
/// microseconds-of-day.
fn time32_millisecond_to_time(
    array: &dyn Array,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType(
                "time32(millisecond) downcast failed".to_string(),
            )
        })?;
    let mut bytes = Vec::with_capacity(row_count * 8);
    for i in 0..row_count {
        let v: i64 = if typed.is_valid(i) {
            (typed.value(i) as i64) * 1_000
        } else {
            0
        };
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    Ok((
        LogicalType::new(LogicalTypeId::Time),
        VectorData::Fixed(bytes),
    ))
}

/// Arrow `FixedSizeBinary(16)` is the canonical UUID layout. DuckDB's UUID
/// is also 16 bytes on the wire — copy raw.
fn fixed_size_binary_to_uuid(
    array: &dyn Array,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("fixed-size-binary downcast failed".to_string())
        })?;
    let mut bytes = Vec::with_capacity(row_count * 16);
    let zero = [0u8; 16];
    for i in 0..row_count {
        let src: &[u8] = if typed.is_valid(i) {
            typed.value(i)
        } else {
            &zero
        };
        debug_assert_eq!(src.len(), 16, "FixedSizeBinary(16) row width");
        bytes.extend_from_slice(src);
    }
    Ok((
        LogicalType::new(LogicalTypeId::Uuid),
        VectorData::Fixed(bytes),
    ))
}

/// Write one row of DuckDB's `interval_t { months: i32, days: i32, micros: i64 }`
/// into the buffer as 16 little-endian bytes.
fn push_interval(out: &mut Vec<u8>, months: i32, days: i32, micros: i64) {
    out.extend_from_slice(&months.to_le_bytes());
    out.extend_from_slice(&days.to_le_bytes());
    out.extend_from_slice(&micros.to_le_bytes());
}

/// Arrow `Interval(YearMonth)` is i32 months. days/micros default to 0.
fn interval_yearmonth_to_interval(
    array: &dyn Array,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<IntervalYearMonthArray>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("interval-yearmonth downcast failed".to_string())
        })?;
    let mut bytes = Vec::with_capacity(row_count * 16);
    for i in 0..row_count {
        let months = if typed.is_valid(i) { typed.value(i) } else { 0 };
        push_interval(&mut bytes, months, 0, 0);
    }
    Ok((
        LogicalType::new(LogicalTypeId::Interval),
        VectorData::Fixed(bytes),
    ))
}

/// Arrow `Interval(DayTime)` packs 32-bit days + 32-bit ms. Widen ms ->
/// micros (×1000) for DuckDB.
fn interval_daytime_to_interval(
    array: &dyn Array,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<IntervalDayTimeArray>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("interval-daytime downcast failed".to_string())
        })?;
    let mut bytes = Vec::with_capacity(row_count * 16);
    for i in 0..row_count {
        if typed.is_valid(i) {
            let v = typed.value(i);
            let micros = (v.milliseconds as i64) * 1_000;
            push_interval(&mut bytes, 0, v.days, micros);
        } else {
            push_interval(&mut bytes, 0, 0, 0);
        }
    }
    Ok((
        LogicalType::new(LogicalTypeId::Interval),
        VectorData::Fixed(bytes),
    ))
}

/// Arrow `Interval(MonthDayNano)` carries 32-bit months + 32-bit days + 64-bit
/// nanoseconds. DuckDB's micros field is i64; we floor-divide ns by 1000.
fn interval_monthdaynano_to_interval(
    array: &dyn Array,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType(
                "interval-monthdaynano downcast failed".to_string(),
            )
        })?;
    let mut bytes = Vec::with_capacity(row_count * 16);
    for i in 0..row_count {
        if typed.is_valid(i) {
            let v = typed.value(i);
            let micros = v.nanoseconds / 1_000;
            push_interval(&mut bytes, v.months, v.days, micros);
        } else {
            push_interval(&mut bytes, 0, 0, 0);
        }
    }
    Ok((
        LogicalType::new(LogicalTypeId::Interval),
        VectorData::Fixed(bytes),
    ))
}

/// Trait abstracting `ListArray` (i32 offsets) and `LargeListArray` (i64
/// offsets). Both expose `value_offsets()` and `values()`; the offset type
/// converts cleanly into `u64` for DuckDB's `list_entry_t.offset`/`length`.
trait OffsetListArray: Array + 'static {
    fn offsets_at(&self, idx: usize) -> (i64, i64);
    fn values_ref(&self) -> &dyn Array;
}

impl OffsetListArray for ListArray {
    fn offsets_at(&self, idx: usize) -> (i64, i64) {
        let offs = self.value_offsets();
        (offs[idx] as i64, offs[idx + 1] as i64)
    }
    fn values_ref(&self) -> &dyn Array {
        ListArray::values(self).as_ref()
    }
}

impl OffsetListArray for LargeListArray {
    fn offsets_at(&self, idx: usize) -> (i64, i64) {
        let offs = self.value_offsets();
        (offs[idx], offs[idx + 1])
    }
    fn values_ref(&self) -> &dyn Array {
        LargeListArray::values(self).as_ref()
    }
}

fn list_array_to_list<A: OffsetListArray>(
    array: &dyn Array,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array.as_any().downcast_ref::<A>().ok_or_else(|| {
        crate::WireError::UnsupportedArrowType("list downcast failed".to_string())
    })?;
    let mut entries = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let (start, end) = typed.offsets_at(i);
        entries.push((start as u64, (end - start) as u64));
    }
    let values_array = typed.values_ref();
    let child_count = values_array.len();
    let child_vector = column_to_vector(values_array, child_count)?;
    let child_logical = child_vector.logical_type.clone();
    Ok((
        LogicalType::with_extra(
            LogicalTypeId::List,
            ExtraTypeInfo::List {
                child: Box::new(child_logical),
            },
        ),
        VectorData::List {
            entries,
            child_count: child_count as u64,
            child: Box::new(child_vector),
        },
    ))
}

fn struct_array_to_struct(
    array: &dyn Array,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("struct downcast failed".to_string())
        })?;
    let fields = typed.fields();
    let mut children = Vec::with_capacity(fields.len());
    let mut field_types = Vec::with_capacity(fields.len());
    for (i, f) in fields.iter().enumerate() {
        let child_arr = typed.column(i);
        let child_vec = column_to_vector(child_arr.as_ref(), row_count)?;
        field_types.push((f.name().clone(), child_vec.logical_type.clone()));
        children.push(child_vec);
    }
    Ok((
        LogicalType::with_extra(
            LogicalTypeId::Struct,
            ExtraTypeInfo::Struct {
                fields: field_types,
            },
        ),
        VectorData::Struct { children },
    ))
}

/// Arrow `Map` is physically `List<Struct<keys, values>>`. We reuse the LIST
/// `VectorData` and stamp the parent `LogicalTypeId::Map` so the downstream
/// codec emits the right type id (the inner `ExtraTypeInfo::List` is identical
/// to what DuckDB itself stores for `LogicalType::MAP`).
fn map_array_to_map(
    array: &dyn Array,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| crate::WireError::UnsupportedArrowType("map downcast failed".to_string()))?;
    let mut entries = Vec::with_capacity(row_count);
    let offs = typed.value_offsets();
    for i in 0..row_count {
        let start = offs[i] as u64;
        let end = offs[i + 1] as u64;
        entries.push((start, end - start));
    }
    // Arrow stores Map.entries() as a StructArray with the keys + values children.
    let entries_arr: &dyn Array = typed.entries();
    let child_count = entries_arr.len();
    let child_vector = column_to_vector(entries_arr, child_count)?;
    let child_logical = child_vector.logical_type.clone();
    Ok((
        LogicalType::with_extra(
            LogicalTypeId::Map,
            ExtraTypeInfo::List {
                child: Box::new(child_logical),
            },
        ),
        VectorData::List {
            entries,
            child_count: child_count as u64,
            child: Box::new(child_vector),
        },
    ))
}

/// Arrow `Dictionary(KeyType, Utf8|LargeUtf8)` -> DuckDB `ENUM`. The dictionary
/// strings populate `ExtraTypeInfo::Enum.values`; per-row keys narrow to u8/u16/u32
/// indices based on the dictionary size tier (see [`enum_physical_width`]).
fn dictionary_to_enum(
    array: &dyn Array,
    row_count: usize,
    key_dt: &DataType,
) -> crate::Result<(LogicalType, VectorData)> {
    // Each per-row key arrives as u64 (any int type fits, including u64).
    // dictionary_keys_to_u64 monomorphizes on the Arrow key type and rejects
    // negative signed indices (ENUM indices are unsigned in DuckDB).
    let (values, keys) = match key_dt {
        DataType::Int8 => {
            dictionary_keys_to_u64::<arrow_types::Int8Type, _>(array, row_count, |v| {
                check_unsigned_key(v as i64)
            })?
        }
        DataType::Int16 => {
            dictionary_keys_to_u64::<arrow_types::Int16Type, _>(array, row_count, |v| {
                check_unsigned_key(v as i64)
            })?
        }
        DataType::Int32 => {
            dictionary_keys_to_u64::<arrow_types::Int32Type, _>(array, row_count, |v| {
                check_unsigned_key(v as i64)
            })?
        }
        DataType::Int64 => dictionary_keys_to_u64::<arrow_types::Int64Type, _>(
            array,
            row_count,
            check_unsigned_key,
        )?,
        DataType::UInt8 => {
            dictionary_keys_to_u64::<arrow_types::UInt8Type, _>(array, row_count, |v| Ok(v as u64))?
        }
        DataType::UInt16 => dictionary_keys_to_u64::<arrow_types::UInt16Type, _>(
            array,
            row_count,
            |v| Ok(v as u64),
        )?,
        DataType::UInt32 => dictionary_keys_to_u64::<arrow_types::UInt32Type, _>(
            array,
            row_count,
            |v| Ok(v as u64),
        )?,
        DataType::UInt64 => {
            dictionary_keys_to_u64::<arrow_types::UInt64Type, _>(array, row_count, Ok)?
        }
        other => {
            return Err(crate::WireError::UnsupportedArrowType(format!(
                "Dictionary key type {other:?} not supported for ENUM mapping"
            )))
        }
    };

    let width = enum_physical_width(values.len());
    let mut bytes = Vec::with_capacity(row_count * width);
    for k in keys {
        match width {
            1 => bytes.push(k as u8),
            2 => bytes.extend_from_slice(&(k as u16).to_le_bytes()),
            4 => bytes.extend_from_slice(&(k as u32).to_le_bytes()),
            _ => {
                return Err(crate::WireError::UnsupportedLogicalType(
                    LogicalTypeId::Enum,
                ))
            }
        }
    }
    Ok((
        LogicalType::with_extra(LogicalTypeId::Enum, ExtraTypeInfo::Enum { values }),
        VectorData::Fixed(bytes),
    ))
}

fn check_unsigned_key(raw: i64) -> crate::Result<u64> {
    if raw < 0 {
        return Err(crate::WireError::UnsupportedArrowType(format!(
            "Dictionary key {raw} is negative; ENUM indices must be unsigned"
        )));
    }
    Ok(raw as u64)
}

/// Helper: downcast to `DictionaryArray<K>` and emit (dictionary strings,
/// per-row keys widened to u64). The `key_to_u64` closure handles the
/// signed-vs-unsigned native-type conversion.
fn dictionary_keys_to_u64<K, F>(
    array: &dyn Array,
    row_count: usize,
    key_to_u64: F,
) -> crate::Result<(Vec<String>, Vec<u64>)>
where
    K: arrow_array::types::ArrowDictionaryKeyType,
    F: Fn(K::Native) -> crate::Result<u64>,
{
    let typed = array
        .as_any()
        .downcast_ref::<DictionaryArray<K>>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("dictionary downcast failed".to_string())
        })?;
    let values_arr = typed.values();
    let values: Vec<String> = if let Some(a) = values_arr.as_string_opt::<i32>() {
        (0..a.len()).map(|i| a.value(i).to_string()).collect()
    } else if let Some(a) = values_arr.as_string_opt::<i64>() {
        (0..a.len()).map(|i| a.value(i).to_string()).collect()
    } else {
        return Err(crate::WireError::UnsupportedArrowType(
            "Dictionary value type must be Utf8 or LargeUtf8 for ENUM".to_string(),
        ));
    };
    let key_arr = typed.keys();
    let mut keys = Vec::with_capacity(row_count);
    for i in 0..row_count {
        if key_arr.is_valid(i) {
            keys.push(key_to_u64(key_arr.value(i))?);
        } else {
            keys.push(0);
        }
    }
    Ok((values, keys))
}

/// Arrow `FixedSizeList<T, N>` -> DuckDB `ARRAY<T, N>`. Flat child vector with
/// `N * row_count` elements; no per-row offsets.
fn fixed_size_list_to_array(
    array: &dyn Array,
    row_count: usize,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("fixed-size-list downcast failed".to_string())
        })?;
    let array_size = typed.value_length();
    if array_size < 0 {
        return Err(crate::WireError::UnsupportedArrowType(format!(
            "FixedSizeList with negative element size {array_size}"
        )));
    }
    let array_size_u32 = u32::try_from(array_size).map_err(|_| {
        crate::WireError::UnsupportedArrowType(format!(
            "FixedSizeList size {array_size} exceeds u32"
        ))
    })?;
    let child_arr: &dyn Array = typed.values();
    let child_count = (array_size as usize) * row_count;
    let child_vector = column_to_vector(child_arr, child_count)?;
    let child_logical = child_vector.logical_type.clone();
    Ok((
        LogicalType::with_extra(
            LogicalTypeId::Array,
            ExtraTypeInfo::Array {
                child: Box::new(child_logical),
                size: array_size_u32,
            },
        ),
        VectorData::Array {
            array_size: array_size as u64,
            child: Box::new(child_vector),
        },
    ))
}

/// Arrow `Decimal128(precision, scale)` -> DuckDB `DECIMAL(precision, scale)`.
///
/// DuckDB picks an integer width based on the precision tier
/// ([`decimal_physical_width`]); we narrow each i128 row accordingly. The
/// narrowing is value-preserving because `Decimal128` guarantees the digit
/// count fits within precision. Negative scales would not fit `uint8_t` on the
/// wire and are rejected up front.
fn decimal128_to_decimal(
    array: &dyn Array,
    row_count: usize,
    precision: u8,
    scale: i8,
) -> crate::Result<(LogicalType, VectorData)> {
    let scale_u8 = u8::try_from(scale).map_err(|_| {
        crate::WireError::UnsupportedArrowType(format!(
            "Decimal128 with negative scale {scale} is not representable in DuckDB DecimalTypeInfo"
        ))
    })?;
    let typed = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("decimal128 downcast failed".to_string())
        })?;
    let width = decimal_physical_width(precision);
    let mut bytes = Vec::with_capacity(row_count * width);
    for i in 0..row_count {
        let v: i128 = if typed.is_valid(i) { typed.value(i) } else { 0 };
        match width {
            2 => bytes.extend_from_slice(&(v as i16).to_le_bytes()),
            4 => bytes.extend_from_slice(&(v as i32).to_le_bytes()),
            8 => bytes.extend_from_slice(&(v as i64).to_le_bytes()),
            16 => bytes.extend_from_slice(&v.to_le_bytes()),
            _ => {
                return Err(crate::WireError::UnsupportedLogicalType(
                    LogicalTypeId::Decimal,
                ))
            }
        }
    }
    Ok((
        LogicalType::with_extra(
            LogicalTypeId::Decimal,
            ExtraTypeInfo::Decimal {
                precision,
                scale: scale_u8,
            },
        ),
        VectorData::Fixed(bytes),
    ))
}

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

fn string_view_to_strings(array: &dyn Array) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("string-view downcast failed".to_string())
        })?;
    let len = typed.len();
    let mut values = Vec::with_capacity(len);
    for i in 0..len {
        if typed.is_null(i) {
            values.push(None);
        } else {
            values.push(Some(typed.value(i).to_string()));
        }
    }
    Ok((
        LogicalType::new(LogicalTypeId::Varchar),
        VectorData::Strings(values),
    ))
}

trait BinaryLikeArray: Array + 'static {
    fn value_at(&self, idx: usize) -> &[u8];
}

impl BinaryLikeArray for BinaryArray {
    fn value_at(&self, idx: usize) -> &[u8] {
        self.value(idx)
    }
}

impl BinaryLikeArray for LargeBinaryArray {
    fn value_at(&self, idx: usize) -> &[u8] {
        self.value(idx)
    }
}

fn binary_to_blobs<A: BinaryLikeArray>(
    array: &dyn Array,
) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array.as_any().downcast_ref::<A>().ok_or_else(|| {
        crate::WireError::UnsupportedArrowType("binary downcast failed".to_string())
    })?;
    let len = typed.len();
    let mut values = Vec::with_capacity(len);
    for i in 0..len {
        if typed.is_null(i) {
            values.push(None);
        } else {
            values.push(Some(typed.value_at(i).to_vec()));
        }
    }
    Ok((
        LogicalType::new(LogicalTypeId::Blob),
        VectorData::Blobs(values),
    ))
}

fn binary_view_to_blobs(array: &dyn Array) -> crate::Result<(LogicalType, VectorData)> {
    let typed = array
        .as_any()
        .downcast_ref::<BinaryViewArray>()
        .ok_or_else(|| {
            crate::WireError::UnsupportedArrowType("binary-view downcast failed".to_string())
        })?;
    let len = typed.len();
    let mut values = Vec::with_capacity(len);
    for i in 0..len {
        if typed.is_null(i) {
            values.push(None);
        } else {
            values.push(Some(typed.value(i).to_vec()));
        }
    }
    Ok((
        LogicalType::new(LogicalTypeId::Blob),
        VectorData::Blobs(values),
    ))
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

// -----------------------------------------------------------------------------
// Reverse direction: Vector / DataChunk -> Arrow
// -----------------------------------------------------------------------------

/// Convert a single `Vector` back to an Arrow array. Inverse of
/// `column_to_vector`.
fn vector_to_array(vector: &Vector, row_count: usize) -> crate::Result<std::sync::Arc<dyn Array>> {
    use std::sync::Arc;
    let nulls = validity_to_null_buffer(vector.validity.as_deref(), row_count)?;
    match (&vector.data, vector.logical_type.id) {
        (VectorData::Strings(values), _) => {
            // VARCHAR -> StringArray. Capacity is bounded by the decoded
            // payload, never by the wire header alone (issue #612).
            ensure_rows_backed(values.len(), row_count)?;
            let mut builder = arrow_array::builder::StringBuilder::with_capacity(row_count, 0);
            for v in values.iter().take(row_count) {
                match v {
                    Some(s) => builder.append_value(s),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (VectorData::Blobs(values), _) => {
            ensure_rows_backed(values.len(), row_count)?;
            let mut builder = arrow_array::builder::BinaryBuilder::with_capacity(row_count, 0);
            for v in values.iter().take(row_count) {
                match v {
                    Some(b) => builder.append_value(b),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (VectorData::Fixed(_), _) => {
            let VectorData::Fixed(bytes) = &vector.data else {
                unreachable!()
            };
            fixed_to_array(
                vector.logical_type.id,
                bytes,
                row_count,
                nulls,
                &vector.logical_type,
            )
        }
        (
            VectorData::List {
                entries,
                child_count,
                child,
            },
            id,
        ) => list_or_map_to_array(
            id,
            &vector.logical_type,
            entries,
            *child_count,
            child,
            row_count,
            nulls,
        ),
        (VectorData::Struct { children }, _) => {
            struct_to_array(&vector.logical_type, children, row_count, nulls)
        }
        (VectorData::Array { array_size, child }, _) => fixed_size_list_to_array_arrow(
            &vector.logical_type,
            *array_size,
            child,
            row_count,
            nulls,
        ),
    }
}

/// Convert a `VectorData::List` back to an Arrow `ListArray`. When the parent
/// LogicalTypeId is `Map`, wraps the result in a `MapArray` — DuckDB stores
/// MAP as a LIST<STRUCT<key, value>> physically, so the same VectorData shape
/// produces both Arrow types based on which id the caller set.
fn list_or_map_to_array(
    id: LogicalTypeId,
    parent_type: &LogicalType,
    entries: &[(u64, u64)],
    child_count: u64,
    child: &Vector,
    row_count: usize,
    nulls: Option<arrow_buffer::NullBuffer>,
) -> crate::Result<std::sync::Arc<dyn Array>> {
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use std::sync::Arc;
    // The header promised `row_count` rows; the entries list has to back every
    // one of them before anything is sized from `row_count` (issue #612).
    ensure_rows_backed(entries.len(), row_count)?;
    let entries = &entries[..row_count];

    // Pass 1 (pure arithmetic, no allocation): every `(off, len)` must fit
    // Arrow's i32 offsets, and so must the compacted total. `off + len` and
    // the running total are wire-controlled u64s; unchecked they wrap in
    // release and hand `OffsetBuffer::new` a non-monotonic buffer (a panic).
    let i32_max = i32::MAX as u64;
    let mut total: u64 = 0;
    let mut contiguous = true;
    let mut expected_off: u64 = 0;
    for &(off, len) in entries {
        let end = off
            .checked_add(len)
            .filter(|end| *end <= i32_max)
            .ok_or_else(|| list_offset_error(off, len))?;
        total = total
            .checked_add(len)
            .filter(|t| *t <= i32_max)
            .ok_or_else(|| list_offset_error(off, len))?;
        if off != expected_off {
            contiguous = false;
        }
        expected_off = end;
    }

    let child_array = vector_to_array(child, child_count as usize)?;
    let child_len = child_array.len() as u64;
    // Pass 2: every range must lie inside the decoded child. `Array::slice`
    // asserts this and would panic on a hostile entry.
    for &(off, len) in entries {
        // Cannot overflow: checked against i32::MAX in pass 1.
        let end = off + len;
        if end > child_len {
            return Err(crate::WireError::CountExceedsRemaining {
                count: end,
                remaining: child_len,
            });
        }
    }

    // For Arrow ListArray the offsets must be monotonic starting at 0. DuckDB
    // sometimes stores list entries with gaps in the child buffer (a row's
    // `offset` doesn't match the prior row's `offset + length`); when it does
    // we compact by copying the relevant child slice for each row. The fast
    // path reuses the child as-is when the entries are already contiguous.
    let (final_offsets, final_child) = if contiguous {
        let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
        offsets.push(0);
        for &(off, len) in entries {
            // Fits: `off + len <= i32::MAX` was established in pass 1.
            offsets.push((off + len) as i32);
        }
        (offsets, child_array)
    } else {
        // Compact: build new offsets [0, len0, len0+len1, ...] and a new child
        // array that's a concatenation of each row's slice.
        let mut new_offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
        new_offsets.push(0);
        let mut accum: u64 = 0;
        let mut slices: Vec<std::sync::Arc<dyn Array>> = Vec::with_capacity(row_count);
        for &(off, len) in entries {
            // `total <= i32::MAX` from pass 1 bounds every partial sum.
            accum += len;
            new_offsets.push(accum as i32);
            slices.push(child_array.slice(off as usize, len as usize));
        }
        let refs: Vec<&dyn Array> = slices.iter().map(|a| a.as_ref()).collect();
        let concatenated = arrow::compute::concat(&refs).map_err(|e| {
            crate::WireError::UnsupportedArrowType(format!(
                "list compact: arrow concat failed: {e}"
            ))
        })?;
        (new_offsets, concatenated)
    };
    let offsets_buf = OffsetBuffer::new(ScalarBuffer::from(final_offsets));
    let dt = logical_type_to_arrow(parent_type)?;
    match id {
        LogicalTypeId::List => {
            let field = match &dt {
                arrow_schema::DataType::List(f) => f.clone(),
                _ => {
                    return Err(crate::WireError::UnsupportedArrowType(format!(
                        "expected DataType::List, got {dt:?}"
                    )))
                }
            };
            let list = arrow_array::ListArray::try_new(field, offsets_buf, final_child, nulls)
                .map_err(arrow_err)?;
            Ok(Arc::new(list))
        }
        LogicalTypeId::Map => {
            // The child of MAP is STRUCT(key, value). Arrow MapArray wraps it
            // as a single field named "entries".
            let entries_field = match &dt {
                arrow_schema::DataType::Map(f, _) => f.clone(),
                _ => {
                    return Err(crate::WireError::UnsupportedArrowType(format!(
                        "expected DataType::Map, got {dt:?}"
                    )))
                }
            };
            let decoded_struct = final_child
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .ok_or_else(|| {
                    crate::WireError::UnsupportedArrowType(
                        "MAP child must decode as StructArray".to_string(),
                    )
                })?;
            // `struct_to_array` marks every field nullable; the map entries
            // type has a non-nullable key. Rebuild the entries with the exact
            // field set `logical_type_to_arrow` produced so the two agree.
            // `try_new` rejects a key column that carries nulls.
            let entries_fields = match entries_field.data_type() {
                arrow_schema::DataType::Struct(f) => f.clone(),
                other => {
                    return Err(crate::WireError::UnsupportedArrowType(format!(
                        "MAP entries field must be a struct, got {other:?}"
                    )))
                }
            };
            let (_, columns, struct_nulls) = decoded_struct.clone().into_parts();
            let struct_arr = arrow_array::StructArray::try_new_with_length(
                entries_fields,
                columns,
                struct_nulls,
                decoded_struct.len(),
            )
            .map_err(arrow_err)?;
            let map = arrow_array::MapArray::try_new(
                entries_field,
                offsets_buf,
                struct_arr,
                nulls,
                false,
            )
            .map_err(arrow_err)?;
            Ok(Arc::new(map))
        }
        other => Err(crate::WireError::UnsupportedLogicalType(other)),
    }
}

/// A LIST/MAP entry whose `offset + length` (or the compacted running total)
/// does not fit Arrow's i32 offsets.
fn list_offset_error(off: u64, len: u64) -> crate::WireError {
    crate::WireError::UnsupportedArrowType(format!(
        "list entry (offset {off}, length {len}) exceeds Arrow's i32 offset limit"
    ))
}

/// Map an arrow constructor failure to a codec error. Every `try_new` in this
/// module goes through here so a malformed chunk is a `WireError`, never an
/// arrow assert.
fn arrow_err(e: arrow_schema::ArrowError) -> crate::WireError {
    crate::WireError::UnsupportedArrowType(format!("arrow rejected decoded chunk: {e}"))
}

/// The header promised `row_count` rows but the decoded payload only backs
/// `backed`. Every `row_count`-sized allocation in this module is preceded by
/// this check so a hostile header cannot drive a huge reservation (issue #612).
fn ensure_rows_backed(backed: usize, row_count: usize) -> crate::Result<()> {
    if backed < row_count {
        return Err(crate::WireError::CountExceedsRemaining {
            count: row_count as u64,
            remaining: backed as u64,
        });
    }
    Ok(())
}

/// Fixed-width variant of [`ensure_rows_backed`]: the buffer must hold at
/// least `row_count * width` bytes. Overflow of the product is itself a
/// mismatch.
fn ensure_fixed_backed(bytes: &[u8], row_count: usize, width: usize) -> crate::Result<()> {
    let needed = row_count
        .checked_mul(width)
        .ok_or(crate::WireError::CountExceedsRemaining {
            count: row_count as u64,
            remaining: bytes.len() as u64,
        })?;
    if bytes.len() < needed {
        return Err(crate::WireError::CountExceedsRemaining {
            count: needed as u64,
            remaining: bytes.len() as u64,
        });
    }
    Ok(())
}

/// Build a primitive array through `try_new` so a values/nulls length
/// mismatch surfaces as a [`crate::WireError`] instead of an arrow panic.
fn primitive<T: arrow_array::ArrowPrimitiveType>(
    values: arrow_buffer::ScalarBuffer<T::Native>,
    nulls: Option<arrow_buffer::NullBuffer>,
) -> crate::Result<std::sync::Arc<dyn Array>> {
    let array = arrow_array::PrimitiveArray::<T>::try_new(values, nulls).map_err(arrow_err)?;
    Ok(std::sync::Arc::new(array))
}

fn struct_to_array(
    parent_type: &LogicalType,
    children: &[Vector],
    row_count: usize,
    nulls: Option<arrow_buffer::NullBuffer>,
) -> crate::Result<std::sync::Arc<dyn Array>> {
    use std::sync::Arc;
    let fields = match &parent_type.extra {
        Some(crate::data_chunk::ExtraTypeInfo::Struct { fields }) => fields,
        _ => {
            return Err(crate::WireError::UnsupportedLogicalType(
                LogicalTypeId::Struct,
            ))
        }
    };
    if fields.len() != children.len() {
        return Err(crate::WireError::UnexpectedField {
            expected: fields.len() as u16,
            actual: children.len() as u16,
        });
    }
    let mut arrow_fields = Vec::with_capacity(fields.len());
    let mut arrow_arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(children.len());
    for ((name, _ty), child) in fields.iter().zip(children.iter()) {
        let arr = vector_to_array(child, row_count)?;
        arrow_fields.push(arrow_schema::Field::new(
            name.clone(),
            arr.data_type().clone(),
            true,
        ));
        arrow_arrays.push(arr);
    }
    let fields_ref: arrow_schema::Fields = arrow_fields.into();
    // `try_new_with_length` validates every child against `row_count` and
    // returns an error instead of asserting.
    let array =
        arrow_array::StructArray::try_new_with_length(fields_ref, arrow_arrays, nulls, row_count)
            .map_err(arrow_err)?;
    Ok(Arc::new(array))
}

fn fixed_size_list_to_array_arrow(
    parent_type: &LogicalType,
    array_size: u64,
    child: &Vector,
    row_count: usize,
    nulls: Option<arrow_buffer::NullBuffer>,
) -> crate::Result<std::sync::Arc<dyn Array>> {
    use std::sync::Arc;
    let dt = logical_type_to_arrow(parent_type)?;
    let field = match &dt {
        arrow_schema::DataType::FixedSizeList(f, _) => f.clone(),
        _ => {
            return Err(crate::WireError::UnsupportedArrowType(format!(
                "expected DataType::FixedSizeList, got {dt:?}"
            )))
        }
    };
    // `logical_type_to_arrow` already rejected sizes past i32::MAX; the
    // product is still wire-controlled and must not wrap.
    let size = i32::try_from(array_size).map_err(|_| {
        crate::WireError::UnsupportedArrowType(format!(
            "ARRAY size {array_size} exceeds Arrow's i32 FixedSizeList limit"
        ))
    })?;
    let child_count =
        (size as usize)
            .checked_mul(row_count)
            .ok_or(crate::WireError::CountExceedsRemaining {
                count: array_size,
                remaining: row_count as u64,
            })?;
    let child_arr = vector_to_array(child, child_count)?;
    let array = arrow_array::FixedSizeListArray::try_new_with_length(
        field, size, child_arr, nulls, row_count,
    )
    .map_err(arrow_err)?;
    Ok(Arc::new(array))
}

/// Build a typed Arrow array from a tightly-packed little-endian byte buffer.
/// Mirrors the forward `fixed_from_array` path, monomorphised on `LogicalTypeId`.
fn fixed_to_array(
    id: LogicalTypeId,
    bytes: &[u8],
    row_count: usize,
    nulls: Option<arrow_buffer::NullBuffer>,
    logical_type: &LogicalType,
) -> crate::Result<std::sync::Arc<dyn Array>> {
    use arrow_buffer::ScalarBuffer;
    use arrow_types as t;
    use std::sync::Arc;
    match id {
        LogicalTypeId::Boolean => {
            ensure_fixed_backed(bytes, row_count, 1)?;
            let mut builder = arrow_array::builder::BooleanBuilder::with_capacity(row_count);
            for (i, byte) in bytes.iter().enumerate().take(row_count) {
                let valid = nulls.as_ref().map(|n| n.is_valid(i)).unwrap_or(true);
                if valid {
                    builder.append_value(*byte != 0);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        LogicalTypeId::TinyInt => {
            primitive::<t::Int8Type>(scalar_buffer_le::<i8>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::SmallInt => {
            primitive::<t::Int16Type>(scalar_buffer_le::<i16>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::Integer => {
            primitive::<t::Int32Type>(scalar_buffer_le::<i32>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::BigInt => {
            primitive::<t::Int64Type>(scalar_buffer_le::<i64>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::UTinyInt => {
            primitive::<t::UInt8Type>(scalar_buffer_le::<u8>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::USmallInt => {
            primitive::<t::UInt16Type>(scalar_buffer_le::<u16>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::UInteger => {
            primitive::<t::UInt32Type>(scalar_buffer_le::<u32>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::UBigInt => {
            primitive::<t::UInt64Type>(scalar_buffer_le::<u64>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::Float => {
            primitive::<t::Float32Type>(scalar_buffer_le::<f32>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::Double => {
            primitive::<t::Float64Type>(scalar_buffer_le::<f64>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::Date => {
            primitive::<t::Date32Type>(scalar_buffer_le::<i32>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::Time => {
            primitive::<t::Time64MicrosecondType>(scalar_buffer_le::<i64>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::TimeNs => {
            primitive::<t::Time64NanosecondType>(scalar_buffer_le::<i64>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::Timestamp => primitive::<t::TimestampMicrosecondType>(
            scalar_buffer_le::<i64>(bytes, row_count)?,
            nulls,
        ),
        LogicalTypeId::TimestampSec => {
            primitive::<t::TimestampSecondType>(scalar_buffer_le::<i64>(bytes, row_count)?, nulls)
        }
        LogicalTypeId::TimestampMs => primitive::<t::TimestampMillisecondType>(
            scalar_buffer_le::<i64>(bytes, row_count)?,
            nulls,
        ),
        LogicalTypeId::TimestampNs => primitive::<t::TimestampNanosecondType>(
            scalar_buffer_le::<i64>(bytes, row_count)?,
            nulls,
        ),
        LogicalTypeId::Uuid => {
            ensure_fixed_backed(bytes, row_count, 16)?;
            let mut builder =
                arrow_array::builder::FixedSizeBinaryBuilder::with_capacity(row_count, 16);
            for i in 0..row_count {
                let valid = nulls.as_ref().map(|n| n.is_valid(i)).unwrap_or(true);
                if valid {
                    let slice = checked_fixed_slice(bytes, i, 16)?;
                    builder.append_value(slice).map_err(|e| {
                        crate::WireError::UnsupportedArrowType(format!("uuid append: {e}"))
                    })?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        LogicalTypeId::Interval => {
            // DuckDB interval_t = { months: i32, days: i32, micros: i64 }; Arrow's
            // closest match is Interval(MonthDayNano) which stores months/days/ns.
            // Widen micros -> nanoseconds on the way back.
            use arrow_array::types::IntervalMonthDayNano;
            ensure_fixed_backed(bytes, row_count, 16)?;
            let mut values: Vec<IntervalMonthDayNano> = Vec::with_capacity(row_count);
            for i in 0..row_count {
                // Checked slicing: row_count is wire-controlled and may exceed
                // the supplied buffer; a clear error beats an OOB panic.
                let chunk = checked_fixed_slice(bytes, i, 16)?;
                let months = i32::from_le_bytes(chunk[0..4].try_into().unwrap());
                let days = i32::from_le_bytes(chunk[4..8].try_into().unwrap());
                let micros = i64::from_le_bytes(chunk[8..16].try_into().unwrap());
                values.push(IntervalMonthDayNano {
                    months,
                    days,
                    nanoseconds: micros.saturating_mul(1_000),
                });
            }
            primitive::<t::IntervalMonthDayNanoType>(ScalarBuffer::from(values), nulls)
        }
        LogicalTypeId::Decimal => {
            let (precision, scale) = match &logical_type.extra {
                Some(crate::data_chunk::ExtraTypeInfo::Decimal { precision, scale }) => {
                    (*precision, *scale)
                }
                _ => {
                    return Err(crate::WireError::UnsupportedLogicalType(
                        LogicalTypeId::Decimal,
                    ))
                }
            };
            let width = crate::data_chunk::decimal_physical_width(precision);
            ensure_fixed_backed(bytes, row_count, width)?;
            let mut values: Vec<i128> = Vec::with_capacity(row_count);
            for i in 0..row_count {
                // `width` is wire-influenced via `precision`; slice with bounds
                // checks rather than trusting `row_count * width <= bytes.len()`.
                let chunk = checked_fixed_slice(bytes, i, width)?;
                let v: i128 = match width {
                    2 => i16::from_le_bytes(chunk[0..2].try_into().unwrap()) as i128,
                    4 => i32::from_le_bytes(chunk[0..4].try_into().unwrap()) as i128,
                    8 => i64::from_le_bytes(chunk[0..8].try_into().unwrap()) as i128,
                    16 => i128::from_le_bytes(chunk[0..16].try_into().unwrap()),
                    _ => {
                        return Err(crate::WireError::UnsupportedLogicalType(
                            LogicalTypeId::Decimal,
                        ))
                    }
                };
                values.push(v);
            }
            let arr = Decimal128Array::try_new(ScalarBuffer::from(values), nulls)
                .map_err(arrow_err)?
                .with_precision_and_scale(precision, scale as i8)
                .map_err(|e| {
                    crate::WireError::UnsupportedArrowType(format!("decimal precision/scale: {e}"))
                })?;
            Ok(Arc::new(arr))
        }
        LogicalTypeId::Enum => {
            let values = match &logical_type.extra {
                Some(crate::data_chunk::ExtraTypeInfo::Enum { values }) => values,
                _ => {
                    return Err(crate::WireError::UnsupportedLogicalType(
                        LogicalTypeId::Enum,
                    ))
                }
            };
            let value_arr: Arc<dyn Array> = Arc::new(StringArray::from_iter_values(
                values.iter().map(|s| s.as_str()),
            ));
            let width = crate::data_chunk::enum_physical_width(values.len());
            // `DictionaryArray::try_new` validates every key against the
            // dictionary; a wire index past `values.len()` is an error here
            // rather than an assert inside arrow.
            match width {
                1 => {
                    let keys = arrow_array::UInt8Array::try_new(
                        scalar_buffer_le::<u8>(bytes, row_count)?,
                        nulls,
                    )
                    .map_err(arrow_err)?;
                    let dict =
                        arrow_array::DictionaryArray::<t::UInt8Type>::try_new(keys, value_arr)
                            .map_err(arrow_err)?;
                    Ok(Arc::new(dict))
                }
                2 => {
                    let keys = arrow_array::UInt16Array::try_new(
                        scalar_buffer_le::<u16>(bytes, row_count)?,
                        nulls,
                    )
                    .map_err(arrow_err)?;
                    let dict =
                        arrow_array::DictionaryArray::<t::UInt16Type>::try_new(keys, value_arr)
                            .map_err(arrow_err)?;
                    Ok(Arc::new(dict))
                }
                4 => {
                    let keys = arrow_array::UInt32Array::try_new(
                        scalar_buffer_le::<u32>(bytes, row_count)?,
                        nulls,
                    )
                    .map_err(arrow_err)?;
                    let dict =
                        arrow_array::DictionaryArray::<t::UInt32Type>::try_new(keys, value_arr)
                            .map_err(arrow_err)?;
                    Ok(Arc::new(dict))
                }
                _ => Err(crate::WireError::UnsupportedLogicalType(
                    LogicalTypeId::Enum,
                )),
            }
        }
        other => Err(crate::WireError::UnsupportedLogicalType(other)),
    }
}

/// Slice a fixed-width element out of a tightly-packed buffer with bounds
/// checks. `index` is the row, `width` the per-element byte width. Returns a
/// [`crate::WireError::CountExceedsRemaining`] instead of panicking when a
/// wire-supplied `row_count` runs past the supplied buffer (remote DoS guard).
fn checked_fixed_slice(bytes: &[u8], index: usize, width: usize) -> crate::Result<&[u8]> {
    let start = index.checked_mul(width).ok_or_else(|| {
        crate::WireError::UnsupportedArrowType("fixed buffer offset overflow".to_string())
    })?;
    let end = start.checked_add(width).ok_or_else(|| {
        crate::WireError::UnsupportedArrowType("fixed buffer offset overflow".to_string())
    })?;
    bytes
        .get(start..end)
        .ok_or(crate::WireError::CountExceedsRemaining {
            count: end as u64,
            remaining: bytes.len() as u64,
        })
}

/// Turn a decoded validity mask into an Arrow null buffer of exactly
/// `row_count` bits. A mask shorter than `row_count` is a header/payload
/// mismatch: arrow's array constructors reject (or assert on) a null buffer
/// whose length differs from the values, so fail here with a codec error.
fn validity_to_null_buffer(
    validity: Option<&[bool]>,
    row_count: usize,
) -> crate::Result<Option<arrow_buffer::NullBuffer>> {
    let Some(bits) = validity else {
        return Ok(None);
    };
    ensure_rows_backed(bits.len(), row_count)?;
    let mut builder = arrow_buffer::BooleanBufferBuilder::new(row_count);
    for &b in bits.iter().take(row_count) {
        builder.append(b);
    }
    Ok(Some(arrow_buffer::NullBuffer::new(builder.finish())))
}

trait FromLeBytesScalar: Sized + arrow_array::ArrowNativeTypeOp {
    fn from_le(bytes: &[u8]) -> Self;
}

macro_rules! impl_from_le {
    ($t:ty, $n:expr) => {
        impl FromLeBytesScalar for $t {
            fn from_le(bytes: &[u8]) -> Self {
                <$t>::from_le_bytes(bytes.try_into().unwrap())
            }
        }
    };
}
impl_from_le!(i8, 1);
impl_from_le!(i16, 2);
impl_from_le!(i32, 4);
impl_from_le!(i64, 8);
impl_from_le!(u8, 1);
impl_from_le!(u16, 2);
impl_from_le!(u32, 4);
impl_from_le!(u64, 8);
impl_from_le!(f32, 4);
impl_from_le!(f64, 8);

fn scalar_buffer_le<T: FromLeBytesScalar>(
    bytes: &[u8],
    row_count: usize,
) -> crate::Result<arrow_buffer::ScalarBuffer<T>> {
    let width = std::mem::size_of::<T>();
    // A wire `row_count` that exceeds the supplied byte buffer must surface a
    // clear error instead of an index-out-of-bounds panic that aborts the
    // consuming query task (remote DoS). Checking up front also bounds the
    // allocation below by the bytes actually present, not by the header.
    ensure_fixed_backed(bytes, row_count, width)?;
    let values: Vec<T> = bytes
        .chunks_exact(width)
        .take(row_count)
        .map(T::from_le)
        .collect();
    Ok(arrow_buffer::ScalarBuffer::from(values))
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
        // Dictionary(Int32, Binary) isn't a valid ENUM payload — DuckDB ENUM
        // values are strings only. Expect the bridge to fall through to the
        // unsupported branch so the server emits a clear SQE-EXEC error.
        use arrow_array::{BinaryArray, DictionaryArray, Int32Array};
        let keys = Int32Array::from(vec![0, 1, 0]);
        let values: Arc<dyn Array> =
            Arc::new(BinaryArray::from(vec![b"red".as_ref(), b"blue".as_ref()]));
        let dict: DictionaryArray<arrow_array::types::Int32Type> =
            DictionaryArray::try_new(keys, values).unwrap();
        let arr: Arc<dyn Array> = Arc::new(dict);
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("color", dt, arr)]);
        let err = record_batch_to_data_chunk(&batch).unwrap_err();
        match err {
            crate::WireError::UnsupportedArrowType(msg) => assert!(msg.contains("Dictionary")),
            other => panic!("expected UnsupportedArrowType, got {other:?}"),
        }
    }

    #[test]
    fn date32_column_maps_to_logical_date() {
        use arrow_array::Date32Array;
        let arr: Arc<dyn Array> = Arc::new(Date32Array::from(vec![100, 200, 300]));
        let batch = batch_with(vec![("d", DataType::Date32, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Date);
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                let expected: Vec<u8> = [100i32, 200, 300]
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect();
                assert_eq!(bytes, &expected);
            }
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn timestamp_microsecond_column_maps_to_logical_timestamp() {
        use arrow_array::TimestampMicrosecondArray;
        let arr: Arc<dyn Array> = Arc::new(TimestampMicrosecondArray::from(vec![
            1_700_000_000_000_000i64,
            1_700_000_001_000_000,
        ]));
        let dt = DataType::Timestamp(TimeUnit::Microsecond, None);
        let batch = batch_with(vec![("ts", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Timestamp);
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => assert_eq!(bytes.len(), 16, "two i64 timestamps"),
            other => panic!("expected Fixed VectorData, got {other:?}"),
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

    #[test]
    fn time32_second_widens_to_microseconds() {
        // 1h = 3600 s -> 3_600_000_000 us
        let arr: Arc<dyn Array> = Arc::new(Time32SecondArray::from(vec![0, 3600, 86_399]));
        let batch = batch_with(vec![("t", DataType::Time32(TimeUnit::Second), arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Time);
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                let expected: Vec<u8> = [0i64, 3_600_000_000, 86_399_000_000]
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect();
                assert_eq!(bytes, &expected);
            }
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn time32_millisecond_widens_to_microseconds() {
        let arr: Arc<dyn Array> =
            Arc::new(Time32MillisecondArray::from(vec![0, 1_000, 86_399_999]));
        let batch = batch_with(vec![("t", DataType::Time32(TimeUnit::Millisecond), arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Time);
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                let expected: Vec<u8> = [0i64, 1_000_000, 86_399_999_000]
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect();
                assert_eq!(bytes, &expected);
            }
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn time64_microsecond_passes_through() {
        let arr: Arc<dyn Array> = Arc::new(Time64MicrosecondArray::from(vec![
            0,
            12 * 3_600_000_000,
            86_399_999_999,
        ]));
        let batch = batch_with(vec![("t", DataType::Time64(TimeUnit::Microsecond), arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Time);
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => assert_eq!(bytes.len(), 24),
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn time64_nanosecond_maps_to_timens() {
        let arr: Arc<dyn Array> =
            Arc::new(Time64NanosecondArray::from(vec![Some(0), None, Some(1)]));
        let batch = batch_with(vec![("t", DataType::Time64(TimeUnit::Nanosecond), arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::TimeNs);
        assert_eq!(chunk.columns[0].validity, Some(vec![true, false, true]));
    }

    #[test]
    fn fixed_size_binary_16_maps_to_uuid() {
        let uuid_a = [0x11u8; 16];
        let uuid_b = [0x22u8; 16];
        let arr =
            FixedSizeBinaryArray::try_from_iter([uuid_a.to_vec(), uuid_b.to_vec()].into_iter())
                .unwrap();
        let arr: Arc<dyn Array> = Arc::new(arr);
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("u", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Uuid);
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                assert_eq!(bytes.len(), 32);
                assert_eq!(&bytes[..16], &uuid_a);
                assert_eq!(&bytes[16..], &uuid_b);
            }
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn fixed_size_binary_non_16_is_rejected() {
        // FixedSizeBinary(8) is a valid Arrow type but not a UUID; we surface
        // a clear UnsupportedArrowType error rather than silently truncating.
        let arr = FixedSizeBinaryArray::try_from_iter([[1u8; 8].to_vec()].into_iter()).unwrap();
        let arr: Arc<dyn Array> = Arc::new(arr);
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("b", dt, arr)]);
        let err = record_batch_to_data_chunk(&batch).unwrap_err();
        match err {
            crate::WireError::UnsupportedArrowType(msg) => {
                assert!(msg.contains("FixedSizeBinary(8)"), "msg was {msg}");
            }
            other => panic!("expected UnsupportedArrowType, got {other:?}"),
        }
    }

    #[test]
    fn interval_yearmonth_widens_to_16_byte_struct() {
        // 14 months = 1 year + 2 months
        let arr: Arc<dyn Array> = Arc::new(IntervalYearMonthArray::from(vec![14]));
        let batch = batch_with(vec![(
            "i",
            DataType::Interval(IntervalUnit::YearMonth),
            arr,
        )]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Interval);
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                assert_eq!(bytes.len(), 16);
                let mut expected = Vec::new();
                expected.extend_from_slice(&14i32.to_le_bytes());
                expected.extend_from_slice(&0i32.to_le_bytes());
                expected.extend_from_slice(&0i64.to_le_bytes());
                assert_eq!(bytes, &expected);
            }
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn interval_daytime_scales_ms_to_micros() {
        use arrow_array::types::IntervalDayTime;
        // 3 days + 5_000 ms = 5_000_000 micros
        let arr: Arc<dyn Array> = Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime {
            days: 3,
            milliseconds: 5_000,
        }]));
        let batch = batch_with(vec![("i", DataType::Interval(IntervalUnit::DayTime), arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                let mut expected = Vec::new();
                expected.extend_from_slice(&0i32.to_le_bytes());
                expected.extend_from_slice(&3i32.to_le_bytes());
                expected.extend_from_slice(&5_000_000i64.to_le_bytes());
                assert_eq!(bytes, &expected);
            }
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn decimal128_p4s2_narrows_to_i16_le() {
        // precision 4 -> 2-byte i16 storage. -123 with scale 2 == -1.23.
        let arr: Arc<dyn Array> = Arc::new(
            Decimal128Array::from(vec![123i128, -123, 0])
                .with_precision_and_scale(4, 2)
                .unwrap(),
        );
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("d", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        let lt = &chunk.columns[0].logical_type;
        assert_eq!(lt.id, LogicalTypeId::Decimal);
        assert_eq!(
            lt.extra,
            Some(ExtraTypeInfo::Decimal {
                precision: 4,
                scale: 2,
            })
        );
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                let expected: Vec<u8> = [123i16, -123, 0]
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect();
                assert_eq!(bytes, &expected);
            }
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn decimal128_p9s0_narrows_to_i32_le() {
        let arr: Arc<dyn Array> = Arc::new(
            Decimal128Array::from(vec![1i128, 2, 3])
                .with_precision_and_scale(9, 0)
                .unwrap(),
        );
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("d", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => assert_eq!(bytes.len(), 12, "three i32 values"),
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn decimal128_p18s6_narrows_to_i64_le() {
        let arr: Arc<dyn Array> = Arc::new(
            Decimal128Array::from(vec![1_000_000i128, -1_000_000])
                .with_precision_and_scale(18, 6)
                .unwrap(),
        );
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("d", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                let expected: Vec<u8> = [1_000_000i64, -1_000_000]
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect();
                assert_eq!(bytes, &expected);
            }
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn decimal128_p38_keeps_full_i128() {
        let arr: Arc<dyn Array> = Arc::new(
            Decimal128Array::from(vec![i128::MAX, i128::MIN])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        );
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("d", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => assert_eq!(bytes.len(), 32, "two i128 values"),
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn decimal128_with_negative_scale_is_rejected() {
        // DuckDB's DecimalTypeInfo.scale is uint8_t — negative scales can't
        // round-trip through the wire.
        let arr: Arc<dyn Array> = Arc::new(
            Decimal128Array::from(vec![1i128])
                .with_precision_and_scale(10, -2)
                .unwrap(),
        );
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("d", dt, arr)]);
        let err = record_batch_to_data_chunk(&batch).unwrap_err();
        match err {
            crate::WireError::UnsupportedArrowType(msg) => {
                assert!(msg.contains("negative scale"), "msg was {msg}");
            }
            other => panic!("expected UnsupportedArrowType, got {other:?}"),
        }
    }

    #[test]
    fn decimal128_round_trips_through_full_data_chunk_codec() {
        let arr: Arc<dyn Array> = Arc::new(
            Decimal128Array::from(vec![Some(150i128), None, Some(-150)])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("amount", dt, arr)]);
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

    #[test]
    fn interval_monthdaynano_floors_ns_to_micros() {
        use arrow_array::types::IntervalMonthDayNano;
        // 1 month + 2 days + 1_500 ns -> 1 us (floor)
        let arr: Arc<dyn Array> = Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano {
                months: 1,
                days: 2,
                nanoseconds: 1_500,
            },
        ]));
        let batch = batch_with(vec![(
            "i",
            DataType::Interval(IntervalUnit::MonthDayNano),
            arr,
        )]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                let mut expected = Vec::new();
                expected.extend_from_slice(&1i32.to_le_bytes());
                expected.extend_from_slice(&2i32.to_le_bytes());
                expected.extend_from_slice(&1i64.to_le_bytes());
                assert_eq!(bytes, &expected);
            }
            other => panic!("expected Fixed VectorData, got {other:?}"),
        }
    }

    #[test]
    fn list_array_round_trips_through_data_chunk() {
        use arrow_array::builder::{Int32Builder, ListBuilder};
        let mut b = ListBuilder::new(Int32Builder::new());
        b.values().append_value(10);
        b.values().append_value(20);
        b.append(true);
        b.append(true); // empty list
        b.values().append_value(30);
        b.values().append_value(40);
        b.values().append_value(50);
        b.append(true);
        let list_arr = b.finish();
        let arr: Arc<dyn Array> = Arc::new(list_arr);
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("xs", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::List);
        match &chunk.columns[0].data {
            VectorData::List {
                entries,
                child_count,
                child,
            } => {
                assert_eq!(entries, &vec![(0, 2), (2, 0), (2, 3)]);
                assert_eq!(*child_count, 5);
                assert_eq!(child.logical_type.id, LogicalTypeId::Integer);
            }
            other => panic!("expected VectorData::List, got {other:?}"),
        }

        // Round-trip through the codec.
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

    #[test]
    fn struct_array_round_trips_through_data_chunk() {
        use arrow_array::builder::{Int32Builder, StringBuilder, StructBuilder};
        use arrow_schema::Field;
        let fields = vec![
            Arc::new(Field::new("id", DataType::Int32, true)),
            Arc::new(Field::new("name", DataType::Utf8, true)),
        ];
        let mut builder = StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(Int32Builder::new()),
                Box::new(StringBuilder::new()),
            ],
        );
        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(1);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("alice");
        builder.append(true);
        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(2);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("bob");
        builder.append(true);
        let struct_arr = builder.finish();
        let arr: Arc<dyn Array> = Arc::new(struct_arr);
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("s", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Struct);
        match &chunk.columns[0].data {
            VectorData::Struct { children } => {
                assert_eq!(children.len(), 2);
                assert_eq!(children[0].logical_type.id, LogicalTypeId::Integer);
                assert_eq!(children[1].logical_type.id, LogicalTypeId::Varchar);
            }
            other => panic!("expected VectorData::Struct, got {other:?}"),
        }

        // Round-trip through the codec.
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

    #[test]
    fn fixed_size_list_round_trips_as_array() {
        use arrow_array::builder::{FixedSizeListBuilder, Int32Builder};
        let mut b = FixedSizeListBuilder::new(Int32Builder::new(), 3);
        for v in [10, 20, 30, 40, 50, 60] {
            b.values().append_value(v);
        }
        b.append(true);
        b.append(true);
        let fixed_arr = b.finish();
        let arr: Arc<dyn Array> = Arc::new(fixed_arr);
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("a", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Array);
        match &chunk.columns[0].logical_type.extra {
            Some(ExtraTypeInfo::Array { size, .. }) => assert_eq!(*size, 3),
            other => panic!("expected ExtraTypeInfo::Array, got {other:?}"),
        }
        match &chunk.columns[0].data {
            VectorData::Array { array_size, child } => {
                assert_eq!(*array_size, 3);
                assert_eq!(child.logical_type.id, LogicalTypeId::Integer);
            }
            other => panic!("expected VectorData::Array, got {other:?}"),
        }

        // Round-trip through the codec.
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

    #[test]
    fn dictionary_int8_utf8_maps_to_enum() {
        use arrow_array::{Int8Array, StringArray};
        // 3 unique values, 5 rows.
        let keys = Int8Array::from(vec![0i8, 1, 2, 1, 0]);
        let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["red", "green", "blue"]));
        let dict: DictionaryArray<arrow_types::Int8Type> =
            DictionaryArray::try_new(keys, values).unwrap();
        let arr: Arc<dyn Array> = Arc::new(dict);
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("color", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Enum);
        match &chunk.columns[0].logical_type.extra {
            Some(ExtraTypeInfo::Enum { values }) => {
                assert_eq!(values, &vec!["red", "green", "blue"]);
            }
            other => panic!("expected ExtraTypeInfo::Enum, got {other:?}"),
        }
        // 3 dict entries -> u8 tier -> 5 bytes for 5 rows.
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => assert_eq!(bytes, &vec![0u8, 1, 2, 1, 0]),
            other => panic!("expected Fixed, got {other:?}"),
        }

        // Full codec round-trip.
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

    #[test]
    fn dictionary_int32_utf8_widens_to_u16_tier() {
        use arrow_array::{Int32Array, StringArray};
        // 300 dictionary entries forces the u16 tier on the wire.
        let value_strs: Vec<String> = (0..300).map(|i| format!("v{i}")).collect();
        let values: Arc<dyn Array> = Arc::new(StringArray::from(
            value_strs.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        ));
        let keys = Int32Array::from(vec![0i32, 299, 100, 50]);
        let dict: DictionaryArray<arrow_types::Int32Type> =
            DictionaryArray::try_new(keys, values).unwrap();
        let arr: Arc<dyn Array> = Arc::new(dict);
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("x", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        match &chunk.columns[0].data {
            VectorData::Fixed(bytes) => {
                let expected: Vec<u8> = [0u16, 299, 100, 50]
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect();
                assert_eq!(bytes, &expected);
            }
            other => panic!("expected Fixed, got {other:?}"),
        }
    }

    #[test]
    fn map_array_round_trips_via_list_layout() {
        use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        // Row 0: {"a" -> 1, "b" -> 2}
        builder.keys().append_value("a");
        builder.values().append_value(1);
        builder.keys().append_value("b");
        builder.values().append_value(2);
        builder.append(true).unwrap();
        // Row 1: {} (empty map)
        builder.append(true).unwrap();
        let map_arr = builder.finish();
        let arr: Arc<dyn Array> = Arc::new(map_arr);
        let dt = arr.data_type().clone();
        let batch = batch_with(vec![("m", dt, arr)]);
        let chunk = record_batch_to_data_chunk(&batch).unwrap();
        assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Map);
        match &chunk.columns[0].logical_type.extra {
            Some(ExtraTypeInfo::List { child }) => {
                assert_eq!(child.id, LogicalTypeId::Struct);
            }
            other => panic!("expected ExtraTypeInfo::List wrapping STRUCT, got {other:?}"),
        }
        match &chunk.columns[0].data {
            VectorData::List {
                entries,
                child_count,
                ..
            } => {
                assert_eq!(entries, &vec![(0, 2), (2, 0)]);
                assert_eq!(*child_count, 2);
            }
            other => panic!("expected VectorData::List, got {other:?}"),
        }

        // Round-trip through the codec.
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

    // ── Arrow-bridge OOB guards (QUACK-03/04) ───────────────────────────

    #[test]
    fn fixed_to_array_rejects_short_buffer_without_panicking() {
        // QUACK-03: a Fixed INTEGER vector claiming 1000 rows but carrying only
        // 4 bytes must surface an error from the checked slicing, not panic.
        let vector = Vector {
            logical_type: LogicalType::new(LogicalTypeId::Integer),
            validity: None,
            data: VectorData::Fixed(vec![1, 0, 0, 0]), // one i32
        };
        let err = vector_to_array(&vector, 1000).unwrap_err();
        assert!(matches!(
            err,
            crate::WireError::CountExceedsRemaining { .. }
        ));
    }

    #[test]
    fn checked_fixed_slice_rejects_out_of_range_index() {
        // QUACK-04: the shared UUID/Decimal/Interval slicer must return an error
        // when row*width runs past the buffer instead of indexing OOB.
        let bytes = [0u8; 16]; // room for exactly one 16-byte element
        assert!(checked_fixed_slice(&bytes, 0, 16).is_ok());
        let err = checked_fixed_slice(&bytes, 1, 16).unwrap_err();
        assert!(matches!(
            err,
            crate::WireError::CountExceedsRemaining { .. }
        ));
    }

    #[test]
    fn uuid_vector_with_short_buffer_errors_not_panics() {
        // QUACK-04: a UUID column claiming 100 rows with a 16-byte buffer.
        let vector = Vector {
            logical_type: LogicalType::new(LogicalTypeId::Uuid),
            validity: None,
            data: VectorData::Fixed(vec![0u8; 16]),
        };
        let err = vector_to_array(&vector, 100).unwrap_err();
        assert!(matches!(
            err,
            crate::WireError::CountExceedsRemaining { .. }
        ));
    }

    /// 606: `result_types` is derived from the stream schema so a zero-row
    /// result still names its columns. The schema-derived type must be
    /// byte-identical to what the value path would have stamped, or a client
    /// that decodes with the declared type reads the wrong bytes.
    #[test]
    fn schema_derived_types_match_value_derived_types() {
        let cases: Vec<(DataType, Arc<dyn Array>)> = vec![
            (
                DataType::Int32,
                Arc::new(Int32Array::from(vec![1])) as Arc<dyn Array>,
            ),
            (DataType::Int64, Arc::new(Int64Array::from(vec![1i64]))),
            (DataType::Float64, Arc::new(Float64Array::from(vec![1.0]))),
            (DataType::Utf8, Arc::new(StringArray::from(vec!["a"]))),
            (
                DataType::Date64,
                Arc::new(Date64Array::from(vec![86_400_000i64])),
            ),
            (
                DataType::Time32(TimeUnit::Millisecond),
                Arc::new(Time32MillisecondArray::from(vec![1_000i32])),
            ),
            (
                DataType::Decimal128(12, 3),
                Arc::new(
                    Decimal128Array::from(vec![1234i128])
                        .with_precision_and_scale(12, 3)
                        .unwrap(),
                ),
            ),
        ];
        for (dt, array) in cases {
            let batch = batch_with(vec![("c", dt.clone(), array)]);
            let chunk = record_batch_to_data_chunk(&batch).unwrap();
            let from_schema = arrow_schema_to_logical(batch.schema().as_ref()).unwrap();
            assert_eq!(
                from_schema[0], chunk.columns[0].logical_type,
                "schema-derived type disagrees with value-derived type for {dt:?}"
            );
        }
    }

    /// The schema path must cover an *empty* result too: that is the whole
    /// reason it exists (there is no first batch to read types off).
    #[test]
    fn empty_schema_still_yields_column_types() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let types = arrow_schema_to_logical(&schema).unwrap();
        assert_eq!(types.len(), 2);
        assert_eq!(types[0].id, LogicalTypeId::Integer);
        assert_eq!(types[1].id, LogicalTypeId::Varchar);
    }

    /// ENUM values are data, not type, so the type-only path has to refuse
    /// rather than invent an empty dictionary (whose physical key width would
    /// differ from the one the value path picks).
    #[test]
    fn dictionary_is_rejected_by_the_type_only_path() {
        let dt = DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8));
        assert!(matches!(
            arrow_type_to_logical(&dt),
            Err(crate::WireError::UnsupportedArrowType(_))
        ));
    }
}
