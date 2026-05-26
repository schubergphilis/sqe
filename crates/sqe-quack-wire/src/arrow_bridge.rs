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
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    IntervalYearMonthArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray,
    MapArray, RecordBatch, StringArray, StringViewArray, StructArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};

use crate::data_chunk::{
    decimal_physical_width, DataChunk, ExtraTypeInfo, LogicalType, LogicalTypeId, Vector,
    VectorData,
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
        DataType::Time32(TimeUnit::Second) => {
            time32_second_to_time(array, row_count)?
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            time32_millisecond_to_time(array, row_count)?
        }
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
        // Arrow interval types all widen into DuckDB's 16-byte `interval_t`
        // { months: i32, days: i32, micros: i64 }.
        DataType::Interval(IntervalUnit::YearMonth) => interval_yearmonth_to_interval(array, row_count)?,
        DataType::Interval(IntervalUnit::DayTime) => interval_daytime_to_interval(array, row_count)?,
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
            crate::WireError::UnsupportedArrowType(
                "interval-yearmonth downcast failed".to_string(),
            )
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
    let typed = array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| crate::WireError::UnsupportedArrowType("list downcast failed".to_string()))?;
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
    let typed = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
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
            _ => unreachable!("decimal_physical_width returned an unexpected width"),
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
        // Dictionary(Int32, Utf8) isn't mapped yet — DuckDB ENUM needs custom
        // EnumTypeInfo handling. Expect the dispatch table to surface
        // UnsupportedArrowType so the server emits a clear SQE-EXEC error.
        use arrow_array::{DictionaryArray, Int32Array, StringArray};
        let keys = Int32Array::from(vec![0, 1, 0]);
        let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["red", "blue"]));
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
        let arr: Arc<dyn Array> = Arc::new(Time64NanosecondArray::from(vec![Some(0), None, Some(1)]));
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
        let batch = batch_with(vec![("i", DataType::Interval(IntervalUnit::YearMonth), arr)]);
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
            vec![Box::new(Int32Builder::new()), Box::new(StringBuilder::new())],
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
            VectorData::List { entries, child_count, .. } => {
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
}
