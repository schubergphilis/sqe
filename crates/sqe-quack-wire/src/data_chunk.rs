//! `DataChunk`, `Vector`, and `LogicalType` codec for the Quack RPC.
//!
//! Mirrors:
//! - `src/common/types.cpp::LogicalType::Serialize` (field 100 id, optional 101 type_info)
//! - `src/common/types/vector.cpp::Vector::Serialize` (FLAT path only; compressed
//!   dictionary/constant/sequence forms are decoded but never emitted)
//! - `src/common/types/data_chunk.cpp::DataChunk::Serialize` (rows, types list,
//!   columns list)
//!
//! Pinned to `SerializationCompatibility::FromIndex(7)` (DuckDB v1.5.x).

use crate::codec::{BinaryDeserializer, BinarySerializer};

/// Subset of DuckDB's `LogicalTypeId` (uint8_t). Covers all common scalar
/// types plus the nested-type markers; nested type _info_ (LIST<T>, STRUCT<...>)
/// is not yet implemented in `LogicalType::type_info`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalTypeId {
    Invalid = 0,
    SqlNull = 1,
    Boolean = 10,
    TinyInt = 11,
    SmallInt = 12,
    Integer = 13,
    BigInt = 14,
    Date = 15,
    Time = 16,
    TimestampSec = 17,
    TimestampMs = 18,
    Timestamp = 19,
    TimestampNs = 20,
    Decimal = 21,
    Float = 22,
    Double = 23,
    Char = 24,
    Varchar = 25,
    Blob = 26,
    Interval = 27,
    UTinyInt = 28,
    USmallInt = 29,
    UInteger = 30,
    UBigInt = 31,
    TimestampTz = 32,
    TimestampTzNs = 33,
    TimeTz = 34,
    TimeNs = 35,
    Bit = 36,
    UHugeInt = 49,
    HugeInt = 50,
    Uuid = 54,
}

impl LogicalTypeId {
    pub fn from_u8(value: u8) -> crate::Result<Self> {
        Ok(match value {
            0 => Self::Invalid,
            1 => Self::SqlNull,
            10 => Self::Boolean,
            11 => Self::TinyInt,
            12 => Self::SmallInt,
            13 => Self::Integer,
            14 => Self::BigInt,
            15 => Self::Date,
            16 => Self::Time,
            17 => Self::TimestampSec,
            18 => Self::TimestampMs,
            19 => Self::Timestamp,
            20 => Self::TimestampNs,
            21 => Self::Decimal,
            22 => Self::Float,
            23 => Self::Double,
            24 => Self::Char,
            25 => Self::Varchar,
            26 => Self::Blob,
            27 => Self::Interval,
            28 => Self::UTinyInt,
            29 => Self::USmallInt,
            30 => Self::UInteger,
            31 => Self::UBigInt,
            32 => Self::TimestampTz,
            33 => Self::TimestampTzNs,
            34 => Self::TimeTz,
            35 => Self::TimeNs,
            36 => Self::Bit,
            49 => Self::UHugeInt,
            50 => Self::HugeInt,
            54 => Self::Uuid,
            _ => return Err(crate::WireError::UnknownLogicalTypeId(value)),
        })
    }

    /// Constant byte width for fixed-size types. None for variable-length
    /// types (VARCHAR, BLOB) and unsupported types.
    ///
    /// Width matches DuckDB's internal C++ representation: `Time`/`TimeNs`/`TimeTz`
    /// are i64 microseconds (or ns / packed micros+offset), and `Interval` is the
    /// 16-byte `interval_t { months: i32, days: i32, micros: i64 }`.
    pub fn fixed_width(self) -> Option<usize> {
        use LogicalTypeId::*;
        Some(match self {
            Boolean | TinyInt | UTinyInt => 1,
            SmallInt | USmallInt => 2,
            Integer | UInteger | Float | Date => 4,
            BigInt | UBigInt | Double | Time | TimeNs | TimeTz | Timestamp | TimestampSec
            | TimestampMs | TimestampNs | TimestampTz | TimestampTzNs => 8,
            HugeInt | UHugeInt | Uuid | Interval => 16,
            _ => return None,
        })
    }
}

/// Subset of DuckDB's `ExtraTypeInfoType` (uint8_t). Acts as the discriminant
/// for the `ExtraTypeInfo` variant on the wire (field 100 inside the type_info
/// object).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtraTypeInfoType {
    Invalid = 0,
    Generic = 1,
    Decimal = 2,
    String = 3,
    List = 4,
    Struct = 5,
    Enum = 6,
    Unbound = 7,
    AggregateState = 8,
    Array = 9,
    Any = 10,
    IntegerLiteral = 11,
    Template = 12,
    Geo = 13,
}

impl ExtraTypeInfoType {
    pub fn from_u8(value: u8) -> crate::Result<Self> {
        Ok(match value {
            0 => Self::Invalid,
            1 => Self::Generic,
            2 => Self::Decimal,
            3 => Self::String,
            4 => Self::List,
            5 => Self::Struct,
            6 => Self::Enum,
            7 => Self::Unbound,
            8 => Self::AggregateState,
            9 => Self::Array,
            10 => Self::Any,
            11 => Self::IntegerLiteral,
            12 => Self::Template,
            13 => Self::Geo,
            _ => return Err(crate::WireError::UnknownExtraTypeInfoType(value)),
        })
    }
}

/// Parameterised type info attached to a `LogicalType` via field 101.
/// Only the variants we encode/decode end-to-end are listed; everything else
/// surfaces as `WireError::UnsupportedExtraTypeInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtraTypeInfo {
    Decimal { precision: u8, scale: u8 },
}

impl ExtraTypeInfo {
    pub fn discriminant(&self) -> ExtraTypeInfoType {
        match self {
            ExtraTypeInfo::Decimal { .. } => ExtraTypeInfoType::Decimal,
        }
    }

    /// Encode an `ExtraTypeInfo` *inside* a begin_object/end_object pair the
    /// caller already opened. Mirrors `ExtraTypeInfo::Serialize` (base then
    /// subclass). Alias and extension_info default-omit because we never set
    /// them.
    pub fn encode_inner(&self, s: &mut BinarySerializer) {
        // Base field 100: type discriminant — WriteProperty<ExtraTypeInfoType>.
        s.begin_property(100);
        s.write_u8(self.discriminant() as u8);
        s.end_property();
        // Base field 101 (alias, default ""): omitted.
        // Base field 102 (deleted modifiers): never emitted by current DuckDB.
        // Base field 103 (extension_info, default null): omitted.
        match self {
            ExtraTypeInfo::Decimal { precision, scale } => {
                // Field 200 (width) is WritePropertyWithDefault<uint8_t> with
                // default 0. Real DECIMALs always have width >= 1, but we
                // honour the default-elision rule for byte-level compat.
                if *precision != 0 {
                    s.begin_property(200);
                    s.write_u8(*precision);
                    s.end_property();
                }
                if *scale != 0 {
                    s.begin_property(201);
                    s.write_u8(*scale);
                    s.end_property();
                }
            }
        }
    }

    /// Decode an `ExtraTypeInfo` after the caller has confirmed an object is
    /// open. Reads the base discriminant first, then dispatches to the
    /// subclass; consumes the trailing 0xFFFF object terminator.
    pub fn decode_inner(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(100)?;
        let kind = ExtraTypeInfoType::from_u8(d.read_u8()?)?;
        // Base field 101 (alias, default ""): consume if present.
        if d.read_optional(101)? {
            let _alias = d.read_string()?;
        }
        // Field 102 was deleted in v1.5; refuse to silently skip if a legacy
        // writer ever emits it because we can't determine the value's length.
        if d.read_optional(102)? {
            return Err(crate::WireError::UnexpectedField {
                expected: 200,
                actual: 102,
            });
        }
        // Field 103 (extension_info) — not implemented.
        if d.read_optional(103)? {
            return Err(crate::WireError::UnsupportedExtensionTypeInfo);
        }
        match kind {
            ExtraTypeInfoType::Decimal => {
                let precision = if d.read_optional(200)? { d.read_u8()? } else { 0 };
                let scale = if d.read_optional(201)? { d.read_u8()? } else { 0 };
                Ok(ExtraTypeInfo::Decimal { precision, scale })
            }
            other => Err(crate::WireError::UnsupportedExtraTypeInfo(other)),
        }
    }
}

/// Physical storage width for a DECIMAL with the given precision.
/// Matches `DecimalType::GetInternalType` in DuckDB: precision 1-4 -> i16,
/// 5-9 -> i32, 10-18 -> i64, 19-38 -> i128.
pub fn decimal_physical_width(precision: u8) -> usize {
    match precision {
        0..=4 => 2,
        5..=9 => 4,
        10..=18 => 8,
        _ => 16,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalType {
    pub id: LogicalTypeId,
    /// `None` for primitive types whose `ExtraTypeInfo` field is the default
    /// (null `shared_ptr`). `Some(...)` carries the parameters required to
    /// reconstruct the type — today only `DECIMAL(precision, scale)`.
    pub extra: Option<ExtraTypeInfo>,
}

impl LogicalType {
    pub fn new(id: LogicalTypeId) -> Self {
        Self { id, extra: None }
    }

    pub fn with_extra(id: LogicalTypeId, extra: ExtraTypeInfo) -> Self {
        Self {
            id,
            extra: Some(extra),
        }
    }

    /// Per-row byte width for fixed-size types. None for variable-length
    /// (`VARCHAR`/`BLOB`). For `DECIMAL` the width depends on the precision
    /// stored in `extra`; for every other id we fall back to
    /// `LogicalTypeId::fixed_width`.
    pub fn physical_width(&self) -> Option<usize> {
        match (self.id, &self.extra) {
            (LogicalTypeId::Decimal, Some(ExtraTypeInfo::Decimal { precision, .. })) => {
                Some(decimal_physical_width(*precision))
            }
            (id, _) => id.fixed_width(),
        }
    }

    pub fn encode(&self, s: &mut BinarySerializer) {
        s.begin_property(100);
        s.write_u8(self.id as u8);
        s.end_property();
        // Field 101 ("type_info") is WritePropertyWithDefault<shared_ptr<ExtraTypeInfo>>.
        // When non-null the on-wire shape is: field_id, nullable present byte
        // (1), object content, 0xFFFF terminator.
        if let Some(extra) = &self.extra {
            s.begin_optional_property(101, true);
            s.begin_nullable(true);
            s.begin_object();
            extra.encode_inner(s);
            s.end_object();
            s.end_nullable(true);
            s.end_optional_property(true);
        }
    }

    pub fn decode(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(100)?;
        let id = LogicalTypeId::from_u8(d.read_u8()?)?;
        let extra = if d.read_optional(101)? {
            let present = d.read_nullable_present()?;
            if !present {
                return Err(crate::WireError::NullExtraTypeInfo);
            }
            let extra = ExtraTypeInfo::decode_inner(d)?;
            d.expect_object_end()?;
            Some(extra)
        } else {
            None
        };
        Ok(LogicalType { id, extra })
    }
}

// -----------------------------------------------------------------------------
// Vector (FLAT only)
// -----------------------------------------------------------------------------

/// Storage for one column's worth of data. Mirrors DuckDB's flat-vector
/// representation: a tightly packed buffer for fixed-width types, or a per-row
/// list for variable-width types (VARCHAR / BLOB).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorData {
    /// Raw little-endian bytes, `count * fixed_width(type)` long.
    Fixed(Vec<u8>),
    /// One entry per row. `None` means null at that position. The validity
    /// mask is derived from the `None` positions during encode.
    Strings(Vec<Option<String>>),
    /// Same shape as `Strings`, used for BLOB.
    Blobs(Vec<Option<Vec<u8>>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Vector {
    pub logical_type: LogicalType,
    /// Per-row validity. `None` means "all rows valid" (no validity mask
    /// written on the wire). `Some(v)` means rows with `v[i] == false` are
    /// null.
    pub validity: Option<Vec<bool>>,
    pub data: VectorData,
}

impl Vector {
    pub fn new_fixed(id: LogicalTypeId, data: Vec<u8>) -> Self {
        Self {
            logical_type: LogicalType::new(id),
            validity: None,
            data: VectorData::Fixed(data),
        }
    }

    pub fn new_strings(values: Vec<Option<String>>) -> Self {
        let validity = if values.iter().any(|v| v.is_none()) {
            Some(values.iter().map(|v| v.is_some()).collect())
        } else {
            None
        };
        Self {
            logical_type: LogicalType::new(LogicalTypeId::Varchar),
            validity,
            data: VectorData::Strings(values),
        }
    }

    pub fn encode(&self, count: usize, s: &mut BinarySerializer) {
        let has_validity = self.validity.is_some();
        s.begin_property(100);
        s.write_bool(has_validity);
        s.end_property();

        if let Some(validity) = &self.validity {
            // Bit-packed: bit `i` of byte `i / 8` is 1 if row `i` is valid.
            // DuckDB stores in `u64` chunks (`ceil(count / 64) * 8` bytes).
            let n_u64 = count.div_ceil(64);
            let mut mask_bytes = vec![0u8; n_u64 * 8];
            for (i, &valid) in validity.iter().enumerate().take(count) {
                if valid {
                    mask_bytes[i / 8] |= 1 << (i % 8);
                }
            }
            s.begin_property(101);
            s.write_data_ptr(&mask_bytes);
            s.end_property();
        }

        s.begin_property(102);
        match &self.data {
            VectorData::Fixed(bytes) => s.write_data_ptr(bytes),
            VectorData::Strings(strs) => {
                s.begin_list(count as u64);
                for entry in strs.iter().take(count) {
                    let value = entry.as_deref().unwrap_or("");
                    s.write_string(value);
                }
                s.end_list();
            }
            VectorData::Blobs(blobs) => {
                s.begin_list(count as u64);
                for entry in blobs.iter().take(count) {
                    let empty = Vec::new();
                    let value = entry.as_ref().unwrap_or(&empty);
                    s.write_data_ptr(value);
                }
                s.end_list();
            }
        }
        s.end_property();
    }

    pub fn decode(
        logical_type: LogicalType,
        count: usize,
        d: &mut BinaryDeserializer<'_>,
    ) -> crate::Result<Self> {
        // Field 90 ("vector_type") is only present for compressed formats.
        // Standard DuckDB FLAT vectors skip it.
        if d.read_optional(90)? {
            let raw = d.read_u8()?;
            return Err(crate::WireError::UnsupportedVectorType(raw));
        }

        d.expect_field(100)?;
        let has_validity = d.read_bool()?;

        let validity = if has_validity {
            d.expect_field(101)?;
            let raw = d.read_data_ptr()?;
            let mut bits = Vec::with_capacity(count);
            for i in 0..count {
                let byte = raw.get(i / 8).copied().unwrap_or(0);
                bits.push(byte & (1 << (i % 8)) != 0);
            }
            Some(bits)
        } else {
            None
        };

        d.expect_field(102)?;
        let data = if logical_type.physical_width().is_some() {
            VectorData::Fixed(d.read_data_ptr()?)
        } else {
            match logical_type.id {
                LogicalTypeId::Varchar => {
                    let actual = d.read_list_count()? as usize;
                    let take = actual.min(count);
                    let mut values = Vec::with_capacity(take);
                    let validity_ref = validity.as_deref();
                    for i in 0..actual {
                        let s_value = d.read_string()?;
                        let valid = validity_ref
                            .map(|v| v.get(i).copied().unwrap_or(true))
                            .unwrap_or(true);
                        values.push(if valid { Some(s_value) } else { None });
                    }
                    VectorData::Strings(values)
                }
                LogicalTypeId::Blob => {
                    let actual = d.read_list_count()? as usize;
                    let mut values = Vec::with_capacity(actual);
                    let validity_ref = validity.as_deref();
                    for i in 0..actual {
                        let bytes = d.read_data_ptr()?;
                        let valid = validity_ref
                            .map(|v| v.get(i).copied().unwrap_or(true))
                            .unwrap_or(true);
                        values.push(if valid { Some(bytes) } else { None });
                    }
                    VectorData::Blobs(values)
                }
                other => return Err(crate::WireError::UnsupportedLogicalType(other)),
            }
        };

        Ok(Vector {
            logical_type,
            validity,
            data,
        })
    }
}

// -----------------------------------------------------------------------------
// DataChunk
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataChunk {
    pub row_count: u32,
    pub columns: Vec<Vector>,
}

impl DataChunk {
    pub fn encode(&self, s: &mut BinarySerializer) {
        // rows: u32 raw (DuckDB's sel_t — varint per WriteProperty).
        s.begin_property(100);
        s.write_u32(self.row_count);
        s.end_property();

        // types list: one LogicalType object per column.
        s.begin_property(101);
        s.begin_list(self.columns.len() as u64);
        for column in &self.columns {
            s.begin_object();
            column.logical_type.encode(s);
            s.end_object();
        }
        s.end_list();
        s.end_property();

        // columns list: one Vector object per column.
        s.begin_property(102);
        s.begin_list(self.columns.len() as u64);
        for column in &self.columns {
            s.begin_object();
            column.encode(self.row_count as usize, s);
            s.end_object();
        }
        s.end_list();
        s.end_property();
    }

    pub fn decode(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(100)?;
        let row_count = d.read_u32()?;

        d.expect_field(101)?;
        let type_count = d.read_list_count()? as usize;
        let mut types = Vec::with_capacity(type_count);
        for _ in 0..type_count {
            let t = LogicalType::decode(d)?;
            d.expect_object_end()?;
            types.push(t);
        }

        d.expect_field(102)?;
        let column_count = d.read_list_count()? as usize;
        if column_count != type_count {
            return Err(crate::WireError::UnexpectedField {
                expected: type_count as u16,
                actual: column_count as u16,
            });
        }
        let mut columns = Vec::with_capacity(column_count);
        for t in types {
            let column = Vector::decode(t, row_count as usize, d)?;
            d.expect_object_end()?;
            columns.push(column);
        }

        Ok(DataChunk { row_count, columns })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_type_id_roundtrips_all_known_variants() {
        for variant in [
            LogicalTypeId::Boolean,
            LogicalTypeId::TinyInt,
            LogicalTypeId::SmallInt,
            LogicalTypeId::Integer,
            LogicalTypeId::BigInt,
            LogicalTypeId::HugeInt,
            LogicalTypeId::Float,
            LogicalTypeId::Double,
            LogicalTypeId::Varchar,
            LogicalTypeId::Blob,
            LogicalTypeId::Date,
            LogicalTypeId::Timestamp,
        ] {
            assert_eq!(LogicalTypeId::from_u8(variant as u8).unwrap(), variant);
        }
    }

    #[test]
    fn logical_type_id_rejects_unknown_value() {
        let err = LogicalTypeId::from_u8(200).unwrap_err();
        assert!(matches!(err, crate::WireError::UnknownLogicalTypeId(200)));
    }

    #[test]
    fn logical_type_primitive_encodes_only_id_field() {
        let lt = LogicalType::new(LogicalTypeId::Integer);
        let mut s = BinarySerializer::new();
        s.begin_object();
        lt.encode(&mut s);
        s.end_object();
        // field 100 = u16 LE [0x64, 0x00], varint 13 = [0x0D], terminator [0xFF, 0xFF]
        assert_eq!(s.into_bytes(), &[0x64, 0x00, 0x0D, 0xFF, 0xFF]);
    }

    #[test]
    fn logical_type_roundtrips() {
        for id in [
            LogicalTypeId::Boolean,
            LogicalTypeId::Integer,
            LogicalTypeId::BigInt,
            LogicalTypeId::Double,
            LogicalTypeId::Varchar,
        ] {
            let lt = LogicalType::new(id);
            let mut s = BinarySerializer::new();
            s.begin_object();
            lt.encode(&mut s);
            s.end_object();
            let bytes = s.into_bytes();

            let mut d = BinaryDeserializer::new(&bytes);
            let decoded = LogicalType::decode(&mut d).unwrap();
            d.expect_object_end().unwrap();
            assert_eq!(decoded, lt);
        }
    }

    fn roundtrip_vector(vector: Vector, count: usize) -> Vector {
        let mut s = BinarySerializer::new();
        s.begin_object();
        vector.encode(count, &mut s);
        s.end_object();
        let bytes = s.into_bytes();
        let mut d = BinaryDeserializer::new(&bytes);
        let decoded =
            Vector::decode(vector.logical_type.clone(), count, &mut d).expect("decode vector");
        d.expect_object_end().unwrap();
        decoded
    }

    #[test]
    fn fixed_width_vector_roundtrips_without_nulls() {
        let values = [1i32, 2, 3, 4, 5];
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let v = Vector::new_fixed(LogicalTypeId::Integer, bytes);
        let decoded = roundtrip_vector(v.clone(), values.len());
        assert_eq!(decoded, v);
    }

    #[test]
    fn fixed_width_vector_roundtrips_with_nulls() {
        // i32 column of length 4, third row null.
        let raw = [10i32, 20, 0, 40];
        let bytes: Vec<u8> = raw.iter().flat_map(|v| v.to_le_bytes()).collect();
        let v = Vector {
            logical_type: LogicalType::new(LogicalTypeId::Integer),
            validity: Some(vec![true, true, false, true]),
            data: VectorData::Fixed(bytes),
        };
        let decoded = roundtrip_vector(v.clone(), 4);
        assert_eq!(decoded, v);
    }

    #[test]
    fn varchar_vector_roundtrips_without_nulls() {
        let values = vec![
            Some("hello".to_string()),
            Some("".to_string()),
            Some("world".to_string()),
        ];
        let v = Vector::new_strings(values.clone());
        let decoded = roundtrip_vector(v.clone(), values.len());
        assert_eq!(decoded.data, VectorData::Strings(values));
    }

    #[test]
    fn varchar_vector_roundtrips_with_nulls() {
        let values = vec![Some("a".to_string()), None, Some("c".to_string())];
        let v = Vector::new_strings(values.clone());
        let decoded = roundtrip_vector(v.clone(), values.len());
        assert_eq!(decoded.data, VectorData::Strings(values));
        assert_eq!(decoded.validity, Some(vec![true, false, true]));
    }

    #[test]
    fn data_chunk_roundtrips_single_column_integers() {
        let raw = [42i32, 43, 44];
        let bytes: Vec<u8> = raw.iter().flat_map(|v| v.to_le_bytes()).collect();
        let chunk = DataChunk {
            row_count: raw.len() as u32,
            columns: vec![Vector::new_fixed(LogicalTypeId::Integer, bytes)],
        };

        let mut s = BinarySerializer::new();
        s.begin_object();
        chunk.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = DataChunk::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, chunk);
    }

    #[test]
    fn data_chunk_roundtrips_mixed_columns_with_nulls() {
        // 3 rows, two columns: INTEGER (one null), VARCHAR (one null at a different row).
        let int_bytes: Vec<u8> = [1i32, 0, 3].iter().flat_map(|v| v.to_le_bytes()).collect();
        let int_col = Vector {
            logical_type: LogicalType::new(LogicalTypeId::Integer),
            validity: Some(vec![true, false, true]),
            data: VectorData::Fixed(int_bytes),
        };
        let str_col = Vector::new_strings(vec![
            Some("alice".to_string()),
            Some("bob".to_string()),
            None,
        ]);
        let chunk = DataChunk {
            row_count: 3,
            columns: vec![int_col, str_col],
        };

        let mut s = BinarySerializer::new();
        s.begin_object();
        chunk.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = DataChunk::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, chunk);
    }

    #[test]
    fn vector_rejects_compressed_format_with_clear_error() {
        // Forge a wire fragment that starts with field 90 (vector_type=2 CONSTANT).
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_property(90);
        s.write_u8(2); // CONSTANT_VECTOR
        s.end_property();
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let err = Vector::decode(LogicalType::new(LogicalTypeId::Integer), 1, &mut d).unwrap_err();
        assert!(matches!(err, crate::WireError::UnsupportedVectorType(2)));
    }

    #[test]
    fn decimal_physical_width_matches_duckdb_tiers() {
        // Boundary precisions per DecimalType::GetInternalType.
        assert_eq!(decimal_physical_width(1), 2);
        assert_eq!(decimal_physical_width(4), 2);
        assert_eq!(decimal_physical_width(5), 4);
        assert_eq!(decimal_physical_width(9), 4);
        assert_eq!(decimal_physical_width(10), 8);
        assert_eq!(decimal_physical_width(18), 8);
        assert_eq!(decimal_physical_width(19), 16);
        assert_eq!(decimal_physical_width(38), 16);
    }

    #[test]
    fn extratypeinfo_decimal_round_trips_inside_object() {
        let extra = ExtraTypeInfo::Decimal {
            precision: 10,
            scale: 2,
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        extra.encode_inner(&mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = ExtraTypeInfo::decode_inner(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, extra);
    }

    #[test]
    fn extratypeinfo_decimal_omits_zero_scale_field() {
        // Scale 0 is the WritePropertyWithDefault default -> field 201 omitted.
        let extra = ExtraTypeInfo::Decimal {
            precision: 5,
            scale: 0,
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        extra.encode_inner(&mut s);
        s.end_object();
        let bytes = s.into_bytes();

        // Expected layout:
        //   field 100 (type discriminant) = 0x64 0x00, varint 2 = 0x02
        //   field 200 (width)             = 0xC8 0x00, varint 5 = 0x05
        //   terminator                    = 0xFF 0xFF
        assert_eq!(
            bytes,
            &[0x64, 0x00, 0x02, 0xC8, 0x00, 0x05, 0xFF, 0xFF],
            "scale=0 must not emit field 201"
        );

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = ExtraTypeInfo::decode_inner(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, extra);
    }

    #[test]
    fn extratypeinfo_unsupported_variant_returns_clear_error() {
        // Forge a wire object with type discriminant = LIST_TYPE_INFO (4).
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_property(100);
        s.write_u8(ExtraTypeInfoType::List as u8);
        s.end_property();
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let err = ExtraTypeInfo::decode_inner(&mut d).unwrap_err();
        assert!(matches!(
            err,
            crate::WireError::UnsupportedExtraTypeInfo(ExtraTypeInfoType::List)
        ));
    }

    #[test]
    fn logical_type_with_decimal_extra_round_trips() {
        let lt = LogicalType::with_extra(
            LogicalTypeId::Decimal,
            ExtraTypeInfo::Decimal {
                precision: 18,
                scale: 6,
            },
        );
        let mut s = BinarySerializer::new();
        s.begin_object();
        lt.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = LogicalType::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, lt);
    }

    #[test]
    fn logical_type_primitive_still_omits_field_101() {
        // Pre-existing behaviour: primitive types never emit type_info.
        let lt = LogicalType::new(LogicalTypeId::Integer);
        let mut s = BinarySerializer::new();
        s.begin_object();
        lt.encode(&mut s);
        s.end_object();
        // field 100 = 0x64 0x00, varint 13 = 0x0D, terminator 0xFF 0xFF — no field 101.
        assert_eq!(s.into_bytes(), &[0x64, 0x00, 0x0D, 0xFF, 0xFF]);
    }

    #[test]
    fn decimal_vector_uses_physical_width_for_fixed_path() {
        // DECIMAL(10, 2) -> physical width 8 bytes (i64).
        let lt = LogicalType::with_extra(
            LogicalTypeId::Decimal,
            ExtraTypeInfo::Decimal {
                precision: 10,
                scale: 2,
            },
        );
        let raw: [i64; 3] = [12345, -67890, 0];
        let bytes: Vec<u8> = raw.iter().flat_map(|v| v.to_le_bytes()).collect();
        let v = Vector {
            logical_type: lt.clone(),
            validity: None,
            data: VectorData::Fixed(bytes.clone()),
        };

        let mut s = BinarySerializer::new();
        s.begin_object();
        v.encode(raw.len(), &mut s);
        s.end_object();
        let encoded = s.into_bytes();

        let mut d = BinaryDeserializer::new(&encoded);
        let decoded = Vector::decode(lt, raw.len(), &mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded.data, VectorData::Fixed(bytes));
    }
}
