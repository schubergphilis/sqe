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

/// Maximum nesting depth for `LogicalType` / `ExtraTypeInfo` / `Vector`
/// decode. A wire-controlled `LIST<LIST<LIST<...>>>` (or the equivalent
/// `Vector` recursion) drives mutual recursion that overflows the stack and
/// aborts the process via SIGSEGV — an uncatchable failure even under the
/// unwind panic strategy. The cap turns that into a clean `WireError`.
/// 32 levels is far past anything DuckDB emits in practice.
pub const MAX_DECODE_DEPTH: u8 = 32;

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
    Struct = 100,
    List = 101,
    Map = 102,
    Enum = 104,
    Union = 107,
    Array = 108,
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
            100 => Self::Struct,
            101 => Self::List,
            102 => Self::Map,
            104 => Self::Enum,
            107 => Self::Union,
            108 => Self::Array,
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
    Decimal {
        precision: u8,
        scale: u8,
    },
    /// LIST<T>. `child` is the element type.
    List {
        child: Box<LogicalType>,
    },
    /// STRUCT(...). `fields` is the ordered list of (name, type) pairs.
    Struct {
        fields: Vec<(String, LogicalType)>,
    },
    /// ARRAY<T, N>. Fixed-size variant of LIST — every row carries exactly
    /// `size` elements of `child`. Wire layout is `ArrayTypeInfo`: field 200
    /// (child_type, WriteProperty) + field 201 (size, WritePropertyWithDefault
    /// default 0).
    Array {
        child: Box<LogicalType>,
        size: u32,
    },
    /// ENUM. `values` are the dictionary entries in insertion order; rows
    /// carry an unsigned integer index whose width depends on `values.len()`
    /// (see [`enum_physical_width`]). DuckDB writes this as a hand-rolled
    /// `EnumTypeInfo` rather than via the auto-generated serializer:
    /// field 200 (values_count, u64) + field 201 (list of strings).
    Enum {
        values: Vec<String>,
    },
}

impl ExtraTypeInfo {
    pub fn discriminant(&self) -> ExtraTypeInfoType {
        match self {
            ExtraTypeInfo::Decimal { .. } => ExtraTypeInfoType::Decimal,
            ExtraTypeInfo::List { .. } => ExtraTypeInfoType::List,
            ExtraTypeInfo::Struct { .. } => ExtraTypeInfoType::Struct,
            ExtraTypeInfo::Array { .. } => ExtraTypeInfoType::Array,
            ExtraTypeInfo::Enum { .. } => ExtraTypeInfoType::Enum,
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
            ExtraTypeInfo::List { child } => {
                // Field 200 (child_type) is WriteProperty<LogicalType> — always written.
                s.begin_property(200);
                s.begin_object();
                child.encode(s);
                s.end_object();
                s.end_property();
            }
            ExtraTypeInfo::Struct { fields } => {
                // Field 200 (child_types) is WritePropertyWithDefault<child_list_t<LogicalType>>
                // — omitted when the list is empty. Each entry is a pair object with
                // field 0 (first=name) and field 1 (second=LogicalType).
                if !fields.is_empty() {
                    s.begin_property(200);
                    s.begin_list(fields.len() as u64);
                    for (name, ty) in fields {
                        s.begin_object();
                        s.begin_property(0);
                        s.write_string(name);
                        s.end_property();
                        s.begin_property(1);
                        s.begin_object();
                        ty.encode(s);
                        s.end_object();
                        s.end_property();
                        s.end_object();
                    }
                    s.end_list();
                    s.end_property();
                }
            }
            ExtraTypeInfo::Array { child, size } => {
                // Field 200 (child_type) is WriteProperty<LogicalType> — always written.
                s.begin_property(200);
                s.begin_object();
                child.encode(s);
                s.end_object();
                s.end_property();
                // Field 201 (size) is WritePropertyWithDefault<uint32_t> default 0.
                if *size != 0 {
                    s.begin_property(201);
                    s.write_u32(*size);
                    s.end_property();
                }
            }
            ExtraTypeInfo::Enum { values } => {
                // Field 200 (values_count) is WriteProperty<idx_t> — always written.
                s.begin_property(200);
                s.write_u64(values.len() as u64);
                s.end_property();
                // Field 201 (values) is WriteList<string>. begin_list emits the
                // count varint; each element is a string with its own length+bytes.
                s.begin_property(201);
                s.begin_list(values.len() as u64);
                for v in values {
                    s.write_string(v);
                }
                s.end_list();
                s.end_property();
            }
        }
    }

    /// Decode an `ExtraTypeInfo` after the caller has confirmed an object is
    /// open. Reads the base discriminant first, then dispatches to the
    /// subclass; consumes the trailing 0xFFFF object terminator.
    pub fn decode_inner(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        Self::decode_inner_with_depth(d, MAX_DECODE_DEPTH)
    }

    /// Depth-bounded variant of [`decode_inner`]. `remaining_depth` is the
    /// number of further nesting levels permitted before the codec bails with
    /// [`crate::WireError::RecursionLimitExceeded`].
    pub(crate) fn decode_inner_with_depth(
        d: &mut BinaryDeserializer<'_>,
        remaining_depth: u8,
    ) -> crate::Result<Self> {
        let Some(child_depth) = remaining_depth.checked_sub(1) else {
            return Err(crate::WireError::RecursionLimitExceeded {
                max: MAX_DECODE_DEPTH,
            });
        };
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
                let precision = if d.read_optional(200)? {
                    d.read_u8()?
                } else {
                    0
                };
                let scale = if d.read_optional(201)? {
                    d.read_u8()?
                } else {
                    0
                };
                Ok(ExtraTypeInfo::Decimal { precision, scale })
            }
            ExtraTypeInfoType::List => {
                d.expect_field(200)?;
                let child = LogicalType::decode_with_depth(d, child_depth)?;
                d.expect_object_end()?;
                Ok(ExtraTypeInfo::List {
                    child: Box::new(child),
                })
            }
            ExtraTypeInfoType::Struct => {
                let fields = if d.read_optional(200)? {
                    // Bound the count against bytes remaining before allocating:
                    // each pair object costs well over one byte on the wire.
                    let count = d.read_bounded_count()?;
                    let mut out = Vec::with_capacity(count);
                    for _ in 0..count {
                        d.expect_field(0)?;
                        let name = d.read_string()?;
                        d.expect_field(1)?;
                        let ty = LogicalType::decode_with_depth(d, child_depth)?;
                        d.expect_object_end()?; // end inner LogicalType
                        d.expect_object_end()?; // end pair
                        out.push((name, ty));
                    }
                    out
                } else {
                    Vec::new()
                };
                Ok(ExtraTypeInfo::Struct { fields })
            }
            ExtraTypeInfoType::Array => {
                d.expect_field(200)?;
                let child = LogicalType::decode_with_depth(d, child_depth)?;
                d.expect_object_end()?;
                let size = if d.read_optional(201)? {
                    d.read_u32()?
                } else {
                    0
                };
                Ok(ExtraTypeInfo::Array {
                    child: Box::new(child),
                    size,
                })
            }
            ExtraTypeInfoType::Enum => {
                d.expect_field(200)?;
                let count = d.read_u64()? as usize;
                d.expect_field(201)?;
                // `read_bounded_count` rejects a listed count larger than the
                // bytes that could possibly back it (each string >= 1 byte).
                let listed = d.read_bounded_count()?;
                if listed != count {
                    return Err(crate::WireError::UnexpectedField {
                        expected: count as u16,
                        actual: listed as u16,
                    });
                }
                let mut values = Vec::with_capacity(listed);
                for _ in 0..listed {
                    values.push(d.read_string()?);
                }
                Ok(ExtraTypeInfo::Enum { values })
            }
            other => Err(crate::WireError::UnsupportedExtraTypeInfo(other)),
        }
    }
}

/// Physical storage width for an ENUM index, given the dictionary cardinality.
/// Matches `EnumTypeInfo::DictType`: <=256 entries fit in u8, <=65536 in u16,
/// otherwise u32. The u64 cap exists in the wire schema but DuckDB never picks
/// it (validation rejects enums beyond u32::MAX).
pub fn enum_physical_width(dict_size: usize) -> usize {
    if dict_size <= u8::MAX as usize + 1 {
        1
    } else if dict_size <= u16::MAX as usize + 1 {
        2
    } else {
        4
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
            (LogicalTypeId::Enum, Some(ExtraTypeInfo::Enum { values })) => {
                Some(enum_physical_width(values.len()))
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
        Self::decode_with_depth(d, MAX_DECODE_DEPTH)
    }

    /// Depth-bounded variant of [`decode`]. `remaining_depth` caps how many
    /// further nesting levels the type tree may have before the codec returns
    /// [`crate::WireError::RecursionLimitExceeded`], preventing a wire-driven
    /// stack-overflow / process abort.
    pub(crate) fn decode_with_depth(
        d: &mut BinaryDeserializer<'_>,
        remaining_depth: u8,
    ) -> crate::Result<Self> {
        let Some(child_depth) = remaining_depth.checked_sub(1) else {
            return Err(crate::WireError::RecursionLimitExceeded {
                max: MAX_DECODE_DEPTH,
            });
        };
        d.expect_field(100)?;
        let id = LogicalTypeId::from_u8(d.read_u8()?)?;
        let extra = if d.read_optional(101)? {
            let present = d.read_nullable_present()?;
            if !present {
                return Err(crate::WireError::NullExtraTypeInfo);
            }
            let extra = ExtraTypeInfo::decode_inner_with_depth(d, child_depth)?;
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
/// representation: a tightly packed buffer for fixed-width types, a per-row
/// list for variable-width types (VARCHAR / BLOB), or a nested vector for
/// LIST / STRUCT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorData {
    /// Raw little-endian bytes, `count * fixed_width(type)` long.
    Fixed(Vec<u8>),
    /// One entry per row. `None` means null at that position. The validity
    /// mask is derived from the `None` positions during encode.
    Strings(Vec<Option<String>>),
    /// Same shape as `Strings`, used for BLOB.
    Blobs(Vec<Option<Vec<u8>>>),
    /// LIST<T>. `entries[i] = (offset, length)` slices into `child`. `child`
    /// is a flat vector containing all elements; its row count is
    /// `child_count` (independent of the parent's row count).
    List {
        entries: Vec<(u64, u64)>,
        child_count: u64,
        child: Box<Vector>,
    },
    /// STRUCT(...). `children` are parallel vectors, one per field, each
    /// sharing the parent's row count.
    Struct { children: Vec<Vector> },
    /// ARRAY<T, N>. Fixed-size flattened child vector — child row count is
    /// `array_size * parent_row_count`. Distinct from `List` because the
    /// wire layout omits per-row entries (sizes are constant).
    Array { array_size: u64, child: Box<Vector> },
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

        // Field layout depends on PhysicalType: scalars/varchar/blob use field
        // 102, STRUCT uses field 103, LIST uses 104+105+106. The variant of
        // `data` is the source of truth.
        match &self.data {
            VectorData::Fixed(bytes) => {
                s.begin_property(102);
                s.write_data_ptr(bytes);
                s.end_property();
            }
            VectorData::Strings(strs) => {
                s.begin_property(102);
                s.begin_list(count as u64);
                for entry in strs.iter().take(count) {
                    let value = entry.as_deref().unwrap_or("");
                    s.write_string(value);
                }
                s.end_list();
                s.end_property();
            }
            VectorData::Blobs(blobs) => {
                s.begin_property(102);
                s.begin_list(count as u64);
                for entry in blobs.iter().take(count) {
                    let empty = Vec::new();
                    let value = entry.as_ref().unwrap_or(&empty);
                    s.write_data_ptr(value);
                }
                s.end_list();
                s.end_property();
            }
            VectorData::Struct { children } => {
                // STRUCT: field 103 "children" is a list of child vector objects.
                s.begin_property(103);
                s.begin_list(children.len() as u64);
                for child in children {
                    s.begin_object();
                    child.encode(count, s);
                    s.end_object();
                }
                s.end_list();
                s.end_property();
            }
            VectorData::Array { array_size, child } => {
                // ARRAY: field 103 (array_size, u64), field 104 (child vector
                // with array_size * count elements).
                s.begin_property(103);
                s.write_u64(*array_size);
                s.end_property();
                s.begin_property(104);
                s.begin_object();
                child.encode((*array_size as usize) * count, s);
                s.end_object();
                s.end_property();
            }
            VectorData::List {
                entries,
                child_count,
                child,
            } => {
                // LIST: field 104 (list_size, u64), field 105 (entries list of
                // {offset, length} objects), field 106 (child vector object).
                s.begin_property(104);
                s.write_u64(*child_count);
                s.end_property();

                s.begin_property(105);
                s.begin_list(count as u64);
                for (offset, length) in entries.iter().take(count) {
                    s.begin_object();
                    s.begin_property(100);
                    s.write_u64(*offset);
                    s.end_property();
                    s.begin_property(101);
                    s.write_u64(*length);
                    s.end_property();
                    s.end_object();
                }
                s.end_list();
                s.end_property();

                s.begin_property(106);
                s.begin_object();
                child.encode(*child_count as usize, s);
                s.end_object();
                s.end_property();
            }
        }
    }

    pub fn decode(
        logical_type: LogicalType,
        count: usize,
        d: &mut BinaryDeserializer<'_>,
    ) -> crate::Result<Self> {
        Self::decode_with_depth(logical_type, count, d, MAX_DECODE_DEPTH)
    }

    /// Depth-bounded variant of [`decode`]. Caps nested LIST/ARRAY/STRUCT
    /// recursion so a wire-controlled deeply nested vector cannot overflow the
    /// stack and abort the process.
    pub(crate) fn decode_with_depth(
        logical_type: LogicalType,
        count: usize,
        d: &mut BinaryDeserializer<'_>,
        remaining_depth: u8,
    ) -> crate::Result<Self> {
        let Some(child_depth) = remaining_depth.checked_sub(1) else {
            return Err(crate::WireError::RecursionLimitExceeded {
                max: MAX_DECODE_DEPTH,
            });
        };
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
            // `count` is the parent's row_count, itself a wire-supplied value.
            // Allocate incrementally instead of `with_capacity(count)` so a
            // bogus 4e9 row_count does not pre-allocate gigabytes before the
            // first byte is read.
            let mut bits = Vec::new();
            for i in 0..count {
                let byte = raw.get(i / 8).copied().unwrap_or(0);
                bits.push(byte & (1 << (i % 8)) != 0);
            }
            Some(bits)
        } else {
            None
        };

        let data = match logical_type.id {
            // MAP shares LIST's physical layout: DuckDB models `MAP<K,V>` as
            // a `LogicalTypeId::Map` whose `ExtraTypeInfo::List` carries a
            // `STRUCT(key, value)` child. The Vector wire bytes are identical.
            LogicalTypeId::List | LogicalTypeId::Map => {
                // LIST: field 104 (list_size), 105 (entries), 106 (child).
                d.expect_field(104)?;
                let child_count = d.read_u64()?;
                d.expect_field(105)?;
                // Each entry object costs several bytes; bound before allocating.
                let actual = d.read_bounded_count()?;
                let mut entries = Vec::with_capacity(actual);
                for _ in 0..actual {
                    d.expect_field(100)?;
                    let offset = d.read_u64()?;
                    d.expect_field(101)?;
                    let length = d.read_u64()?;
                    d.expect_object_end()?;
                    entries.push((offset, length));
                }
                d.expect_field(106)?;
                let child_logical = match &logical_type.extra {
                    Some(ExtraTypeInfo::List { child }) => (**child).clone(),
                    _ => return Err(crate::WireError::UnsupportedLogicalType(logical_type.id)),
                };
                let child_vec =
                    Vector::decode_with_depth(child_logical, child_count as usize, d, child_depth)?;
                d.expect_object_end()?;
                VectorData::List {
                    entries,
                    child_count,
                    child: Box::new(child_vec),
                }
            }
            LogicalTypeId::Array => {
                // ARRAY: field 103 (array_size, u64), field 104 (child vector).
                d.expect_field(103)?;
                let array_size = d.read_u64()?;
                d.expect_field(104)?;
                let child_logical = match &logical_type.extra {
                    Some(ExtraTypeInfo::Array { child, .. }) => (**child).clone(),
                    _ => {
                        return Err(crate::WireError::UnsupportedLogicalType(
                            LogicalTypeId::Array,
                        ))
                    }
                };
                let child_vec = Vector::decode_with_depth(
                    child_logical,
                    (array_size as usize).saturating_mul(count),
                    d,
                    child_depth,
                )?;
                d.expect_object_end()?;
                VectorData::Array {
                    array_size,
                    child: Box::new(child_vec),
                }
            }
            // UNION reuses STRUCT's physical layout — DuckDB's
            // `LogicalType::UNION` factory builds a StructTypeInfo with a
            // UTINYINT "tag" prepended to the member list, so the wire is
            // identical (field 103 + parallel child vectors). The only
            // difference is the parent LogicalTypeId.
            LogicalTypeId::Struct | LogicalTypeId::Union => {
                d.expect_field(103)?;
                // Each child vector costs at least one byte; bound the count.
                let child_count = d.read_bounded_count()?;
                let field_types: Vec<LogicalType> = match &logical_type.extra {
                    Some(ExtraTypeInfo::Struct { fields }) => {
                        fields.iter().map(|(_, ty)| ty.clone()).collect()
                    }
                    _ => return Err(crate::WireError::UnsupportedLogicalType(logical_type.id)),
                };
                if child_count != field_types.len() {
                    return Err(crate::WireError::UnexpectedField {
                        expected: field_types.len() as u16,
                        actual: child_count as u16,
                    });
                }
                let mut children = Vec::with_capacity(child_count);
                for ty in field_types {
                    let child = Vector::decode_with_depth(ty, count, d, child_depth)?;
                    d.expect_object_end()?;
                    children.push(child);
                }
                VectorData::Struct { children }
            }
            _ if logical_type.physical_width().is_some() => {
                d.expect_field(102)?;
                let bytes = d.read_data_ptr()?;
                // Validate the buffer is large enough for `count` fixed-width
                // values before building the Fixed vector. Without this, a
                // server claiming row_count = 1e6 with a 4-byte buffer makes
                // the Arrow bridge (`scalar_buffer_le`) slice out of bounds and
                // panic the consuming query task. `physical_width()` is Some on
                // this arm, and a wire-controlled DECIMAL precision can only
                // pick a known small width (2/4/8/16).
                let width = logical_type
                    .physical_width()
                    .expect("physical_width is Some on this match arm");
                let needed =
                    count
                        .checked_mul(width)
                        .ok_or(crate::WireError::CountExceedsRemaining {
                            count: count as u64,
                            remaining: bytes.len() as u64,
                        })?;
                if bytes.len() < needed {
                    return Err(crate::WireError::CountExceedsRemaining {
                        count: needed as u64,
                        remaining: bytes.len() as u64,
                    });
                }
                VectorData::Fixed(bytes)
            }
            LogicalTypeId::Varchar => {
                d.expect_field(102)?;
                // Bound against bytes remaining: each string is >= 1 byte.
                let actual = d.read_bounded_count()?;
                let take = actual.min(count);
                let mut values = Vec::with_capacity(take);
                let validity_ref = validity.as_deref();
                for i in 0..actual {
                    let valid = validity_ref
                        .map(|v| v.get(i).copied().unwrap_or(true))
                        .unwrap_or(true);
                    // Real DuckDB writes uninitialised bytes (often non-UTF-8)
                    // at NULL VARCHAR positions rather than an empty string.
                    // Skip them by length without UTF-8 validation when the
                    // row is known to be null.
                    if valid {
                        values.push(Some(d.read_string()?));
                    } else {
                        d.skip_string()?;
                        values.push(None);
                    }
                }
                VectorData::Strings(values)
            }
            LogicalTypeId::Blob => {
                d.expect_field(102)?;
                // Each blob entry is a length-prefixed slot (>= 1 byte): bound.
                let actual = d.read_bounded_count()?;
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
        // Each LogicalType object costs several bytes; bound before allocating.
        let type_count = d.read_bounded_count()?;
        let mut types = Vec::with_capacity(type_count);
        for _ in 0..type_count {
            let t = LogicalType::decode(d)?;
            d.expect_object_end()?;
            types.push(t);
        }

        d.expect_field(102)?;
        let column_count = d.read_bounded_count()?;
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
    fn varchar_vector_decodes_null_rows_with_garbage_string_bytes() {
        // Real DuckDB writes uninitialised bytes (often non-UTF-8) at NULL
        // VARCHAR positions instead of an empty string. Forge that exact
        // wire shape (1 row, NULL validity, 1-byte 0x80 payload) and verify
        // decode succeeds with a None rather than tripping UTF-8 validation.
        // Hand-rolled byte layout, since the safe BinarySerializer API
        // can't write non-UTF-8 strings (and shouldn't be able to).
        let bytes: Vec<u8> = [
            // field 100 (has_validity) = true
            0x64, 0x00, 0x01,
            // field 101 (validity mask) — 8 bytes, bit 0 = 0 (invalid)
            0x65, 0x00, 0x08, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            // field 102 (data list) — count=1, then a single 1-byte "string" = 0x80
            0x66, 0x00, 0x01, 0x01, 0x80, // object terminator
            0xff, 0xff,
        ]
        .to_vec();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = Vector::decode(LogicalType::new(LogicalTypeId::Varchar), 1, &mut d).unwrap();
        d.expect_object_end().unwrap();
        match decoded.data {
            VectorData::Strings(values) => assert_eq!(values, vec![None]),
            other => panic!("expected Strings, got {other:?}"),
        }
        assert_eq!(decoded.validity, Some(vec![false]));
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
        // Forge a wire object with type discriminant = AGGREGATE_STATE_TYPE_INFO
        // (8) — recognised in the discriminant enum but not implemented as a
        // codec variant yet.
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_property(100);
        s.write_u8(ExtraTypeInfoType::AggregateState as u8);
        s.end_property();
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let err = ExtraTypeInfo::decode_inner(&mut d).unwrap_err();
        assert!(matches!(
            err,
            crate::WireError::UnsupportedExtraTypeInfo(ExtraTypeInfoType::AggregateState)
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
    fn extratypeinfo_list_round_trips_inside_object() {
        let extra = ExtraTypeInfo::List {
            child: Box::new(LogicalType::new(LogicalTypeId::Integer)),
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
    fn extratypeinfo_struct_round_trips_with_multiple_fields() {
        let extra = ExtraTypeInfo::Struct {
            fields: vec![
                ("a".to_string(), LogicalType::new(LogicalTypeId::Integer)),
                ("b".to_string(), LogicalType::new(LogicalTypeId::Varchar)),
                (
                    "c".to_string(),
                    LogicalType::with_extra(
                        LogicalTypeId::Decimal,
                        ExtraTypeInfo::Decimal {
                            precision: 10,
                            scale: 2,
                        },
                    ),
                ),
            ],
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
    fn extratypeinfo_struct_empty_fields_omits_field_200() {
        // child_types is WritePropertyWithDefault — empty list omits the field.
        let extra = ExtraTypeInfo::Struct { fields: Vec::new() };
        let mut s = BinarySerializer::new();
        s.begin_object();
        extra.encode_inner(&mut s);
        s.end_object();
        let bytes = s.into_bytes();
        // field 100 (discriminant) = 0x64 0x00, varint 5 (STRUCT) = 0x05, terminator.
        assert_eq!(bytes, &[0x64, 0x00, 0x05, 0xFF, 0xFF]);
    }

    #[test]
    fn nested_list_of_struct_round_trips() {
        // LIST<STRUCT(name VARCHAR, score INTEGER)>
        let extra = ExtraTypeInfo::List {
            child: Box::new(LogicalType::with_extra(
                LogicalTypeId::Struct,
                ExtraTypeInfo::Struct {
                    fields: vec![
                        ("name".to_string(), LogicalType::new(LogicalTypeId::Varchar)),
                        (
                            "score".to_string(),
                            LogicalType::new(LogicalTypeId::Integer),
                        ),
                    ],
                },
            )),
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
    fn list_vector_round_trips_int_children() {
        // LIST<INT>: 3 rows -> [10,20], [], [30,40,50]
        let int_bytes: Vec<u8> = [10i32, 20, 30, 40, 50]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let child = Vector::new_fixed(LogicalTypeId::Integer, int_bytes);
        let v = Vector {
            logical_type: LogicalType::with_extra(
                LogicalTypeId::List,
                ExtraTypeInfo::List {
                    child: Box::new(LogicalType::new(LogicalTypeId::Integer)),
                },
            ),
            validity: None,
            data: VectorData::List {
                entries: vec![(0, 2), (2, 0), (2, 3)],
                child_count: 5,
                child: Box::new(child),
            },
        };

        let mut s = BinarySerializer::new();
        s.begin_object();
        v.encode(3, &mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = Vector::decode(v.logical_type.clone(), 3, &mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn struct_vector_round_trips_two_columns() {
        // STRUCT(id INTEGER, name VARCHAR), 2 rows.
        let int_bytes: Vec<u8> = [1i32, 2].iter().flat_map(|v| v.to_le_bytes()).collect();
        let id_col = Vector::new_fixed(LogicalTypeId::Integer, int_bytes);
        let name_col =
            Vector::new_strings(vec![Some("alice".to_string()), Some("bob".to_string())]);
        let v = Vector {
            logical_type: LogicalType::with_extra(
                LogicalTypeId::Struct,
                ExtraTypeInfo::Struct {
                    fields: vec![
                        ("id".to_string(), LogicalType::new(LogicalTypeId::Integer)),
                        ("name".to_string(), LogicalType::new(LogicalTypeId::Varchar)),
                    ],
                },
            ),
            validity: None,
            data: VectorData::Struct {
                children: vec![id_col, name_col],
            },
        };

        let mut s = BinarySerializer::new();
        s.begin_object();
        v.encode(2, &mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = Vector::decode(v.logical_type.clone(), 2, &mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, v);
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

    #[test]
    fn extratypeinfo_array_round_trips_with_size_field() {
        let extra = ExtraTypeInfo::Array {
            child: Box::new(LogicalType::new(LogicalTypeId::Integer)),
            size: 4,
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
    fn extratypeinfo_array_size_zero_is_field_default() {
        // size = 0 is the WritePropertyWithDefault default — field 201 must
        // not be emitted, and decode should reconstruct 0.
        let extra = ExtraTypeInfo::Array {
            child: Box::new(LogicalType::new(LogicalTypeId::Integer)),
            size: 0,
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
        // Spot-check: field 201 must not appear in the serialized output.
        // The first occurrence of byte 201 (0xC9) followed by 0x00 would
        // indicate field 201 was emitted; absence is the easier check.
        // We look for the pattern 0xC9, 0x00 (field 201 LE).
        let leaked_field_201 = bytes.windows(2).any(|w| w == [0xC9, 0x00]);
        assert!(
            !leaked_field_201,
            "field 201 must not be emitted when size=0"
        );
    }

    #[test]
    fn array_vector_round_trips_fixed_size_children() {
        // ARRAY<INT, 3>: 2 rows, child holds 6 ints in row-major layout.
        let int_bytes: Vec<u8> = [10i32, 20, 30, 40, 50, 60]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let child = Vector::new_fixed(LogicalTypeId::Integer, int_bytes);
        let lt = LogicalType::with_extra(
            LogicalTypeId::Array,
            ExtraTypeInfo::Array {
                child: Box::new(LogicalType::new(LogicalTypeId::Integer)),
                size: 3,
            },
        );
        let v = Vector {
            logical_type: lt.clone(),
            validity: None,
            data: VectorData::Array {
                array_size: 3,
                child: Box::new(child),
            },
        };

        let mut s = BinarySerializer::new();
        s.begin_object();
        v.encode(2, &mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = Vector::decode(lt, 2, &mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn enum_physical_width_matches_duckdb_tiers() {
        assert_eq!(enum_physical_width(1), 1);
        assert_eq!(enum_physical_width(256), 1);
        assert_eq!(enum_physical_width(257), 2);
        assert_eq!(enum_physical_width(65_536), 2);
        assert_eq!(enum_physical_width(65_537), 4);
        assert_eq!(enum_physical_width(1_000_000), 4);
    }

    #[test]
    fn extratypeinfo_enum_round_trips_three_values() {
        let extra = ExtraTypeInfo::Enum {
            values: vec!["red".to_string(), "green".to_string(), "blue".to_string()],
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
    fn enum_vector_round_trips_with_u8_indices() {
        // 3-value ENUM, physical width = 1 byte.
        let lt = LogicalType::with_extra(
            LogicalTypeId::Enum,
            ExtraTypeInfo::Enum {
                values: vec!["red".to_string(), "green".to_string(), "blue".to_string()],
            },
        );
        // Rows: 0, 2, 1, 0 -> indices into the dict.
        let v = Vector {
            logical_type: lt.clone(),
            validity: None,
            data: VectorData::Fixed(vec![0u8, 2, 1, 0]),
        };

        let mut s = BinarySerializer::new();
        s.begin_object();
        v.encode(4, &mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = Vector::decode(lt, 4, &mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn enum_vector_round_trips_with_u16_indices() {
        // Force the u16 tier with a dictionary of 300 entries.
        let values: Vec<String> = (0..300).map(|i| format!("v{i}")).collect();
        let lt = LogicalType::with_extra(
            LogicalTypeId::Enum,
            ExtraTypeInfo::Enum {
                values: values.clone(),
            },
        );
        assert_eq!(lt.physical_width(), Some(2));

        let raw_indices: [u16; 4] = [0, 299, 100, 50];
        let bytes: Vec<u8> = raw_indices.iter().flat_map(|v| v.to_le_bytes()).collect();
        let v = Vector {
            logical_type: lt.clone(),
            validity: None,
            data: VectorData::Fixed(bytes),
        };

        let mut s = BinarySerializer::new();
        s.begin_object();
        v.encode(4, &mut s);
        s.end_object();
        let encoded = s.into_bytes();

        let mut d = BinaryDeserializer::new(&encoded);
        let decoded = Vector::decode(lt, 4, &mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn union_vector_reuses_struct_physical_layout() {
        // DuckDB's `LogicalType::UNION(members)` factory builds a
        // StructTypeInfo with a hidden UTINYINT "tag" prepended to the
        // member list. The wire bytes are identical to a STRUCT — only the
        // outer LogicalTypeId changes from Struct to Union.
        // 3 rows: row 0 picks member "a" (tag=0), row 1 picks "b" (tag=1),
        // row 2 picks "a" (tag=0).
        let tag_col = Vector::new_fixed(LogicalTypeId::UTinyInt, vec![0u8, 1, 0]);
        let a_bytes: Vec<u8> = [10i32, 0, 30]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let a_col = Vector {
            logical_type: LogicalType::new(LogicalTypeId::Integer),
            validity: Some(vec![true, false, true]),
            data: VectorData::Fixed(a_bytes),
        };
        let b_col = Vector::new_strings(vec![None, Some("hi".to_string()), None]);

        let union_lt = LogicalType::with_extra(
            LogicalTypeId::Union,
            ExtraTypeInfo::Struct {
                fields: vec![
                    ("".to_string(), LogicalType::new(LogicalTypeId::UTinyInt)),
                    ("a".to_string(), LogicalType::new(LogicalTypeId::Integer)),
                    ("b".to_string(), LogicalType::new(LogicalTypeId::Varchar)),
                ],
            },
        );
        let v = Vector {
            logical_type: union_lt.clone(),
            validity: None,
            data: VectorData::Struct {
                children: vec![tag_col, a_col, b_col],
            },
        };

        let mut s = BinarySerializer::new();
        s.begin_object();
        v.encode(3, &mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = Vector::decode(union_lt, 3, &mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn map_vector_reuses_list_physical_layout() {
        // MAP<VARCHAR, INTEGER> = LogicalTypeId::Map + ExtraTypeInfo::List
        // { child: STRUCT(key VARCHAR, value INTEGER) }. Wire payload is
        // identical to LIST<STRUCT<...>>.
        let key_col = Vector::new_strings(vec![Some("a".to_string()), Some("b".to_string())]);
        let val_bytes: Vec<u8> = [1i32, 2].iter().flat_map(|v| v.to_le_bytes()).collect();
        let val_col = Vector::new_fixed(LogicalTypeId::Integer, val_bytes);
        let struct_lt = LogicalType::with_extra(
            LogicalTypeId::Struct,
            ExtraTypeInfo::Struct {
                fields: vec![
                    ("key".to_string(), LogicalType::new(LogicalTypeId::Varchar)),
                    (
                        "value".to_string(),
                        LogicalType::new(LogicalTypeId::Integer),
                    ),
                ],
            },
        );
        let entry_vec = Vector {
            logical_type: struct_lt.clone(),
            validity: None,
            data: VectorData::Struct {
                children: vec![key_col, val_col],
            },
        };

        let map_lt = LogicalType::with_extra(
            LogicalTypeId::Map,
            ExtraTypeInfo::List {
                child: Box::new(struct_lt),
            },
        );
        // One row: {"a" -> 1, "b" -> 2}; two entries total.
        let v = Vector {
            logical_type: map_lt.clone(),
            validity: None,
            data: VectorData::List {
                entries: vec![(0, 2)],
                child_count: 2,
                child: Box::new(entry_vec),
            },
        };

        let mut s = BinarySerializer::new();
        s.begin_object();
        v.encode(1, &mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = Vector::decode(map_lt, 1, &mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, v);
    }

    // ── Decoder safety guards (QUACK-01/02/03/04) ───────────────────────

    /// Build a `LogicalType` nested `depth` levels deep as
    /// LIST<LIST<...<INTEGER>>>.
    fn nested_list_type(depth: usize) -> LogicalType {
        let mut t = LogicalType::new(LogicalTypeId::Integer);
        for _ in 0..depth {
            t = LogicalType::with_extra(
                LogicalTypeId::List,
                ExtraTypeInfo::List { child: Box::new(t) },
            );
        }
        t
    }

    #[test]
    fn deeply_nested_logical_type_is_rejected_not_overflowed() {
        // QUACK-01: a type nested past MAX_DECODE_DEPTH must return a clean
        // error instead of overflowing the stack.
        let lt = nested_list_type((MAX_DECODE_DEPTH as usize) + 5);
        let mut s = BinarySerializer::new();
        s.begin_object();
        lt.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let err = LogicalType::decode(&mut d).unwrap_err();
        assert!(matches!(
            err,
            crate::WireError::RecursionLimitExceeded { .. }
        ));
    }

    #[test]
    fn moderately_nested_logical_type_still_decodes() {
        // A depth well within the cap must round-trip normally.
        let lt = nested_list_type(8);
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
    fn datachunk_rejects_oversized_type_count_without_allocating() {
        // QUACK-02: a type-list count far larger than the bytes remaining must
        // be rejected by the bounded-count check rather than driving a huge
        // Vec::with_capacity.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&100u16.to_le_bytes()); // field 100 (row_count)
        crate::varint::encode_unsigned(0, &mut bytes); // row_count = 0
        bytes.extend_from_slice(&101u16.to_le_bytes()); // field 101 (types list)
        crate::varint::encode_unsigned(1_000_000_000, &mut bytes); // bogus count
                                                                   // no payload follows.

        let mut d = BinaryDeserializer::new(&bytes);
        let err = DataChunk::decode(&mut d).unwrap_err();
        assert!(matches!(
            err,
            crate::WireError::CountExceedsRemaining { .. }
        ));
    }

    #[test]
    fn fixed_vector_rejects_row_count_exceeding_buffer() {
        // QUACK-03: a fixed-width column claiming many rows but carrying a tiny
        // data buffer must error, not produce a Fixed vector that later panics
        // on OOB slicing in the Arrow bridge.
        // Hand-roll: has_validity=false, field 102 data_ptr of 4 bytes, decoded
        // as INTEGER (width 4) with count = 1000.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&100u16.to_le_bytes()); // field 100 (has_validity)
        bytes.push(0); // false
        bytes.extend_from_slice(&102u16.to_le_bytes()); // field 102 (data ptr)
        crate::varint::encode_unsigned(4, &mut bytes); // data length 4
        bytes.extend_from_slice(&[1, 0, 0, 0]); // a single i32

        let mut d = BinaryDeserializer::new(&bytes);
        let err =
            Vector::decode(LogicalType::new(LogicalTypeId::Integer), 1000, &mut d).unwrap_err();
        assert!(matches!(
            err,
            crate::WireError::CountExceedsRemaining { .. }
        ));
    }

    #[test]
    fn fixed_vector_accepts_buffer_matching_row_count() {
        // The same shape with a buffer that matches count*width must decode.
        let raw: Vec<u8> = [1i32, 2, 3].iter().flat_map(|v| v.to_le_bytes()).collect();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&100u16.to_le_bytes());
        bytes.push(0); // has_validity = false
        bytes.extend_from_slice(&102u16.to_le_bytes());
        crate::varint::encode_unsigned(raw.len() as u64, &mut bytes);
        bytes.extend_from_slice(&raw);

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = Vector::decode(LogicalType::new(LogicalTypeId::Integer), 3, &mut d).unwrap();
        assert_eq!(decoded.data, VectorData::Fixed(raw));
    }

    #[test]
    fn enum_decode_rejects_oversized_value_count() {
        // QUACK-02/04: an ENUM type_info claiming a huge values_count with no
        // backing bytes must be rejected, not pre-allocate gigabytes.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&100u16.to_le_bytes()); // field 100 discriminant
        crate::varint::encode_unsigned(ExtraTypeInfoType::Enum as u64, &mut bytes);
        bytes.extend_from_slice(&200u16.to_le_bytes()); // field 200 values_count
        crate::varint::encode_unsigned(5_000_000_000, &mut bytes); // huge count
        bytes.extend_from_slice(&201u16.to_le_bytes()); // field 201 values list
        crate::varint::encode_unsigned(5_000_000_000, &mut bytes); // listed count
                                                                   // no string payload follows.

        let mut d = BinaryDeserializer::new(&bytes);
        let err = ExtraTypeInfo::decode_inner(&mut d).unwrap_err();
        assert!(matches!(
            err,
            crate::WireError::CountExceedsRemaining { .. }
        ));
    }
}
