//! Order-preserving Z-order (Morton) key UDF for sort-compaction.
//!
//! `system.rewrite_data_files(strategy => 'sort', sort_order => 'zorder(a, b)')`
//! clusters rows so that points close in the multi-column space land close in
//! the file layout, which lets readers prune row groups on any of the clustered
//! columns (not just a single leading sort column). We do that by projecting a
//! synthetic Z-value, sorting on it, then dropping it before the write.
//!
//! The UDF is `__sqe_zvalue(col0, col1, ...) -> FixedSizeBinary(n_cols * 8)`.
//! Each column is first encoded to an 8-byte, big-endian, order-preserving key
//! (so lexicographic byte order equals value order), then the per-column keys
//! are bit-interleaved in the classic Morton fashion: output bit `p` takes bit
//! `p / n_cols` (from the MSB) of column `p % n_cols`. Sorting the resulting
//! fixed-size-binary column ascending yields Z-order.
//!
//! NULLs encode to an all-zero key, so they sort first. Strings/binary use
//! their first 8 bytes (right-padded), so clustering is on the prefix.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryBuilder,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Bytes of order-preserving key produced per input column before interleave.
const KEY_BYTES_PER_COL: usize = 8;

/// SQL name of the UDF. Underscore-prefixed to mark it engine-internal.
pub const ZVALUE_UDF_NAME: &str = "__sqe_zvalue";

/// Build the Z-value scalar UDF for registration on a session context.
pub fn zorder_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ZOrderUdf::new())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ZOrderUdf {
    signature: Signature,
}

impl ZOrderUdf {
    fn new() -> Self {
        Self {
            // Any column types, one or more. Per-type validity is enforced in
            // invoke_with_args so we can give a precise error on unsupported
            // types rather than a generic signature-mismatch.
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ZOrderUdf {
    fn name(&self) -> &str {
        ZVALUE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(
                "__sqe_zvalue requires at least one argument".to_string(),
            ));
        }
        let width = (arg_types.len() * KEY_BYTES_PER_COL) as i32;
        Ok(DataType::FixedSizeBinary(width))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n_rows = args.number_rows;
        let n_cols = args.args.len();
        if n_cols == 0 {
            return Err(DataFusionError::Plan(
                "__sqe_zvalue requires at least one argument".to_string(),
            ));
        }

        // Materialise each argument to an array so scalars and arrays share one
        // code path, then encode each column to a per-row 8-byte key.
        let mut col_keys: Vec<Vec<[u8; KEY_BYTES_PER_COL]>> = Vec::with_capacity(n_cols);
        for arg in &args.args {
            let array = arg.clone().into_array(n_rows)?;
            col_keys.push(encode_column(&array)?);
        }

        let width = n_cols * KEY_BYTES_PER_COL;
        let mut builder = FixedSizeBinaryBuilder::new(width as i32);
        let mut row_keys: Vec<[u8; KEY_BYTES_PER_COL]> = vec![[0u8; KEY_BYTES_PER_COL]; n_cols];
        for row in 0..n_rows {
            for (c, keys) in col_keys.iter().enumerate() {
                row_keys[c] = keys[row];
            }
            let interleaved = interleave(&row_keys);
            builder
                .append_value(&interleaved)
                .map_err(|e| DataFusionError::Internal(format!("zvalue build failed: {e}")))?;
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Encode one arrow array to a per-row order-preserving 8-byte key. A null in a
/// row yields an all-zero key (sorts first).
// Index-based loops write into a pre-sized `out` by row position while skipping
// nulls; that reads clearer here than zip/enumerate gymnastics.
#[allow(clippy::needless_range_loop)]
fn encode_column(array: &ArrayRef) -> DFResult<Vec<[u8; KEY_BYTES_PER_COL]>> {
    let n = array.len();
    let mut out = vec![[0u8; KEY_BYTES_PER_COL]; n];

    macro_rules! encode_primitive {
        ($arr_ty:ty, $to_u64:expr) => {{
            let a = array.as_any().downcast_ref::<$arr_ty>().unwrap();
            for i in 0..n {
                if a.is_null(i) {
                    continue; // leave all-zero
                }
                let v = a.value(i);
                out[i] = ($to_u64)(v).to_be_bytes();
            }
        }};
    }

    match array.data_type() {
        DataType::Int8 => encode_primitive!(Int8Array, |v: i8| sign_flip(v as i64)),
        DataType::Int16 => encode_primitive!(Int16Array, |v: i16| sign_flip(v as i64)),
        DataType::Int32 => encode_primitive!(Int32Array, |v: i32| sign_flip(v as i64)),
        DataType::Int64 => encode_primitive!(Int64Array, |v: i64| sign_flip(v)),
        DataType::UInt8 => encode_primitive!(UInt8Array, |v: u8| v as u64),
        DataType::UInt16 => encode_primitive!(UInt16Array, |v: u16| v as u64),
        DataType::UInt32 => encode_primitive!(UInt32Array, |v: u32| v as u64),
        DataType::UInt64 => encode_primitive!(UInt64Array, |v: u64| v),
        DataType::Float32 => encode_primitive!(Float32Array, |v: f32| float_key(v as f64)),
        DataType::Float64 => encode_primitive!(Float64Array, |v: f64| float_key(v)),
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            for i in 0..n {
                if a.is_null(i) {
                    continue;
                }
                out[i] = (a.value(i) as u64).to_be_bytes();
            }
        }
        DataType::Date32 => encode_primitive!(Date32Array, |v: i32| sign_flip(v as i64)),
        DataType::Date64 => encode_primitive!(Date64Array, |v: i64| sign_flip(v)),
        DataType::Timestamp(unit, _) => {
            macro_rules! ts {
                ($arr_ty:ty) => {{
                    let a = array.as_any().downcast_ref::<$arr_ty>().unwrap();
                    for i in 0..n {
                        if a.is_null(i) {
                            continue;
                        }
                        out[i] = sign_flip(a.value(i)).to_be_bytes();
                    }
                }};
            }
            match unit {
                TimeUnit::Second => ts!(TimestampSecondArray),
                TimeUnit::Millisecond => ts!(TimestampMillisecondArray),
                TimeUnit::Microsecond => ts!(TimestampMicrosecondArray),
                TimeUnit::Nanosecond => ts!(TimestampNanosecondArray),
            }
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..n {
                if a.is_null(i) {
                    continue;
                }
                out[i] = prefix_key(a.value(i).as_bytes());
            }
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            for i in 0..n {
                if a.is_null(i) {
                    continue;
                }
                out[i] = prefix_key(a.value(i).as_bytes());
            }
        }
        DataType::Binary => {
            let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            for i in 0..n {
                if a.is_null(i) {
                    continue;
                }
                out[i] = prefix_key(a.value(i));
            }
        }
        DataType::LargeBinary => {
            let a = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            for i in 0..n {
                if a.is_null(i) {
                    continue;
                }
                out[i] = prefix_key(a.value(i));
            }
        }
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "__sqe_zvalue does not support column type {other:?}"
            )));
        }
    }
    Ok(out)
}

/// Flip the sign bit of a signed integer so two's-complement order becomes
/// unsigned big-endian order (negatives sort before positives).
#[inline]
fn sign_flip(v: i64) -> u64 {
    (v as u64) ^ (1u64 << 63)
}

/// IEEE-754 order-preserving transform: negatives are flipped entirely,
/// non-negatives have only the sign bit set, so the bit pattern sorts in value
/// order.
#[inline]
fn float_key(f: f64) -> u64 {
    let bits = f.to_bits();
    let mask = if bits & (1u64 << 63) != 0 {
        u64::MAX
    } else {
        1u64 << 63
    };
    bits ^ mask
}

/// First 8 bytes of a byte string, right-padded with zeros. A shorter value is
/// a prefix of a longer one with the same leading bytes and sorts first.
#[inline]
fn prefix_key(bytes: &[u8]) -> [u8; KEY_BYTES_PER_COL] {
    let mut k = [0u8; KEY_BYTES_PER_COL];
    let take = bytes.len().min(KEY_BYTES_PER_COL);
    k[..take].copy_from_slice(&bytes[..take]);
    k
}

/// Bit-interleave M 8-byte keys into an M*8-byte Morton key. Output bit `p`
/// (from the MSB) is bit `p / M` (from the MSB) of column `p % M`.
fn interleave(keys: &[[u8; KEY_BYTES_PER_COL]]) -> Vec<u8> {
    let m = keys.len();
    let words: Vec<u64> = keys.iter().map(|k| u64::from_be_bytes(*k)).collect();
    let total_bits = m * 64;
    let mut out = vec![0u8; m * KEY_BYTES_PER_COL];
    for p in 0..total_bits {
        let col = p % m;
        let src_bit = p / m; // 0 = MSB
        let bit = (words[col] >> (63 - src_bit)) & 1;
        if bit == 1 {
            let byte = p / 8;
            let off = 7 - (p % 8); // MSB-first within the byte
            out[byte] |= 1u8 << off;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};

    fn z(cols: Vec<ArrayRef>) -> Vec<Vec<u8>> {
        let n_rows = cols[0].len();
        let udf = ZOrderUdf::new();
        let width = cols.len() * KEY_BYTES_PER_COL;
        let args = ScalarFunctionArgs {
            args: cols.iter().cloned().map(ColumnarValue::Array).collect(),
            arg_fields: cols
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    Arc::new(arrow::datatypes::Field::new(
                        format!("c{i}"),
                        c.data_type().clone(),
                        true,
                    ))
                })
                .collect(),
            number_rows: n_rows,
            return_field: Arc::new(arrow::datatypes::Field::new(
                "z",
                DataType::FixedSizeBinary(width as i32),
                false,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = out.into_array(n_rows).unwrap();
        let fsb = arr
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        (0..n_rows).map(|i| fsb.value(i).to_vec()).collect()
    }

    fn i64col(v: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(v))
    }

    #[test]
    fn single_i64_ascending() {
        let out = z(vec![i64col(vec![Some(1), Some(2), Some(100)])]);
        assert!(out[0] < out[1]);
        assert!(out[1] < out[2]);
    }

    #[test]
    fn single_i64_negatives() {
        let out = z(vec![i64col(vec![Some(-5), Some(-1), Some(0), Some(3)])]);
        assert!(out[0] < out[1]);
        assert!(out[1] < out[2]);
        assert!(out[2] < out[3]);
    }

    #[test]
    fn single_f64_ordering() {
        let out = z(vec![
            Arc::new(Float64Array::from(vec![Some(-1.5), Some(0.0), Some(2.5)])) as ArrayRef,
        ]);
        assert!(out[0] < out[1]);
        assert!(out[1] < out[2]);
    }

    #[test]
    fn single_utf8_prefix_ordering() {
        let out = z(vec![Arc::new(StringArray::from(vec![
            Some("aaa"),
            Some("aab"),
            Some("ab"),
            Some("aba"),
        ])) as ArrayRef]);
        // "aaa" < "aab"
        assert!(out[0] < out[1]);
        // "ab" < "aba": shorter prefix sorts first
        assert!(out[2] < out[3]);
    }

    #[test]
    fn null_sorts_first() {
        let out = z(vec![i64col(vec![None, Some(-1), Some(5)])]);
        assert!(out[0] < out[1], "null must sort before any non-null here");
        assert!(out[1] < out[2]);
    }

    #[test]
    fn boolean_false_before_true() {
        let out = z(vec![
            Arc::new(BooleanArray::from(vec![Some(false), Some(true)])) as ArrayRef,
        ]);
        assert!(out[0] < out[1]);
    }

    #[test]
    fn two_column_width_and_locality() {
        // Hold column A constant, vary B ascending: the interleaved key must
        // increase, because the high dimension is equal and the low dimension
        // decides.
        let a = i64col(vec![Some(7), Some(7), Some(7)]);
        let b = i64col(vec![Some(1), Some(2), Some(3)]);
        let out = z(vec![a, b]);
        assert_eq!(
            out[0].len(),
            16,
            "two 8-byte columns interleave to 16 bytes"
        );
        assert!(out[0] < out[1]);
        assert!(out[1] < out[2]);
    }

    #[test]
    fn high_bit_wins_across_dimensions() {
        // Morton z-order does NOT make one dimension globally dominant: the
        // single most-significant differing bit across ALL dimensions decides.
        // When column A differs in HIGH bits (1<<40 vs 2<<40) while column B
        // differs only in lower bits (9999 vs 0), A's difference is more
        // significant in the interleaved key, so A decides the order.
        let a = i64col(vec![Some(1 << 40), Some(2 << 40)]);
        let b = i64col(vec![Some(9999), Some(0)]);
        let out = z(vec![a, b]);
        assert!(
            out[0] < out[1],
            "the more-significant differing bit (column A) must decide the order"
        );

        // Conversely, when B's difference sits in more significant bits than A's
        // tiny low-bit difference, B decides: this is expected z-order locality,
        // not dimension precedence.
        let a2 = i64col(vec![Some(1), Some(2)]);
        let b2 = i64col(vec![Some(9999), Some(0)]);
        let out2 = z(vec![a2, b2]);
        assert!(
            out2[0] > out2[1],
            "B's high-bit difference outranks A's low-bit difference (z-order, not precedence)"
        );
    }
}
