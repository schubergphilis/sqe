//! Mask UDF for column hashing.
//!
//! Two modes:
//!
//! - **Keyed (HMAC-SHA256)** with a per-deployment secret loaded from
//!   `coordinator.policy.mask_key` (or `SQE_POLICY__MASK_KEY`).
//! - **Unkeyed (plain SHA-256)** for backwards compatibility with existing
//!   deployments. Unkeyed hashing is vulnerable to offline rainbow-table
//!   attacks against low-entropy values (SSN, phone, employee ID) and
//!   defeats the privacy intent of column masking. Operators should
//!   migrate to keyed mode.
//!
//! The function is registered under the SQL name `sha256` regardless of mode
//! so SELECT statements that already use `sha256(col)` continue to work.
//!
//! Usage:
//!   ctx.register_udf(sha256_udf(None));            // legacy unkeyed
//!   ctx.register_udf(sha256_udf(Some(key_bytes))); // HMAC-SHA256

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tracing::warn;

type HmacSha256 = Hmac<Sha256>;

/// Compute either HMAC-SHA256 (when `key` is `Some`) or plain SHA-256 (when
/// `key` is `None`). The hex digest is returned as a Utf8 string.
fn hash_value(key: Option<&[u8]>, val: &[u8]) -> String {
    match key {
        Some(k) => {
            // new_from_slice never errors for HMAC-SHA256; the API permits
            // any key length so the .expect is correct here.
            let mut mac = HmacSha256::new_from_slice(k)
                .expect("HMAC-SHA256 accepts any key length");
            mac.update(val);
            let result = mac.finalize().into_bytes();
            hex_lower(&result)
        }
        None => {
            let mut hasher = Sha256::new();
            hasher.update(val);
            hex_lower(&hasher.finalize())
        }
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[derive(Debug)]
struct Sha256Func {
    signature: Signature,
    /// HMAC key when set. `None` means legacy unkeyed SHA-256.
    key: Option<Arc<Vec<u8>>>,
}

impl PartialEq for Sha256Func {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
            && match (&self.key, &other.key) {
                (Some(a), Some(b)) => a.as_slice() == b.as_slice(),
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for Sha256Func {}

impl std::hash::Hash for Sha256Func {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
        match &self.key {
            Some(k) => {
                state.write_u8(1);
                k.as_slice().hash(state);
            }
            None => state.write_u8(0),
        }
    }
}

impl Sha256Func {
    fn new(key: Option<Arc<Vec<u8>>>) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
            key,
        }
    }
}

impl ScalarUDFImpl for Sha256Func {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "sha256"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let arg = &args.args[0];
        let key = self.key.as_deref().map(Vec::as_slice);
        match arg {
            ColumnarValue::Array(array) => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(
                            "sha256: expected Utf8 array".to_string(),
                        )
                    })?;

                let result: StringArray = string_array
                    .iter()
                    .map(|opt_val| opt_val.map(|val| hash_value(key, val.as_bytes())))
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            ColumnarValue::Scalar(scalar) => {
                if let datafusion::scalar::ScalarValue::Utf8(Some(val)) = scalar {
                    let hash = hash_value(key, val.as_bytes());
                    Ok(ColumnarValue::Scalar(
                        datafusion::scalar::ScalarValue::Utf8(Some(hash)),
                    ))
                } else if let datafusion::scalar::ScalarValue::Utf8(None) = scalar {
                    Ok(ColumnarValue::Scalar(
                        datafusion::scalar::ScalarValue::Utf8(None),
                    ))
                } else {
                    Err(datafusion::error::DataFusionError::Internal(
                        "sha256: expected Utf8 scalar".to_string(),
                    ))
                }
            }
        }
    }
}

/// Create the sha256 scalar UDF.
///
/// When `key` is `Some`, the UDF computes HMAC-SHA256 with the provided key.
/// When `key` is `None`, it computes plain SHA-256 and logs a one-shot
/// warning so operators know they are running in the legacy unsafe mode.
///
/// The HMAC variant defeats the offline rainbow-table attack against
/// low-entropy column values. Same key must persist across coordinator
/// restarts or query results for the same input will differ across runs.
pub fn sha256_udf(key: Option<Arc<Vec<u8>>>) -> ScalarUDF {
    if key.is_none() {
        warn!(
            "sqe-policy: sha256 column-mask UDF registered without a key. \
             Hashed values are vulnerable to offline brute force on low-entropy \
             columns (SSN, phone, employee ID). Set coordinator.policy.mask_key \
             to enable HMAC-SHA256."
        );
    }
    ScalarUDF::from(Sha256Func::new(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::ColumnarValue;

    fn make_args(args: Vec<ColumnarValue>, num_rows: usize) -> ScalarFunctionArgs {
        let return_field =
            Arc::new(Field::new("sha256", DataType::Utf8, true));
        ScalarFunctionArgs {
            args,
            arg_fields: vec![Arc::new(Field::new("input", DataType::Utf8, true))],
            number_rows: num_rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    #[test]
    fn test_sha256_scalar_unkeyed_matches_legacy() {
        // Unkeyed mode must keep producing the historical SHA-256 digest so
        // existing query results, audit baselines, and dbt seed snapshots
        // don't shift when the UDF is upgraded.
        let func = Sha256Func::new(None);
        let input = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(
            "hello".to_string(),
        )));
        let result = func.invoke_with_args(make_args(vec![input], 1)).unwrap();
        if let ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(hash))) = result {
            assert_eq!(
                hash,
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
            );
        } else {
            panic!("Expected Utf8 scalar");
        }
    }

    #[test]
    fn test_sha256_array_unkeyed_matches_legacy() {
        let func = Sha256Func::new(None);
        let array = Arc::new(StringArray::from(vec![
            Some("hello"),
            None,
            Some("world"),
        ])) as ArrayRef;
        let input = ColumnarValue::Array(array);
        let result = func.invoke_with_args(make_args(vec![input], 3)).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(
                str_arr.value(0),
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
            );
            assert!(str_arr.is_null(1));
            assert!(!str_arr.value(2).is_empty());
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_sha256_null() {
        let func = Sha256Func::new(None);
        let input = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(None));
        let result = func.invoke_with_args(make_args(vec![input], 1)).unwrap();
        if let ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(None)) = result {
            // OK, null in, null out
        } else {
            panic!("Expected null Utf8 scalar");
        }
    }

    /// Regression for issue #37: keyed mode must produce a digest that
    /// differs from the unkeyed digest for the same input, and the keyed
    /// digest must be a known HMAC-SHA256 output so we catch silent
    /// regressions in the underlying crate.
    #[test]
    fn test_sha256_keyed_differs_from_unkeyed() {
        let plain = Sha256Func::new(None);
        let keyed = Sha256Func::new(Some(Arc::new(b"pepper".to_vec())));

        let make_input = || {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(
                "123-45-6789".to_string(),
            )))
        };

        let plain_result = plain.invoke_with_args(make_args(vec![make_input()], 1)).unwrap();
        let keyed_result = keyed.invoke_with_args(make_args(vec![make_input()], 1)).unwrap();

        let plain_hash = match plain_result {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(h))) => h,
            other => panic!("expected Utf8 scalar, got {other:?}"),
        };
        let keyed_hash = match keyed_result {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(h))) => h,
            other => panic!("expected Utf8 scalar, got {other:?}"),
        };

        assert_ne!(plain_hash, keyed_hash, "HMAC must differ from plain SHA-256");
        // RFC 4231-style sanity check: HMAC-SHA256(key="pepper", msg="123-45-6789").
        // Reference computed with `printf 123-45-6789 |
        // openssl dgst -sha256 -mac HMAC -macopt key:pepper`.
        assert_eq!(
            keyed_hash,
            "40966d99be0fda85dac0b2f8d9f00b434b89b0d3e2b3f4bdcbf1dd31a2b7092f",
            "HMAC-SHA256 output drift"
        );
    }

    #[test]
    fn test_sha256_keyed_deterministic_within_run() {
        let func = Sha256Func::new(Some(Arc::new(b"k".to_vec())));
        let input1 = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(
            "x".to_string(),
        )));
        let input2 = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(
            "x".to_string(),
        )));
        let r1 = func.invoke_with_args(make_args(vec![input1], 1)).unwrap();
        let r2 = func.invoke_with_args(make_args(vec![input2], 1)).unwrap();
        let h1 = match r1 {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(h))) => h,
            _ => panic!(),
        };
        let h2 = match r2 {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(h))) => h,
            _ => panic!(),
        };
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_sha256_keyed_different_keys_produce_different_digests() {
        let func_a = Sha256Func::new(Some(Arc::new(b"key-a".to_vec())));
        let func_b = Sha256Func::new(Some(Arc::new(b"key-b".to_vec())));
        let input_a = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(
            "value".to_string(),
        )));
        let input_b = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(
            "value".to_string(),
        )));
        let ha = match func_a.invoke_with_args(make_args(vec![input_a], 1)).unwrap() {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(h))) => h,
            _ => panic!(),
        };
        let hb = match func_b.invoke_with_args(make_args(vec![input_b], 1)).unwrap() {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(h))) => h,
            _ => panic!(),
        };
        assert_ne!(ha, hb);
    }
}
