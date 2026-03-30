//! SHA-256 scalar UDF for column masking.
//!
//! DataFusion does not ship a built-in sha256 function. This UDF provides
//! one for use in column mask expressions. Register it on the SessionContext
//! before executing queries that use hash-based column masking.
//!
//! Usage:
//!   ctx.register_udf(sha256_udf::sha256_udf());
//!   -- then in SQL: SELECT sha256(ssn) FROM employees

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use sha2::{Digest, Sha256};

#[derive(Debug, PartialEq, Eq, Hash)]
struct Sha256Func {
    signature: Signature,
}

impl Sha256Func {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
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
                    .map(|opt_val| {
                        opt_val.map(|val| {
                            let mut hasher = Sha256::new();
                            hasher.update(val.as_bytes());
                            format!("{:x}", hasher.finalize())
                        })
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            ColumnarValue::Scalar(scalar) => {
                if let datafusion::scalar::ScalarValue::Utf8(Some(val)) = scalar {
                    let mut hasher = Sha256::new();
                    hasher.update(val.as_bytes());
                    let hash = format!("{:x}", hasher.finalize());
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

/// Create the sha256 scalar UDF. Register on a SessionContext with:
/// ```ignore
/// ctx.register_udf(sha256_udf());
/// ```
pub fn sha256_udf() -> ScalarUDF {
    ScalarUDF::from(Sha256Func::new())
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
    fn test_sha256_scalar() {
        let func = Sha256Func::new();
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
    fn test_sha256_array() {
        let func = Sha256Func::new();
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
        let func = Sha256Func::new();
        let input = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(None));
        let result = func.invoke_with_args(make_args(vec![input], 1)).unwrap();
        if let ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(None)) = result {
            // OK - null in, null out
        } else {
            panic!("Expected null Utf8 scalar");
        }
    }
}
