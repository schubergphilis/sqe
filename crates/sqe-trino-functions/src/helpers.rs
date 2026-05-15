//! Shared helpers for Trino-compatible UDFs.
//!
//! Both `trino_functions` and `trino_functions_ext` need to lift a per-string
//! transform to a `ColumnarValue`. Previously each file defined its own copy
//! (`string_transform` / `str_transform`) with diverging null semantics. The
//! merged helpers below follow Trino semantics: non-Utf8 inputs collapse to
//! `Utf8(None)` rather than being coerced via `to_string`.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::ColumnarValue;

/// Apply a per-string transform to a single `ColumnarValue`.
///
/// `f` returns `Option<String>`: returning `None` produces a Utf8 null in the
/// output position. Non-Utf8 scalar inputs short-circuit to `Utf8(None)`,
/// matching Trino's null-propagation semantics.
pub(crate) fn str_transform(
    arg: &ColumnarValue,
    f: impl Fn(&str) -> Option<String>,
) -> DFResult<ColumnarValue> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(f(s))))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        ColumnarValue::Array(arr) => {
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;
            let result: StringArray = str_arr.iter().map(|opt| opt.and_then(&f)).collect();
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
        _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
    }
}

/// Apply a per-string transform across two `ColumnarValue`s.
///
/// Supports scalar/scalar and array/scalar combinations. Non-Utf8 inputs
/// collapse to `Utf8(None)` and propagate per-row null when one side is array.
pub(crate) fn str_transform_2(
    args: &[ColumnarValue],
    f: impl Fn(&str, &str) -> Option<String>,
) -> DFResult<ColumnarValue> {
    match (&args[0], &args[1]) {
        (
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s1)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s1))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s2)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s2))),
        ) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(f(s1, s2)))),
        (
            ColumnarValue::Array(arr),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s2)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s2))),
        ) => {
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;
            let result: StringArray = str_arr
                .iter()
                .map(|opt| opt.and_then(|s| f(s, s2)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
        _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;

    #[test]
    fn str_transform_returns_null_for_non_utf8_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Int64(Some(42)));
        let out = str_transform(&input, |s| Some(s.to_uppercase())).unwrap();
        match out {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("expected Utf8(None), got {other:?}"),
        }
    }

    #[test]
    fn str_transform_2_returns_null_for_non_utf8_scalar() {
        let input = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".into()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(42))),
        ];
        let out = str_transform_2(&input, |a, b| Some(format!("{a}{b}"))).unwrap();
        match out {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("expected Utf8(None), got {other:?}"),
        }
    }
}
