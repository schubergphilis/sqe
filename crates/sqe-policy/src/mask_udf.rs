//! Hive-style partial-masking UDF for column masking.
//!
//! Implements Ranger's MASK / MASK_SHOW_LAST_4 / MASK_SHOW_FIRST_4 vocabulary:
//! keep the first `show_first` and last `show_last` characters visible; replace
//! every other character using char-class substitution:
//!   - ASCII uppercase -> `upper`
//!   - ASCII lowercase -> `lower`
//!   - ASCII digit     -> `digit`
//!   - anything else   -> left unchanged (punctuation, spaces, non-ASCII)
//!
//! Counting is by Unicode scalar (chars), matching Hive's behaviour.
//!
//! Usage:
//!   // Ranger MASK_SHOW_LAST_4: keep last 4, mask everything else with 'x'.
//!   ctx.register_udf(mask_partial_udf(0, 4, 'x', 'x', 'x'));
//!
//!   // Ranger MASK_SHOW_FIRST_4
//!   ctx.register_udf(mask_partial_udf(4, 0, 'x', 'x', 'x'));
//!
//!   // Ranger MASK (Hive defaults: upper->X, lower->x, digit->n)
//!   ctx.register_udf(mask_partial_udf(0, 0, 'X', 'x', 'n'));

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Mask `s` Hive-style: keep the first `show_first` and last `show_last`
/// characters; for every other character, ASCII uppercase->`upper`,
/// ASCII lowercase->`lower`, ASCII digit->`digit`, anything else unchanged.
/// Counts by Unicode scalar (chars). If show_first+show_last >= len, all shown.
fn mask_str(
    s: &str,
    show_first: usize,
    show_last: usize,
    upper: char,
    lower: char,
    digit: char,
) -> String {
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len();
    chars
        .iter()
        .enumerate()
        .map(|(i, &c)| {
            let shown = i < show_first || i >= n.saturating_sub(show_last);
            if shown {
                c
            } else if c.is_alphabetic() {
                // Unicode-aware: mask non-ASCII letters too (accented, Cyrillic,
                // CJK, etc.). Caseless scripts report is_uppercase() == false and
                // map to `lower`. ASCII-only checks here leaked non-Latin PII raw.
                if c.is_uppercase() {
                    upper
                } else {
                    lower
                }
            } else if c.is_numeric() {
                digit
            } else {
                // Punctuation, whitespace, symbols pass through (Hive behavior).
                c
            }
        })
        .collect()
}

#[derive(Debug)]
struct MaskPartialFunc {
    signature: Signature,
    show_first: u32,
    show_last: u32,
    upper: char,
    lower: char,
    digit: char,
}

impl PartialEq for MaskPartialFunc {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
            && self.show_first == other.show_first
            && self.show_last == other.show_last
            && self.upper == other.upper
            && self.lower == other.lower
            && self.digit == other.digit
    }
}

impl Eq for MaskPartialFunc {}

impl std::hash::Hash for MaskPartialFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
        self.show_first.hash(state);
        self.show_last.hash(state);
        self.upper.hash(state);
        self.lower.hash(state);
        self.digit.hash(state);
    }
}

impl MaskPartialFunc {
    fn new(show_first: u32, show_last: u32, upper: char, lower: char, digit: char) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
            show_first,
            show_last,
            upper,
            lower,
            digit,
        }
    }
}

impl ScalarUDFImpl for MaskPartialFunc {
    fn name(&self) -> &str {
        "sqe_mask_partial"
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
        let show_first = self.show_first as usize;
        let show_last = self.show_last as usize;
        let upper = self.upper;
        let lower = self.lower;
        let digit = self.digit;

        match arg {
            ColumnarValue::Array(array) => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(
                            "sqe_mask_partial: expected Utf8 array".to_string(),
                        )
                    })?;

                let result: StringArray = string_array
                    .iter()
                    .map(|opt_val| {
                        opt_val
                            .map(|val| mask_str(val, show_first, show_last, upper, lower, digit))
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            ColumnarValue::Scalar(scalar) => {
                if let datafusion::scalar::ScalarValue::Utf8(Some(val)) = scalar {
                    let masked = mask_str(val, show_first, show_last, upper, lower, digit);
                    Ok(ColumnarValue::Scalar(
                        datafusion::scalar::ScalarValue::Utf8(Some(masked)),
                    ))
                } else if let datafusion::scalar::ScalarValue::Utf8(None) = scalar {
                    Ok(ColumnarValue::Scalar(
                        datafusion::scalar::ScalarValue::Utf8(None),
                    ))
                } else {
                    Err(datafusion::error::DataFusionError::Internal(
                        "sqe_mask_partial: expected Utf8 scalar".to_string(),
                    ))
                }
            }
        }
    }
}

/// Create the sqe_mask_partial scalar UDF.
///
/// Keeps the first `show_first` and last `show_last` characters of the input
/// string visible. Every other character is replaced by char-class:
///   - ASCII uppercase -> `upper`
///   - ASCII lowercase -> `lower`
///   - ASCII digit     -> `digit`
///   - punctuation / non-ASCII -> left as-is
///
/// This realises Ranger's MASK, MASK_SHOW_LAST_4, and MASK_SHOW_FIRST_4
/// mask types. Multiple calls with different params produce distinct UDF
/// instances (PartialEq / Hash include all five params) so DataFusion CSE
/// never conflates them.
pub fn mask_partial_udf(
    show_first: u32,
    show_last: u32,
    upper: char,
    lower: char,
    digit: char,
) -> ScalarUDF {
    ScalarUDF::from(MaskPartialFunc::new(show_first, show_last, upper, lower, digit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::ColumnarValue;

    // ---- pure-fn tests -------------------------------------------------------

    #[test]
    fn show_last_4_on_ssn() {
        // MASK_SHOW_LAST_4 template: mask upper/lower/digit all to 'x', keep
        // punctuation + last 4.
        assert_eq!(mask_str("111-11-1111", 0, 4, 'x', 'x', 'x'), "xxx-xx-1111");
    }

    #[test]
    fn show_first_4() {
        assert_eq!(mask_str("abcdefgh", 4, 0, 'x', 'x', 'x'), "abcdxxxx");
    }

    #[test]
    fn full_mask_hive_defaults() {
        // MASK template: Hive defaults upper->X, lower->x, digit->n,
        // punctuation kept. "Ab9-z" -> "Xxn-x".
        assert_eq!(mask_str("Ab9-z", 0, 0, 'X', 'x', 'n'), "Xxn-x");
    }

    #[test]
    fn show_n_longer_than_string_keeps_all() {
        assert_eq!(mask_str("ab", 0, 4, 'x', 'x', 'x'), "ab");
    }

    #[test]
    fn empty_string() {
        assert_eq!(mask_str("", 0, 4, 'x', 'x', 'x'), "");
    }

    #[test]
    fn overlap_first_and_last_all_shown() {
        // show_first 2 + show_last 2 on a 3-char string => all shown.
        assert_eq!(mask_str("abc", 2, 2, 'x', 'x', 'x'), "abc");
    }

    #[test]
    fn full_mask_hides_non_ascii_letters() {
        // Regression: ASCII-only char classes left non-Latin PII unmasked.
        // A full MASK over Cyrillic must not return the original characters.
        let masked = mask_str("Иван", 0, 0, 'X', 'x', 'n');
        assert_ne!(masked, "Иван", "non-ASCII letters must be masked");
        assert!(
            !masked.chars().any(|c| c.is_alphabetic() && c != 'X' && c != 'x'),
            "every letter must be replaced by a mask char, got {masked:?}"
        );
    }

    #[test]
    fn full_mask_hides_cjk_and_keeps_show_last() {
        // CJK has no case -> maps to `lower`. show_last keeps the tail visible.
        let masked = mask_str("北京1", 0, 1, 'X', 'x', 'n');
        assert!(masked.ends_with('1'), "show_last tail kept: {masked:?}");
        assert!(!masked.contains('北') && !masked.contains('京'), "CJK masked: {masked:?}");
    }

    #[test]
    fn full_mask_keeps_ascii_punctuation_and_behaviour() {
        // Existing ASCII behaviour is preserved by the Unicode-aware logic.
        assert_eq!(mask_str("Ab9-z", 0, 0, 'X', 'x', 'n'), "Xxn-x");
    }

    // ---- UDF-level tests -----------------------------------------------------

    fn make_args(args: Vec<ColumnarValue>, num_rows: usize) -> ScalarFunctionArgs {
        let return_field = Arc::new(Field::new("sqe_mask_partial", DataType::Utf8, true));
        ScalarFunctionArgs {
            args,
            arg_fields: vec![Arc::new(Field::new("input", DataType::Utf8, true))],
            number_rows: num_rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    #[test]
    fn test_array_path_show_last_4() {
        // Input: ["111-11-1111", None, "AB12cd"]
        // show_last=4, upper/lower/digit all 'x'
        // "111-11-1111": show last 4 = "1111", mask rest -> "xxx-xx-1111"
        // None           -> None
        // "AB12cd":      show last 4 = "12cd", mask "AB" -> "xx" -> "xx12cd"
        let func = MaskPartialFunc::new(0, 4, 'x', 'x', 'x');
        let array = Arc::new(StringArray::from(vec![
            Some("111-11-1111"),
            None,
            Some("AB12cd"),
        ])) as ArrayRef;
        let input = ColumnarValue::Array(array);
        let result = func.invoke_with_args(make_args(vec![input], 3)).unwrap();
        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(str_arr.value(0), "xxx-xx-1111");
            assert!(str_arr.is_null(1));
            assert_eq!(str_arr.value(2), "xx12cd");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_scalar_utf8_path() {
        let func = MaskPartialFunc::new(0, 4, 'x', 'x', 'x');
        let input = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(
            "111-11-1111".to_string(),
        )));
        let result = func.invoke_with_args(make_args(vec![input], 1)).unwrap();
        if let ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(val))) = result {
            assert_eq!(val, "xxx-xx-1111");
        } else {
            panic!("Expected Utf8 scalar result");
        }
    }

    #[test]
    fn test_scalar_null_in_null_out() {
        let func = MaskPartialFunc::new(0, 4, 'x', 'x', 'x');
        let input = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(None));
        let result = func.invoke_with_args(make_args(vec![input], 1)).unwrap();
        if let ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(None)) = result {
            // null in, null out — correct
        } else {
            panic!("Expected null Utf8 scalar");
        }
    }

    #[test]
    fn test_inequality_show_first_vs_show_last() {
        let show_last = MaskPartialFunc::new(0, 4, 'x', 'x', 'x');
        let show_first = MaskPartialFunc::new(4, 0, 'x', 'x', 'x');
        assert_ne!(show_last, show_first);
    }

    #[test]
    fn test_inequality_different_mask_chars() {
        let a = MaskPartialFunc::new(0, 4, 'x', 'x', 'x');
        let b = MaskPartialFunc::new(0, 4, 'X', 'x', 'n');
        assert_ne!(a, b);
    }

    #[test]
    fn test_pub_constructor_returns_scalar_udf() {
        // Smoke test: the public constructor compiles and wraps correctly.
        let udf = mask_partial_udf(0, 4, 'x', 'x', 'x');
        assert_eq!(udf.name(), "sqe_mask_partial");
    }
}
