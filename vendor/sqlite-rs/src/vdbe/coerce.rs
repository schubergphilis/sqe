// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Text-to-numeric coercion and checked arithmetic (spec 008,
//! Requirement 5). Integer overflow promotes to REAL rather than
//! silently wrapping — the CVE-2025-29087/3277 class.

use crate::format::format_real;
use crate::record::Value;

/// Locates the longest valid numeric prefix of `s`: optional leading
/// whitespace, optional sign, digits, an optional decimal point and
/// digits, and an optional exponent. Returns the byte range of the
/// literal and whether it is float-shaped (has a `.` or exponent), or
/// `None` if no numeric prefix exists at all.
/// Advances `pos` past a run of bytes matching `pred`, returning the
/// count consumed.
fn skip_while(b: &[u8], pos: &mut usize, pred: impl Fn(u8) -> bool) -> usize {
    let start = *pos;
    while let Some(&c) = b.get(*pos) {
        if !pred(c) {
            break;
        }
        *pos = pos.saturating_add(1);
    }
    pos.saturating_sub(start)
}

fn scan_number_prefix(s: &str) -> Option<(usize, usize, bool)> {
    let b = s.as_bytes();
    let mut i = 0;
    skip_while(b, &mut i, |c| c.is_ascii_whitespace());
    let start = i;
    if matches!(b.get(i), Some(b'+') | Some(b'-')) {
        i = i.saturating_add(1);
    }
    let int_len = skip_while(b, &mut i, |c| c.is_ascii_digit());
    let mut end = i;
    let mut is_float = false;
    if b.get(end) == Some(&b'.') {
        let mut j = end.saturating_add(1);
        let frac_len = skip_while(b, &mut j, |c| c.is_ascii_digit());
        if int_len > 0 || frac_len > 0 {
            is_float = true;
            end = j;
        }
    }
    if int_len == 0 && !is_float {
        return None;
    }
    if matches!(b.get(end), Some(b'e') | Some(b'E')) {
        let mut j = end.saturating_add(1);
        if matches!(b.get(j), Some(b'+') | Some(b'-')) {
            j = j.saturating_add(1);
        }
        let exp_digits = skip_while(b, &mut j, |c| c.is_ascii_digit());
        if exp_digits > 0 {
            end = j;
            is_float = true;
        }
    }
    Some((start, end, is_float))
}

/// Coerces `s` to a numeric `Value` by parsing its longest valid numeric
/// prefix; a non-numeric or empty string coerces to `0`.
pub fn coerce_text_to_numeric(s: &str) -> Value {
    let Some((start, end, is_float)) = scan_number_prefix(s) else {
        return Value::Integer(0);
    };
    let literal = &s[start..end];
    if is_float {
        return literal
            .parse::<f64>()
            .map_or(Value::Integer(0), Value::Real);
    }
    match literal.parse::<i64>() {
        Ok(i) => Value::Integer(i),
        Err(_) => literal
            .parse::<f64>()
            .map_or(Value::Integer(0), Value::Real),
    }
}

fn as_numeric(v: &Value) -> Value {
    match v {
        Value::Integer(_) | Value::Real(_) => v.clone(),
        Value::Text(s) => coerce_text_to_numeric(s),
        Value::Null | Value::Blob(_) => Value::Integer(0),
    }
}

#[allow(clippy::cast_precision_loss)]
fn to_f64(v: &Value) -> f64 {
    match v {
        Value::Integer(i) => *i as f64,
        Value::Real(r) => *r,
        _ => 0.0,
    }
}

fn arith(
    a: &Value,
    b: &Value,
    int_op: fn(i64, i64) -> Option<i64>,
    float_op: fn(f64, f64) -> f64,
) -> Value {
    match (as_numeric(a), as_numeric(b)) {
        (Value::Integer(x), Value::Integer(y)) => match int_op(x, y) {
            Some(v) => Value::Integer(v),
            None => Value::Real(float_op(x as f64, y as f64)),
        },
        (x, y) => Value::Real(float_op(to_f64(&x), to_f64(&y))),
    }
}

/// Adds two values, coercing text operands numerically. Overflow
/// promotes to REAL rather than wrapping.
pub fn checked_add(a: &Value, b: &Value) -> Value {
    arith(a, b, i64::checked_add, |x, y| x + y)
}

/// Subtracts two values, coercing text operands numerically. Overflow
/// promotes to REAL rather than wrapping.
pub fn checked_sub(a: &Value, b: &Value) -> Value {
    arith(a, b, i64::checked_sub, |x, y| x - y)
}

/// Multiplies two values, coercing text operands numerically. Overflow
/// promotes to REAL rather than wrapping.
pub fn checked_mul(a: &Value, b: &Value) -> Value {
    arith(a, b, i64::checked_mul, |x, y| x * y)
}

/// Divides two values, coercing text operands numerically. Integer
/// division by zero yields NULL (SQLite's rule, not a panic or an
/// error); `i64::MIN / -1` overflows and promotes to REAL like any other
/// arithmetic overflow.
pub fn checked_div(a: &Value, b: &Value) -> Value {
    match (as_numeric(a), as_numeric(b)) {
        (Value::Integer(_), Value::Integer(0)) => Value::Null,
        (Value::Integer(x), Value::Integer(y)) => match x.checked_div(y) {
            Some(v) => Value::Integer(v),
            None => Value::Real(x as f64 / y as f64),
        },
        (x, y) => {
            let (x, y) = (to_f64(&x), to_f64(&y));
            if y == 0.0 {
                Value::Null
            } else {
                Value::Real(x / y)
            }
        }
    }
}

/// Computes the remainder of two values, coercing text operands
/// numerically. Integer remainder by zero yields NULL, matching
/// `checked_div`'s divide-by-zero rule; non-integer operands are
/// truncated to integer first, per SQLite's `%` operator semantics.
pub fn checked_rem(a: &Value, b: &Value) -> Value {
    let (na, nb) = (as_numeric(a), as_numeric(b));
    // `%` truncates both operands to INTEGER for the modulo itself
    // (oracle: `7%2.5` computes as `7%2`), but — like every other
    // arithmetic operator — the *result* promotes to REAL if either
    // operand was REAL (oracle: `typeof(7%2.5)` is `real`, value `1.0`,
    // not the integer `1` a naive integer-only remainder would give).
    let is_real = matches!(na, Value::Real(_)) || matches!(nb, Value::Real(_));
    let (x, y) = (cast_to_integer(&na), cast_to_integer(&nb));
    if y == 0 {
        return Value::Null;
    }
    // i64::MIN % -1 == 0, no overflow needed but checked_rem is conservative.
    let result = x.checked_rem(y).unwrap_or_default();
    #[allow(clippy::cast_precision_loss)]
    if is_real {
        Value::Real(result as f64)
    } else {
        Value::Integer(result)
    }
}

/// `CAST(... AS INTEGER)`: truncates a REAL toward zero rather than
/// rounding or flooring.
#[allow(clippy::cast_possible_truncation)]
pub fn cast_to_integer(v: &Value) -> i64 {
    match v {
        Value::Integer(i) => *i,
        Value::Real(r) => r.trunc() as i64,
        Value::Text(s) => match coerce_text_to_numeric(s) {
            Value::Integer(i) => i,
            Value::Real(r) => r.trunc() as i64,
            _ => 0,
        },
        Value::Null | Value::Blob(_) => 0,
    }
}

/// Bitwise AND, coercing both operands to INTEGER first (SQLite's rule
/// for `&`/`|`/`<<`/`>>` — no REAL path, unlike arithmetic).
pub fn bit_and(a: &Value, b: &Value) -> Value {
    Value::Integer(cast_to_integer(&as_numeric(a)) & cast_to_integer(&as_numeric(b)))
}

/// Bitwise OR, coercing both operands to INTEGER first.
pub fn bit_or(a: &Value, b: &Value) -> Value {
    Value::Integer(cast_to_integer(&as_numeric(a)) | cast_to_integer(&as_numeric(b)))
}

/// Bitwise NOT (`~x`), coercing the operand to INTEGER first.
pub fn bit_not(a: &Value) -> Value {
    Value::Integer(!cast_to_integer(&as_numeric(a)))
}

/// Shifts `a` left/right by `b` bits, both coerced to INTEGER first.
/// Matches SQLite's `vdbe.c` `OP_ShiftLeft`/`OP_ShiftRight` handling: a
/// negative shift amount reverses direction, and a magnitude of 64 or
/// more collapses to 0 (non-negative operand) or -1 (negative operand)
/// rather than relying on Rust's shift-amount-in-range requirement.
#[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
fn shift(a: i64, b: i64, mut left: bool) -> i64 {
    let mut n = b;
    if n < 0 {
        n = if n > -64 { n.wrapping_neg() } else { 64 };
        left = !left;
    }
    if n >= 64 {
        return if a >= 0 { 0 } else { -1 };
    }
    let ua = a as u64;
    let result = if left {
        ua << n
    } else if a >= 0 {
        ua >> n
    } else {
        !(!ua >> n)
    };
    result as i64
}

/// Left shift (`<<`), coercing both operands to INTEGER first.
pub fn shift_left(a: &Value, b: &Value) -> Value {
    let (x, y) = (
        cast_to_integer(&as_numeric(a)),
        cast_to_integer(&as_numeric(b)),
    );
    Value::Integer(shift(x, y, true))
}

/// Right shift (`>>`), coercing both operands to INTEGER first.
pub fn shift_right(a: &Value, b: &Value) -> Value {
    let (x, y) = (
        cast_to_integer(&as_numeric(a)),
        cast_to_integer(&as_numeric(b)),
    );
    Value::Integer(shift(x, y, false))
}

/// Renders `v` as `CAST(v AS TEXT)` would, for `||` operands.
fn as_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Integer(i) => i.to_string(),
        Value::Real(r) => format_real(*r),
        Value::Text(s) => s.to_string(),
        Value::Blob(b) => String::from_utf8_lossy(b).into_owned(),
    }
}

/// String concatenation (`||`): both operands coerce to TEXT: NULL
/// propagation is handled by the caller (`arithmetic::binary_op`), same
/// as every other binary opcode.
pub fn concat(a: &Value, b: &Value) -> Value {
    Value::Text(format!("{}{}", as_text(a), as_text(b)).into())
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn coercion_parses_longest_valid_numeric_prefix() {
        assert_eq!(coerce_text_to_numeric("123abc"), Value::Integer(123));
        assert_eq!(coerce_text_to_numeric("  123  "), Value::Integer(123));
        assert_eq!(coerce_text_to_numeric("abc"), Value::Integer(0));
        assert_eq!(coerce_text_to_numeric(""), Value::Integer(0));
        assert_eq!(coerce_text_to_numeric("0x10"), Value::Integer(0));
        assert_eq!(coerce_text_to_numeric(".5"), Value::Real(0.5));
        assert_eq!(coerce_text_to_numeric("1e3"), Value::Real(1000.0));
        assert_eq!(coerce_text_to_numeric("1e+3"), Value::Real(1000.0));
        assert_eq!(coerce_text_to_numeric("1e-3"), Value::Real(0.001));
    }

    #[test]
    fn arithmetic_matches_oracle_coercion_vectors() {
        assert_eq!(
            checked_add(
                &Value::Text("123abc".to_string().into()),
                &Value::Integer(1)
            ),
            Value::Integer(124)
        );
        assert_eq!(
            checked_add(
                &Value::Text("  123  ".to_string().into()),
                &Value::Integer(1)
            ),
            Value::Integer(124)
        );
        assert_eq!(
            checked_add(&Value::Text("abc".to_string().into()), &Value::Integer(1)),
            Value::Integer(1)
        );
    }

    #[test]
    fn integer_overflow_promotes_to_real_never_wraps() {
        let max = Value::Integer(i64::MAX);
        match checked_add(&max, &Value::Integer(1)) {
            Value::Real(r) => assert!((r - 9_223_372_036_854_775_808.0).abs() < 1.0),
            other => panic!("expected REAL promotion, got {other:?}"),
        }
        match checked_mul(&max, &Value::Integer(2)) {
            Value::Real(r) => assert!(r > i64::MAX as f64),
            other => panic!("expected REAL promotion, got {other:?}"),
        }
    }

    #[test]
    fn cast_to_integer_truncates_toward_zero() {
        assert_eq!(cast_to_integer(&Value::Real(3.9)), 3);
        assert_eq!(cast_to_integer(&Value::Real(-3.9)), -3);
        assert_eq!(cast_to_integer(&Value::Integer(7)), 7);
        assert_eq!(
            cast_to_integer(&Value::Text("12abc".to_string().into())),
            12
        );
        assert_eq!(
            cast_to_integer(&Value::Text("3.9abc".to_string().into())),
            3
        );
        assert_eq!(cast_to_integer(&Value::Null), 0);
        assert_eq!(cast_to_integer(&Value::Blob(vec![1, 2, 3].into())), 0);
    }

    #[test]
    fn checked_sub_matches_oracle_coercion_vectors() {
        assert_eq!(
            checked_sub(&Value::Integer(5), &Value::Integer(3)),
            Value::Integer(2)
        );
    }

    #[test]
    fn bitwise_and_or_not_match_oracle_vectors() {
        assert_eq!(
            bit_and(&Value::Integer(5), &Value::Integer(3)),
            Value::Integer(1)
        );
        assert_eq!(
            bit_or(&Value::Integer(5), &Value::Integer(3)),
            Value::Integer(7)
        );
        assert_eq!(bit_not(&Value::Integer(5)), Value::Integer(-6));
        assert_eq!(bit_not(&Value::Integer(0)), Value::Integer(-1));
        assert_eq!(bit_not(&Value::Integer(-7)), Value::Integer(6));
    }

    #[test]
    fn shift_matches_oracle_vectors() {
        assert_eq!(
            shift_left(&Value::Integer(5), &Value::Integer(1)),
            Value::Integer(10)
        );
        assert_eq!(
            shift_left(&Value::Integer(0), &Value::Integer(1)),
            Value::Integer(0)
        );
        assert_eq!(
            shift_left(&Value::Integer(-7), &Value::Integer(1)),
            Value::Integer(-14)
        );
        assert_eq!(
            shift_right(&Value::Integer(5), &Value::Integer(1)),
            Value::Integer(2)
        );
        assert_eq!(
            shift_right(&Value::Integer(0), &Value::Integer(1)),
            Value::Integer(0)
        );
        assert_eq!(
            shift_right(&Value::Integer(-7), &Value::Integer(1)),
            Value::Integer(-4)
        );
    }

    #[test]
    fn shift_handles_negative_and_oversized_amounts() {
        // Negative shift amount reverses direction (SQLite vdbe.c rule).
        assert_eq!(
            shift_left(&Value::Integer(5), &Value::Integer(-1)),
            Value::Integer(2)
        );
        assert_eq!(
            shift_right(&Value::Integer(5), &Value::Integer(-1)),
            Value::Integer(10)
        );
        // Magnitude >= 64 collapses to 0/-1 by sign.
        assert_eq!(
            shift_left(&Value::Integer(5), &Value::Integer(64)),
            Value::Integer(0)
        );
        assert_eq!(
            shift_right(&Value::Integer(-5), &Value::Integer(64)),
            Value::Integer(-1)
        );
    }

    #[test]
    fn concat_coerces_to_text_and_propagates_no_null_itself() {
        assert_eq!(
            concat(
                &Value::Text("apple".to_string().into()),
                &Value::Text("x".to_string().into())
            ),
            Value::Text("applex".to_string().into())
        );
        assert_eq!(
            concat(&Value::Integer(1), &Value::Real(2.5)),
            Value::Text("12.5".to_string().into())
        );
    }

    #[test]
    fn arithmetic_on_real_operands_avoids_integer_path() {
        assert_eq!(
            checked_add(&Value::Real(1.5), &Value::Real(2.5)),
            Value::Real(4.0)
        );
        assert_eq!(
            checked_mul(&Value::Null, &Value::Integer(5)),
            Value::Integer(0)
        );
    }
}
