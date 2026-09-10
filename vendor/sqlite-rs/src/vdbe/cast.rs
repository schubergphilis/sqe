// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `CAST` value conversion (spec 008, its own design study distinct from
//! column-affinity coercion — #142): SQLite's `sqlite3VdbeMemCast`.
//! Unlike [`crate::vdbe::affinity::apply_affinity`] (which only converts
//! a *fully well-formed* numeric string, or forces an `Integer` into
//! `Real` for REAL affinity, and never touches BLOB), `CAST` converts
//! every source type to the target affinity's storage class, parses
//! only the *longest numeric prefix* of a string (`'123abc'` casts to
//! `123`, not left as text), and never errors — an unconvertible source
//! becomes `0`, `0.0`, or an empty/verbatim string as the target
//! demands. Every rule below is oracle-verified (`sqlite3 :memory:`
//! against the pinned 3.53.4 build), not paraphrased from memory, per
//! this ticket's own instruction.

use crate::format::format_real;
use crate::record::Value;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::coerce::coerce_text_to_numeric;

/// Casts `value` to `target`'s storage class. `NULL` casts to `NULL`
/// under every target — the one rule common to all five arms.
pub fn cast_to(value: &Value, target: Affinity) -> Value {
    if matches!(value, Value::Null) {
        return Value::Null;
    }
    match target {
        Affinity::Text => cast_to_text(value),
        Affinity::Blob => cast_to_blob(value),
        Affinity::Integer => cast_to_integer(value),
        Affinity::Real => cast_to_real(value),
        Affinity::Numeric => cast_to_numeric(value),
    }
}

/// `CAST(x AS TEXT)`: numbers render as their decimal text; a blob's
/// raw bytes are reinterpreted as text verbatim (oracle: `CAST(x'4142'
/// AS TEXT)` = `'AB'`); text is unchanged.
fn cast_to_text(value: &Value) -> Value {
    match value {
        Value::Text(_) => value.clone(),
        Value::Integer(i) => Value::Text(i.to_string().into()),
        Value::Real(r) => Value::Text(format_real(*r).into()),
        Value::Blob(bytes) => Value::Text(String::from_utf8_lossy(bytes).into_owned().into()),
        Value::Null => Value::Null,
    }
}

/// `CAST(x AS BLOB)`: the mirror of [`cast_to_text`] — anything that
/// isn't already a blob first renders to its text form, then that
/// text's raw bytes become the blob (oracle: `CAST(5 AS BLOB)` = the
/// 1-byte blob `'5'`).
fn cast_to_blob(value: &Value) -> Value {
    match value {
        Value::Blob(_) => value.clone(),
        Value::Text(s) => Value::Blob(s.as_bytes().into()),
        Value::Integer(i) => Value::Blob(i.to_string().into_bytes().into()),
        Value::Real(r) => Value::Blob(format_real(*r).into_bytes().into()),
        Value::Null => Value::Null,
    }
}

/// `CAST(x AS INTEGER)`: a REAL truncates toward zero, saturating at
/// `i64::MIN`/`MAX` for out-of-range magnitudes (oracle: `CAST(1e300 AS
/// INTEGER)` = `9223372036854775807`) — Rust's `as i64` float cast
/// already saturates rather than invoking UB, so no extra clamping is
/// needed. Text/blob take the longest numeric prefix (`'123abc'` → 123,
/// `'apple'` → 0, `x'4142'` → textified `"AB"` → 0 — no numeric prefix).
fn cast_to_integer(value: &Value) -> Value {
    match value {
        Value::Integer(_) => value.clone(),
        Value::Real(r) => Value::Integer(real_to_i64(*r)),
        Value::Text(s) => integer_from_numeric(coerce_text_to_numeric(s)),
        Value::Blob(bytes) => {
            integer_from_numeric(coerce_text_to_numeric(&String::from_utf8_lossy(bytes)))
        }
        Value::Null => Value::Null,
    }
}

fn integer_from_numeric(numeric: Value) -> Value {
    match numeric {
        Value::Integer(i) => Value::Integer(i),
        Value::Real(r) => Value::Integer(real_to_i64(r)),
        _ => Value::Integer(0), // unreachable: coerce_text_to_numeric only returns Integer/Real
    }
}

#[allow(clippy::cast_possible_truncation)]
fn real_to_i64(r: f64) -> i64 {
    r as i64
}

/// `CAST(x AS REAL)`: an INTEGER converts exactly (`5` → `5.0`, even
/// `i64::MAX` — oracle: `9223372036854775807.0` widens lossily to
/// `9.2233720368547758e18`, which is just what `as f64` already does).
/// Text/blob take the longest numeric prefix, same as
/// [`cast_to_integer`], but always land as a float.
fn cast_to_real(value: &Value) -> Value {
    match value {
        Value::Real(_) => value.clone(),
        #[allow(clippy::cast_precision_loss)]
        Value::Integer(i) => Value::Real(*i as f64),
        Value::Text(s) => real_from_numeric(coerce_text_to_numeric(s)),
        Value::Blob(bytes) => {
            real_from_numeric(coerce_text_to_numeric(&String::from_utf8_lossy(bytes)))
        }
        Value::Null => Value::Null,
    }
}

fn real_from_numeric(numeric: Value) -> Value {
    match numeric {
        Value::Real(r) => Value::Real(r),
        #[allow(clippy::cast_precision_loss)]
        Value::Integer(i) => Value::Real(i as f64),
        _ => Value::Real(0.0), // unreachable: coerce_text_to_numeric only returns Integer/Real
    }
}

/// `CAST(x AS NUMERIC)`: an already-numeric value (INTEGER or REAL)
/// passes through untouched — oracle: `CAST(5.0 AS NUMERIC)` stays
/// `5.0` (real), it is NOT downgraded to `5`. Text/blob take the
/// longest numeric prefix and, unlike [`cast_to_real`], DO get
/// downgraded to INTEGER when the parsed value has no fractional part
/// (oracle: `CAST('5.0' AS NUMERIC)` = `5`, typeof `integer`) — SQLite's
/// `applyNumericAffinity` rule, applied only to the text→number
/// conversion step, never to a value that was already numeric.
fn cast_to_numeric(value: &Value) -> Value {
    match value {
        Value::Integer(_) | Value::Real(_) => value.clone(),
        Value::Text(s) => downgrade_whole_reals(coerce_text_to_numeric(s)),
        Value::Blob(bytes) => {
            downgrade_whole_reals(coerce_text_to_numeric(&String::from_utf8_lossy(bytes)))
        }
        Value::Null => Value::Null,
    }
}

fn downgrade_whole_reals(numeric: Value) -> Value {
    match numeric {
        Value::Real(r) if r.fract() == 0.0 && r.is_finite() && in_i64_range(r) => {
            Value::Integer(real_to_i64(r))
        }
        other => other,
    }
}

/// Whether `r` falls within `i64`'s representable range — guards the
/// `as i64` downgrade above, mirroring `src/vdbe/control.rs`'s identical
/// guard for `MustBeInt`.
fn in_i64_range(r: f64) -> bool {
    (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&r)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn text(s: &str) -> Value {
        Value::Text(s.into())
    }

    #[test]
    fn cast_to_integer_matches_oracle_truth_table() {
        // Each pair is a `sqlite3 :memory:` query verified against the
        // pinned 3.53.4 oracle (see this module's doc comment).
        assert_eq!(
            cast_to(&text("apple"), Affinity::Integer),
            Value::Integer(0)
        );
        assert_eq!(
            cast_to(&text("123abc"), Affinity::Integer),
            Value::Integer(123)
        );
        assert_eq!(
            cast_to(&text("  42  "), Affinity::Integer),
            Value::Integer(42)
        );
        assert_eq!(cast_to(&Value::Null, Affinity::Integer), Value::Null);
        assert_eq!(
            cast_to(&Value::Real(3.9), Affinity::Integer),
            Value::Integer(3)
        );
        assert_eq!(
            cast_to(&Value::Real(-3.9), Affinity::Integer),
            Value::Integer(-3)
        );
        assert_eq!(
            cast_to(&Value::Blob(vec![0x41, 0x42].into()), Affinity::Integer),
            Value::Integer(0)
        );
        assert_eq!(
            cast_to(&Value::Real(1e300), Affinity::Integer),
            Value::Integer(i64::MAX)
        );
        assert_eq!(
            cast_to(&Value::Real(-1e300), Affinity::Integer),
            Value::Integer(i64::MIN)
        );
        // Saturation applies through the text-prefix path too, not just
        // a REAL source — negative and positive overflow are both
        // clamped rather than wrapping.
        assert_eq!(
            cast_to(&text("99999999999999999999"), Affinity::Integer),
            Value::Integer(i64::MAX)
        );
        assert_eq!(
            cast_to(&text("-99999999999999999999"), Affinity::Integer),
            Value::Integer(i64::MIN)
        );
        // A blob whose bytes form a numeric-looking string parses like
        // text, not just falling back to 0 (oracle: `CAST(x'3435' AS
        // INTEGER)` = `45`, the bytes "45").
        assert_eq!(
            cast_to(&Value::Blob(vec![0x34, 0x35].into()), Affinity::Integer),
            Value::Integer(45)
        );
    }

    #[test]
    fn cast_to_real_matches_oracle_truth_table() {
        assert_eq!(cast_to(&text("apple"), Affinity::Real), Value::Real(0.0));
        assert_eq!(cast_to(&text("3.5abc"), Affinity::Real), Value::Real(3.5));
        assert_eq!(
            cast_to(&Value::Integer(5), Affinity::Real),
            Value::Real(5.0)
        );
        assert_eq!(
            cast_to(&Value::Blob(vec![0x41, 0x42].into()), Affinity::Real),
            Value::Real(0.0)
        );
        assert_eq!(cast_to(&Value::Null, Affinity::Real), Value::Null);
    }

    #[test]
    fn cast_to_text_matches_oracle_truth_table() {
        assert_eq!(cast_to(&Value::Integer(5), Affinity::Text), text("5"));
        assert_eq!(cast_to(&Value::Real(5.5), Affinity::Text), text("5.5"));
        assert_eq!(
            cast_to(&Value::Blob(vec![0x41, 0x42].into()), Affinity::Text),
            text("AB")
        );
        assert_eq!(cast_to(&Value::Null, Affinity::Text), Value::Null);
    }

    #[test]
    fn cast_to_blob_matches_oracle_truth_table() {
        assert_eq!(
            cast_to(&text("abc"), Affinity::Blob),
            Value::Blob(b"abc".to_vec().into())
        );
        assert_eq!(
            cast_to(&Value::Integer(5), Affinity::Blob),
            Value::Blob(b"5".to_vec().into())
        );
        assert_eq!(cast_to(&Value::Null, Affinity::Blob), Value::Null);
    }

    #[test]
    fn cast_to_numeric_matches_oracle_truth_table() {
        assert_eq!(
            cast_to(&Value::Integer(5), Affinity::Numeric),
            Value::Integer(5)
        );
        // The one asymmetry: an already-REAL value is untouched (stays
        // 5.0), but the *same value spelled as text* downgrades to
        // INTEGER — oracle-verified, not a guess.
        assert_eq!(
            cast_to(&Value::Real(5.0), Affinity::Numeric),
            Value::Real(5.0)
        );
        assert_eq!(cast_to(&text("5.0"), Affinity::Numeric), Value::Integer(5));
        assert_eq!(cast_to(&text("5"), Affinity::Numeric), Value::Integer(5));
        assert_eq!(cast_to(&text("abc"), Affinity::Numeric), Value::Integer(0));
        assert_eq!(
            cast_to(&Value::Blob(vec![0x41, 0x42].into()), Affinity::Numeric),
            Value::Integer(0)
        );
        assert_eq!(cast_to(&Value::Null, Affinity::Numeric), Value::Null);
    }
}
