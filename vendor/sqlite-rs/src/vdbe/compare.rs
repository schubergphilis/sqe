// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Cross-type comparison order (spec 008, Requirement 2): NULL < numeric
//! < text < blob, with INTEGER and REAL merged into one numeric class.

use std::cmp::Ordering;

use crate::record::{compare_text, Collation, Value};

#[inline]
fn value_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Integer(_) | Value::Real(_) => 1,
        Value::Text(_) => 2,
        Value::Blob(_) => 3,
    }
}

/// Compares an `i64` against an `f64` the way SQLite does: a straight
/// `as f64` cast loses precision near `i64::MAX`/`MIN`, which would
/// wrongly report `i64::MAX == (i64::MAX as f64)` even though the
/// nearest representable double for that magnitude has already rounded
/// past it. Mirrors sqlite3IntFloatCompare (util.c).
fn compare_int_real(i: i64, r: f64) -> Ordering {
    // NaN sorts as greater than every integer too (#615): must agree with
    // `compare_real`'s NaN-is-the-maximum convention, or a NaN `Real`
    // sitting between an `Integer` and a plain `Real` breaks transitivity
    // (`Integer(0) < Real(tiny)`, `Real(tiny) < Real(NaN)`, yet
    // `Integer(0) > Real(NaN)` under the old "NaN < every integer" rule).
    if r.is_nan() {
        return Ordering::Less;
    }
    if r < -9_223_372_036_854_775_808.0 {
        return Ordering::Greater;
    }
    if r >= 9_223_372_036_854_775_808.0 {
        return Ordering::Less;
    }
    #[allow(clippy::cast_possible_truncation)]
    let y = r as i64;
    if i < y {
        return Ordering::Less;
    }
    if i > y {
        return Ordering::Greater;
    }
    #[allow(clippy::cast_precision_loss)]
    let s = i as f64;
    s.partial_cmp(&r).unwrap_or(Ordering::Equal)
}

/// Compares two `f64`s as a total order (found by fuzzing, #615): plain
/// `partial_cmp().unwrap_or(Ordering::Equal)` reports NaN as "equal" to
/// every other value, which breaks transitivity the moment a NaN sits
/// between two ordinary, distinct reals (`a < NaN` and `NaN < c` were both
/// reported as `Equal`, yet `a != c`). NaN sorts as greater than every
/// non-NaN real and equal to any other NaN, the same convention
/// `compare_int_real` already uses for NaN-vs-integer.
fn compare_real(x: f64, y: f64) -> Ordering {
    match (x.is_nan(), y.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
    }
}

/// Total order over `Value`s: NULL < numeric < text < blob, per spec 008
/// Requirement 2. `collation` governs text-vs-text comparisons only.
#[inline]
pub fn compare(a: &Value, b: &Value, collation: Collation) -> Ordering {
    let (ra, rb) = (value_rank(a), value_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Real(x), Value::Real(y)) => compare_real(*x, *y),
        (Value::Integer(x), Value::Real(y)) => compare_int_real(*x, *y),
        (Value::Real(x), Value::Integer(y)) => compare_int_real(*y, *x).reverse(),
        (Value::Text(x), Value::Text(y)) => compare_text(x, y, collation),
        (Value::Blob(x), Value::Blob(y)) => x.cmp(y),
        _ => Ordering::Equal, // unreachable: value_rank already separated these
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_is_lower_than_every_other_class() {
        for other in [
            Value::Integer(1),
            Value::Text("a".to_string().into()),
            Value::Blob(vec![0].into()),
        ] {
            assert_eq!(
                compare(&Value::Null, &other, Collation::Binary),
                Ordering::Less
            );
        }
    }

    #[test]
    fn numeric_sorts_below_text_below_blob() {
        let one = Value::Integer(1);
        let a = Value::Text("a".to_string().into());
        let blob = Value::Blob(vec![0].into());
        assert_eq!(compare(&one, &a, Collation::Binary), Ordering::Less);
        assert_eq!(compare(&one, &blob, Collation::Binary), Ordering::Less);
        assert_eq!(compare(&a, &blob, Collation::Binary), Ordering::Less);
    }

    #[test]
    fn integer_and_real_merge_into_one_numeric_class() {
        assert_eq!(
            compare(&Value::Integer(2), &Value::Real(2.0), Collation::Binary),
            Ordering::Equal
        );
        // i64::MAX as a REAL rounds up past the exact integer value.
        assert_eq!(
            compare(
                &Value::Integer(9_223_372_036_854_775_807),
                &Value::Real(9_223_372_036_854_775_807.0),
                Collation::Binary
            ),
            Ordering::Less
        );
    }

    /// #615 (found by `tests/fuzz/fuzz_targets/semantics_compare.rs`,
    /// seed `tests/fuzz/seeds/semantics_compare/nan_breaks_transitivity_615`):
    /// `partial_cmp().unwrap_or(Ordering::Equal)` reported NaN as "equal"
    /// to every other REAL, so `a < NaN` and `NaN < c` both came back
    /// `Equal` even though `a != c` — breaking the total-order contract
    /// (spec 008 Req 2) transitivity assumes.
    #[test]
    fn real_nan_sorts_above_every_other_real_and_is_transitive() {
        let a = Value::Real(1.140_835_715_797_277_5e-303);
        let nan = Value::Real(f64::NAN);
        let c = Value::Real(5.300_644_564_512_085e-299);

        let a_nan = compare(&a, &nan, Collation::Binary);
        let nan_a = compare(&nan, &a, Collation::Binary);
        assert_eq!(a_nan, Ordering::Less);
        assert_eq!(nan_a, a_nan.reverse());

        let nan_c = compare(&nan, &c, Collation::Binary);
        assert_eq!(nan_c, Ordering::Greater);

        // NaN never reports Equal against a distinct non-NaN real, so the
        // fuzz target's Equal-branch transitivity check can't misfire on
        // it — the actual `a` vs `c` order is Less, and NaN's presence
        // between them no longer papers over that.
        assert_eq!(compare(&a, &c, Collation::Binary), Ordering::Less);
        assert_eq!(
            compare(
                &Value::Real(f64::NAN),
                &Value::Real(f64::NAN),
                Collation::Binary
            ),
            Ordering::Equal
        );
    }

    /// #615, second fuzz-found transitivity break: `compare_int_real`'s
    /// old "NaN sorts below every integer" rule disagreed with
    /// `compare_real`'s "NaN sorts above every non-NaN real" rule, so a
    /// NaN `Real` sitting between an `Integer` and a plain `Real` broke
    /// transitivity even after the first fix landed.
    #[test]
    fn integer_vs_nan_real_agrees_with_real_vs_nan_real() {
        let zero = Value::Integer(0);
        let tiny = Value::Real(8.204_292_603_659_346e-304);
        let nan = Value::Real(f64::NAN);

        assert_eq!(compare(&zero, &tiny, Collation::Binary), Ordering::Less);
        assert_eq!(compare(&tiny, &nan, Collation::Binary), Ordering::Less);
        // Transitivity: zero < tiny < NaN must imply zero < NaN.
        assert_eq!(compare(&zero, &nan, Collation::Binary), Ordering::Less);
        assert_eq!(
            compare(&nan, &zero, Collation::Binary),
            Ordering::Greater,
            "must stay antisymmetric with the above"
        );
    }
}
