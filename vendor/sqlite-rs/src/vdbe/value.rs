// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! NULL propagation and three-valued logic (spec 008, Requirement 4).
//! `None` represents SQL NULL throughout this module's `Option<bool>`
//! results — never a boolean.

use std::cmp::Ordering;

use crate::record::{Collation, Value};
use crate::vdbe::compare::compare;

/// `=`: NULL propagates (a NULL operand yields `None`, not a boolean).
pub fn sql_eq(a: &Value, b: &Value, collation: Collation) -> Option<bool> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return None;
    }
    Some(compare(a, b, collation) == Ordering::Equal)
}

/// `<`: NULL propagates (a NULL operand yields `None`, not a boolean).
pub fn sql_lt(a: &Value, b: &Value, collation: Collation) -> Option<bool> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return None;
    }
    Some(compare(a, b, collation) == Ordering::Less)
}

/// `IS`: treats NULL as an ordinary comparable value rather than
/// propagating it.
pub fn is(a: &Value, b: &Value, collation: Collation) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        _ => compare(a, b, collation) == Ordering::Equal,
    }
}

/// `IS NOT`: the negation of `IS`.
pub fn is_not(a: &Value, b: &Value, collation: Collation) -> bool {
    !is(a, b, collation)
}

/// `NOT`: three-valued negation. `NOT NULL` is NULL.
pub fn not(a: Option<bool>) -> Option<bool> {
    a.map(|b| !b)
}

/// `AND`: a `false` operand dominates even if the other is NULL;
/// otherwise NULL propagates.
pub fn and(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

/// `OR`: a `true` operand dominates even if the other is NULL;
/// otherwise NULL propagates.
pub fn or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_propagates_through_equality() {
        assert_eq!(sql_eq(&Value::Null, &Value::Null, Collation::Binary), None);
        assert_eq!(
            sql_eq(&Value::Integer(1), &Value::Null, Collation::Binary),
            None
        );
    }

    #[test]
    fn non_null_comparisons_evaluate_normally() {
        assert_eq!(
            sql_eq(&Value::Integer(1), &Value::Integer(1), Collation::Binary),
            Some(true)
        );
        assert_eq!(
            sql_eq(&Value::Integer(1), &Value::Integer(2), Collation::Binary),
            Some(false)
        );
        assert_eq!(
            sql_lt(&Value::Integer(1), &Value::Integer(2), Collation::Binary),
            Some(true)
        );
        assert_eq!(
            sql_lt(&Value::Integer(2), &Value::Integer(1), Collation::Binary),
            Some(false)
        );
    }

    #[test]
    fn sql_lt_propagates_null() {
        assert_eq!(
            sql_lt(&Value::Null, &Value::Integer(1), Collation::Binary),
            None
        );
    }

    #[test]
    fn is_and_is_not_treat_null_as_comparable() {
        assert!(is(&Value::Null, &Value::Null, Collation::Binary));
        assert!(!is_not(&Value::Null, &Value::Null, Collation::Binary));
        assert!(!is(&Value::Null, &Value::Integer(1), Collation::Binary));
        assert!(is(
            &Value::Integer(1),
            &Value::Integer(1),
            Collation::Binary
        ));
        assert!(!is(
            &Value::Integer(1),
            &Value::Integer(2),
            Collation::Binary
        ));
    }

    #[test]
    fn and_or_follow_three_valued_logic() {
        assert_eq!(and(None, Some(false)), Some(false));
        assert_eq!(and(None, Some(true)), None);
        assert_eq!(and(Some(true), Some(true)), Some(true));
        assert_eq!(or(None, Some(true)), Some(true));
        assert_eq!(or(None, Some(false)), None);
        assert_eq!(or(Some(false), Some(false)), Some(false));
    }

    #[test]
    fn not_null_is_null() {
        assert_eq!(not(None), None);
        assert_eq!(not(Some(true)), Some(false));
    }
}
