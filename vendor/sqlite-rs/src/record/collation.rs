// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Collating functions for text comparison (spec 008, Requirement 3).

use std::cmp::Ordering;

/// A text collating function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Collation {
    /// Byte-for-byte comparison. SQLite's default.
    Binary,
    /// ASCII-only case folding — NOT Unicode. `ß`/`SS` and `é`/`É` never
    /// compare equal.
    NoCase,
    /// BINARY comparison after stripping trailing spaces from both
    /// operands (not from storage).
    RTrim,
}

/// Compares two strings under the given collation.
#[inline]
pub fn compare_text(a: &str, b: &str, collation: Collation) -> Ordering {
    match collation {
        Collation::Binary => a.as_bytes().cmp(b.as_bytes()),
        Collation::NoCase => a
            .as_bytes()
            .iter()
            .map(u8::to_ascii_lowercase)
            .cmp(b.as_bytes().iter().map(u8::to_ascii_lowercase)),
        Collation::RTrim => {
            let a = a.trim_end_matches(' ');
            let b = b.trim_end_matches(' ');
            a.as_bytes().cmp(b.as_bytes())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_is_case_sensitive() {
        assert_ne!(
            compare_text("abc", "ABC", Collation::Binary),
            Ordering::Equal
        );
    }

    #[test]
    fn nocase_folds_ascii_only() {
        assert_eq!(compare_text("I", "i", Collation::NoCase), Ordering::Equal);
        assert_ne!(
            compare_text("straße", "STRASSE", Collation::NoCase),
            Ordering::Equal
        );
        assert_ne!(compare_text("é", "É", Collation::NoCase), Ordering::Equal);
    }

    #[test]
    fn rtrim_ignores_only_trailing_spaces() {
        assert_eq!(
            compare_text("abc ", "abc", Collation::RTrim),
            Ordering::Equal
        );
        assert_eq!(
            compare_text("abc", "abc  ", Collation::RTrim),
            Ordering::Equal
        );
        assert_ne!(
            compare_text(" abc", "abc", Collation::RTrim),
            Ordering::Equal
        );
    }
}
