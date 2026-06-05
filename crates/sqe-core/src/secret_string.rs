//! Newtype wrapper for credential strings.
//!
//! `SecretString` exists to make credential leakage a compile-time concern
//! rather than a code review concern. The default `#[derive(Debug)]` on a
//! struct containing `String` fields will happily print the raw bytes to a
//! panic handler, an `anyhow!` chain, or a `tracing::error!` call. Every
//! credential-bearing field that adopts this type gets three protections for
//! free:
//!
//! 1. `Debug` renders `<set>` or `<unset>` rather than the secret material.
//! 2. `Display` is intentionally absent. Callers cannot accidentally `format!`
//!    the value into a log line; they must call [`SecretString::expose`].
//! 3. `Drop` zeroizes the backing buffer so the bytes do not linger after the
//!    value goes out of scope.
//!
//! Construction is explicit via [`SecretString::new`] or `From<String>`. There
//! is no `Deref<Target = str>`; equality compares the raw bytes via
//! [`SecretString::ct_eq`] in constant time so callers cannot pick the
//! short-circuiting comparison accidentally.

use std::fmt;

use serde::{Deserialize, Deserializer};
use zeroize::Zeroize;

/// A credential-bearing string that does not leak through `Debug`, `Display`,
/// or accidental logging.
///
/// See the module docs for the full rationale. Use `expose()` (or
/// `expose_bytes()`) at the boundary where the raw value must cross into an
/// HTTP header, a query string, or a credential provider.
#[derive(Clone, Default)]
pub struct SecretString(String);

impl SecretString {
    /// Wrap a raw `String`. The caller is responsible for ensuring the input
    /// was sourced from a credential boundary (config file, OIDC token
    /// response, Flight handshake metadata) and not, e.g., a SQL literal.
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// Borrow the raw value. The name is deliberate: every disclosure site is
    /// greppable.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Borrow the raw bytes. Useful for SHA-256 fingerprinting and other
    /// digest-style use cases where allocating a fresh `String` is wasteful.
    pub fn expose_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Returns `true` when the wrapped string has no bytes. Treat this as the
    /// "missing credential" sentinel rather than reaching into `expose()` to
    /// re-check.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Length in bytes. Useful for fingerprint formatting; never exposes the
    /// material.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Constant-time equality. Prevents callers from picking the variable-time
    /// short-circuit comparator by accident.
    ///
    /// CORE-03 (accepted tradeoff): the early return on a length mismatch leaks,
    /// via timing, whether the candidate's length matches the secret's. This is
    /// the standard tradeoff for variable-length secret comparison (the `subtle`
    /// crate behaves the same), and the leaked length is far lower value than
    /// the secret itself. Comparing fixed-length digests would close the leak
    /// at the cost of hashing both sides on every check; not worth it here.
    pub fn ct_eq(&self, other: &Self) -> bool {
        let a = self.0.as_bytes();
        let b = other.0.as_bytes();
        if a.len() != b.len() {
            return false;
        }
        let mut diff: u8 = 0;
        for (x, y) in a.iter().zip(b.iter()) {
            diff |= x ^ y;
        }
        diff == 0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_empty() { "<unset>" } else { "<set>" })
    }
}

// No `Display`, no `Deref`, no `AsRef<str>`. Every callsite that needs the raw
// material must go through `expose()`.

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SecretString {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl Drop for SecretString {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ok(Self(raw))
    }
}

// Intentionally no `Serialize`. Round-tripping credentials through `serde_json`
// or `toml::to_string` would defeat the redaction. If a config dump path needs
// to emit the field, it should write a fixed sentinel ("<redacted>") rather
// than call `Serialize`.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_shows_set_for_non_empty() {
        let s = SecretString::new("ey-very-secret-jwt".to_string());
        let d = format!("{:?}", s);
        assert_eq!(d, "<set>", "got {d}");
        assert!(!d.contains("ey-very-secret-jwt"));
    }

    #[test]
    fn debug_shows_unset_for_empty() {
        let s = SecretString::default();
        let d = format!("{:?}", s);
        assert_eq!(d, "<unset>");
    }

    #[test]
    fn debug_inside_struct_does_not_leak() {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Carrier {
            user: String,
            token: SecretString,
        }
        let c = Carrier {
            user: "alice".into(),
            token: SecretString::new("payload-must-not-appear".into()),
        };
        let d = format!("{:?}", c);
        assert!(d.contains("alice"));
        assert!(!d.contains("payload-must-not-appear"), "leaked: {d}");
        assert!(d.contains("<set>"), "presence sentinel missing: {d}");
    }

    #[test]
    fn expose_returns_inner_string() {
        let s = SecretString::new("abc".to_string());
        assert_eq!(s.expose(), "abc");
        assert_eq!(s.expose_bytes(), b"abc");
    }

    #[test]
    fn is_empty_and_len() {
        let empty = SecretString::default();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        let set = SecretString::new("hello".to_string());
        assert!(!set.is_empty());
        assert_eq!(set.len(), 5);
    }

    #[test]
    fn ct_eq_matches_equal_strings() {
        let a = SecretString::new("abc".to_string());
        let b = SecretString::new("abc".to_string());
        assert!(a.ct_eq(&b));
    }

    #[test]
    fn ct_eq_rejects_different_strings() {
        let a = SecretString::new("abc".to_string());
        let b = SecretString::new("abd".to_string());
        assert!(!a.ct_eq(&b));
    }

    #[test]
    fn ct_eq_rejects_different_lengths() {
        let a = SecretString::new("abc".to_string());
        let b = SecretString::new("abcd".to_string());
        assert!(!a.ct_eq(&b));
    }

    #[test]
    fn from_str_and_string_constructors() {
        let from_str: SecretString = "hello".into();
        let from_string: SecretString = "hello".to_string().into();
        assert!(from_str.ct_eq(&from_string));
    }

    #[test]
    fn deserialize_from_toml_string() {
        #[derive(Deserialize)]
        struct W {
            tok: SecretString,
        }
        let w: W = toml::from_str("tok = \"value\"").unwrap();
        assert_eq!(w.tok.expose(), "value");
        assert_eq!(format!("{:?}", w.tok), "<set>");
    }
}
