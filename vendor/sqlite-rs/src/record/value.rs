// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use std::sync::Arc;

/// A single decoded column value, per SQLite's dynamic type system.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// SQL `NULL`.
    Null,
    /// A signed integer, stored as 1/2/3/4/6/8 bytes on disk per the serial type.
    Integer(i64),
    /// An 8-byte IEEE 754 floating-point value.
    Real(f64),
    /// A text value, decoded according to the database's `TextEncoding`.
    Text(Arc<str>),
    /// An uninterpreted byte sequence.
    Blob(Arc<[u8]>),
}

/// `Value` must stay `Send + Sync` (#688): the embedding API's
/// connection handle hands result rows to another thread, and a row is
/// a `Vec<Value>`. `Rc` payloads made that impossible, and nothing but a
/// compile-time check keeps it from silently regressing — swapping
/// either payload back to `Rc` would otherwise only fail much later, in
/// whichever consumer tried to cross a thread.
///
/// This is deliberately *not* a claim about `Pager`/`PageSource`, which
/// stay `Rc` per ADR-0013 and ADR-0017. See ADR-0039.
const fn assert_value_send_sync<T: Send + Sync>() {}
const _: () = assert_value_send_sync::<Value>();

/// Conversions into [`Value`], so binding a parameter does not require a
/// caller to name `Arc`.
///
/// Spec 013 Requirement 6 asks that a consumer using only the embedding API
/// never has to reach into the engine. `Value::Text` and `Value::Blob` hold
/// `Arc` payloads (ADR-0039, so a row can cross a thread), which is an
/// implementation detail of the *storage*, not something a caller binding
/// the string `"x"` should have to construct.
///
/// `bool` maps to `Integer(0)`/`Integer(1)` because that is what SQLite
/// stores — it has no boolean storage class. `Option<T>` maps `None` to
/// `Null`, which is what makes a nullable column bindable without a match.
mod conversions {
    use super::Value;
    use std::sync::Arc;

    impl From<i64> for Value {
        fn from(v: i64) -> Self {
            Value::Integer(v)
        }
    }

    impl From<i32> for Value {
        fn from(v: i32) -> Self {
            Value::Integer(i64::from(v))
        }
    }

    impl From<bool> for Value {
        fn from(v: bool) -> Self {
            Value::Integer(i64::from(v))
        }
    }

    impl From<f64> for Value {
        fn from(v: f64) -> Self {
            Value::Real(v)
        }
    }

    impl From<&str> for Value {
        fn from(v: &str) -> Self {
            Value::Text(Arc::from(v))
        }
    }

    impl From<String> for Value {
        fn from(v: String) -> Self {
            Value::Text(Arc::from(v.as_str()))
        }
    }

    impl From<&[u8]> for Value {
        fn from(v: &[u8]) -> Self {
            Value::Blob(Arc::from(v))
        }
    }

    impl From<Vec<u8>> for Value {
        fn from(v: Vec<u8>) -> Self {
            Value::Blob(Arc::from(v.as_slice()))
        }
    }

    impl<T: Into<Value>> From<Option<T>> for Value {
        fn from(v: Option<T>) -> Self {
            match v {
                Some(inner) => inner.into(),
                None => Value::Null,
            }
        }
    }
}

/// The database's text encoding, from database header byte 56.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextEncoding {
    /// UTF-8.
    Utf8,
    /// UTF-16 little-endian.
    Utf16Le,
    /// UTF-16 big-endian.
    Utf16Be,
}
