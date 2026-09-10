// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
/// Errors from decoding a SQLite record (the payload format used by table and index B-tree
/// cells).
#[derive(Debug, PartialEq, Eq)]
pub enum RecordError {
    /// The record buffer ended before decoding could complete.
    UnexpectedEof {
        /// Byte offset into the record where the read past the end started.
        offset: usize,
    },

    /// The declared header length is too small to even contain the header-length varint itself.
    HeaderTooShort {
        /// The header length declared by the header-length varint.
        declared: usize,
        /// The size in bytes of the header-length varint itself.
        varint_len: usize,
    },

    /// A serial-type varint in the header read past the declared header length.
    HeaderOverrun {
        /// Byte offset of the header entry that overran.
        offset: usize,
        /// The declared total header length.
        header_len: usize,
    },

    /// Bytes remained in the record buffer after all header-declared columns were decoded.
    TrailingData {
        /// Number of unconsumed trailing bytes.
        trailing: usize,
    },

    /// A text value's bytes were not valid UTF-8 under a UTF-8 `TextEncoding`.
    InvalidUtf8,

    /// A text value's bytes were not valid UTF-16 under a UTF-16 `TextEncoding`.
    InvalidUtf16,
}

impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordError::UnexpectedEof { offset } => {
                write!(f, "unexpected end of input at byte offset {offset}")
            }
            RecordError::HeaderTooShort {
                declared,
                varint_len,
            } => write!(
                f,
                "record header length {declared} is shorter than its own header-length varint ({varint_len} bytes)"
            ),
            RecordError::HeaderOverrun { offset, header_len } => write!(
                f,
                "record header entry at offset {offset} extends past the declared header length {header_len}"
            ),
            RecordError::TrailingData { trailing } => write!(
                f,
                "record has {trailing} unconsumed trailing byte(s) after decoding all columns"
            ),
            RecordError::InvalidUtf8 => write!(f, "invalid UTF-8 in text value"),
            RecordError::InvalidUtf16 => write!(f, "invalid UTF-16 in text value"),
        }
    }
}

impl std::error::Error for RecordError {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn display_all_variants() {
        assert_eq!(
            RecordError::UnexpectedEof { offset: 5 }.to_string(),
            "unexpected end of input at byte offset 5"
        );
        assert_eq!(
            RecordError::HeaderTooShort {
                declared: 1,
                varint_len: 2
            }
            .to_string(),
            "record header length 1 is shorter than its own header-length varint (2 bytes)"
        );
        assert_eq!(
            RecordError::HeaderOverrun {
                offset: 3,
                header_len: 4
            }
            .to_string(),
            "record header entry at offset 3 extends past the declared header length 4"
        );
        assert_eq!(
            RecordError::TrailingData { trailing: 7 }.to_string(),
            "record has 7 unconsumed trailing byte(s) after decoding all columns"
        );
        assert_eq!(
            RecordError::InvalidUtf8.to_string(),
            "invalid UTF-8 in text value"
        );
        assert_eq!(
            RecordError::InvalidUtf16.to_string(),
            "invalid UTF-16 in text value"
        );
    }

    #[test]
    fn debug_and_eq() {
        let err = RecordError::InvalidUtf8;
        assert_eq!(err, RecordError::InvalidUtf8);
        assert_ne!(format!("{err:?}"), "");
    }

    #[test]
    fn implements_std_error() {
        let err = RecordError::InvalidUtf8;
        assert!(std::error::Error::source(&err).is_none());
    }
}
