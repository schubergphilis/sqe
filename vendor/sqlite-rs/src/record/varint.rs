// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::error::RecordError;

/// Decodes a SQLite varint: big-endian, 7 bits per byte with a high-bit
/// continuation flag, up to 9 bytes (the 9th contributes a full 8 bits with
/// no continuation flag). Returns the decoded value and the number of bytes
/// consumed. Never panics — a truncated buffer returns `Err`.
#[allow(
    clippy::arithmetic_side_effects,
    reason = "i ranges over the compile-time-constant 0..8, so i + 1 never overflows"
)]
#[inline]
pub fn decode_varint(buf: &[u8]) -> Result<(u64, usize), RecordError> {
    let mut result: u64 = 0;
    for i in 0..8 {
        let byte = *buf.get(i).ok_or(RecordError::UnexpectedEof { offset: i })?;
        result = (result << 7) | (byte & 0x7f) as u64;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
    }
    let byte = *buf.get(8).ok_or(RecordError::UnexpectedEof { offset: 8 })?;
    result = (result << 8) | byte as u64;
    Ok((result, 9))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;

    #[test]
    fn single_byte_values() {
        assert_eq!(decode_varint(&[0x00]), Ok((0, 1)));
        assert_eq!(decode_varint(&[0x7f]), Ok((127, 1)));
    }

    #[test]
    fn two_byte_boundary() {
        // 128 is the smallest value that needs a second byte.
        assert_eq!(decode_varint(&[0x81, 0x00]), Ok((128, 2)));
        assert_eq!(decode_varint(&[0xff, 0x7f]), Ok((0x3fff, 2)));
    }

    #[test]
    fn every_length_from_1_to_9_bytes() {
        // One extra 7-bit group per length, each shifted in from a 0x80-flagged byte.
        let cases: &[(&[u8], u64, usize)] = &[
            (&[0x00], 0, 1),
            (&[0x81, 0x00], 1 << 7, 2),
            (&[0x81, 0x80, 0x00], 1 << 14, 3),
            (&[0x81, 0x80, 0x80, 0x00], 1 << 21, 4),
            (&[0x81, 0x80, 0x80, 0x80, 0x00], 1 << 28, 5),
            (&[0x81, 0x80, 0x80, 0x80, 0x80, 0x00], 1 << 35, 6),
            (&[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00], 1 << 42, 7),
            (
                &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00],
                1 << 49,
                8,
            ),
            // The 9-byte form: first 8 bytes all-continuation, 9th contributes a full byte.
            (
                &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
                u64::MAX,
                9,
            ),
        ];
        for (bytes, expected_value, expected_len) in cases {
            assert_eq!(
                decode_varint(bytes),
                Ok((*expected_value, *expected_len)),
                "input {bytes:x?}"
            );
        }
    }

    #[test]
    fn truncated_input_errors_not_panics() {
        assert_eq!(
            decode_varint(&[]),
            Err(RecordError::UnexpectedEof { offset: 0 })
        );
        assert_eq!(
            decode_varint(&[0x80]),
            Err(RecordError::UnexpectedEof { offset: 1 })
        );
        // 8 continuation bytes with no 9th byte present.
        assert_eq!(
            decode_varint(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
            Err(RecordError::UnexpectedEof { offset: 8 })
        );
    }
}
