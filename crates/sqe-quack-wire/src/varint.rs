//! LEB128 varint encoding.
//!
//! - `encode_unsigned`/`decode_unsigned`: standard LEB128
//! - `encode_signed`/`decode_signed`: sign-extended LEB128 (terminates when
//!   remaining bits all match the sign), matching DuckDB's `EncodeSignedLEB128`
//!   in `src/include/duckdb/common/serializer/encoding_util.hpp`.

pub fn encode_unsigned(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return;
        }
    }
}

pub fn encode_signed(mut value: i64, out: &mut Vec<u8>) {
    loop {
        let byte = (value as u8) & 0x7F;
        value >>= 7; // arithmetic shift preserves sign
        let done = (value == 0 && byte & 0x40 == 0) || (value == -1 && byte & 0x40 != 0);
        if done {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Decode a single sign-extended LEB128 varint from `input`.
/// Returns the value and the number of bytes consumed.
pub fn decode_signed(input: &[u8]) -> crate::Result<(i64, usize)> {
    let mut value: i64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in input.iter().enumerate() {
        if i >= 10 {
            return Err(crate::WireError::VarintOverflow);
        }
        value |= ((byte & 0x7F) as i64) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            // sign-extend if the sign bit (0x40) of the last byte is set and
            // we did not consume all 64 bits.
            if shift < 64 && byte & 0x40 != 0 {
                value |= -1i64 << shift;
            }
            return Ok((value, i + 1));
        }
    }
    Err(crate::WireError::UnexpectedEof)
}

/// Decode a single unsigned LEB128 varint from `input`.
/// Returns the value and the number of bytes consumed.
pub fn decode_unsigned(input: &[u8]) -> crate::Result<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in input.iter().enumerate() {
        if i >= 10 {
            return Err(crate::WireError::VarintOverflow);
        }
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
    }
    Err(crate::WireError::UnexpectedEof)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsigned_encodes_zero_as_single_zero_byte() {
        let mut buf = Vec::new();
        encode_unsigned(0, &mut buf);
        assert_eq!(buf, &[0x00]);
    }

    #[test]
    fn unsigned_encodes_single_byte_for_values_below_128() {
        let mut buf = Vec::new();
        encode_unsigned(0x7F, &mut buf);
        assert_eq!(buf, &[0x7F]);
    }

    #[test]
    fn unsigned_encodes_128_as_two_bytes() {
        let mut buf = Vec::new();
        encode_unsigned(128, &mut buf);
        assert_eq!(buf, &[0x80, 0x01]);
    }

    #[test]
    fn unsigned_encodes_u64_max_as_ten_bytes() {
        let mut buf = Vec::new();
        encode_unsigned(u64::MAX, &mut buf);
        assert_eq!(
            buf,
            &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]
        );
    }

    #[test]
    fn decode_unsigned_roundtrips_a_range_of_values() {
        for value in [0u64, 1, 127, 128, 16_383, 16_384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            encode_unsigned(value, &mut buf);
            let (decoded, consumed) = decode_unsigned(&buf).expect("decode");
            assert_eq!(decoded, value, "roundtrip failed for {value}");
            assert_eq!(consumed, buf.len(), "consumed mismatch for {value}");
        }
    }

    #[test]
    fn decode_unsigned_reports_eof_when_continuation_unterminated() {
        let buf = [0x80u8, 0x80, 0x80]; // continuation bits but no terminator
        let err = decode_unsigned(&buf).unwrap_err();
        assert!(matches!(err, crate::WireError::UnexpectedEof));
    }

    #[test]
    fn decode_unsigned_reports_overflow_at_eleven_bytes() {
        let buf = [0xFFu8; 11];
        let err = decode_unsigned(&buf).unwrap_err();
        assert!(matches!(err, crate::WireError::VarintOverflow));
    }

    #[test]
    fn signed_encodes_zero_as_single_zero_byte() {
        let mut buf = Vec::new();
        encode_signed(0, &mut buf);
        assert_eq!(buf, &[0x00]);
    }

    #[test]
    fn signed_encodes_minus_one_as_single_seven_f() {
        // -1 fits in one byte: low 7 bits all set, sign-extend matches.
        let mut buf = Vec::new();
        encode_signed(-1, &mut buf);
        assert_eq!(buf, &[0x7F]);
    }

    #[test]
    fn signed_encodes_sixty_four_with_continuation() {
        // 64 has bit 6 set, would look negative without a continuation byte.
        let mut buf = Vec::new();
        encode_signed(64, &mut buf);
        assert_eq!(buf, &[0xC0, 0x00]);
    }

    #[test]
    fn signed_encodes_minus_sixty_four_as_single_byte() {
        // -64 = 0b1000000 truncated to 7 bits = 0x40, sign-extends to -1.
        let mut buf = Vec::new();
        encode_signed(-64, &mut buf);
        assert_eq!(buf, &[0x40]);
    }

    #[test]
    fn signed_roundtrips_across_boundaries() {
        for value in [
            0i64,
            1,
            -1,
            63,
            64,
            -64,
            -65,
            i32::MAX as i64,
            i32::MIN as i64,
            i64::MAX,
            i64::MIN,
        ] {
            let mut buf = Vec::new();
            encode_signed(value, &mut buf);
            let (decoded, consumed) = decode_signed(&buf).expect("decode");
            assert_eq!(decoded, value, "roundtrip failed for {value}");
            assert_eq!(consumed, buf.len(), "consumed mismatch for {value}");
        }
    }
}
