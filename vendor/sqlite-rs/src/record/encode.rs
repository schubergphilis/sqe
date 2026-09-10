// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::value::{TextEncoding, Value};

/// Encodes a varint: the inverse of `decode_varint`. Mirrors the
/// decoder's bit layout — big-endian, 7 bits per byte with a high-bit
/// continuation flag, up to 9 bytes — always producing the minimal
/// encoding (no redundant continuation bytes).
#[allow(
    clippy::arithmetic_side_effects,
    reason = "groups/i/shift all range over the compile-time-constant 0..8, so these additions and the 7x multiply never overflow"
)]
pub(crate) fn encode_varint(value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    write_varint_into(value, &mut out);
    out
}

/// Like [`encode_varint`], but appends directly to a caller-owned buffer
/// instead of allocating a fresh `Vec<u8>` per call — the hot encode path
/// (`encode_record_into`) calls this once per column and once for the
/// header length, so avoiding a per-call allocation here matters.
#[allow(
    clippy::arithmetic_side_effects,
    reason = "groups/i/shift all range over the compile-time-constant 0..8, so these additions and the 7x multiply never overflow"
)]
pub(crate) fn write_varint_into(value: u64, out: &mut Vec<u8>) {
    // The 9-byte form only kicks in once the value needs more than 56
    // bits (8 groups of 7): the decoder's own threshold (it reads 8
    // 7-bit groups, then an unconditional 9th full-byte group).
    if value < (1u64 << 56) {
        let mut groups = 1u32;
        while groups < 8 && value >= (1u64 << (7 * groups)) {
            groups += 1;
        }
        for i in 0..groups {
            let shift = 7 * (groups - 1 - i);
            #[allow(clippy::cast_possible_truncation)]
            let mut byte = ((value >> shift) & 0x7f) as u8;
            if i != groups - 1 {
                byte |= 0x80;
            }
            out.push(byte);
        }
    } else {
        let top56 = value >> 8;
        for i in 0..8 {
            let shift = 7 * (7 - i);
            #[allow(clippy::cast_possible_truncation)]
            let byte = (((top56 >> shift) & 0x7f) as u8) | 0x80;
            out.push(byte);
        }
        #[allow(clippy::cast_possible_truncation)]
        out.push((value & 0xff) as u8);
    }
}

/// The number of bytes [`write_varint_into`] would emit for `value`,
/// without emitting them — used to size the record header without a
/// trial-encode loop.
#[allow(
    clippy::arithmetic_side_effects,
    reason = "groups ranges over the compile-time-constant 0..8, so this addition never overflows"
)]
fn varint_len(value: u64) -> usize {
    if value < (1u64 << 56) {
        let mut groups = 1u32;
        while groups < 8 && value >= (1u64 << (7 * groups)) {
            groups += 1;
        }
        groups as usize
    } else {
        9
    }
}

// 24-bit and 48-bit signed integer ranges, per sqlite3VdbeSerialType's
// integer-width selection (smallest serial type that losslessly holds
// the value).
const I24_MIN: i64 = -(1 << 23);
const I24_MAX: i64 = (1 << 23) - 1;
const I48_MIN: i64 = -(1 << 47);
const I48_MAX: i64 = (1 << 47) - 1;

fn integer_serial_type(i: i64) -> u64 {
    if i == 0 {
        8
    } else if i == 1 {
        9
    } else if i8::try_from(i).is_ok() {
        1
    } else if i16::try_from(i).is_ok() {
        2
    } else if (I24_MIN..=I24_MAX).contains(&i) {
        3
    } else if i32::try_from(i).is_ok() {
        4
    } else if (I48_MIN..=I48_MAX).contains(&i) {
        5
    } else {
        6
    }
}

fn integer_body_len(serial_type: u64) -> usize {
    match serial_type {
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 5,
        6 => 8,
        _ => 0, // 8/9: zero-byte constants
    }
}

fn write_integer_body_into(i: i64, serial_type: u64, out: &mut Vec<u8>) {
    match serial_type {
        1 => out.push(i as u8),
        2 => out.extend_from_slice(&(i as i16).to_be_bytes()),
        3 => out.extend_from_slice(&i.to_be_bytes()[5..8]),
        4 => out.extend_from_slice(&(i as i32).to_be_bytes()),
        5 => out.extend_from_slice(&i.to_be_bytes()[2..8]),
        6 => out.extend_from_slice(&i.to_be_bytes()),
        _ => {} // 8/9: zero-byte constants
    }
}

/// Byte length of `s` once encoded under `encoding`, without allocating
/// the encoded bytes — used to size a TEXT column's serial type.
fn encoded_text_len(s: &str, encoding: TextEncoding) -> usize {
    match encoding {
        TextEncoding::Utf8 => s.len(),
        TextEncoding::Utf16Le | TextEncoding::Utf16Be => s.encode_utf16().count().saturating_mul(2),
    }
}

fn write_text_body_into(s: &str, encoding: TextEncoding, out: &mut Vec<u8>) {
    match encoding {
        TextEncoding::Utf8 => out.extend_from_slice(s.as_bytes()),
        TextEncoding::Utf16Le => {
            out.extend(s.encode_utf16().flat_map(|u| u.to_le_bytes()));
        }
        TextEncoding::Utf16Be => {
            out.extend(s.encode_utf16().flat_map(|u| u.to_be_bytes()));
        }
    }
}

fn blob_serial_type(len: usize) -> u64 {
    12u64.saturating_add(2u64.saturating_mul(len as u64))
}

fn text_serial_type(len: usize) -> u64 {
    13u64.saturating_add(2u64.saturating_mul(len as u64))
}

/// Returns a value's serial type and encoded body length, per the
/// record-format doc: the smallest integer width that losslessly holds an
/// INTEGER, the 8-byte IEEE-754 form for REAL, and the `12+2*len`/
/// `13+2*len` scheme for BLOB/TEXT. Body bytes are written separately (by
/// [`write_body_into`]) so this stays allocation-free.
fn serial_type_and_body_len(value: &Value, encoding: TextEncoding) -> (u64, usize) {
    match value {
        Value::Null => (0, 0),
        Value::Integer(i) => {
            let st = integer_serial_type(*i);
            (st, integer_body_len(st))
        }
        Value::Real(_) => (7, 8),
        Value::Blob(b) => (blob_serial_type(b.len()), b.len()),
        Value::Text(s) => {
            let len = encoded_text_len(s, encoding);
            (text_serial_type(len), len)
        }
    }
}

fn write_body_into(value: &Value, serial_type: u64, encoding: TextEncoding, out: &mut Vec<u8>) {
    match value {
        Value::Null => {}
        Value::Integer(i) => write_integer_body_into(*i, serial_type, out),
        Value::Real(r) => out.extend_from_slice(&r.to_be_bytes()),
        Value::Blob(b) => out.extend_from_slice(b),
        Value::Text(s) => write_text_body_into(s, encoding, out),
    }
}

/// Encodes column values into a record payload, per the record-format
/// doc: a varint header length, one varint serial type per column, then
/// the column bodies back-to-back. The inverse of
/// [`super::decode::decode_record`] — round-tripping through both
/// functions reproduces the original values, and the byte layout matches
/// spec 003 exactly (reused as-is for `MakeRecord`'s in-memory rows).
pub fn encode_record(values: &[Value], encoding: TextEncoding) -> Vec<u8> {
    let mut out = Vec::new();
    let mut serial_types = Vec::new();
    encode_record_into(values, encoding, &mut out, &mut serial_types);
    out
}

/// Like [`encode_record`], but writes into caller-owned buffers instead of
/// allocating fresh ones. `out`/`serial_types` are cleared first; callers
/// that reuse the same buffers across calls (e.g. once per row in a hot
/// loop, like `MakeRecord`'s `Vm::encode_scratch`, #631) amortize both
/// allocations instead of paying a fresh one per row.
///
/// Computes each column's serial type/body-length up front into
/// `serial_types` (no per-column `Vec<u8>` allocation — only fixed-size
/// ints and lengths), then writes the header and bodies directly into
/// `out` in two passes. This avoids the allocator churn of building a
/// `Vec<(u64, Vec<u8>)>` of per-column bodies plus a separate
/// serial-type-bytes buffer per row, which dominated `SorterInsert`/
/// `MakeRecord` profiles under GROUP BY/ORDER BY workloads (#572);
/// reusing `serial_types` itself (rather than collecting a fresh one
/// per call) closes the remaining allocation #631 found still there.
pub fn encode_record_into(
    values: &[Value],
    encoding: TextEncoding,
    out: &mut Vec<u8>,
    serial_types: &mut Vec<(u64, usize)>,
) {
    out.clear();
    serial_types.clear();
    serial_types.extend(values.iter().map(|v| serial_type_and_body_len(v, encoding)));

    let mut header_body_len = 0usize;
    let mut bodies_len = 0usize;
    for (st, len) in serial_types.iter() {
        header_body_len = header_body_len.saturating_add(varint_len(*st));
        bodies_len = bodies_len.saturating_add(*len);
    }

    // header_len includes its own varint's length; grow until the
    // varint's own encoded size is consistent with the declared length.
    let mut header_len = header_body_len.saturating_add(1);
    #[allow(clippy::cast_possible_truncation)]
    while varint_len(header_len as u64).saturating_add(header_body_len) != header_len {
        header_len = header_len.saturating_add(1);
    }

    out.reserve(header_len.saturating_add(bodies_len));
    #[allow(clippy::cast_possible_truncation)]
    write_varint_into(header_len as u64, out);
    for (st, _) in serial_types.iter() {
        write_varint_into(*st, out);
    }
    for (value, (st, _)) in values.iter().zip(serial_types.iter()) {
        write_body_into(value, *st, encoding, out);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::super::decode::decode_record;
    use super::*;

    #[test]
    fn round_trips_through_decode_record() {
        let values = vec![
            Value::Null,
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(-1),
            Value::Integer(i64::MAX),
            Value::Integer(i64::MIN),
            Value::Real(1.5),
            Value::Text("hello".to_string().into()),
            Value::Text(String::new().into()),
            Value::Blob(vec![0xde, 0xad, 0xbe, 0xef].into()),
            Value::Blob(Vec::new().into()),
        ];
        let payload = encode_record(&values, TextEncoding::Utf8);
        assert_eq!(decode_record(&payload, TextEncoding::Utf8), Ok(values));
    }

    #[test]
    fn integer_widths_pick_smallest_serial_type() {
        let cases: &[(i64, u64)] = &[
            (0, 8),
            (1, 9),
            (2, 1),
            (i8::MIN as i64, 1),
            (i8::MAX as i64 + 1, 2),
            (i16::MIN as i64, 2),
            (i16::MAX as i64 + 1, 3),
            (I24_MIN, 3),
            (I24_MAX + 1, 4),
            (i32::MIN as i64, 4),
            (i32::MAX as i64 + 1, 5),
            (I48_MIN, 5),
            (I48_MAX + 1, 6),
            (i64::MAX, 6),
            (i64::MIN, 6),
        ];
        for (v, expected_st) in cases {
            let (st, _) = serial_type_and_body_len(&Value::Integer(*v), TextEncoding::Utf8);
            assert_eq!(
                st, *expected_st,
                "value {v} expected serial type {expected_st}"
            );
        }
    }

    #[test]
    fn matches_spec_003_header_shape_for_a_multi_column_row() {
        // Mirrors the decoder's own fixture-construction convention.
        let values = vec![Value::Integer(42), Value::Text("abc".to_string().into())];
        let payload = encode_record(&values, TextEncoding::Utf8);
        // header_len(1) + serial_type(42 -> type 1, 1 byte) + serial_type(abc -> 13+2*3=19, 1 byte) = 3
        assert_eq!(payload[0], 3);
        assert_eq!(payload[1], 1); // type 1: i8
        assert_eq!(payload[2], 19); // type 13+2*3
        assert_eq!(payload[3], 42);
        assert_eq!(&payload[4..7], b"abc");
    }

    /// #368 tagged MC/DC vector (obligation `encode_33`, decision
    /// `groups < 8 && value >= (1u64 << (7 * groups))`): both leaves true
    /// on the loop's first check — `groups` must grow past 1.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__encode_33__v1_groups_grows() {
        assert_eq!(encode_varint(128).len(), 2);
    }

    /// #368 tagged MC/DC vector (obligation `encode_33`): leaf A
    /// (`groups < 8`) true, leaf B false on the first check — the loop
    /// body never runs, `groups` stays 1. Independence pair for B against
    /// `mcdc__encode_33__v1_groups_grows`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__encode_33__v2_groups_stays_one() {
        assert_eq!(encode_varint(5).len(), 1);
    }

    /// #368 tagged MC/DC vector (obligation `encode_33`): leaf A false
    /// (`groups` reaches 8, short-circuiting B) — the largest value still
    /// under the 9-byte-form threshold. Independence pair for A against
    /// `mcdc__encode_33__v1_groups_grows`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__encode_33__v3_groups_caps_at_eight() {
        assert_eq!(encode_varint((1u64 << 56) - 1).len(), 8);
    }

    /// MC/DC vector (obligation `encode_68`, `varint_len`'s mirror of
    /// `encode_33`'s decision `groups < 8 && value >= (1u64 << (7 *
    /// groups))`): both leaves true on the loop's first check.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__encode_68__v1_groups_grows() {
        assert_eq!(varint_len(200), 2);
    }

    /// MC/DC vector (obligation `encode_68`): leaf A (`groups < 8`) true,
    /// leaf B false on the first check — the loop body never runs.
    /// Independence pair for B against `mcdc__encode_68__v1_groups_grows`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__encode_68__v2_groups_stays_one() {
        assert_eq!(varint_len(50), 1);
    }

    /// MC/DC vector (obligation `encode_68`): leaf A false (`groups`
    /// reaches 8, short-circuiting B) — a value under the 9-byte-form
    /// threshold large enough to grow all the way to 8 groups.
    /// Independence pair for A against `mcdc__encode_68__v1_groups_grows`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__encode_68__v3_groups_caps_at_eight() {
        assert_eq!(varint_len(1u64 << 55), 8);
    }
}
