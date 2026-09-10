// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use std::sync::Arc;

use super::error::RecordError;
use super::value::{TextEncoding, Value};
use super::varint::decode_varint;

/// Decodes a record (the payload of a table b-tree cell) into column
/// values, per the record-format doc: varint header length, then one
/// varint serial type per column, then the column bodies back-to-back.
/// Never panics — any truncation or malformed serial type returns `Err`.
pub fn decode_record(payload: &[u8], encoding: TextEncoding) -> Result<Vec<Value>, RecordError> {
    let (header_len, n) = decode_varint_at(payload, 0)?;
    let header_len = header_len as usize;
    if header_len < n {
        return Err(RecordError::HeaderTooShort {
            declared: header_len,
            varint_len: n,
        });
    }

    let mut serial_types = Vec::new();
    let mut pos = n;
    while pos < header_len {
        let (serial_type, len) = decode_varint_at(payload, pos)?;
        // pos and len are both bounded by payload's real (finite) length via
        // decode_varint_at's own bounds check, never by header_len's
        // (attacker-declared, unbounded) value — saturating_add can't
        // change the outcome of the overrun check below, only avoid a
        // theoretical wraparound on it.
        if pos.saturating_add(len) > header_len {
            return Err(RecordError::HeaderOverrun {
                offset: pos,
                header_len,
            });
        }
        serial_types.push(serial_type);
        pos = pos.saturating_add(len);
    }

    let mut body_pos = header_len;
    let mut values = Vec::with_capacity(serial_types.len());
    for serial_type in serial_types {
        let (value, len) = decode_serial_value(serial_type, payload, body_pos, encoding)?;
        values.push(value);
        body_pos = body_pos.saturating_add(len);
    }
    if body_pos != payload.len() {
        return Err(RecordError::TrailingData {
            trailing: payload.len().saturating_sub(body_pos),
        });
    }
    Ok(values)
}

/// Walks a record payload's header once, returning each column's serial
/// type paired with the byte offset (into `payload`) of that column's
/// body — never decodes any column body. This is the header-walk logic
/// [`decode_column`] builds on; [`parse_header_into`] is the same walk
/// for a caller (the row header cache, #458, in `src/vdbe/cursor.rs`)
/// that wants to reuse an existing `Vec`'s allocation across rows rather
/// than pay a fresh allocation per call.
pub(crate) fn parse_header(payload: &[u8]) -> Result<Vec<(u64, usize)>, RecordError> {
    let mut entries = Vec::new();
    parse_header_into(payload, &mut entries)?;
    Ok(entries)
}

/// Like [`parse_header`], but appends into a caller-supplied (cleared)
/// `Vec` instead of allocating a new one — lets a per-cursor cache reuse
/// its backing allocation across every row it's repositioned onto,
/// rather than allocating and freeing a `Vec` per row.
pub(crate) fn parse_header_into(
    payload: &[u8],
    entries: &mut Vec<(u64, usize)>,
) -> Result<(), RecordError> {
    entries.clear();
    let (header_len, n) = decode_varint_at(payload, 0)?;
    let header_len = header_len as usize;
    if header_len < n {
        return Err(RecordError::HeaderTooShort {
            declared: header_len,
            varint_len: n,
        });
    }

    let mut pos = n;
    let mut body_pos = header_len;
    while pos < header_len {
        let (serial_type, len) = decode_varint_at(payload, pos)?;
        if pos.saturating_add(len) > header_len {
            return Err(RecordError::HeaderOverrun {
                offset: pos,
                header_len,
            });
        }
        pos = pos.saturating_add(len);
        entries.push((serial_type, body_pos));
        body_pos = body_pos.saturating_add(serial_type_len(serial_type));
    }
    Ok(())
}

/// Decodes only column `idx` of a record's payload — the header entries
/// (serial types) for every column are walked to compute their body
/// sizes/offsets, but only `idx`'s body is decoded/allocated, unlike
/// [`decode_record`]. Used by the VDBE's `Column` opcode (#439) so a WHERE
/// clause that rejects a row never pays to decode the row's other
/// columns. Returns `Value::Null` for an out-of-range `idx`, matching
/// `decode_record(..)[idx]`'s `unwrap_or(Value::Null)` convention at call
/// sites.
pub fn decode_column(
    payload: &[u8],
    idx: usize,
    encoding: TextEncoding,
) -> Result<Value, RecordError> {
    let entries = parse_header(payload)?;
    match entries.get(idx) {
        Some(&(serial_type, offset)) => {
            let (value, _) = decode_serial_value(serial_type, payload, offset, encoding)?;
            Ok(value)
        }
        None => Ok(Value::Null),
    }
}

/// Column count of `payload`'s record — walks only the header (no value
/// decoding), for callers that need to know how many columns a record has
/// (e.g. "does this index row have a trailing rowid past the key prefix")
/// without paying to decode any of them.
pub fn record_column_count(payload: &[u8]) -> Result<usize, RecordError> {
    Ok(parse_header(payload)?.len())
}

/// Decodes only the first `max_columns` columns of a record's payload —
/// the header entries (serial types) for every column are walked to
/// compute body offsets, but only the requested prefix's bodies are
/// decoded/allocated, unlike [`decode_record`]. Used by `SorterInsert`
/// (#507) so a bounded top-K sorter's per-row comparison never pays to
/// decode payload columns past the sort key. `max_columns` beyond the
/// record's actual column count is clamped, matching [`decode_column`]'s
/// out-of-range convention rather than erroring.
pub fn decode_record_upto(
    payload: &[u8],
    max_columns: usize,
    encoding: TextEncoding,
) -> Result<Vec<Value>, RecordError> {
    let mut entries = Vec::new();
    decode_record_upto_into(payload, max_columns, encoding, &mut entries)
}

/// Like [`decode_record_upto`], but parses the header into a
/// caller-supplied (cleared) scratch `Vec` instead of allocating a new
/// one per call — for a hot loop that calls this once per row (e.g. the
/// sorter's per-`SorterInsert` key decode), reusing the same backing
/// allocation avoids a `Vec` grow/realloc on every row.
pub(crate) fn decode_record_upto_into(
    payload: &[u8],
    max_columns: usize,
    encoding: TextEncoding,
    entries: &mut Vec<(u64, usize)>,
) -> Result<Vec<Value>, RecordError> {
    parse_header_into(payload, entries)?;
    let n = max_columns.min(entries.len());
    let mut values = Vec::with_capacity(n);
    for &(serial_type, offset) in entries.iter().take(n) {
        let (value, _) = decode_serial_value(serial_type, payload, offset, encoding)?;
        values.push(value);
    }
    Ok(values)
}

/// Decodes only the columns listed in `wanted` (in any order/spread,
/// duplicates allowed) — the header entries (serial types/offsets) for
/// every column are still walked (cheap: varint decodes only, no value
/// bodies touched), but [`decode_serial_value`] only runs for `wanted`'s
/// indices. The result has exactly `wanted.len()` values, in `wanted`'s
/// own order (an out-of-range index decodes as `Value::Null`) — not one
/// slot per record column like [`decode_record_upto_into`], so a caller
/// (e.g. the sorter's per-`SorterInsert` key decode, #631) that only
/// ever wants a handful of columns out of a much wider row gets a
/// correspondingly small allocation, addressed by `wanted`'s own
/// position rather than the column's original index. Unlike
/// [`decode_record_upto_into`], a `wanted` index doesn't have to be a
/// small contiguous prefix — the sort key can (and often does) sit past
/// other columns the row also carries, without paying to decode any of
/// them just to reach it.
pub(crate) fn decode_record_only_into(
    payload: &[u8],
    wanted: &[usize],
    encoding: TextEncoding,
    entries: &mut Vec<(u64, usize)>,
) -> Result<Vec<Value>, RecordError> {
    if let [only] = wanted {
        return Ok(vec![decode_single_column(payload, *only, encoding)?]);
    }
    parse_header_into(payload, entries)?;
    let mut values = Vec::with_capacity(wanted.len());
    for &idx in wanted {
        let value = match entries.get(idx) {
            Some(&(serial_type, offset)) => {
                decode_serial_value(serial_type, payload, offset, encoding)?.0
            }
            None => Value::Null,
        };
        values.push(value);
    }
    Ok(values)
}

/// Single-key fast path for [`decode_record_only_into`] (#631 spike,
/// mirroring sqlite3's specialized sort-key comparators in
/// `vdbesort.c`, adapted to this codebase's decode-once-at-insert
/// design and `unsafe_code = "deny"`): walks the header column by
/// column exactly like [`parse_header_into`] does, but never
/// materializes it into an `entries` `Vec` at all — stops the walk the
/// instant `idx`'s own header entry is reached, decodes just that one
/// column, and returns. For the overwhelmingly common case (one GROUP
/// BY/ORDER BY key column, not several), this skips every per-column
/// `Vec::push` [`parse_header_into`] would otherwise do, including for
/// the columns skipped over on the way to `idx`.
fn decode_single_column(
    payload: &[u8],
    idx: usize,
    encoding: TextEncoding,
) -> Result<Value, RecordError> {
    let (header_len, n) = decode_varint_at(payload, 0)?;
    let header_len = header_len as usize;
    if header_len < n {
        return Err(RecordError::HeaderTooShort {
            declared: header_len,
            varint_len: n,
        });
    }
    let mut pos = n;
    let mut body_pos = header_len;
    let mut col = 0usize;
    while pos < header_len {
        let (serial_type, len) = decode_varint_at(payload, pos)?;
        if pos.saturating_add(len) > header_len {
            return Err(RecordError::HeaderOverrun {
                offset: pos,
                header_len,
            });
        }
        pos = pos.saturating_add(len);
        if col == idx {
            return Ok(decode_serial_value(serial_type, payload, body_pos, encoding)?.0);
        }
        body_pos = body_pos.saturating_add(serial_type_len(serial_type));
        col = col.saturating_add(1);
    }
    Ok(Value::Null)
}

/// Number of body bytes a serial type occupies, without decoding the
/// value it holds — lets [`decode_column`] skip past columns before the
/// requested index using only the (cheap) header entries, never touching
/// their bodies. Mirrors the lengths [`decode_serial_value`] returns.
fn serial_type_len(serial_type: u64) -> usize {
    match serial_type {
        0 | 8 | 9 | 10 | 11 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 | 7 => 8,
        n if n % 2 == 0 => (n.wrapping_sub(12) / 2) as usize,
        n => (n.wrapping_sub(13) / 2) as usize,
    }
}

/// `decode_varint`, but against `buf` starting at absolute offset `pos`,
/// with errors reporting the absolute offset rather than one relative to
/// a sub-slice.
fn decode_varint_at(buf: &[u8], pos: usize) -> Result<(u64, usize), RecordError> {
    let slice = buf
        .get(pos..)
        .ok_or(RecordError::UnexpectedEof { offset: pos })?;
    decode_varint(slice).map_err(|e| match e {
        RecordError::UnexpectedEof { offset } => RecordError::UnexpectedEof {
            offset: pos.saturating_add(offset),
        },
        other => other,
    })
}

fn take(buf: &[u8], pos: usize, len: usize) -> Result<&[u8], RecordError> {
    let end = pos
        .checked_add(len)
        .ok_or(RecordError::UnexpectedEof { offset: pos })?;
    buf.get(pos..end)
        .ok_or(RecordError::UnexpectedEof { offset: pos })
}

/// Like [`take`], but returns an owned fixed-size array instead of a slice
/// — lets fixed-width serial types (i16/i32/i64/f64) destructure their
/// bytes directly instead of indexing into a slice.
fn take_array<const N: usize>(buf: &[u8], pos: usize) -> Result<[u8; N], RecordError> {
    take(buf, pos, N)?
        .try_into()
        .map_err(|_| RecordError::UnexpectedEof { offset: pos })
}

/// Decodes one column body given its serial type. Returns the value and
/// the number of body bytes it occupies.
pub(crate) fn decode_serial_value(
    serial_type: u64,
    buf: &[u8],
    pos: usize,
    encoding: TextEncoding,
) -> Result<(Value, usize), RecordError> {
    match serial_type {
        0 => Ok((Value::Null, 0)),
        1 => {
            let [b0] = take_array(buf, pos)?;
            Ok((Value::Integer(b0 as i8 as i64), 1))
        }
        2 => {
            let b = take_array(buf, pos)?;
            Ok((Value::Integer(i16::from_be_bytes(b) as i64), 2))
        }
        3 => {
            let [b0, b1, b2] = take_array(buf, pos)?;
            let mut v = ((b0 as i64) << 16) | ((b1 as i64) << 8) | (b2 as i64);
            if b0 & 0x80 != 0 {
                v = v.wrapping_sub(1 << 24); // sign-extend 24-bit; magnitude is tiny relative to i64
            }
            Ok((Value::Integer(v), 3))
        }
        4 => {
            let b = take_array(buf, pos)?;
            Ok((Value::Integer(i32::from_be_bytes(b) as i64), 4))
        }
        5 => {
            let bytes: [u8; 6] = take_array(buf, pos)?;
            let [b0, ..] = bytes;
            let mut v: i64 = 0;
            for byte in bytes {
                v = (v << 8) | byte as i64;
            }
            if b0 & 0x80 != 0 {
                v = v.wrapping_sub(1 << 48); // sign-extend 48-bit; magnitude is tiny relative to i64
            }
            Ok((Value::Integer(v), 6))
        }
        6 => {
            let b = take_array(buf, pos)?;
            Ok((Value::Integer(i64::from_be_bytes(b)), 8))
        }
        7 => {
            let b = take_array(buf, pos)?;
            let value = f64::from_be_bytes(b);
            // SQLite decodes a NaN payload as NULL rather than a real NaN
            // (sqlite3VdbeSerialGet's IsNaN(x) check) — matched here for
            // binary-compatible read behavior.
            if value.is_nan() {
                Ok((Value::Null, 8))
            } else {
                Ok((Value::Real(value), 8))
            }
        }
        8 => Ok((Value::Integer(0), 0)),
        9 => Ok((Value::Integer(1), 0)),
        // Types 10/11 are reserved/internal (type 10 is SQLite's virtual-table
        // "no-change" marker) and never appear in a well-formed database, but
        // upstream decodes both as NULL rather than treating them as
        // corruption — matched here rather than erroring.
        10 | 11 => Ok((Value::Null, 0)),
        // n is guaranteed >= 12 here (match arms above exhaustively cover
        // 0..=11), so n.wrapping_sub(12) never wraps; n itself is an
        // attacker-controlled varint value, but the resulting length still
        // flows into take()'s checked_add, so an implausible declared length
        // errors there rather than overflowing here.
        n if n % 2 == 0 => {
            let len = (n.wrapping_sub(12) / 2) as usize;
            let bytes = take(buf, pos, len)?;
            Ok((Value::Blob(bytes.into()), len))
        }
        // n is guaranteed >= 13 here: odd, and 0..=11 handled above.
        n => {
            let len = (n.wrapping_sub(13) / 2) as usize;
            let bytes = take(buf, pos, len)?;
            let text = decode_text(bytes, encoding)?;
            Ok((Value::Text(text), len))
        }
    }
}

/// Decodes text bytes straight into `Arc<str>`. The UTF-8 case (by far the
/// common one) builds the `Arc<str>` directly from the validated byte slice
/// instead of routing through an intermediate `String`, avoiding a second
/// allocation and copy per text column.
fn decode_text(bytes: &[u8], encoding: TextEncoding) -> Result<Arc<str>, RecordError> {
    match encoding {
        TextEncoding::Utf8 => std::str::from_utf8(bytes)
            .map(Arc::from)
            .map_err(|_| RecordError::InvalidUtf8),
        TextEncoding::Utf16Le => decode_utf16(bytes, u16::from_le_bytes).map(Arc::from),
        TextEncoding::Utf16Be => decode_utf16(bytes, u16::from_be_bytes).map(Arc::from),
    }
}

fn decode_utf16(bytes: &[u8], unit_from_bytes: fn([u8; 2]) -> u16) -> Result<String, RecordError> {
    if !bytes.len().is_multiple_of(2) {
        return Err(RecordError::InvalidUtf16);
    }
    let units = bytes.as_chunks::<2>().0.iter().map(|c| unit_from_bytes(*c));
    char::decode_utf16(units)
        .collect::<Result<String, _>>()
        .map_err(|_| RecordError::InvalidUtf16)
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

    fn varint_bytes(mut value: u64) -> Vec<u8> {
        // Minimal varint encoder for building test payloads (mirrors the
        // decoder's bit layout; only used to construct fixtures).
        let mut bytes = [0u8; 9];
        for i in (0..8).rev() {
            bytes[i] = (value & 0x7f) as u8;
            value >>= 7;
            if i != 7 {
                bytes[i] |= 0x80;
            }
        }
        if value == 0 {
            // Find the shortest valid encoding by trimming leading continuation bytes.
            let first_nonzero = bytes[..8]
                .iter()
                .position(|&b| b & 0x7f != 0 || b == 0x80)
                .unwrap_or(7);
            let mut out = bytes[first_nonzero..8].to_vec();
            *out.last_mut().unwrap() &= 0x7f;
            out
        } else {
            bytes[8] = value as u8;
            bytes.to_vec()
        }
    }

    fn record_bytes(serial_types_and_bodies: &[(u64, &[u8])]) -> Vec<u8> {
        let mut header = Vec::new();
        for (st, _) in serial_types_and_bodies {
            header.extend(varint_bytes(*st));
        }
        // header_len includes its own varint's length; try lengths until stable.
        let mut header_len = header.len() + 1;
        loop {
            let hl_bytes = varint_bytes(header_len as u64);
            if hl_bytes.len() + header.len() == header_len {
                let mut out = hl_bytes;
                out.extend(&header);
                for (_, body) in serial_types_and_bodies {
                    out.extend(*body);
                }
                return out;
            }
            header_len += 1;
        }
    }

    #[test]
    fn null_value() {
        let payload = record_bytes(&[(0, &[])]);
        assert_eq!(
            decode_record(&payload, TextEncoding::Utf8),
            Ok(vec![Value::Null])
        );
    }

    #[test]
    fn integer_widths_and_edge_values() {
        // type 1: i8 range
        for v in [0i8, 1, -1, i8::MIN, i8::MAX] {
            let payload = record_bytes(&[(1, &v.to_be_bytes())]);
            assert_eq!(
                decode_record(&payload, TextEncoding::Utf8),
                Ok(vec![Value::Integer(v as i64)])
            );
        }
        // type 2: i16 range
        for v in [0i16, i16::MIN, i16::MAX] {
            let payload = record_bytes(&[(2, &v.to_be_bytes())]);
            assert_eq!(
                decode_record(&payload, TextEncoding::Utf8),
                Ok(vec![Value::Integer(v as i64)])
            );
        }
        // type 3: 24-bit signed range (no native type — build bytes by hand)
        let cases_24: &[(i64, [u8; 3])] = &[
            (0, [0x00, 0x00, 0x00]),
            (-1, [0xff, 0xff, 0xff]),
            (8388607, [0x7f, 0xff, 0xff]),
            (-8388608, [0x80, 0x00, 0x00]),
        ];
        for (expected, bytes) in cases_24 {
            let payload = record_bytes(&[(3, bytes)]);
            assert_eq!(
                decode_record(&payload, TextEncoding::Utf8),
                Ok(vec![Value::Integer(*expected)])
            );
        }
        // type 4: i32 range
        for v in [0i32, i32::MIN, i32::MAX] {
            let payload = record_bytes(&[(4, &v.to_be_bytes())]);
            assert_eq!(
                decode_record(&payload, TextEncoding::Utf8),
                Ok(vec![Value::Integer(v as i64)])
            );
        }
        // type 5: 48-bit signed range
        let cases_48: &[(i64, [u8; 6])] = &[
            (0, [0, 0, 0, 0, 0, 0]),
            (-1, [0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
            (140737488355327, [0x7f, 0xff, 0xff, 0xff, 0xff, 0xff]),
            (-140737488355328, [0x80, 0x00, 0x00, 0x00, 0x00, 0x00]),
        ];
        for (expected, bytes) in cases_48 {
            let payload = record_bytes(&[(5, bytes)]);
            assert_eq!(
                decode_record(&payload, TextEncoding::Utf8),
                Ok(vec![Value::Integer(*expected)])
            );
        }
        // type 6: full i64 range
        for v in [0i64, -1, i64::MIN, i64::MAX] {
            let payload = record_bytes(&[(6, &v.to_be_bytes())]);
            assert_eq!(
                decode_record(&payload, TextEncoding::Utf8),
                Ok(vec![Value::Integer(v)])
            );
        }
        // type 8/9: zero-byte integer constants
        let payload = record_bytes(&[(8, &[]), (9, &[])]);
        assert_eq!(
            decode_record(&payload, TextEncoding::Utf8),
            Ok(vec![Value::Integer(0), Value::Integer(1)])
        );
    }

    #[test]
    fn real_edge_values_bit_identical() {
        let cases = [
            0.0f64,
            -0.0,
            1.5,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN,
            f64::MAX,
        ];
        for v in cases {
            let payload = record_bytes(&[(7, &v.to_be_bytes())]);
            let decoded = decode_record(&payload, TextEncoding::Utf8).unwrap();
            match &decoded[..] {
                [Value::Real(r)] => assert_eq!(r.to_bits(), v.to_bits(), "value {v} bit mismatch"),
                other => panic!("expected one Real, got {other:?}"),
            }
        }
    }

    #[test]
    fn real_nan_decodes_as_null() {
        // Matches sqlite3VdbeSerialGet: a NaN float payload decodes as NULL,
        // not as a Real(NaN), same as upstream SQLite.
        let payload = record_bytes(&[(7, &f64::NAN.to_be_bytes())]);
        assert_eq!(
            decode_record(&payload, TextEncoding::Utf8),
            Ok(vec![Value::Null])
        );
    }

    #[test]
    fn blob_including_zero_length() {
        let payload = record_bytes(&[(12, &[]), (20, &[0xde, 0xad, 0xbe, 0xef])]);
        assert_eq!(
            decode_record(&payload, TextEncoding::Utf8),
            Ok(vec![
                Value::Blob(vec![].into()),
                Value::Blob(vec![0xde, 0xad, 0xbe, 0xef].into())
            ])
        );
    }

    #[test]
    fn text_utf8_including_empty() {
        let payload = record_bytes(&[(13, &[]), (13 + 2 * 5, b"hello")]);
        assert_eq!(
            decode_record(&payload, TextEncoding::Utf8),
            Ok(vec![
                Value::Text(String::new().into()),
                Value::Text("hello".to_string().into())
            ])
        );
    }

    #[test]
    fn text_utf16le_and_utf16be() {
        let s = "hé"; // 2 chars, needs non-ASCII to actually exercise 2-byte units
        let utf16_units: Vec<u16> = s.encode_utf16().collect();
        let le_bytes: Vec<u8> = utf16_units.iter().flat_map(|u| u.to_le_bytes()).collect();
        let be_bytes: Vec<u8> = utf16_units.iter().flat_map(|u| u.to_be_bytes()).collect();

        let payload_le = record_bytes(&[(13 + 2 * le_bytes.len() as u64, &le_bytes)]);
        assert_eq!(
            decode_record(&payload_le, TextEncoding::Utf16Le),
            Ok(vec![Value::Text(s.to_string().into())])
        );

        let payload_be = record_bytes(&[(13 + 2 * be_bytes.len() as u64, &be_bytes)]);
        assert_eq!(
            decode_record(&payload_be, TextEncoding::Utf16Be),
            Ok(vec![Value::Text(s.to_string().into())])
        );
    }

    #[test]
    fn reserved_serial_types_decode_as_null() {
        // Types 10/11 never appear in a well-formed database, but upstream
        // SQLite decodes both as NULL (type 10 doubles as the virtual-table
        // "no-change" marker) rather than treating them as corruption.
        let payload = record_bytes(&[(10, &[])]);
        assert_eq!(
            decode_record(&payload, TextEncoding::Utf8),
            Ok(vec![Value::Null])
        );
        let payload = record_bytes(&[(11, &[])]);
        assert_eq!(
            decode_record(&payload, TextEncoding::Utf8),
            Ok(vec![Value::Null])
        );
    }

    #[test]
    fn header_len_exactly_equal_to_its_own_varint_len_is_a_valid_empty_record() {
        // header_len == n (the varint's own byte count) is the boundary
        // case: a record whose header is nothing but the header-length
        // varint itself, declaring zero columns. Pins `header_len < n`
        // against mutation to `<=`, which would wrongly reject this as
        // HeaderTooShort.
        let payload = vec![0x01]; // header_len = 1, encoded in 1 byte
        assert_eq!(decode_record(&payload, TextEncoding::Utf8), Ok(vec![]));
    }

    #[test]
    fn header_shorter_than_its_own_varint_errors() {
        // A header-length varint encoded with redundant continuation bytes
        // can claim a `header_len` smaller than the varint's own byte count.
        let payload = vec![0x80, 0x00]; // encodes header_len = 0 using 2 bytes
        assert_eq!(
            decode_record(&payload, TextEncoding::Utf8),
            Err(RecordError::HeaderTooShort {
                declared: 0,
                varint_len: 2
            })
        );
    }

    #[test]
    fn header_entry_overrunning_declared_length_errors() {
        // header_len = 2 leaves exactly 1 byte for serial-type entries, but
        // the entry at offset 1 is encoded as a 2-byte varint (0x81, 0x00)
        // that would extend into what's declared as the record body — it
        // must not be silently reinterpreted as body bytes.
        let payload = vec![0x02, 0x81, 0x00];
        assert_eq!(
            decode_record(&payload, TextEncoding::Utf8),
            Err(RecordError::HeaderOverrun {
                offset: 1,
                header_len: 2
            })
        );
    }

    #[test]
    fn trailing_bytes_after_last_column_error() {
        let mut payload = record_bytes(&[(0, &[])]);
        payload.push(0xff); // unconsumed trailing byte
        assert_eq!(
            decode_record(&payload, TextEncoding::Utf8),
            Err(RecordError::TrailingData { trailing: 1 })
        );
    }

    #[test]
    fn invalid_utf8_errors_not_panics() {
        let invalid = [0xff, 0xfe];
        let payload = record_bytes(&[(13 + 2 * invalid.len() as u64, &invalid)]);
        assert_eq!(
            decode_record(&payload, TextEncoding::Utf8),
            Err(RecordError::InvalidUtf8)
        );
    }

    #[test]
    fn decode_column_matches_decode_record_at_every_index() {
        let payload = record_bytes(&[
            (1, &[42]),
            (0, &[]),
            (13 + 2 * 5, b"hello"),
            (7, &2.5f64.to_be_bytes()),
        ]);
        let full = decode_record(&payload, TextEncoding::Utf8).unwrap();
        for (idx, expected) in full.iter().enumerate() {
            assert_eq!(
                decode_column(&payload, idx, TextEncoding::Utf8),
                Ok(expected.clone())
            );
        }
    }

    #[test]
    fn decode_record_upto_matches_decode_record_prefix() {
        let payload = record_bytes(&[
            (1, &[42]),
            (0, &[]),
            (13 + 2 * 5, b"hello"),
            (7, &2.5f64.to_be_bytes()),
        ]);
        let full = decode_record(&payload, TextEncoding::Utf8).unwrap();
        for n in 0..=full.len() {
            assert_eq!(
                decode_record_upto(&payload, n, TextEncoding::Utf8).unwrap(),
                full[..n]
            );
        }
    }

    #[test]
    fn decode_record_only_into_single_wanted_matches_the_general_path() {
        // #631 spike: `decode_record_only_into`'s single-key fast path
        // (`decode_single_column`) must agree with decoding every
        // column the general way, for a key at every position —
        // leading, middle, and trailing — not just index 0.
        let payload = record_bytes(&[
            (1, &[42]),
            (0, &[]),
            (13 + 2 * 5, b"hello"),
            (7, &2.5f64.to_be_bytes()),
        ]);
        let full = decode_record(&payload, TextEncoding::Utf8).unwrap();
        let mut entries = Vec::new();
        for (idx, expected) in full.iter().enumerate() {
            let single =
                decode_record_only_into(&payload, &[idx], TextEncoding::Utf8, &mut entries)
                    .unwrap();
            assert_eq!(single, vec![expected.clone()], "index {idx}");
        }
    }

    #[test]
    fn decode_record_only_into_single_wanted_out_of_range_is_null() {
        let payload = record_bytes(&[(1, &[42]), (0, &[])]);
        let mut entries = Vec::new();
        let result =
            decode_record_only_into(&payload, &[100], TextEncoding::Utf8, &mut entries).unwrap();
        assert_eq!(result, vec![Value::Null]);
    }

    #[test]
    fn decode_record_upto_beyond_column_count_clamps_like_decode_record() {
        let payload = record_bytes(&[(1, &[42]), (0, &[])]);
        let full = decode_record(&payload, TextEncoding::Utf8).unwrap();
        assert_eq!(
            decode_record_upto(&payload, 100, TextEncoding::Utf8).unwrap(),
            full
        );
    }

    #[test]
    fn decode_record_upto_header_errors_still_surface() {
        let payload = vec![0x80, 0x00]; // header_len = 0 via 2-byte varint
        assert_eq!(
            decode_record_upto(&payload, 1, TextEncoding::Utf8),
            Err(RecordError::HeaderTooShort {
                declared: 0,
                varint_len: 2
            })
        );
    }

    #[test]
    fn decode_column_out_of_range_is_null() {
        let payload = record_bytes(&[(1, &[42])]);
        assert_eq!(
            decode_column(&payload, 5, TextEncoding::Utf8),
            Ok(Value::Null)
        );
    }

    #[test]
    fn decode_column_header_errors_still_surface() {
        let payload = vec![0x80, 0x00]; // header_len = 0 via 2-byte varint
        assert_eq!(
            decode_column(&payload, 0, TextEncoding::Utf8),
            Err(RecordError::HeaderTooShort {
                declared: 0,
                varint_len: 2
            })
        );
    }

    #[test]
    fn truncated_record_at_every_offset_errors_not_panics() {
        let payload = record_bytes(&[
            (1, &[42]),
            (13 + 2 * 5, b"hello"),
            (7, &2.5f64.to_be_bytes()),
        ]);
        for cut in 0..payload.len() {
            let result = decode_record(&payload[..cut], TextEncoding::Utf8);
            assert!(
                result.is_err(),
                "truncating to {cut} bytes should error, got {result:?}"
            );
        }
        // Full payload still decodes fine, confirming the truncation loop
        // above is actually exercising a valid record and not testing nothing.
        assert!(decode_record(&payload, TextEncoding::Utf8).is_ok());
    }
}
