//! Pure-Rust port of DuckDB's `BinarySerializer` / `BinaryDeserializer`.
//!
//! Wire format reference: `docs/quack-protocol.md` and DuckDB's
//! `src/common/serializer/binary_serializer.cpp`.
//!
//! - Field IDs (`field_id_t`) are raw little-endian `u16`.
//! - `MESSAGE_TERMINATOR_FIELD_ID = 0xFFFF` ends an object.
//! - Integers (u8/i8 through u64/i64 plus hugeint) are LEB128-encoded.
//! - Booleans and floats are raw little-endian.
//! - Strings are length-prefixed (varint) then raw bytes.

use crate::varint;

pub const MESSAGE_TERMINATOR_FIELD_ID: u16 = 0xFFFF;

pub struct BinarySerializer {
    out: Vec<u8>,
}

impl Default for BinarySerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl BinarySerializer {
    pub fn new() -> Self {
        Self { out: Vec::new() }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.out
    }

    pub fn begin_object(&mut self) {
        // OnObjectBegin: no output
    }

    pub fn end_object(&mut self) {
        // OnObjectEnd: write the terminator field_id (raw u16 LE).
        self.out
            .extend_from_slice(&MESSAGE_TERMINATOR_FIELD_ID.to_le_bytes());
    }

    pub fn begin_property(&mut self, field_id: u16) {
        // OnPropertyBegin: write field_id as raw u16 LE.
        self.out.extend_from_slice(&field_id.to_le_bytes());
    }

    pub fn end_property(&mut self) {
        // OnPropertyEnd: no output.
    }

    pub fn begin_optional_property(&mut self, field_id: u16, present: bool) {
        // OnOptionalPropertyBegin: write field_id only if present.
        if present {
            self.begin_property(field_id);
        }
    }

    pub fn end_optional_property(&mut self, _present: bool) {
        // OnOptionalPropertyEnd: no output.
    }

    pub fn begin_list(&mut self, count: u64) {
        varint::encode_unsigned(count, &mut self.out);
    }

    pub fn end_list(&mut self) {
        // OnListEnd: no output.
    }

    /// Write the leading "nullable present" byte that DuckDB's
    /// `OnNullableBegin` emits before serialised `unique_ptr<T>` /
    /// `Option<T>` values. The value is `1` when present, `0` when null.
    /// Used by `unique_ptr<DataChunkWrapper>` and the list-of-unique_ptr
    /// shape that `PrepareResponse.results` and `FetchResponse.results`
    /// take on the wire.
    pub fn begin_nullable(&mut self, present: bool) {
        self.out.push(present as u8);
    }

    pub fn end_nullable(&mut self, _present: bool) {
        // OnNullableEnd: no output.
    }

    pub fn write_bool(&mut self, value: bool) {
        self.out.push(value as u8);
    }

    pub fn write_u8(&mut self, value: u8) {
        varint::encode_unsigned(value as u64, &mut self.out);
    }

    pub fn write_u16(&mut self, value: u16) {
        varint::encode_unsigned(value as u64, &mut self.out);
    }

    pub fn write_u32(&mut self, value: u32) {
        varint::encode_unsigned(value as u64, &mut self.out);
    }

    pub fn write_u64(&mut self, value: u64) {
        varint::encode_unsigned(value, &mut self.out);
    }

    pub fn write_i8(&mut self, value: i8) {
        varint::encode_signed(value as i64, &mut self.out);
    }

    pub fn write_i16(&mut self, value: i16) {
        varint::encode_signed(value as i64, &mut self.out);
    }

    pub fn write_i32(&mut self, value: i32) {
        varint::encode_signed(value as i64, &mut self.out);
    }

    pub fn write_i64(&mut self, value: i64) {
        varint::encode_signed(value, &mut self.out);
    }

    pub fn write_hugeint(&mut self, value: i128) {
        // DuckDB hugeint = struct { int64_t lower; int64_t upper; }
        // serialised as two signed LEB128 values: lower first, then upper.
        let lower = value as u64 as i64;
        let upper = (value >> 64) as i64;
        varint::encode_signed(lower, &mut self.out);
        varint::encode_signed(upper, &mut self.out);
    }

    pub fn write_f32(&mut self, value: f32) {
        self.out.extend_from_slice(&value.to_le_bytes());
    }

    pub fn write_f64(&mut self, value: f64) {
        self.out.extend_from_slice(&value.to_le_bytes());
    }

    pub fn write_string(&mut self, value: &str) {
        let bytes = value.as_bytes();
        varint::encode_unsigned(bytes.len() as u64, &mut self.out);
        self.out.extend_from_slice(bytes);
    }

    /// DuckDB `WriteDataPtr(ptr, count)`: varint length followed by raw bytes.
    pub fn write_data_ptr(&mut self, bytes: &[u8]) {
        varint::encode_unsigned(bytes.len() as u64, &mut self.out);
        self.out.extend_from_slice(bytes);
    }
}

pub struct BinaryDeserializer<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> BinaryDeserializer<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn remaining(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    fn read_u16_le(&mut self) -> crate::Result<u16> {
        if self.buf.len() - self.pos < 2 {
            return Err(crate::WireError::UnexpectedEof);
        }
        let v = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    pub fn peek_field(&self) -> crate::Result<u16> {
        if self.buf.len() - self.pos < 2 {
            return Err(crate::WireError::UnexpectedEof);
        }
        Ok(u16::from_le_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
        ]))
    }

    pub fn expect_field(&mut self, field_id: u16) -> crate::Result<()> {
        let actual = self.read_u16_le()?;
        if actual != field_id {
            return Err(crate::WireError::UnexpectedField {
                expected: field_id,
                actual,
            });
        }
        Ok(())
    }

    pub fn expect_object_end(&mut self) -> crate::Result<()> {
        let actual = self.read_u16_le()?;
        if actual != MESSAGE_TERMINATOR_FIELD_ID {
            return Err(crate::WireError::UnexpectedField {
                expected: MESSAGE_TERMINATOR_FIELD_ID,
                actual,
            });
        }
        Ok(())
    }

    /// If the next field is `field_id`, consume it and return true. Otherwise
    /// leave the position unchanged and return false. Used for optional fields.
    pub fn read_optional(&mut self, field_id: u16) -> crate::Result<bool> {
        let peeked = self.peek_field()?;
        if peeked == field_id {
            self.pos += 2;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Consume DuckDB's "nullable present" byte that precedes a serialised
    /// `unique_ptr<T>` value. Returns `true` when the value follows (most
    /// common case in our message set), `false` when the upstream sent a
    /// null pointer.
    pub fn read_nullable_present(&mut self) -> crate::Result<bool> {
        self.read_bool()
    }

    pub fn read_bool(&mut self) -> crate::Result<bool> {
        if self.pos >= self.buf.len() {
            return Err(crate::WireError::UnexpectedEof);
        }
        let v = self.buf[self.pos] != 0;
        self.pos += 1;
        Ok(v)
    }

    pub fn read_u8(&mut self) -> crate::Result<u8> {
        self.read_u64().map(|v| v as u8)
    }

    pub fn read_u16(&mut self) -> crate::Result<u16> {
        self.read_u64().map(|v| v as u16)
    }

    pub fn read_u32(&mut self) -> crate::Result<u32> {
        self.read_u64().map(|v| v as u32)
    }

    pub fn read_u64(&mut self) -> crate::Result<u64> {
        let (value, consumed) = varint::decode_unsigned(&self.buf[self.pos..])?;
        self.pos += consumed;
        Ok(value)
    }

    pub fn read_i8(&mut self) -> crate::Result<i8> {
        self.read_i64().map(|v| v as i8)
    }

    pub fn read_i16(&mut self) -> crate::Result<i16> {
        self.read_i64().map(|v| v as i16)
    }

    pub fn read_i32(&mut self) -> crate::Result<i32> {
        self.read_i64().map(|v| v as i32)
    }

    pub fn read_i64(&mut self) -> crate::Result<i64> {
        let (value, consumed) = varint::decode_signed(&self.buf[self.pos..])?;
        self.pos += consumed;
        Ok(value)
    }

    pub fn read_hugeint(&mut self) -> crate::Result<i128> {
        let lower = self.read_i64()? as u64 as i128;
        let upper = self.read_i64()? as i128;
        Ok((upper << 64) | lower)
    }

    pub fn read_f32(&mut self) -> crate::Result<f32> {
        if self.buf.len() - self.pos < 4 {
            return Err(crate::WireError::UnexpectedEof);
        }
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(f32::from_le_bytes(buf))
    }

    pub fn read_f64(&mut self) -> crate::Result<f64> {
        if self.buf.len() - self.pos < 8 {
            return Err(crate::WireError::UnexpectedEof);
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(f64::from_le_bytes(buf))
    }

    pub fn read_string(&mut self) -> crate::Result<String> {
        let len = self.read_u64()? as usize;
        if self.buf.len() - self.pos < len {
            return Err(crate::WireError::UnexpectedEof);
        }
        let bytes = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        let s = std::str::from_utf8(bytes)
            .map_err(|_| crate::WireError::InvalidUtf8)?
            .to_owned();
        Ok(s)
    }

    /// Consume a length-prefixed string slot without UTF-8 validation. Used
    /// for NULL VARCHAR rows: real DuckDB writes uninitialised garbage
    /// bytes (e.g. an `0x80` byte that fails UTF-8) instead of an empty
    /// string at NULL positions. The decoder must skip those bytes by
    /// position without trying to interpret them.
    pub fn skip_string(&mut self) -> crate::Result<()> {
        let len = self.read_u64()? as usize;
        if self.buf.len() - self.pos < len {
            return Err(crate::WireError::UnexpectedEof);
        }
        self.pos += len;
        Ok(())
    }

    pub fn read_list_count(&mut self) -> crate::Result<u64> {
        self.read_u64()
    }

    /// DuckDB `ReadDataPtr`: varint length followed by raw bytes. Returns an
    /// owned copy so the caller does not need to track buffer lifetimes.
    pub fn read_data_ptr(&mut self) -> crate::Result<Vec<u8>> {
        let len = self.read_u64()? as usize;
        if self.buf.len() - self.pos < len {
            return Err(crate::WireError::UnexpectedEof);
        }
        let bytes = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_object_encodes_to_terminator_only() {
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.end_object();
        assert_eq!(s.into_bytes(), &[0xFF, 0xFF]);
    }

    #[test]
    fn bool_property_encodes_as_field_id_then_raw_byte() {
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_property(1);
        s.write_bool(true);
        s.end_property();
        s.end_object();
        assert_eq!(s.into_bytes(), &[0x01, 0x00, 0x01, 0xFF, 0xFF]);
    }

    #[test]
    fn u32_property_uses_varint_encoding() {
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_property(2);
        s.write_u32(128); // varint 128 = 0x80 0x01
        s.end_property();
        s.end_object();
        assert_eq!(s.into_bytes(), &[0x02, 0x00, 0x80, 0x01, 0xFF, 0xFF]);
    }

    #[test]
    fn i64_property_uses_signed_varint_encoding() {
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_property(3);
        s.write_i64(-1); // signed varint -1 = 0x7F
        s.end_property();
        s.end_object();
        assert_eq!(s.into_bytes(), &[0x03, 0x00, 0x7F, 0xFF, 0xFF]);
    }

    #[test]
    fn f64_property_uses_raw_le_bytes() {
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_property(4);
        s.write_f64(1.0);
        s.end_property();
        s.end_object();
        // 1.0 in IEEE-754 double is 0x3FF0000000000000 (big-endian);
        // little-endian: 00 00 00 00 00 00 F0 3F
        assert_eq!(
            s.into_bytes(),
            &[0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0xFF, 0xFF]
        );
    }

    #[test]
    fn string_property_is_varint_length_then_bytes() {
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_property(5);
        s.write_string("hi");
        s.end_property();
        s.end_object();
        // length 2 (varint = 0x02), then bytes "hi"
        assert_eq!(s.into_bytes(), &[0x05, 0x00, 0x02, b'h', b'i', 0xFF, 0xFF]);
    }

    #[test]
    fn hugeint_property_writes_lower_then_upper() {
        // DuckDB hugeint serialises as two LEB128 signed values: lower, upper.
        // For i128 = 1: lower=1, upper=0 -> [0x01, 0x00]
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_property(6);
        s.write_hugeint(1);
        s.end_property();
        s.end_object();
        assert_eq!(s.into_bytes(), &[0x06, 0x00, 0x01, 0x00, 0xFF, 0xFF]);
    }

    #[test]
    fn optional_property_skips_field_id_when_absent() {
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_optional_property(7, false);
        s.end_optional_property(false);
        s.end_object();
        // No field_id written for an absent optional.
        assert_eq!(s.into_bytes(), &[0xFF, 0xFF]);
    }

    #[test]
    fn deserializer_reads_empty_object() {
        let bytes = [0xFFu8, 0xFF];
        let mut d = BinaryDeserializer::new(&bytes);
        d.expect_object_end().expect("terminator");
    }

    #[test]
    fn deserializer_reads_bool_property() {
        let bytes = [0x01u8, 0x00, 0x01, 0xFF, 0xFF];
        let mut d = BinaryDeserializer::new(&bytes);
        d.expect_field(1).unwrap();
        assert!(d.read_bool().unwrap());
        d.expect_object_end().unwrap();
    }

    #[test]
    fn deserializer_reads_u32_varint() {
        let bytes = [0x02u8, 0x00, 0x80, 0x01, 0xFF, 0xFF];
        let mut d = BinaryDeserializer::new(&bytes);
        d.expect_field(2).unwrap();
        assert_eq!(d.read_u32().unwrap(), 128);
        d.expect_object_end().unwrap();
    }

    #[test]
    fn deserializer_reads_string() {
        let bytes = [0x05u8, 0x00, 0x02, b'h', b'i', 0xFF, 0xFF];
        let mut d = BinaryDeserializer::new(&bytes);
        d.expect_field(5).unwrap();
        assert_eq!(d.read_string().unwrap(), "hi");
        d.expect_object_end().unwrap();
    }

    #[test]
    fn deserializer_handles_absent_optional_property() {
        // No field for optional, then terminator.
        let bytes = [0xFFu8, 0xFF];
        let mut d = BinaryDeserializer::new(&bytes);
        // Peek at the terminator: optional field 7 is absent.
        assert!(!d.read_optional(7).unwrap());
        d.expect_object_end().unwrap();
    }

    #[test]
    fn deserializer_handles_present_optional_property() {
        let bytes = [0x07u8, 0x00, 0x01, 0xFF, 0xFF];
        let mut d = BinaryDeserializer::new(&bytes);
        assert!(d.read_optional(7).unwrap());
        assert!(d.read_bool().unwrap());
        d.expect_object_end().unwrap();
    }

    #[test]
    fn write_data_ptr_roundtrips_empty_and_nonempty() {
        for payload in [&[][..], &[0xDE, 0xAD, 0xBE, 0xEF][..], &[0xAA; 4096][..]] {
            let mut s = BinarySerializer::new();
            s.begin_object();
            s.begin_property(1);
            s.write_data_ptr(payload);
            s.end_property();
            s.end_object();
            let bytes = s.into_bytes();

            let mut d = BinaryDeserializer::new(&bytes);
            d.expect_field(1).unwrap();
            let decoded = d.read_data_ptr().unwrap();
            d.expect_object_end().unwrap();
            assert_eq!(decoded, payload);
        }
    }

    #[test]
    fn deserializer_full_roundtrip_against_serializer() {
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_property(1);
        s.write_bool(true);
        s.end_property();
        s.begin_property(2);
        s.write_u32(128);
        s.end_property();
        s.begin_property(3);
        s.write_i64(-1);
        s.end_property();
        s.begin_property(4);
        s.write_string("hello");
        s.end_property();
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        d.expect_field(1).unwrap();
        assert!(d.read_bool().unwrap());
        d.expect_field(2).unwrap();
        assert_eq!(d.read_u32().unwrap(), 128);
        d.expect_field(3).unwrap();
        assert_eq!(d.read_i64().unwrap(), -1);
        d.expect_field(4).unwrap();
        assert_eq!(d.read_string().unwrap(), "hello");
        d.expect_object_end().unwrap();
    }

    #[test]
    fn list_begin_writes_count_as_varint() {
        let mut s = BinarySerializer::new();
        s.begin_object();
        s.begin_property(8);
        s.begin_list(3);
        s.write_u32(10);
        s.write_u32(20);
        s.write_u32(200); // 200 = 0xC8 0x01 in varint
        s.end_list();
        s.end_property();
        s.end_object();
        assert_eq!(
            s.into_bytes(),
            &[0x08, 0x00, 0x03, 0x0A, 0x14, 0xC8, 0x01, 0xFF, 0xFF]
        );
    }
}
