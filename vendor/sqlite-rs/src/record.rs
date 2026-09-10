// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! SQLite record format decoding: varints, serial types, and the record
//! header walk. Pure computation, no I/O — the b-tree layer hands this
//! module raw payload bytes; this module never reads a page itself.

mod collation;
mod decode;
mod encode;
mod error;
mod value;
mod varint;

pub use collation::{compare_text, Collation};
pub use decode::{decode_column, decode_record, decode_record_upto, record_column_count};
pub(crate) use decode::{decode_record_only_into, decode_serial_value, parse_header_into};
pub(crate) use encode::encode_varint;
pub use encode::{encode_record, encode_record_into};
pub use error::RecordError;
pub use value::{TextEncoding, Value};
pub use varint::decode_varint;
