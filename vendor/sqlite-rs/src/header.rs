// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! SQLite database header (bytes 0-99 of the main database file).
//!
//! Page-1 trap: the 100-byte header occupies the start of page 1, but page
//! 1's own b-tree cell-pointer array is relative to byte 0 of the page, not
//! byte 100. The b-tree layer must account for the header when computing
//! in-page offsets on page 1 — this module only parses the header itself.

use crate::record::TextEncoding;

/// Byte length of the SQLite database header (bytes 0-99 of page 1).
pub const HEADER_LEN: usize = 100;

/// Page size stock `sqlite3` has defaulted to for new databases since
/// 3.12.0 — used when bootstrapping a brand-new file with no page size
/// of its own to inherit (#448).
pub const DEFAULT_PAGE_SIZE: u32 = 4096;

const MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// Failure parsing or validating a [`DatabaseHeader`].
#[derive(Debug, PartialEq, Eq)]
pub enum HeaderError {
    /// `buf` is shorter than [`HEADER_LEN`].
    TooShort {
        /// Actual length of the buffer that was passed in.
        len: usize,
    },

    /// Bytes 0-15 don't match the SQLite magic string.
    InvalidMagic,

    /// Bytes 16-17 don't encode a valid page size.
    InvalidPageSize {
        /// The raw, unresolved 16-bit page-size field.
        raw: u16,
    },

    /// Byte 18 or 19 (write/read version) is neither 1 nor 2.
    InvalidFileFormatVersion {
        /// Which of the two version bytes was invalid.
        field: VersionField,
        /// The invalid byte value.
        value: u8,
    },

    /// Byte 20 (reserved space) leaves no usable bytes in the page.
    InvalidReservedSpace {
        /// The invalid reserved-space byte.
        reserved_space: u8,
        /// The page size it was checked against.
        page_size: u32,
    },

    /// Bytes 56-59 don't encode a recognized text encoding.
    InvalidTextEncoding {
        /// The raw, unrecognized text-encoding value.
        raw: u32,
    },
}

impl std::fmt::Display for HeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeaderError::TooShort { len } => {
                write!(f, "header is {len} bytes, need at least 100")
            }
            HeaderError::InvalidMagic => {
                write!(f, "missing or invalid SQLite magic string")
            }
            HeaderError::InvalidPageSize { raw } => write!(
                f,
                "invalid page size encoding {raw} (must be a power of two from 512 to 32768, or 1 for 65536)"
            ),
            HeaderError::InvalidFileFormatVersion { field, value } => write!(
                f,
                "invalid {field:?} version byte {value} (must be 1 or 2)"
            ),
            HeaderError::InvalidReservedSpace {
                reserved_space,
                page_size,
            } => write!(
                f,
                "reserved space {reserved_space} leaves no usable bytes in a {page_size}-byte page"
            ),
            HeaderError::InvalidTextEncoding { raw } => {
                write!(f, "invalid text encoding {raw} (must be 1, 2, or 3)")
            }
        }
    }
}

impl std::error::Error for HeaderError {}

/// Which header byte an [`HeaderError::InvalidFileFormatVersion`] refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionField {
    /// Byte 18, the file format write version.
    Write,
    /// Byte 19, the file format read version.
    Read,
}

/// The journal mode declared by the write/read version bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    /// The rollback journal (write/read version bytes not both 2).
    Legacy,
    /// Write-ahead logging (write/read version bytes both 2).
    Wal,
}

/// `PRAGMA synchronous` (#645): how aggressively `Pager::flush` fsyncs
/// on commit. Unlike [`JournalMode`], this is never read from or
/// written to the header bytes — stock SQLite keeps it purely as
/// per-connection state, defaulting to `Full` on every fresh
/// connection, and so does `Pager`. Lives here (rather than
/// `src/pager.rs`, its otherwise-natural home) only so `vdbe/pragma.rs`
/// can name it without importing `crate::pager` directly, which spec
/// 001-architecture Requirement 1 ("VDBE does not know file format")
/// forbids — see ADR-0036.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SynchronousMode {
    /// No fsyncs at all on commit — fastest, least crash-safe.
    Off = 0,
    /// Skips the fsync(s) stock `FULL` performs that aren't needed for
    /// basic crash *consistency* (as opposed to guaranteeing durability
    /// of the most recent commits after a power loss).
    Normal = 1,
    /// fsyncs at every point needed to guarantee a commit survives a
    /// crash or power loss — this pager's behavior before #645, and
    /// still the default.
    #[default]
    Full = 2,
}

/// The parsed 100-byte SQLite database header.
///
/// See the page-1 trap note in the module doc: this struct describes only
/// the header itself, not how page 1's cell pointers are addressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseHeader {
    /// Bytes 16-17, after the `1` = 65536 encoding is resolved.
    pub page_size: u32,
    /// Byte 18: file format write version.
    pub write_version: u8,
    /// Byte 19: file format read version.
    pub read_version: u8,
    /// Byte 20: bytes reserved at the end of each page.
    pub reserved_space: u8,
    /// Bytes 28-31: number of pages in the database.
    pub page_count: u32,
    /// Bytes 32-35: page number of the first freelist trunk page (0 if none).
    pub freelist_trunk_page: u32,
    /// Bytes 36-39: total number of freelist pages.
    pub freelist_page_count: u32,
    /// Bytes 40-43: schema cookie.
    pub schema_cookie: u32,
    /// Bytes 44-47: schema format number.
    pub schema_format: u32,
    /// Bytes 52-55: page number of the largest root b-tree page in
    /// auto-vacuum/incremental-vacuum mode (0 otherwise). Non-zero means
    /// pointer-map pages are interleaved in the file.
    pub largest_root_btree_page: u32,
    /// Bytes 56-59: database text encoding.
    pub text_encoding: TextEncoding,
    /// Bytes 60-63: user version, set by the `user_version` pragma.
    pub user_version: u32,
    /// Bytes 68-71: application ID, set by the `application_id` pragma.
    pub application_id: u32,
}

/// Reads one byte at `offset`. `buf` is assumed already length-checked
/// against [`HEADER_LEN`] by [`DatabaseHeader::parse`]; this still returns
/// `Err` rather than indexing directly, so the bound never has to be
/// re-proven by inspection.
fn read_u8(buf: &[u8], offset: usize) -> Result<u8, HeaderError> {
    buf.get(offset)
        .copied()
        .ok_or(HeaderError::TooShort { len: buf.len() })
}

/// Reads a big-endian `u32` at `offset..offset+4`.
fn read_u32(buf: &[u8], offset: usize) -> Result<u32, HeaderError> {
    let end = offset
        .checked_add(4)
        .ok_or(HeaderError::TooShort { len: buf.len() })?;
    let bytes: [u8; 4] = buf
        .get(offset..end)
        .ok_or(HeaderError::TooShort { len: buf.len() })?
        .try_into()
        .map_err(|_| HeaderError::TooShort { len: buf.len() })?;
    Ok(u32::from_be_bytes(bytes))
}

/// Writes `bytes` at `offset` into `buf`, silently doing nothing if that
/// range is out of bounds — `new_empty_page1` builds `buf` at
/// `page_size` bytes (parse() requires `page_size >= 512`), so every call
/// site here is in range by construction; this just avoids the panicking
/// index/slice syntax clippy flags in library code.
fn put(buf: &mut [u8], offset: usize, bytes: &[u8]) {
    if let Some(slice) = buf.get_mut(offset..offset.saturating_add(bytes.len())) {
        slice.copy_from_slice(bytes);
    }
}

impl DatabaseHeader {
    /// Parses the 100-byte database header from the start of a database
    /// file. `buf` may be longer (e.g. a full page) but must be at least
    /// [`HEADER_LEN`] bytes. Never panics: malformed input returns `Err`.
    pub fn parse(buf: &[u8]) -> Result<Self, HeaderError> {
        if buf.len() < HEADER_LEN {
            return Err(HeaderError::TooShort { len: buf.len() });
        }

        if buf.get(0..16) != Some(MAGIC.as_slice()) {
            return Err(HeaderError::InvalidMagic);
        }

        let raw_page_size = u16::from_be_bytes([read_u8(buf, 16)?, read_u8(buf, 17)?]);
        let page_size: u32 = if raw_page_size == 1 {
            65536
        } else {
            raw_page_size as u32
        };
        if page_size < 512 || !page_size.is_power_of_two() {
            return Err(HeaderError::InvalidPageSize { raw: raw_page_size });
        }

        let write_version = read_u8(buf, 18)?;
        if !matches!(write_version, 1 | 2) {
            return Err(HeaderError::InvalidFileFormatVersion {
                field: VersionField::Write,
                value: write_version,
            });
        }
        let read_version = read_u8(buf, 19)?;
        if !matches!(read_version, 1 | 2) {
            return Err(HeaderError::InvalidFileFormatVersion {
                field: VersionField::Read,
                value: read_version,
            });
        }

        let reserved_space = read_u8(buf, 20)?;
        if reserved_space as u32 >= page_size {
            return Err(HeaderError::InvalidReservedSpace {
                reserved_space,
                page_size,
            });
        }

        let page_count = read_u32(buf, 28)?;
        let freelist_trunk_page = read_u32(buf, 32)?;
        let freelist_page_count = read_u32(buf, 36)?;
        let schema_cookie = read_u32(buf, 40)?;
        let schema_format = read_u32(buf, 44)?;
        let largest_root_btree_page = read_u32(buf, 52)?;

        let text_encoding_raw = read_u32(buf, 56)?;
        let text_encoding = match text_encoding_raw {
            1 => TextEncoding::Utf8,
            2 => TextEncoding::Utf16Le,
            3 => TextEncoding::Utf16Be,
            other => return Err(HeaderError::InvalidTextEncoding { raw: other }),
        };

        let user_version = read_u32(buf, 60)?;
        let application_id = read_u32(buf, 68)?;

        Ok(DatabaseHeader {
            page_size,
            write_version,
            read_version,
            reserved_space,
            page_count,
            freelist_trunk_page,
            freelist_page_count,
            schema_cookie,
            schema_format,
            largest_root_btree_page,
            text_encoding,
            user_version,
            application_id,
        })
    }

    /// The journal mode declared by the write/read version bytes.
    pub fn journal_mode(&self) -> JournalMode {
        if self.write_version == 2 && self.read_version == 2 {
            JournalMode::Wal
        } else {
            JournalMode::Legacy
        }
    }

    /// Usable bytes per page: `page_size - reserved_space`.
    #[allow(
        clippy::arithmetic_side_effects,
        reason = "parse() rejects reserved_space >= page_size, so this never underflows"
    )]
    pub fn usable_page_size(&self) -> u32 {
        self.page_size - self.reserved_space as u32
    }

    /// Builds a fresh page 1 (header bytes + an empty leaf schema table,
    /// `page_size` bytes total) for a brand-new database file that has no
    /// bytes of its own yet — the CLI `exec`/`repl` bootstrap path (#448)
    /// writes this once before running a script's first statement, the
    /// same way stock `sqlite3 <file> "<sql>"` lazily creates its target
    /// file. Byte layout mirrors [`crate::btree::write_leaf_page`]'s
    /// empty-page case: page type `0x0d` (leaf table), zero cells, cell
    /// content area starting at `page_size` (all still-free space).
    #[allow(
        clippy::cast_possible_truncation,
        reason = "parse() requires page_size to be a power of two >= 512; the 65536 case is the only one needing the on-disk 1-encoding, handled below"
    )]
    pub fn new_empty_page1(page_size: u32) -> Vec<u8> {
        let mut page1 = vec![0u8; page_size as usize];
        put(&mut page1, 0, MAGIC);
        let raw_page_size: u16 = if page_size == 65536 {
            1
        } else {
            page_size as u16
        };
        put(&mut page1, 16, &raw_page_size.to_be_bytes());
        put(&mut page1, 18, &[1]); // write_version
        put(&mut page1, 19, &[1]); // read_version
        put(&mut page1, 21, &[64]); // max embedded payload fraction
        put(&mut page1, 22, &[32]); // min embedded payload fraction
        put(&mut page1, 23, &[32]); // leaf payload fraction
        put(&mut page1, 28, &1u32.to_be_bytes()); // page_count
        put(&mut page1, 44, &4u32.to_be_bytes()); // schema_format
        put(&mut page1, 56, &1u32.to_be_bytes()); // text_encoding = UTF-8

        // Page 1's b-tree header starts right after the 100-byte file
        // header; an empty leaf page's cell content area starts at the
        // very end of the page (nothing written into it yet).
        put(&mut page1, HEADER_LEN, &[0x0d]); // LEAF_TABLE
        let content_start: u16 = if page_size == 65536 {
            0
        } else {
            page_size as u16
        };
        put(&mut page1, HEADER_LEN + 5, &content_start.to_be_bytes());

        page1
    }
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
    use std::path::Path;

    fn fixture(family: &str, name: &str) -> Vec<u8> {
        // `cargo test` runs with the working directory set to the crate
        // root, so a path relative to it needs no `env!("CARGO_MANIFEST_DIR")`
        // — the mvl-limit gate (Makefile) doesn't allow that macro here.
        let path = Path::new("tests/corpus/fixtures").join(family).join(name);
        std::fs::read(&path).unwrap_or_else(|e| panic!("reading fixture {path:?}: {e}"))
    }

    #[test]
    fn page_size_512() {
        let header = DatabaseHeader::parse(&fixture("pagesizes", "page_size_512.db")).unwrap();
        assert_eq!(header.page_size, 512);
        assert_eq!(header.reserved_space, 0);
        assert_eq!(header.journal_mode(), JournalMode::Legacy);
    }

    #[test]
    fn page_size_65536_via_one_encoding() {
        let header = DatabaseHeader::parse(&fixture("pagesizes", "page_size_65536.db")).unwrap();
        assert_eq!(header.page_size, 65536);
        assert_eq!(header.reserved_space, 0);
    }

    #[test]
    fn reserved_bytes_0() {
        let header = DatabaseHeader::parse(&fixture("pagesizes", "reserved_bytes_0.db")).unwrap();
        assert_eq!(header.page_size, 4096);
        assert_eq!(header.reserved_space, 0);
        assert_eq!(header.usable_page_size(), 4096);
    }

    #[test]
    fn reserved_bytes_12() {
        let header = DatabaseHeader::parse(&fixture("pagesizes", "reserved_bytes_12.db")).unwrap();
        assert_eq!(header.page_size, 4096);
        assert_eq!(header.reserved_space, 12);
        assert_eq!(header.usable_page_size(), 4084);
    }

    #[test]
    fn encoding_utf8() {
        let header = DatabaseHeader::parse(&fixture("encodings", "utf8.db")).unwrap();
        assert_eq!(header.text_encoding, TextEncoding::Utf8);
    }

    #[test]
    fn encoding_utf16le() {
        let header = DatabaseHeader::parse(&fixture("encodings", "utf16le.db")).unwrap();
        assert_eq!(header.text_encoding, TextEncoding::Utf16Le);
    }

    #[test]
    fn encoding_utf16be() {
        let header = DatabaseHeader::parse(&fixture("encodings", "utf16be.db")).unwrap();
        assert_eq!(header.text_encoding, TextEncoding::Utf16Be);
    }

    #[test]
    fn empty_file_is_too_short_not_a_panic() {
        let err = DatabaseHeader::parse(&fixture("invalid", "empty.db")).unwrap_err();
        assert_eq!(err, HeaderError::TooShort { len: 0 });
    }

    #[test]
    fn truncated_header_is_too_short_not_a_panic() {
        let bytes = fixture("invalid", "truncated.db");
        let len = bytes.len();
        let err = DatabaseHeader::parse(&bytes).unwrap_err();
        assert_eq!(err, HeaderError::TooShort { len });
    }

    #[test]
    fn bad_magic_is_rejected() {
        let err = DatabaseHeader::parse(&fixture("invalid", "magic.db")).unwrap_err();
        assert_eq!(err, HeaderError::InvalidMagic);
    }

    #[test]
    fn invalid_text_encoding_errors() {
        let mut bytes = fixture("encodings", "utf8.db");
        bytes[56..60].copy_from_slice(&99u32.to_be_bytes());
        let err = DatabaseHeader::parse(&bytes).unwrap_err();
        assert_eq!(err, HeaderError::InvalidTextEncoding { raw: 99 });
    }

    #[test]
    fn invalid_page_size_errors() {
        let mut bytes = fixture("pagesizes", "page_size_512.db");
        bytes[16..18].copy_from_slice(&3u16.to_be_bytes());
        let err = DatabaseHeader::parse(&bytes).unwrap_err();
        assert_eq!(err, HeaderError::InvalidPageSize { raw: 3 });
    }

    #[test]
    fn wal_journal_mode_detected() {
        let mut bytes = fixture("pagesizes", "reserved_bytes_0.db");
        bytes[18] = 2;
        bytes[19] = 2;
        let header = DatabaseHeader::parse(&bytes).unwrap();
        assert_eq!(header.journal_mode(), JournalMode::Wal);
    }

    #[test]
    fn mismatched_journal_version_bytes_are_legacy_not_wal() {
        // Only one of write_version/read_version is 2 here — pins the `&&`
        // in journal_mode() against a mutation to `||`, which would
        // wrongly report Wal as soon as either byte is 2.
        let mut bytes = fixture("pagesizes", "reserved_bytes_0.db");
        bytes[18] = 2;
        bytes[19] = 1;
        let header = DatabaseHeader::parse(&bytes).unwrap();
        assert_eq!(header.journal_mode(), JournalMode::Legacy);
    }

    #[test]
    fn page_size_below_512_but_power_of_two_is_rejected() {
        // 256 is a power of two but below the 512 floor — pins the `||` in
        // parse()'s page-size check against a mutation to `&&`, which
        // would wrongly accept this since is_power_of_two() alone is true.
        let mut bytes = fixture("pagesizes", "page_size_512.db");
        bytes[16..18].copy_from_slice(&256u16.to_be_bytes());
        let err = DatabaseHeader::parse(&bytes).unwrap_err();
        assert_eq!(err, HeaderError::InvalidPageSize { raw: 256 });
    }

    #[test]
    fn page_size_above_512_but_not_a_power_of_two_is_rejected() {
        // 600 clears the 512 floor but isn't a power of two — the other
        // half of the `||` boundary above.
        let mut bytes = fixture("pagesizes", "page_size_512.db");
        bytes[16..18].copy_from_slice(&600u16.to_be_bytes());
        let err = DatabaseHeader::parse(&bytes).unwrap_err();
        assert_eq!(err, HeaderError::InvalidPageSize { raw: 600 });
    }
}
