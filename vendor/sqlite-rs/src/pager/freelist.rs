// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Freelist trunk/leaf page parsing (SQLite file format, "Freelist
//! Pages"), used by [`crate::pager::Pager::allocate_page`] and
//! [`crate::pager::Pager::deallocate_page`]. Freelist pages are never part
//! of the b-tree traversal (see `src/pager.rs`'s module doc), so this is a
//! separate module rather than living in `src/btree.rs`.
//!
//! Trunk page layout (big-endian u32s):
//!   0..4   pointer to the next trunk page (0 if this is the last trunk)
//!   4..8   number of leaf page numbers that follow
//!   8..    leaf page numbers, 4 bytes each
//!
//! A leaf page's own contents are never interpreted — it is pure free
//! space, addressed only by its page number in a trunk's leaf array.

/// Errors from parsing or writing a freelist trunk page.
#[derive(Debug, PartialEq, Eq)]
pub enum FreelistError {
    /// The page buffer was too short to hold a field at the given offset.
    PageTooShort {
        /// Byte offset of the field that could not be read/written.
        offset: usize,
        /// Actual length of the page buffer.
        len: usize,
    },
}

impl std::fmt::Display for FreelistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FreelistError::PageTooShort { offset, len } => write!(
                f,
                "freelist trunk page is {len} bytes, too short to read a field at offset {offset}"
            ),
        }
    }
}

impl std::error::Error for FreelistError {}

/// Reads a big-endian `u32` at `offset` in `buf`, never panicking on a
/// truncated/corrupt page.
fn read_u32(buf: &[u8], offset: usize) -> Result<u32, FreelistError> {
    let end = offset.saturating_add(4);
    let bytes: [u8; 4] = buf
        .get(offset..end)
        .ok_or(FreelistError::PageTooShort {
            offset,
            len: buf.len(),
        })?
        .try_into()
        .map_err(|_| FreelistError::PageTooShort {
            offset,
            len: buf.len(),
        })?;
    Ok(u32::from_be_bytes(bytes))
}

/// Writes a big-endian `u32` at `offset` in `buf`, never panicking on a
/// too-short buffer.
fn write_u32(buf: &mut [u8], offset: usize, value: u32) -> Result<(), FreelistError> {
    let end = offset.saturating_add(4);
    let len = buf.len();
    let slice = buf
        .get_mut(offset..end)
        .ok_or(FreelistError::PageTooShort { offset, len })?;
    slice.copy_from_slice(&value.to_be_bytes());
    Ok(())
}

/// The maximum number of leaf page numbers a trunk page of `page_size`
/// bytes can hold: every byte after the 8-byte trunk header, 4 bytes per
/// leaf entry.
pub fn max_leaves_per_trunk(page_size: u32) -> u32 {
    (page_size.saturating_sub(8)) / 4
}

/// A parsed freelist trunk page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrunkPage {
    /// Page number of the next trunk page, or 0 if this is the last trunk.
    pub next_trunk: u32,
    /// Free page numbers listed in this trunk.
    pub leaves: Vec<u32>,
}

impl TrunkPage {
    /// Parses a trunk page's leading fields and leaf array out of a raw
    /// page buffer. A leaf count that claims more entries than the buffer
    /// actually holds is truncated to what fits, rather than erroring —
    /// best-effort recovery for a corrupt count, since a missing leaf
    /// entry only leaks a page, it never corrupts anything readable.
    pub fn parse(buf: &[u8]) -> Result<Self, FreelistError> {
        let next_trunk = read_u32(buf, 0)?;
        let claimed_count = read_u32(buf, 4)? as usize;
        let max_fit = buf.len().saturating_sub(8) / 4;
        let leaf_count = claimed_count.min(max_fit);
        let mut leaves = Vec::with_capacity(leaf_count);
        for i in 0..leaf_count {
            let offset = 8usize.saturating_add(i.saturating_mul(4));
            leaves.push(read_u32(buf, offset)?);
        }
        Ok(TrunkPage { next_trunk, leaves })
    }

    /// Writes this trunk page's fields back into a raw page buffer,
    /// zeroing any bytes past the leaf array so no stale leaf entries
    /// linger from a previous, longer version of this trunk.
    pub fn write(&self, buf: &mut [u8]) -> Result<(), FreelistError> {
        write_u32(buf, 0, self.next_trunk)?;
        write_u32(buf, 4, self.leaves.len() as u32)?;
        for (i, leaf) in self.leaves.iter().enumerate() {
            let offset = 8usize.saturating_add(i.saturating_mul(4));
            write_u32(buf, offset, *leaf)?;
        }
        let tail_start = 8usize.saturating_add(self.leaves.len().saturating_mul(4));
        if let Some(tail) = buf.get_mut(tail_start..) {
            tail.fill(0);
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_empty_trunk() {
        let mut buf = vec![0xffu8; 4096];
        let trunk = TrunkPage {
            next_trunk: 0,
            leaves: vec![],
        };
        trunk.write(&mut buf).unwrap();
        assert_eq!(TrunkPage::parse(&buf).unwrap(), trunk);
        assert!(buf[8..].iter().all(|&b| b == 0));
    }

    #[test]
    fn round_trips_trunk_with_leaves() {
        let mut buf = vec![0u8; 4096];
        let trunk = TrunkPage {
            next_trunk: 42,
            leaves: vec![7, 8, 9],
        };
        trunk.write(&mut buf).unwrap();
        assert_eq!(TrunkPage::parse(&buf).unwrap(), trunk);
    }

    #[test]
    fn max_leaves_matches_page_size() {
        assert_eq!(max_leaves_per_trunk(4096), (4096 - 8) / 4);
        assert_eq!(max_leaves_per_trunk(512), (512 - 8) / 4);
    }

    #[test]
    fn parse_never_panics_on_truncated_page() {
        let buf = vec![0u8; 4];
        assert!(matches!(
            TrunkPage::parse(&buf),
            Err(FreelistError::PageTooShort { .. })
        ));
    }

    #[test]
    fn parse_truncates_overclaimed_leaf_count() {
        let mut buf = vec![0u8; 16];
        write_u32(&mut buf, 0, 0).unwrap();
        write_u32(&mut buf, 4, 1000).unwrap();
        let trunk = TrunkPage::parse(&buf).unwrap();
        assert_eq!(trunk.leaves.len(), (16 - 8) / 4);
    }
}
