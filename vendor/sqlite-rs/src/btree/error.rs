// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use crate::pager::PagerError;
use crate::record::RecordError;
use crate::vfs::PageError;

/// Errors arising from b-tree page decoding, cursor traversal, and
/// row/index mutation.
#[derive(Debug)]
pub enum BtreeError {
    /// A cell's key record failed to decode.
    InvalidKeyRecord(RecordError),

    /// The pager/VFS failed to read the given page.
    PageSource {
        /// Page that failed to load.
        page_num: u32,
        /// Underlying VFS read failure.
        source: PageError,
    },

    /// A page's byte buffer is smaller than a b-tree page header requires.
    PageTooShort {
        /// Page that is too short.
        page_num: u32,
        /// Actual length of the page buffer, in bytes.
        len: usize,
    },

    /// A cursor operation was invoked before the cursor was positioned.
    CursorNotPositioned {
        /// Operation that required a positioned cursor.
        operation: &'static str,
        /// Positioning call the caller must make first.
        required: &'static str,
    },

    /// A page's type byte does not match any known b-tree page type.
    UnexpectedPageType {
        /// Page with the unexpected type.
        page_num: u32,
        /// Raw page-type byte found on the page.
        page_type: u8,
    },

    /// A cell pointer array index resolved outside the page's cell content area.
    InvalidCellPointer {
        /// Page containing the invalid cell pointer.
        page_num: u32,
        /// Index into the cell pointer array that was out of bounds.
        index: usize,
    },

    /// A varint embedded in a cell (payload length or rowid) failed to decode.
    InvalidCellVarint {
        /// Page containing the cell.
        page_num: u32,
        /// Underlying varint/record decode failure.
        source: RecordError,
    },

    /// A cell's payload bytes end before its declared local payload size.
    PayloadTooShort {
        /// Page containing the truncated cell.
        page_num: u32,
    },

    /// A cell declares a payload length exceeding what the file format allows.
    PayloadTooLarge {
        /// Page containing the offending cell.
        page_num: u32,
        /// Declared payload length, in bytes.
        payload_len: u64,
    },

    /// An overflow chain was followed for more than `max` pages without terminating.
    OverflowChainTooLong {
        /// First page of the overflow chain.
        page_num: u32,
        /// Maximum chain length allowed before this is treated as a cycle.
        max: usize,
    },

    /// An overflow chain revisited a page it had already traversed.
    OverflowChainCycle {
        /// First page of the overflow chain.
        page_num: u32,
        /// Page that was visited twice.
        revisited_page: u32,
    },

    /// An overflow chain terminated before yielding all declared payload bytes.
    OverflowChainTruncated {
        /// First page of the overflow chain.
        page_num: u32,
    },

    /// A cursor traversal visited more than `max` pages, indicating a likely cycle.
    TraversalTooLong {
        /// Maximum number of pages allowed in a single traversal.
        max: usize,
    },

    /// A lower-level pager operation failed.
    Pager(PagerError),

    /// An insert targeted a rowid that already exists in the table.
    DuplicateRowid {
        /// Rowid that already exists.
        rowid: i64,
    },

    /// An interior page's cells contain no entry routing to the given child page.
    MissingChildRoute {
        /// Interior page missing the routing entry.
        page_num: u32,
        /// Child page that could not be located.
        child: u32,
    },

    /// A delete targeted a rowid that does not exist in the table.
    RowidNotFound {
        /// Rowid that could not be found.
        rowid: i64,
    },

    /// An insert into a unique index targeted a key that already exists.
    DuplicateKey,

    /// A delete targeted an index key that does not exist.
    KeyNotFound,

    /// A `sqlite_master` entry names a rootpage number outside the valid page range.
    ///
    /// `name` is leaked (via [`Box::leak`]) rather than owned as a `String`:
    /// this is a cold, rare corruption-detection path, and leaking here keeps
    /// `BtreeError` itself free of drop glue (`needs_drop::<BtreeError>() ==
    /// false`), which matters because it is constructed on the hot
    /// `TableCursor::seek` path (see #679).
    InvalidRootPage {
        /// Name of the offending `sqlite_master` entry.
        name: &'static str,
        /// Out-of-range rootpage value.
        rootpage: i64,
    },

    /// A delete targeted a `sqlite_master` entry that does not exist.
    ///
    /// `name` is leaked for the same reason as [`BtreeError::InvalidRootPage`].
    MasterEntryNotFound {
        /// Name of the entry that could not be found.
        name: &'static str,
    },

    /// An internal invariant was violated; the message describes what was expected.
    Internal(&'static str),
}

impl std::fmt::Display for BtreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BtreeError::InvalidKeyRecord(source) => write!(f, "decoding a key record: {source}"),
            BtreeError::PageSource { page_num, source } => {
                write!(f, "reading page {page_num}: {source}")
            }
            BtreeError::PageTooShort { page_num, len } => write!(
                f,
                "page {page_num} is too short ({len} bytes) to contain a b-tree page header"
            ),
            BtreeError::CursorNotPositioned {
                operation,
                required,
            } => write!(
                f,
                "{operation} called on a cursor that was never positioned by {required}"
            ),
            BtreeError::UnexpectedPageType {
                page_num,
                page_type,
            } => write!(
                f,
                "page {page_num} has unexpected b-tree page type {page_type:#x}"
            ),
            BtreeError::InvalidCellPointer { page_num, index } => write!(
                f,
                "page {page_num} cell pointer at index {index} is out of bounds"
            ),
            BtreeError::InvalidCellVarint { page_num, source } => {
                write!(f, "page {page_num} cell varint decode failed: {source}")
            }
            BtreeError::PayloadTooShort { page_num } => write!(
                f,
                "page {page_num} cell payload is shorter than its declared local size"
            ),
            BtreeError::PayloadTooLarge {
                page_num,
                payload_len,
            } => write!(
                f,
                "page {page_num} declares an implausible payload length {payload_len}"
            ),
            BtreeError::OverflowChainTooLong { page_num, max } => write!(
                f,
                "overflow chain from page {page_num} exceeded {max} pages (possible cycle)"
            ),
            BtreeError::OverflowChainCycle {
                page_num,
                revisited_page,
            } => write!(
                f,
                "overflow chain from page {page_num} revisited page {revisited_page} (cycle)"
            ),
            BtreeError::OverflowChainTruncated { page_num } => write!(
                f,
                "overflow chain from page {page_num} ended before all payload bytes were read"
            ),
            BtreeError::TraversalTooLong { max } => write!(
                f,
                "b-tree traversal visited more than {max} pages (possible cycle)"
            ),
            BtreeError::Pager(source) => write!(f, "pager error: {source}"),
            BtreeError::DuplicateRowid { rowid } => {
                write!(f, "cannot insert duplicate rowid {rowid}")
            }
            BtreeError::MissingChildRoute { page_num, child } => write!(
                f,
                "interior page {page_num} has no routing entry for child page {child}"
            ),
            BtreeError::RowidNotFound { rowid } => {
                write!(f, "cannot delete rowid {rowid}: no such row")
            }
            BtreeError::DuplicateKey => write!(f, "cannot insert duplicate index key"),
            BtreeError::KeyNotFound => write!(f, "cannot delete index key: no such entry"),
            BtreeError::InvalidRootPage { name, rootpage } => write!(
                f,
                "sqlite_master entry {name:?} has out-of-range rootpage {rootpage}"
            ),
            BtreeError::MasterEntryNotFound { name } => write!(
                f,
                "cannot delete sqlite_master entry {name:?}: no such entry"
            ),
            BtreeError::Internal(msg) => write!(f, "internal invariant violated: {msg}"),
        }
    }
}

impl std::error::Error for BtreeError {}

impl From<RecordError> for BtreeError {
    fn from(source: RecordError) -> Self {
        BtreeError::InvalidKeyRecord(source)
    }
}

impl From<PagerError> for BtreeError {
    fn from(source: PagerError) -> Self {
        BtreeError::Pager(source)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn display_all_variants() {
        assert!(BtreeError::InvalidKeyRecord(RecordError::InvalidUtf8)
            .to_string()
            .contains("decoding a key record"));
        assert!(BtreeError::PageSource {
            page_num: 1,
            source: PageError::InvalidPageNumber,
        }
        .to_string()
        .contains("reading page 1"));
        assert_eq!(
            BtreeError::PageTooShort {
                page_num: 1,
                len: 3
            }
            .to_string(),
            "page 1 is too short (3 bytes) to contain a b-tree page header"
        );
        assert_eq!(
            BtreeError::CursorNotPositioned {
                operation: "next",
                required: "seek",
            }
            .to_string(),
            "next called on a cursor that was never positioned by seek"
        );
        assert_eq!(
            BtreeError::UnexpectedPageType {
                page_num: 2,
                page_type: 0xff
            }
            .to_string(),
            "page 2 has unexpected b-tree page type 0xff"
        );
        assert_eq!(
            BtreeError::InvalidCellPointer {
                page_num: 2,
                index: 9
            }
            .to_string(),
            "page 2 cell pointer at index 9 is out of bounds"
        );
        assert!(BtreeError::InvalidCellVarint {
            page_num: 2,
            source: RecordError::InvalidUtf8,
        }
        .to_string()
        .contains("cell varint decode failed"));
        assert_eq!(
            BtreeError::PayloadTooShort { page_num: 2 }.to_string(),
            "page 2 cell payload is shorter than its declared local size"
        );
        assert_eq!(
            BtreeError::PayloadTooLarge {
                page_num: 2,
                payload_len: 999
            }
            .to_string(),
            "page 2 declares an implausible payload length 999"
        );
        assert_eq!(
            BtreeError::OverflowChainTooLong {
                page_num: 2,
                max: 10
            }
            .to_string(),
            "overflow chain from page 2 exceeded 10 pages (possible cycle)"
        );
        assert_eq!(
            BtreeError::OverflowChainCycle {
                page_num: 2,
                revisited_page: 3
            }
            .to_string(),
            "overflow chain from page 2 revisited page 3 (cycle)"
        );
        assert_eq!(
            BtreeError::OverflowChainTruncated { page_num: 2 }.to_string(),
            "overflow chain from page 2 ended before all payload bytes were read"
        );
        assert_eq!(
            BtreeError::TraversalTooLong { max: 100 }.to_string(),
            "b-tree traversal visited more than 100 pages (possible cycle)"
        );
        assert!(BtreeError::Pager(PagerError::PendingTransaction)
            .to_string()
            .contains("pager error"));
        assert_eq!(
            BtreeError::DuplicateRowid { rowid: 5 }.to_string(),
            "cannot insert duplicate rowid 5"
        );
        assert_eq!(
            BtreeError::MissingChildRoute {
                page_num: 2,
                child: 4
            }
            .to_string(),
            "interior page 2 has no routing entry for child page 4"
        );
        assert_eq!(
            BtreeError::RowidNotFound { rowid: 5 }.to_string(),
            "cannot delete rowid 5: no such row"
        );
        assert_eq!(
            BtreeError::DuplicateKey.to_string(),
            "cannot insert duplicate index key"
        );
        assert_eq!(
            BtreeError::KeyNotFound.to_string(),
            "cannot delete index key: no such entry"
        );
        assert_eq!(
            BtreeError::InvalidRootPage {
                name: "t",
                rootpage: -1
            }
            .to_string(),
            "sqlite_master entry \"t\" has out-of-range rootpage -1"
        );
        assert_eq!(
            BtreeError::MasterEntryNotFound { name: "t" }.to_string(),
            "cannot delete sqlite_master entry \"t\": no such entry"
        );
        assert_eq!(
            BtreeError::Internal("bad state").to_string(),
            "internal invariant violated: bad state"
        );
    }

    #[test]
    fn from_conversions() {
        let e: BtreeError = RecordError::InvalidUtf8.into();
        assert!(matches!(e, BtreeError::InvalidKeyRecord(_)));

        let e: BtreeError = PagerError::PendingTransaction.into();
        assert!(matches!(e, BtreeError::Pager(_)));
    }

    #[test]
    fn implements_std_error() {
        let err = BtreeError::DuplicateKey;
        assert!(std::error::Error::source(&err).is_none());
    }
}
