// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::freelist::FreelistError;
use super::journal::JournalError;
use super::wal::WalError;
use crate::vfs::{PageError, VfsError};

/// Errors surfaced by the pager while opening a database, replaying
/// recovery state, or servicing page reads/writes.
#[derive(Debug)]
pub enum PagerError {
    /// A hot rollback journal exists at `path`: the previous writer did not
    /// clean up, so the main database file may not reflect committed data.
    HotJournal {
        /// Path to the hot journal file that triggered the refusal.
        path: String,
    },

    /// The rollback journal itself failed to parse.
    Journal(JournalError),

    /// The write-ahead log at `path` failed to parse or validate.
    Wal {
        /// Path to the WAL file that failed.
        path: String,
        /// The underlying WAL parsing/validation error.
        source: WalError,
    },

    /// A page-level error propagated from the storage layer.
    Page(PageError),

    /// A VFS-level I/O or locking error.
    Vfs(VfsError),

    /// A freelist trunk/leaf page failed to parse.
    Freelist(FreelistError),

    /// `journal_mode` cannot be changed while a transaction is pending.
    PendingTransaction,

    /// Switching `journal_mode` out of WAL requires a checkpoint that fully
    /// back-fills the WAL into the main file first; the checkpoint left
    /// frames behind.
    CheckpointIncomplete,
}

impl std::fmt::Display for PagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PagerError::HotJournal { path } => write!(
                f,
                "hot rollback journal present at {path}: database was not cleanly closed and its main \
                 file may not reflect committed data; refusing to open read-only rather than risk \
                 serving pre-rollback pages as committed"
            ),
            PagerError::Journal(source) => write!(f, "rollback journal is corrupt: {source}"),
            PagerError::Wal { path, source } => write!(f, "reading WAL at {path}: {source}"),
            PagerError::Page(source) => write!(f, "{source}"),
            PagerError::Vfs(source) => write!(f, "{source}"),
            PagerError::Freelist(source) => write!(f, "{source}"),
            PagerError::PendingTransaction => {
                write!(f, "cannot change journal_mode with a pending transaction")
            }
            PagerError::CheckpointIncomplete => write!(
                f,
                "checkpoint did not fully back-fill the WAL while switching journal_mode out of WAL"
            ),
        }
    }
}

impl std::error::Error for PagerError {}

impl From<PageError> for PagerError {
    fn from(source: PageError) -> Self {
        PagerError::Page(source)
    }
}

impl From<VfsError> for PagerError {
    fn from(source: VfsError) -> Self {
        PagerError::Vfs(source)
    }
}

impl From<FreelistError> for PagerError {
    fn from(source: FreelistError) -> Self {
        PagerError::Freelist(source)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn display_all_variants() {
        assert!(PagerError::HotJournal {
            path: "db.sqlite-journal".to_string()
        }
        .to_string()
        .contains("hot rollback journal present at db.sqlite-journal"));
        assert!(PagerError::Journal(JournalError::HeaderTooShort(3))
            .to_string()
            .contains("rollback journal is corrupt"));
        assert!(PagerError::Wal {
            path: "db.sqlite-wal".to_string(),
            source: WalError::HeaderTooShort { len: 2 },
        }
        .to_string()
        .contains("reading WAL at db.sqlite-wal"));
        assert_eq!(
            PagerError::Page(PageError::InvalidPageNumber).to_string(),
            "invalid page number 0"
        );
        assert_eq!(
            PagerError::Vfs(VfsError::NotFound {
                path: "x".to_string()
            })
            .to_string(),
            "file not found: x"
        );
        assert_eq!(
            PagerError::Freelist(FreelistError::PageTooShort { offset: 1, len: 2 }).to_string(),
            "freelist trunk page is 2 bytes, too short to read a field at offset 1"
        );
        assert_eq!(
            PagerError::PendingTransaction.to_string(),
            "cannot change journal_mode with a pending transaction"
        );
        assert_eq!(
            PagerError::CheckpointIncomplete.to_string(),
            "checkpoint did not fully back-fill the WAL while switching journal_mode out of WAL"
        );
    }

    #[test]
    fn from_conversions() {
        let e: PagerError = PageError::InvalidPageNumber.into();
        assert!(matches!(e, PagerError::Page(PageError::InvalidPageNumber)));

        let e: PagerError = VfsError::NotFound {
            path: "x".to_string(),
        }
        .into();
        assert!(matches!(e, PagerError::Vfs(VfsError::NotFound { .. })));

        let e: PagerError = FreelistError::PageTooShort { offset: 1, len: 2 }.into();
        assert!(matches!(e, PagerError::Freelist(_)));
    }

    #[test]
    fn implements_std_error() {
        let err = PagerError::PendingTransaction;
        assert!(std::error::Error::source(&err).is_none());
    }
}
