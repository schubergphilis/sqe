//! Durable [`SeqCursor`] persisting the last-shipped sequence so audit export
//! resumes after a restart without gaps or duplicates.

use std::path::PathBuf;

/// Persists "the highest audit `integrity.seq` already shipped" to a small file.
///
/// The file contains a single decimal `u64`. A missing or corrupt file resets to
/// `last = 0` and marks the cursor as `fresh` so the shipper can decide whether
/// to seek to the spool tail rather than backfilling history.
pub struct SeqCursor {
    path: PathBuf,
    last: u64,
    /// True when the cursor was initialised from a missing or corrupt file.
    pub fresh: bool,
}

impl SeqCursor {
    /// Load the cursor from `path`.
    ///
    /// If the file is missing or cannot be parsed as a `u64`, the cursor resets
    /// to `last = 0` and `fresh = true`.
    ///
    /// `start_at_beginning` is reserved for future shipper use; the current
    /// implementation always resets corrupt/missing files to 0 regardless of
    /// this flag. The shipper (Task 8) uses `fresh` to decide whether to seek
    /// to the spool tail when `start_at_beginning == false`.
    pub fn load(path: PathBuf, _start_at_beginning: bool) -> Self {
        match std::fs::read_to_string(&path) {
            Ok(contents) => match contents.trim().parse::<u64>() {
                Ok(seq) => SeqCursor { path, last: seq, fresh: false },
                Err(_) => SeqCursor { path, last: 0, fresh: true },
            },
            Err(_) => SeqCursor { path, last: 0, fresh: true },
        }
    }

    /// The highest seq already shipped. A record with `seq <= last()` is skipped.
    pub fn last(&self) -> u64 {
        self.last
    }

    /// Advance the cursor to `seq` and fsync to disk so it survives a crash.
    pub fn advance_to(&mut self, seq: u64) -> std::io::Result<()> {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;
        write!(file, "{seq}")?;
        file.sync_all()?;
        self.last = seq;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SeqCursor;

    #[test]
    fn cursor_roundtrips_and_fsyncs() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("audit.cursor");
        let mut c = SeqCursor::load(p.clone(), true);
        assert_eq!(c.last(), 0);
        c.advance_to(7).unwrap();
        let c2 = SeqCursor::load(p, true);
        assert_eq!(c2.last(), 7);
    }

    #[test]
    fn cursor_corrupt_file_resets() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("audit.cursor");
        std::fs::write(&p, b"not-a-number").unwrap();
        let c = SeqCursor::load(p, true);
        assert_eq!(c.last(), 0); // corrupt -> reset to start
    }
}
