//! Batch-interval progress reporting for the streaming compaction writer
//! (Phase 4c distributed-compaction hardening, Task 2).
//!
//! `write_data_files_streaming`'s batch loop is the only point in a
//! `compact_file_group` rewrite that ticks at a fine enough grain to bound a
//! mid-compute hang: the delete-applying read and the rolling write both
//! flow through it one `RecordBatch` at a time. [`ProgressReporter`] wraps a
//! caller-supplied callback with a batch-count interval so the writer loop
//! can call `on_batch` unconditionally after every batch without needing to
//! know or care how often the callback should actually fire.
//!
//! Deliberately free of any Iceberg/Arrow/Flight types so it is trivial to
//! unit-test the interval/monotonicity behavior in isolation (see the tests
//! below), independent of the S3-gated end-to-end rewrite tests.

/// Fires a caller-supplied callback every `interval_batches` calls to
/// [`Self::on_batch`], carrying the cumulative row count at the time of the
/// call.
///
/// `interval_batches == 0` is treated the same as `1` (fire every batch)
/// rather than dividing by zero or never firing; a caller that wants no
/// progress reporting at all should pass `None` for the `Option<ProgressReporter>`
/// itself instead of constructing one with a zero interval.
pub struct ProgressReporter {
    interval_batches: usize,
    batches_since_last: usize,
    callback: Box<dyn FnMut(u64) + Send>,
}

impl ProgressReporter {
    /// Build a reporter that calls `callback(rows_read_so_far)` once every
    /// `interval_batches` calls to [`Self::on_batch`].
    pub fn new(interval_batches: usize, callback: Box<dyn FnMut(u64) + Send>) -> Self {
        Self {
            interval_batches: interval_batches.max(1),
            batches_since_last: 0,
            callback,
        }
    }

    /// Record that one more batch was processed, with `rows_read_so_far` the
    /// cumulative row count after that batch. Fires the callback once the
    /// configured interval is reached and resets the counter; a no-op on
    /// every other call.
    pub fn on_batch(&mut self, rows_read_so_far: u64) {
        self.batches_since_last += 1;
        if self.batches_since_last >= self.interval_batches {
            self.batches_since_last = 0;
            (self.callback)(rows_read_so_far);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn recording_reporter(interval: usize) -> (ProgressReporter, Arc<Mutex<Vec<u64>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_cb = seen.clone();
        let reporter = ProgressReporter::new(
            interval,
            Box::new(move |rows| seen_for_cb.lock().unwrap().push(rows)),
        );
        (reporter, seen)
    }

    #[test]
    fn fires_every_interval_batches_and_no_more() {
        let (mut reporter, seen) = recording_reporter(4);
        // 10 batches, one row per batch => floor(10 / 4) = 2 callback firings.
        for rows in 1..=10u64 {
            reporter.on_batch(rows);
        }
        let seen = seen.lock().unwrap();
        assert_eq!(
            *seen,
            vec![4, 8],
            "must fire at batch 4 and batch 8, not before/after"
        );
    }

    #[test]
    fn rows_read_is_monotonically_increasing_across_firings() {
        let (mut reporter, seen) = recording_reporter(3);
        let cumulative_rows = [10u64, 25, 40, 61, 90, 100, 130];
        for rows in cumulative_rows {
            reporter.on_batch(rows);
        }
        let seen = seen.lock().unwrap();
        assert!(
            seen.windows(2).all(|w| w[1] > w[0]),
            "progress values must be strictly increasing: {seen:?}"
        );
        assert_eq!(*seen, vec![40, 100]);
    }

    #[test]
    fn interval_of_zero_behaves_like_one() {
        let (mut reporter, seen) = recording_reporter(0);
        reporter.on_batch(5);
        reporter.on_batch(9);
        assert_eq!(*seen.lock().unwrap(), vec![5, 9]);
    }

    #[test]
    fn interval_larger_than_batch_count_never_fires() {
        let (mut reporter, seen) = recording_reporter(100);
        for rows in 1..=10u64 {
            reporter.on_batch(rows);
        }
        assert!(
            seen.lock().unwrap().is_empty(),
            "must not fire before the interval is reached"
        );
    }

    #[test]
    fn fires_exactly_once_when_batch_count_equals_interval() {
        let (mut reporter, seen) = recording_reporter(5);
        for rows in 1..=5u64 {
            reporter.on_batch(rows);
        }
        assert_eq!(*seen.lock().unwrap(), vec![5]);
    }
}
