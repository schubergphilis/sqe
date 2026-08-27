//! Whether a Hive external table's scan can be served by workers, and why not
//! when it cannot.
//!
//! Cycle 5 moves Hive scans onto workers, but not every table on day one. The
//! decision lives here, as a pure function over the table's shape, for two
//! reasons: it is the thing tests need to pin down without a cluster, and the
//! REASON has to survive to `EXPLAIN`.
//!
//! The failure mode this module exists to prevent is a silent fallback. A Hive
//! table that quietly runs coordinator-local still returns correct rows, so
//! nothing looks wrong; the feature simply never engages and nobody notices.
//! Every negative answer therefore carries a reason string, and the reason is
//! recorded rather than discarded.

use super::serde_map::HiveFileFormat;

/// Why a table cannot be scanned on workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unservable {
    /// The worker has no reader for this format.
    ///
    /// No format answers this today: parquet, CSV and line-delimited JSON all
    /// have worker-side readers as of cycle 5, task 5. The variant stays as the
    /// answer for a format added to `HiveFileFormat` later, which
    /// [`servability`]'s exhaustive match forces someone to give.
    Format(HiveFileFormat),
    /// Nothing to split: no partition entries were planned, so there is no
    /// per-partition unit of work to distribute.
    NoPartitionEntries,
    /// Distributed execution is switched off for Hive scans.
    Disabled,
}

impl Unservable {
    /// A short reason, shaped for `EXPLAIN` and for a log line.
    pub fn reason(&self) -> String {
        match self {
            Unservable::Format(f) => format!(
                "worker execution has no reader for this table's format, which is {}",
                match f {
                    HiveFileFormat::Csv => "csv",
                    HiveFileFormat::Json => "json",
                    HiveFileFormat::Parquet => "parquet",
                }
            ),
            Unservable::NoPartitionEntries => {
                "no partition entries to distribute; scan runs coordinator-local".to_string()
            }
            Unservable::Disabled => {
                "distributed Hive scans are disabled by configuration".to_string()
            }
        }
    }
}

/// Decide whether this scan can be dispatched to workers.
///
/// `Ok(())` means servable. `Err(reason)` means run coordinator-local AND say
/// so; it is never a query failure, because falling back is always correct.
pub fn servability(
    enabled: bool,
    format: HiveFileFormat,
    planned_entry_count: usize,
) -> Result<(), Unservable> {
    if !enabled {
        return Err(Unservable::Disabled);
    }
    // Every declared format now has a worker-side reader: parquet through
    // ParquetRecordBatchStreamBuilder, CSV and line-delimited JSON through
    // `sqe_worker::hive_reader` (cycle 5, task 5).
    //
    // Written as an exhaustive match rather than a passing `if`, on purpose. A
    // new variant on `HiveFileFormat` must not become silently servable: the
    // worker would then be dispatched a format it has no reader for and would
    // fail inside a parquet footer read, reporting a corrupt file. This makes
    // that a compile error instead, and `Unservable::Format` is the answer to
    // give when it happens.
    match format {
        HiveFileFormat::Parquet | HiveFileFormat::Csv | HiveFileFormat::Json => {}
    }
    if planned_entry_count == 0 {
        return Err(Unservable::NoPartitionEntries);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parquet_with_entries_is_servable() {
        assert!(servability(true, HiveFileFormat::Parquet, 4).is_ok());
    }

    #[test]
    fn every_declared_format_is_servable_now_that_the_readers_exist() {
        // Cycle 5 task 5 added the CSV and JSON readers. Leaving this refusal
        // in place would have made those readers unreachable while every CSV
        // table kept falling back -- which looks exactly like success.
        for fmt in [
            HiveFileFormat::Parquet,
            HiveFileFormat::Csv,
            HiveFileFormat::Json,
        ] {
            assert!(
                servability(true, fmt, 4).is_ok(),
                "{fmt:?} has a worker-side reader and must be servable"
            );
        }
    }

    #[test]
    fn the_format_reason_still_names_the_format() {
        // `Unservable::Format` is the answer for a format added later with no
        // worker reader. Its reason has to name the format, or EXPLAIN says a
        // table is unservable without saying why.
        for (fmt, want) in [(HiveFileFormat::Csv, "csv"), (HiveFileFormat::Json, "json")] {
            let reason = Unservable::Format(fmt).reason();
            assert!(
                reason.contains(want),
                "the reason must name the format so EXPLAIN says WHY, got: {reason}"
            );
        }
    }

    #[test]
    fn no_entries_is_not_servable() {
        assert_eq!(
            servability(true, HiveFileFormat::Parquet, 0),
            Err(Unservable::NoPartitionEntries)
        );
    }

    #[test]
    fn disabled_wins_over_everything_else() {
        // Checked first on purpose: when distribution is switched off the
        // answer must be "disabled", not a format complaint about a table that
        // was never going to be dispatched anyway.
        assert_eq!(
            servability(false, HiveFileFormat::Csv, 0),
            Err(Unservable::Disabled)
        );
    }

    #[test]
    fn every_reason_is_non_empty_and_mentions_no_placeholder() {
        for u in [
            Unservable::Format(HiveFileFormat::Csv),
            Unservable::NoPartitionEntries,
            Unservable::Disabled,
        ] {
            let r = u.reason();
            assert!(!r.is_empty());
            assert!(!r.contains("TODO") && !r.contains("{}"), "got: {r}");
        }
    }
}
