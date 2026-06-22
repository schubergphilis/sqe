use std::io::{BufRead, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use crate::audit::export::{ocsf_to_ship_record, LogShipExporter, SeqCursor, ShipRecord};

/// Prometheus metric handles updated by the shipper on each pass.
///
/// All fields are `Option` so the shipper compiles and runs without a metrics
/// registry attached (e.g. in unit tests that do not wire Prometheus).
#[derive(Clone, Default)]
pub struct ShipperMetrics {
    /// `sqe_audit_export_records_total{status}` counter vec.
    pub records_total: Option<prometheus::IntCounterVec>,
    /// `sqe_audit_export_batch_failures_total` counter.
    pub batch_failures_total: Option<prometheus::IntCounter>,
    /// `sqe_audit_export_spool_lag_bytes` gauge.
    pub spool_lag_bytes: Option<prometheus::Gauge>,
    /// `sqe_audit_export_cursor_seq` gauge.
    pub cursor_seq: Option<prometheus::Gauge>,
    /// `sqe_audit_export_last_success_timestamp` gauge.
    pub last_success_timestamp: Option<prometheus::Gauge>,
}

/// Where to start reading on a fresh cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartAt {
    /// Begin shipping from `cursor.last()` = 0 (nothing previously shipped).
    Beginning,
    /// Scan the spool to its current tail seq and advance the cursor there
    /// WITHOUT shipping any backlog. Records written before this point are
    /// treated as already seen.
    Now,
}

/// Result of one `ship_once` pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShipOutcome {
    /// Number of records successfully exported.
    pub shipped: usize,
    /// The seq the cursor was advanced to (0 when nothing shipped or failed).
    pub advanced_to: u64,
    /// True when `export_batch` returned an error; cursor was NOT advanced.
    pub failed: bool,
}

/// Tails the OCSF JSONL spool, batches records, and exports them via
/// `LogShipExporter`. Advances the cursor ONLY after a successful export ack
/// (at-least-once guarantee).
pub struct OtlpLogShipper {
    spool_path: PathBuf,
    cursor: SeqCursor,
    exporter: Arc<dyn LogShipExporter>,
    batch_max: usize,
    max_spool_bytes: u64,
    /// Byte offset after the newline of the last successfully acked record.
    /// The next pass seeks here before reading.
    committed_offset: u64,
    /// Tracks whether we have already emitted a warning for the current
    /// threshold crossing so we don't spam. Resets when size drops below
    /// `max_spool_bytes`.
    spool_warn_emitted: bool,
    flush_interval_ms: u64,
    /// Optional Prometheus metric handles. `None` in tests and when the
    /// coordinator does not wire a registry (e.g. disabled path).
    metrics: ShipperMetrics,
}

impl OtlpLogShipper {
    /// Create a new shipper.
    ///
    /// - `start_at = Now` with a fresh cursor: scan the spool to its current
    ///   tail seq and advance the cursor WITHOUT shipping (no backfill).
    /// - `start_at = Beginning` (or a non-fresh cursor): ship from
    ///   `cursor.last()`.
    pub fn new(
        spool_path: PathBuf,
        cursor_path: PathBuf,
        exporter: Arc<dyn LogShipExporter>,
        batch_max: usize,
        max_spool_bytes: u64,
        start_at: StartAt,
        flush_interval_ms: u64,
    ) -> Self {
        let cursor = SeqCursor::load(cursor_path, start_at == StartAt::Beginning);

        let mut cursor = cursor;
        let committed_offset = if start_at == StartAt::Now && cursor.fresh {
            // Cold-start "now": scan spool to EOF to find the tail offset and
            // the max seq already written. Advance the cursor so we do not
            // backfill old history.
            let (offset, max_seq) = Self::scan_to_tail(&spool_path, cursor.last());
            if max_seq > 0 {
                // Advance in-memory and on-disk so next pass starts after tail.
                let _ = cursor.advance_to(max_seq);
            }
            offset
        } else {
            // Beginning or resumed cursor: start from offset 0 and let the
            // seq filter skip records already acked by a previous run.
            0
        };

        OtlpLogShipper {
            spool_path,
            cursor,
            exporter,
            batch_max,
            max_spool_bytes,
            committed_offset,
            spool_warn_emitted: false,
            flush_interval_ms,
            metrics: ShipperMetrics::default(),
        }
    }

    /// Attach Prometheus metric handles. Call before `run`.
    ///
    /// When not called (or called with a default `ShipperMetrics`), the shipper
    /// operates normally but does not update any metrics.
    pub fn with_metrics(mut self, m: ShipperMetrics) -> Self {
        self.metrics = m;
        self
    }

    /// Scan the spool linearly to find the byte offset after the last complete
    /// line and the max seq seen. Returns `(offset, max_seq)`.
    fn scan_to_tail(spool_path: &PathBuf, start_seq: u64) -> (u64, u64) {
        let file = match std::fs::File::open(spool_path) {
            Ok(f) => f,
            Err(_) => return (0, start_seq),
        };
        let mut reader = std::io::BufReader::new(file);
        let mut max_seq = start_seq;
        let mut offset: u64 = 0;
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(n) => {
                    // Only count complete lines (those that end in '\n').
                    if line.ends_with('\n') {
                        if let Some(rec) = ocsf_to_ship_record(line.trim_end_matches('\n')) {
                            if rec.seq > max_seq {
                                max_seq = rec.seq;
                            }
                        }
                        offset += n as u64;
                    }
                    // Incomplete last line: do not advance offset past it.
                }
                Err(_) => break,
            }
        }
        (offset, max_seq)
    }

    /// Perform one tail-batch-export-advance pass.
    ///
    /// Returns a `ShipOutcome`. The cursor advances ONLY when `export_batch`
    /// returns `Ok`. On error the cursor and `committed_offset` are unchanged.
    pub async fn ship_once(&mut self) -> ShipOutcome {
        // Check spool size and warn once per threshold crossing.
        if let Ok(meta) = std::fs::metadata(&self.spool_path) {
            let size = meta.len();
            if size > self.max_spool_bytes {
                if !self.spool_warn_emitted {
                    tracing::warn!(
                        spool = %self.spool_path.display(),
                        size_bytes = size,
                        max_bytes = self.max_spool_bytes,
                        "audit spool exceeds size threshold; shipper may be lagging"
                    );
                    self.spool_warn_emitted = true;
                }
            } else {
                self.spool_warn_emitted = false;
            }
        }

        // Open the spool and seek to the committed offset.
        let file = match std::fs::File::open(&self.spool_path) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(err = %e, "audit shipper: cannot open spool file");
                return ShipOutcome { shipped: 0, advanced_to: 0, failed: true };
            }
        };
        let mut reader = std::io::BufReader::new(file);
        if let Err(e) = reader.seek(SeekFrom::Start(self.committed_offset)) {
            tracing::warn!(err = %e, "audit shipper: seek failed");
            return ShipOutcome { shipped: 0, advanced_to: 0, failed: true };
        }

        // Read complete lines (stop at a line without a trailing newline).
        let mut records: Vec<ShipRecord> = Vec::new();
        // The byte offset at the end of each candidate line, relative to the
        // spool file start.
        let mut candidate_offset = self.committed_offset;
        // The offset at the end of the last record that was actually added to
        // `records`.
        let mut last_record_offset = self.committed_offset;

        let mut line = String::new();
        loop {
            if records.len() >= self.batch_max {
                break;
            }
            line.clear();
            let n = match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(err = %e, "audit shipper: read error");
                    break;
                }
            };

            // A line lacking a trailing newline is incomplete (half-written).
            // Do not consume it; leave it for the next pass.
            if !line.ends_with('\n') {
                break;
            }

            candidate_offset += n as u64;

            let trimmed = line.trim_end_matches('\n');
            match ocsf_to_ship_record(trimmed) {
                None => {
                    tracing::warn!(line = %trimmed, "audit shipper: failed to parse OCSF line; skipping");
                    // Advance offset past this garbled-but-complete line so we
                    // don't re-read it next pass.
                    last_record_offset = candidate_offset;
                }
                Some(rec) if rec.seq <= self.cursor.last() => {
                    // Already shipped (pre-cursor). Advance offset so we skip
                    // it efficiently on the next pass.
                    last_record_offset = candidate_offset;
                }
                Some(rec) => {
                    last_record_offset = candidate_offset;
                    records.push(rec);
                }
            }
        }

        if records.is_empty() {
            return ShipOutcome { shipped: 0, advanced_to: 0, failed: false };
        }

        let max_seq = records.iter().map(|r| r.seq).max().unwrap_or(0);
        let count = records.len();

        match self.exporter.export_batch(&records).await {
            Ok(()) => {
                if let Err(e) = self.cursor.advance_to(max_seq) {
                    tracing::warn!(err = %e, "audit shipper: failed to advance cursor");
                }
                self.committed_offset = last_record_offset;
                ShipOutcome { shipped: count, advanced_to: max_seq, failed: false }
            }
            Err(e) => {
                tracing::warn!(err = %e, "audit shipper: export_batch failed; cursor not advanced");
                ShipOutcome { shipped: 0, advanced_to: 0, failed: true }
            }
        }
    }

    /// Run the shipping loop until `shutdown` is set to `true`.
    ///
    /// - Calls `ship_once` every `flush_interval_ms`.
    /// - If a full batch was shipped, runs immediately again (no sleep).
    /// - After a failed pass, backs off exponentially up to 60 seconds.
    pub async fn run(mut self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut backoff_ms: u64 = 1_000;
        const MAX_BACKOFF_MS: u64 = 60_000;

        loop {
            // Check shutdown before starting the next pass.
            if *shutdown.borrow() {
                break;
            }

            let outcome = self.ship_once().await;

            // Update Prometheus metrics after each pass.
            self.update_metrics(&outcome);

            if outcome.failed {
                tracing::warn!(
                    backoff_ms,
                    "audit shipper: export failed; backing off before retry"
                );
                let sleep_fut = tokio::time::sleep(
                    std::time::Duration::from_millis(backoff_ms),
                );
                tokio::select! {
                    _ = sleep_fut => {}
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() { break; }
                    }
                }
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                continue;
            }

            // Reset backoff after a successful pass.
            backoff_ms = 1_000;

            // If a full batch was shipped, run immediately (may be more data).
            if outcome.shipped == self.batch_max {
                continue;
            }

            // Normal interval sleep.
            let sleep_fut =
                tokio::time::sleep(std::time::Duration::from_millis(self.flush_interval_ms));
            tokio::select! {
                _ = sleep_fut => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
            }
        }
    }

    /// Update Prometheus metrics from a `ShipOutcome`.
    fn update_metrics(&self, outcome: &ShipOutcome) {
        let m = &self.metrics;

        if outcome.failed {
            if let Some(ref c) = m.batch_failures_total {
                c.inc();
            }
            if let Some(ref cv) = m.records_total {
                cv.with_label_values(&["failure"]).inc_by(0);
            }
        } else if outcome.shipped > 0 {
            if let Some(ref cv) = m.records_total {
                cv.with_label_values(&["success"])
                    .inc_by(outcome.shipped as u64);
            }
            if let Some(ref g) = m.cursor_seq {
                g.set(outcome.advanced_to as f64);
            }
            if let Some(ref g) = m.last_success_timestamp {
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                g.set(ts);
            }
        }

        // Spool lag: file size minus committed offset.
        if let Some(ref g) = m.spool_lag_bytes {
            let lag = std::fs::metadata(&self.spool_path)
                .map(|m| m.len().saturating_sub(self.committed_offset) as f64)
                .unwrap_or(0.0);
            g.set(lag);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{sample_query_event, to_ocsf};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use tempfile::tempdir;

    // ---------------------------------------------------------------------------
    // Test stub exporter
    // ---------------------------------------------------------------------------

    struct StubExporter {
        fail: AtomicBool,
        shipped: Mutex<Vec<u64>>,
    }

    impl StubExporter {
        fn new(fail: bool) -> Arc<Self> {
            Arc::new(Self {
                fail: AtomicBool::new(fail),
                shipped: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl LogShipExporter for StubExporter {
        async fn export_batch(&self, records: &[ShipRecord]) -> Result<(), String> {
            if self.fail.load(Ordering::SeqCst) {
                return Err("collector down".into());
            }
            self.shipped
                .lock()
                .unwrap()
                .extend(records.iter().map(|r| r.seq));
            Ok(())
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    fn ocsf_line(seq: u64) -> String {
        let mut ev = sample_query_event();
        ev.integrity.seq = seq;
        serde_json::to_string(&to_ocsf(&ev)).unwrap()
    }

    fn write_spool(path: &std::path::Path, seqs: &[u64]) {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        for &seq in seqs {
            writeln!(f, "{}", ocsf_line(seq)).unwrap();
        }
    }

    fn make_shipper(
        spool: PathBuf,
        cursor_path: PathBuf,
        exporter: Arc<dyn LogShipExporter>,
        batch_max: usize,
        start_at: StartAt,
    ) -> OtlpLogShipper {
        OtlpLogShipper::new(
            spool,
            cursor_path,
            exporter,
            batch_max,
            u64::MAX, // max_spool_bytes: large so no warning in tests
            start_at,
            500,      // flush_interval_ms
        )
    }

    // ---------------------------------------------------------------------------
    // PROOF TEST: outage freezes cursor, then replays without loss
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn outage_freezes_cursor_then_replays_without_loss() {
        let dir = tempdir().unwrap();
        let spool = dir.path().join("audit.jsonl");
        let cursor_path = dir.path().join("audit.cursor");

        write_spool(&spool, &[1, 2, 3, 4, 5]);

        let stub = StubExporter::new(true); // start failing
        let mut shipper = make_shipper(
            spool,
            cursor_path.clone(),
            stub.clone(),
            10,
            StartAt::Beginning,
        );

        // Pass 1: exporter is down; cursor must stay at 0.
        let o1 = shipper.ship_once().await;
        assert!(o1.failed, "ship_once must report failed when exporter is down");
        // Reload the cursor from disk; it must still be 0 (never written).
        let disk_cursor = SeqCursor::load(cursor_path.clone(), true);
        assert_eq!(disk_cursor.last(), 0, "cursor frozen on outage");
        assert!(
            stub.shipped.lock().unwrap().is_empty(),
            "nothing should have been recorded as shipped during outage"
        );

        // Recover the exporter.
        stub.fail.store(false, Ordering::SeqCst);

        // Pass 2: should replay all 5 records in order and advance cursor to 5.
        let o2 = shipper.ship_once().await;
        assert!(!o2.failed, "ship_once must succeed after recovery");
        assert_eq!(o2.shipped, 5);
        assert_eq!(o2.advanced_to, 5);

        let seqs = stub.shipped.lock().unwrap().clone();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5], "backlog shipped in order, no loss");

        // Cursor on disk must now be 5.
        let disk_cursor2 = SeqCursor::load(cursor_path, true);
        assert_eq!(disk_cursor2.last(), 5, "cursor advanced after ack");
    }

    // ---------------------------------------------------------------------------
    // TEST: restart resumes from cursor; no gap, no duplicate
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn restart_resumes_from_cursor_no_gap() {
        let dir = tempdir().unwrap();
        let spool = dir.path().join("audit.jsonl");
        let cursor_path = dir.path().join("audit.cursor");

        write_spool(&spool, &[1, 2, 3]);

        let stub = StubExporter::new(false);
        let mut shipper = make_shipper(
            spool.clone(),
            cursor_path.clone(),
            stub.clone(),
            10,
            StartAt::Beginning,
        );

        // Ship 1..=3 successfully.
        let o1 = shipper.ship_once().await;
        assert!(!o1.failed);
        assert_eq!(o1.shipped, 3);
        assert_eq!(o1.advanced_to, 3);

        // Drop the shipper (simulates process restart).
        drop(shipper);

        // Append records 4 and 5 to the spool.
        write_spool(&spool, &[4, 5]);

        // New shipper over the same cursor; cursor.last() == 3 (non-fresh).
        let stub2 = StubExporter::new(false);
        let mut shipper2 = make_shipper(
            spool,
            cursor_path.clone(),
            stub2.clone(),
            10,
            StartAt::Beginning,
        );

        let o2 = shipper2.ship_once().await;
        assert!(!o2.failed);
        assert_eq!(o2.shipped, 2, "only records 4 and 5 should be shipped on resume");

        let seqs = stub2.shipped.lock().unwrap().clone();
        assert_eq!(seqs, vec![4, 5], "no re-shipment of already-acked records");

        let disk_cursor = SeqCursor::load(cursor_path, true);
        assert_eq!(disk_cursor.last(), 5);
    }

    // ---------------------------------------------------------------------------
    // TEST: StartAt::Now with a fresh cursor does NOT backfill existing records;
    //       a record appended AFTER construction IS shipped on the next pass.
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn start_at_now_fresh_cursor_no_backfill_then_ships_new() {
        let dir = tempdir().unwrap();
        let spool = dir.path().join("audit.jsonl");
        let cursor_path = dir.path().join("audit.cursor");

        // Pre-populate the spool with seqs 1, 2, 3 (the "backlog").
        write_spool(&spool, &[1, 2, 3]);

        let stub = StubExporter::new(false);
        let mut shipper = make_shipper(
            spool.clone(),
            cursor_path.clone(),
            stub.clone(),
            10,
            StartAt::Now,
        );

        // First pass: cursor was fresh + StartAt::Now, so the backlog is NOT shipped.
        let o1 = shipper.ship_once().await;
        assert!(!o1.failed, "ship_once should not fail");
        assert_eq!(o1.shipped, 0, "StartAt::Now must not backfill existing records");
        assert!(
            stub.shipped.lock().unwrap().is_empty(),
            "no records should be shipped on the first pass with StartAt::Now"
        );

        // Append a new record (seq 4) AFTER the shipper was constructed.
        write_spool(&spool, &[4]);

        // Second pass: only seq 4 (the new record) should be shipped.
        let o2 = shipper.ship_once().await;
        assert!(!o2.failed);
        assert_eq!(o2.shipped, 1, "only the post-construction record should be shipped");
        assert_eq!(o2.advanced_to, 4);

        let seqs = stub.shipped.lock().unwrap().clone();
        assert_eq!(seqs, vec![4], "seqs 1-3 must not be shipped; only seq 4");
    }

    // ---------------------------------------------------------------------------
    // TEST: half-written last line is NOT shipped this pass
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn tail_ignores_half_written_last_line() {
        let dir = tempdir().unwrap();
        let spool = dir.path().join("audit.jsonl");
        let cursor_path = dir.path().join("audit.cursor");

        // Write seq 1 as a complete line.
        write_spool(&spool, &[1]);

        // Append seq 2 WITHOUT a trailing newline (simulates a partial write).
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&spool)
                .unwrap();
            write!(f, "{}", ocsf_line(2)).unwrap(); // no writeln! -> no '\n'
        }

        let stub = StubExporter::new(false);
        let mut shipper = make_shipper(
            spool,
            cursor_path,
            stub.clone(),
            10,
            StartAt::Beginning,
        );

        let o = shipper.ship_once().await;
        assert!(!o.failed);
        assert_eq!(o.shipped, 1, "only the complete line (seq 1) should ship");
        assert_eq!(o.advanced_to, 1);

        let seqs = stub.shipped.lock().unwrap().clone();
        assert_eq!(seqs, vec![1], "seq 2 (incomplete line) must not be shipped");
    }
}
