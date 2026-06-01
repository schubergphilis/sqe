//! In-memory time-series sampler for the web-UI metrics dashboard.
//!
//! `MetricsHistory` is a ring-buffer of `MetricsSample` values collected every
//! 10 seconds over a 12-hour window (4320 samples at capacity). The
//! `/api/v1/metrics/history` endpoint aggregates the raw ring-buffer into at
//! most 48 fixed-width buckets before sending, keeping the payload small.
//!
//! ## Response shape (bucket fields)
//!
//! Each element of `buckets` contains:
//! - `tUnixMs`        — bucket start time (floor to 900 s), Unix epoch ms
//! - `total`          — Δtotal_queries in the bucket (completed + failed, clamped ≥0)
//! - `finished`       — Δfinished_queries in the bucket (clamped ≥0)
//! - `failed`         — Δfailed_queries in the bucket (clamped ≥0)
//! - `rowsOut`        — Δtotal_output_rows in the bucket (clamped ≥0)
//! - `avgLatencyMs`   — Δexec_ms_sum / Δfinished (0 when Δfinished==0)
//! - `avgActive`      — mean active-query count within the bucket
//! - `maxActive`      — peak active-query count within the bucket
//! - `avgMemPct`      — mean memory-pool utilisation % within the bucket
//! - `maxMemPct`      — peak memory-pool utilisation % within the bucket

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::execution::runtime_env::RuntimeEnv;
use tokio_util::sync::CancellationToken;

use crate::query_tracker::QueryTracker;
use sqe_core::TaskGuard;

// ── Sample ─────────────────────────────────────────────────────

/// A single point-in-time reading from the coordinator.
#[derive(Clone, Debug)]
pub struct MetricsSample {
    pub unix_ms: u64,
    pub active_queries: usize,
    pub mem_used_bytes: u64,
    pub mem_limit_bytes: u64,
    pub total_queries: usize,
    pub failed_queries: usize,
    /// Cumulative output rows across all tracked query records at sample time.
    pub total_output_rows: u64,
    /// Count of records in `QueryState::Finished` at sample time.
    pub finished_queries: usize,
    /// Sum of `execution_ms` across all `QueryState::Finished` records at sample time.
    pub exec_ms_sum: u64,
}

// ── Ring-buffer ────────────────────────────────────────────────

/// Thread-safe bounded ring-buffer of `MetricsSample` values.
pub struct MetricsHistory {
    inner: Mutex<VecDeque<MetricsSample>>,
    capacity: usize,
}

impl MetricsHistory {
    /// Create a new ring-buffer with the given `capacity`.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Append a sample. When the buffer is full the oldest entry is dropped.
    pub fn record(&self, sample: MetricsSample) {
        let mut buf = self.inner.lock().expect("MetricsHistory mutex poisoned");
        if buf.len() >= self.capacity {
            buf.pop_front();
        }
        buf.push_back(sample);
    }

    /// Return a clone of all samples in chronological order (oldest first).
    pub fn snapshot(&self) -> Vec<MetricsSample> {
        let buf = self.inner.lock().expect("MetricsHistory mutex poisoned");
        buf.iter().cloned().collect()
    }
}

// ── Sampler ────────────────────────────────────────────────────

/// Spawn a supervised background task that records one `MetricsSample` per
/// `interval` into `history`.
///
/// The task cooperatively exits when its `CancellationToken` fires (on
/// `TaskGuard` drop).
pub fn spawn_sampler(
    history: Arc<MetricsHistory>,
    runtime: Arc<RuntimeEnv>,
    tracker: Arc<QueryTracker>,
    interval: Duration,
) -> TaskGuard {
    sqe_core::spawn_supervised("metrics-sampler", move |token: CancellationToken| async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // skip first immediate tick
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = ticker.tick() => {}
            }

            let pool = &runtime.memory_pool;
            let mem_used = crate::memory::used_bytes(pool) as u64;
            let mem_limit = crate::memory::limit_bytes(pool) as u64;
            let active_queries = tracker.active_count();

            let records = tracker.records();
            let total_queries = records.len();
            let failed_queries = records
                .iter()
                .filter(|r| r.state == crate::query_tracker::QueryState::Failed)
                .count();
            let total_output_rows: u64 = records.iter().map(|r| r.output_rows as u64).sum();
            let finished_queries = records
                .iter()
                .filter(|r| r.state == crate::query_tracker::QueryState::Finished)
                .count();
            let exec_ms_sum: u64 = records
                .iter()
                .filter(|r| r.state == crate::query_tracker::QueryState::Finished)
                .map(|r| r.execution_ms)
                .sum();

            let unix_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            history.record(MetricsSample {
                unix_ms,
                active_queries,
                mem_used_bytes: mem_used,
                mem_limit_bytes: mem_limit,
                total_queries,
                failed_queries,
                total_output_rows,
                finished_queries,
                exec_ms_sum,
            });
        }
    })
}

// ── Bucketing ──────────────────────────────────────────────────

/// Wire-format bucket emitted by `/api/v1/metrics/history`.
///
/// All delta fields are clamped to 0 (the tracker's moka cache is not monotone:
/// entries evict and counts can drop between samples).
#[derive(serde::Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HistoryBucket {
    /// Bucket start time, floored to the nearest `BUCKET_SECS` boundary.
    pub t_unix_ms: u64,
    /// Δtotal_queries (all completed+failed queries) within the bucket.
    pub total: u64,
    /// Δfinished_queries (successfully finished) within the bucket.
    pub finished: u64,
    /// Δfailed_queries within the bucket.
    pub failed: u64,
    /// Δtotal_output_rows within the bucket.
    pub rows_out: u64,
    /// Mean latency (ms) for finished queries: Δexec_ms_sum / Δfinished (0 when Δfinished==0).
    pub avg_latency_ms: f64,
    /// Mean active-query count within the bucket (point-in-time readings).
    pub avg_active: f64,
    /// Peak active-query count within the bucket.
    pub max_active: usize,
    /// Mean memory-pool utilisation % within the bucket.
    pub avg_mem_pct: f64,
    /// Peak memory-pool utilisation % within the bucket.
    pub max_mem_pct: f64,
}

/// Wire-format envelope returned by the history endpoint.
#[derive(serde::Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HistoryResponse {
    pub bucket_seconds: u64,
    pub buckets: Vec<HistoryBucket>,
}

/// Fixed bucket width: 900 s = 15 min. With a 12-hour window (43200 s) this
/// gives at most 48 buckets. When history is partial, fewer buckets are
/// returned.
pub const BUCKET_SECS: u64 = 900;

/// Aggregate a raw sample snapshot into time buckets.
///
/// This is a pure function so it can be unit-tested without spawning tasks.
pub fn bucket_samples(samples: &[MetricsSample]) -> HistoryResponse {
    if samples.is_empty() {
        return HistoryResponse {
            bucket_seconds: BUCKET_SECS,
            buckets: Vec::new(),
        };
    }

    // Group samples by 900-second bucket index derived from unix_ms.
    // We use a BTreeMap so buckets come out in chronological order.
    let bucket_ms = BUCKET_SECS * 1000;
    let mut map: std::collections::BTreeMap<u64, Vec<&MetricsSample>> =
        std::collections::BTreeMap::new();
    for s in samples {
        let key = (s.unix_ms / bucket_ms) * bucket_ms;
        map.entry(key).or_default().push(s);
    }

    let mut buckets = Vec::with_capacity(map.len());
    for (bucket_start_ms, group) in &map {
        // Active query stats
        let total_active: usize = group.iter().map(|s| s.active_queries).sum();
        let max_active = group.iter().map(|s| s.active_queries).max().unwrap_or(0);
        let avg_active = if group.is_empty() {
            0.0
        } else {
            total_active as f64 / group.len() as f64
        };

        // Memory % stats
        let mem_pcts: Vec<f64> = group
            .iter()
            .map(|s| {
                if s.mem_limit_bytes == 0 {
                    0.0
                } else {
                    (s.mem_used_bytes as f64 / s.mem_limit_bytes as f64) * 100.0
                }
            })
            .collect();
        let avg_mem_pct = if mem_pcts.is_empty() {
            0.0
        } else {
            mem_pcts.iter().sum::<f64>() / mem_pcts.len() as f64
        };
        let max_mem_pct = mem_pcts.iter().cloned().fold(0.0_f64, f64::max);

        // Query deltas: within-bucket (first sample to last sample).
        // The moka cache is NOT monotonic (entries evict), so clamp all deltas to 0.
        let first = group.first().unwrap();
        let last = group.last().unwrap();
        let total =
            (last.total_queries as i64 - first.total_queries as i64).max(0) as u64;
        let finished =
            (last.finished_queries as i64 - first.finished_queries as i64).max(0) as u64;
        let failed =
            (last.failed_queries as i64 - first.failed_queries as i64).max(0) as u64;
        let rows_out =
            (last.total_output_rows as i64 - first.total_output_rows as i64).max(0) as u64;
        let delta_exec_ms =
            (last.exec_ms_sum as i64 - first.exec_ms_sum as i64).max(0) as u64;
        let avg_latency_ms = if finished == 0 {
            0.0
        } else {
            delta_exec_ms as f64 / finished as f64
        };

        buckets.push(HistoryBucket {
            t_unix_ms: *bucket_start_ms,
            total,
            finished,
            failed,
            rows_out,
            avg_latency_ms,
            avg_active,
            max_active,
            avg_mem_pct,
            max_mem_pct,
        });
    }

    HistoryResponse {
        bucket_seconds: BUCKET_SECS,
        buckets,
    }
}

// ── Tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ms_now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    // ── ring-buffer ──

    #[test]
    fn metrics_history_records_and_snapshots() {
        let h = MetricsHistory::new(5);
        for i in 0..3 {
            h.record(MetricsSample {
                unix_ms: 1000 * i,
                active_queries: i as usize,
                mem_used_bytes: i * 100,
                mem_limit_bytes: 1024,
                total_queries: i as usize,
                failed_queries: 0,
                total_output_rows: 0,
                finished_queries: 0,
                exec_ms_sum: 0,
            });
        }
        let snap = h.snapshot();
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].unix_ms, 0);
        assert_eq!(snap[2].unix_ms, 2000);
    }

    #[test]
    fn metrics_history_evicts_oldest_at_capacity() {
        let h = MetricsHistory::new(3);
        for i in 0..5u64 {
            h.record(MetricsSample {
                unix_ms: i * 1000,
                active_queries: 0,
                mem_used_bytes: 0,
                mem_limit_bytes: 0,
                total_queries: 0,
                failed_queries: 0,
                total_output_rows: 0,
                finished_queries: 0,
                exec_ms_sum: 0,
            });
        }
        let snap = h.snapshot();
        // Oldest 2 entries (unix_ms 0 and 1000) must have been evicted.
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].unix_ms, 2000, "oldest surviving entry should be unix_ms=2000");
        assert_eq!(snap[2].unix_ms, 4000);
    }

    #[test]
    fn metrics_history_snapshot_order_is_chronological() {
        let h = MetricsHistory::new(10);
        let base = ms_now();
        for i in 0..5u64 {
            h.record(MetricsSample {
                unix_ms: base + i * 500,
                active_queries: 0,
                mem_used_bytes: 0,
                mem_limit_bytes: 0,
                total_queries: 0,
                failed_queries: 0,
                total_output_rows: 0,
                finished_queries: 0,
                exec_ms_sum: 0,
            });
        }
        let snap = h.snapshot();
        for w in snap.windows(2) {
            assert!(w[0].unix_ms <= w[1].unix_ms, "snapshot not in order");
        }
    }

    // ── bucketing ──

    #[test]
    fn bucket_samples_empty_returns_empty() {
        let r = bucket_samples(&[]);
        assert_eq!(r.bucket_seconds, BUCKET_SECS);
        assert!(r.buckets.is_empty());
    }

    #[test]
    fn bucket_samples_single_sample() {
        let s = MetricsSample {
            unix_ms: 0,
            active_queries: 2,
            mem_used_bytes: 512,
            mem_limit_bytes: 1024,
            total_queries: 10,
            failed_queries: 1,
            total_output_rows: 50,
            finished_queries: 8,
            exec_ms_sum: 4000,
        };
        let r = bucket_samples(&[s]);
        assert_eq!(r.buckets.len(), 1);
        let b = &r.buckets[0];
        // Single sample: first==last, all deltas are 0
        assert_eq!(b.total, 0);
        assert_eq!(b.finished, 0);
        assert_eq!(b.failed, 0);
        assert_eq!(b.rows_out, 0);
        assert_eq!(b.avg_latency_ms, 0.0); // Δfinished==0 guard
        assert_eq!(b.avg_active, 2.0);
        assert_eq!(b.max_active, 2);
        assert!((b.avg_mem_pct - 50.0).abs() < 0.001);
        assert!((b.max_mem_pct - 50.0).abs() < 0.001);
    }

    #[test]
    fn bucket_samples_mem_pct_zero_when_limit_zero() {
        let s = MetricsSample {
            unix_ms: 0,
            active_queries: 0,
            mem_used_bytes: 1024,
            mem_limit_bytes: 0, // unlimited pool
            total_queries: 0,
            failed_queries: 0,
            total_output_rows: 0,
            finished_queries: 0,
            exec_ms_sum: 0,
        };
        let r = bucket_samples(&[s]);
        assert_eq!(r.buckets.len(), 1);
        assert_eq!(r.buckets[0].avg_mem_pct, 0.0);
        assert_eq!(r.buckets[0].max_mem_pct, 0.0);
    }

    #[test]
    fn bucket_samples_clamps_negative_delta() {
        // Simulate cache eviction: total_queries drops from bucket-first to bucket-last.
        // Bucket width is 900_000 ms; put both samples in the same bucket.
        let base_ms: u64 = 900_000; // exactly one bucket boundary
        let s1 = MetricsSample {
            unix_ms: base_ms,
            active_queries: 0,
            mem_used_bytes: 0,
            mem_limit_bytes: 0,
            total_queries: 100,
            failed_queries: 10,
            total_output_rows: 5000,
            finished_queries: 80,
            exec_ms_sum: 80_000,
        };
        let s2 = MetricsSample {
            unix_ms: base_ms + 10_000,
            active_queries: 0,
            mem_used_bytes: 0,
            mem_limit_bytes: 0,
            total_queries: 5,       // eviction caused apparent drop
            failed_queries: 1,      // same
            total_output_rows: 100, // apparent drop
            finished_queries: 3,    // apparent drop
            exec_ms_sum: 500,       // apparent drop
        };
        let r = bucket_samples(&[s1, s2]);
        assert_eq!(r.buckets.len(), 1);
        assert_eq!(r.buckets[0].total, 0, "negative delta should be clamped to 0");
        assert_eq!(r.buckets[0].failed, 0, "negative delta should be clamped to 0");
        assert_eq!(r.buckets[0].rows_out, 0, "negative delta should be clamped to 0");
        assert_eq!(r.buckets[0].avg_latency_ms, 0.0, "zero Δfinished -> 0 latency");
    }

    #[test]
    fn bucket_samples_two_buckets_delta() {
        // Two samples in the same bucket with increasing totals.
        let base_ms: u64 = 0;
        let s1 = MetricsSample {
            unix_ms: base_ms,
            active_queries: 1,
            mem_used_bytes: 100,
            mem_limit_bytes: 1000,
            total_queries: 5,
            failed_queries: 0,
            total_output_rows: 100,
            finished_queries: 4,
            exec_ms_sum: 2000,
        };
        let s2 = MetricsSample {
            unix_ms: base_ms + 10_000,
            active_queries: 3,
            mem_used_bytes: 200,
            mem_limit_bytes: 1000,
            total_queries: 10,
            failed_queries: 2,
            total_output_rows: 350,
            finished_queries: 7,
            exec_ms_sum: 3500,
        };
        let r = bucket_samples(&[s1, s2]);
        assert_eq!(r.buckets.len(), 1);
        let b = &r.buckets[0];
        assert_eq!(b.total, 5);      // Δtotal_queries
        assert_eq!(b.finished, 3);   // Δfinished_queries
        assert_eq!(b.failed, 2);     // Δfailed_queries
        assert_eq!(b.rows_out, 250); // Δtotal_output_rows
        // avg_latency_ms = Δexec_ms_sum / Δfinished = (3500-2000) / 3 = 500.0
        assert!((b.avg_latency_ms - 500.0).abs() < 0.001);
        assert_eq!(b.max_active, 3);
        assert!((b.avg_active - 2.0).abs() < 0.001);
    }

    #[test]
    fn bucket_samples_avg_latency_zero_when_no_finished() {
        // When Δfinished == 0, avgLatencyMs should be 0.0 (not NaN/Inf).
        let base_ms: u64 = 0;
        let s1 = MetricsSample {
            unix_ms: base_ms,
            active_queries: 1,
            mem_used_bytes: 0,
            mem_limit_bytes: 1000,
            total_queries: 0,
            failed_queries: 0,
            total_output_rows: 0,
            finished_queries: 0,
            exec_ms_sum: 0,
        };
        let s2 = MetricsSample {
            unix_ms: base_ms + 10_000,
            active_queries: 2,
            mem_used_bytes: 0,
            mem_limit_bytes: 1000,
            total_queries: 1,
            failed_queries: 1, // all went to failed, none finished
            total_output_rows: 0,
            finished_queries: 0,
            exec_ms_sum: 0,
        };
        let r = bucket_samples(&[s1, s2]);
        assert_eq!(r.buckets.len(), 1);
        let b = &r.buckets[0];
        assert_eq!(b.finished, 0);
        assert_eq!(b.avg_latency_ms, 0.0);
    }

    #[test]
    fn bucket_samples_produces_at_most_48_buckets_for_12h() {
        // 4320 samples at 10 s each = 12 h. Each 900 s bucket holds 90 samples.
        // Total distinct bucket keys = 12*60*60 / 900 = 48.
        let base_ms: u64 = 0;
        let mut samples = Vec::with_capacity(4320);
        for i in 0..4320u64 {
            samples.push(MetricsSample {
                unix_ms: base_ms + i * 10_000,
                active_queries: (i % 5) as usize,
                mem_used_bytes: i * 10,
                mem_limit_bytes: 50_000,
                total_queries: i as usize,
                failed_queries: (i / 100) as usize,
                total_output_rows: i * 20,
                finished_queries: i.saturating_sub(i / 100) as usize,
                exec_ms_sum: i * 100,
            });
        }
        let r = bucket_samples(&samples);
        assert_eq!(r.buckets.len(), 48);
    }
}
