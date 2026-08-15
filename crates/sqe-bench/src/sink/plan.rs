//! Sizing for direct-to-Iceberg bank runs.
//!
//! The demo requirement arrives as a byte target ("4 TB per day for 12
//! days"), not a row count. A pilot calibration converts bytes to rows
//! honestly: it generates a sample of real transaction rows, compresses
//! them with the exact writer settings the run will use, and measures
//! compressed bytes per row. The run plan (rows per day, shards per day,
//! projected files and duration) derives from that measurement.

use std::io::Cursor;

use anyhow::Context;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::generate::bank::{self, BankPlan};
use crate::generate::GenerateConfig;
use crate::sink::iceberg::{human_bytes, BankRunSpec};

/// Rows in the calibration sample. Large enough that dictionary and
/// compression ratios stabilize, small enough to finish in seconds.
const PILOT_ROWS: u64 = 262_144;

/// Aim for roughly this many target-size data files per generation shard.
/// Fewer means many tiny commits-worth of shards; more means less
/// parallelism per day.
const FILES_PER_SHARD: u64 = 2;

/// Parse a human byte size: `4t`, `500g`, `128m`, `64k`, or plain bytes.
pub fn parse_size(s: &str) -> anyhow::Result<u64> {
    let t = s.trim().to_ascii_lowercase();
    let (num, mult): (&str, u64) = match t.as_bytes().last() {
        Some(b'k') => (&t[..t.len() - 1], 1 << 10),
        Some(b'm') => (&t[..t.len() - 1], 1 << 20),
        Some(b'g') => (&t[..t.len() - 1], 1 << 30),
        Some(b't') => (&t[..t.len() - 1], 1 << 40),
        _ => (&t[..], 1),
    };
    let v: f64 = num
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid size '{s}': {e}"))?;
    anyhow::ensure!(v > 0.0, "size '{s}' must be positive");
    Ok((v * mult as f64) as u64)
}

/// Result of the pilot measurement.
#[derive(Debug, Clone, Copy)]
pub struct Calibration {
    /// Compressed Parquet bytes per transaction row.
    pub bytes_per_row: f64,
    /// Single-thread generation + compression throughput, bytes/second of
    /// compressed output.
    pub thread_bytes_per_sec: f64,
}

/// Generate and compress a sample of transaction rows in memory and
/// measure compressed size and single-thread throughput.
pub fn calibrate(config: &GenerateConfig, accounts: u64) -> anyhow::Result<Calibration> {
    let started = std::time::Instant::now();
    let schema = bank::transaction_schema();
    let mut props = WriterProperties::builder().set_compression(config.compression.to_parquet());
    if let Some(rgs) = config.row_group_size {
        props = props.set_max_row_group_row_count(Some(rgs));
    }
    let mut buf = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props.build()))
        .context("creating calibration writer")?;
    for batch in bank::transaction_day_shard(
        bank::DEFAULT_START_DAY,
        PILOT_ROWS,
        0,
        0..accounts.max(1),
        bank::unit_seed("transaction-pilot", 0, 0),
    ) {
        writer.write(&batch).context("writing calibration batch")?;
    }
    writer.close().context("closing calibration writer")?;

    let bytes = buf.into_inner().len() as f64;
    let secs = started.elapsed().as_secs_f64().max(1e-6);
    Ok(Calibration {
        bytes_per_row: bytes / PILOT_ROWS as f64,
        thread_bytes_per_sec: bytes / secs,
    })
}

/// A fully sized run, ready to print or execute.
#[derive(Debug, Clone, Copy)]
pub struct RunPlan {
    pub spec: BankRunSpec,
    pub calibration: Calibration,
    pub bytes_per_day: u64,
}

impl RunPlan {
    /// Build a plan from the byte target using a calibration measurement.
    pub fn from_bytes_per_day(
        bytes_per_day: u64,
        mut plan: BankPlan,
        calibration: Calibration,
        target_file_size: usize,
        resume: bool,
    ) -> Self {
        let rows_per_day = (bytes_per_day as f64 / calibration.bytes_per_row).ceil() as u64;
        plan.txn_rows_per_day = rows_per_day.max(1);
        let shards = shards_per_day(bytes_per_day, target_file_size);
        Self {
            spec: BankRunSpec {
                plan,
                txn_shards_per_day: shards,
                target_file_size,
                resume,
                clean: false,
            },
            calibration,
            bytes_per_day,
        }
    }

    /// Build a plan from an explicit row count (no byte target).
    pub fn from_rows_per_day(
        plan: BankPlan,
        calibration: Calibration,
        target_file_size: usize,
        resume: bool,
    ) -> Self {
        let bytes_per_day = (plan.txn_rows_per_day as f64 * calibration.bytes_per_row) as u64;
        let shards = shards_per_day(bytes_per_day, target_file_size);
        Self {
            spec: BankRunSpec {
                plan,
                txn_shards_per_day: shards,
                target_file_size,
                resume,
                clean: false,
            },
            calibration,
            bytes_per_day,
        }
    }

    /// Print the sized plan: what will be generated and a projection of
    /// how long it takes at the measured single-thread rate.
    pub fn print(&self, threads: usize) {
        let p = &self.spec.plan;
        let total_bytes = self.bytes_per_day * p.days as u64;
        let files_per_day = self.bytes_per_day / self.spec.target_file_size.max(1) as u64;
        println!("Run plan");
        println!(
            "  days:              {} (from {})",
            p.days,
            super::iceberg::format_day(p.start_day)
        );
        println!("  customers:         {}", p.customers);
        println!("  accounts:          {}", p.accounts());
        println!(
            "  bytes/row:         {:.1} B compressed (pilot of {} rows)",
            self.calibration.bytes_per_row, PILOT_ROWS
        );
        println!(
            "  transaction:       {} rows/day, ~{}/day, ~{} files/day in {} shards/day",
            p.txn_rows_per_day,
            human_bytes(self.bytes_per_day),
            files_per_day.max(1),
            self.spec.txn_shards_per_day
        );
        println!(
            "  account_balance:   {} rows/day (end-of-day snapshot per account)",
            p.accounts()
        );
        println!(
            "  total:             ~{} across {} days",
            human_bytes(total_bytes),
            p.days
        );
        let gen_rate = self.calibration.thread_bytes_per_sec * threads as f64;
        println!(
            "  projected:         ~{:.1} h generation at {}/s ({} workers, pilot rate)",
            total_bytes as f64 / gen_rate / 3600.0,
            human_bytes(gen_rate as u64),
            threads
        );
        for gbit in [10u64, 25] {
            let net = gbit as f64 * 1e9 / 8.0;
            println!(
                "                     network floor at {gbit} Gbit/s: ~{:.1} h",
                total_bytes as f64 / net / 3600.0
            );
        }
    }
}

/// Shards per day sized so each shard emits about `FILES_PER_SHARD`
/// target-size files.
fn shards_per_day(bytes_per_day: u64, target_file_size: usize) -> u32 {
    let shard_bytes = (target_file_size as u64 * FILES_PER_SHARD).max(1);
    bytes_per_day.div_ceil(shard_bytes).clamp(1, 65_536) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::config::CompressionKind;

    #[test]
    fn parse_size_accepts_suffixes() {
        assert_eq!(parse_size("4t").unwrap(), 4 << 40);
        assert_eq!(parse_size("500g").unwrap(), 500 << 30);
        assert_eq!(parse_size("128M").unwrap(), 128 << 20);
        assert_eq!(parse_size("64k").unwrap(), 64 << 10);
        assert_eq!(parse_size("12345").unwrap(), 12345);
        assert_eq!(
            parse_size("1.5g").unwrap(),
            (1.5 * (1u64 << 30) as f64) as u64
        );
        assert!(parse_size("abc").is_err());
        assert!(parse_size("-4t").is_err());
    }

    #[test]
    fn shards_scale_with_day_bytes() {
        // 4 TB / (2 x 512 MB) = 4096 shards.
        assert_eq!(shards_per_day(4 << 40, 512 << 20), 4096);
        // Tiny day still gets one shard.
        assert_eq!(shards_per_day(1 << 20, 512 << 20), 1);
    }

    #[test]
    fn calibration_measures_plausible_row_size() {
        let config = GenerateConfig {
            threads: 1,
            compression: CompressionKind::Zstd3,
            row_group_size: None,
        };
        let cal = calibrate(&config, 25_000_000).unwrap();
        // A transaction row has ~90 bytes of high-entropy strings alone;
        // anything below 40 B or above 400 B means the schema or the
        // generator changed shape and the sizing math is off.
        assert!(
            (40.0..400.0).contains(&cal.bytes_per_row),
            "bytes/row {} out of expected envelope",
            cal.bytes_per_row
        );
        assert!(cal.thread_bytes_per_sec > 0.0);
    }

    #[test]
    fn plan_from_bytes_derives_rows() {
        let cal = Calibration {
            bytes_per_row: 200.0,
            thread_bytes_per_sec: 100e6,
        };
        let plan = BankPlan {
            customers: 1000,
            start_day: bank::DEFAULT_START_DAY,
            days: 12,
            txn_rows_per_day: 0,
        };
        let rp = RunPlan::from_bytes_per_day(4 << 40, plan, cal, 512 << 20, false);
        // 4 TB at 200 B/row = ~22 billion rows/day.
        let expected = ((4u64 << 40) as f64 / 200.0).ceil() as u64;
        assert_eq!(rp.spec.plan.txn_rows_per_day, expected);
        assert_eq!(rp.spec.txn_shards_per_day, 4096);
        assert_eq!(rp.spec.plan.days, 12);
    }
}
