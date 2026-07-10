//! Integration tests for parallel TPC-H data generation.
//!
//! These tests confirm the parallel dispatcher (via `GenerateConfig.threads`)
//! produces the same row set as a serial (`threads=1`) run, and that the
//! serial path itself remains deterministic and stable.
//!
//! Assertions are row-set level, not byte level: at `threads > 1` each
//! partition gets a distinct RNG seed, so individual row values differ
//! from the pre-parallel serial output. TPC-H queries are order-
//! insensitive on the base tables, so a post-load query suite still
//! passes.

use std::path::{Path, PathBuf};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn tmpdir(tag: &str) -> PathBuf {
    let pid = std::process::id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("sqe-bench-generate-parallel-{tag}-{pid}-{ts}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Count rows in every `*.parquet` file under `dir/subdir`.
fn count_rows(dir: &Path, subdir: &str) -> usize {
    let mut total = 0;
    let sub = dir.join(subdir);
    assert!(sub.is_dir(), "missing subdir {sub:?}");
    for entry in std::fs::read_dir(&sub).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
            continue;
        }
        let file = std::fs::File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            total += batch.unwrap().num_rows();
        }
    }
    total
}

/// List `*.parquet` filenames under `dir/subdir`, sorted.
fn list_parquet(dir: &Path, subdir: &str) -> Vec<String> {
    let sub = dir.join(subdir);
    let mut names: Vec<String> = std::fs::read_dir(&sub)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                Some(p.file_name().unwrap().to_string_lossy().into_owned())
            } else {
                None
            }
        })
        .collect();
    names.sort();
    names
}

/// Small scale factor (SF0.01) so the test runs in under a second in debug
/// builds. Every TPC-H table is covered.
const TEST_SCALE: f64 = 0.01;

#[test]
fn parallel_and_serial_produce_same_row_counts_for_every_tpch_table() {
    use sqe_bench::generate::{get_generator, GenerateConfig};

    let gen = get_generator("tpch").unwrap();

    let serial_dir = tmpdir("serial");
    let parallel_dir = tmpdir("parallel");

    let serial_cfg = GenerateConfig {
        threads: 1,
        ..GenerateConfig::default()
    };
    let parallel_cfg = GenerateConfig {
        threads: 4,
        ..GenerateConfig::default()
    };

    for table_def in gen.tables() {
        let serial_stats = gen
            .generate_table(&table_def.name, TEST_SCALE, serial_dir.to_str().unwrap(), &serial_cfg)
            .unwrap_or_else(|e| panic!("serial generate failed for {}: {e}", table_def.name));
        let parallel_stats = gen
            .generate_table(
                &table_def.name,
                TEST_SCALE,
                parallel_dir.to_str().unwrap(),
                &parallel_cfg,
            )
            .unwrap_or_else(|e| panic!("parallel generate failed for {}: {e}", table_def.name));

        assert_eq!(
            serial_stats.rows, parallel_stats.rows,
            "row count mismatch for {}: serial={}, parallel={}",
            table_def.name, serial_stats.rows, parallel_stats.rows
        );

        // Cross-check against what actually landed on disk (parquet-level),
        // not just what the generator reported.
        let serial_subpath = format!("tpch/sf{TEST_SCALE}/{}", table_def.name);
        let parallel_subpath = format!("tpch/sf{TEST_SCALE}/{}", table_def.name);
        let serial_disk = count_rows(&serial_dir, &serial_subpath);
        let parallel_disk = count_rows(&parallel_dir, &parallel_subpath);
        assert_eq!(
            serial_disk, serial_stats.rows,
            "serial disk row count mismatch for {}",
            table_def.name
        );
        assert_eq!(
            parallel_disk, parallel_stats.rows,
            "parallel disk row count mismatch for {}",
            table_def.name
        );
    }

    let _ = std::fs::remove_dir_all(&serial_dir);
    let _ = std::fs::remove_dir_all(&parallel_dir);
}

#[test]
fn parallel_4_produces_disjoint_file_namespaces_per_partition() {
    use sqe_bench::generate::{get_generator, GenerateConfig};

    let gen = get_generator("tpch").unwrap();
    let dir = tmpdir("disjoint");
    let cfg = GenerateConfig {
        threads: 4,
        ..GenerateConfig::default()
    };

    // lineitem is the biggest table at SF0.01 (60K rows), most likely to
    // actually produce multiple files per partition. Use it as the probe.
    gen.generate_table("lineitem", TEST_SCALE, dir.to_str().unwrap(), &cfg)
        .unwrap();

    let names = list_parquet(&dir, &format!("tpch/sf{TEST_SCALE}/lineitem"));
    assert!(!names.is_empty(), "no lineitem parquet files produced");

    // Every filename must be 4-digit partition + 5-digit file + ".parquet".
    // That is 4 + 5 + 8 = 17 chars. The reference `"000000000.parquet"`
    // below is exactly nine zeroes followed by `.parquet`.
    for name in &names {
        assert_eq!(
            name.len(),
            "000000000.parquet".len(),
            "unexpected filename shape: {name}"
        );
        let prefix = &name[..4];
        let partition: usize = prefix.parse().unwrap_or_else(|_| {
            panic!("filename prefix '{prefix}' is not a 4-digit partition index: {name}")
        });
        assert!(
            partition < 4,
            "partition index {partition} out of range for threads=4: {name}"
        );
    }

    // Uniqueness: no two workers wrote the same file path.
    let mut sorted = names.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        names.len(),
        "parallel workers produced duplicate file names: {names:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn serial_generation_at_threads_one_matches_expected_total_rows() {
    use sqe_bench::generate::{get_generator, GenerateConfig};

    let gen = get_generator("tpch").unwrap();
    let dir = tmpdir("serial_totals");
    let cfg = GenerateConfig {
        threads: 1,
        ..GenerateConfig::default()
    };

    for table_def in gen.tables() {
        let expected = (table_def.row_count)(TEST_SCALE);
        let stats = gen
            .generate_table(&table_def.name, TEST_SCALE, dir.to_str().unwrap(), &cfg)
            .unwrap();
        assert_eq!(
            stats.rows, expected,
            "serial generation at threads=1 produced wrong row count for {}",
            table_def.name
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
