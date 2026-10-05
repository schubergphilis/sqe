pub mod bank;
pub mod clickbench;
pub mod config;
pub mod parquet_writer;
pub mod ssb;
pub mod tpcbb;
pub mod tpcc;
pub mod tpcds;
pub mod tpce;
pub mod tpch;

use arrow_schema::SchemaRef;
use std::time::Duration;

pub use config::GenerateConfig;

/// Stateful, lazy batch production. Each call retains only the current batch;
/// RNG and basket/session state remain owned by the iterator between calls.
pub(crate) type BatchIterator = Box<dyn Iterator<Item = arrow_array::RecordBatch> + Send>;

/// `ids` ordered by `(key(id), id)` with `key(id) < buckets`: a two-pass
/// counting sort, 4 bytes per id. The first pass counts, the second
/// recomputes each key and scatters, so no (key, id) pair is stored. Fact
/// generators use it to emit rows in the order `load` sorts them on write.
pub(crate) fn counting_order(
    ids: std::ops::RangeInclusive<u32>,
    buckets: usize,
    key: impl Fn(u32) -> usize,
) -> Vec<u32> {
    let mut start = vec![0usize; buckets + 1];
    for id in ids.clone() {
        start[key(id) + 1] += 1;
    }
    for k in 1..start.len() {
        start[k] += start[k - 1];
    }
    let mut out = vec![0u32; start[buckets]];
    for id in ids {
        let k = key(id);
        out[start[k]] = id;
        start[k] += 1;
    }
    out
}

/// SplitMix64 finalizer: a cheap, well-mixed hash. Fact generators derive
/// the fields that decide a row's position (an order's date and line count)
/// from it, so the sort index can compute them for every id without seeding
/// a ChaCha generator per id.
pub(crate) fn mix64(z: u64) -> u64 {
    let z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Scale a row count ensuring at least 1 row for small scale factors.
pub(crate) fn scaled(scale: f64, base: f64) -> usize {
    (scale * base).max(1.0) as usize
}

pub struct TableDef {
    pub name: String,
    pub schema: SchemaRef,
    pub row_count: fn(f64) -> usize,
}

/// One unit of batch production: a boxed factory the driver calls on a
/// blocking thread. `FnOnce` because a shard is produced exactly once.
pub struct BatchShard {
    pub make: Box<dyn FnOnce() -> Box<dyn Iterator<Item = arrow_array::RecordBatch> + Send> + Send>,
}

/// A table's rows exposed as Arrow batches for the Iceberg direct sink.
/// `shards` is the disjoint work list; serial generators return one shard,
/// range-splitting generators (tpch, bank) return `config.threads` shards.
pub struct BatchSource {
    /// The table's Arrow schema. Part of the `BatchSource` contract for
    /// consumers that validate or introspect the shape before writing; the
    /// current `run_direct` driver derives its write schema from the
    /// catalog table instead, so it does not read this field.
    pub schema: SchemaRef,
    pub total_rows: usize,
    pub shards: Vec<BatchShard>,
}

#[derive(Debug)]
pub struct GenerateStats {
    pub table: String,
    pub rows: usize,
    pub bytes: usize,
    pub files: usize,
    pub duration: Duration,
}

pub trait BenchmarkGenerator: Send + Sync {
    fn name(&self) -> &str;
    fn tables(&self) -> Vec<TableDef>;
    fn generate_table(
        &self,
        table: &str,
        scale: f64,
        output_dir: &str,
        config: &GenerateConfig,
    ) -> anyhow::Result<GenerateStats>;

    /// The column whose non-decreasing order this generator emits `table`
    /// in, if any. A table `load` sorts on write by that same column needs
    /// no sort on the direct sink. `generated_sort_orders_hold` checks the
    /// claim against the rows.
    fn sorted_by(&self, _table: &str) -> Option<&'static str> {
        None
    }

    /// Produce a table's rows as Arrow batches for the Iceberg direct sink.
    /// Default: unsupported (generators opt in one at a time).
    fn generate_batches(
        &self,
        table: &str,
        _scale: f64,
        _config: &GenerateConfig,
    ) -> anyhow::Result<BatchSource> {
        anyhow::bail!(
            "{}: generate_batches not implemented for table {table}",
            self.name()
        )
    }
}

/// Dispatch per-partition row generation across `config.threads` OS threads.
///
/// `gen_range(range, seed)` must be a pure function that returns an iterator
/// of RecordBatch for the given row range. Each partition gets a disjoint
/// range from [`config::partition`] and a deterministic seed from
/// [`config::seed_for_table_partition`].
///
/// Each worker owns its own parquet writer, writing into a unique file
/// prefix of the form `{part_idx:04}` (e.g. `0003`). Files inside one
/// partition rotate at the 128 MiB cap handled by
/// [`parquet_writer::write_parquet_stream`].
///
/// The returned [`GenerateStats`] aggregates rows, bytes, and file counts
/// across all partitions.
///
/// Determinism: at `config.threads == 1`, the single partition spans the
/// whole row range with `seed = base_seed`, reproducing the pre-parallel
/// row order and RNG state exactly.
pub fn parallel_generate_table<G, I>(
    table_name: &str,
    schema: SchemaRef,
    total_rows: usize,
    base_seed: u64,
    output_dir: &str,
    config: &GenerateConfig,
    gen_range: G,
) -> anyhow::Result<GenerateStats>
where
    G: Fn(std::ops::Range<usize>, u64) -> I + Sync,
    I: IntoIterator<Item = arrow_array::RecordBatch> + Send,
    I::IntoIter: Send,
{
    use std::sync::Mutex;

    let start = std::time::Instant::now();
    let threads = config.threads.max(1);

    // Single-threaded fast path: skip thread::scope overhead and preserve
    // byte-identical output with the pre-parallel serial code (seed unchanged,
    // whole range produced as one partition).
    if threads == 1 {
        let batches = gen_range(0..total_rows, base_seed);
        let (files, bytes) = parquet_writer::write_parquet_stream(
            batches, schema, output_dir, table_name, "", config,
        )?;
        return Ok(GenerateStats {
            table: table_name.to_string(),
            rows: total_rows,
            bytes: bytes as usize,
            files,
            duration: start.elapsed(),
        });
    }

    let ranges = config::partition(total_rows, threads);
    let errors: Mutex<Vec<anyhow::Error>> = Mutex::new(Vec::new());
    let per_partition: Mutex<Vec<(usize, u64)>> = Mutex::new(Vec::new());

    std::thread::scope(|s| {
        for (part_idx, range) in ranges.into_iter().enumerate() {
            let schema = schema.clone();
            let output_dir = output_dir.to_string();
            let table_name = table_name.to_string();
            let config = *config;
            let gen_range = &gen_range;
            let errors = &errors;
            let per_partition = &per_partition;

            s.spawn(move || {
                let prefix = format!("{part_idx:04}");
                let seed = config::seed_for_table_partition(base_seed, part_idx);
                let iter = gen_range(range, seed);
                match parquet_writer::write_parquet_stream(
                    iter,
                    schema,
                    &output_dir,
                    &table_name,
                    &prefix,
                    &config,
                ) {
                    Ok((files, bytes)) => {
                        per_partition.lock().unwrap().push((files, bytes));
                    }
                    Err(e) => {
                        errors.lock().unwrap().push(e);
                    }
                }
            });
        }
    });

    let errors = errors.into_inner().unwrap();
    if let Some(e) = errors.into_iter().next() {
        return Err(e);
    }

    let per_partition = per_partition.into_inner().unwrap();
    let total_files: usize = per_partition.iter().map(|(f, _)| *f).sum();
    let total_bytes: u64 = per_partition.iter().map(|(_, b)| *b).sum();

    Ok(GenerateStats {
        table: table_name.to_string(),
        rows: total_rows,
        bytes: total_bytes as usize,
        files: total_files,
        duration: start.elapsed(),
    })
}

pub fn get_generator(name: &str) -> anyhow::Result<Box<dyn BenchmarkGenerator>> {
    match name {
        "bank" => Ok(Box::new(bank::BankGenerator)),
        "tpch" => Ok(Box::new(tpch::TpchGenerator)),
        "ssb" => Ok(Box::new(ssb::SsbGenerator)),
        "tpcc" => Ok(Box::new(tpcc::TpccGenerator)),
        "tpce" => Ok(Box::new(tpce::TpceGenerator)),
        "tpcbb" => Ok(Box::new(tpcbb::TpcbbGenerator)),
        "clickbench" => Ok(Box::new(clickbench::ClickBenchGenerator)),
        "tpcds" => Ok(Box::new(tpcds::TpcdsGenerator)),
        _ => anyhow::bail!(
            "Unknown benchmark: {name}. Supported: tpch, ssb, tpcc, tpce, tpcbb, clickbench, tpcds, bank"
        ),
    }
}

#[cfg(test)]
mod batch_source_tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn generation_golden() {
        use std::hash::{Hash, Hasher};
        // Captured from the eager generators before their streaming migration.
        // `SQE_BENCH_RECORD_GOLDEN=1` prints the current hashes instead of
        // checking them, for a deliberate change to generated values.
        let record = std::env::var_os("SQE_BENCH_RECORD_GOLDEN").is_some();
        let mut recorded = std::collections::BTreeMap::new();
        let expected: std::collections::HashMap<String, (usize, u64)> =
            serde_json::from_str(include_str!("generation-golden.json")).unwrap();
        for suite in ["ssb", "tpcds", "tpcbb", "tpcc", "clickbench"] {
            let generator = get_generator(suite).unwrap();
            for table in generator.tables() {
                let source = generator
                    .generate_batches(&table.name, 0.01, &GenerateConfig::default())
                    .unwrap();
                let mut hash = std::collections::hash_map::DefaultHasher::new();
                let mut rows = 0;
                for shard in source.shards {
                    for batch in (shard.make)() {
                        let mut bytes = Vec::new();
                        let mut writer =
                            arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())
                                .unwrap();
                        writer.write(&batch).unwrap();
                        writer.finish().unwrap();
                        drop(writer);
                        bytes.hash(&mut hash);
                        rows += batch.num_rows();
                    }
                }
                assert_eq!(rows, source.total_rows);
                let key = format!("{suite}.{}", table.name);
                if record {
                    recorded.insert(key, (rows, hash.finish()));
                    continue;
                }
                assert_eq!(
                    Some(&(rows, hash.finish())),
                    expected.get(&key),
                    "values changed for {key}",
                );
            }
        }
        if record {
            println!("{}", serde_json::to_string_pretty(&recorded).unwrap());
        }
    }

    /// Every `sorted_by` claim names the key `load` sorts the table by, and
    /// the generated rows really are non-decreasing on it. SF0.1 has enough
    /// tickets and return rows that a row landing in the wrong place shows.
    #[test]
    fn generated_sort_orders_hold() {
        use arrow_array::{Array, Int32Array};
        let mut checked = 0;
        for suite in [
            "tpch",
            "ssb",
            "tpcds",
            "tpcbb",
            "tpcc",
            "tpce",
            "clickbench",
        ] {
            let generator = get_generator(suite).unwrap();
            for table in generator.tables() {
                let Some(key) = generator.sorted_by(&table.name) else {
                    continue;
                };
                assert_eq!(
                    Some(key),
                    crate::layout::table_layout(suite, &table.name).sort_key(),
                    "{suite}.{}: sorted_by differs from load's sort key",
                    table.name
                );
                let source = generator
                    .generate_batches(&table.name, 0.1, &GenerateConfig::default())
                    .unwrap();
                let mut prev = i32::MIN;
                let mut rows = 0;
                for shard in source.shards {
                    for batch in (shard.make)() {
                        let col = batch
                            .column_by_name(key)
                            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
                            .unwrap_or_else(|| panic!("{suite}.{}.{key} is Int32", table.name));
                        for i in 0..col.len() {
                            let v = col.value(i);
                            assert!(
                                v >= prev,
                                "{suite}.{}: {key} drops from {prev} to {v} at row {}",
                                table.name,
                                rows + i
                            );
                            prev = v;
                        }
                        rows += col.len();
                    }
                }
                assert_eq!(rows, source.total_rows, "{suite}.{}", table.name);
                checked += 1;
            }
        }
        assert_eq!(checked, 8, "ssb lineorder + 7 TPC-DS facts");
    }

    #[test]
    fn sf100_sources_generate_only_requested_batches() {
        for suite in ["ssb", "tpcds", "tpcbb", "tpcc", "clickbench"] {
            let generator = get_generator(suite).unwrap();
            for table in generator.tables() {
                // A date-ordered fact table builds its sort index (4 bytes per
                // order or return row) before the first batch; at SF100 that
                // is ~26 s in a debug build, so those tables run at SF10. The
                // index is the only up-front work; rows are still produced
                // lazily.
                let scale = if generator.sorted_by(&table.name).is_some() {
                    10.0
                } else {
                    100.0
                };
                let source = generator
                    .generate_batches(&table.name, scale, &GenerateConfig::default())
                    .unwrap();
                assert_eq!(source.total_rows, (table.row_count)(scale));
                // Creating a source for hundreds of millions of rows must not
                // produce the table. Dropping an early consumer must stop it.
                for shard in source.shards {
                    for batch in (shard.make)().take(2) {
                        assert_eq!(batch.schema(), table.schema);
                        assert!(batch.num_rows() <= 10_000);
                    }
                }
            }
        }
    }

    #[test]
    fn staged_stream_preserves_values_and_writer_configuration() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::basic::Compression;

        let output = tempfile::tempdir().unwrap();
        let config = GenerateConfig {
            compression: config::CompressionKind::Snappy,
            row_group_size: Some(2048),
            ..GenerateConfig::default()
        };
        for (suite, table) in [
            ("ssb", "lineorder"),
            ("tpcds", "store_sales"),
            ("tpcbb", "web_clickstreams"),
        ] {
            let generator = get_generator(suite).unwrap();
            let stats = generator
                .generate_table(table, 0.01, output.path().to_str().unwrap(), &config)
                .unwrap();
            let source = generator.generate_batches(table, 0.01, &config).unwrap();
            assert_eq!(stats.rows, source.total_rows);
            let schema = source.schema;
            let expected: Vec<_> = source.shards.into_iter().flat_map(|s| (s.make)()).collect();
            let expected = arrow::compute::concat_batches(&schema, &expected).unwrap();
            let directory = output.path().join(format!("{suite}/sf0.01/{table}"));
            let mut files: Vec<_> = std::fs::read_dir(directory)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect();
            files.sort();
            let mut actual = Vec::new();
            for file in files {
                let reader =
                    ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(file).unwrap())
                        .unwrap();
                for row_group in reader.metadata().row_groups() {
                    assert!(row_group.num_rows() <= 2048);
                    assert!(row_group
                        .columns()
                        .iter()
                        .all(|c| c.compression() == Compression::SNAPPY));
                }
                actual.extend(reader.build().unwrap().map(Result::unwrap));
            }
            let actual = arrow::compute::concat_batches(&schema, &actual).unwrap();
            assert_eq!(
                actual, expected,
                "staged values changed for {suite}.{table}"
            );
        }
    }

    #[test]
    fn batch_shard_make_produces_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let s2 = schema.clone();
        let shard = BatchShard {
            make: Box::new(move || {
                let b = RecordBatch::try_new(
                    s2.clone(),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                )
                .unwrap();
                Box::new(std::iter::once(b))
            }),
        };
        let src = BatchSource {
            schema,
            total_rows: 3,
            shards: vec![shard],
        };
        let rows: usize = (src.shards.into_iter().next().unwrap().make)()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 3);
        assert_eq!(src.total_rows, 3);
    }
}

#[cfg(test)]
mod sweep_tests {
    use super::*;

    /// Generate every table of every benchmark at small scale. Any ColVal
    /// variant that does not match its declared field type panics in
    /// cols_to_arrays (or the equivalent builder), so this sweep catches
    /// schema/value drift in tables that have no dedicated test.
    #[test]
    fn every_generator_table_builds_clean_batches() {
        // nosemgrep: opt.aikido.semgrep-rules.rust.lang.security.temp-dir -- test-only scratch dir for synthetic benchmark data, pid suffixed
        let dir = std::env::temp_dir().join(format!("sqe-bench-sweep-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = GenerateConfig {
            threads: 1,
            ..Default::default()
        };
        for bench in [
            "tpch",
            "ssb",
            "tpcds",
            "tpcc",
            "tpce",
            "tpcbb",
            "clickbench",
            "bank",
        ] {
            let g = get_generator(bench).unwrap();
            for t in g.tables() {
                g.generate_table(&t.name, 0.001, dir.to_str().unwrap(), &config)
                    .unwrap_or_else(|e| panic!("{bench}.{} failed: {e}", t.name));
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
