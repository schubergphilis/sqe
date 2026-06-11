pub mod clickbench;
pub mod config;
pub mod parquet_writer;
pub mod ssb;
pub mod tpcc;
pub mod tpce;
pub mod tpch;
pub mod tpcbb;
pub mod tpcds;

use arrow_schema::SchemaRef;
use std::time::Duration;

pub use config::GenerateConfig;

/// Scale a row count ensuring at least 1 row for small scale factors.
pub(crate) fn scaled(scale: f64, base: f64) -> usize {
    (scale * base).max(1.0) as usize
}

// These fields and methods will be consumed by the generator implementations
// added in Task 7; allow dead_code for now so clippy stays clean.
#[allow(dead_code)]
pub struct TableDef {
    pub name: String,
    pub schema: SchemaRef,
    pub row_count: fn(f64) -> usize,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct GenerateStats {
    pub table: String,
    pub rows: usize,
    pub bytes: usize,
    pub files: usize,
    pub duration: Duration,
}

pub trait BenchmarkGenerator: Send + Sync {
    #[allow(dead_code)]
    fn name(&self) -> &str;
    fn tables(&self) -> Vec<TableDef>;
    fn generate_table(
        &self,
        table: &str,
        scale: f64,
        output_dir: &str,
        config: &GenerateConfig,
    ) -> anyhow::Result<GenerateStats>;
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
            batches,
            schema,
            output_dir,
            table_name,
            "",
            config,
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
        "tpch" => Ok(Box::new(tpch::TpchGenerator)),
        "ssb" => Ok(Box::new(ssb::SsbGenerator)),
        "tpcc" => Ok(Box::new(tpcc::TpccGenerator)),
        "tpce" => Ok(Box::new(tpce::TpceGenerator)),
        "tpcbb" => Ok(Box::new(tpcbb::TpcbbGenerator)),
        "clickbench" => Ok(Box::new(clickbench::ClickBenchGenerator)),
        "tpcds" => Ok(Box::new(tpcds::TpcdsGenerator)),
        _ => anyhow::bail!(
            "Unknown benchmark: {name}. Supported: tpch, ssb, tpcc, tpce, tpcbb, clickbench, tpcds"
        ),
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
        let dir = std::env::temp_dir().join(format!("sqe-bench-sweep-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = GenerateConfig { threads: 1, ..Default::default() };
        for bench in ["tpch", "ssb", "tpcds", "tpcc", "tpce", "tpcbb", "clickbench"] {
            let g = get_generator(bench).unwrap();
            for t in g.tables() {
                g.generate_table(&t.name, 0.001, dir.to_str().unwrap(), &config)
                    .unwrap_or_else(|e| panic!("{bench}.{} failed: {e}", t.name));
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
