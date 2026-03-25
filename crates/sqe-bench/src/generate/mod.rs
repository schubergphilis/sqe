pub mod clickbench;
pub mod parquet_writer;
pub mod ssb;
pub mod tpcc;
pub mod tpce;
pub mod tpch;
pub mod tpcbb;
pub mod tpcds;

use arrow_schema::SchemaRef;
use std::time::Duration;

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
    ) -> anyhow::Result<GenerateStats>;
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
