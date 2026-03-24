use super::{BenchmarkGenerator, GenerateStats, TableDef};

pub struct TpchGenerator;

impl BenchmarkGenerator for TpchGenerator {
    fn name(&self) -> &str {
        "tpch"
    }

    /// Returns an empty list until Task 7 fills in the TPC-H schema definitions.
    fn tables(&self) -> Vec<TableDef> {
        vec![]
    }

    fn generate_table(
        &self,
        _table: &str,
        _scale: f64,
        _output: &str,
    ) -> anyhow::Result<GenerateStats> {
        anyhow::bail!("TPC-H generator not yet implemented")
    }
}
