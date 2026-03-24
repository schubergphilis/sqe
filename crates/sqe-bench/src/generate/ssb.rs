use super::{BenchmarkGenerator, GenerateStats, TableDef};

pub struct SsbGenerator;

impl BenchmarkGenerator for SsbGenerator {
    fn name(&self) -> &str {
        "ssb"
    }

    /// Returns an empty list until Task 7 fills in the SSB schema definitions.
    fn tables(&self) -> Vec<TableDef> {
        vec![]
    }

    fn generate_table(
        &self,
        _table: &str,
        _scale: f64,
        _output: &str,
    ) -> anyhow::Result<GenerateStats> {
        anyhow::bail!("SSB generator not yet implemented")
    }
}
