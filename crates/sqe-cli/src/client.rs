use std::collections::HashSet;

use arrow::array::{Array, RecordBatch};
use arrow::util::display::{ArrayFormatter, FormatOptions};

/// Common result type returned by both Flight and HTTP clients.
#[derive(Debug, Default)]
pub struct QueryResult {
    pub columns: Vec<String>,
    /// Display text per cell. A NULL cell holds `""`; [`Self::nulls`] tells
    /// it apart from an empty string.
    pub rows: Vec<Vec<String>>,
    /// `(row, column)` of every NULL cell, so `-f json` prints `null` and
    /// the table prints `NULL` whatever the backend.
    pub nulls: HashSet<(usize, usize)>,
}

impl QueryResult {
    /// No columns, no rows: the result of a statement.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Rows of text without NULLs (DDL, listings).
    pub fn text(columns: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        Self {
            columns,
            rows,
            nulls: HashSet::new(),
        }
    }

    /// True when the cell at `row`, `column` is SQL NULL.
    pub fn is_null(&self, row: usize, column: usize) -> bool {
        self.nulls.contains(&(row, column))
    }

    /// Render Arrow batches. Column names come from `schema` even when
    /// `batches` is empty.
    pub fn from_batches(
        schema: &arrow_schema::Schema,
        batches: &[RecordBatch],
    ) -> Result<Self, arrow_schema::ArrowError> {
        let mut result = Self::text(
            schema.fields().iter().map(|f| f.name().clone()).collect(),
            Vec::new(),
        );
        let opts = FormatOptions::default();
        for batch in batches {
            let formatters = batch
                .columns()
                .iter()
                .map(|col| ArrayFormatter::try_new(col.as_ref(), &opts))
                .collect::<Result<Vec<_>, _>>()?;
            // Logical nulls: a dictionary or run-end column keeps its NULLs
            // in the values, not in a validity buffer of its own.
            let nulls: Vec<_> = batch.columns().iter().map(|c| c.logical_nulls()).collect();
            for row in 0..batch.num_rows() {
                let index = result.rows.len();
                for (column, mask) in nulls.iter().enumerate() {
                    if mask.as_ref().is_some_and(|m| m.is_null(row)) {
                        result.nulls.insert((index, column));
                    }
                }
                let cells = formatters.iter().map(|f| f.value(row).to_string());
                result.rows.push(cells.collect());
            }
        }
        Ok(result)
    }
}

/// Trait abstracting over Flight SQL and HTTP backends.
#[async_trait::async_trait]
pub trait SqlClient: Send {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, Box<dyn std::error::Error>>;
}
