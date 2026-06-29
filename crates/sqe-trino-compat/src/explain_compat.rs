//! Trino-compatibility reshape for `EXPLAIN` results.
//!
//! SQE returns rich, structured EXPLAIN output: plain `EXPLAIN` is a
//! `[plan_type, plan]` table (one row per plan stage), and `EXPLAIN ANALYZE` /
//! `EXPLAIN FULL` are wide numeric metrics tables (one row per operator). Trino
//! instead returns every EXPLAIN variant as a single `Query Plan` varchar
//! column, and BI/JDBC tools string-match that exact column name. This layer
//! collapses SQE's shape into that single column on the HTTP result boundary
//! only -- native Flight SQL clients keep the structured table.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};

/// True when `sql` is any `EXPLAIN` variant (`EXPLAIN`, `EXPLAIN ANALYZE`,
/// `EXPLAIN FULL`). Gates the reshape so a user query that happens to alias a
/// column to `plan` is never touched.
pub fn is_explain(sql: &str) -> bool {
    sql.trim_start().to_ascii_uppercase().starts_with("EXPLAIN")
}

/// Reshape an EXPLAIN result into Trino's single `Query Plan` varchar column.
///
/// Two input shapes are recognized; anything else is returned unchanged:
///  - plain `EXPLAIN` (`[plan_type, plan]`): the stages are joined into a
///    single row, each prefixed with a `-- <plan_type> --` header, matching
///    Trino's one-row contract so a tool reading row 0 gets the whole plan.
///  - `EXPLAIN ANALYZE` / `EXPLAIN FULL` (wide metrics table): each operator
///    row is rendered as one readable `col=value` text line, one output row per
///    input row.
pub fn reshape_explain_to_trino(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    let Some(first) = batches.first() else {
        return batches;
    };
    let schema = first.schema();

    if let (Ok(pt_idx), Ok(plan_idx)) = (schema.index_of("plan_type"), schema.index_of("plan")) {
        match render_plan_table(&batches, pt_idx, plan_idx) {
            Some(text) => single_query_plan(vec![text]),
            None => batches,
        }
    } else {
        single_query_plan(render_metrics_table(&batches))
    }
}

/// Join a `[plan_type, plan]` table into one Trino-style plan string. Returns
/// `None` (caller passes the batches through) if the columns are not the
/// expected string arrays.
fn render_plan_table(batches: &[RecordBatch], pt_idx: usize, plan_idx: usize) -> Option<String> {
    let mut sections: Vec<String> = Vec::new();
    for b in batches {
        let pt = b.column(pt_idx).as_any().downcast_ref::<StringArray>()?;
        let plan = b.column(plan_idx).as_any().downcast_ref::<StringArray>()?;
        for i in 0..b.num_rows() {
            let header = if pt.is_null(i) { "" } else { pt.value(i) };
            let body = if plan.is_null(i) { "" } else { plan.value(i) };
            sections.push(format!("-- {header} --\n{body}"));
        }
    }
    Some(sections.join("\n\n"))
}

/// Render a wide metrics table to one `col=value` text line per row, skipping
/// null cells.
fn render_metrics_table(batches: &[RecordBatch]) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for b in batches {
        let schema = b.schema();
        for row in 0..b.num_rows() {
            let cells: Vec<String> = b
                .columns()
                .iter()
                .enumerate()
                .filter(|(_, col)| !col.is_null(row))
                .map(|(idx, col)| {
                    let name = schema.field(idx).name();
                    let value =
                        arrow::util::display::array_value_to_string(col, row).unwrap_or_default();
                    format!("{name}={value}")
                })
                .collect();
            lines.push(cells.join(" "));
        }
    }
    lines
}

/// Build the single-column `Query Plan` batch (capital Q, capital P, one space
/// -- the exact name Trino emits and BI tools match on).
fn single_query_plan(lines: Vec<String>) -> Vec<RecordBatch> {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "Query Plan",
        arrow_schema::DataType::Utf8,
        false,
    )]));
    let col: arrow_array::ArrayRef = Arc::new(StringArray::from(lines));
    match RecordBatch::try_new(schema, vec![col]) {
        Ok(b) => vec![b],
        Err(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int32Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn detects_explain_variants() {
        assert!(is_explain("EXPLAIN SELECT 1"));
        assert!(is_explain("  explain analyze select 1"));
        assert!(is_explain("EXPLAIN FULL SELECT 1"));
        assert!(!is_explain("SELECT 1"));
        assert!(!is_explain("SELECT plan FROM t"));
    }

    fn plan_value(b: &RecordBatch, row: usize) -> String {
        b.column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .to_string()
    }

    #[test]
    fn plain_explain_collapses_to_single_query_plan_row() {
        // Mirrors explain.rs's exact output schema: [plan_type, plan].
        let schema = Arc::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["logical_plan", "physical_plan"])),
                Arc::new(StringArray::from(vec!["LogicalText", "PhysicalText"])),
            ],
        )
        .unwrap();

        let out = reshape_explain_to_trino(vec![b]);
        assert_eq!(out.len(), 1);
        // Single column named exactly "Query Plan".
        assert_eq!(
            out[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["Query Plan"]
        );
        // One row holding the whole plan (Trino's contract), both stages present.
        assert_eq!(out[0].num_rows(), 1);
        let text = plan_value(&out[0], 0);
        assert!(text.contains("-- logical_plan --"), "{text}");
        assert!(text.contains("LogicalText"), "{text}");
        assert!(text.contains("-- physical_plan --"), "{text}");
        assert!(text.contains("PhysicalText"), "{text}");
    }

    #[test]
    fn explain_analyze_metrics_render_one_line_per_row() {
        // A representative subset of explain.rs's EXPLAIN ANALYZE schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("step", DataType::Int32, false),
            Field::new("operation", DataType::Utf8, false),
            Field::new("output_rows", DataType::Int64, true),
            Field::new("elapsed_ms", DataType::Float64, true),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![0, 1])),
                Arc::new(StringArray::from(vec!["ProjectionExec", "TableScan"])),
                Arc::new(Int64Array::from(vec![Some(42), None])),
                Arc::new(Float64Array::from(vec![Some(1.5), Some(2.0)])),
            ],
        )
        .unwrap();

        let out = reshape_explain_to_trino(vec![b]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].schema().field(0).name(), "Query Plan");
        // One output row per operator row.
        assert_eq!(out[0].num_rows(), 2);
        let row0 = plan_value(&out[0], 0);
        assert!(row0.contains("step=0"), "{row0}");
        assert!(row0.contains("operation=ProjectionExec"), "{row0}");
        assert!(row0.contains("output_rows=42"), "{row0}");
        // Null cells are skipped: row 1 has no output_rows.
        let row1 = plan_value(&out[0], 1);
        assert!(!row1.contains("output_rows="), "{row1}");
        assert!(row1.contains("operation=TableScan"), "{row1}");
    }

    #[test]
    fn empty_input_is_returned_unchanged() {
        let out = reshape_explain_to_trino(vec![]);
        assert!(out.is_empty());
    }

    #[test]
    fn any_non_plan_table_renders_via_the_metrics_path() {
        // The is_explain gate guarantees the helper only ever sees EXPLAIN
        // output, so a batch lacking the plan_type/plan columns must be one of
        // the metrics variants. Confirm such a batch renders rather than panics.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let out = reshape_explain_to_trino(vec![b]);
        assert_eq!(out[0].schema().field(0).name(), "Query Plan");
        assert_eq!(out[0].num_rows(), 1);
        assert_eq!(plan_value(&out[0], 0), "x=1");
    }
}
