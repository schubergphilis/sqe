use crate::client::QueryResult;
use crate::OutputFormat;

pub fn print_query_result(result: &QueryResult, format: &OutputFormat) {
    match format {
        OutputFormat::Table => print_table(result),
        OutputFormat::Csv => print_separated(result, ','),
        OutputFormat::Tsv => print_separated(result, '\t'),
        OutputFormat::Json => print_json(result),
    }
}

fn print_table(result: &QueryResult) {
    if result.columns.is_empty() {
        eprintln!("(0 rows)");
        return;
    }

    // Compute column widths
    let mut widths: Vec<usize> = result.columns.iter().map(|c| c.len()).collect();
    for (r, row) in result.rows.iter().enumerate() {
        for (i, cell) in row.iter().enumerate() {
            let cell = table_cell(result, r, i, cell);
            if let Some(w) = widths.get_mut(i) {
                if cell.len() > *w {
                    *w = cell.len();
                }
            }
        }
    }

    let border: String = widths
        .iter()
        .map(|w| format!("+{}", "-".repeat(w + 2)))
        .collect::<String>()
        + "+";

    // Header
    println!("{border}");
    let header: Vec<String> = result
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!(" {:width$} ", c, width = widths[i]))
        .collect();
    println!("|{}|", header.join("|"));

    // Separator
    println!("{border}");

    // Rows
    for (r, row) in result.rows.iter().enumerate() {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, v)| {
                format!(
                    " {:width$} ",
                    table_cell(result, r, i, v),
                    width = widths.get(i).copied().unwrap_or(0)
                )
            })
            .collect();
        println!("|{}|", cells.join("|"));
    }

    println!("{border}");
    eprintln!("({} rows)", result.rows.len());
}

/// The table shows NULL as `NULL`, so it differs from an empty string.
fn table_cell<'a>(result: &QueryResult, row: usize, column: usize, value: &'a str) -> &'a str {
    if result.is_null(row, column) {
        "NULL"
    } else {
        value
    }
}

fn print_separated(result: &QueryResult, sep: char) {
    if result.columns.is_empty() {
        return;
    }
    let sep_str = sep.to_string();
    println!("{}", result.columns.join(&sep_str));
    for row in &result.rows {
        let cells: Vec<String> = row.iter().map(|v| sep_escape(v, sep)).collect();
        println!("{}", cells.join(&sep_str));
    }
}

fn sep_escape(value: &str, sep: char) -> String {
    if value.contains(sep) || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn print_json(result: &QueryResult) {
    for line in json_lines(result) {
        println!("{line}");
    }
}

/// One JSON object per row. Values stay display strings; NULL is `null`.
fn json_lines(result: &QueryResult) -> Vec<String> {
    result
        .rows
        .iter()
        .enumerate()
        .map(|(r, row)| {
            let obj: serde_json::Map<String, serde_json::Value> = result
                .columns
                .iter()
                .zip(row.iter())
                .enumerate()
                .map(|(i, (col, val))| {
                    let value = if result.is_null(r, i) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::String(val.clone())
                    };
                    (col.clone(), value)
                })
                .collect();
            serde_json::to_string(&obj).unwrap_or_default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_prints_null_apart_from_an_empty_string() {
        let mut result = QueryResult::text(
            vec!["n".to_string(), "e".to_string()],
            vec![vec![String::new(), String::new()]],
        );
        result.nulls.insert((0, 0));
        let expected = r#"{"n":null,"e":""}"#;
        assert_eq!(json_lines(&result), vec![expected.to_string()]);
        assert_eq!(table_cell(&result, 0, 0, ""), "NULL");
        assert_eq!(table_cell(&result, 0, 1, ""), "");
    }
}
