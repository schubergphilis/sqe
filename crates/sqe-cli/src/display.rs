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
    for row in &result.rows {
        for (i, cell) in row.iter().enumerate() {
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
    for row in &result.rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, v)| {
                format!(
                    " {:width$} ",
                    v,
                    width = widths.get(i).copied().unwrap_or(0)
                )
            })
            .collect();
        println!("|{}|", cells.join("|"));
    }

    println!("{border}");
    eprintln!("({} rows)", result.rows.len());
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
    for row in &result.rows {
        let obj: serde_json::Map<String, serde_json::Value> = result
            .columns
            .iter()
            .zip(row.iter())
            .map(|(col, val)| (col.clone(), serde_json::Value::String(val.clone())))
            .collect();
        println!("{}", serde_json::to_string(&obj).unwrap_or_default());
    }
}
