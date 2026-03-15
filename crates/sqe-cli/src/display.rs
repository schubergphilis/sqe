use crate::client::QueryResult;

pub fn print_query_result(result: &QueryResult) {
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

    // Header
    let header: Vec<String> = result
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
        .collect();
    println!(" {} ", header.join(" | "));

    // Separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("-{}-", sep.join("-+-"));

    // Rows
    for row in &result.rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, v)| format!("{:width$}", v, width = widths.get(i).copied().unwrap_or(0)))
            .collect();
        println!(" {} ", cells.join(" | "));
    }

    eprintln!("({} rows)", result.rows.len());
}
