//! Identifier quoting for generated SQL (SHOW CREATE TABLE).

/// Trino's reserved words: a column with one of these names must be quoted
/// for a Trino client to run the DDL back.
const RESERVED: &str = "
    alter and as between by case cast constraint create cross cube current_catalog
    current_date current_path current_role current_schema current_time current_timestamp
    current_user deallocate delete describe distinct drop else end escape except execute
    exists extract false for from full group grouping having in inner insert intersect into
    is join json_array json_exists json_object json_query json_table json_value left like
    listagg localtime localtimestamp natural normalize not null on or order outer prepare
    recursive right rollup select skip table then trim true uescape union unnest using
    values when where with";

/// `name` as SQL: bare when it is a lowercase identifier that is not a
/// reserved word, else inside `quote` with an embedded `quote` doubled.
/// Unquoted identifiers fold to lowercase (#337), so a name with an
/// uppercase letter only survives a round trip quoted.
pub fn quote_ident_if_needed(name: &str, quote: char) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !RESERVED.split_whitespace().any(|w| w == name);
    if plain {
        name.to_string()
    } else {
        let doubled: String = [quote, quote].iter().collect();
        format!("{quote}{}{quote}", name.replace(quote, &doubled))
    }
}

/// A dotted name with each part quoted when needed.
pub fn quote_qualified_if_needed(parts: &[String], quote: char) -> String {
    parts
        .iter()
        .map(|p| quote_ident_if_needed(p, quote))
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_only_what_needs_it() {
        assert_eq!(quote_ident_if_needed("order_id", '"'), "order_id");
        assert_eq!(quote_ident_if_needed("select", '"'), "\"select\"");
        assert_eq!(quote_ident_if_needed("My Col", '"'), "\"My Col\"");
        assert_eq!(quote_ident_if_needed("odd\"q", '"'), "\"odd\"\"q\"");
        assert_eq!(quote_ident_if_needed("1st", '`'), "`1st`");
        assert_eq!(
            quote_qualified_if_needed(&["s".into(), "My T".into()], '"'),
            "s.\"My T\""
        );
    }
}
