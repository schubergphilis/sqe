//! `HiveTableSpec` <-> `HiveManifest` DDL rendering, shared by the embedded
//! CLI and the coordinator's `SHOW CREATE TABLE`.
//!
//! Moved here (from `sqe-cli::embedded`, which had it module-private) so the
//! coordinator can reach it without a second copy. Two independent copies of
//! either function is exactly how the round-trip invariant they share
//! (`try_extract_hive_table` on `render_hive_create_table_ddl`'s output must
//! reproduce `hive_spec_from_manifest`'s input spec) drifts apart silently:
//! a change to one side in one crate would not be caught by the other
//! crate's tests. Keeping one copy in `sqe-catalog::hive` and having both
//! callers use it is the fix, not a convenience.

use super::manifest::{GlueColumn, HiveManifest};

/// The inverse of `HiveManifest::from_spec`: rebuild a `HiveTableSpec` from
/// a stored manifest, so `SHOW CREATE TABLE` renders DDL through the same
/// `HiveTableSpec -> text` path a `CREATE EXTERNAL TABLE` statement went
/// through instead of a bespoke, independently-drifting formatter.
///
/// Two fields cannot round-trip because the manifest never carried them:
/// `if_not_exists` is a creation-time modifier, not part of the table
/// definition, so it is always `false` here. `stored_as` is discarded by
/// `from_spec` in favor of the derived Hadoop `input_format`/`output_format`
/// classes, so this always leaves it `None` and carries the classes
/// explicitly instead; `render_hive_create_table_ddl` renders that as
/// `STORED AS INPUTFORMAT '...' OUTPUTFORMAT '...'`, which is also what real
/// Hive's own `SHOW CREATE TABLE` normalizes a `STORED AS
/// TEXTFILE`/`PARQUET`/... keyword into.
pub fn hive_spec_from_manifest(
    catalog: &str,
    m: &HiveManifest,
) -> sqe_sql::hive_ddl::HiveTableSpec {
    let glue_to_hive = |c: &GlueColumn| sqe_sql::hive_ddl::HiveColumn {
        name: c.name.clone(),
        sql_type: c.r#type.clone(),
        comment: c.comment.clone(),
    };
    sqe_sql::hive_ddl::HiveTableSpec {
        catalog: Some(catalog.to_string()),
        database: Some(m.database_name.clone()),
        name: m.name.clone(),
        if_not_exists: false,
        comment: m.description.clone(),
        columns: m.storage_descriptor.columns.iter().map(glue_to_hive).collect(),
        partition_keys: m.partition_keys.iter().map(glue_to_hive).collect(),
        location: m.storage_descriptor.location.clone(),
        serde_class: m.storage_descriptor.serde_info.serialization_library.clone(),
        serde_properties: m.storage_descriptor.serde_info.parameters.clone(),
        stored_as: None,
        input_format: m.storage_descriptor.input_format.clone(),
        output_format: m.storage_descriptor.output_format.clone(),
        table_properties: m.parameters.clone(),
    }
}

/// Render `spec` as Athena/Glue-compatible `CREATE EXTERNAL TABLE` DDL.
///
/// Round-trip contract: `sqe_sql::hive_ddl::try_extract_hive_table` on the
/// output must produce a `HiveTableSpec` equal to `spec`. Pinned by the
/// embedded test `show_create_table_renders_athena_compatible_ddl`.
pub fn render_hive_create_table_ddl(spec: &sqe_sql::hive_ddl::HiveTableSpec) -> String {
    let table_ref = match (&spec.catalog, &spec.database) {
        (Some(c), Some(d)) => format!("{c}.{d}.{}", spec.name),
        (None, Some(d)) => format!("{d}.{}", spec.name),
        _ => spec.name.clone(),
    };

    let mut out = format!("CREATE EXTERNAL TABLE {table_ref} (\n");
    let cols: Vec<String> = spec
        .columns
        .iter()
        .map(|c| format!("   {} {}", c.name, c.sql_type))
        .collect();
    out.push_str(&cols.join(",\n"));
    out.push_str("\n)");

    if !spec.partition_keys.is_empty() {
        let pk: Vec<String> = spec
            .partition_keys
            .iter()
            .map(|c| format!("{} {}", c.name, c.sql_type))
            .collect();
        out.push_str(&format!("\nPARTITIONED BY ({})", pk.join(", ")));
    }

    if let Some(class) = &spec.serde_class {
        out.push_str(&format!("\nROW FORMAT SERDE '{}'", sql_quote(class)));
        if !spec.serde_properties.is_empty() {
            out.push_str(&format!(
                "\nWITH SERDEPROPERTIES ({})",
                render_props(&spec.serde_properties)
            ));
        }
    }

    if let (Some(i), Some(o)) = (&spec.input_format, &spec.output_format) {
        out.push_str(&format!(
            "\nSTORED AS INPUTFORMAT '{}' OUTPUTFORMAT '{}'",
            sql_quote(i),
            sql_quote(o)
        ));
    }

    out.push_str(&format!("\nLOCATION '{}'", sql_quote(&spec.location)));

    if !spec.table_properties.is_empty() {
        out.push_str(&format!(
            "\nTBLPROPERTIES ({})",
            render_props(&spec.table_properties)
        ));
    }

    out
}

/// `'k' = 'v'` pairs, comma-joined, for `WITH SERDEPROPERTIES (...)` and
/// `TBLPROPERTIES (...)`. `BTreeMap` iteration is already deterministically
/// ordered, which only matters here for a stable rendering across calls;
/// the parsed-back `BTreeMap` is unordered-equal regardless.
fn render_props(props: &std::collections::BTreeMap<String, String>) -> String {
    props
        .iter()
        .map(|(k, v)| format!("'{}' = '{}'", sql_quote(k), sql_quote(v)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Escape a value for a single-quoted SQL string literal by doubling any
/// embedded quote. The values rendered here (Hadoop class names,
/// delimiters, property keys/values, file paths) never contain a backslash
/// that would need `hive_ddl::decode_hive_escape`-style handling in
/// practice; if one ever did, re-parsing would pass it through unchanged
/// rather than misinterpreting it, since that function only recognizes a
/// fixed, short list of escape sequences and leaves everything else alone.
fn sql_quote(value: &str) -> String {
    value.replace('\'', "''")
}
