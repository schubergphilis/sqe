//! `SHOW CREATE TABLE` text for an Iceberg table. The coordinator and
//! embedded mode both render through [`render_create_table`], so the DDL a
//! table prints is the same in both and reads back through CREATE TABLE.

use iceberg::spec::TableMetadata;

/// One column of the rendered DDL.
pub struct DdlColumn {
    pub name: String,
    /// The type as CREATE TABLE takes it back (`integer`, `timestamp(9)`).
    pub ddl_type: String,
    pub nullable: bool,
}

/// `CREATE TABLE <name> (<columns>)`, then, when the table's Iceberg
/// metadata is at hand, its `PARTITIONED BY`, sort order and
/// `TBLPROPERTIES`. Names are quoted where an unquoted one would not come
/// back the same: a reserved word, an uppercase letter, a space.
pub fn render_create_table(
    name_parts: &[String],
    columns: &[DdlColumn],
    metadata: Option<&TableMetadata>,
) -> String {
    let mut ddl = format!(
        "CREATE TABLE {} (\n",
        sqe_sql::quote_qualified_if_needed(name_parts, '"')
    );
    let columns: Vec<String> = columns
        .iter()
        .map(|c| {
            let name = sqe_sql::quote_ident_if_needed(&c.name, '"');
            let not_null = if c.nullable { "" } else { " NOT NULL" };
            format!("   {name} {}{not_null}", c.ddl_type)
        })
        .collect();
    ddl.push_str(&columns.join(",\n"));
    ddl.push_str("\n)");
    if let Some(metadata) = metadata {
        if let Some(partitioned_by) = render_partitioned_by(metadata) {
            ddl.push_str(&partitioned_by);
        }
        if let Some(sort_order) = render_sort_order(metadata) {
            ddl.push_str(&sort_order);
        }
        ddl.push_str(&render_tblproperties(metadata));
    }
    ddl
}

/// `TBLPROPERTIES (k = 'v', ...)` for the properties a user set, sorted by
/// key. The Iceberg-managed keys are filtered out so the round-trip matches
/// what CREATE TABLE set.
fn render_tblproperties(metadata: &TableMetadata) -> String {
    // Iceberg-internal property keys we never want to surface, and keys the
    // catalog owns: storage paths bound to this table's location, and the
    // retention, layout and identity settings a managed catalog (Unity
    // Catalog) stamps on every table. Copied into a new table they would
    // point it at this table's files.
    const SUPPRESSED_PREFIXES: &[&str] = &[
        "current-",
        "snapshot-count",
        "last-",
        "uuid",
        "format-version",
        "default-",
        "owner",
        "write.data.path",
        "write.metadata.path",
        "write.object-storage.",
        "gc.",
        "history.expire.",
        "databricks.",
        "io.unitycatalog.",
    ];
    let mut user_props: Vec<(String, String)> = metadata
        .properties()
        .iter()
        .filter(|(k, _)| !SUPPRESSED_PREFIXES.iter().any(|pre| k.starts_with(pre)))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    // The version is metadata, not a property; CREATE TABLE reads it back
    // through this key. Always printed: the server's default version is
    // configurable, so leaving it out could re-create a v2 table as v3, or a
    // v3 one as v2.
    let version = match metadata.format_version() {
        iceberg::spec::FormatVersion::V1 => "1",
        iceberg::spec::FormatVersion::V2 => "2",
        iceberg::spec::FormatVersion::V3 => "3",
    };
    user_props.push(("format-version".to_string(), version.to_string()));
    user_props.sort_by(|a, b| a.0.cmp(&b.0));
    let props: Vec<String> = user_props
        .iter()
        .map(|(k, v)| {
            format!(
                "   '{}' = '{}'",
                k.replace('\'', "''"),
                v.replace('\'', "''")
            )
        })
        .collect();
    format!("\nTBLPROPERTIES (\n{}\n)", props.join(",\n"))
}

/// `PARTITIONED BY (...)` for the table's default partition spec, in the
/// transform syntax CREATE TABLE takes back (`region`, `month(ts)`,
/// `bucket(16, id)`). `None` for an unpartitioned table. A field whose
/// source column or transform SQE cannot name drops the whole clause:
/// a partial spec would round-trip as a different table.
pub fn render_partitioned_by(metadata: &TableMetadata) -> Option<String> {
    use iceberg::spec::Transform;
    let spec = metadata.default_partition_spec();
    if spec.fields().is_empty() {
        return None;
    }
    let schema = metadata.current_schema();
    let mut parts = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let col = sqe_sql::quote_ident_if_needed(schema.name_by_field_id(field.source_id)?, '"');
        parts.push(match field.transform {
            Transform::Identity => col,
            Transform::Year => format!("year({col})"),
            Transform::Month => format!("month({col})"),
            Transform::Day => format!("day({col})"),
            Transform::Hour => format!("hour({col})"),
            Transform::Bucket(n) => format!("bucket({n}, {col})"),
            Transform::Truncate(w) => format!("truncate({w}, {col})"),
            Transform::Void => format!("void({col})"),
            Transform::Unknown => return None,
        });
    }
    Some(format!("\nPARTITIONED BY ({})", parts.join(", ")))
}

/// The table's default sort order as an SQL comment line
/// (`-- sort order: region ASC NULLS FIRST, bucket(16, id) DESC NULLS LAST`).
/// SQE's CREATE TABLE has no clause that sets a sort order, so a comment
/// shows it and the DDL still reads back. `None` when the table is unsorted.
pub fn render_sort_order(metadata: &TableMetadata) -> Option<String> {
    use iceberg::spec::{NullOrder, SortDirection, Transform};
    let order = metadata.default_sort_order();
    if order.is_unsorted() {
        return None;
    }
    let schema = metadata.current_schema();
    let fields: Vec<String> = order
        .fields
        .iter()
        .map(|f| {
            let col = schema
                .name_by_field_id(f.source_id)
                .map(|n| sqe_sql::quote_ident_if_needed(n, '"'))
                .unwrap_or_else(|| format!("<field {}>", f.source_id));
            let expr = match f.transform {
                Transform::Identity => col,
                Transform::Bucket(n) => format!("bucket({n}, {col})"),
                Transform::Truncate(w) => format!("truncate({w}, {col})"),
                other => format!("{other}({col})"),
            };
            let dir = match f.direction {
                SortDirection::Ascending => "ASC",
                SortDirection::Descending => "DESC",
            };
            let nulls = match f.null_order {
                NullOrder::First => "NULLS FIRST",
                NullOrder::Last => "NULLS LAST",
            };
            format!("{expr} {dir} {nulls}")
        })
        .collect();
    Some(format!("\n-- sort order: {}", fields.join(", ")))
}
