//! The `sqe.json-columns` table property (#848).
//!
//! Before format v3 a `JSON` column is stored as an Iceberg string: the
//! stored type cannot tell it from a `VARCHAR`. The property lists the
//! top-level columns declared `JSON`, by name, and a scan marks those
//! fields as JSON text again ([`JSON_FIELD_MARKER`]), so `CAST(j AS JSON)`
//! returns the value instead of quoting it and the Trino protocol reports
//! the column as `json`.
//!
//! Keyed by name, like `sqe.column-tags`: `ALTER TABLE .. RENAME COLUMN` and
//! `DROP COLUMN` rewrite it in the same commit as the schema change. On a
//! v3 table a declared `JSON` column is a variant and is not listed; only a
//! string column retyped with `SET DATA TYPE JSON` is.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};

/// Table property key: JSON array of the top-level column names that hold
/// JSON text.
pub const JSON_COLUMNS_PROP: &str = "sqe.json-columns";

/// Field metadata key that marks a string field as JSON text. The same key
/// as `sqe_trino_protocol::encode::JSON_TEXT_MARKER` (a test in
/// `sqe-trino-functions` holds them together).
pub const JSON_FIELD_MARKER: &str = "sqe.json";

/// One schema change that moves a JSON column's name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonColumnChange {
    /// A column now holds JSON text (`ADD COLUMN j JSON`,
    /// `ALTER COLUMN j SET DATA TYPE JSON`).
    Add(String),
    /// A column no longer holds JSON text (dropped, or retyped).
    Remove(String),
    /// A column was renamed.
    Rename { old: String, new: String },
}

/// The JSON columns a table's properties list. A missing property is the
/// empty set; so is a value that is not a JSON array of strings (a user
/// can set the property by hand), since a wrong marker only changes how a
/// value is rendered.
pub fn json_columns(properties: &HashMap<String, String>) -> BTreeSet<String> {
    properties
        .get(JSON_COLUMNS_PROP)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .map(|names| names.into_iter().collect())
        .unwrap_or_default()
}

/// The property value for `columns`: a JSON array, sorted.
pub fn encode(columns: &BTreeSet<String>) -> String {
    serde_json::to_string(columns).unwrap_or_else(|_| "[]".to_string())
}

/// Apply `changes` in order. `None` when the set did not change, so a plain
/// ALTER on a table without JSON columns writes no property.
pub fn apply_changes(
    current: &BTreeSet<String>,
    changes: &[JsonColumnChange],
) -> Option<BTreeSet<String>> {
    let mut next = current.clone();
    for change in changes {
        match change {
            JsonColumnChange::Add(name) => {
                next.insert(name.clone());
            }
            JsonColumnChange::Remove(name) => {
                next.remove(name);
            }
            JsonColumnChange::Rename { old, new } => {
                if next.remove(old) {
                    next.insert(new.clone());
                }
            }
        }
    }
    (next != *current).then_some(next)
}

/// True for the Arrow string types a JSON column can be read as.
pub fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// True when `field` is a string field carrying [`JSON_FIELD_MARKER`].
pub fn is_marked(field: &Field) -> bool {
    is_string_type(field.data_type())
        && field.metadata().get(JSON_FIELD_MARKER).map(String::as_str) == Some("true")
}

/// The names of the top-level fields of `schema` that are marked JSON text:
/// what a CTAS records in [`JSON_COLUMNS_PROP`].
pub fn marked_columns(schema: &Schema) -> BTreeSet<String> {
    schema
        .fields()
        .iter()
        .filter(|f| is_marked(f))
        .map(|f| f.name().clone())
        .collect()
}

/// `schema` with [`JSON_FIELD_MARKER`] on every top-level string field named
/// in `columns`. `None` when no field changes.
pub fn mark_schema(schema: &Schema, columns: &BTreeSet<String>) -> Option<Schema> {
    if columns.is_empty() {
        return None;
    }
    let mut changed = false;
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| {
            if !columns.contains(f.name()) || !is_string_type(f.data_type()) || is_marked(f) {
                return Arc::clone(f);
            }
            changed = true;
            let mut metadata = f.metadata().clone();
            metadata.insert(JSON_FIELD_MARKER.to_string(), "true".to_string());
            Arc::new(f.as_ref().clone().with_metadata(metadata))
        })
        .collect();
    changed.then(|| Schema::new_with_metadata(fields, schema.metadata().clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(value: &str) -> HashMap<String, String> {
        HashMap::from([(JSON_COLUMNS_PROP.to_string(), value.to_string())])
    }

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn missing_or_malformed_property_is_empty() {
        assert!(json_columns(&HashMap::new()).is_empty());
        assert!(json_columns(&props("payload")).is_empty());
        assert!(json_columns(&props("{\"a\":1}")).is_empty());
        assert_eq!(json_columns(&props("[\"b\",\"a\"]")), set(&["a", "b"]));
    }

    #[test]
    fn encode_round_trips_sorted() {
        let cols = set(&["z", "a"]);
        assert_eq!(encode(&cols), "[\"a\",\"z\"]");
        assert_eq!(json_columns(&props(&encode(&cols))), cols);
    }

    #[test]
    fn changes_follow_add_rename_and_remove() {
        let current = set(&["j"]);
        let renamed = apply_changes(
            &current,
            &[JsonColumnChange::Rename {
                old: "j".into(),
                new: "k".into(),
            }],
        );
        assert_eq!(renamed, Some(set(&["k"])));
        assert_eq!(
            apply_changes(&current, &[JsonColumnChange::Remove("j".into())]),
            Some(BTreeSet::new())
        );
        assert_eq!(
            apply_changes(&current, &[JsonColumnChange::Add("x".into())]),
            Some(set(&["j", "x"]))
        );
    }

    #[test]
    fn unrelated_changes_write_nothing() {
        let current = set(&["j"]);
        assert_eq!(
            apply_changes(
                &current,
                &[
                    JsonColumnChange::Remove("other".into()),
                    JsonColumnChange::Rename {
                        old: "x".into(),
                        new: "y".into(),
                    },
                    JsonColumnChange::Add("j".into()),
                ],
            ),
            None
        );
    }

    #[test]
    fn mark_schema_marks_named_string_fields_only() {
        let schema = Schema::new(vec![
            Field::new("j", DataType::Utf8View, true),
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]);
        let marked = mark_schema(&schema, &set(&["j", "n", "missing"])).expect("changed");
        assert!(is_marked(marked.field(0)));
        assert!(!is_marked(marked.field(1)));
        assert!(marked.field(1).metadata().is_empty());
        assert!(!is_marked(marked.field(2)));
        assert_eq!(marked_columns(&marked), set(&["j"]));
        assert!(mark_schema(&marked, &set(&["j"])).is_none());
        assert!(mark_schema(&schema, &BTreeSet::new()).is_none());
    }

    #[test]
    fn mark_schema_keeps_existing_field_metadata() {
        let field = Field::new("j", DataType::Utf8, true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "3".to_string(),
        )]));
        let marked = mark_schema(&Schema::new(vec![field]), &set(&["j"])).expect("changed");
        let md = marked.field(0).metadata();
        assert_eq!(md.get("PARQUET:field_id").map(String::as_str), Some("3"));
        assert_eq!(md.get(JSON_FIELD_MARKER).map(String::as_str), Some("true"));
    }
}
