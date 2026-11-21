//! Column tags resolved by Iceberg field id (#920): the bridge from an
//! Iceberg [`Schema`] to [`sqe_core::column_tags::ids`].

use std::collections::HashMap;

use iceberg::spec::Schema;
use sqe_core::column_tags::ids::{self, FieldIds, Resolved, IDS_PROP_KEY};
use sqe_core::column_tags::PROP_KEY;

/// An Iceberg schema's field ids by column path (`addr.zip` when nested).
pub struct SchemaFieldIds<'a>(pub &'a Schema);

impl FieldIds for SchemaFieldIds<'_> {
    fn id_of(&self, name: &str) -> Option<i32> {
        self.0.field_id_by_name(name)
    }

    fn name_of(&self, id: i32) -> Option<String> {
        self.0.name_by_field_id(id).map(str::to_string)
    }
}

/// A table's `sqe.column-tags`, resolved against `schema`: the schema the
/// scan exposes (a snapshot's for time travel), or the current one. An error
/// is an unknown tag state; the caller fails closed.
pub fn resolve_table_tags(
    properties: &HashMap<String, String>,
    schema: &Schema,
) -> Result<Resolved, serde_json::Error> {
    ids::resolve(
        properties.get(PROP_KEY).map(String::as_str),
        properties.get(IDS_PROP_KEY).map(String::as_str),
        &SchemaFieldIds(schema),
    )
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PrimitiveType, StructType, Type};

    use super::*;

    fn schema() -> Schema {
        let addr = StructType::new(vec![NestedField::optional(
            4,
            "postcode",
            Type::Primitive(PrimitiveType::String),
        )
        .into()]);
        Schema::builder()
            .with_fields(vec![
                NestedField::optional(3, "mail", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(2, "address", Type::Struct(addr)).into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn ids_resolve_top_level_and_nested_paths() {
        let s = schema();
        let f = SchemaFieldIds(&s);
        assert_eq!(f.id_of("mail"), Some(3));
        assert_eq!(f.id_of("address.postcode"), Some(4));
        assert_eq!(f.name_of(4).as_deref(), Some("address.postcode"));
        assert_eq!(f.name_of(99), None);
    }

    #[test]
    fn a_table_resolves_its_renamed_columns() {
        let tags = r#"{"email":["pii"],"addr.zip":["geo"]}"#;
        let ids = format!(
            r#"{{"v":1,"tags_sha256":"{}","ids":{{"email":3,"addr.zip":4}}}}"#,
            ids::digest(tags)
        );
        let props = HashMap::from([
            (PROP_KEY.to_string(), tags.to_string()),
            (IDS_PROP_KEY.to_string(), ids),
        ]);
        let r = resolve_table_tags(&props, &schema()).unwrap();
        let mut cols: Vec<_> = r.applied.keys().cloned().collect();
        cols.sort();
        assert_eq!(cols, ["address.postcode", "mail"]);
    }
}
