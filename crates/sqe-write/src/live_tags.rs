//! The column tags a tag writer reads and writes back, by field id (#920).
//!
//! A writer resolves the live `sqe.column-tags` against the current schema,
//! edits the map under the columns' current names, and records the result
//! with ids from the schema the commit leaves current. Orphaned entries (their
//! column was dropped) are carried as they are. A rollback puts back the exact
//! property values it read, so the ids stay bound to their map.

use iceberg::spec::{Schema, TableMetadata};
use iceberg::TableUpdate;
use sqe_catalog::column_tag_ids::{resolve_table_tags, SchemaFieldIds};
use sqe_core::column_tags::ids::{self, Resolved, IDS_PROP_KEY};
use sqe_core::column_tags::{ColumnTagMap, PROP_KEY};

/// A table's column tags as a writer found them.
#[derive(Debug, Clone)]
pub(crate) struct LiveTags {
    /// The map resolved against the table's current schema.
    pub resolved: Resolved,
    raw_tags: Option<String>,
    raw_ids: Option<String>,
}

impl LiveTags {
    /// Read and resolve the tags of `metadata`. An error is an unknown tag
    /// state: the writer refuses rather than wipe what it cannot read.
    pub fn read(metadata: &TableMetadata) -> Result<Self, serde_json::Error> {
        let properties = metadata.properties();
        Ok(Self {
            resolved: resolve_table_tags(properties, metadata.current_schema())?,
            raw_tags: properties.get(PROP_KEY).cloned(),
            raw_ids: properties.get(IDS_PROP_KEY).cloned(),
        })
    }

    /// The entries that apply, under the current column names.
    pub fn applied(&self) -> &ColumnTagMap {
        &self.resolved.applied
    }

    /// The property updates that store `applied` (current names) plus the
    /// orphans, with ids from `committed`, the schema current after the
    /// commit. Ids no entry needs remove the ids property.
    pub fn record(
        &self,
        applied: &ColumnTagMap,
        committed: &Schema,
    ) -> Result<Vec<TableUpdate>, serde_json::Error> {
        let recorded = ids::record(applied, &self.resolved.orphaned, &SchemaFieldIds(committed))?;
        let mut set = std::collections::HashMap::from([(PROP_KEY.to_string(), recorded.tags)]);
        let mut updates = Vec::new();
        match recorded.ids {
            Some(value) => {
                set.insert(IDS_PROP_KEY.to_string(), value);
            }
            None if self.raw_ids.is_some() => updates.push(TableUpdate::RemoveProperties {
                removals: vec![IDS_PROP_KEY.to_string()],
            }),
            None => {}
        }
        updates.insert(0, TableUpdate::SetProperties { updates: set });
        Ok(updates)
    }

    /// The updates that put both properties back exactly as they were read.
    pub fn restore(&self) -> Vec<TableUpdate> {
        let mut set = std::collections::HashMap::new();
        let mut removals = Vec::new();
        for (key, raw) in [(PROP_KEY, &self.raw_tags), (IDS_PROP_KEY, &self.raw_ids)] {
            match raw {
                Some(value) => {
                    set.insert(key.to_string(), value.clone());
                }
                None => removals.push(key.to_string()),
            }
        }
        let mut updates = vec![TableUpdate::SetProperties { updates: set }];
        if !removals.is_empty() {
            updates.push(TableUpdate::RemoveProperties { removals });
        }
        updates
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use sqe_core::column_tags::ColumnTag;

    use super::*;

    fn schema(fields: &[(i32, &str)]) -> Schema {
        let fields = fields.iter().map(|(id, name)| {
            NestedField::optional(*id, *name, Type::Primitive(PrimitiveType::String)).into()
        });
        Schema::builder()
            .with_fields(fields.collect::<Vec<_>>())
            .build()
            .unwrap()
    }

    fn live(tags: Option<&str>, ids: Option<&str>, current: &Schema) -> LiveTags {
        let mut properties = HashMap::new();
        if let Some(tags) = tags {
            properties.insert(PROP_KEY.to_string(), tags.to_string());
        }
        if let Some(ids) = ids {
            properties.insert(IDS_PROP_KEY.to_string(), ids.to_string());
        }
        LiveTags {
            resolved: resolve_table_tags(&properties, current).unwrap(),
            raw_tags: properties.get(PROP_KEY).cloned(),
            raw_ids: properties.get(IDS_PROP_KEY).cloned(),
        }
    }

    fn set_of(updates: &[TableUpdate]) -> HashMap<String, String> {
        match &updates[0] {
            TableUpdate::SetProperties { updates } => updates.clone(),
            other => panic!("expected SetProperties, got {other:?}"),
        }
    }

    /// A legacy map gets ids from the committed schema on its next write,
    /// and the ids are bound to the value written next to them.
    #[test]
    fn a_write_records_ids_bound_to_the_map() {
        let s = schema(&[(1, "ssn"), (2, "email")]);
        let live = live(Some(r#"{"ssn":["PII"]}"#), None, &s);
        let mut applied = live.applied().clone();
        applied.insert("email".into(), vec![ColumnTag::key("PII")]);
        let set = set_of(&live.record(&applied, &s).unwrap());
        let ids = ids::parse_ids(&set[IDS_PROP_KEY]).unwrap();
        assert_eq!(ids.tags_sha256, ids::digest(&set[PROP_KEY]));
        assert_eq!(ids.ids, [("email".into(), 2), ("ssn".into(), 1)].into());
    }

    /// An orphan rides along with its id; it stays unapplied.
    #[test]
    fn orphans_are_carried() {
        let s = schema(&[(1, "ssn")]);
        let tags = r#"{"gone":["PII"],"ssn":["PII"]}"#;
        let bound = format!(
            r#"{{"v":1,"tags_sha256":"{}","ids":{{"gone":7,"ssn":1}}}}"#,
            ids::digest(tags)
        );
        let live = live(Some(tags), Some(&bound), &s);
        assert_eq!(live.applied().keys().collect::<Vec<_>>(), ["ssn"]);
        let set = set_of(&live.record(live.applied(), &s).unwrap());
        let props = HashMap::from([
            (PROP_KEY.to_string(), set[PROP_KEY].clone()),
            (IDS_PROP_KEY.to_string(), set[IDS_PROP_KEY].clone()),
        ]);
        let again = resolve_table_tags(&props, &s).unwrap();
        assert_eq!(again.orphaned["gone"].0, 7);
        assert_eq!(again.applied, live.resolved.applied);
    }

    /// Removing every tag removes the ids property too.
    #[test]
    fn an_empty_map_drops_the_ids() {
        let s = schema(&[(1, "ssn")]);
        let tags = r#"{"ssn":["PII"]}"#;
        let bound = format!(
            r#"{{"v":1,"tags_sha256":"{}","ids":{{"ssn":1}}}}"#,
            ids::digest(tags)
        );
        let live = live(Some(tags), Some(&bound), &s);
        let updates = live.record(&ColumnTagMap::new(), &s).unwrap();
        assert!(matches!(
            &updates[1],
            TableUpdate::RemoveProperties { removals } if removals == &[IDS_PROP_KEY.to_string()]
        ));
    }

    /// A rollback restores the exact bytes, so the digest still binds.
    #[test]
    fn restore_puts_back_the_exact_values() {
        let s = schema(&[(1, "ssn")]);
        let tags = r#"{ "ssn": ["PII"] }"#;
        let live = live(Some(tags), None, &s);
        let updates = live.restore();
        assert_eq!(set_of(&updates)[PROP_KEY], tags);
        assert!(matches!(
            &updates[1],
            TableUpdate::RemoveProperties { removals } if removals == &[IDS_PROP_KEY.to_string()]
        ));
    }
}
