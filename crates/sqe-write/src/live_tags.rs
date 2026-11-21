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
    /// commit. When no entry needs an id, existing ids are overwritten with
    /// an empty set bound to the new map. The updates never remove a
    /// property: that is a catalog privilege a tag steward need not hold
    /// (#936).
    pub fn record(
        &self,
        applied: &ColumnTagMap,
        committed: &Schema,
    ) -> Result<Vec<TableUpdate>, serde_json::Error> {
        let recorded = ids::record(applied, &self.resolved.orphaned, &SchemaFieldIds(committed))?;
        let ids = match recorded.ids {
            Some(value) => Some(value),
            None if self.raw_ids.is_some() => Some(ids::empty_ids(&recorded.tags)?),
            None => None,
        };
        let mut set = std::collections::HashMap::from([(PROP_KEY.to_string(), recorded.tags)]);
        if let Some(value) = ids {
            set.insert(IDS_PROP_KEY.to_string(), value);
        }
        Ok(vec![TableUpdate::SetProperties { updates: set }])
    }

    /// The update that puts both properties back as they were read: the
    /// exact values, so the digest still binds. A property that was absent
    /// comes back as its empty form (`{}`, ids bound to the restored map),
    /// never as a removal (#936).
    pub fn restore(&self) -> Vec<TableUpdate> {
        let tags = self.raw_tags.clone().unwrap_or_else(|| "{}".to_string());
        let ids = match &self.raw_ids {
            Some(value) => value.clone(),
            None => ids::empty_ids(&tags).expect("serialising a fixed struct"),
        };
        let set = std::collections::HashMap::from([
            (PROP_KEY.to_string(), tags),
            (IDS_PROP_KEY.to_string(), ids),
        ]);
        vec![TableUpdate::SetProperties { updates: set }]
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

    /// Removing every tag empties the ids, bound to the new map, with one
    /// SetProperties and no removal (#936).
    #[test]
    fn an_empty_map_empties_the_ids() {
        let s = schema(&[(1, "ssn")]);
        let tags = r#"{"ssn":["PII"]}"#;
        let bound = format!(
            r#"{{"v":1,"tags_sha256":"{}","ids":{{"ssn":1}}}}"#,
            ids::digest(tags)
        );
        let live = live(Some(tags), Some(&bound), &s);
        let updates = live.record(&ColumnTagMap::new(), &s).unwrap();
        assert_eq!(updates.len(), 1, "{updates:?}");
        let set = set_of(&updates);
        let ids = ids::parse_ids(&set[IDS_PROP_KEY]).unwrap();
        assert!(ids.ids.is_empty());
        assert_eq!(ids.tags_sha256, ids::digest(&set[PROP_KEY]));
    }

    /// No tag write, in any start state, removes a property: removal is a
    /// catalog privilege a tag steward need not hold (#936).
    #[test]
    fn no_tag_write_removes_a_property() {
        let s = schema(&[(1, "ssn"), (2, "email")]);
        let tags = r#"{"ssn":["PII"]}"#;
        let bound = format!(
            r#"{{"v":1,"tags_sha256":"{}","ids":{{"ssn":1}}}}"#,
            ids::digest(tags)
        );
        let mut tagged = ColumnTagMap::new();
        tagged.insert("email".into(), vec![ColumnTag::key("PII")]);
        let starts = [
            (None, None),
            (Some(tags), None),
            (Some(tags), Some(bound.as_str())),
            (None, Some(bound.as_str())),
        ];
        for (raw_tags, raw_ids) in starts {
            let state = live(raw_tags, raw_ids, &s);
            let writes = [
                state.record(&ColumnTagMap::new(), &s).unwrap(),
                state.record(&tagged, &s).unwrap(),
                state.record(state.applied(), &s).unwrap(),
                state.restore(),
            ];
            for updates in writes {
                assert!(
                    updates
                        .iter()
                        .all(|u| matches!(u, TableUpdate::SetProperties { .. })),
                    "{raw_tags:?} {raw_ids:?}: {updates:?}"
                );
            }
        }
    }

    /// A rollback restores the exact bytes, so the digest still binds; an
    /// absent property comes back empty, never as a removal (#936).
    #[test]
    fn restore_puts_back_the_exact_values() {
        let s = schema(&[(1, "ssn")]);
        let tags = r#"{ "ssn": ["PII"] }"#;
        let updates = live(Some(tags), None, &s).restore();
        assert_eq!(updates.len(), 1, "{updates:?}");
        let set = set_of(&updates);
        assert_eq!(set[PROP_KEY], tags);
        let ids = ids::parse_ids(&set[IDS_PROP_KEY]).unwrap();
        assert!(ids.ids.is_empty());
        assert_eq!(ids.tags_sha256, ids::digest(tags));

        let set = set_of(&live(None, None, &s).restore());
        let resolved = resolve_table_tags(&set, &s).unwrap();
        assert!(resolved.applied.is_empty() && resolved.orphaned.is_empty());
    }
}
