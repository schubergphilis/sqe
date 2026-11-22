//! A schema's tags, as a scan provider carries them (#924).
//!
//! With `policy.inherit_schema_tags` on, every column of a table also holds
//! the tags of its schema. A provider is built with one of three states, and
//! the constructor takes it as an argument: there is no value that means "no
//! schema tags" except [`SchemaTagState::NotInherited`] (flag off) and
//! [`SchemaTagState::Known`] with an empty list (flag on, nothing tagged).
//! `Unknown` (a 403, any other error, a malformed property) refuses every
//! read of the table, like malformed table tags do.

use std::collections::HashMap;

use iceberg::{Catalog, NamespaceIdent};
use sqe_core::column_tags::ColumnTag;
use sqe_core::object_tags;
use tracing::debug;

/// What a provider knows about its schema's tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaTagState {
    /// `inherit_schema_tags` is off: schema tags do not affect enforcement
    /// and no catalog call was made to read them.
    NotInherited,
    /// The schema's tags (empty when it has none).
    Known(Vec<ColumnTag>),
    /// The flag is on and the tags could not be read.
    Unknown,
}

impl SchemaTagState {
    /// Read `sqe.schema-tags` off a namespace's properties. An absent key is
    /// no tags; a malformed value is `Unknown`.
    pub fn from_namespace_properties(props: &HashMap<String, String>) -> Self {
        match object_tags::parse(props.get(object_tags::SCHEMA_PROP_KEY).map(String::as_str)) {
            Ok(tags) => Self::Known(tags),
            Err(e) => {
                debug!(error = %e, "malformed sqe.schema-tags, schema tag state UNKNOWN (fail closed)");
                Self::Unknown
            }
        }
    }

    /// The state for one table load. With `enabled` off this makes no
    /// catalog call. With it on, it reads the namespace through `catalog`
    /// (the caller's own, so Polaris applies the caller's grants) and any
    /// error is `Unknown`.
    pub async fn load(enabled: bool, catalog: &dyn Catalog, namespace: &NamespaceIdent) -> Self {
        if !enabled {
            return Self::NotInherited;
        }
        match catalog.get_namespace(namespace).await {
            Ok(ns) => Self::from_namespace_properties(ns.properties()),
            Err(e) => {
                debug!(error = %e, namespace = ?namespace, "cannot read the schema's tags, schema tag state UNKNOWN (fail closed)");
                Self::Unknown
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(raw: Option<&str>) -> HashMap<String, String> {
        raw.map(|r| (object_tags::SCHEMA_PROP_KEY.to_string(), r.to_string()))
            .into_iter()
            .collect()
    }

    #[test]
    fn absent_key_is_known_empty() {
        assert_eq!(
            SchemaTagState::from_namespace_properties(&props(None)),
            SchemaTagState::Known(vec![])
        );
    }

    #[test]
    fn tags_are_read_with_their_values() {
        assert_eq!(
            SchemaTagState::from_namespace_properties(&props(Some(
                r#"["pii",{"owner":"finance"}]"#
            ))),
            SchemaTagState::Known(vec![
                ColumnTag::key("pii"),
                ColumnTag::with_value("owner", "finance"),
            ])
        );
    }

    #[test]
    fn malformed_value_is_unknown() {
        assert_eq!(
            SchemaTagState::from_namespace_properties(&props(Some(r#"{"a":["b"]}"#))),
            SchemaTagState::Unknown
        );
        assert_eq!(
            SchemaTagState::from_namespace_properties(&props(Some("not json"))),
            SchemaTagState::Unknown
        );
    }
}
