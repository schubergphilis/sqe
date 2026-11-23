//! A catalog's tags, as a scan provider carries them (#924).
//!
//! With `policy.inherit_catalog_tags` on, every column of every table of a
//! Polaris catalog also holds the tags of that catalog (`ALTER CATALOG ... SET
//! TAGS`, stored in the catalog property `sqe.catalog-tags`). A provider is
//! built with one of three states, and the constructor takes it as an
//! argument next to [`crate::schema_tags::SchemaTagState`]: there is no value
//! that means "no catalog tags" except [`CatalogTagState::NotInherited`] (flag
//! off) and [`CatalogTagState::Known`] with an empty list.
//!
//! This is a separate type, not a second list on the schema state, because
//! the Ranger tag projection reads schema tags (`tag_load_observer`) and must
//! never see catalog tags: they are enforced by SQE only.

use std::collections::HashMap;

use sqe_core::column_tags::ColumnTag;
use sqe_core::object_tags;
use sqe_core::SqeError;
use tracing::debug;

/// What a provider knows about its catalog's tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogTagState {
    /// `inherit_catalog_tags` is off: catalog tags do not affect enforcement
    /// and no catalog call was made to read them.
    NotInherited,
    /// The catalog's tags (empty when it has none, or when it is not a
    /// Polaris REST catalog and so cannot hold any).
    Known(Vec<ColumnTag>),
    /// The flag is on and the tags could not be read (a 403, any other
    /// error, a malformed property). Refuses every read of the table.
    Unknown,
}

impl CatalogTagState {
    /// The tags that enforcement inherits: none when the flag is off, the
    /// catalog's when known, and `None` when the state is `Unknown` (the
    /// caller must fail closed).
    pub fn as_tags(&self) -> Option<&[ColumnTag]> {
        match self {
            Self::NotInherited => Some(&[]),
            Self::Known(tags) => Some(tags),
            Self::Unknown => None,
        }
    }

    /// [`Self::as_tags`], owned.
    pub fn into_tags(self) -> Option<Vec<ColumnTag>> {
        match self {
            Self::NotInherited => Some(Vec::new()),
            Self::Known(tags) => Some(tags),
            Self::Unknown => None,
        }
    }

    /// Read `sqe.catalog-tags` off a catalog's properties. An absent key is
    /// no tags; a malformed value is `Unknown`.
    pub fn from_catalog_properties(props: &HashMap<String, String>) -> Self {
        match object_tags::parse(props.get(object_tags::CATALOG_PROP_KEY).map(String::as_str)) {
            Ok(tags) => Self::Known(tags),
            Err(e) => {
                debug!(error = %e, "malformed sqe.catalog-tags, catalog tag state UNKNOWN (fail closed)");
                Self::Unknown
            }
        }
    }

    /// The state for the answer of `SessionCatalog::catalog_properties`. A
    /// catalog that is not Polaris REST answers `NotImplemented`: it cannot
    /// hold catalog tags, so it has none. Every other error, a 403 included,
    /// is `Unknown`.
    pub fn from_properties_result(result: Result<HashMap<String, String>, SqeError>) -> Self {
        match result {
            Ok(props) => Self::from_catalog_properties(&props),
            Err(SqeError::NotImplemented(_)) => Self::Known(Vec::new()),
            Err(e) => {
                debug!(error = %e, "cannot read the catalog's tags, catalog tag state UNKNOWN (fail closed)");
                Self::Unknown
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(raw: Option<&str>) -> HashMap<String, String> {
        raw.map(|r| (object_tags::CATALOG_PROP_KEY.to_string(), r.to_string()))
            .into_iter()
            .collect()
    }

    #[test]
    fn absent_key_is_known_empty() {
        assert_eq!(
            CatalogTagState::from_catalog_properties(&props(None)),
            CatalogTagState::Known(vec![])
        );
    }

    #[test]
    fn tags_are_read_with_their_values() {
        assert_eq!(
            CatalogTagState::from_catalog_properties(&props(Some(
                r#"["pii",{"owner":"finance"}]"#
            ))),
            CatalogTagState::Known(vec![
                ColumnTag::key("pii"),
                ColumnTag::with_value("owner", "finance"),
            ])
        );
    }

    #[test]
    fn malformed_value_is_unknown() {
        assert_eq!(
            CatalogTagState::from_catalog_properties(&props(Some(r#"{"a":["b"]}"#))),
            CatalogTagState::Unknown
        );
        assert_eq!(
            CatalogTagState::from_catalog_properties(&props(Some("not json"))),
            CatalogTagState::Unknown
        );
    }

    #[test]
    fn enforcement_tags_fail_closed_on_unknown() {
        assert_eq!(CatalogTagState::NotInherited.as_tags(), Some(&[][..]));
        assert_eq!(CatalogTagState::Unknown.as_tags(), None);
        let known = CatalogTagState::Known(vec![ColumnTag::key("pii")]);
        assert_eq!(known.as_tags(), Some(&[ColumnTag::key("pii")][..]));
        assert_eq!(known.into_tags(), Some(vec![ColumnTag::key("pii")]));
        assert_eq!(CatalogTagState::Unknown.into_tags(), None);
    }

    #[test]
    fn a_non_polaris_catalog_has_no_tags_and_any_other_error_is_unknown() {
        assert_eq!(
            CatalogTagState::from_properties_result(Err(SqeError::NotImplemented("x".into()))),
            CatalogTagState::Known(vec![])
        );
        assert_eq!(
            CatalogTagState::from_properties_result(Err(SqeError::Catalog("403".into()))),
            CatalogTagState::Unknown
        );
        assert_eq!(
            CatalogTagState::from_properties_result(Ok(props(Some(r#"["pii"]"#)))),
            CatalogTagState::Known(vec![ColumnTag::key("pii")])
        );
    }
}
