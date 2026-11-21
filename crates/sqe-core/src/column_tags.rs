//! The `sqe.column-tags` table property: column name to tag list.
//!
//! A tag is either a bare key or a key with a value:
//!
//! ```json
//! {"email": ["pii", {"pii_kind": "email"}], "region": ["geo"]}
//! ```
//!
//! A key-only tag is a JSON string, as it always was, so a table with no
//! tag values serializes byte for byte the way older SQE versions wrote it.
//! A tag with a value is a one-entry object `{"key": "value"}`. Tag policies
//! match on the key only; the value is kept for display (`SHOW TAGS`,
//! `information_schema.column_tags`) and for later policy conditions.
//!
//! An SQE that predates values fails to parse a list holding an object and
//! treats the table's tag state as unknown, so it fails closed (masks or
//! refuses) rather than open.
//!
//! Every reader of the property should go through [`parse`] (writers that
//! must keep values) or [`parse_keys`] (policy, which needs keys only).

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub mod ids;

/// Table property key holding the column tags.
pub const PROP_KEY: &str = "sqe.column-tags";

/// One tag on a column: a key and an optional value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnTag {
    pub key: String,
    pub value: Option<String>,
}

impl ColumnTag {
    /// A key-only tag.
    pub fn key(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: None,
        }
    }

    /// A tag with a value.
    pub fn with_value(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }
}

/// Column name to its tags, in stored order.
pub type ColumnTagMap = HashMap<String, Vec<ColumnTag>>;

impl Serialize for ColumnTag {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.value {
            None => serializer.serialize_str(&self.key),
            Some(value) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry(&self.key, value)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ColumnTag {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TagVisitor;

        impl<'de> Visitor<'de> for TagVisitor {
            type Value = ColumnTag;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a tag name or a one-entry {\"tag\": \"value\"} object")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<ColumnTag, E> {
                Ok(ColumnTag::key(v))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<ColumnTag, A::Error> {
                let Some((key, value)) = map.next_entry::<String, String>()? else {
                    return Err(de::Error::custom(
                        "a tag object must hold exactly one entry",
                    ));
                };
                if map.next_entry::<String, String>()?.is_some() {
                    return Err(de::Error::custom(
                        "a tag object must hold exactly one entry",
                    ));
                }
                Ok(ColumnTag::with_value(key, value))
            }
        }

        deserializer.deserialize_any(TagVisitor)
    }
}

/// Parse a `sqe.column-tags` value, keeping tag values.
pub fn parse(raw: &str) -> Result<ColumnTagMap, serde_json::Error> {
    serde_json::from_str(raw)
}

/// Parse a `sqe.column-tags` value into column to tag keys, the shape tag
/// policies match on. Values are dropped.
pub fn parse_keys(raw: &str) -> Result<HashMap<String, Vec<String>>, serde_json::Error> {
    parse(raw).map(|map| keys(&map))
}

/// The tag keys of each column, in stored order.
pub fn keys(map: &ColumnTagMap) -> HashMap<String, Vec<String>> {
    map.iter()
        .map(|(column, tags)| (column.clone(), tags.iter().map(|t| t.key.clone()).collect()))
        .collect()
}

/// Key-only tags from a column to tag keys map, as older code builds it.
pub fn from_keys(map: &HashMap<String, Vec<String>>) -> ColumnTagMap {
    map.iter()
        .map(|(column, tags)| (column.clone(), tags.iter().map(ColumnTag::key).collect()))
        .collect()
}

/// Serialize a tag map for the property, with columns in a stable order.
pub fn to_json(map: &ColumnTagMap) -> Result<String, serde_json::Error> {
    let sorted: BTreeMap<&String, &Vec<ColumnTag>> = map.iter().collect();
    serde_json::to_string(&sorted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_only_lists_parse_as_before() {
        let map = parse(r#"{"email":["PII","GDPR"]}"#).unwrap();
        assert_eq!(
            map["email"],
            vec![ColumnTag::key("PII"), ColumnTag::key("GDPR")]
        );
        assert_eq!(
            parse_keys(r#"{"email":["PII","GDPR"]}"#).unwrap()["email"],
            vec!["PII", "GDPR"]
        );
    }

    #[test]
    fn valued_tags_parse_and_keys_drop_the_value() {
        let raw = r#"{"card":["pii",{"pii_kind":"card"}]}"#;
        let map = parse(raw).unwrap();
        assert_eq!(
            map["card"],
            vec![
                ColumnTag::key("pii"),
                ColumnTag::with_value("pii_kind", "card")
            ]
        );
        assert_eq!(parse_keys(raw).unwrap()["card"], vec!["pii", "pii_kind"]);
    }

    #[test]
    fn key_only_maps_serialize_byte_for_byte_as_before() {
        let map = from_keys(&HashMap::from([
            ("region".to_string(), vec!["geo".to_string()]),
            ("email".to_string(), vec!["pii".to_string()]),
        ]));
        assert_eq!(
            to_json(&map).unwrap(),
            r#"{"email":["pii"],"region":["geo"]}"#
        );
    }

    #[test]
    fn valued_tags_round_trip() {
        let raw = r#"{"card":["pii",{"pii_kind":"card"}]}"#;
        assert_eq!(to_json(&parse(raw).unwrap()).unwrap(), raw);
    }

    #[test]
    fn malformed_shapes_are_errors() {
        for raw in [
            "not json",
            r#"["PII"]"#,
            r#"{"email":"PII"}"#,
            r#"{"email":[{"a":"1","b":"2"}]}"#,
            r#"{"email":[{}]}"#,
            r#"{"email":[{"a":1}]}"#,
            r#"{"email":[1]}"#,
            r#"{"email":[null]}"#,
        ] {
            assert!(parse(raw).is_err(), "{raw} must not parse");
            assert!(parse_keys(raw).is_err(), "{raw} must not parse as keys");
        }
    }

    #[test]
    fn empty_lists_and_objects_parse() {
        assert!(parse("{}").unwrap().is_empty());
        assert!(parse(r#"{"c":[]}"#).unwrap()["c"].is_empty());
    }
}
