//! Tags on a table, a schema or a catalog, as opposed to its columns (#922).
//!
//! A table's tags live in the table property [`TABLE_PROP_KEY`], a schema's
//! in the namespace property [`SCHEMA_PROP_KEY`], a catalog's in the Polaris
//! catalog property [`CATALOG_PROP_KEY`]. Each holds a JSON list in the
//! entry format `sqe.column-tags` uses for one column:
//!
//! ```json
//! ["gold", {"owner": "finance"}]
//! ```
//!
//! Object tags are metadata: no policy reads them. `[]` and a missing key
//! both mean "no tags". UNSET of the last tag writes `[]`, because a
//! namespace property update cannot remove a key.

use std::collections::BTreeMap;

use crate::column_tags::ColumnTag;

/// Table property key holding the table's own tags.
pub const TABLE_PROP_KEY: &str = "sqe.table-tags";

/// Namespace property key holding the schema's tags.
pub const SCHEMA_PROP_KEY: &str = "sqe.schema-tags";

/// Polaris catalog property key holding the catalog's tags.
pub const CATALOG_PROP_KEY: &str = "sqe.catalog-tags";

/// Parse an object tag property value. A missing or blank value is no tags.
pub fn parse(raw: Option<&str>) -> Result<Vec<ColumnTag>, serde_json::Error> {
    match raw.map(str::trim) {
        None | Some("") => Ok(Vec::new()),
        Some(raw) => serde_json::from_str(raw),
    }
}

/// Serialize object tags for the property, in stored order.
pub fn to_json(tags: &[ColumnTag]) -> Result<String, serde_json::Error> {
    serde_json::to_string(tags)
}

/// Add `keys` to `current`, or remove them. A set key takes its value from
/// `values`, or none when `values` has no entry for it, and keeps its place
/// when it was already there. Removal matches the key whatever its value.
pub fn apply(
    current: &[ColumnTag],
    set: bool,
    keys: &[String],
    values: &BTreeMap<String, String>,
) -> Vec<ColumnTag> {
    if !set {
        return current
            .iter()
            .filter(|t| !keys.contains(&t.key))
            .cloned()
            .collect();
    }
    let mut out: Vec<ColumnTag> = current.to_vec();
    for key in keys {
        let tag = match values.get(key) {
            Some(v) => ColumnTag::with_value(key.clone(), v.clone()),
            None => ColumnTag::key(key.clone()),
        };
        match out.iter_mut().find(|t| t.key == *key) {
            Some(existing) => *existing = tag,
            None => out.push(tag),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(k: &[&str]) -> Vec<String> {
        k.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn round_trips_the_column_entry_format() {
        let tags = vec![
            ColumnTag::key("gold"),
            ColumnTag::with_value("owner", "finance"),
        ];
        let json = to_json(&tags).unwrap();
        assert_eq!(json, r#"["gold",{"owner":"finance"}]"#);
        assert_eq!(parse(Some(&json)).unwrap(), tags);
    }

    #[test]
    fn missing_blank_and_empty_list_are_no_tags() {
        assert!(parse(None).unwrap().is_empty());
        assert!(parse(Some("  ")).unwrap().is_empty());
        assert!(parse(Some("[]")).unwrap().is_empty());
        assert!(parse(Some(r#"{"a":["b"]}"#)).is_err());
    }

    #[test]
    fn set_replaces_a_value_in_place_and_appends_new_keys() {
        let current = vec![
            ColumnTag::with_value("owner", "finance"),
            ColumnTag::key("gold"),
        ];
        let values = BTreeMap::from([("owner".to_string(), "risk".to_string())]);
        let got = apply(&current, true, &keys(&["owner", "pii"]), &values);
        assert_eq!(
            got,
            vec![
                ColumnTag::with_value("owner", "risk"),
                ColumnTag::key("gold"),
                ColumnTag::key("pii"),
            ]
        );
        // Setting a key without a value drops its old value.
        let got = apply(&got, true, &keys(&["owner"]), &BTreeMap::new());
        assert_eq!(got[0], ColumnTag::key("owner"));
    }

    #[test]
    fn unset_removes_by_key_and_can_empty_the_list() {
        let current = vec![
            ColumnTag::with_value("owner", "finance"),
            ColumnTag::key("gold"),
        ];
        let got = apply(
            &current,
            false,
            &keys(&["owner", "absent"]),
            &BTreeMap::new(),
        );
        assert_eq!(got, vec![ColumnTag::key("gold")]);
        let got = apply(&got, false, &keys(&["gold"]), &BTreeMap::new());
        assert_eq!(to_json(&got).unwrap(), "[]");
    }
}
