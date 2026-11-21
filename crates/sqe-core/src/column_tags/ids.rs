//! `sqe.column-tag-ids`: the Iceberg field id each key of `sqe.column-tags`
//! named when SQE last wrote the map (#920).
//!
//! ```json
//! {"v":1,"tags_sha256":"<hex>","ids":{"email":3,"addr.zip":12}}
//! ```
//!
//! A rename by any engine keeps the field id, so an entry whose id is in the
//! schema applies to the column's current name. A drop retires the id, so an
//! entry whose id is gone is orphaned: kept and shown, never applied. An
//! entry with no recorded id applies by name, as before.
//!
//! `tags_sha256` binds the ids to the exact `sqe.column-tags` value they were
//! computed for. A writer that rewrites the map without the ids (an older
//! SQE, another engine) leaves a mismatch, and the ids are ignored: stale
//! ids must never orphan an entry a newer write meant to apply.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{parse, to_json, ColumnTag, ColumnTagMap};

/// Table property key holding the field ids of the column tags.
pub const IDS_PROP_KEY: &str = "sqe.column-tag-ids";

const VERSION: u32 = 1;

/// The parsed `sqe.column-tag-ids` property.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TagIds {
    pub v: u32,
    pub tags_sha256: String,
    pub ids: BTreeMap<String, i32>,
}

/// A schema's field ids by column path (`addr.zip` for a nested field).
pub trait FieldIds {
    /// The field id of the column at `name`.
    fn id_of(&self, name: &str) -> Option<i32>;
    /// The current path of the column with field id `id`.
    fn name_of(&self, id: i32) -> Option<String>;
}

/// The SHA-256, lower-case hex, of a `sqe.column-tags` value.
pub fn digest(tags_value: &str) -> String {
    hex::encode(Sha256::digest(tags_value.as_bytes()))
}

/// Parse a `sqe.column-tag-ids` value. An unknown version is an error: the
/// caller treats the tag state as unknown and fails closed.
pub fn parse_ids(raw: &str) -> Result<TagIds, serde_json::Error> {
    let ids: TagIds = serde_json::from_str(raw)?;
    if ids.v != VERSION {
        return Err(serde::de::Error::custom(format!(
            "unsupported {IDS_PROP_KEY} version {}",
            ids.v
        )));
    }
    Ok(ids)
}

/// Column tags resolved against a schema.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolved {
    /// The entries that apply, under each column's current name.
    pub applied: ColumnTagMap,
    /// Entries whose recorded field id is not in the schema, under the name
    /// they were stored with: the recorded id and the tags.
    pub orphaned: BTreeMap<String, (i32, Vec<ColumnTag>)>,
}

/// Resolve the `sqe.column-tags` value `tags` against `schema`, using the
/// `sqe.column-tag-ids` value `ids` when it matches `tags`. Absent tags are
/// an empty map. An error (either property malformed) is an unknown tag
/// state.
pub fn resolve(
    tags: Option<&str>,
    ids: Option<&str>,
    schema: &dyn FieldIds,
) -> Result<Resolved, serde_json::Error> {
    let Some(tags) = tags else {
        // Ids without a map describe nothing; still refuse a malformed one.
        if let Some(raw) = ids {
            parse_ids(raw)?;
        }
        return Ok(Resolved::default());
    };
    let map = parse(tags)?;
    let recorded = match ids.map(parse_ids).transpose()? {
        Some(ids) if ids.tags_sha256 == digest(tags) => ids.ids,
        _ => BTreeMap::new(),
    };
    let mut resolved = Resolved::default();
    let sorted: BTreeMap<String, Vec<ColumnTag>> = map.into_iter().collect();
    for (name, tags) in sorted {
        let current = match recorded.get(&name) {
            None => name,
            Some(&id) => match schema.name_of(id) {
                Some(current) => current,
                None => {
                    resolved.orphaned.insert(name, (id, tags));
                    continue;
                }
            },
        };
        merge_into(resolved.applied.entry(current).or_default(), tags);
    }
    Ok(resolved)
}

/// Append the tags whose key `into` does not hold yet: a column named by two
/// entries gets the union of their keys (more masking, never less).
fn merge_into(into: &mut Vec<ColumnTag>, tags: Vec<ColumnTag>) {
    for tag in tags {
        if !into.iter().any(|t| t.key == tag.key) {
            into.push(tag);
        }
    }
}

/// What a writer stores: the `sqe.column-tags` value and the matching
/// `sqe.column-tag-ids` value (`None`: remove the property, no id to keep).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recorded {
    pub tags: String,
    pub ids: Option<String>,
}

/// The property values for `applied` (current names) plus the `orphaned`
/// entries no applied column has taken the name of, with ids from `schema`,
/// the table's committed schema.
pub fn record(
    applied: &ColumnTagMap,
    orphaned: &BTreeMap<String, (i32, Vec<ColumnTag>)>,
    schema: &dyn FieldIds,
) -> Result<Recorded, serde_json::Error> {
    let mut map = applied.clone();
    let mut ids = BTreeMap::new();
    for name in applied.keys() {
        if let Some(id) = schema.id_of(name) {
            ids.insert(name.clone(), id);
        }
    }
    for (name, (id, tags)) in orphaned {
        if !map.contains_key(name) {
            map.insert(name.clone(), tags.clone());
            ids.insert(name.clone(), *id);
        }
    }
    let tags = to_json(&map)?;
    let ids = (!ids.is_empty())
        .then(|| {
            serde_json::to_string(&TagIds {
                v: VERSION,
                tags_sha256: digest(&tags),
                ids,
            })
        })
        .transpose()?;
    Ok(Recorded { tags, ids })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// `email` was field 3 and is now `mail`; field 9 is a new `email`.
    struct Schema(HashMap<&'static str, i32>);

    impl FieldIds for Schema {
        fn id_of(&self, name: &str) -> Option<i32> {
            self.0.get(name).copied()
        }
        fn name_of(&self, id: i32) -> Option<String> {
            self.0
                .iter()
                .find(|(_, v)| **v == id)
                .map(|(k, _)| k.to_string())
        }
    }

    fn schema(cols: &[(&'static str, i32)]) -> Schema {
        Schema(cols.iter().copied().collect())
    }

    fn ids(tags: &str, ids: &[(&str, i32)]) -> String {
        let ids = ids.iter().map(|(n, i)| (n.to_string(), *i)).collect();
        let v = TagIds {
            v: 1,
            tags_sha256: digest(tags),
            ids,
        };
        serde_json::to_string(&v).unwrap()
    }

    fn applied(r: &Resolved) -> BTreeMap<&str, Vec<&str>> {
        let mut out = BTreeMap::new();
        for (c, t) in &r.applied {
            out.insert(c.as_str(), t.iter().map(|t| t.key.as_str()).collect());
        }
        out
    }

    const TAGS: &str = r#"{"email":["pii"],"zip":["geo"]}"#;

    #[test]
    fn a_rename_is_followed_by_field_id() {
        let s = schema(&[("mail", 3), ("zip", 4)]);
        let r = resolve(Some(TAGS), Some(&ids(TAGS, &[("email", 3)])), &s).unwrap();
        let want = BTreeMap::from([("mail", vec!["pii"]), ("zip", vec!["geo"])]);
        assert_eq!(applied(&r), want);
        assert!(r.orphaned.is_empty());
    }

    #[test]
    fn a_drop_and_re_add_under_the_same_name_is_orphaned() {
        let s = schema(&[("email", 9), ("zip", 4)]);
        let r = resolve(Some(TAGS), Some(&ids(TAGS, &[("email", 3)])), &s).unwrap();
        assert_eq!(applied(&r), BTreeMap::from([("zip", vec!["geo"])]));
        assert_eq!(r.orphaned["email"].0, 3);
    }

    #[test]
    fn no_ids_or_stale_ids_apply_by_name() {
        let s = schema(&[("email", 9), ("zip", 4)]);
        let by_name = BTreeMap::from([("email", vec!["pii"]), ("zip", vec!["geo"])]);
        let r = resolve(Some(TAGS), None, &s).unwrap();
        assert_eq!(applied(&r), by_name);
        // Ids computed for another map: another writer changed it.
        let stale = ids(r#"{"email":["pii"]}"#, &[("email", 3)]);
        let r = resolve(Some(TAGS), Some(&stale), &s).unwrap();
        assert_eq!(applied(&r), by_name);
        assert!(r.orphaned.is_empty());
    }

    #[test]
    fn malformed_ids_are_an_error() {
        let s = schema(&[]);
        for bad in ["{", r#"{"ids":{}}"#, r#"{"v":2,"tags_sha256":"","ids":{}}"#] {
            assert!(resolve(Some(TAGS), Some(bad), &s).is_err(), "{bad}");
            assert!(resolve(None, Some(bad), &s).is_err(), "{bad}");
        }
    }

    #[test]
    fn two_entries_on_one_column_take_the_union() {
        let tags = r#"{"email":["pii"],"mail":["pii","gdpr"]}"#;
        let s = schema(&[("mail", 3)]);
        let r = resolve(Some(tags), Some(&ids(tags, &[("email", 3)])), &s).unwrap();
        assert_eq!(applied(&r), BTreeMap::from([("mail", vec!["pii", "gdpr"])]));
    }

    #[test]
    fn a_nested_path_resolves_like_a_column() {
        let tags = r#"{"addr.zip":["geo"]}"#;
        let s = schema(&[("address.zip", 12)]);
        let r = resolve(Some(tags), Some(&ids(tags, &[("addr.zip", 12)])), &s).unwrap();
        assert_eq!(applied(&r), BTreeMap::from([("address.zip", vec!["geo"])]));
    }

    #[test]
    fn record_round_trips_and_keeps_orphans_until_their_name_is_taken() {
        let s = schema(&[("mail", 3), ("zip", 4)]);
        let r = resolve(Some(TAGS), Some(&ids(TAGS, &[("email", 3)])), &s).unwrap();
        let out = record(&r.applied, &r.orphaned, &s).unwrap();
        assert_eq!(out.tags, r#"{"mail":["pii"],"zip":["geo"]}"#);
        let back = resolve(Some(&out.tags), out.ids.as_deref(), &s).unwrap();
        assert_eq!(back, r);
        // An orphan stays with its id while no applied column has its name.
        let s = schema(&[("email", 9), ("zip", 4)]);
        let r = resolve(Some(TAGS), Some(&ids(TAGS, &[("email", 3)])), &s).unwrap();
        let out = record(&r.applied, &r.orphaned, &s).unwrap();
        let back = resolve(Some(&out.tags), out.ids.as_deref(), &s).unwrap();
        assert_eq!(back.orphaned["email"].0, 3);
        // Tagging the new `email` replaces it.
        let mut now = r.applied.clone();
        now.insert("email".into(), vec![ColumnTag::key("contact")]);
        let out = record(&now, &r.orphaned, &s).unwrap();
        let back = resolve(Some(&out.tags), out.ids.as_deref(), &s).unwrap();
        assert!(back.orphaned.is_empty());
        assert_eq!(applied(&back)["email"], vec!["contact"]);
    }

    #[test]
    fn a_map_naming_no_column_records_no_ids() {
        let out = record(&parse(TAGS).unwrap(), &BTreeMap::new(), &schema(&[])).unwrap();
        assert_eq!(out.ids, None);
    }
}
