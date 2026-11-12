//! User-supplied Iceberg snapshot summary properties.
//!
//! A [`SnapshotProperties`] is a validated set of key/value pairs that a write
//! stamps into the `summary` of the snapshot it commits (`SET
//! SNAPSHOT_PROPERTIES`, Flight SQL ingest `snapshot-property.<key>` options).
//! Validation happens once, at construction, so every later consumer can treat
//! the set as safe to hand to `set_snapshot_properties`.
//!
//! The rules live in the `write-snapshot-properties` OpenSpec change
//! (requirement "Keys and values are validated"):
//!
//! - key: 1 to [`MAX_KEY_BYTES`] bytes from `[A-Za-z0-9._-]`;
//! - key refused, case-insensitively, when it is an Iceberg computed summary
//!   key, `operation`, `wap.id`, `published-wap-id` or `source-snapshot-id`;
//! - key refused when it starts, case-insensitively, with `partitions.`,
//!   `sqe.`, `commit.` or `write.`;
//! - value: at most [`MAX_VALUE_BYTES`] bytes, no control characters;
//! - at most [`MAX_PAIRS`] pairs and [`MAX_TOTAL_BYTES`] bytes of keys and
//!   values together (the summary is repeated in `metadata.json` for every
//!   snapshot).

use std::collections::BTreeMap;

use crate::error::{Result, SqeError, SqeErrorCode};

/// Longest accepted key, in bytes.
pub const MAX_KEY_BYTES: usize = 128;
/// Longest accepted value, in bytes.
pub const MAX_VALUE_BYTES: usize = 1024;
/// Most key/value pairs in one set.
pub const MAX_PAIRS: usize = 32;
/// Most bytes of keys and values together in one set (8 KiB).
pub const MAX_TOTAL_BYTES: usize = 8 * 1024;

/// Summary keys that Iceberg computes for every snapshot and would overwrite.
///
/// Source: `COMPUTED_SUMMARY_PROPERTIES` in
/// `vendor/iceberg-rust/crates/iceberg/src/spec/snapshot_summary.rs`
/// (23 keys), plus `added-dvs` and `removed-dvs`, which that file declares as
/// summary-key constants but does not list in the array. Refusing them costs
/// nothing and keeps the set safe if the vendored list grows to cover them.
/// The `partitions.` prefix of that file is handled by [`RESERVED_PREFIXES`].
const COMPUTED_SUMMARY_KEYS: &[&str] = &[
    "added-data-files",
    "added-delete-files",
    "added-equality-deletes",
    "added-files-size",
    "added-position-deletes",
    "added-position-delete-files",
    "added-records",
    "deleted-data-files",
    "deleted-records",
    "added-equality-delete-files",
    "removed-delete-files",
    "removed-equality-deletes",
    "removed-equality-delete-files",
    "removed-files-size",
    "removed-position-deletes",
    "removed-position-delete-files",
    "total-equality-deletes",
    "total-position-deletes",
    "total-data-files",
    "total-delete-files",
    "total-records",
    "total-files-size",
    "changed-partition-count",
    "added-dvs",
    "removed-dvs",
];

/// Keys that other engines or the snapshot producer act on.
const RESERVED_KEYS: &[&str] = &[
    "operation",
    "wap.id",
    "published-wap-id",
    "source-snapshot-id",
];

/// Key prefixes that SQE or Iceberg own: `partitions.` is the per-partition
/// summary, `sqe.` holds `sqe.maintenance.*` (a compaction marker), and
/// `commit.` / `write.` are read by the snapshot producer as manifest-merge
/// configuration.
const RESERVED_PREFIXES: &[&str] = &["partitions.", "sqe.", "commit.", "write."];

/// A validated set of snapshot summary properties, ordered by key.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SnapshotProperties(BTreeMap<String, String>);

fn invalid(message: String) -> SqeError {
    SqeError::with_code(SqeErrorCode::InvalidArguments, message)
}

fn check_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(invalid(
            "snapshot property key is empty: a key must be 1 to 128 bytes".to_string(),
        ));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(invalid(format!(
            "snapshot property key '{}' is {} bytes: a key is at most {MAX_KEY_BYTES} bytes",
            truncate_for_message(key),
            key.len()
        )));
    }
    if let Some(bad) = key
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        return Err(invalid(format!(
            "snapshot property key '{}' holds the character {bad:?}: a key may only use A-Z, a-z, 0-9, '.', '_' and '-'",
            key.escape_debug()
        )));
    }
    let lower = key.to_ascii_lowercase();
    if COMPUTED_SUMMARY_KEYS.contains(&lower.as_str()) {
        return Err(invalid(format!(
            "snapshot property key '{key}' is reserved: Iceberg computes it for every snapshot"
        )));
    }
    if RESERVED_KEYS.contains(&lower.as_str()) {
        return Err(invalid(format!(
            "snapshot property key '{key}' is reserved: the engine or other readers act on it"
        )));
    }
    if let Some(prefix) = RESERVED_PREFIXES.iter().find(|p| lower.starts_with(**p)) {
        return Err(invalid(format!(
            "snapshot property key '{key}' is reserved: keys starting with '{prefix}' belong to the engine"
        )));
    }
    Ok(())
}

fn check_value(key: &str, value: &str) -> Result<()> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(invalid(format!(
            "snapshot property '{key}' has a {}-byte value: a value is at most {MAX_VALUE_BYTES} bytes",
            value.len()
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid(format!(
            "snapshot property '{key}' has a value with a control character: control characters are not allowed"
        )));
    }
    Ok(())
}

/// Check the whole-set caps. The offending key is the one that crossed the cap.
fn check_caps(map: &BTreeMap<String, String>) -> Result<()> {
    let mut total = 0usize;
    for (i, (k, v)) in map.iter().enumerate() {
        if i >= MAX_PAIRS {
            return Err(invalid(format!(
                "snapshot property '{k}' is pair {}: a set holds at most {MAX_PAIRS} pairs",
                i + 1
            )));
        }
        total += k.len() + v.len();
        if total > MAX_TOTAL_BYTES {
            return Err(invalid(format!(
                "snapshot property '{k}' brings the set to {total} bytes: keys and values together are at most {MAX_TOTAL_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

/// Keep an over-long key readable in an error message.
fn truncate_for_message(key: &str) -> String {
    const SHOWN: usize = 32;
    if key.len() <= SHOWN {
        key.to_string()
    } else {
        let mut end = SHOWN;
        while !key.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &key[..end])
    }
}

impl SnapshotProperties {
    /// Validate `pairs` and build the set.
    ///
    /// A key that appears twice is an error, so a typo cannot silently drop a
    /// value. Duplicates are matched exactly: `A` and `a` are different keys,
    /// as they are in an Iceberg summary.
    pub fn try_new(pairs: impl IntoIterator<Item = (String, String)>) -> Result<Self> {
        let mut map = BTreeMap::new();
        for (key, value) in pairs {
            check_key(&key)?;
            check_value(&key, &value)?;
            if map.contains_key(&key) {
                return Err(invalid(format!(
                    "snapshot property key '{key}' appears more than once"
                )));
            }
            map.insert(key, value);
            // Fail as soon as the pair cap is crossed, so a huge input is not
            // buffered before it is refused.
            if map.len() > MAX_PAIRS {
                break;
            }
        }
        check_caps(&map)?;
        Ok(Self(map))
    }

    /// This set with `overrides` laid over it: the override wins per key. The
    /// caps are checked again on the result, because the union can exceed
    /// them when neither input does.
    pub fn merged_with(&self, overrides: &SnapshotProperties) -> Result<Self> {
        let mut map = self.0.clone();
        for (k, v) in &overrides.0 {
            map.insert(k.clone(), v.clone());
        }
        check_caps(&map)?;
        Ok(Self(map))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }

    pub fn into_inner(self) -> BTreeMap<String, String> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(k: &str, v: &str) -> (String, String) {
        (k.to_string(), v.to_string())
    }

    fn one(k: &str, v: &str) -> Result<SnapshotProperties> {
        SnapshotProperties::try_new([kv(k, v)])
    }

    fn err_text(r: Result<SnapshotProperties>) -> String {
        let e = r.expect_err("expected a validation error");
        assert_eq!(e.error_code(), SqeErrorCode::InvalidArguments);
        e.client_message()
    }

    #[test]
    fn accepts_plain_pairs_and_orders_by_key() {
        let p = SnapshotProperties::try_new([
            kv("copy.run", "42"),
            kv("app_name", "loader"),
            kv("Z-1", ""),
        ])
        .unwrap();
        assert_eq!(p.len(), 3);
        assert!(!p.is_empty());
        let keys: Vec<&str> = p.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, ["Z-1", "app_name", "copy.run"]);
        assert_eq!(p.as_map().get("copy.run").map(String::as_str), Some("42"));
        assert_eq!(p.into_inner().len(), 3);
    }

    #[test]
    fn empty_set_is_valid() {
        let p = SnapshotProperties::try_new(Vec::new()).unwrap();
        assert!(p.is_empty());
        assert_eq!(p, SnapshotProperties::default());
    }

    #[test]
    fn key_length_edges() {
        assert!(one(&"k".repeat(128), "v").is_ok());
        let msg = err_text(one(&"k".repeat(129), "v"));
        assert!(msg.contains("129 bytes"), "{msg}");
        let msg = err_text(one("", "v"));
        assert!(msg.contains("empty"), "{msg}");
    }

    #[test]
    fn key_charset() {
        assert!(one("aZ09._-", "v").is_ok());
        for bad in ["a b", "a/b", "a:b", "a=b", "é", "a\n", "a'b", "日本"] {
            let msg = err_text(one(bad, "v"));
            assert!(msg.contains("may only use"), "{bad:?}: {msg}");
        }
    }

    #[test]
    fn computed_keys_refused_case_insensitively() {
        for key in COMPUTED_SUMMARY_KEYS {
            let msg = err_text(one(key, "0"));
            assert!(msg.contains(key) && msg.contains("computes"), "{msg}");
            let upper = key.to_ascii_uppercase();
            let msg = err_text(one(&upper, "0"));
            assert!(msg.contains(&upper), "{msg}");
        }
        assert!(COMPUTED_SUMMARY_KEYS.contains(&"added-records"));
        assert!(COMPUTED_SUMMARY_KEYS.contains(&"total-records"));
        assert!(COMPUTED_SUMMARY_KEYS.contains(&"changed-partition-count"));
    }

    #[test]
    fn reserved_keys_refused_case_insensitively() {
        for key in [
            "operation",
            "OPERATION",
            "Operation",
            "wap.id",
            "WAP.ID",
            "published-wap-id",
            "Published-Wap-Id",
            "source-snapshot-id",
            "SOURCE-SNAPSHOT-ID",
        ] {
            let msg = err_text(one(key, "x"));
            assert!(msg.contains(key) && msg.contains("reserved"), "{msg}");
        }
    }

    #[test]
    fn reserved_prefixes_refused_case_insensitively() {
        for key in [
            "partitions.dt",
            "Partitions.x",
            "sqe.maintenance.kind",
            "Sqe.x",
            "SQE.X",
            "commit.manifest.min-count-to-merge",
            "COMMIT.x",
            "write.target-file-size-bytes",
            "Write.x",
        ] {
            let msg = err_text(one(key, "x"));
            assert!(msg.contains(key) && msg.contains("reserved"), "{msg}");
        }
        // Near misses are fine: the prefix must match, dot included.
        for key in [
            "sqe",
            "sqex.a",
            "partitions",
            "commit",
            "writer.id",
            "my.write.x",
        ] {
            assert!(one(key, "x").is_ok(), "{key} should be accepted");
        }
    }

    #[test]
    fn value_length_edges() {
        assert!(one("k", &"v".repeat(1024)).is_ok());
        let msg = err_text(one("k", &"v".repeat(1025)));
        assert!(msg.contains("'k'") && msg.contains("1025"), "{msg}");
        // Bytes, not chars: 513 two-byte chars is 1026 bytes.
        assert!(one("k", &"é".repeat(512)).is_ok());
        assert!(one("k", &"é".repeat(513)).is_err());
    }

    #[test]
    fn value_control_characters_refused() {
        for bad in [
            "a\nb", "a\tb", "a\rb", "\u{0}", "\u{1}", "\u{1f}", "\u{7f}", "\u{85}",
        ] {
            let msg = err_text(one("k", bad));
            assert!(msg.contains("control character"), "{bad:?}: {msg}");
        }
        assert!(one("k", "plain text é 日本 🦀 \\ \"q\"").is_ok());
    }

    #[test]
    fn pair_count_edges() {
        let make = |n: usize| {
            (0..n)
                .map(|i| kv(&format!("k{i:02}"), "v"))
                .collect::<Vec<_>>()
        };
        assert_eq!(SnapshotProperties::try_new(make(32)).unwrap().len(), 32);
        let msg = err_text(SnapshotProperties::try_new(make(33)));
        assert!(msg.contains("at most 32 pairs"), "{msg}");
    }

    #[test]
    fn total_bytes_edges() {
        // 8 pairs of exactly 1024 key+value bytes each: 8192 in total.
        let make = |last_value_len: usize| {
            let mut pairs: Vec<(String, String)> = (0..7)
                .map(|i| (format!("k{i}"), "v".repeat(1024 - 2)))
                .collect();
            pairs.push(("k7".to_string(), "v".repeat(last_value_len)));
            pairs
        };
        // 7 * 1024 + 2 + 1022 = 8192
        assert!(SnapshotProperties::try_new(make(1022)).is_ok());
        let msg = err_text(SnapshotProperties::try_new(make(1023)));
        assert!(msg.contains("8192") || msg.contains("8193"), "{msg}");
        assert!(msg.contains("k7"), "{msg}");
    }

    #[test]
    fn duplicate_key_is_an_error() {
        let msg = err_text(SnapshotProperties::try_new([kv("a", "1"), kv("a", "2")]));
        assert!(
            msg.contains("'a'") && msg.contains("more than once"),
            "{msg}"
        );
        // Different case is a different key.
        assert!(SnapshotProperties::try_new([kv("a", "1"), kv("A", "2")]).is_ok());
    }

    #[test]
    fn error_names_the_offending_key_not_a_good_one() {
        let msg = err_text(SnapshotProperties::try_new([
            kv("good", "1"),
            kv("sqe.maintenance.kind", "x"),
        ]));
        assert!(msg.contains("sqe.maintenance.kind"), "{msg}");
        assert!(!msg.contains("good"), "{msg}");
    }

    #[test]
    fn merge_override_wins_per_key() {
        let base = SnapshotProperties::try_new([kv("a", "1"), kv("b", "2")]).unwrap();
        let over = SnapshotProperties::try_new([kv("b", "9"), kv("c", "3")]).unwrap();
        let merged = base.merged_with(&over).unwrap();
        let got: Vec<(&str, &str)> = merged.iter().collect();
        assert_eq!(got, [("a", "1"), ("b", "9"), ("c", "3")]);
        // Inputs are untouched.
        assert_eq!(base.len(), 2);
        assert_eq!(over.len(), 2);
    }

    #[test]
    fn merge_with_empty_is_identity() {
        let base = SnapshotProperties::try_new([kv("a", "1")]).unwrap();
        let empty = SnapshotProperties::default();
        assert_eq!(base.merged_with(&empty).unwrap(), base);
        assert_eq!(empty.merged_with(&base).unwrap(), base);
    }

    #[test]
    fn merge_rechecks_pair_cap() {
        let a = SnapshotProperties::try_new((0..20).map(|i| kv(&format!("a{i:02}"), "v"))).unwrap();
        let b = SnapshotProperties::try_new((0..20).map(|i| kv(&format!("b{i:02}"), "v"))).unwrap();
        let msg = err_text(a.merged_with(&b));
        assert!(msg.contains("at most 32 pairs"), "{msg}");
        // Overlapping keys do not grow the set.
        assert_eq!(a.merged_with(&a).unwrap().len(), 20);
    }

    #[test]
    fn merge_rechecks_byte_cap() {
        let big = |prefix: &str| {
            SnapshotProperties::try_new(
                (0..4).map(|i| kv(&format!("{prefix}{i}"), &"v".repeat(1000))),
            )
            .unwrap()
        };
        // 4 * (2 + 1000) = 4008 bytes each; 8016 together fits, three would not.
        let a = big("a");
        let b = big("b");
        let ab = a.merged_with(&b).unwrap();
        let c = big("c");
        let msg = err_text(ab.merged_with(&c));
        assert!(msg.contains("bytes"), "{msg}");
    }
}
