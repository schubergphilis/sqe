//! `TagSource` implementation backed by the `TableMetadataCache`.
//!
//! The `sqe-policy` crate cannot depend on `sqe-catalog` (dependency
//! inversion), so the Iceberg-backed `TagSource` lives here and is injected
//! as `Arc<dyn TagSource>` into the policy pipeline.
//!
//! Tags are stored as the Iceberg table property `sqe.column-tags`, a JSON
//! object mapping column names to tag lists:
//!
//! ```json
//! {"email": ["PII", "GDPR"], "salary": ["PII", "CONFIDENTIAL"]}
//! ```
//!
//! # Sync cache access
//!
//! `TagSource::column_tags` is synchronous. `TableMetadataCache` is backed by
//! `moka::future::Cache`, whose `iter()` method is synchronous and returns a
//! snapshot of the current entries without blocking on async I/O. We use this
//! via `TableMetadataCache::properties_for` (added in sqe-catalog) so no
//! `block_on` is needed and the rewriter can stay sync.
//!
//! # Cache key note
//!
//! Cache keys are `{token_fingerprint}|{namespace}.{table}`. The
//! `properties_for` accessor scans for any entry whose key ENDS WITH
//! `|{namespace}.{table}`, because table properties are user-independent
//! (user-scoping exists only to prevent S3 vended credentials baked into the
//! `Table` from crossing sessions — issue #49). The first matching entry is
//! used; all matching entries hold the same properties.
//!
//! Note: the existing key format omits warehouse/catalog, so tables with the
//! same namespace + name in different warehouses would collide. This mirrors
//! the existing `table_cache_key` behavior in `SessionCatalog`.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::NamespaceIdent;
use tracing::debug;

use sqe_catalog::TableMetadataCache;
use sqe_policy::TagSource;
use sqe_sql::tags::{ColumnTagOp, TagAction};

/// Iceberg table property key holding the column->tag-label JSON map.
pub(crate) const PROP_KEY: &str = "sqe.column-tags";

/// Tag source backed by the coordinator's shared `TableMetadataCache`.
///
/// During query planning the scan path has already loaded the target table into
/// the cache via `SessionCatalog::load_table`. `CacheTagSource::column_tags`
/// then reads the table properties out of the already-cached entry
/// synchronously, with zero extra network calls. On a cache miss (table not
/// yet loaded, or cache disabled via `ttl_secs = 0`) it returns an empty map,
/// which is fail-safe: no tags means no extra masking.
pub struct CacheTagSource {
    cache: Arc<TableMetadataCache>,
}

impl CacheTagSource {
    /// Create a new `CacheTagSource` wrapping the shared global cache.
    pub fn new(cache: Arc<TableMetadataCache>) -> Self {
        Self { cache }
    }
}

impl TagSource for CacheTagSource {
    fn column_tags(
        &self,
        _catalog: Option<&str>,
        namespace: &[String],
        table: &str,
    ) -> HashMap<String, Vec<String>> {
        // Build the namespace display string the same way `table_cache_key` does:
        // `NamespaceIdent::Display` joins parts with `.`.
        let ns_display = match NamespaceIdent::from_vec(namespace.to_vec()) {
            Ok(ns) => ns.to_string(),
            Err(e) => {
                debug!(
                    error = %e,
                    namespace = ?namespace,
                    table = %table,
                    "tag_source: invalid namespace, returning empty tags"
                );
                return HashMap::new();
            }
        };

        let props = match self.cache.properties_for(&ns_display, table) {
            Some(p) => p,
            None => {
                debug!(
                    namespace = %ns_display,
                    table = %table,
                    "tag_source: table not in cache, returning empty tags"
                );
                return HashMap::new();
            }
        };

        parse_column_tags(&props)
    }
}

/// Parse the `sqe.column-tags` property from a table's property map.
///
/// Extracted as a pure function so unit tests can exercise the parsing logic
/// without a live cache or iceberg `Table` object.
///
/// Returns an empty map on any failure (absent key, malformed JSON, wrong
/// JSON shape) — fail-safe: no tags means no extra masking.
pub(crate) fn parse_column_tags(
    props: &HashMap<String, String>,
) -> HashMap<String, Vec<String>> {
    let raw = match props.get(PROP_KEY) {
        Some(v) => v,
        None => return HashMap::new(),
    };

    match serde_json::from_str::<HashMap<String, Vec<String>>>(raw) {
        Ok(map) => map,
        Err(e) => {
            debug!(
                error = %e,
                raw = %raw,
                "tag_source: malformed sqe.column-tags JSON, returning empty tags"
            );
            HashMap::new()
        }
    }
}

/// Apply column-tag operations to the current tag map (merge semantics).
/// `Set` unions tags (dedup, stable order). `Unset` removes the listed tags, or
/// ALL tags on the column when the list is empty. A column whose tag list becomes
/// empty is dropped from the map.
pub(crate) fn apply_tag_ops(
    current: &HashMap<String, Vec<String>>,
    ops: &[ColumnTagOp],
) -> HashMap<String, Vec<String>> {
    let mut map = current.clone();
    for op in ops {
        match op.action {
            TagAction::Set => {
                let entry = map.entry(op.column.clone()).or_default();
                for t in &op.tags {
                    if !entry.contains(t) {
                        entry.push(t.clone());
                    }
                }
                if entry.is_empty() {
                    map.remove(&op.column);
                }
            }
            TagAction::Unset => {
                if op.tags.is_empty() {
                    map.remove(&op.column);
                } else if let Some(entry) = map.get_mut(&op.column) {
                    entry.retain(|t| !op.tags.contains(t));
                    if entry.is_empty() {
                        map.remove(&op.column);
                    }
                }
            }
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_sql::tags::{ColumnTagOp, TagAction};

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn map(pairs: &[(&str, &[&str])]) -> std::collections::HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(c, ts)| (c.to_string(), ts.iter().map(|t| t.to_string()).collect()))
            .collect()
    }

    #[test]
    fn apply_tag_ops_set_merges_and_dedups() {
        let cur = map(&[("email", &["PII"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec!["PII".into(), "GDPR".into()],
            action: TagAction::Set,
        }];
        let got = apply_tag_ops(&cur, &ops);
        assert_eq!(got.get("email").unwrap(), &vec!["PII".to_string(), "GDPR".to_string()]);
    }

    #[test]
    fn apply_tag_ops_set_leaves_other_columns_untouched() {
        let cur = map(&[("email", &["PII"]), ("region", &["GEO"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec!["GDPR".into()],
            action: TagAction::Set,
        }];
        let got = apply_tag_ops(&cur, &ops);
        assert_eq!(got.get("region").unwrap(), &vec!["GEO".to_string()]);
    }

    #[test]
    fn apply_tag_ops_unset_named_removes_one() {
        let cur = map(&[("email", &["PII", "GDPR"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec!["GDPR".into()],
            action: TagAction::Unset,
        }];
        let got = apply_tag_ops(&cur, &ops);
        assert_eq!(got.get("email").unwrap(), &vec!["PII".to_string()]);
    }

    #[test]
    fn apply_tag_ops_unset_all_drops_column() {
        let cur = map(&[("email", &["PII", "GDPR"]), ("region", &["GEO"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec![],
            action: TagAction::Unset,
        }];
        let got = apply_tag_ops(&cur, &ops);
        assert!(!got.contains_key("email"));
        assert!(got.contains_key("region"));
    }

    #[test]
    fn apply_tag_ops_unset_last_tag_drops_column() {
        let cur = map(&[("email", &["PII"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec!["PII".into()],
            action: TagAction::Unset,
        }];
        let got = apply_tag_ops(&cur, &ops);
        assert!(!got.contains_key("email"));
    }

    #[test]
    fn valid_json_returns_correct_map() {
        let p = props(&[(
            "sqe.column-tags",
            r#"{"email":["PII","GDPR"],"salary":["PII","CONFIDENTIAL"]}"#,
        )]);
        let got = parse_column_tags(&p);
        assert_eq!(got.get("email").unwrap(), &vec!["PII", "GDPR"]);
        assert_eq!(
            got.get("salary").unwrap(),
            &vec!["PII", "CONFIDENTIAL"]
        );
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn absent_property_returns_empty() {
        let p = props(&[("other.prop", "value")]);
        assert!(parse_column_tags(&p).is_empty());
    }

    #[test]
    fn empty_properties_returns_empty() {
        assert!(parse_column_tags(&HashMap::new()).is_empty());
    }

    #[test]
    fn malformed_json_returns_empty_fail_safe() {
        let p = props(&[("sqe.column-tags", "not-valid-json{{")]);
        assert!(parse_column_tags(&p).is_empty());
    }

    #[test]
    fn wrong_json_shape_returns_empty() {
        // JSON is valid but not HashMap<String, Vec<String>>
        let p = props(&[("sqe.column-tags", r#"{"email": "not-an-array"}"#)]);
        assert!(parse_column_tags(&p).is_empty());
    }

    #[test]
    fn empty_tags_array_is_valid() {
        let p = props(&[("sqe.column-tags", r#"{"col": []}"#)]);
        let got = parse_column_tags(&p);
        assert_eq!(got.get("col").unwrap(), &Vec::<String>::new());
    }

    #[test]
    fn empty_json_object_returns_empty_map() {
        let p = props(&[("sqe.column-tags", "{}")]);
        let got = parse_column_tags(&p);
        assert!(got.is_empty());
    }
}
