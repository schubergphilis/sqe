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
//! A tag with a value is a one-entry object in the list
//! (`{"card": [{"pci": "card"}, "PII"]}`; see `sqe_core::column_tags`). The
//! `TagSource` and the Ranger projection see the keys only.
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
//! Cache keys are `{token_fingerprint}|{warehouse}|{namespace}.{table}`. The
//! `properties_for` accessor scans for any entry whose key ENDS WITH
//! `|{catalog}|{namespace}.{table}`, because table properties are user-independent
//! (user-scoping exists only to prevent S3 vended credentials baked into the
//! `Table` from crossing sessions — issue #49). The first matching entry is
//! used; all matching entries hold the same properties.
//!

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::NamespaceIdent;
use tracing::debug;

use sqe_catalog::TableMetadataCache;
use sqe_core::column_tags::{self, ColumnTag, ColumnTagMap};
use sqe_policy::TagSource;
use sqe_sql::tags::{ColumnTagOp, TagAction};

/// Iceberg table property key holding the column->tag-label JSON map.
pub const PROP_KEY: &str = column_tags::PROP_KEY;

/// Tag source backed by the coordinator's shared `TableMetadataCache`.
///
/// The rewriter reads an Iceberg scan's tags off its provider first
/// ([`scan_column_tags`]); this is the fallback for scans that hook does not
/// recognise, and the audit writer's lookup.
///
/// During query planning the scan path has already loaded the target table into
/// the cache via `SessionCatalog::load_table`. `CacheTagSource::column_tags`
/// then reads the table properties out of the already-cached entry
/// synchronously, with zero extra network calls. On a cache miss (table not
/// yet loaded, or cache disabled via `ttl_secs = 0`) it returns `None`
/// (tag state UNKNOWN), which the rewriter treats as fail-CLOSED: an unseen
/// tag mask or tag row-filter must not be silently skipped. A cache HIT
/// returns `Some(map)` even when the parsed map is empty (known: no tags).
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
        catalog: Option<&str>,
        namespace: &[String],
        table: &str,
    ) -> Option<HashMap<String, Vec<String>>> {
        // Build the namespace display string the same way `table_cache_key` does:
        // `NamespaceIdent::Display` joins parts with `.`.
        let ns_display = match NamespaceIdent::from_vec(namespace.to_vec()) {
            Ok(ns) => ns.to_string(),
            Err(e) => {
                debug!(
                    error = %e,
                    namespace = ?namespace,
                    table = %table,
                    "tag_source: invalid namespace, tag state UNKNOWN (fail closed)"
                );
                // Cannot build the cache key => cannot know the tag state.
                return None;
            }
        };

        let metadata = match self.cache.metadata_for(catalog, &ns_display, table) {
            Some(m) => m,
            None => {
                debug!(
                    namespace = %ns_display,
                    table = %table,
                    "tag_source: table not in cache (miss or cache disabled), \
                     tag state UNKNOWN (fail closed)"
                );
                // Cache miss / disabled cache: the tag state is unknown, NOT
                // known-empty. Return None so the rewriter fails closed.
                return None;
            }
        };

        // Cache hit: absent `sqe.column-tags` is known-empty (untagged).
        // Malformed or wrong-shape JSON is UNKNOWN, same as a cache miss:
        // return None so the rewriter fail-closes instead of unmasking.
        // Entries recorded with a field id resolve against the current schema
        // (#920), so a renamed column keeps its tags.
        match resolved_column_tags(metadata.properties(), metadata.current_schema()) {
            Ok(map) => Some(map),
            Err(e) => {
                debug!(
                    error = %e,
                    "tag_source: malformed sqe.column-tags JSON, tag state UNKNOWN (fail closed)"
                );
                None
            }
        }
    }
}

/// Read the column tags off an Iceberg scan's provider, for
/// `PolicyPlanRewriter::with_scan_tags`.
///
/// Planning built the `SqeTableProvider` from `load_table` with the caller's
/// token, revalidating a stale cache entry first, so its properties are the
/// table's current ones. Reading them here does not depend on the metadata
/// cache still holding the table: a disabled cache (`ttl_secs = 0`), an
/// eviction, or a commit racing the planning load no longer makes the tag
/// state unknown (#842). A `FOR INCREMENTAL BETWEEN SNAPSHOT` scan's
/// `IncrementalTableProvider` carries the properties of the same kind of load
/// (#844); one built without them is `NotRecognised`, never untagged. A
/// metadata TVF's output carries no column tags (the target's are applied
/// inside the provider), so it is known-untagged (#846), and so is
/// DataFusion's `generate_series` / `range`, which computes its rows and
/// reads no table (#847). The information_schema tag views
/// (`sqe_information_schema_tags`) list tag names and values, not tagged
/// column data, so they are known-untagged too. Any other provider (Hive, views) is
/// `NotRecognised` and goes to `CacheTagSource` / `HiveTagSource`.
pub fn scan_column_tags(
    provider: &dyn datafusion::catalog::TableProvider,
) -> sqe_policy::plan_rewriter::ScanTagState {
    use sqe_policy::plan_rewriter::ScanTagState;

    if sqe_catalog::iceberg_metadata_tvf::is_metadata_table_provider(provider)
        || is_series_generator(provider)
        || sqe_catalog::tag_views::is_tag_view_provider(provider)
    {
        return ScanTagState::Known(HashMap::new());
    }
    let any = provider as &dyn std::any::Any;
    // Tags recorded with a field id resolve against the schema the scan
    // reads (the snapshot's under time travel), so a rename applies them
    // under the column's name in that schema (#920).
    let (properties, schema) = if let Some(iceberg) =
        any.downcast_ref::<sqe_catalog::table_provider::SqeTableProvider>()
    {
        (
            iceberg.table().metadata().properties(),
            iceberg.scan_schema(),
        )
    } else if let Some(incremental) =
        any.downcast_ref::<sqe_catalog::incremental_provider::IncrementalTableProvider>()
    {
        // A `FOR INCREMENTAL BETWEEN SNAPSHOT` scan carries the properties
        // and schema its planning load read (#844).
        match (incremental.table_properties(), incremental.table_schema()) {
            (Some(properties), Some(schema)) => (properties, schema.clone()),
            _ => return ScanTagState::NotRecognised,
        }
    } else {
        return ScanTagState::NotRecognised;
    };
    match resolved_column_tags(properties, &schema) {
        Ok(map) => ScanTagState::Known(map),
        Err(e) => {
            debug!(
                error = %e,
                "tag_source: malformed sqe.column-tags on the scanned table, tag state UNKNOWN (fail closed)"
            );
            ScanTagState::Unknown
        }
    }
}

/// DataFusion's built-in `generate_series` and `range` table functions both
/// return a `GenerateSeriesTable`. Matched by type so that any other
/// unrecognised provider still fails closed.
fn is_series_generator(provider: &dyn datafusion::catalog::TableProvider) -> bool {
    (provider as &dyn std::any::Any)
        .downcast_ref::<datafusion::functions_table::generate_series::GenerateSeriesTable>()
        .is_some()
}

/// Parse the `sqe.column-tags` property from a table's property map.
///
/// Extracted as a pure function so unit tests can exercise the parsing logic
/// without a live cache or iceberg `Table` object.
///
/// * Absent key → `Ok` empty map (known: table is untagged).
/// * Valid JSON object mapping column → tag list → `Ok` of that map.
/// * Malformed JSON or wrong shape → `Err` (unknown). Callers must not treat
///   this as empty: the rewriter fail-closes, and SET/UNSET TAGS must refuse
///   rather than wipe existing tags.
///
/// Tag values are dropped: policy matches on keys only. Writers that
/// read-modify-write the property use [`parse_column_tag_entries`] so the
/// values survive.
pub fn parse_column_tags(
    props: &HashMap<String, String>,
) -> Result<HashMap<String, Vec<String>>, serde_json::Error> {
    parse_column_tag_entries(props).map(|map| column_tags::keys(&map))
}

/// The tag keys that apply to `schema`'s columns, under their names there.
///
/// Like [`parse_column_tags`], but entries recorded with a field id in
/// `sqe.column-tag-ids` follow the column through a rename, and an entry
/// whose column was dropped is not applied (#920). Malformed tags or ids are
/// `Err`: the tag state is unknown and the caller fails closed.
pub fn resolved_column_tags(
    props: &HashMap<String, String>,
    schema: &iceberg::spec::Schema,
) -> Result<HashMap<String, Vec<String>>, serde_json::Error> {
    applied_column_tag_entries(props, schema).map(|applied| column_tags::keys(&applied))
}

/// [`resolved_column_tags`] keeping each tag's value: the entries that apply
/// to `schema`'s columns, under their current names (#920).
pub fn applied_column_tag_entries(
    props: &HashMap<String, String>,
    schema: &iceberg::spec::Schema,
) -> Result<ColumnTagMap, serde_json::Error> {
    sqe_catalog::column_tag_ids::resolve_table_tags(props, schema).map(|r| r.applied)
}

/// [`resolved_column_tags`] against `table`'s current schema.
pub fn table_column_tags(
    table: &iceberg::table::Table,
) -> Result<HashMap<String, Vec<String>>, serde_json::Error> {
    let metadata = table.metadata();
    resolved_column_tags(metadata.properties(), metadata.current_schema())
}

/// [`parse_column_tags`] keeping each tag's value (`["pii", {"kind": "card"}]`).
pub fn parse_column_tag_entries(
    props: &HashMap<String, String>,
) -> Result<ColumnTagMap, serde_json::Error> {
    match props.get(PROP_KEY) {
        Some(raw) => column_tags::parse(raw),
        None => Ok(ColumnTagMap::new()),
    }
}

/// What a raw property write (`ALTER TABLE ... SET TBLPROPERTIES / SET
/// PROPERTIES`, `CREATE [OR REPLACE] TABLE ... TBLPROPERTIES / WITH`) does to
/// `sqe.column-tags`, read from the options after the Trino vocabulary is
/// resolved, so `extra_properties = MAP(ARRAY['sqe.column-tags'], ...)` counts
/// like the key written out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagPropertyWrite {
    /// The value set under the exact key, the last one when given twice (as
    /// the write keeps it). `None` when only an off-case spelling is given.
    pub value: Option<String>,
    /// A key equal to `sqe.column-tags` only ignoring case. Iceberg keys are
    /// case-sensitive, so the tag source never reads it, but it is policy
    /// shaped and stays an admin's to write.
    pub off_case_key: bool,
}

/// The `sqe.column-tags` write `stmt` makes, `None` when it names no such key.
/// An error is the one the write itself would raise on the same options.
pub fn tag_property_write(
    stmt: &sqlparser::ast::Statement,
) -> sqe_core::Result<Option<TagPropertyWrite>> {
    use sqlparser::ast::{Expr, SqlOption, Value};
    let resolved = written_table_options(stmt)?;
    let mut write = TagPropertyWrite {
        value: None,
        off_case_key: false,
    };
    let mut named = false;
    for opt in resolved.iter().flat_map(|t| t.options()) {
        let SqlOption::KeyValue { key, value } = opt else {
            continue;
        };
        if key.value == PROP_KEY {
            named = true;
            write.value = Some(match value {
                Expr::Value(v) => match &v.value {
                    Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
                    _ => value.to_string(),
                },
                other => other.to_string(),
            });
        } else if key.value.eq_ignore_ascii_case(PROP_KEY) {
            named = true;
            write.off_case_key = true;
        }
    }
    Ok(named.then_some(write))
}

/// True when `stmt` writes the table property `key` in any case, through
/// any spelling (`extra_properties` included).
pub fn writes_table_property(
    stmt: &sqlparser::ast::Statement,
    key: &str,
) -> sqe_core::Result<bool> {
    use sqlparser::ast::SqlOption;
    Ok(written_table_options(stmt)?
        .iter()
        .flat_map(|t| t.options())
        .any(|opt| matches!(opt, SqlOption::KeyValue { key: k, .. } if k.value.eq_ignore_ascii_case(key))))
}

/// The table options a raw property write sets, after the Trino vocabulary
/// is resolved. Empty for a statement that sets none.
fn written_table_options(
    stmt: &sqlparser::ast::Statement,
) -> sqe_core::Result<Vec<crate::write_handler::trino_props::TrinoTableOptions>> {
    use crate::write_handler::trino_props::TrinoTableOptions;
    use sqlparser::ast::{AlterTableOperation, Statement};
    Ok(match stmt {
        Statement::AlterTable(at) => at
            .operations
            .iter()
            .filter_map(|op| match op {
                AlterTableOperation::SetTblProperties { table_properties } => {
                    Some(TrinoTableOptions::parse_alter(table_properties))
                }
                _ => None,
            })
            .collect::<sqe_core::Result<_>>()?,
        Statement::CreateTable(ct) => vec![TrinoTableOptions::parse(
            &ct.table_options,
            ct.partition_by.is_some(),
        )?],
        _ => Vec::new(),
    })
}

/// The columns whose tags differ between two `sqe.column-tags` maps: added,
/// changed and removed ones, never unchanged ones. This is the set a raw write
/// of the property (`SET TBLPROPERTIES`, `CREATE OR REPLACE ... TBLPROPERTIES`)
/// has to be authorised for, the same per-column check `SET TAGS` gets.
///
/// Tag lists compare as sets (order and duplicates do not matter), and an empty
/// list is the same as no entry. A tag whose value changes is a change. Column
/// names compare exactly, so a key whose case changes counts as two columns:
/// one removed, one added.
pub fn changed_tag_columns(
    old: &ColumnTagMap,
    new: &ColumnTagMap,
) -> std::collections::BTreeSet<String> {
    use std::collections::BTreeSet;
    let tags = |m: &ColumnTagMap, c: &str| -> BTreeSet<ColumnTag> {
        m.get(c)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    };
    old.keys()
        .chain(new.keys())
        .filter(|c| tags(old, c) != tags(new, c))
        .cloned()
        .collect()
}

/// Apply column-tag operations to the current tag map (merge semantics).
/// `Set` unions tags (dedup by key, stable order) and sets each named tag's
/// value to the one the statement gave, or to none for a key-only tag, as
/// Databricks does. `Unset` removes the listed tags whatever their value, or
/// ALL tags on the column when the list is empty. A column whose tag list
/// becomes empty is dropped from the map.
pub(crate) fn apply_tag_ops(current: &ColumnTagMap, ops: &[ColumnTagOp]) -> ColumnTagMap {
    let mut map = current.clone();
    for op in ops {
        match op.action {
            TagAction::Set => {
                let entry = map.entry(op.column.clone()).or_default();
                for t in &op.tags {
                    let value = op.values.get(t).cloned();
                    match entry.iter_mut().find(|e| &e.key == t) {
                        Some(existing) => existing.value = value,
                        None => entry.push(ColumnTag {
                            key: t.clone(),
                            value,
                        }),
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
                    entry.retain(|t| !op.tags.contains(&t.key));
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
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn map(pairs: &[(&str, &[&str])]) -> std::collections::HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(c, ts)| (c.to_string(), ts.iter().map(|t| t.to_string()).collect()))
            .collect()
    }

    // The key-only tests below predate tag values; these wrappers run them
    // through the real functions on key-only maps. The value tests call
    // `super::` directly.
    fn apply_tag_ops(
        current: &HashMap<String, Vec<String>>,
        ops: &[ColumnTagOp],
    ) -> HashMap<String, Vec<String>> {
        column_tags::keys(&super::apply_tag_ops(&column_tags::from_keys(current), ops))
    }

    fn changed_tag_columns(
        old: &HashMap<String, Vec<String>>,
        new: &HashMap<String, Vec<String>>,
    ) -> std::collections::BTreeSet<String> {
        super::changed_tag_columns(&column_tags::from_keys(old), &column_tags::from_keys(new))
    }

    fn set_op(column: &str, tags: &[(&str, Option<&str>)]) -> ColumnTagOp {
        ColumnTagOp {
            column: column.into(),
            tags: tags.iter().map(|(k, _)| k.to_string()).collect(),
            values: tags
                .iter()
                .filter_map(|(k, v)| v.map(|v| (k.to_string(), v.to_string())))
                .collect(),
            action: TagAction::Set,
        }
    }

    #[test]
    fn set_stores_replaces_and_clears_a_value() {
        let set = super::apply_tag_ops(
            &ColumnTagMap::new(),
            &[set_op("card", &[("pci", Some("card")), ("pii", None)])],
        );
        assert_eq!(
            set["card"],
            vec![ColumnTag::with_value("pci", "card"), ColumnTag::key("pii")]
        );
        // Setting a key again replaces its value in place, no duplicate.
        let replaced = super::apply_tag_ops(&set, &[set_op("card", &[("pci", Some("pan"))])]);
        assert_eq!(
            replaced["card"],
            vec![ColumnTag::with_value("pci", "pan"), ColumnTag::key("pii")]
        );
        // Setting it key-only drops the value, as Databricks does.
        let cleared = super::apply_tag_ops(&replaced, &[set_op("card", &[("pci", None)])]);
        assert_eq!(
            cleared["card"],
            vec![ColumnTag::key("pci"), ColumnTag::key("pii")]
        );
    }

    #[test]
    fn unset_removes_a_valued_tag_by_key() {
        let cur = super::apply_tag_ops(
            &ColumnTagMap::new(),
            &[set_op("card", &[("pci", Some("card")), ("pii", None)])],
        );
        let ops = [ColumnTagOp {
            column: "card".into(),
            tags: vec!["pci".into()],
            values: Default::default(),
            action: TagAction::Unset,
        }];
        assert_eq!(
            super::apply_tag_ops(&cur, &ops)["card"],
            vec![ColumnTag::key("pii")]
        );
    }

    #[test]
    fn a_value_only_change_is_a_changed_column() {
        let old = super::apply_tag_ops(
            &ColumnTagMap::new(),
            &[set_op("card", &[("pci", Some("card"))])],
        );
        let new = super::apply_tag_ops(&old, &[set_op("card", &[("pci", Some("pan"))])]);
        assert_eq!(
            super::changed_tag_columns(&old, &new),
            ["card".to_string()].into_iter().collect()
        );
        assert!(super::changed_tag_columns(&old, &old).is_empty());
    }

    #[test]
    fn a_valued_tag_gives_policy_the_same_keys() {
        let p = props(&[(PROP_KEY, r#"{"card":[{"pci":"card"},"pii"]}"#)]);
        assert_eq!(
            parse_column_tags(&p).expect("valued tags parse")["card"],
            vec!["pci".to_string(), "pii".to_string()]
        );
        // Bad shapes stay unknown, never empty.
        for bad in [r#"{"card":[{}]}"#, r#"{"card":[{"a":"1","b":"2"}]}"#] {
            assert!(
                parse_column_tags(&props(&[(PROP_KEY, bad)])).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn written_property_keeps_values_and_key_only_json_is_unchanged() {
        let key_only = column_tags::from_keys(&map(&[("email", &["PII", "GDPR"])]));
        assert_eq!(
            column_tags::to_json(&key_only).unwrap(),
            r#"{"email":["PII","GDPR"]}"#
        );
        let valued = super::apply_tag_ops(&key_only, &[set_op("email", &[("PII", Some("high"))])]);
        let json = column_tags::to_json(&valued).unwrap();
        assert_eq!(json, r#"{"email":[{"PII":"high"},"GDPR"]}"#);
        assert_eq!(
            parse_column_tag_entries(&props(&[(PROP_KEY, &json)])).unwrap(),
            valued
        );
    }

    #[test]
    fn tag_property_write_reads_every_spelling() {
        let w = |sql: &str| {
            let stmt =
                sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, sql)
                    .expect("parse")
                    .remove(0);
            tag_property_write(&stmt).expect("options")
        };
        let set = |v: &str| {
            Some(TagPropertyWrite {
                value: Some(v.to_string()),
                off_case_key: false,
            })
        };
        assert_eq!(
            w(r#"ALTER TABLE t SET TBLPROPERTIES ('sqe.column-tags' = '{"a":["PII"]}')"#),
            set(r#"{"a":["PII"]}"#)
        );
        // Twice: the last value is the one written.
        assert_eq!(
            w("ALTER TABLE t SET TBLPROPERTIES ('sqe.column-tags' = 'x', 'sqe.column-tags' = '{}')"),
            set("{}")
        );
        // Trino's extra_properties puts the key in too.
        assert_eq!(
            w("ALTER TABLE t SET TBLPROPERTIES (extra_properties = MAP(ARRAY['sqe.column-tags'], ARRAY['{}']))"),
            set("{}")
        );
        assert_eq!(
            w("CREATE TABLE t (a INT) WITH (extra_properties = MAP(ARRAY['sqe.column-tags'], ARRAY['{}']))"),
            set("{}")
        );
        assert_eq!(
            w("CREATE OR REPLACE TABLE t (a INT) TBLPROPERTIES ('sqe.column-tags' = '{}')"),
            set("{}")
        );
        assert_eq!(
            w("CREATE TABLE t TBLPROPERTIES ('sqe.column-tags' = '{}') AS SELECT 1 AS a"),
            set("{}")
        );
        assert_eq!(
            w("ALTER TABLE t SET TBLPROPERTIES ('SQE.Column-Tags' = '{}')"),
            Some(TagPropertyWrite {
                value: None,
                off_case_key: true
            })
        );
        assert_eq!(
            w("ALTER TABLE t SET TBLPROPERTIES ('sqe.maintenance.x' = '1')"),
            None
        );
        assert_eq!(w("CREATE TABLE t (a INT)"), None);
        assert_eq!(w("SELECT 1"), None);
    }

    #[test]
    fn changed_tag_columns_is_added_changed_and_removed() {
        let set = |cs: &[&str]| -> std::collections::BTreeSet<String> {
            cs.iter().map(|c| c.to_string()).collect()
        };
        let old = map(&[
            ("ssn", &["PII"]),
            ("email", &["PII", "GDPR"]),
            ("phone", &["PII"]),
            ("note", &[]),
        ]);
        let new = map(&[
            ("ssn", &["PII"]),                  // unchanged
            ("email", &["GDPR", "PII", "PII"]), // same set, other order
            ("phone", &["SECRET"]),             // changed
            ("dob", &["PII"]),                  // added
            ("blank", &[]),                     // empty is no entry
        ]);
        // `note` (empty -> absent) is no change either.
        assert_eq!(changed_tag_columns(&old, &new), set(&["phone", "dob"]));
        // Removing a column counts.
        assert_eq!(
            changed_tag_columns(&old, &map(&[("ssn", &["PII"])])),
            set(&["email", "phone"])
        );
        // Clearing the map touches every tagged column; a new table every named one.
        assert_eq!(
            changed_tag_columns(&old, &HashMap::new()),
            set(&["ssn", "email", "phone"])
        );
        assert_eq!(
            changed_tag_columns(&HashMap::new(), &old),
            set(&["ssn", "email", "phone"])
        );
        assert!(changed_tag_columns(&old, &old).is_empty());
        // A key that changes case is two columns.
        assert_eq!(
            changed_tag_columns(&map(&[("ssn", &["PII"])]), &map(&[("SSN", &["PII"])])),
            set(&["ssn", "SSN"])
        );
    }

    #[test]
    fn apply_tag_ops_set_merges_and_dedups() {
        let cur = map(&[("email", &["PII"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec!["PII".into(), "GDPR".into()],
            values: Default::default(),
            action: TagAction::Set,
        }];
        let got = apply_tag_ops(&cur, &ops);
        assert_eq!(
            got.get("email").unwrap(),
            &vec!["PII".to_string(), "GDPR".to_string()]
        );
    }

    #[test]
    fn apply_tag_ops_set_leaves_other_columns_untouched() {
        let cur = map(&[("email", &["PII"]), ("region", &["GEO"])]);
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec!["GDPR".into()],
            values: Default::default(),
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
            values: Default::default(),
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
            values: Default::default(),
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
            values: Default::default(),
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
        let got = parse_column_tags(&p).expect("valid map must parse");
        assert_eq!(got.get("email").unwrap(), &vec!["PII", "GDPR"]);
        assert_eq!(got.get("salary").unwrap(), &vec!["PII", "CONFIDENTIAL"]);
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn absent_property_returns_empty() {
        let p = props(&[("other.prop", "value")]);
        let got = parse_column_tags(&p).expect("missing key is known-empty");
        assert!(got.is_empty());
    }

    #[test]
    fn empty_properties_returns_empty() {
        let got = parse_column_tags(&HashMap::new()).expect("missing key is known-empty");
        assert!(got.is_empty());
    }

    #[test]
    fn malformed_json_is_unknown() {
        let p = props(&[("sqe.column-tags", "not-valid-json{{")]);
        assert!(
            parse_column_tags(&p).is_err(),
            "malformed JSON must not be treated as empty tags"
        );
    }

    #[test]
    fn wrong_json_shape_object_value_is_unknown() {
        // JSON is valid but not HashMap<String, Vec<String>>
        let p = props(&[("sqe.column-tags", r#"{"email": "not-an-array"}"#)]);
        assert!(parse_column_tags(&p).is_err());
    }

    #[test]
    fn wrong_json_shape_array_is_unknown() {
        let p = props(&[("sqe.column-tags", r#"["PII","GDPR"]"#)]);
        assert!(parse_column_tags(&p).is_err());
    }

    #[test]
    fn wrong_json_shape_string_is_unknown() {
        let p = props(&[("sqe.column-tags", r#""PII""#)]);
        assert!(parse_column_tags(&p).is_err());
    }

    #[test]
    fn empty_tags_array_is_valid() {
        let p = props(&[("sqe.column-tags", r#"{"col": []}"#)]);
        let got = parse_column_tags(&p).expect("empty tag list is valid");
        assert_eq!(got.get("col").unwrap(), &Vec::<String>::new());
    }

    #[test]
    fn empty_json_object_returns_empty_map() {
        let p = props(&[("sqe.column-tags", "{}")]);
        let got = parse_column_tags(&p).expect("empty object is a valid empty map");
        assert!(got.is_empty());
    }

    // ── set_column_tags read-merge-serialize round-trip ──────────────────────
    //
    // `CatalogOps::set_column_tags` does: load table -> `parse_column_tags` ->
    // `apply_tag_ops` -> `serde_json::to_string` under `PROP_KEY` ->
    // SetProperties commit. The commit hop needs a live REST catalog
    // (`session_catalog_for`), and sqe-coordinator has no in-memory Iceberg
    // catalog harness, so the FULL commit path is exercised by the separate
    // Ranger-backend validation run (live-catalog integration test). These tests
    // pin the pure read-merge-serialize core of that method: the same parse ->
    // apply -> serialize -> parse-back loop, asserting the written property
    // round-trips and a second SET merges into the first. `apply_tag_ops` itself
    // is covered above; this targets the JSON serialize + property round-trip.

    /// Serialize the merged map exactly as `set_column_tags` does (to a JSON
    /// string under `PROP_KEY`), wrap it back into a property map, and parse it
    /// out — mirroring write-then-read against the catalog property store.
    fn serialize_to_props(map: &HashMap<String, Vec<String>>) -> HashMap<String, String> {
        let json = serde_json::to_string(map).expect("merged tag map must serialize");
        props(&[(PROP_KEY, &json)])
    }

    #[test]
    fn set_tags_writes_property_that_reads_back_merged() {
        // No existing tags (fresh table).
        let current = parse_column_tags(&HashMap::new()).expect("missing key is known-empty");
        assert!(current.is_empty());

        // SET TAGS ('PII','GDPR') ON COLUMN email.
        let ops = vec![ColumnTagOp {
            column: "email".into(),
            tags: vec!["PII".into(), "GDPR".into()],
            values: Default::default(),
            action: TagAction::Set,
        }];
        let merged = apply_tag_ops(&current, &ops);

        // Serialize under PROP_KEY (what set_column_tags commits) and read back.
        let written = serialize_to_props(&merged);
        assert!(
            written.contains_key(PROP_KEY),
            "property must be written under PROP_KEY"
        );
        let read_back = parse_column_tags(&written).expect("written property must parse");
        assert_eq!(
            read_back.get("email").unwrap(),
            &vec!["PII".to_string(), "GDPR".to_string()],
            "written property must read back as the merged tag map"
        );
    }

    #[test]
    fn second_set_tags_merges_into_persisted_map() {
        // First SET TAGS, persisted to a property map.
        let first = apply_tag_ops(
            &HashMap::new(),
            &[ColumnTagOp {
                column: "email".into(),
                tags: vec!["PII".into()],
                values: Default::default(),
                action: TagAction::Set,
            }],
        );
        let persisted = serialize_to_props(&first);

        // Second SET TAGS reads the persisted property and merges a new tag.
        let current = parse_column_tags(&persisted).expect("persisted map must parse");
        let second = apply_tag_ops(
            &current,
            &[ColumnTagOp {
                column: "email".into(),
                tags: vec!["GDPR".into()],
                values: Default::default(),
                action: TagAction::Set,
            }],
        );
        let read_back =
            parse_column_tags(&serialize_to_props(&second)).expect("merged property must parse");
        assert_eq!(
            read_back.get("email").unwrap(),
            &vec!["PII".to_string(), "GDPR".to_string()],
            "second SET TAGS must merge into the persisted map (PII + GDPR)"
        );
    }
}

/// One column-name change a schema evolution makes to the tag map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TagKeyChange {
    /// `RENAME COLUMN old TO new`: the association must follow the column.
    Rename { old: String, new: String },
    /// `DROP COLUMN c`: the association is gone with it.
    Drop { column: String },
}

/// Rewrite the column-tag map to follow a schema change.
///
/// `sqe.column-tags` is keyed by column NAME while Iceberg identifies a column by
/// field id, so a rename leaves the association pointing at a name that no longer
/// exists. Nothing then matches, no mask fires, and a governed column comes back
/// RAW. In both engines, because both resolve the association by name. A routine
/// rename silently unmasks a column and nothing reports it.
///
/// Returns `None` when nothing changed, so a caller can skip the property write
/// entirely rather than committing an identical value.
///
/// Deliberate choices:
///
/// * A rename onto a name that ALREADY carries tags UNIONS them rather than
///   replacing. Replacing could drop a mask, which is the fail-open direction. The
///   union can only over-mask.
/// * A dropped column's association is REMOVED. Leaving it looks harmless because
///   a column absent from the schema is never consulted, but re-adding a column of
///   the same name later would silently inherit the old classification.
/// * A rename of an UNtagged column is not a change, so a table with no tags never
///   gets a property written by a rename.
pub(crate) fn rewrite_tag_keys(
    current: &ColumnTagMap,
    changes: &[TagKeyChange],
) -> Option<ColumnTagMap> {
    if current.is_empty() || changes.is_empty() {
        return None;
    }
    let mut map = current.clone();
    let mut touched = false;

    for change in changes {
        match change {
            TagKeyChange::Rename { old, new } => {
                if old == new {
                    continue;
                }
                if let Some(tags) = map.remove(old) {
                    touched = true;
                    let entry = map.entry(new.clone()).or_default();
                    for tag in tags {
                        // By key: a tag both names carry keeps the target's value.
                        if !entry.iter().any(|e| e.key == tag.key) {
                            entry.push(tag);
                        }
                    }
                }
            }
            TagKeyChange::Drop { column } => {
                if map.remove(column).is_some() {
                    touched = true;
                }
            }
        }
    }
    touched.then_some(map)
}

#[cfg(test)]
mod tag_key_rewrite_tests {
    use super::*;

    fn map(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(c, tags)| (c.to_string(), tags.iter().map(|t| t.to_string()).collect()))
            .collect()
    }

    /// The real rewrite on a key-only map, compared as keys.
    fn rewrite_tag_keys(
        current: &HashMap<String, Vec<String>>,
        changes: &[TagKeyChange],
    ) -> Option<HashMap<String, Vec<String>>> {
        super::rewrite_tag_keys(&column_tags::from_keys(current), changes)
            .map(|m| column_tags::keys(&m))
    }

    /// A rename carries tag values; on a union the target keeps its own value.
    #[test]
    fn a_rename_carries_values_and_the_target_keeps_its_own() {
        let current: ColumnTagMap = HashMap::from([
            (
                "ssn".to_string(),
                vec![
                    ColumnTag::with_value("pii", "ssn"),
                    ColumnTag::with_value("geo", "eu"),
                ],
            ),
            (
                "tax_id".to_string(),
                vec![ColumnTag::with_value("pii", "tax")],
            ),
        ]);
        let out = super::rewrite_tag_keys(
            &current,
            &[TagKeyChange::Rename {
                old: "ssn".into(),
                new: "tax_id".into(),
            }],
        )
        .expect("changed");
        assert_eq!(
            out["tax_id"],
            vec![
                ColumnTag::with_value("pii", "tax"),
                ColumnTag::with_value("geo", "eu")
            ]
        );
    }

    /// THE fix. Before this, the association kept naming `ssn` after the rename, so
    /// no tag matched `tax_id` and the column came back RAW in both engines.
    #[test]
    fn a_rename_moves_the_association_to_the_new_name() {
        let current = map(&[("ssn", &["pii"]), ("region", &["geo"])]);
        let out = rewrite_tag_keys(
            &current,
            &[TagKeyChange::Rename {
                old: "ssn".into(),
                new: "tax_id".into(),
            }],
        )
        .expect("the map changed");
        assert_eq!(
            out.get("tax_id").map(Vec::as_slice),
            Some(&["pii".to_string()][..])
        );
        assert!(
            !out.contains_key("ssn"),
            "the old key must be gone: {out:?}"
        );
        assert_eq!(
            out.get("region").map(Vec::as_slice),
            Some(&["geo".to_string()][..])
        );
    }

    /// A rename onto a name that already carries tags UNIONS. Replacing could drop
    /// a mask, which is the fail-open direction; a union can only over-mask.
    #[test]
    fn a_rename_onto_a_tagged_name_unions_rather_than_replacing() {
        let current = map(&[("ssn", &["pii"]), ("tax_id", &["sensitive"])]);
        let out = rewrite_tag_keys(
            &current,
            &[TagKeyChange::Rename {
                old: "ssn".into(),
                new: "tax_id".into(),
            }],
        )
        .expect("changed");
        let mut got = out.get("tax_id").cloned().expect("tax_id present");
        got.sort();
        assert_eq!(got, vec!["pii".to_string(), "sensitive".to_string()]);
    }

    /// A dropped column's association goes with it. Leaving it looks inert, because
    /// a column absent from the schema is never consulted, but re-adding the same
    /// name later would silently inherit the old classification.
    #[test]
    fn a_drop_removes_the_association() {
        let current = map(&[("ssn", &["pii"]), ("region", &["geo"])]);
        let out = rewrite_tag_keys(
            &current,
            &[TagKeyChange::Drop {
                column: "ssn".into(),
            }],
        )
        .expect("changed");
        assert!(!out.contains_key("ssn"));
        assert!(out.contains_key("region"));
    }

    /// Renaming an UNtagged column changes nothing, so no property is written.
    /// Without this a plain rename on an untagged table would commit a redundant
    /// SetProperties on every ALTER.
    #[test]
    fn renaming_an_untagged_column_is_not_a_change() {
        let current = map(&[("ssn", &["pii"])]);
        assert_eq!(
            rewrite_tag_keys(
                &current,
                &[TagKeyChange::Rename {
                    old: "region".into(),
                    new: "area".into()
                }]
            ),
            None
        );
    }

    /// A table with no tags at all is never touched.
    #[test]
    fn a_table_with_no_tags_is_never_touched() {
        assert_eq!(
            rewrite_tag_keys(
                &HashMap::new(),
                &[TagKeyChange::Rename {
                    old: "a".into(),
                    new: "b".into()
                }]
            ),
            None
        );
    }

    /// Renaming to the same name is a no-op, not a self-move that could drop the key.
    #[test]
    fn renaming_to_the_same_name_is_a_no_op() {
        let current = map(&[("ssn", &["pii"])]);
        assert_eq!(
            rewrite_tag_keys(
                &current,
                &[TagKeyChange::Rename {
                    old: "ssn".into(),
                    new: "ssn".into()
                }]
            ),
            None
        );
    }

    /// Several changes in one statement all apply, including a chained rename.
    #[test]
    fn a_chain_of_renames_in_one_statement_lands_on_the_last_name() {
        let current = map(&[("a", &["t1"]), ("z", &["t2"])]);
        let out = rewrite_tag_keys(
            &current,
            &[
                TagKeyChange::Rename {
                    old: "a".into(),
                    new: "b".into(),
                },
                TagKeyChange::Rename {
                    old: "b".into(),
                    new: "c".into(),
                },
                TagKeyChange::Drop { column: "z".into() },
            ],
        )
        .expect("changed");
        assert_eq!(
            out.get("c").map(Vec::as_slice),
            Some(&["t1".to_string()][..])
        );
        assert!(!out.contains_key("a") && !out.contains_key("b"));
        assert!(!out.contains_key("z"));
    }
}

#[cfg(test)]
mod scan_column_tags_tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{Catalog, CatalogBuilder, TableCreation};
    use sqe_catalog::table_provider::SqeTableProvider;
    use sqe_core::column_tags::ids::IDS_PROP_KEY;
    use sqe_policy::plan_rewriter::ScanTagState;

    /// One column, `ssn`, field id 1.
    fn ssn_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![NestedField::optional(
                1,
                "ssn",
                Type::Primitive(PrimitiveType::String),
            )
            .into()])
            .build()
            .unwrap()
    }

    /// The provider planning builds for a table carrying `properties`.
    async fn provider_with(properties: &[(&str, &str)]) -> SqeTableProvider {
        let catalog = iceberg::memory::MemoryCatalogBuilder::default()
            .load(
                "memory",
                [("warehouse".to_string(), "memory:///wh".to_string())].into(),
            )
            .await
            .unwrap();
        let ns = NamespaceIdent::new("ns".into());
        catalog
            .create_namespace(&ns, Default::default())
            .await
            .unwrap();
        let schema = ssn_schema();
        let table = catalog
            .create_table(
                &ns,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .properties(
                        properties
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect::<HashMap<_, _>>(),
                    )
                    .build(),
            )
            .await
            .unwrap();
        SqeTableProvider::try_new(table).await.unwrap()
    }

    #[tokio::test]
    async fn an_iceberg_scan_reports_its_tags() {
        let provider = provider_with(&[(PROP_KEY, r#"{"ssn":["PII"]}"#)]).await;
        assert_eq!(
            scan_column_tags(&provider),
            ScanTagState::Known(HashMap::from([(
                "ssn".to_string(),
                vec!["PII".to_string()]
            )]))
        );
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn a_tag_view_is_known_untagged() {
        use datafusion::catalog::TableFunctionImpl;
        use datafusion::prelude::lit;
        use sqe_catalog::tag_views::TagViewFunction;

        let catalog = iceberg::memory::MemoryCatalogBuilder::default()
            .load(
                "memory",
                [("warehouse".to_string(), "memory:///wh".to_string())].into(),
            )
            .await
            .unwrap();
        let catalog = Arc::new(sqe_catalog::SessionCatalog::local(
            Arc::new(catalog),
            "lake",
            &Default::default(),
        ));
        let function = TagViewFunction::new(Arc::new(move |_| {
            Ok((Arc::clone(&catalog), "lake".to_string()))
        }));
        let provider = function.call(&[lit("column_tags")]).unwrap();
        assert_eq!(
            scan_column_tags(provider.as_ref()),
            ScanTagState::Known(HashMap::new())
        );
    }

    #[tokio::test]
    async fn an_untagged_iceberg_scan_is_known_empty() {
        let provider = provider_with(&[]).await;
        assert_eq!(
            scan_column_tags(&provider),
            ScanTagState::Known(HashMap::new())
        );
    }

    #[tokio::test]
    async fn malformed_tags_are_unknown_not_empty() {
        let provider = provider_with(&[(PROP_KEY, "not-json{{")]).await;
        assert_eq!(scan_column_tags(&provider), ScanTagState::Unknown);
    }

    /// An incremental read of a table carrying `properties` (#844).
    fn incremental_with(
        properties: Option<&[(&str, &str)]>,
    ) -> sqe_catalog::incremental_provider::IncrementalTableProvider {
        use sqe_catalog::incremental_provider::IncrementalTableProvider;
        use sqe_catalog::incremental_scan::IncrementalPlan;
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("ssn", arrow::datatypes::DataType::Utf8, true),
        ]));
        let provider = IncrementalTableProvider::new(schema, IncrementalPlan::default(), None);
        match properties {
            Some(properties) => provider.with_table_properties(
                properties
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                Arc::new(ssn_schema()),
            ),
            None => provider,
        }
    }

    /// The `sqe.column-tag-ids` value binding `tags` with `ids` (#920).
    fn ids_for(tags: &str, ids: &str) -> String {
        format!(
            r#"{{"v":1,"tags_sha256":"{}","ids":{ids}}}"#,
            column_tags::ids::digest(tags)
        )
    }

    /// Tagged as `email` with field id 1: the column was renamed to `ssn`
    /// since, so the tag applies under `ssn` (#920).
    #[tokio::test]
    async fn a_renamed_column_keeps_its_tags() {
        let tags = r#"{"email":["PII"]}"#;
        let ids = ids_for(tags, r#"{"email":1}"#);
        let want = ScanTagState::Known(HashMap::from([(
            "ssn".to_string(),
            vec!["PII".to_string()],
        )]));
        let provider = provider_with(&[(PROP_KEY, tags), (IDS_PROP_KEY, &ids)]).await;
        assert_eq!(scan_column_tags(&provider), want);
        let incremental = incremental_with(Some(&[(PROP_KEY, tags), (IDS_PROP_KEY, &ids)]));
        assert_eq!(scan_column_tags(&incremental), want);
    }

    /// Tagged column id 7 was dropped: a column re-added under the same
    /// name is a new column and does not inherit the tag (#920).
    #[tokio::test]
    async fn a_dropped_columns_tags_do_not_apply_to_a_new_column() {
        let tags = r#"{"ssn":["PII"]}"#;
        let ids = ids_for(tags, r#"{"ssn":7}"#);
        let provider = provider_with(&[(PROP_KEY, tags), (IDS_PROP_KEY, &ids)]).await;
        assert_eq!(
            scan_column_tags(&provider),
            ScanTagState::Known(HashMap::new())
        );
    }

    /// Ids bound to another tags value (a writer that does not know about
    /// ids changed the tags) are ignored: the tags apply by name.
    #[tokio::test]
    async fn stale_ids_fall_back_to_names() {
        let ids = ids_for(r#"{"other":["PII"]}"#, r#"{"ssn":7}"#);
        let provider =
            provider_with(&[(PROP_KEY, r#"{"ssn":["PII"]}"#), (IDS_PROP_KEY, &ids)]).await;
        assert_eq!(
            scan_column_tags(&provider),
            ScanTagState::Known(HashMap::from([(
                "ssn".to_string(),
                vec!["PII".to_string()]
            )]))
        );
    }

    /// Malformed ids make the tag state unknown: fail closed.
    #[tokio::test]
    async fn malformed_ids_are_unknown() {
        let provider =
            provider_with(&[(PROP_KEY, r#"{"ssn":["PII"]}"#), (IDS_PROP_KEY, "nope")]).await;
        assert_eq!(scan_column_tags(&provider), ScanTagState::Unknown);
    }

    #[test]
    fn an_incremental_scan_reports_its_tables_tags() {
        let provider = incremental_with(Some(&[(PROP_KEY, r#"{"ssn":["PII"]}"#)]));
        assert_eq!(
            scan_column_tags(&provider),
            ScanTagState::Known(HashMap::from([(
                "ssn".to_string(),
                vec!["PII".to_string()]
            )]))
        );
    }

    #[test]
    fn an_untagged_incremental_scan_is_known_empty() {
        let provider = incremental_with(Some(&[]));
        assert_eq!(
            scan_column_tags(&provider),
            ScanTagState::Known(HashMap::new())
        );
    }

    #[test]
    fn malformed_incremental_tags_are_unknown() {
        let provider = incremental_with(Some(&[(PROP_KEY, "not-json{{")]));
        assert_eq!(scan_column_tags(&provider), ScanTagState::Unknown);
    }

    /// Without the table's properties nothing says the read is untagged,
    /// so it must fall back to the cache, never read as Known(empty).
    #[test]
    fn an_incremental_scan_without_properties_is_not_recognised() {
        let provider = incremental_with(None);
        assert_eq!(scan_column_tags(&provider), ScanTagState::NotRecognised);
    }

    /// The provider DataFusion builds for `name(args)` in a FROM clause.
    fn builtin_tvf(name: &str, args: &[i64]) -> Arc<dyn datafusion::catalog::TableProvider> {
        use datafusion::logical_expr::lit;
        let func = datafusion::functions_table::all_default_table_functions()
            .into_iter()
            .find(|f| f.name() == name)
            .unwrap();
        let args: Vec<_> = args.iter().map(|v| lit(*v)).collect();
        let state = datafusion::prelude::SessionContext::new().state();
        func.create_table_provider_with_args(datafusion::catalog::TableFunctionArgs::new(
            &args, &state,
        ))
        .unwrap()
    }

    #[test]
    fn generate_series_and_range_are_known_untagged() {
        for name in ["generate_series", "range"] {
            let provider = builtin_tvf(name, &[0, 9]);
            assert_eq!(
                scan_column_tags(provider.as_ref()),
                ScanTagState::Known(HashMap::new()),
                "{name}"
            );
        }
    }

    #[test]
    fn any_other_provider_is_not_recognised() {
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let empty = datafusion::datasource::empty::EmptyTable::new(schema);
        assert_eq!(scan_column_tags(&empty), ScanTagState::NotRecognised);
    }
}
