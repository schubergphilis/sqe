//! Table properties a CREATE TABLE / CTAS statement sets: the user's
//! `TBLPROPERTIES` / `WITH` options plus the format-version directive.

use std::collections::HashMap;

use iceberg::spec::FormatVersion;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};
use sqe_core::{SqeError, SqeErrorCode};

use super::format_version_properties;
use crate::commit::commit_with_retry;
use crate::ident::fold_unquoted_ident;

/// Fold a `Vec<SqlOption>` (from sqlparser-rs `TBLPROPERTIES (...)` or
/// `WITH (...)` clauses) into a property HashMap.
///
/// Only `KeyValue` options are materialised. Existing entries in `props`
/// (typically `format-version` set by the SQE auto-upgrade path) are
/// preserved when the user did not explicitly set them; an explicit
/// user-supplied value wins so callers can pin a different format version.
/// Extract the user-specified option list from a sqlparser 0.62
/// `CreateTableOptions`. The 0.62 AST collapses the old separate
/// `table_properties` (TBLPROPERTIES) and `with_options` (WITH (...)) fields
/// into this single enum; every property-bearing variant carries a
/// `Vec<SqlOption>`, so we return that slice regardless of the syntax the user
/// wrote. `None` (no options) yields an empty slice.
pub(crate) fn create_table_options_as_slice(
    options: &sqlparser::ast::CreateTableOptions,
) -> &[sqlparser::ast::SqlOption] {
    use sqlparser::ast::CreateTableOptions;
    match options {
        CreateTableOptions::With(opts)
        | CreateTableOptions::Options(opts)
        | CreateTableOptions::Plain(opts)
        | CreateTableOptions::TableProperties(opts) => opts.as_slice(),
        CreateTableOptions::None => &[],
    }
}

/// The properties a CREATE TABLE / CTAS hands to `create_table`: the
/// format-version directive for the backend plus every user TBLPROPERTIES /
/// WITH option. A non-REST catalog rejects any reserved property, including a
/// user's own `'format-version' = '3'`; the version it asked for is already in
/// `TableCreation.format_version` (`plan_create_table_schema` reads the
/// option), so the reserved keys are dropped there (#854). `rest_catalog`:
/// the catalog speaks the Iceberg REST protocol.
pub(crate) fn create_table_properties(
    rest_catalog: bool,
    format_version: FormatVersion,
    options: &[sqlparser::ast::SqlOption],
) -> std::collections::HashMap<String, String> {
    let mut props = if rest_catalog {
        format_version_properties(format_version)
    } else {
        std::collections::HashMap::new()
    };
    merge_user_table_properties(&mut props, options);
    if !rest_catalog {
        props.retain(|k, _| {
            !iceberg::spec::TableProperties::RESERVED_PROPERTIES.contains(&k.as_str())
        });
    }
    props
}

/// Record `columns` in the `sqe.json-columns` property (#848). Nothing is
/// written for an empty set, so a table without JSON columns keeps the
/// properties it had.
pub(crate) fn record_json_columns(
    props: &mut std::collections::HashMap<String, String>,
    columns: std::collections::BTreeSet<String>,
) {
    use sqe_core::json_columns::{encode, JSON_COLUMNS_PROP};
    if columns.is_empty() {
        return;
    }
    props.insert(JSON_COLUMNS_PROP.to_string(), encode(&columns));
}

/// The folded names of the columns a CREATE TABLE declares `JSON`.
pub(crate) fn declared_json_columns(
    columns: &[sqlparser::ast::ColumnDef],
) -> std::collections::BTreeSet<String> {
    columns
        .iter()
        .filter(|col| matches!(col.data_type, sqlparser::ast::DataType::JSON))
        .map(|col| fold_unquoted_ident(&col.name))
        .collect()
}

pub(crate) fn merge_user_table_properties(
    props: &mut std::collections::HashMap<String, String>,
    options: &[sqlparser::ast::SqlOption],
) {
    use sqlparser::ast::{CommentDef, SqlOption};
    for opt in options {
        // `CREATE TABLE t COMMENT '...'`: the `comment` property, where
        // `COMMENT ON TABLE` and Trino's Iceberg connector keep it (#897).
        if let SqlOption::Comment(CommentDef::WithEq(c) | CommentDef::WithoutEq(c)) = opt {
            props.insert("comment".to_string(), c.clone());
        }
        if let SqlOption::KeyValue { key, value } = opt {
            // Trino's spelling of the format version is an option, not a
            // property: `TableCreation.format_version` carries it (#897).
            if key.value.eq_ignore_ascii_case("format_version") {
                continue;
            }
            let k = key.value.clone();
            let v = sql_expr_to_property_string(value);
            props.insert(k, v);
        }
    }
}

/// Turn a sqlparser `Expr` value used in TBLPROPERTIES / WITH into the
/// raw string Iceberg expects. We trim surrounding string-literal quotes
/// so `'merge-on-read'` becomes `merge-on-read` rather than `'merge-on-read'`.
pub(super) fn sql_expr_to_property_string(expr: &sqlparser::ast::Expr) -> String {
    use sqlparser::ast::{Expr, Value};
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            Value::Number(n, _) => n.clone(),
            Value::Boolean(b) => b.to_string(),
            other => other.to_string(),
        },
        other => other.to_string(),
    }
}

/// The properties an in-place `CREATE OR REPLACE TABLE` sets on the live
/// table (#894): the statement's `TBLPROPERTIES` / `WITH` options, set over
/// the live ones as an Iceberg replace transaction does (keys the statement
/// does not name stay). The replace keeps the table, so the reserved keys
/// (`format-version`, ...) and SQE's `format_version` option are dropped.
pub(crate) fn replace_in_place_properties(
    options: &[sqlparser::ast::SqlOption],
) -> std::collections::HashMap<String, String> {
    let mut props = std::collections::HashMap::new();
    merge_user_table_properties(&mut props, options);
    props.retain(|k, _| {
        !k.eq_ignore_ascii_case("format_version")
            && !iceberg::spec::TableProperties::RESERVED_PROPERTIES
                .iter()
                .any(|r| k.eq_ignore_ascii_case(r))
    });
    props
}

/// The property changes a `CREATE OR REPLACE` commits with its data.
#[derive(Debug, Default)]
pub(crate) struct PropertyChanges {
    pub(crate) set: HashMap<String, String>,
    pub(crate) remove: Vec<String>,
}

/// The property changes of a `CREATE OR REPLACE` that changes the table's
/// columns or partitioning (#892). The table stays, so its live properties
/// stay too. On top of the statement's keys ([`replace_in_place_properties`]):
/// - `sqe.json-columns` is recomputed from the SELECT's `json_columns`, and
///   removed when none is left (#848);
/// - `sqe.column-tags` keeps only the entries of `columns` the new schema
///   still has, matched by top-level name, and is removed when none is left
///   (#896). A tag value that does not parse stays as it is: the policy
///   rewriter fails closed on it, and dropping it would silently clear the
///   tags.
pub(crate) fn replace_shape_properties(
    options: &[sqlparser::ast::SqlOption],
    live: &HashMap<String, String>,
    columns: &[&str],
    json_columns: std::collections::BTreeSet<String>,
) -> PropertyChanges {
    use crate::tag_source_impl::PROP_KEY as TAGS;
    use sqe_core::json_columns::JSON_COLUMNS_PROP;
    let mut changes = PropertyChanges {
        set: replace_in_place_properties(options),
        remove: Vec::new(),
    };
    record_json_columns(&mut changes.set, json_columns);
    if !changes.set.contains_key(JSON_COLUMNS_PROP) && live.contains_key(JSON_COLUMNS_PROP) {
        changes.remove.push(JSON_COLUMNS_PROP.to_string());
    }
    let Some(live_tags) = live.get(TAGS) else {
        return changes;
    };
    if changes.set.contains_key(TAGS) {
        return changes;
    }
    let Ok(mut tags) =
        serde_json::from_str::<std::collections::BTreeMap<String, Vec<String>>>(live_tags)
    else {
        return changes;
    };
    let kept = |c: &str| columns.iter().any(|n| c.eq_ignore_ascii_case(n));
    tags.retain(|c, _| kept(c) || kept(c.split('.').next().unwrap_or(c)));
    if tags.is_empty() {
        changes.remove.push(TAGS.to_string());
    } else if let Ok(kept) = serde_json::to_string(&tags) {
        if kept != *live_tags {
            changes.set.insert(TAGS.to_string(), kept);
        }
    }
    changes
}

/// `tx` with `properties` applied as one more action of the same commit.
/// No changes: `tx` unchanged.
pub(super) fn with_properties(
    tx: Transaction,
    properties: &PropertyChanges,
) -> iceberg::Result<Transaction> {
    if properties.set.is_empty() && properties.remove.is_empty() {
        return Ok(tx);
    }
    let mut action = tx.update_table_properties();
    for (k, v) in &properties.set {
        action = action.set(k.clone(), v.clone());
    }
    for k in &properties.remove {
        action = action.remove(k.clone());
    }
    action.apply(tx)
}

/// Set `properties` on a table in a commit of their own: the in-place replace
/// of an empty table by an empty SELECT changes no data, so it commits no
/// rewrite the properties could ride on (#894).
pub(super) async fn commit_properties(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    properties: &PropertyChanges,
) -> sqe_core::Result<()> {
    if properties.set.is_empty() && properties.remove.is_empty() {
        return Ok(());
    }
    let op = "ctas-replace-properties";
    commit_with_retry(catalog, table_ident, op, |fresh| async move {
        let tx = with_properties(Transaction::new(&fresh), properties)?;
        tx.commit(catalog).await
    })
    .await
    .map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("Failed to set the table properties of CREATE OR REPLACE TABLE: {e}"),
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_in_place_properties_keep_user_keys_and_drop_reserved_ones() {
        let sql = "CREATE OR REPLACE TABLE t TBLPROPERTIES ('a.b' = '2', \
                   'write.merge.mode' = 'copy-on-write', 'format-version' = '3', \
                   'format_version' = '3') AS SELECT 1";
        let dialect = sqlparser::dialect::GenericDialect {};
        let stmt = sqlparser::parser::Parser::parse_sql(&dialect, sql)
            .expect("parse")
            .remove(0);
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("not a CREATE TABLE");
        };
        let props = replace_in_place_properties(create_table_options_as_slice(&ct.table_options));
        let mut got: Vec<_> = props.into_iter().collect();
        got.sort();
        let want = [("a.b", "2"), ("write.merge.mode", "copy-on-write")]
            .map(|(k, v)| (k.to_string(), v.to_string()));
        assert_eq!(got, want);
    }

    fn create_options(sql: &str) -> Vec<sqlparser::ast::SqlOption> {
        let dialect = sqlparser::dialect::GenericDialect {};
        let stmt = sqlparser::parser::Parser::parse_sql(&dialect, sql)
            .expect("parse")
            .remove(0);
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("not a CREATE TABLE");
        };
        create_table_options_as_slice(&ct.table_options).to_vec()
    }

    /// #897: Trino's `format_version` sets the version and is not stored as
    /// a property, on a REST catalog and on the others.
    #[test]
    fn create_table_properties_do_not_store_trino_format_version() {
        let opts = create_options("CREATE TABLE t WITH (format_version = 3, a = 'x') AS SELECT 1");
        let rest = create_table_properties(true, FormatVersion::V3, &opts);
        assert_eq!(rest.get("format-version").map(String::as_str), Some("3"));
        assert!(!rest.contains_key("format_version"), "{rest:?}");
        assert_eq!(rest.get("a").map(String::as_str), Some("x"));
        let local = create_table_properties(false, FormatVersion::V3, &opts);
        assert_eq!(local, map(&[("a", "x")]));
    }

    /// #897: a table comment, written inline or folded into the options by
    /// the front door, is the `comment` property.
    #[test]
    fn create_table_comment_is_the_comment_property() {
        for sql in [
            "CREATE TABLE t COMMENT 'a mart' AS SELECT 1",
            "CREATE TABLE t COMMENT = 'a mart' AS SELECT 1",
            "CREATE TABLE t WITH (format_version = 2, comment = 'a mart') AS SELECT 1",
        ] {
            let props = create_table_properties(false, FormatVersion::V2, &create_options(sql));
            assert_eq!(props, map(&[("comment", "a mart")]), "{sql}");
        }
    }

    /// #897: the version a new table (CREATE TABLE or CTAS) is created at.
    #[test]
    fn new_table_format_version_follows_the_option() {
        use crate::write_handler::new_table_format_version as version;
        let v2 = FormatVersion::V2;
        for (sql, want) in [
            ("CREATE TABLE t AS SELECT 1", FormatVersion::V2),
            (
                "CREATE TABLE t WITH (format_version = 3) AS SELECT 1",
                FormatVersion::V3,
            ),
            (
                "CREATE TABLE t WITH (format_version = '3') AS SELECT 1",
                FormatVersion::V3,
            ),
            (
                "CREATE TABLE t WITH (FORMAT_VERSION = 1) AS SELECT 1",
                FormatVersion::V1,
            ),
            (
                "CREATE TABLE t TBLPROPERTIES ('format-version' = '3') AS SELECT 1",
                FormatVersion::V3,
            ),
        ] {
            let got = version(&create_options(sql), None, v2).unwrap();
            assert_eq!(got, want, "{sql}");
        }
        // A variant column needs V3: it upgrades a default, and contradicts
        // an explicit lower version.
        let none = create_options("CREATE TABLE t AS SELECT 1");
        let got = version(&none, Some("variant"), v2).unwrap();
        assert_eq!(got, FormatVersion::V3);
        let low = create_options("CREATE TABLE t WITH (format_version = 2) AS SELECT 1");
        let err = version(&low, Some("variant"), v2).unwrap_err().to_string();
        assert!(err.contains("needs format v3"), "{err}");
        let bad = create_options("CREATE TABLE t WITH (format_version = 4) AS SELECT 1");
        assert!(version(&bad, None, v2).is_err());
    }

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn shape_changes(live: &HashMap<String, String>, columns: &[&str]) -> PropertyChanges {
        replace_shape_properties(&[], live, columns, Default::default())
    }

    #[test]
    fn replace_shape_properties_recompute_json_columns() {
        let live = map(&[("keep", "x"), ("sqe.json-columns", "[\"j\"]")]);
        let changes = shape_changes(&live, &["id"]);
        assert!(changes.set.is_empty(), "{:?}", changes.set);
        assert_eq!(changes.remove, ["sqe.json-columns"]);

        let json = ["k".to_string()].into_iter().collect();
        let changes = replace_shape_properties(&[], &live, &["k"], json);
        assert_eq!(changes.set["sqe.json-columns"], "[\"k\"]");
        assert!(changes.remove.is_empty());
    }

    #[test]
    fn replace_shape_properties_keep_tags_of_columns_still_there() {
        let tags = r#"{"email":["PII"],"gone":["PII"],"addr.zip":["PII"],"ADDR2":["X"]}"#;
        let live = map(&[("sqe.column-tags", tags)]);
        let changes = shape_changes(&live, &["id", "Email", "addr", "addr2"]);
        assert_eq!(
            changes.set["sqe.column-tags"],
            r#"{"ADDR2":["X"],"addr.zip":["PII"],"email":["PII"]}"#
        );

        // No tagged column left: the key goes.
        let changes = shape_changes(&live, &["id"]);
        assert!(changes.set.is_empty(), "{:?}", changes.set);
        assert_eq!(changes.remove, ["sqe.column-tags"]);
    }

    #[test]
    fn replace_shape_properties_leave_malformed_tags_alone() {
        let live = map(&[("sqe.column-tags", "not json")]);
        let changes = shape_changes(&live, &["id"]);
        assert!(changes.set.is_empty() && changes.remove.is_empty());
    }
}
