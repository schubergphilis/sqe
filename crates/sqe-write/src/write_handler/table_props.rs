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
    use sqlparser::ast::SqlOption;
    for opt in options {
        if let SqlOption::KeyValue { key, value } = opt {
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

/// Live keys a staged replace does not carry: they name the old table's
/// storage paths, and the staged table has a location of its own.
const LOCATION_BOUND_PROPERTIES: &[&str] = &[
    "write.data.path",
    "write.metadata.path",
    "write.object-storage.path",
    "write.folder-storage.path",
];

/// Carry the live table's properties onto a staged `CREATE OR REPLACE`
/// (#896), as the in-place replace keeps them (#894): every live key the
/// statement does not set, except the reserved keys, the location-bound
/// paths and `sqe.json-columns` (recomputed from the SELECT).
/// `sqe.column-tags` keeps only the entries for `columns` the new schema
/// still has, matched by top-level name. A tag value that does not parse is
/// carried as it is: the policy rewriter fails closed on it, and dropping it
/// would silently clear the tags.
pub(crate) fn carry_live_properties(
    props: &mut HashMap<String, String>,
    live: &HashMap<String, String>,
    columns: &[&str],
) {
    use crate::tag_source_impl::PROP_KEY as TAGS;
    use sqe_core::json_columns::JSON_COLUMNS_PROP;
    let is = |k: &str, set: &[&str]| set.iter().any(|r| k.eq_ignore_ascii_case(r));
    for (k, v) in live {
        if props.keys().any(|p| p.eq_ignore_ascii_case(k))
            || is(k, &iceberg::spec::TableProperties::RESERVED_PROPERTIES)
            || is(k, LOCATION_BOUND_PROPERTIES)
            || k.eq_ignore_ascii_case(JSON_COLUMNS_PROP)
        {
            continue;
        }
        let value = if k == TAGS {
            match serde_json::from_str::<std::collections::BTreeMap<String, Vec<String>>>(v) {
                Ok(mut tags) => {
                    let kept = |c: &str| columns.iter().any(|n| c.eq_ignore_ascii_case(n));
                    tags.retain(|c, _| kept(c) || kept(c.split('.').next().unwrap_or(c)));
                    if tags.is_empty() {
                        continue;
                    }
                    serde_json::to_string(&tags).unwrap_or_else(|_| v.clone())
                }
                Err(_) => v.clone(),
            }
        } else {
            v.clone()
        };
        props.insert(k.clone(), value);
    }
}

/// `tx` with `properties` set as one more action of the same commit. No
/// properties: `tx` unchanged.
pub(super) fn with_properties(
    tx: Transaction,
    properties: &HashMap<String, String>,
) -> iceberg::Result<Transaction> {
    if properties.is_empty() {
        return Ok(tx);
    }
    let mut action = tx.update_table_properties();
    for (k, v) in properties {
        action = action.set(k.clone(), v.clone());
    }
    action.apply(tx)
}

/// Set `properties` on a table in a commit of their own: the in-place replace
/// of an empty table by an empty SELECT changes no data, so it commits no
/// rewrite the properties could ride on (#894).
pub(super) async fn commit_properties(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    properties: &HashMap<String, String>,
) -> sqe_core::Result<()> {
    if properties.is_empty() {
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

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn carry_live_properties_keeps_unset_keys_and_skips_bound_ones() {
        let mut props = map(&[("a", "2"), ("format-version", "2")]);
        let live = map(&[
            ("A", "1"),
            ("keep", "x"),
            ("format-version", "3"),
            ("write.data.path", "s3://old/data"),
            ("sqe.json-columns", "[\"j\"]"),
        ]);
        carry_live_properties(&mut props, &live, &["id"]);
        let mut got: Vec<_> = props.into_iter().collect();
        got.sort();
        let want = [("a", "2"), ("format-version", "2"), ("keep", "x")]
            .map(|(k, v)| (k.to_string(), v.to_string()));
        assert_eq!(got, want);
    }

    #[test]
    fn carry_live_properties_keeps_tags_of_columns_still_there() {
        let tags = r#"{"email":["PII"],"gone":["PII"],"addr.zip":["PII"],"ADDR2":["X"]}"#;
        let live = map(&[("sqe.column-tags", tags)]);
        let mut props = HashMap::new();
        carry_live_properties(&mut props, &live, &["id", "Email", "addr", "addr2"]);
        assert_eq!(
            props["sqe.column-tags"],
            r#"{"ADDR2":["X"],"addr.zip":["PII"],"email":["PII"]}"#
        );

        // No tagged column left: the key goes.
        let mut props = HashMap::new();
        carry_live_properties(&mut props, &live, &["id"]);
        assert!(props.is_empty(), "{props:?}");
    }

    #[test]
    fn carry_live_properties_carries_malformed_tags_verbatim() {
        let live = map(&[("sqe.column-tags", "not json")]);
        let mut props = HashMap::new();
        carry_live_properties(&mut props, &live, &["id"]);
        assert_eq!(props["sqe.column-tags"], "not json");
    }
}
