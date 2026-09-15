//! Athena / Trino views stored in Glue (issue #667).
//!
//! Athena keeps a view in Glue as a table with `TableType = VIRTUAL_VIEW`, an
//! empty `StorageDescriptor` (no location, no input format, no serde) and the
//! view definition in `ViewOriginalText`, encoded the way Presto and Trino do:
//!
//! ```text
//! /* Presto View: <base64 of {"originalSql": "...", "catalog": "awsdatacatalog",
//!                             "schema": "db", "columns": [...], ...}> */
//! ```
//!
//! Before this module the Hive provider treated such an entry as a storage
//! table with a missing format: it appeared in `SHOW TABLES`, and a `SELECT`
//! on it logged "storage format is not readable" and answered "table not
//! found". An LLM client over MCP saw the table, queried it, got not-found and
//! retried in circles.
//!
//! This module decodes the payload; `HiveSchemaProvider::table` plans the
//! view's SQL in a scratch session over the same Hive catalog (the pattern
//! `SqeSchemaProvider::plan_view` uses for Iceberg views) and serves it as a
//! DataFusion `ViewTable`. Base tables resolve through the ordinary Hive
//! provider, so policy rewriting sees the underlying scans, not the view.

use base64::Engine;

use super::HiveError;

/// The decoded body of a Presto/Trino view definition. Only the fields SQE
/// needs; the payload also carries `owner`, `runAsInvoker` and `comment`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrestoView {
    /// The `SELECT` as the author wrote it, in Trino dialect. Unqualified and
    /// schema-qualified table references are relative to `catalog`.`schema`.
    pub original_sql: String,
    /// Athena always writes `awsdatacatalog`; Trino writes the catalog the
    /// view was created in.
    #[serde(default)]
    pub catalog: Option<String>,
    /// The database the view lives in, which is also the default schema for
    /// unqualified references in `original_sql`.
    #[serde(default)]
    pub schema: Option<String>,
    /// Declared output columns. Informational: the planned SQL is the source
    /// of truth for the schema SQE serves.
    #[serde(default)]
    pub columns: Vec<PrestoViewColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct PrestoViewColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
}

/// Glue's `TableType` value for a view.
pub const VIRTUAL_VIEW: &str = "VIRTUAL_VIEW";

/// Comment prefixes Athena, Presto and Trino wrap the base64 payload in.
const PREFIXES: &[&str] = &["/* Presto View:", "/* Trino View:"];

/// Decodes `ViewOriginalText` into its [`PrestoView`].
///
/// A view whose text does not carry a recognized prefix (a Hive view written
/// by Spark or Hive itself stores plain SQL there) is reported as a
/// validation error naming what was found, so the log says why a view was
/// hidden rather than pretending it is a table.
pub fn decode_presto_view(view_original_text: &str) -> Result<PrestoView, HiveError> {
    let text = view_original_text.trim();
    let Some(body) = PREFIXES
        .iter()
        .find_map(|p| text.strip_prefix(p))
        .and_then(|rest| rest.trim_end().strip_suffix("*/"))
    else {
        return Err(HiveError::Validation(
            "view definition is not a Presto/Trino view payload (expected \
             `/* Presto View: <base64> */`); Hive-native views are not supported"
                .to_string(),
        ));
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(body.trim())
        .map_err(|e| HiveError::Validation(format!("view payload is not valid base64: {e}")))?;
    let view: PrestoView = serde_json::from_slice(&bytes)
        .map_err(|e| HiveError::Validation(format!("view payload is not valid JSON: {e}")))?;
    if view.original_sql.trim().is_empty() {
        return Err(HiveError::Validation(
            "view payload carries an empty originalSql".to_string(),
        ));
    }
    Ok(view)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(json: &str) -> String {
        format!(
            "/* Presto View: {} */",
            base64::engine::general_purpose::STANDARD.encode(json)
        )
    }

    #[test]
    fn decodes_an_athena_payload() {
        let text = encode(
            r#"{"originalSql":"SELECT id, amount FROM orders WHERE amount > 10",
                "catalog":"awsdatacatalog","schema":"sales",
                "columns":[{"name":"id","type":"bigint"},{"name":"amount","type":"double"}],
                "owner":"arn:aws:iam::123456789012:user/x","runAsInvoker":false}"#,
        );
        let v = decode_presto_view(&text).unwrap();
        assert_eq!(
            v.original_sql,
            "SELECT id, amount FROM orders WHERE amount > 10"
        );
        assert_eq!(v.catalog.as_deref(), Some("awsdatacatalog"));
        assert_eq!(v.schema.as_deref(), Some("sales"));
        assert_eq!(v.columns.len(), 2);
        assert_eq!(v.columns[1].type_name, "double");
    }

    #[test]
    fn accepts_the_trino_prefix_and_surrounding_whitespace() {
        let b64 = base64::engine::general_purpose::STANDARD
            .encode(r#"{"originalSql":"SELECT 1 AS one","schema":"s"}"#);
        let text = format!("  /* Trino View:   {b64}   */  ");
        let v = decode_presto_view(&text).unwrap();
        assert_eq!(v.original_sql, "SELECT 1 AS one");
        assert!(v.catalog.is_none());
        assert!(v.columns.is_empty());
    }

    #[test]
    fn plain_sql_is_reported_as_unsupported_not_as_a_table() {
        let err = decode_presto_view("SELECT * FROM t").unwrap_err();
        assert!(matches!(err, HiveError::Validation(_)));
        assert!(err.to_string().contains("Hive-native views are not supported"), "{err}");
    }

    #[test]
    fn corrupt_payloads_name_the_failure() {
        let err = decode_presto_view("/* Presto View: !!! */").unwrap_err();
        assert!(err.to_string().contains("base64"), "{err}");

        let text = format!(
            "/* Presto View: {} */",
            base64::engine::general_purpose::STANDARD.encode("not json")
        );
        let err = decode_presto_view(&text).unwrap_err();
        assert!(err.to_string().contains("JSON"), "{err}");

        let empty = encode(r#"{"originalSql":"   "}"#);
        let err = decode_presto_view(&empty).unwrap_err();
        assert!(err.to_string().contains("empty originalSql"), "{err}");
    }
}
