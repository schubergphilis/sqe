use arrow_array::RecordBatch;
use serde::Serialize;

use crate::types::{arrow_to_trino_type, arrow_value_to_json};

// ── Trino /v1/info response ──────────────────────────────────

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfo {
    pub node_version: NodeVersion,
    pub environment: String,
    pub coordinator: bool,
    pub starting: bool,
    pub uptime: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeVersion {
    pub version: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoResponse {
    pub id: String,
    pub info_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<TrinoColumn>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<Vec<serde_json::Value>>>,
    pub stats: TrinoStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<TrinoError>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoColumn {
    pub name: String,
    pub r#type: String,
    pub type_signature: TrinoTypeSignature,
}

/// Trino column type signature — required by the Trino JDBC driver.
///
/// The driver calls `ClientTypeSignature.getRawType()` on every column;
/// if `typeSignature` is missing from the JSON the field deserializes as
/// `null` and the driver throws NPE.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoTypeSignature {
    pub raw_type: String,
    #[serde(default)]
    pub arguments: Vec<TrinoTypeArgument>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoTypeArgument {
    pub kind: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoStats {
    pub state: String,
    pub queued: bool,
    pub scheduled: bool,
    pub nodes: u32,
    pub total_splits: u32,
    pub completed_splits: u32,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoError {
    pub message: String,
    pub error_code: i32,
    pub error_name: String,
    pub error_type: String,
}

/// Build a `TrinoTypeSignature` from a Trino type string.
///
/// For parameterized types like `decimal(18,2)`, the precision and scale
/// are extracted into `arguments`. For simple types, `arguments` is empty.
pub fn type_signature_for(trino_type: &str) -> TrinoTypeSignature {
    // Handle "decimal(p,s)" → rawType "decimal", arguments [{LONG,p},{LONG,s}]
    if let Some(rest) = trino_type.strip_prefix("decimal(") {
        if let Some(params) = rest.strip_suffix(')') {
            let parts: Vec<&str> = params.split(',').collect();
            if parts.len() == 2 {
                let args: Vec<TrinoTypeArgument> = parts
                    .iter()
                    .filter_map(|p| p.trim().parse::<i64>().ok())
                    .map(|v| TrinoTypeArgument {
                        kind: "LONG".to_string(),
                        value: serde_json::json!(v),
                    })
                    .collect();
                return TrinoTypeSignature {
                    raw_type: "decimal".to_string(),
                    arguments: args,
                };
            }
        }
    }

    TrinoTypeSignature {
        raw_type: trino_type.to_string(),
        arguments: vec![],
    }
}

pub fn batches_to_trino(
    batches: &[RecordBatch],
) -> (Vec<TrinoColumn>, Vec<Vec<serde_json::Value>>) {
    if batches.is_empty() {
        return (vec![], vec![]);
    }

    let schema = batches[0].schema();

    let columns: Vec<TrinoColumn> = schema
        .fields()
        .iter()
        .map(|f| {
            let trino_type = arrow_to_trino_type(f.data_type());
            let type_signature = type_signature_for(&trino_type);
            TrinoColumn {
                name: f.name().clone(),
                r#type: trino_type,
                type_signature,
            }
        })
        .collect();

    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row: Vec<serde_json::Value> = (0..batch.num_columns())
                .map(|col_idx| arrow_value_to_json(batch.column(col_idx).as_ref(), row_idx))
                .collect();
            rows.push(row);
        }
    }

    (columns, rows)
}

impl TrinoStats {
    pub fn finished() -> Self {
        Self {
            state: "FINISHED".to_string(),
            queued: false,
            scheduled: true,
            nodes: 1,
            total_splits: 1,
            completed_splits: 1,
        }
    }

    pub fn failed() -> Self {
        Self {
            state: "FAILED".to_string(),
            queued: false,
            scheduled: true,
            nodes: 1,
            total_splits: 1,
            completed_splits: 0,
        }
    }

    /// Stats for an in-progress paginated result.
    pub fn running(completed_pages: usize, total_pages: usize) -> Self {
        Self {
            state: "RUNNING".to_string(),
            queued: false,
            scheduled: true,
            nodes: 1,
            total_splits: total_pages as u32,
            completed_splits: completed_pages as u32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn test_batches_to_trino_empty() {
        let (cols, rows) = batches_to_trino(&[]);
        assert!(cols.is_empty());
        assert!(rows.is_empty());
    }

    #[test]
    fn test_batches_to_trino_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
                Arc::new(arrow_array::StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();

        let (cols, rows) = batches_to_trino(&[batch]);

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].r#type, "bigint");
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].r#type, "varchar");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], serde_json::json!(1));
        assert_eq!(rows[0][1], serde_json::json!("alice"));
        assert_eq!(rows[1][0], serde_json::json!(2));
        assert_eq!(rows[1][1], serde_json::json!("bob"));
    }

    #[test]
    fn test_server_info_serialization() {
        let info = ServerInfo {
            node_version: NodeVersion {
                version: "0.1.0".to_string(),
            },
            environment: "production".to_string(),
            coordinator: true,
            starting: false,
            uptime: "5.00m".to_string(),
        };

        let json = serde_json::to_string(&info).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["nodeVersion"]["version"], "0.1.0");
        assert_eq!(parsed["environment"], "production");
        assert_eq!(parsed["coordinator"], true);
        assert_eq!(parsed["starting"], false);
        assert_eq!(parsed["uptime"], "5.00m");
    }

    #[test]
    fn test_server_info_starting_state() {
        let info = ServerInfo {
            node_version: NodeVersion {
                version: "0.1.0".to_string(),
            },
            environment: "production".to_string(),
            coordinator: true,
            starting: true,
            uptime: "0.00s".to_string(),
        };

        let parsed: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&info).unwrap()).unwrap();
        assert_eq!(parsed["starting"], true);
    }

    #[test]
    fn test_trino_response_serialization() {
        let resp = TrinoResponse {
            id: "q-001".to_string(),
            info_uri: None,
            next_uri: None,
            columns: Some(vec![TrinoColumn {
                name: "x".to_string(),
                r#type: "bigint".to_string(),
                type_signature: type_signature_for("bigint"),
            }]),
            data: Some(vec![vec![serde_json::json!(1)]]),
            stats: TrinoStats::finished(),
            error: None,
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"id\":\"q-001\""));
        assert!(json.contains("\"state\":\"FINISHED\""));
        assert!(!json.contains("nextUri")); // Skipped because None
        assert!(json.contains("\"typeSignature\""), "typeSignature must be present, got: {json}");
        assert!(json.contains("\"rawType\":\"bigint\""), "rawType must be present, got: {json}");
    }

    #[test]
    fn test_trino_response_always_includes_info_uri() {
        let resp = TrinoResponse {
            id: "q-001".to_string(),
            info_uri: None,
            next_uri: None,
            columns: None,
            data: None,
            stats: TrinoStats::finished(),
            error: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"infoUri\":null"), "infoUri must always be present, got: {json}");
    }
}
