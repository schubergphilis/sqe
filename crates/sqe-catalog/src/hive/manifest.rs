//! The sidecar manifest: a Glue-`TableInput`-shaped JSON document that holds
//! a Hive-style external table's definition in object storage, replacing a
//! metastore. Field names mirror Glue's `TableInput` and `StorageDescriptor`
//! exactly, so a later Glue or Hive Metastore backend is a field mapping
//! rather than a redesign.
//!
//! The manifest is untrusted input: anyone who can write to the warehouse
//! prefix can plant one. [`HiveManifest::parse`] treats it that way. It
//! checks the byte size against [`ManifestLimits::max_bytes`] before
//! deserializing anything, rejects a `manifest_version` newer than this
//! release understands rather than parsing it best-effort, and only then
//! checks the count caps and field validity.

use std::collections::BTreeMap;

use sqe_sql::hive_ddl::HiveTableSpec;

use super::HiveError;

/// The manifest format version this release writes and the highest version
/// it will read. A manifest written by a newer engine that bumped this
/// number is rejected outright: parsing it best-effort could silently drop a
/// field this release does not know about yet.
pub const MANIFEST_VERSION: u32 = 1;

/// Caps applied to a manifest before it is trusted. `Default` gives the
/// hard-coded values; Task 6 exposes them as `[hive_external]` config so an
/// operator can tighten or loosen them.
#[derive(Debug, Clone, Copy)]
pub struct ManifestLimits {
    pub max_bytes: usize,
    pub max_columns: usize,
    pub max_partition_keys: usize,
}

impl Default for ManifestLimits {
    fn default() -> Self {
        ManifestLimits {
            max_bytes: 1 << 20,
            max_columns: 4096,
            max_partition_keys: 32,
        }
    }
}

/// A Glue-style column: `StorageDescriptor.Columns[]` and
/// `TableInput.PartitionKeys[]` both use this shape.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GlueColumn {
    pub name: String,
    pub r#type: String,
    #[serde(default)]
    pub comment: Option<String>,
}

/// Mirrors Glue's `SerDeInfo`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SerdeInfo {
    #[serde(default)]
    pub serialization_library: Option<String>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

/// Mirrors Glue's `StorageDescriptor`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StorageDescriptor {
    pub location: String,
    #[serde(default)]
    pub input_format: Option<String>,
    #[serde(default)]
    pub output_format: Option<String>,
    #[serde(default)]
    pub compressed: bool,
    #[serde(default)]
    pub serde_info: SerdeInfo,
    #[serde(default)]
    pub columns: Vec<GlueColumn>,
    #[serde(default)]
    pub sort_columns: Vec<String>,
}

/// SQE-specific extensions that Glue's `TableInput` has no field for. Kept
/// in their own nested block, rather than mixed into `parameters`, so an
/// eventual Glue backend can drop this block wholesale instead of picking
/// SQE keys out of a Glue-owned map.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SqeExtensions {
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default)]
    pub column_tags: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub partition_index: Option<serde_json::Value>,
    #[serde(default)]
    pub statistics: Option<serde_json::Value>,
}

/// The manifest itself: a Glue-`TableInput`-shaped document, stored at
/// `<root>/<database>/<table>/_sqe/table.json`.
///
/// `deny_unknown_fields` is deliberately not used anywhere in this module: a
/// field written by a newer engine version must stay ignorable so an older
/// engine can still read the manifest. What *is* enforced is
/// `manifest_version`, which is checked explicitly in [`HiveManifest::parse`]
/// rather than left to serde.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HiveManifest {
    pub manifest_version: u32,
    pub table_type: String,
    pub database_name: String,
    pub name: String,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub partition_keys: Vec<GlueColumn>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    pub storage_descriptor: StorageDescriptor,
    #[serde(default)]
    pub sqe: SqeExtensions,
}

/// `TEXTFILE`/`PARQUET` etc. as given by `STORED AS`, lowered onto the Hadoop
/// input/output format classes Hive itself would use. Only engaged when the
/// spec gave `stored_as` but not an explicit `INPUTFORMAT`/`OUTPUTFORMAT`
/// pair.
fn formats_from_stored_as(stored_as: &str) -> (Option<String>, Option<String>) {
    match stored_as.to_ascii_uppercase().as_str() {
        "TEXTFILE" => (
            Some("org.apache.hadoop.mapred.TextInputFormat".to_string()),
            Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat".to_string()),
        ),
        "PARQUET" => (
            Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".to_string()),
            Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat".to_string()),
        ),
        _ => (None, None),
    }
}

fn glue_column_of(c: &sqe_sql::hive_ddl::HiveColumn) -> GlueColumn {
    GlueColumn {
        name: c.name.clone(),
        r#type: c.sql_type.clone(),
        comment: c.comment.clone(),
    }
}

impl HiveManifest {
    /// Build a manifest from a freshly-parsed `CREATE EXTERNAL TABLE` spec.
    /// `database` and `owner` come from the session (the spec's own
    /// `database` field may be absent if the statement did not qualify the
    /// name), and `created_at` is caller-supplied so this stays a pure
    /// function of its inputs.
    pub fn from_spec(spec: &HiveTableSpec, database: &str, owner: &str, created_at: &str) -> Self {
        let (derived_input, derived_output) = spec
            .stored_as
            .as_deref()
            .map(formats_from_stored_as)
            .unwrap_or((None, None));

        HiveManifest {
            manifest_version: MANIFEST_VERSION,
            table_type: "EXTERNAL_TABLE".to_string(),
            database_name: database.to_string(),
            name: spec.name.clone(),
            owner: Some(owner.to_string()),
            created_at: Some(created_at.to_string()),
            comment: spec.comment.clone(),
            partition_keys: spec.partition_keys.iter().map(glue_column_of).collect(),
            // Copied verbatim so `sqe.secret` and any other property (e.g.
            // `classification`) both survive the round trip untouched.
            parameters: spec.table_properties.clone(),
            storage_descriptor: StorageDescriptor {
                location: spec.location.clone(),
                input_format: spec.input_format.clone().or(derived_input),
                output_format: spec.output_format.clone().or(derived_output),
                compressed: false,
                serde_info: SerdeInfo {
                    serialization_library: spec.serde_class.clone(),
                    parameters: spec.serde_properties.clone(),
                },
                columns: spec.columns.iter().map(glue_column_of).collect(),
                sort_columns: Vec::new(),
            },
            // `secret` is left `None` here: `parameters` already carries
            // `sqe.secret` verbatim from `table_properties`, and `secret()`
            // reads that first. Writing it into both places would give a
            // hand-edited `parameters["sqe.secret"]` a stale twin in `sqe`
            // that silently disagrees with it.
            sqe: SqeExtensions {
                secret: None,
                column_tags: BTreeMap::new(),
                partition_index: None,
                statistics: None,
            },
        }
    }

    /// Parse and validate an untrusted manifest. Order matters:
    ///
    /// 1. `bytes.len()` against `limits.max_bytes`, before anything is
    ///    deserialized: the cap exists to reject an oversized input, which it
    ///    cannot do if it runs after the input has already been parsed.
    /// 2. Deserialize.
    /// 3. `manifest_version` against [`MANIFEST_VERSION`]: a higher version
    ///    is rejected outright rather than parsed best-effort, since a field
    ///    this release does not know about could change meaning silently.
    /// 4. The count caps (`max_columns`, `max_partition_keys`).
    /// 5. Field validity (non-empty `location`/`name`/`database_name`, no
    ///    column name shared between `columns` and `partition_keys`, and
    ///    `table_type == "EXTERNAL_TABLE"`).
    pub fn parse(bytes: &[u8], limits: &ManifestLimits) -> Result<Self, HiveError> {
        if bytes.len() > limits.max_bytes {
            return Err(HiveError::Validation(format!(
                "manifest size {} bytes exceeds the {} byte cap",
                bytes.len(),
                limits.max_bytes
            )));
        }

        let manifest: HiveManifest = serde_json::from_slice(bytes)
            .map_err(|e| HiveError::Validation(format!("malformed manifest: {e}")))?;

        if manifest.manifest_version > MANIFEST_VERSION {
            return Err(HiveError::Validation(format!(
                "manifest_version {} is newer than this engine understands (max {})",
                manifest.manifest_version, MANIFEST_VERSION
            )));
        }

        if manifest.storage_descriptor.columns.len() > limits.max_columns {
            return Err(HiveError::Validation(format!(
                "manifest has {} columns, exceeding the cap of {}",
                manifest.storage_descriptor.columns.len(),
                limits.max_columns
            )));
        }
        if manifest.partition_keys.len() > limits.max_partition_keys {
            return Err(HiveError::Validation(format!(
                "manifest has {} partition keys, exceeding the cap of {}",
                manifest.partition_keys.len(),
                limits.max_partition_keys
            )));
        }

        if manifest.storage_descriptor.location.is_empty() {
            return Err(HiveError::Validation(
                "storage_descriptor.location must not be empty".to_string(),
            ));
        }
        if manifest.name.is_empty() {
            return Err(HiveError::Validation("name must not be empty".to_string()));
        }
        if manifest.database_name.is_empty() {
            return Err(HiveError::Validation(
                "database_name must not be empty".to_string(),
            ));
        }
        if manifest.table_type != "EXTERNAL_TABLE" {
            return Err(HiveError::Validation(format!(
                "table_type must be EXTERNAL_TABLE, got `{}`",
                manifest.table_type
            )));
        }
        for key in &manifest.partition_keys {
            if manifest
                .storage_descriptor
                .columns
                .iter()
                .any(|c| c.name == key.name)
            {
                return Err(HiveError::Validation(format!(
                    "column `{}` appears in both storage_descriptor.columns and partition_keys",
                    key.name
                )));
            }
        }

        Ok(manifest)
    }

    /// Serialize to the JSON bytes written to the sidecar object.
    pub fn to_bytes(&self) -> Result<Vec<u8>, HiveError> {
        serde_json::to_vec_pretty(self)
            .map_err(|e| HiveError::Validation(format!("failed to serialize manifest: {e}")))
    }

    /// The mount secret name, if any. Checks `parameters["sqe.secret"]`
    /// first (how `TBLPROPERTIES ('sqe.secret' = ...)` lands via
    /// [`HiveManifest::from_spec`]), then falls back to `sqe.secret` in the
    /// `sqe` extension block, so a manifest written by hand either way
    /// works.
    pub fn secret(&self) -> Option<&str> {
        self.parameters
            .get("sqe.secret")
            .map(String::as_str)
            .or(self.sqe.secret.as_deref())
    }
}
