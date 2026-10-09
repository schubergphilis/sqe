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

/// The index format version this release writes and the highest version it
/// will read, checked the same way [`MANIFEST_VERSION`] is: a newer index
/// inside a known manifest version is rejected rather than parsed
/// best-effort. The outer `manifest_version` stays 1 because this slot was
/// reserved from the first write; only its *contents* are versioned here.
pub const PARTITION_INDEX_FORMAT_VERSION: u32 = 1;

/// Hive's sentinel for a partition whose value is NULL. Stored verbatim in
/// an index entry's `values` map and normalized to NULL on read.
pub const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Caps applied to a manifest before it is trusted. `Default` gives the
/// hard-coded values; Task 6 exposes them as `[hive_external]` config so an
/// operator can tighten or loosen them.
#[derive(Debug, Clone, Copy)]
pub struct ManifestLimits {
    pub max_bytes: usize,
    pub max_columns: usize,
    pub max_partition_keys: usize,
    /// Bounds `sqe.partition_index.entries` at ADD time, REPAIR time, and
    /// load. Enforced on load because the manifest is untrusted input: a
    /// planted million-entry index must fail closed, not OOM the reader.
    pub max_partition_index_entries: usize,
}

impl Default for ManifestLimits {
    fn default() -> Self {
        ManifestLimits {
            max_bytes: 1 << 20,
            max_columns: 4096,
            max_partition_keys: 32,
            max_partition_index_entries: 100_000,
        }
    }
}

/// One persisted partition: its key values and where its files live. The
/// location is carried per entry because a Hive partition may live outside
/// the table prefix (`ALTER TABLE ... ADD PARTITION ... LOCATION 's3://...'`),
/// which directory-name discovery cannot express at all.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionIndexEntry {
    /// Partition key name to raw string value, as written in the path or the
    /// DDL. Typed on read against the declared partition-key types.
    pub values: BTreeMap<String, String>,
    /// Where this partition's files live. Validated through the
    /// `[storage.tvf]` allowlist on every load, exactly like
    /// `storage_descriptor.location`.
    pub location: String,
    /// File count under this location, captured when the entry was added or
    /// repaired. Feeds size-only statistics so planning does not list.
    #[serde(default)]
    pub file_count: u64,
    /// Total bytes under this location, captured alongside `file_count`.
    #[serde(default)]
    pub total_bytes: u64,
}

/// The persisted partition list in `sqe.partition_index`. Present means
/// reads plan from it instead of listing the table location; absent keeps
/// cycle-1 discovery behavior.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionIndex {
    pub format_version: u32,
    #[serde(default)]
    pub entries: Vec<PartitionIndexEntry>,
}

/// A Glue-style column: `StorageDescriptor.Columns[]` and
/// `TableInput.PartitionKeys[]` both use this shape.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GlueColumn {
    pub name: String,
    pub r#type: String,
    #[serde(default)]
    pub comment: Option<String>,
    /// Glue's `Column.Parameters`. Carries a column's `sqe.column-tags`
    /// entry (see [`HiveManifest::column_tags`]); a Glue mapping that dropped
    /// this map would report a tagged column as untagged.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, String>,
}

/// Table and column parameter key holding column tags, the same key the
/// Iceberg path reads from table properties.
pub const COLUMN_TAGS_KEY: &str = "sqe.column-tags";

/// Mirrors Glue's `SerDeInfo`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SerdeInfo {
    #[serde(default)]
    pub serialization_library: Option<String>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

/// Mirrors Glue's `StorageDescriptor`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SqeExtensions {
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default)]
    pub column_tags: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub partition_index: Option<PartitionIndex>,
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
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HiveManifest {
    pub manifest_version: u32,
    pub table_type: String,
    pub database_name: String,
    pub name: String,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    /// Glue's `TableInput` calls this `Description`, not `Comment` (that
    /// name is reserved for the column-level field on `GlueColumn`), so this
    /// field mirrors `description` to keep the promise that a later Glue or
    /// HMS backend is a field mapping rather than a redesign.
    #[serde(default)]
    pub description: Option<String>,
    /// Glue's `ViewOriginalText`: the encoded view definition when
    /// `table_type` is `VIRTUAL_VIEW` (see `athena_view.rs`), `None` for a
    /// storage table. Sidecar manifests never write it; it exists so a Glue
    /// view is a field mapping like every other Glue attribute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_original_text: Option<String>,
    #[serde(default)]
    pub partition_keys: Vec<GlueColumn>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    pub storage_descriptor: StorageDescriptor,
    #[serde(default)]
    pub sqe: SqeExtensions,
}

/// `TEXTFILE`/`PARQUET`/etc. as given by `STORED AS`, lowered onto the real
/// Hadoop input/output format classes Hive itself records for that keyword
/// (verified against Hive's own `SHOW CREATE TABLE` output). Only engaged
/// when the spec gave `stored_as` but not an explicit
/// `INPUTFORMAT`/`OUTPUTFORMAT` pair.
///
/// Every standard keyword is covered, not just the two formats this release
/// can read: an ORC or Avro table that fell through this match as `(None,
/// None)` would forget its own format the moment it round-tripped through
/// the manifest, which breaks the promise that a later Glue or HMS backend
/// is a field mapping rather than a redesign. `kind_from_input_format` in
/// `serde_map.rs` is what actually restricts cycle 1 to csv/json/parquet; it
/// rejects every other class by the name recorded here, so a table stored as
/// ORC gets a message naming `OrcInputFormat`, not a class this code made up.
///
/// `_ => (None, None)` is kept for a keyword this function does not
/// recognize at all, so an unknown future keyword still fails loudly
/// downstream instead of being silently renamed into a known class.
///
/// Against the pinned sqlparser-rs 0.62, this arm is provably unreachable:
/// its `FileFormat` enum has exactly seven variants, and all seven are
/// matched above (`TEXTFILE`, `PARQUET`, `ORC`, `AVRO`, `SEQUENCEFILE`,
/// `RCFILE`, `JSONFILE`; bare `JSON` does not parse at all, see the comment
/// on that arm). Breadcrumb for whoever bumps sqlparser next: if a future
/// version adds an eighth `FileFormat` variant, a table declared with it
/// falls through to this arm and fails downstream in `kind_from_input_format`
/// with a message that does NOT name the offending keyword, because nothing
/// retains the raw `STORED AS` text once it falls through unmapped. Add the
/// new variant's real Hadoop classes here rather than leaving it caught by
/// this arm.
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
        "ORC" => (
            Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat".to_string()),
            Some("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat".to_string()),
        ),
        "AVRO" => (
            Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat".to_string()),
            Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat".to_string()),
        ),
        "SEQUENCEFILE" => (
            Some("org.apache.hadoop.mapred.SequenceFileInputFormat".to_string()),
            Some("org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat".to_string()),
        ),
        "RCFILE" => (
            Some("org.apache.hadoop.hive.ql.io.RCFileInputFormat".to_string()),
            Some("org.apache.hadoop.hive.ql.io.RCFileOutputFormat".to_string()),
        ),
        // Not a native Hive storage keyword (Hive's own grammar has no
        // `STORED AS JSON`; sqlparser-rs itself rejects it, only `JSONFILE`
        // parses), but Athena/Glue tooling writes tables this way, and
        // `JSONFILE` does reach here. Both fall onto TextInputFormat, same
        // as TEXTFILE; what makes it a JSON table is the default SerDe class
        // `from_spec` attaches below when no explicit one was given.
        "JSON" | "JSONFILE" => (
            Some("org.apache.hadoop.mapred.TextInputFormat".to_string()),
            Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat".to_string()),
        ),
        _ => (None, None),
    }
}

/// The SerDe class Glue would record for a `STORED AS` keyword when the DDL
/// gave no explicit `ROW FORMAT SERDE`. Only JSON and Parquet need this:
/// Glue's crawler always attaches a SerDe class to those two, where TEXTFILE
/// legitimately has none (delimited text needs no SerDe beyond
/// `LazySimpleSerDe`, which `serde_map.rs` already defaults to). An explicit
/// `ROW FORMAT SERDE` in the DDL always wins; see the `.or(...)` at the call
/// site in [`HiveManifest::from_spec`].
fn default_serde_for_stored_as(stored_as: &str) -> Option<String> {
    match stored_as.to_ascii_uppercase().as_str() {
        "JSON" | "JSONFILE" => Some("org.openx.data.jsonserde.JsonSerDe".to_string()),
        "PARQUET" => {
            Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe".to_string())
        }
        _ => None,
    }
}

fn glue_column_of(c: &sqe_sql::hive_ddl::HiveColumn) -> GlueColumn {
    GlueColumn {
        name: c.name.clone(),
        r#type: c.sql_type.clone(),
        comment: c.comment.clone(),
        parameters: BTreeMap::new(),
    }
}

impl HiveManifest {
    /// True for an Athena / Trino view (`TableType = VIRTUAL_VIEW`); every
    /// other table type is a storage table.
    pub fn is_view(&self) -> bool {
        self.table_type == super::athena_view::VIRTUAL_VIEW
    }

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
        let derived_serde = spec
            .stored_as
            .as_deref()
            .and_then(default_serde_for_stored_as);

        HiveManifest {
            manifest_version: MANIFEST_VERSION,
            table_type: "EXTERNAL_TABLE".to_string(),
            database_name: database.to_string(),
            name: spec.name.clone(),
            owner: Some(owner.to_string()),
            created_at: Some(created_at.to_string()),
            description: spec.comment.clone(),
            view_original_text: None,
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
                    // An explicit ROW FORMAT SERDE always wins over the
                    // STORED AS default, as Hive does.
                    serialization_library: spec.serde_class.clone().or(derived_serde),
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

        if let Some(index) = &manifest.sqe.partition_index {
            validate_partition_index(
                index,
                &manifest.partition_keys,
                limits.max_partition_index_entries,
            )?;
        }

        // Projection config is untrusted input, same as SerDe classes: a
        // planted manifest with projection.dt.type=banana must fail closed
        // rather than being half-interpreted into a full scan.
        super::projection::parse_projection(&manifest.parameters, &manifest.partition_keys)?;

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

    /// Every column's tags, the union of three places a tag can live:
    ///
    /// - `sqe.column_tags`, written by `ALTER TABLE ... SET TAGS` on a
    ///   sidecar root;
    /// - `parameters["sqe.column-tags"]`, a JSON object `{"col": ["TAG"]}`,
    ///   the shape the Iceberg path reads from table properties;
    /// - each storage column's and partition key's own
    ///   `parameters["sqe.column-tags"]`, a JSON array `["TAG", ...]`, which
    ///   is where a Glue catalog carries them.
    ///
    /// A union, never a precedence: a tag can only add a mask. An absent key
    /// is known-untagged. A value that does not parse is `Err`, meaning the
    /// tag state is unknown: callers must deny rather than read the table as
    /// untagged, or a malformed entry would unmask the column it tags.
    pub fn column_tags(&self) -> Result<BTreeMap<String, Vec<String>>, HiveError> {
        let mut tags = self.sqe.column_tags.clone();
        let mut add = |column: &str, labels: Vec<String>| {
            let entry = tags.entry(column.to_string()).or_default();
            for label in labels {
                if !entry.contains(&label) {
                    entry.push(label);
                }
            }
        };
        if let Some(raw) = self.parameters.get(COLUMN_TAGS_KEY) {
            let map: BTreeMap<String, Vec<String>> = serde_json::from_str(raw).map_err(|e| {
                HiveError::Validation(format!(
                    "table parameter `{COLUMN_TAGS_KEY}` is not a JSON object of tag arrays: {e}"
                ))
            })?;
            for (column, labels) in map {
                add(&column, labels);
            }
        }
        for column in self
            .storage_descriptor
            .columns
            .iter()
            .chain(&self.partition_keys)
        {
            if let Some(raw) = column.parameters.get(COLUMN_TAGS_KEY) {
                let labels: Vec<String> = serde_json::from_str(raw).map_err(|e| {
                    HiveError::Validation(format!(
                        "column `{}` parameter `{COLUMN_TAGS_KEY}` is not a JSON array of tags: {e}",
                        column.name
                    ))
                })?;
                add(&column.name, labels);
            }
        }
        Ok(tags)
    }

    /// Persisted `ANALYZE TABLE` counts, if any.
    pub fn hive_statistics(&self) -> Option<HiveTableStatistics> {
        self.sqe
            .statistics
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }
}

/// `sqe.statistics` payload written by `ANALYZE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HiveTableStatistics {
    pub row_count: u64,
    #[serde(default)]
    pub entries: Vec<HivePartitionStatistics>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HivePartitionStatistics {
    pub values: BTreeMap<String, String>,
    pub row_count: u64,
}

/// Validate an untrusted `sqe.partition_index` against the manifest's own
/// partition-key declarations. Runs on every load, same rule as the
/// `storage_descriptor.location` check: anyone who can write to the
/// warehouse prefix can plant an index, so its contents are network input,
/// not engine-authored state.
fn validate_partition_index(
    index: &PartitionIndex,
    partition_keys: &[GlueColumn],
    max_entries: usize,
) -> Result<(), HiveError> {
    if index.format_version > PARTITION_INDEX_FORMAT_VERSION {
        return Err(HiveError::Validation(format!(
            "partition index format_version {} is newer than this engine understands (max {})",
            index.format_version, PARTITION_INDEX_FORMAT_VERSION
        )));
    }
    if index.entries.len() > max_entries {
        return Err(HiveError::Validation(format!(
            "partition index has {} entries, exceeding the cap of {}. Raise \
             [hive] manifest_max_partition_index_entries if the table genuinely has this many.",
            index.entries.len(),
            max_entries
        )));
    }

    for entry in &index.entries {
        if entry.location.is_empty() {
            return Err(HiveError::Validation(
                "partition index entry location must not be empty".to_string(),
            ));
        }

        // Exact set match: a partial value set is a malformed index. A read
        // that prunes on a key the entry omits would silently widen to
        // "unknown", and silently widening is how a stale or planted index
        // becomes a wrong answer instead of a missing one.
        let declared: std::collections::BTreeSet<&str> =
            partition_keys.iter().map(|k| k.name.as_str()).collect();
        let given: std::collections::BTreeSet<&str> = entry.values.keys().map(String::as_str).collect();
        if declared != given {
            let missing: Vec<&str> = declared.difference(&given).copied().collect();
            let extra: Vec<&str> = given.difference(&declared).copied().collect();
            return Err(HiveError::Validation(format!(
                "partition index entry values do not match the declared partition keys \
                 (missing: {missing:?}, unexpected: {extra:?})"
            )));
        }

        // The sentinel is NULL by definition and skips typed parsing.
        for (key, value) in &entry.values {
            if value == HIVE_DEFAULT_PARTITION {
                continue;
            }
            let declared_type = partition_keys
                .iter()
                .find(|k| &k.name == key)
                .map(|k| k.r#type.as_str())
                .unwrap_or_default();
            parse_typed_partition_value(value, declared_type).map_err(|e| {
                HiveError::Validation(format!("partition index entry `{key}={value}`: {e}"))
            })?;
        }
    }

    Ok(())
}

/// Check a raw partition-value string against the declared partition-key
/// type. Partition keys are scalar-only in this release; the supported set
/// is cycle-1 path discovery's (`string`/`varchar`/`int`/`bigint`/`date`),
/// with anything else accepted only when it parses as a plain string.
pub(crate) fn parse_typed_partition_value(
    raw: &str,
    sql_type: &str,
) -> Result<(), HiveError> {
    let base = sql_type.trim().to_ascii_lowercase();
    let base = base.split('(').next().unwrap_or(&base).trim();

    match base {
        "string" | "varchar" | "char" => Ok(()),
        "tinyint" | "smallint" | "int" | "integer" | "bigint" => raw.parse::<i64>().map(|_| ()).map_err(
            |_| HiveError::Validation(format!(
                "`{raw}` does not parse as {sql_type}"
            )),
        ),
        "date" => chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
            .map(|_| ())
            .map_err(|_| {
                HiveError::Validation(format!(
                    "`{raw}` does not parse as {sql_type} (expected YYYY-MM-DD)"
                ))
            }),
        other => Err(HiveError::Validation(format!(
            "`{other}` is not a supported partition-key type in this release"
        ))),
    }
}
