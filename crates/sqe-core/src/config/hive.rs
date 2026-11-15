use serde::Deserialize;

/// One Hive-style external-table warehouse root to attach at coordinator
/// startup, the `[[hive.roots]]` TOML equivalent of `ATTACH <location> AS
/// <name> (TYPE hive_external)`.
#[derive(Debug, Deserialize, Clone)]
pub struct HiveRootConfig {
    /// SQL identifier the root is exposed as (`<name>.<database>.<table>`).
    pub name: String,
    /// Where this root's table metadata comes from. `sidecar` (the default)
    /// reads `_sqe/table.json` under `location`; `glue` reads the AWS Glue
    /// Data Catalog and ignores `location` entirely.
    #[serde(default)]
    pub kind: HiveRootKind,
    /// Warehouse root location (`s3://bucket/prefix/`, a local path, ...).
    /// Required for `sidecar`, unused for `glue`.
    #[serde(default)]
    pub location: String,
    /// `glue` only: the AWS region holding the catalog. Falls back to the
    /// ambient AWS configuration when unset.
    #[serde(default)]
    pub region: Option<String>,
    /// `glue` only: another account's Glue `CatalogId`. `None` is the
    /// caller's own account, which is what almost every deployment wants.
    #[serde(default)]
    pub catalog_id: Option<String>,
    /// `glue` only: restrict the root to these databases. Empty (the
    /// default) exposes every database in the catalog. When set,
    /// `SHOW SCHEMAS` lists only these and a table in any other database is
    /// not found, so a shared catalog can be scoped to the databases one
    /// deployment should see.
    #[serde(default)]
    pub databases: Vec<String>,
    /// Optional named secret (see `CREATE SECRET`) used when a manifest
    /// under this root does not name its own `sqe.secret`. Currently
    /// unused by `build_hive_root`/`session_hive_catalog`, which resolve a
    /// secret from the manifest itself; reserved for a root-level default.
    #[serde(default)]
    pub secret: Option<String>,
    /// Where this root's table and partition data may live, added to
    /// `[hive] allowed_data_prefixes` for this root only (#886). Same
    /// grammar as `[storage.tvf] allowed_object_store_prefixes`, `{user}`
    /// and `{catalog}` included.
    #[serde(default)]
    pub data_prefixes: Vec<String>,
}

/// Where a `[[hive.roots]]` entry reads its table metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HiveRootKind {
    /// `_sqe/table.json` sidecars under the root location, written by SQE.
    #[default]
    Sidecar,
    /// The AWS Glue Data Catalog, read live. SQE does not own it, so DDL
    /// against this root is refused rather than silently dropped.
    Glue,
}

fn default_hive_manifest_max_bytes() -> usize {
    1 << 20
}

fn default_hive_manifest_max_columns() -> usize {
    4096
}

fn default_hive_manifest_max_partition_keys() -> usize {
    32
}

fn default_hive_manifest_max_partition_index_entries() -> usize {
    100_000
}

fn default_hive_metadata_recheck_secs() -> u64 {
    10
}

fn default_hive_metadata_ttl_secs() -> u64 {
    600
}

fn default_hive_metadata_timeout_secs() -> u64 {
    10
}

/// `[hive]`: warehouse roots to attach at coordinator startup, plus the
/// manifest size/shape caps enforced on every root. The four caps default
/// to the exact values `hive::manifest::ManifestLimits::default()` uses
/// (`1048576` bytes, `4096` columns, `32` partition keys, `100000` index
/// entries); a mismatch between the config default and the code default
/// would be a config surface that silently lies about what it controls.
#[derive(Debug, Deserialize, Clone)]
pub struct HiveConfig {
    #[serde(default)]
    pub roots: Vec<HiveRootConfig>,
    /// Where Hive table and partition data may live, for every root
    /// (#886). File TVFs refuse paths under these prefixes, whoever asks.
    /// With neither this nor any root's `data_prefixes` set, Hive locations
    /// fall back to `[storage.tvf] allowed_object_store_prefixes`
    /// (deprecated; removed in the next release).
    #[serde(default)]
    pub allowed_data_prefixes: Vec<String>,
    #[serde(default = "default_hive_manifest_max_bytes")]
    pub manifest_max_bytes: usize,
    #[serde(default = "default_hive_manifest_max_columns")]
    pub manifest_max_columns: usize,
    #[serde(default = "default_hive_manifest_max_partition_keys")]
    pub manifest_max_partition_keys: usize,
    #[serde(default = "default_hive_manifest_max_partition_index_entries")]
    pub manifest_max_partition_index_entries: usize,
    /// How long a Hive table's cached manifest is served without asking the
    /// metastore whether it changed (one HEAD per table per lookup
    /// otherwise). Every DDL and write in this process invalidates the
    /// entry directly, so the window only delays what another writer
    /// changed. `0` checks on every lookup. Default 10, matching
    /// `[catalog] authorization_recheck_secs`.
    #[serde(default = "default_hive_metadata_recheck_secs")]
    pub metadata_recheck_secs: u64,
    /// How long a cached manifest from a metastore with no change token
    /// (Glue) is served before it is fetched again. A sidecar root has
    /// ETags and revalidates instead, so this only bounds Glue-backed
    /// roots. A refetch that returns an unchanged definition keeps the
    /// table's cached file facts, so the TTL mostly bounds `GetTables`
    /// round trips and how long another engine's schema change goes
    /// unseen; the `refresh_metadata` paths drop entries on demand. The
    /// same window bounds the shared database and table-name listings.
    /// Default 600.
    #[serde(default = "default_hive_metadata_ttl_secs")]
    pub metadata_ttl_secs: u64,
    /// Upper bound on one metastore call (Glue `GetTable`/`GetTables`, a
    /// sidecar manifest read). Past it the provider serves the last known
    /// manifest with a warning when it has one, or fails the query with a
    /// named "catalog metadata timeout" error when it does not; a listing
    /// refresh keeps its previous snapshot. `0` disables the bound.
    /// Default 10.
    #[serde(default = "default_hive_metadata_timeout_secs")]
    pub metadata_timeout_secs: u64,
}

impl Default for HiveConfig {
    fn default() -> Self {
        Self {
            roots: Vec::new(),
            allowed_data_prefixes: Vec::new(),
            manifest_max_bytes: default_hive_manifest_max_bytes(),
            manifest_max_columns: default_hive_manifest_max_columns(),
            manifest_max_partition_keys: default_hive_manifest_max_partition_keys(),
            manifest_max_partition_index_entries: default_hive_manifest_max_partition_index_entries(
            ),
            metadata_recheck_secs: default_hive_metadata_recheck_secs(),
            metadata_ttl_secs: default_hive_metadata_ttl_secs(),
            metadata_timeout_secs: default_hive_metadata_timeout_secs(),
        }
    }
}
