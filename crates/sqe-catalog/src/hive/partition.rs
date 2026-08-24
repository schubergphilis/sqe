//! Partition-index maintenance: `ALTER TABLE ADD/DROP PARTITION`,
//! `MSCK REPAIR TABLE`, and index-driven partition listing.
//!
//! The index lives in the manifest's reserved `sqe.partition_index` slot
//! (design D1): each entry carries the partition key values AND the
//! location, because a Hive partition may live outside the table prefix.
//! Present index means reads plan from it; absent keeps cycle-1 discovery.
//!
//! Staleness model (design D2): a stale index omits, never invents. Nothing
//! here validates entries against storage at read time -- that would
//! reintroduce the listing cost the index exists to remove. `MSCK REPAIR
//! TABLE` is the recovery path, and every mutation is a whole-manifest
//! rewrite (one object per table, last-writer-wins where the store has no
//! conditional put, the same documented semantics as CREATE).

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::datasource::listing::ListingTableUrl;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt};

use super::manifest::{
    parse_typed_partition_value, GlueColumn, HiveManifest, HivePartitionStatistics,
    HiveTableStatistics, PartitionIndex, PartitionIndexEntry, HIVE_DEFAULT_PARTITION,
    PARTITION_INDEX_FORMAT_VERSION,
};
use super::projection::projection_enabled;
use super::serde_map::{resolve_format, HiveFileFormat};
use super::store::HiveManifestStore;
use super::HiveError;

pub use super::projection::{
    evaluate_projection, parse_projection, ProjectionConfig, ProjectionKind,
};
pub use super::prune::{prune_entries, prune_entries_owned};

/// One partition to add or drop, as parsed from the DDL.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionSpec {
    /// (key, value) pairs in declaration order; typed against the
    /// manifest's declared partition-key types before use.
    pub values: Vec<(String, String)>,
    /// `ADD PARTITION ... LOCATION '...'`. `None` derives the standard
    /// `k=v/...` layout under the table location.
    pub location: Option<String>,
}

/// A partition for `SHOW PARTITIONS`: the rendered Hive-style spec
/// (`dt=2026-01-02/region=eu`) plus whether it came from the index or from
/// a storage listing.
#[derive(Debug, Clone, PartialEq)]
pub struct ListedPartition {
    pub spec: String,
}

/// `ALTER TABLE ... ADD PARTITION`: append one entry and rewrite the
/// manifest. Never touches data files and never lists other partitions.
///
/// File count and bytes come from LISTing the new partition's own location,
/// so size-only statistics stay accurate without a per-query listing later.
/// A duplicate without `if_not_exists` is an error naming the partition;
/// with it, a no-op.
pub async fn add_partition(
    store: &HiveManifestStore,
    manifest: &mut HiveManifest,
    spec: &PartitionSpec,
    if_not_exists: bool,
) -> Result<(), HiveError> {
    let values = typed_values(manifest, &spec.values)?;
    if projection_enabled(&manifest.parameters) {
        return Err(HiveError::Validation(format!(
            "ALTER TABLE ... ADD PARTITION is not supported on {}.{} because partition \
             projection owns the partition set; change the projection.* TBLPROPERTIES instead",
            manifest.database_name, manifest.name
        )));
    }
    let location = match &spec.location {
        Some(loc) => loc.clone(),
        None => derive_location(
            &manifest.storage_descriptor.location,
            &manifest.partition_keys,
            &values,
        )?,
    };

    // Inspect without taking: a duplicate (error or IF NOT EXISTS no-op)
    // must leave the in-memory manifest's index intact. Taking first and
    // returning early dropped the index from `manifest` while the store
    // still held the previous write, so the next mutation on the same
    // handle started from an empty index.
    if let Some(existing) = manifest
        .sqe
        .partition_index
        .as_ref()
        .and_then(|idx| idx.entries.iter().find(|e| e.values == values))
    {
        if if_not_exists {
            return Ok(());
        }
        return Err(HiveError::Validation(format!(
            "partition {} already exists (at {}); add IF NOT EXISTS to make this a no-op",
            render_spec(&manifest.partition_keys, &values),
            existing.location
        )));
    }

    store.check_location(&location)?;
    let (file_count, total_bytes) = list_partition_stats(store, &location).await?;

    // Clone, don't take: write_index restores `previous` on failure, and
    // previous is whatever is in the slot when it replaces. Taking first
    // made every failed write restore None.
    let mut index = manifest.sqe.partition_index.clone().unwrap_or_else(|| {
        PartitionIndex {
            format_version: PARTITION_INDEX_FORMAT_VERSION,
            entries: Vec::new(),
        }
    });
    index.entries.push(PartitionIndexEntry {
        values,
        location,
        file_count,
        total_bytes,
    });
    write_index(store, manifest, index).await
}

/// `ALTER TABLE ... DROP PARTITION`: remove matching entries and rewrite
/// the manifest. Data files are never touched; `PURGE` is rejected by the
/// coordinator before this runs (cycle 4 owns data deletion).
///
/// An absent partition without `if_exists` is an error naming the
/// partition; with it, a no-op.
pub async fn drop_partition(
    store: &HiveManifestStore,
    manifest: &mut HiveManifest,
    spec: &PartitionSpec,
    if_exists: bool,
) -> Result<(), HiveError> {
    let values = typed_values(manifest, &spec.values)?;

    if projection_enabled(&manifest.parameters) {
        return Err(HiveError::Validation(format!(
            "ALTER TABLE ... DROP PARTITION is not supported on {}.{} because partition \
             projection owns the partition set; change the projection.* TBLPROPERTIES instead",
            manifest.database_name, manifest.name
        )));
    }

    let present = manifest
        .sqe
        .partition_index
        .as_ref()
        .is_some_and(|idx| idx.entries.iter().any(|e| e.values == values));
    if !present {
        if if_exists {
            return Ok(());
        }
        let hint = if manifest.sqe.partition_index.is_none() {
            "run MSCK REPAIR TABLE first or add IF EXISTS to make this a no-op"
        } else {
            "add IF EXISTS to make this a no-op"
        };
        return Err(HiveError::Validation(format!(
            "partition {} does not exist in the partition index; {hint}",
            render_spec(&manifest.partition_keys, &values)
        )));
    }

    let mut index = manifest
        .sqe
        .partition_index
        .clone()
        .expect("present is true only when the index exists");
    index.entries.retain(|e| e.values != values);
    write_index(store, manifest, index).await
}

/// `MSCK REPAIR TABLE`: recursively list the table location, derive
/// partition entries from `k=v/` path segments using cycle-1 discovery
/// rules, and rewrite the whole index wholesale.
///
/// Hive and Athena skip junk rather than aborting the repair: a file at the
/// table root, a directory that is not `k=v`, an undeclared or missing key,
/// or a value that does not parse to the declared type is omitted, and
/// neighboring valid partitions still land in the index. Exceeding the
/// entry cap still fails naming the cap, leaving the existing index
/// unchanged. Projection-enabled tables are a successful no-op: projection
/// owns the partition set, so a listing would not change what a query scans.
pub async fn msck_repair(
    store: &HiveManifestStore,
    manifest: &mut HiveManifest,
    max_entries: usize,
) -> Result<usize, HiveError> {
    if projection_enabled(&manifest.parameters) {
        tracing::debug!(
            database = %manifest.database_name,
            table = %manifest.name,
            "MSCK REPAIR TABLE is a no-op because partition projection owns the partition set"
        );
        return Ok(0);
    }
    let table_url = table_url_of(&manifest.storage_descriptor.location)?;
    let data = data_store_for(store, &manifest.storage_descriptor.location)?;

    let objects = data
        .list(Some(table_url.prefix()))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| HiveError::Store(format!("{}: {e}", table_url.prefix())))?;

    // Group files by their partition directory (the path components between
    // the table prefix and the file), then aggregate counts/sizes per group.
    // Skip paths Hive/Athena would ignore rather than failing the whole repair.
    let mut groups: BTreeMap<String, (BTreeMap<String, String>, u64, u64)> = BTreeMap::new();
    let mut skipped = 0u64;
    for obj in &objects {
        let rel = match obj.location.prefix_match(table_url.prefix()) {
            Some(parts) => parts.collect::<Vec<_>>(),
            None => continue,
        };
        if rel.is_empty() {
            continue;
        }
        let Some((dir_key, values)) = try_msck_entry(manifest, &rel) else {
            skipped += 1;
            continue;
        };
        let entry = groups.entry(dir_key).or_insert_with(|| (values, 0, 0));
        entry.1 += 1;
        entry.2 += obj.size;
    }
    if skipped > 0 {
        tracing::debug!(
            skipped,
            database = %manifest.database_name,
            table = %manifest.name,
            "MSCK REPAIR skipped objects that were not valid k=v partitions"
        );
    }

    if groups.len() > max_entries {
        return Err(HiveError::Validation(format!(
            "MSCK REPAIR TABLE found {} partitions, exceeding the cap of {max_entries}. Raise \
             [hive] manifest_max_partition_index_entries if the table genuinely has this many.",
            groups.len()
        )));
    }

    let mut entries = Vec::with_capacity(groups.len());
    for (dir_key, (values, file_count, total_bytes)) in groups {
        let mut base = manifest.storage_descriptor.location.clone();
        if !base.ends_with('/') {
            base.push('/');
        }
        let location = format!("{base}{dir_key}");
        entries.push(PartitionIndexEntry {
            values,
            location,
            file_count,
            total_bytes,
        });
    }

    let count = entries.len();
    let index = PartitionIndex {
        format_version: PARTITION_INDEX_FORMAT_VERSION,
        entries,
    };
    write_index(store, manifest, index).await?;
    Ok(count)
}

/// Partitions of a table: from the index when present, from a storage
/// listing when not (`SHOW PARTITIONS`' fallback).
pub async fn list_partitions(
    store: &HiveManifestStore,
    manifest: &HiveManifest,
) -> Result<Vec<ListedPartition>, HiveError> {
    if let Some(index) = &manifest.sqe.partition_index {
        let mut specs: Vec<ListedPartition> = index
            .entries
            .iter()
            .map(|e| ListedPartition {
                spec: render_spec(&manifest.partition_keys, &e.values),
            })
            .collect();
        specs.sort_by(|a, b| a.spec.cmp(&b.spec));
        return Ok(specs);
    }

    // Listing fallback: same derivation MSCK uses, but reporting only.
    let table_url = table_url_of(&manifest.storage_descriptor.location)?;
    let data = data_store_for(store, &manifest.storage_descriptor.location)?;
    let objects = data
        .list(Some(table_url.prefix()))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| HiveError::Store(format!("{}: {e}", table_url.prefix())))?;

    let mut specs = Vec::new();
    for obj in &objects {
        let rel = match obj.location.prefix_match(table_url.prefix()) {
            Some(parts) => parts.collect::<Vec<_>>(),
            None => continue,
        };
        if let Some((_, values)) = try_msck_entry(manifest, &rel) {
            specs.push(ListedPartition {
                spec: render_spec(&manifest.partition_keys, &values),
            });
        }
    }
    specs.sort_by(|a, b| a.spec.cmp(&b.spec));
    specs.dedup();
    Ok(specs)
}

/// Type and validate raw `(key, value)` pairs against the manifest's
/// declared partition keys: exact key-set match, NULL sentinel allowed,
/// everything else parses to its declared type. Returns the values keyed by
/// name, which is the index entry's stored shape.
fn typed_values(
    manifest: &HiveManifest,
    raw: &[(String, String)],
) -> Result<BTreeMap<String, String>, HiveError> {
    let declared: BTreeMap<&str, &str> = manifest
        .partition_keys
        .iter()
        .map(|k| (k.name.as_str(), k.r#type.as_str()))
        .collect();

    let mut out = BTreeMap::new();
    for (key, value) in raw {
        let Some(sql_type) = declared.get(key.as_str()) else {
            return Err(HiveError::Validation(format!(
                "`{key}` is not a declared partition key of {}.{} (declared: {})",
                manifest.database_name,
                manifest.name,
                declared
                    .keys()
                    .copied()
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        };
        if value != HIVE_DEFAULT_PARTITION {
            parse_typed_partition_value(value, sql_type).map_err(|e| HiveError::Validation(e.to_string()))?;
        }
        out.insert(key.clone(), value.clone());
    }

    for key in declared.keys() {
        if !out.contains_key(*key) {
            return Err(HiveError::Validation(format!(
                "partition spec is missing declared partition key `{key}`"
            )));
        }
    }
    Ok(out)
}

/// Derive a partition from a relative object path, or `None` to skip it.
///
/// Hive and Athena MSCK ignore (they do not abort on) a file at the table
/// root, a directory segment that is not `k=v`, an undeclared or missing
/// partition key, and a value that does not parse to the declared type.
/// The last path component is the file name; everything before it is the
/// partition directory.
fn try_msck_entry<S: AsRef<str>>(
    manifest: &HiveManifest,
    rel: &[S],
) -> Option<(String, BTreeMap<String, String>)> {
    if rel.len() < 2 {
        return None;
    }
    let mut values = BTreeMap::new();
    let mut dir_key = String::new();
    for part in &rel[..rel.len() - 1] {
        let seg = part.as_ref();
        let (k, v) = seg.split_once('=')?;
        if k.is_empty() {
            return None;
        }
        values.insert(k.to_string(), v.to_string());
        dir_key.push_str(seg);
        dir_key.push('/');
    }
    let raw: Vec<(String, String)> = values.into_iter().collect();
    let typed = typed_values(manifest, &raw).ok()?;
    Some((dir_key, typed))
}

/// The standard `k=v/k=v/` layout under the table location, for an ADD
/// PARTITION that gave no explicit LOCATION. Segments follow PARTITIONED BY
/// declaration order, matching Hive/Athena, not BTreeMap key order.
fn derive_location(
    table_location: &str,
    keys: &[GlueColumn],
    values: &BTreeMap<String, String>,
) -> Result<String, HiveError> {
    let base = table_url_of(table_location)?;
    let base_url: &url::Url = base.as_ref();
    let mut url: String = base_url.as_str().to_string();
    if !url.ends_with('/') {
        url.push('/');
    }
    for k in keys {
        let v = values.get(&k.name).ok_or_else(|| {
            HiveError::Validation(format!(
                "partition spec is missing declared partition key `{}`",
                k.name
            ))
        })?;
        // URL-encode nothing: Hive writes raw segment text, and the value
        // already round-tripped through the tokenizer. A `/` inside a value
        // would corrupt the layout, so refuse rather than mangle.
        if v.contains('/') {
            return Err(HiveError::Validation(format!(
                "partition value `{v}` contains `/`, which cannot be written into a \
                 k=v/ directory layout; give an explicit LOCATION instead"
            )));
        }
        url.push_str(&format!("{}={v}/", k.name));
    }
    Ok(url)
}

/// LIST one partition location for its file count and byte total.
async fn list_partition_stats(
    store: &HiveManifestStore,
    location: &str,
) -> Result<(u64, u64), HiveError> {
    let url = table_url_of(location)?;
    let data = data_store_for(store, location)?;
    let objects = data
        .list(Some(url.prefix()))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| HiveError::Store(format!("{location}: {e}")))?;
    let mut files = 0u64;
    let mut bytes = 0u64;
    for obj in objects {
        if obj.location.as_ref().ends_with('/') {
            continue;
        }
        files += 1;
        bytes += obj.size;
    }
    Ok((files, bytes))
}

/// Resolve the object store for a data location through the root's store,
/// refusing a cross-bucket location by name. The root's store is built from
/// the attach-time `[storage]` credentials; silently using it for a
/// different bucket would either fail opaquely at LIST time or, worse,
/// succeed against the wrong endpoint. Out-of-bucket locations need a
/// per-location store build, which is follow-up work, not silent behavior.
fn data_store_for(
    store: &HiveManifestStore,
    location: &str,
) -> Result<Arc<dyn ObjectStore>, HiveError> {
    let loc = table_url_of(location)?;
    let root = store.root_url();
    let loc_url: &url::Url = loc.as_ref();
    let root_url: &url::Url = root.as_ref();
    let loc_key = (loc_url.scheme(), loc_url.host_str(), loc_url.port());
    let root_key = (root_url.scheme(), root_url.host_str(), root_url.port());
    if loc_key != root_key {
        return Err(HiveError::Validation(format!(
            "location `{location}` is on a different scheme/host than the warehouse root \
             `{}`; index maintenance currently requires both under one root",
            root_url
        )));
    }
    Ok(store.object_store())
}

fn table_url_of(location: &str) -> Result<ListingTableUrl, HiveError> {
    let normalized = if location.ends_with('/') {
        location.to_string()
    } else {
        format!("{location}/")
    };
    ListingTableUrl::parse(&normalized)
        .map_err(|e| HiveError::Validation(format!("invalid location `{location}`: {e}")))
}

/// `dt=2026-01-02/region=eu`, Hive/Athena rendering in *declaration* order
/// (not BTreeMap key order). A table `PARTITIONED BY (region, dt)` must
/// show `region=eu/dt=2026-01-02`.
fn render_spec(keys: &[GlueColumn], values: &BTreeMap<String, String>) -> String {
    let ordered: Vec<String> = keys
        .iter()
        .filter_map(|k| {
            values.get(&k.name).map(|v| {
                let v = if v == HIVE_DEFAULT_PARTITION {
                    HIVE_DEFAULT_PARTITION
                } else {
                    v.as_str()
                };
                format!("{}={v}", k.name)
            })
        })
        .collect();
    if !ordered.is_empty() {
        return ordered.join("/");
    }
    // Fallback when the caller has values but no key list (should not
    // happen for a real manifest): keep a stable rendering.
    values
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("/")
}

/// Cap-check and persist the rewritten index. The cap runs HERE too (not
/// only at parse time) so ADD cannot push a manifest past what a reader
/// will accept: an oversized index that only failed on load would mean the
/// writer just bricked the table for every reader.
async fn write_index(
    store: &HiveManifestStore,
    manifest: &mut HiveManifest,
    index: PartitionIndex,
) -> Result<(), HiveError> {
    // The load-time cap comes from the store's limits; re-validate through
    // the same parse path a reader will use, which checks version, caps,
    // key sets, typing, and locations in one place.
    //
    // Restore the previous slot if validation or put fails, so a cap hit
    // or a forbidden location cannot leave the in-memory handle holding
    // the rejected index (or None) while the store still has the last
    // good one.
    let previous = manifest.sqe.partition_index.replace(index);
    let fail = |manifest: &mut HiveManifest, previous: Option<PartitionIndex>, e: HiveError| {
        manifest.sqe.partition_index = previous;
        e
    };
    let bytes = manifest
        .to_bytes()
        .map_err(|e| fail(manifest, previous.clone(), e))?;
    if let Err(e) = super::manifest::HiveManifest::parse(&bytes, store.limits()) {
        return Err(fail(manifest, previous, e));
    }
    match store.put(manifest, false).await {
        Ok(()) => Ok(()),
        Err(e) => Err(fail(manifest, previous, e)),
    }
}

/// `ANALYZE TABLE`: count rows per partition (Parquet from footers, CSV/JSON
/// by decoding) and persist into `sqe.statistics`.
pub async fn analyze_table(
    store: &HiveManifestStore,
    manifest: &mut HiveManifest,
) -> Result<HiveTableStatistics, HiveError> {
    let format = resolve_format(manifest)?;
    let skip_header = manifest
        .parameters
        .get("skip.header.line.count")
        .map(|s| s == "1")
        .unwrap_or(false);

    let locations: Vec<(BTreeMap<String, String>, String)> =
        if projection_enabled(&manifest.parameters) {
            let cfg = parse_projection(&manifest.parameters, &manifest.partition_keys)?
                .ok_or_else(|| {
                    HiveError::Validation(
                        "projection.enabled=true but projection config did not parse".into(),
                    )
                })?;
            evaluate_projection(
                &cfg,
                &manifest.storage_descriptor.location,
                &[],
                store.limits().max_partition_index_entries,
            )?
            .into_iter()
            .map(|e| (e.values, e.location))
            .collect()
        } else if let Some(index) = &manifest.sqe.partition_index {
            index
                .entries
                .iter()
                .map(|e| (e.values.clone(), e.location.clone()))
                .collect()
        } else {
            vec![(BTreeMap::new(), manifest.storage_descriptor.location.clone())]
        };

    let mut entries = Vec::new();
    let mut total = 0u64;
    for (values, location) in locations {
        let n = count_rows_at(store, &location, format.kind, skip_header).await?;
        total += n;
        if !values.is_empty() {
            entries.push(HivePartitionStatistics {
                values,
                row_count: n,
            });
        }
    }

    let stats = HiveTableStatistics {
        row_count: total,
        entries,
    };
    manifest.sqe.statistics = Some(
        serde_json::to_value(&stats)
            .map_err(|e| HiveError::Validation(format!("serialize statistics: {e}")))?,
    );
    store.put(manifest, false).await?;
    Ok(stats)
}

async fn count_rows_at(
    store: &HiveManifestStore,
    location: &str,
    kind: HiveFileFormat,
    skip_header: bool,
) -> Result<u64, HiveError> {
    let url = table_url_of(location)?;
    let data = data_store_for(store, location)?;
    let objects = data
        .list(Some(url.prefix()))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| HiveError::Store(format!("{location}: {e}")))?;

    let mut rows = 0u64;
    for obj in objects {
        if obj.location.as_ref().ends_with('/') {
            continue;
        }
        rows += match kind {
            HiveFileFormat::Parquet => count_parquet_rows(data.as_ref(), &obj).await?,
            HiveFileFormat::Csv => count_text_rows(data.as_ref(), &obj, skip_header).await?,
            HiveFileFormat::Json => count_text_rows(data.as_ref(), &obj, false).await?,
        };
    }
    Ok(rows)
}

async fn count_parquet_rows(
    store: &dyn ObjectStore,
    obj: &object_store::ObjectMeta,
) -> Result<u64, HiveError> {
    use parquet::file::metadata::ParquetMetaDataReader;
    // Footer-only: the last 8 bytes name the footer length; fetch a trailing
    // window large enough for typical Hive footers, then parse.
    let size = obj.size;
    if size < 8 {
        return Ok(0);
    }
    let window = 1_048_576u64.min(size);
    let start = size - window;
    let bytes = store
        .get_range(&obj.location, start..size)
        .await
        .map_err(|e| HiveError::Store(format!("{}: {e}", obj.location)))?;
    match ParquetMetaDataReader::new().parse_and_finish(&bytes) {
        Ok(meta) => Ok(meta.file_metadata().num_rows() as u64),
        Err(_) if start == 0 => Ok(0),
        Err(_) => {
            // Footer larger than the window: fall back to the whole object.
            let all = store
                .get(&obj.location)
                .await
                .map_err(|e| HiveError::Store(format!("{}: {e}", obj.location)))?
                .bytes()
                .await
                .map_err(|e| HiveError::Store(format!("{}: {e}", obj.location)))?;
            let meta = ParquetMetaDataReader::new()
                .parse_and_finish(&all)
                .map_err(|e| HiveError::Store(format!("{}: parquet footer: {e}", obj.location)))?;
            Ok(meta.file_metadata().num_rows() as u64)
        }
    }
}

async fn count_text_rows(
    store: &dyn ObjectStore,
    obj: &object_store::ObjectMeta,
    skip_header: bool,
) -> Result<u64, HiveError> {
    let bytes = store
        .get(&obj.location)
        .await
        .map_err(|e| HiveError::Store(format!("{}: {e}", obj.location)))?
        .bytes()
        .await
        .map_err(|e| HiveError::Store(format!("{}: {e}", obj.location)))?;
    if bytes.is_empty() {
        return Ok(0);
    }
    let mut n = bytes.iter().filter(|b| **b == b'\n').count() as u64;
    if !bytes.ends_with(b"\n") {
        n += 1;
    }
    if skip_header && n > 0 {
        n -= 1;
    }
    Ok(n)
}
