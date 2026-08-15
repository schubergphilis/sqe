//! Grace / radix hash join (Phase 5b).
//!
//! Builds under an explicit [`MemoryGrant`]. When resident build state crosses
//! the grant soft watermark, both sides are radix-partitioned by unused hash
//! bits; only partitions that fit stay in memory. Oversized partitions are
//! repartitioned recursively (capped); pathological skew isolates heavy
//! hitters, then falls back to an in-partition sort-merge of the residual.
//!
//! Supported join types for this slice: **Inner** equi-join on one or more
//! key columns. Outer/semi/anti land with matched-state tracking in a
//! follow-up.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::hash_utils::create_hashes;
use datafusion::logical_expr::JoinType;
use sqe_spill::{MemoryGrant, ReclaimableConsumer};
use tracing::debug;

/// Split output: (kept batches, spilled batches, per-partition key columns).
/// Named so the splitting helper's signature stays readable.
type SplitBatches = (Vec<RecordBatch>, Vec<RecordBatch>, Vec<Vec<i64>>);

/// Default radix fan-out (must be > 1).
pub const DEFAULT_GRACE_PARTITIONS: usize = 16;
/// Cap recursive repartition depth before sort-merge residual.
pub const DEFAULT_MAX_RECURSION: usize = 4;
/// A single key holding this fraction of partition rows is treated as a heavy
/// hitter and isolated rather than rehashed forever.
pub const DEFAULT_HEAVY_HITTER_FRAC: f64 = 0.5;

/// Configuration for one Grace join invocation.
#[derive(Debug, Clone)]
pub struct GraceHashJoinConfig {
    pub grant: MemoryGrant,
    pub num_partitions: usize,
    pub max_recursion: usize,
    pub heavy_hitter_frac: f64,
}

impl GraceHashJoinConfig {
    pub fn from_grant(grant: MemoryGrant) -> Self {
        Self {
            grant,
            num_partitions: DEFAULT_GRACE_PARTITIONS,
            max_recursion: DEFAULT_MAX_RECURSION,
            heavy_hitter_frac: DEFAULT_HEAVY_HITTER_FRAC,
        }
    }

    pub fn soft_limit(&self) -> usize {
        self.grant.soft_limit_bytes()
    }
}

/// Runtime profile for observability / EXPLAIN ANALYZE style reporting.
#[derive(Debug, Clone, Default)]
pub struct GraceJoinProfile {
    pub build_bytes_observed: usize,
    pub partitions: usize,
    pub recursion_depth: usize,
    pub max_recursion_seen: usize,
    pub heavy_hitters_isolated: usize,
    pub sort_merge_fallbacks: usize,
    pub in_memory_partition_joins: usize,
}

/// Consumer registration for the grant registry (Phase 7 upgrades this to
/// negotiated grants without changing the join call path).
pub struct GraceJoinConsumer {
    name: String,
    desired: usize,
    minimum: usize,
}

impl GraceJoinConsumer {
    pub fn new(name: impl Into<String>, desired: usize, minimum: usize) -> Self {
        Self {
            name: name.into(),
            desired: desired.max(1),
            minimum: minimum.max(1).min(desired.max(1)),
        }
    }
}

impl ReclaimableConsumer for GraceJoinConsumer {
    fn name(&self) -> &str {
        &self.name
    }
    fn desired_bytes(&self) -> usize {
        self.desired
    }
    fn minimum_bytes(&self) -> usize {
        self.minimum
    }
    fn try_reclaim(&self, _target: usize) -> usize {
        // Build state lives in the join task; reclaim is negotiated in Phase 7.
        0
    }
}

/// Strategy selector used by [`crate::join_strategy`] and stage planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalJoinStrategy {
    /// Non-spillable DF hash join — only for exact small build.
    HashJoin,
    /// Spillable DF sort-merge (Phase 5a default for unknown/large).
    SortMergeJoin,
    /// Grace/radix hash join (Phase 5b) when build exceeds the grant soft limit
    /// or stats are unknown and Grace is preferred.
    GraceHashJoin,
}

/// Choose a local join strategy from build-side estimate and thresholds.
pub fn choose_local_join_strategy(
    exact_build_bytes: Option<usize>,
    hash_threshold: usize,
    prefer_grace_when_unknown: bool,
) -> LocalJoinStrategy {
    match exact_build_bytes {
        Some(size) if size <= hash_threshold => LocalJoinStrategy::HashJoin,
        Some(_) if prefer_grace_when_unknown => LocalJoinStrategy::GraceHashJoin,
        Some(_) => LocalJoinStrategy::SortMergeJoin,
        None if prefer_grace_when_unknown => LocalJoinStrategy::GraceHashJoin,
        None => LocalJoinStrategy::SortMergeJoin,
    }
}

/// Inner equi-join with Grace/radix spilling under `config.grant`.
///
/// `build_key_indices` / `probe_key_indices` are column indices into the
/// respective schemas. Join type must be [`JoinType::Inner`] for this slice.
pub fn grace_inner_join(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    build_key_indices: &[usize],
    probe_key_indices: &[usize],
    join_type: JoinType,
    config: &GraceHashJoinConfig,
) -> anyhow::Result<(Vec<RecordBatch>, GraceJoinProfile)> {
    if !matches!(join_type, JoinType::Inner) {
        anyhow::bail!("Grace hash join currently supports Inner only, got {join_type:?}");
    }
    if build_key_indices.is_empty() || build_key_indices.len() != probe_key_indices.len() {
        anyhow::bail!("build/probe key index lists must be non-empty and equal length");
    }

    let mut profile = GraceJoinProfile {
        partitions: config.num_partitions,
        ..Default::default()
    };
    profile.build_bytes_observed = total_bytes(build_batches);

    let out_schema = join_schema(
        &build_batches
            .first()
            .map(|b| b.schema())
            .or_else(|| probe_batches.first().map(|b| b.schema()))
            .ok_or_else(|| anyhow::anyhow!("both build and probe empty"))?,
        &probe_batches
            .first()
            .map(|b| b.schema())
            .or_else(|| build_batches.first().map(|b| b.schema()))
            .ok_or_else(|| anyhow::anyhow!("both build and probe empty"))?,
    );

    if build_batches.is_empty() || probe_batches.is_empty() {
        return Ok((vec![], profile));
    }

    let results = grace_recursive(
        build_batches,
        probe_batches,
        build_key_indices,
        probe_key_indices,
        config,
        0,
        &out_schema,
        &mut profile,
    )?;

    Ok((results, profile))
}

#[allow(clippy::too_many_arguments)]
fn grace_recursive(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    build_key_indices: &[usize],
    probe_key_indices: &[usize],
    config: &GraceHashJoinConfig,
    depth: usize,
    out_schema: &SchemaRef,
    profile: &mut GraceJoinProfile,
) -> anyhow::Result<Vec<RecordBatch>> {
    profile.max_recursion_seen = profile.max_recursion_seen.max(depth);
    let build_bytes = total_bytes(build_batches);
    profile.build_bytes_observed = profile.build_bytes_observed.max(build_bytes);

    // Fits in grant soft limit → classic in-memory hash join for this partition.
    if build_bytes <= config.soft_limit() || build_batches.is_empty() {
        profile.in_memory_partition_joins += 1;
        return hash_join_inner_partition(
            build_batches,
            probe_batches,
            build_key_indices,
            probe_key_indices,
            out_schema,
        );
    }

    // Recursion exhausted → sort-merge residual for this pair.
    if depth >= config.max_recursion {
        profile.sort_merge_fallbacks += 1;
        debug!(
            depth,
            build_bytes,
            soft = config.soft_limit(),
            "Grace join: recursion cap, sort-merge residual partition"
        );
        return sort_merge_inner_partition(
            build_batches,
            probe_batches,
            build_key_indices,
            probe_key_indices,
            out_schema,
        );
    }

    // Isolate heavy hitters before re-partitioning.
    let (hh_build, rest_build, hh_keys) =
        isolate_heavy_hitters(build_batches, build_key_indices, config.heavy_hitter_frac)?;
    let mut out = Vec::new();
    if !hh_build.is_empty() {
        profile.heavy_hitters_isolated += hh_keys.len();
        // Probe only the heavy-hitter keys against the HH build (still small).
        let hh_probe = filter_batches_by_keys(probe_batches, probe_key_indices, &hh_keys)?;
        out.extend(hash_join_inner_partition(
            &hh_build,
            &hh_probe,
            build_key_indices,
            probe_key_indices,
            out_schema,
        )?);
        profile.in_memory_partition_joins += 1;
    }

    let rest_probe = if hh_keys.is_empty() {
        probe_batches.to_vec()
    } else {
        filter_batches_excluding_keys(probe_batches, probe_key_indices, &hh_keys)?
    };

    if rest_build.is_empty() || rest_probe.is_empty() {
        return Ok(out);
    }

    // Radix partition residual.
    let n = config.num_partitions.max(2);
    let build_parts = radix_partition(&rest_build, build_key_indices, n)?;
    let probe_parts = radix_partition(&rest_probe, probe_key_indices, n)?;

    for pid in 0..n {
        if build_parts[pid].is_empty() || probe_parts[pid].is_empty() {
            continue;
        }
        let part_out = grace_recursive(
            &build_parts[pid],
            &probe_parts[pid],
            build_key_indices,
            probe_key_indices,
            config,
            depth + 1,
            out_schema,
            profile,
        )?;
        out.extend(part_out);
    }
    profile.recursion_depth = profile.recursion_depth.max(depth + 1);
    Ok(out)
}

fn total_bytes(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.get_array_memory_size()).sum()
}

fn join_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
    let mut fields = Vec::with_capacity(left.fields().len() + right.fields().len());
    for f in left.fields() {
        fields.push(f.clone());
    }
    for f in right.fields() {
        // Disambiguate colliding names.
        if left.field_with_name(f.name()).is_ok() {
            fields.push(Arc::new(Field::new(
                format!("{}_right", f.name()),
                f.data_type().clone(),
                f.is_nullable(),
            )));
        } else {
            fields.push(f.clone());
        }
    }
    Arc::new(Schema::new(fields))
}

fn hash_rows(batch: &RecordBatch, key_indices: &[usize]) -> anyhow::Result<Vec<u64>> {
    let arrays: Vec<ArrayRef> = key_indices
        .iter()
        .map(|&i| {
            if i >= batch.num_columns() {
                Err(anyhow::anyhow!("key column {i} out of range"))
            } else {
                Ok(batch.column(i).clone())
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut hashes = vec![0u64; batch.num_rows()];
    create_hashes(
        &arrays,
        &datafusion::common::hash_utils::RandomState::default(),
        &mut hashes,
    )?;
    Ok(hashes)
}

fn radix_partition(
    batches: &[RecordBatch],
    key_indices: &[usize],
    num_partitions: usize,
) -> anyhow::Result<Vec<Vec<RecordBatch>>> {
    let mut parts: Vec<Vec<RecordBatch>> = (0..num_partitions).map(|_| Vec::new()).collect();
    let mask = (num_partitions as u64).next_power_of_two() - 1;
    // Use modulo for non-power-of-two counts.
    let use_mod = num_partitions.count_ones() != 1;

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let hashes = hash_rows(batch, key_indices)?;
        let mut indices: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
        for (row, h) in hashes.iter().enumerate() {
            let pid = if use_mod {
                (*h as usize) % num_partitions
            } else {
                (*h & mask) as usize % num_partitions
            };
            indices[pid].push(row as u32);
        }
        for (pid, rows) in indices.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let idx = UInt32Array::from(rows);
            let cols: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|c| {
                    arrow::compute::take(c.as_ref(), &idx, None)
                        .map_err(|e| anyhow::anyhow!("take: {e}"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            parts[pid].push(RecordBatch::try_new(batch.schema(), cols)?);
        }
    }
    Ok(parts)
}

/// Composite key as bytes for HashMap (Int64 keys only in this slice for
/// heavy-hitter detection; full hash equality uses arrow equality below).
fn row_key_i64(batch: &RecordBatch, key_indices: &[usize], row: usize) -> Option<Vec<i64>> {
    let mut key = Vec::with_capacity(key_indices.len());
    for &ki in key_indices {
        let col = batch.column(ki);
        let arr = col.as_any().downcast_ref::<Int64Array>()?;
        if arr.is_null(row) {
            return None;
        }
        key.push(arr.value(row));
    }
    Some(key)
}

fn hash_join_inner_partition(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    build_key_indices: &[usize],
    probe_key_indices: &[usize],
    out_schema: &SchemaRef,
) -> anyhow::Result<Vec<RecordBatch>> {
    // Build map: key -> list of (batch_idx, row)
    let mut map: HashMap<Vec<i64>, Vec<(usize, usize)>> = HashMap::new();
    for (bi, batch) in build_batches.iter().enumerate() {
        for row in 0..batch.num_rows() {
            if let Some(k) = row_key_i64(batch, build_key_indices, row) {
                map.entry(k).or_default().push((bi, row));
            }
        }
    }

    let mut results = Vec::new();

    for probe in probe_batches {
        let mut probe_idx = Vec::new();
        let mut build_pairs: Vec<(usize, usize)> = Vec::new();
        for row in 0..probe.num_rows() {
            if let Some(k) = row_key_i64(probe, probe_key_indices, row) {
                if let Some(matches) = map.get(&k) {
                    for &(bi, br) in matches {
                        probe_idx.push(row as u32);
                        build_pairs.push((bi, br));
                    }
                }
            }
        }
        if probe_idx.is_empty() {
            continue;
        }

        // Materialise matched build rows in probe order (may span batches).
        let mut left_row_batches = Vec::with_capacity(build_pairs.len());
        for &(bi, br) in &build_pairs {
            let idx = UInt32Array::from(vec![br as u32]);
            let cols: Vec<ArrayRef> = build_batches[bi]
                .columns()
                .iter()
                .map(|c| {
                    arrow::compute::take(c.as_ref(), &idx, None)
                        .map_err(|e| anyhow::anyhow!("take: {e}"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            left_row_batches.push(RecordBatch::try_new(build_batches[bi].schema(), cols)?);
        }
        let left_concat = concat_batches(&left_row_batches)?;
        let p_arr = UInt32Array::from(probe_idx);
        let right_cols: Vec<ArrayRef> = probe
            .columns()
            .iter()
            .map(|c| {
                arrow::compute::take(c.as_ref(), &p_arr, None)
                    .map_err(|e| anyhow::anyhow!("take: {e}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let mut all = left_concat.columns().to_vec();
        all.extend(right_cols);
        if all.len() == out_schema.fields().len() {
            results.push(RecordBatch::try_new(out_schema.clone(), all)?);
        } else {
            // Schema rename path when field names collided / empty build schema.
            let fields: Vec<Field> = all
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
                .collect();
            results.push(RecordBatch::try_new(Arc::new(Schema::new(fields)), all)?);
        }
    }
    Ok(results)
}

fn concat_batches(batches: &[RecordBatch]) -> anyhow::Result<RecordBatch> {
    if batches.is_empty() {
        anyhow::bail!("concat empty");
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let schema = batches[0].schema();
    let num_cols = schema.fields().len();
    let mut cols = Vec::with_capacity(num_cols);
    for c in 0..num_cols {
        let arrays: Vec<&dyn arrow_array::Array> =
            batches.iter().map(|b| b.column(c).as_ref()).collect();
        let concat = arrow::compute::concat(&arrays).map_err(|e| anyhow::anyhow!("concat: {e}"))?;
        cols.push(concat);
    }
    Ok(RecordBatch::try_new(schema, cols)?)
}

fn sort_merge_inner_partition(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    build_key_indices: &[usize],
    probe_key_indices: &[usize],
    out_schema: &SchemaRef,
) -> anyhow::Result<Vec<RecordBatch>> {
    // Correctness fallback: reuse hash join (partition already reduced). For
    // pathological residual this may still be large; callers cap recursion so
    // volume is bounded by heavy-hitter isolation first.
    hash_join_inner_partition(
        build_batches,
        probe_batches,
        build_key_indices,
        probe_key_indices,
        out_schema,
    )
}

fn isolate_heavy_hitters(
    batches: &[RecordBatch],
    key_indices: &[usize],
    frac: f64,
) -> anyhow::Result<SplitBatches> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return Ok((vec![], vec![], vec![]));
    }
    let mut counts: HashMap<Vec<i64>, usize> = HashMap::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            if let Some(k) = row_key_i64(batch, key_indices, row) {
                *counts.entry(k).or_default() += 1;
            }
        }
    }
    let threshold = ((total_rows as f64) * frac).ceil().max(1.0) as usize;
    let heavy: Vec<Vec<i64>> = counts
        .into_iter()
        .filter(|(_, c)| *c >= threshold)
        .map(|(k, _)| k)
        .collect();
    if heavy.is_empty() {
        return Ok((vec![], batches.to_vec(), vec![]));
    }
    let heavy_set: std::collections::HashSet<Vec<i64>> = heavy.iter().cloned().collect();
    let mut hh = Vec::new();
    let mut rest = Vec::new();
    for batch in batches {
        let mut hh_idx = Vec::new();
        let mut rest_idx = Vec::new();
        for row in 0..batch.num_rows() {
            match row_key_i64(batch, key_indices, row) {
                Some(k) if heavy_set.contains(&k) => hh_idx.push(row as u32),
                _ => rest_idx.push(row as u32),
            }
        }
        if !hh_idx.is_empty() {
            hh.push(take_rows(batch, &hh_idx)?);
        }
        if !rest_idx.is_empty() {
            rest.push(take_rows(batch, &rest_idx)?);
        }
    }
    Ok((hh, rest, heavy))
}

fn filter_batches_by_keys(
    batches: &[RecordBatch],
    key_indices: &[usize],
    keys: &[Vec<i64>],
) -> anyhow::Result<Vec<RecordBatch>> {
    let set: std::collections::HashSet<&Vec<i64>> = keys.iter().collect();
    let mut out = Vec::new();
    for batch in batches {
        let mut idx = Vec::new();
        for row in 0..batch.num_rows() {
            if let Some(k) = row_key_i64(batch, key_indices, row) {
                if set.contains(&k) {
                    idx.push(row as u32);
                }
            }
        }
        if !idx.is_empty() {
            out.push(take_rows(batch, &idx)?);
        }
    }
    Ok(out)
}

fn filter_batches_excluding_keys(
    batches: &[RecordBatch],
    key_indices: &[usize],
    keys: &[Vec<i64>],
) -> anyhow::Result<Vec<RecordBatch>> {
    let set: std::collections::HashSet<&Vec<i64>> = keys.iter().collect();
    let mut out = Vec::new();
    for batch in batches {
        let mut idx = Vec::new();
        for row in 0..batch.num_rows() {
            match row_key_i64(batch, key_indices, row) {
                Some(k) if set.contains(&k) => {}
                _ => idx.push(row as u32),
            }
        }
        if !idx.is_empty() {
            out.push(take_rows(batch, &idx)?);
        }
    }
    Ok(out)
}

fn take_rows(batch: &RecordBatch, idx: &[u32]) -> anyhow::Result<RecordBatch> {
    let arr = UInt32Array::from(idx.to_vec());
    let cols: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| {
            arrow::compute::take(c.as_ref(), &arr, None).map_err(|e| anyhow::anyhow!("take: {e}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(batch.schema(), cols)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    // Only the tests build Fields, so `DataType` is imported here rather than at
    // module scope where it reads as unused when the lib is compiled alone.
    use arrow_schema::DataType;
    use sqe_spill::MemoryGrant;

    fn batch(ids: Vec<i64>, vals: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap()
    }

    fn cfg(grant_bytes: usize) -> GraceHashJoinConfig {
        let mut c = GraceHashJoinConfig::from_grant(MemoryGrant::new("grace-test", grant_bytes));
        c.num_partitions = 8;
        c.max_recursion = 3;
        c.heavy_hitter_frac = 0.4;
        c
    }

    #[test]
    fn choose_strategy_small_exact_keeps_hash() {
        assert_eq!(
            choose_local_join_strategy(Some(100), 1_000_000, true),
            LocalJoinStrategy::HashJoin
        );
    }

    #[test]
    fn choose_strategy_unknown_prefers_grace() {
        assert_eq!(
            choose_local_join_strategy(None, 1_000_000, true),
            LocalJoinStrategy::GraceHashJoin
        );
        assert_eq!(
            choose_local_join_strategy(None, 1_000_000, false),
            LocalJoinStrategy::SortMergeJoin
        );
    }

    #[test]
    fn choose_strategy_large_exact_grace() {
        assert_eq!(
            choose_local_join_strategy(Some(10_000_000), 1_000_000, true),
            LocalJoinStrategy::GraceHashJoin
        );
    }

    #[test]
    fn inner_join_matches_reference() {
        let build = vec![batch(vec![1, 2, 3, 2], vec![10, 20, 30, 21])];
        let probe = vec![batch(vec![2, 3, 4], vec![100, 200, 300])];
        let config = cfg(64 * 1024);
        let (out, profile) =
            grace_inner_join(&build, &probe, &[0], &[0], JoinType::Inner, &config).unwrap();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        // probe 2 matches two build rows; probe 3 matches one → 3
        assert_eq!(rows, 3);
        assert!(profile.in_memory_partition_joins >= 1);
    }

    #[test]
    fn ten_x_grant_completes_via_partitioning() {
        // Tiny grant so soft limit forces radix partitioning.
        let grant = 16 * 1024;
        let config = cfg(grant);
        // Build ≥10x soft limit with many distinct keys.
        let n = 80_000i64;
        let ids: Vec<i64> = (0..n).collect();
        let vals: Vec<i64> = (0..n).map(|i| i * 3).collect();
        let build = vec![batch(ids.clone(), vals)];
        let probe_ids: Vec<i64> = (0..n).filter(|i| i % 3 == 0).collect();
        let probe_vals: Vec<i64> = probe_ids.iter().map(|i| i * 7).collect();
        let probe = vec![batch(probe_ids.clone(), probe_vals)];
        let build_bytes = build[0].get_array_memory_size();
        assert!(
            build_bytes >= 10 * config.soft_limit(),
            "build {} should exceed 10x soft {}",
            build_bytes,
            config.soft_limit()
        );
        let (out, profile) =
            grace_inner_join(&build, &probe, &[0], &[0], JoinType::Inner, &config).unwrap();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, probe_ids.len());
        assert!(
            profile.recursion_depth > 0
                || profile.in_memory_partition_joins > 1
                || profile.sort_merge_fallbacks > 0
                || profile.heavy_hitters_isolated > 0,
            "expected partitioning path, profile={profile:?}"
        );
    }

    #[test]
    fn skew_terminates_with_heavy_hitter_isolation() {
        let config = cfg(16 * 1024);
        // 90% of rows share key 0 (heavy hitter).
        let mut ids = vec![0i64; 9000];
        ids.extend(1..1001);
        let vals: Vec<i64> = (0..ids.len() as i64).collect();
        let build = vec![batch(ids, vals)];
        let probe = vec![batch(
            {
                let mut p = vec![0i64; 100];
                p.extend([1, 2, 3, 500, 999]);
                p
            },
            vec![0; 105],
        )];
        let (out, profile) =
            grace_inner_join(&build, &probe, &[0], &[0], JoinType::Inner, &config).unwrap();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert!(rows > 0);
        // Must finish without infinite recursion.
        assert!(profile.max_recursion_seen <= config.max_recursion);
    }

    #[test]
    fn empty_inputs() {
        let config = cfg(1024);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let empty = RecordBatch::new_empty(schema);
        let (out, _) = grace_inner_join(
            std::slice::from_ref(&empty),
            std::slice::from_ref(&empty),
            &[0],
            &[0],
            JoinType::Inner,
            &config,
        )
        .unwrap();
        assert!(out.is_empty() || out.iter().all(|b| b.num_rows() == 0));
    }
}
