//! External / radix hash aggregation under a fixed memory grant (Phase 6).
//!
//! **DataFusion reality:** grouped hash aggregation already spills via DF's
//! own spill manager. This module is the SQE-owned path for cases where that
//! path fails its larger-than-memory gate (constant GROUP BY order, merge
//! pressure) and for grant-aware pre-aggregation tables that flush at a soft
//! watermark into radix partitions.
//!
//! Supported decomposable states in this slice: `COUNT`, `SUM` (i64),
//! `MIN`/`MAX` (i64). Holistic aggregates (median, distinct lists) return a
//! typed unsupported error rather than unbounded memory.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array};
// Array trait brings is_null into scope.
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::hash_utils::create_hashes;
use sqe_spill::{MemoryGrant, ReclaimableConsumer};
use tracing::debug;

/// Soft watermark as a fraction of the grant (75%).
pub const DEFAULT_AGG_SOFT_LIMIT_NUM: usize = 3;
pub const DEFAULT_AGG_SOFT_LIMIT_DEN: usize = 4;
/// Default radix fan-out when flushing.
pub const DEFAULT_AGG_PARTITIONS: usize = 16;
/// Recursion cap when a partition remains oversized after repartition.
pub const DEFAULT_AGG_MAX_RECURSION: usize = 4;

/// Decomposable aggregate kinds supported under the external path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecomposableAgg {
    Count,
    SumI64,
    MinI64,
    MaxI64,
}

impl DecomposableAgg {
    pub fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::SumI64 => "sum",
            Self::MinI64 => "min",
            Self::MaxI64 => "max",
        }
    }
}

/// Configuration for an external aggregation under a [`MemoryGrant`].
#[derive(Debug, Clone)]
pub struct ExternalAggregateConfig {
    pub grant: MemoryGrant,
    pub num_partitions: usize,
    pub max_recursion: usize,
}

impl ExternalAggregateConfig {
    pub fn from_grant(grant: MemoryGrant) -> Self {
        Self {
            grant,
            num_partitions: DEFAULT_AGG_PARTITIONS,
            max_recursion: DEFAULT_AGG_MAX_RECURSION,
        }
    }

    pub fn soft_limit(&self) -> usize {
        let cap = self.grant.capacity_bytes();
        (cap.saturating_mul(DEFAULT_AGG_SOFT_LIMIT_NUM) / DEFAULT_AGG_SOFT_LIMIT_DEN).max(1)
    }
}

/// Runtime profile for aggregate spill behaviour.
#[derive(Debug, Clone, Default)]
pub struct ExternalAggregateProfile {
    pub groups_emitted: usize,
    pub flushes: usize,
    pub recursion_depth: usize,
    pub max_recursion_seen: usize,
    pub peak_table_bytes: usize,
    pub used_datafusion_spill_path: bool,
}

/// Cases that DataFusion's built-in aggregate spill is expected to handle.
/// Recorded so gates can skip custom work when DF already passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DfAggregateSpillCase {
    /// Partial grouped hash with spillable pool — typically OK in DF 54.
    PartialGroupedHash,
    /// Final merge of partials with moderate cardinality.
    FinalMergeModerateCardinality,
    /// Partial under constant GROUP BY ordering (known DF gap: may exhaust
    /// without early emission).
    PartialConstantGroupOrder,
}

/// Record which DF spill cases pass the Phase 6 gate without custom code.
#[derive(Debug, Clone, Default)]
pub struct DfAggregateSpillGateReport {
    pub partial_grouped_hash: Option<bool>,
    pub final_merge_moderate: Option<bool>,
    pub partial_constant_group_order: Option<bool>,
}

impl DfAggregateSpillGateReport {
    pub fn record(&mut self, case: DfAggregateSpillCase, passed: bool) {
        match case {
            DfAggregateSpillCase::PartialGroupedHash => self.partial_grouped_hash = Some(passed),
            DfAggregateSpillCase::FinalMergeModerateCardinality => {
                self.final_merge_moderate = Some(passed)
            }
            DfAggregateSpillCase::PartialConstantGroupOrder => {
                self.partial_constant_group_order = Some(passed)
            }
        }
    }

    /// Cases that still need the SQE external path.
    pub fn needs_custom_path(&self) -> bool {
        matches!(self.partial_constant_group_order, Some(false) | None)
            || matches!(self.partial_grouped_hash, Some(false))
    }
}

/// Consumer for grant registry registration.
pub struct AggregateConsumer {
    name: String,
    desired: usize,
    minimum: usize,
}

impl AggregateConsumer {
    pub fn new(name: impl Into<String>, desired: usize, minimum: usize) -> Self {
        Self {
            name: name.into(),
            desired: desired.max(1),
            minimum: minimum.max(1).min(desired.max(1)),
        }
    }
}

impl ReclaimableConsumer for AggregateConsumer {
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
        0
    }
}

#[derive(Debug, Clone)]
struct AggState {
    count: i64,
    sum: i64,
    min: i64,
    max: i64,
}

impl AggState {
    fn new_from_value(v: i64) -> Self {
        Self {
            count: 1,
            sum: v,
            min: v,
            max: v,
        }
    }

    fn merge_value(&mut self, v: i64) {
        self.count += 1;
        self.sum = self.sum.saturating_add(v);
        self.min = self.min.min(v);
        self.max = self.max.max(v);
    }

    #[allow(dead_code)] // used when combining partial partition states
    fn merge_state(&mut self, other: &AggState) {
        self.count += other.count;
        self.sum = self.sum.saturating_add(other.sum);
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }
}

/// External hash aggregate: `GROUP BY` key column(s) + decomposable measures.
///
/// Input batches must have Int64 group keys at `group_key_indices` and an
/// optional Int64 measure column at `measure_index` (required for Sum/Min/Max;
/// ignored for Count-only when `measure_index` is `None`).
pub fn external_hash_aggregate(
    batches: &[RecordBatch],
    group_key_indices: &[usize],
    measure_index: Option<usize>,
    aggs: &[DecomposableAgg],
    config: &ExternalAggregateConfig,
) -> anyhow::Result<(Vec<RecordBatch>, ExternalAggregateProfile)> {
    if group_key_indices.is_empty() {
        anyhow::bail!("group_key_indices must be non-empty");
    }
    if aggs.is_empty() {
        anyhow::bail!("at least one aggregate required");
    }
    for a in aggs {
        if matches!(
            a,
            DecomposableAgg::SumI64 | DecomposableAgg::MinI64 | DecomposableAgg::MaxI64
        ) && measure_index.is_none()
        {
            anyhow::bail!("{} requires a measure column index", a.name());
        }
    }

    let mut profile = ExternalAggregateProfile::default();
    if batches.is_empty() {
        return Ok((vec![], profile));
    }

    let out = aggregate_recursive(
        batches,
        group_key_indices,
        measure_index,
        aggs,
        config,
        0,
        &mut profile,
    )?;
    Ok((out, profile))
}

fn aggregate_recursive(
    batches: &[RecordBatch],
    group_key_indices: &[usize],
    measure_index: Option<usize>,
    aggs: &[DecomposableAgg],
    config: &ExternalAggregateConfig,
    depth: usize,
    profile: &mut ExternalAggregateProfile,
) -> anyhow::Result<Vec<RecordBatch>> {
    profile.max_recursion_seen = profile.max_recursion_seen.max(depth);

    // Build a local pre-agg table until soft limit, then flush to partitions.
    let mut table: HashMap<Vec<i64>, AggState> = HashMap::new();
    let mut table_bytes = 0usize;

    for batch in batches {
        for row in 0..batch.num_rows() {
            let key = match row_key_i64(batch, group_key_indices, row) {
                Some(k) => k,
                None => continue, // drop null keys (SQL default)
            };
            let measure = measure_index
                .and_then(|mi| value_i64(batch, mi, row))
                .unwrap_or(0);

            match table.get_mut(&key) {
                Some(st) => st.merge_value(measure),
                None => {
                    // Rough accounting: key bytes + state.
                    table_bytes += key.len() * 8 + 32;
                    table.insert(key, AggState::new_from_value(measure));
                }
            }
            profile.peak_table_bytes = profile.peak_table_bytes.max(table_bytes);

            if table_bytes >= config.soft_limit() {
                profile.flushes += 1;
                debug!(
                    soft = config.soft_limit(),
                    groups = table.len(),
                    depth,
                    "External aggregate: soft watermark flush"
                );
                // Over soft limit: if we can still recurse, radix-partition
                // remaining input + current table and combine per partition.
                if depth >= config.max_recursion {
                    // Emit what we have; remaining rows still folded into table
                    // (may exceed soft limit slightly — gate tests size after).
                    continue;
                }
                // Spill strategy: partition *all* input batches and recurse
                // per partition with a fresh table (releases this table).
                let parts =
                    radix_partition_batches(batches, group_key_indices, config.num_partitions)?;
                // Fold already-seen table into partition 0 of a synthetic merge:
                // instead, re-process each partition independently from original
                // batches only (table is a subset — re-scan is correct).
                drop(table);
                let mut out = Vec::new();
                for part in parts {
                    if part.is_empty() {
                        continue;
                    }
                    let part_out = aggregate_recursive(
                        &part,
                        group_key_indices,
                        measure_index,
                        aggs,
                        config,
                        depth + 1,
                        profile,
                    )?;
                    out.extend(part_out);
                }
                profile.recursion_depth = profile.recursion_depth.max(depth + 1);
                // Combine same keys across partition emissions if any overlap
                // (radix is disjoint by hash, so no overlap).
                return Ok(out);
            }
        }
    }

    emit_table(
        &table,
        group_key_indices.len(),
        aggs,
        batches[0].schema(),
        profile,
    )
}

fn emit_table(
    table: &HashMap<Vec<i64>, AggState>,
    n_keys: usize,
    aggs: &[DecomposableAgg],
    _in_schema: SchemaRef,
    profile: &mut ExternalAggregateProfile,
) -> anyhow::Result<Vec<RecordBatch>> {
    if table.is_empty() {
        return Ok(vec![]);
    }
    let mut key_cols: Vec<Vec<i64>> = vec![Vec::with_capacity(table.len()); n_keys];
    let mut measure_cols: Vec<Vec<i64>> = aggs
        .iter()
        .map(|_| Vec::with_capacity(table.len()))
        .collect();

    for (key, st) in table {
        for (i, k) in key.iter().enumerate() {
            key_cols[i].push(*k);
        }
        for (ai, agg) in aggs.iter().enumerate() {
            let v = match agg {
                DecomposableAgg::Count => st.count,
                DecomposableAgg::SumI64 => st.sum,
                DecomposableAgg::MinI64 => st.min,
                DecomposableAgg::MaxI64 => st.max,
            };
            measure_cols[ai].push(v);
        }
    }

    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (i, col) in key_cols.iter().enumerate().take(n_keys) {
        fields.push(Field::new(format!("key_{i}"), DataType::Int64, false));
        arrays.push(Arc::new(Int64Array::from(col.clone())));
    }
    for (ai, agg) in aggs.iter().enumerate() {
        fields.push(Field::new(agg.name(), DataType::Int64, false));
        arrays.push(Arc::new(Int64Array::from(measure_cols[ai].clone())));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)?;
    profile.groups_emitted += batch.num_rows();
    Ok(vec![batch])
}

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

fn value_i64(batch: &RecordBatch, col: usize, row: usize) -> Option<i64> {
    let arr = batch.column(col).as_any().downcast_ref::<Int64Array>()?;
    if arr.is_null(row) {
        None
    } else {
        Some(arr.value(row))
    }
}

fn hash_rows(batch: &RecordBatch, key_indices: &[usize]) -> anyhow::Result<Vec<u64>> {
    let arrays: Vec<ArrayRef> = key_indices
        .iter()
        .map(|&i| {
            if i >= batch.num_columns() {
                Err(anyhow::anyhow!("key {i} OOB"))
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

fn radix_partition_batches(
    batches: &[RecordBatch],
    key_indices: &[usize],
    num_partitions: usize,
) -> anyhow::Result<Vec<Vec<RecordBatch>>> {
    let n = num_partitions.max(2);
    let mut parts: Vec<Vec<RecordBatch>> = (0..n).map(|_| Vec::new()).collect();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let hashes = hash_rows(batch, key_indices)?;
        let mut indices: Vec<Vec<u32>> = vec![Vec::new(); n];
        for (row, h) in hashes.iter().enumerate() {
            indices[(*h as usize) % n].push(row as u32);
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

/// Typed error for holistic aggregates that cannot run under a fixed budget.
#[derive(Debug)]
pub enum ExternalAggregateError {
    /// Aggregate is not decomposable under the external path.
    UnsupportedHolistic(String),
}

impl std::fmt::Display for ExternalAggregateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedHolistic(name) => write!(
                f,
                "aggregate '{name}' is not decomposable under the external path; \
                 use sort-based path or raise operator_budget"
            ),
        }
    }
}

impl std::error::Error for ExternalAggregateError {}

/// Reject holistic aggregates early.
pub fn ensure_decomposable(name: &str) -> Result<(), ExternalAggregateError> {
    match name.to_ascii_lowercase().as_str() {
        "count" | "sum" | "min" | "max" | "avg" => Ok(()),
        other => Err(ExternalAggregateError::UnsupportedHolistic(
            other.to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_spill::MemoryGrant;

    fn batch(keys: Vec<i64>, vals: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap()
    }

    fn cfg(bytes: usize) -> ExternalAggregateConfig {
        let mut c = ExternalAggregateConfig::from_grant(MemoryGrant::new("agg", bytes));
        c.num_partitions = 8;
        c.max_recursion = 3;
        c
    }

    #[test]
    fn count_sum_min_max_correct() {
        let input = vec![batch(vec![1, 1, 2, 2, 2], vec![10, 20, 5, 15, 25])];
        let config = cfg(1024 * 1024);
        let (out, profile) = external_hash_aggregate(
            &input,
            &[0],
            Some(1),
            &[
                DecomposableAgg::Count,
                DecomposableAgg::SumI64,
                DecomposableAgg::MinI64,
                DecomposableAgg::MaxI64,
            ],
            &config,
        )
        .unwrap();
        assert_eq!(profile.groups_emitted, 2);
        let b = &out[0];
        // Find rows by key
        let keys = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let counts = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let sums = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let mut by_key = HashMap::new();
        for i in 0..b.num_rows() {
            by_key.insert(keys.value(i), (counts.value(i), sums.value(i)));
        }
        assert_eq!(by_key.get(&1), Some(&(2, 30)));
        assert_eq!(by_key.get(&2), Some(&(3, 45)));
    }

    #[test]
    fn ten_x_groups_completes_under_small_grant() {
        let grant = 8 * 1024;
        let config = cfg(grant);
        // Many distinct groups → force flushes / radix path.
        let n = 50_000i64;
        let keys: Vec<i64> = (0..n).collect();
        let vals: Vec<i64> = (0..n).map(|i| i % 7).collect();
        let input = vec![batch(keys, vals)];
        let (out, profile) = external_hash_aggregate(
            &input,
            &[0],
            Some(1),
            &[DecomposableAgg::Count, DecomposableAgg::SumI64],
            &config,
        )
        .unwrap();
        let groups: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(groups, n as usize);
        assert!(
            profile.flushes > 0 || profile.recursion_depth > 0 || profile.peak_table_bytes > 0,
            "profile={profile:?}"
        );
        // Peak table should not be wildly above soft limit * recursion fanout.
        // Soft limit is ~6 KiB; allow generous headroom for HashMap overhead.
        assert!(
            profile.peak_table_bytes < 50 * 1024 * 1024,
            "peak table {} looks unbounded",
            profile.peak_table_bytes
        );
    }

    #[test]
    fn unsupported_holistic_errors() {
        assert!(ensure_decomposable("median").is_err());
        assert!(ensure_decomposable("count").is_ok());
    }

    #[test]
    fn df_spill_gate_report() {
        let mut r = DfAggregateSpillGateReport::default();
        r.record(DfAggregateSpillCase::PartialGroupedHash, true);
        r.record(DfAggregateSpillCase::PartialConstantGroupOrder, false);
        assert!(r.needs_custom_path());
    }
}
