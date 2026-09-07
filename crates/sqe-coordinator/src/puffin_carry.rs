//! Carry Puffin NDV statistics forward across non-append commits
//! (architecture review 512 / GitLab #604).
//!
//! The read side binds statistics to a snapshot id
//! (`SqeTableProvider` -> `PuffinNdvCache::for_snapshot`), so a snapshot
//! without its own `StatisticsFile` has no NDV estimates at all. Until this
//! module existed, sidecars were emitted only by append-shaped writes, and a
//! table lost its statistics on the first DELETE or the first compaction.
//!
//! Three shapes of commit, one helper:
//!
//! | op         | previous stats | new sketches | result                                   |
//! |------------|----------------|--------------|------------------------------------------|
//! | `Rewrite`  | yes            | --           | previous blobs re-emitted for the new snapshot (row identity unchanged) |
//! | `Rewrite`  | no             | --           | skipped                                  |
//! | `RowDelta` | yes            | --           | previous blobs re-emitted, `ndv` flagged as an upper bound |
//! | `Insert`   | no             | yes          | new sketches only (the pre-512 behaviour) |
//! | `Insert`   | yes            | yes          | per column: theta union of previous body and new sketch; if the previous body does not decode, `ndv = old + new` flagged as an upper bound |
//!
//! ## Why the body decodes
//!
//! `puffin_stats::serialise_theta_body` stores the retained 64-bit hashes
//! little-endian. That is a complete theta sketch except for `theta` itself,
//! which the writer now records in the `sqe.theta` blob property. Older
//! sidecars without that property derive theta from `retained / ndv`, which
//! is exact for sketches in exact mode and within rounding otherwise. Theta
//! union is then: take the smaller theta, keep every hash below it, and if
//! more than `k` survive, cut back to the `k` smallest and set theta to the
//! first hash dropped. No dependency on the `datasketches` crate's internals
//! is needed, so the result is written back in the same on-disk layout.
//!
//! ## Failure policy
//!
//! The data commit has already succeeded when this runs; nothing here can
//! or should roll it back. Every failure is logged, counted in
//! `sqe_puffin_sidecar_total{op,outcome}`, and the staged sidecar object is
//! deleted so no orphan `.stats` file remains. The `UpdateStatistics` commit
//! goes through [`crate::write_handler::commit_with_retry`] so a concurrent
//! writer landing between the data commit and the statistics commit no
//! longer loses the statistics.

use std::collections::HashMap;

use iceberg::spec::StatisticsFile;
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};
use sqe_catalog::puffin_stats::{
    ndv_blobs_from_metadata, puffin_stats_enabled, read_ndv_blobs, sketches_to_blobs,
    write_puffin_sidecar_from_blobs, ColumnSketches, NdvBlob, ThetaSketch, PROP_CARRIED_FROM,
    PROP_NDV_UPPER_BOUND, PROP_THETA,
};
use sqe_metrics::MetricsRegistry;
use tracing::{debug, info, warn};

use crate::commit::commit_with_retry;

/// Nominal entry count `k` of the writer's sketches: `ThetaSketch::builder()`
/// defaults to `lg_k = 12`. A union that retains more than this many hashes
/// is cut back to `k` and its theta lowered accordingly.
pub const NOMINAL_ENTRIES: usize = 4096;

/// Theta of a sketch in exact mode (Java-compatible signed maximum).
pub const MAX_THETA: u64 = i64::MAX as u64;

/// Metric label value for `sqe_puffin_sidecar_total{op}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarryOp {
    /// Compaction / `rewrite_data_files` / `rewrite_manifests`: the same rows
    /// under new file paths. Previous statistics stay exact.
    Rewrite,
    /// Append-shaped: rows were added, sketches for them exist.
    Insert,
    /// `INSERT OVERWRITE`: rows were added (sketched) and whole partitions
    /// were dropped (not sketched). The union of previous and new sketches
    /// is therefore only an upper bound.
    InsertOverwrite,
    /// DELETE / UPDATE / MERGE row deltas (CoW or MoR): rows were removed or
    /// changed, no sketch of the survivors exists. Previous NDV becomes an
    /// upper bound.
    RowDelta,
}

impl CarryOp {
    pub fn label(self) -> &'static str {
        match self {
            CarryOp::Rewrite => "rewrite",
            CarryOp::Insert => "insert",
            CarryOp::InsertOverwrite => "insert_overwrite",
            CarryOp::RowDelta => "row_delta",
        }
    }
}

/// A theta sketch reduced to what the Puffin body stores: sorted, distinct
/// retained hashes, all strictly below `theta`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawThetaSketch {
    theta: u64,
    hashes: Vec<u64>,
}

impl RawThetaSketch {
    /// Build from parts, normalising: hashes at or above `theta` are dropped,
    /// duplicates removed, order ascending.
    pub fn new(theta: u64, mut hashes: Vec<u64>) -> Self {
        let theta = theta.clamp(1, MAX_THETA);
        hashes.retain(|h| *h < theta);
        hashes.sort_unstable();
        hashes.dedup();
        Self { theta, hashes }
    }

    /// Snapshot a live sketch.
    pub fn from_sketch(sketch: &ThetaSketch) -> Self {
        Self::new(sketch.theta64(), sketch.iter().collect())
    }

    /// Decode a blob body written by `serialise_theta_body`.
    ///
    /// Returns `None` when the body is not a whole number of 64-bit hashes,
    /// or when it is empty while `ndv > 0` (a metadata-only blob whose body
    /// was never available: nothing to union with).
    pub fn decode(blob: &NdvBlob) -> Option<Self> {
        if blob.body.len() % 8 != 0 {
            return None;
        }
        if blob.body.is_empty() && blob.ndv > 0 {
            return None;
        }
        let hashes: Vec<u64> = blob
            .body
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().expect("chunks_exact(8) yields 8 bytes")))
            .collect();
        let theta = blob
            .properties
            .get(PROP_THETA)
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|t| *t > 0 && *t <= MAX_THETA)
            .unwrap_or_else(|| derive_theta(hashes.len(), blob.ndv));
        Some(Self::new(theta, hashes))
    }

    /// Theta union with nominal size `k`.
    pub fn union(&self, other: &Self, k: usize) -> Self {
        let k = k.max(1);
        let mut theta = self.theta.min(other.theta);
        let mut hashes: Vec<u64> = self
            .hashes
            .iter()
            .chain(other.hashes.iter())
            .copied()
            .filter(|h| *h < theta)
            .collect();
        hashes.sort_unstable();
        hashes.dedup();
        if hashes.len() > k {
            theta = hashes[k];
            hashes.truncate(k);
        }
        Self { theta, hashes }
    }

    /// Distinct-count estimate, rounded, mirroring `ThetaSketch::estimate`.
    pub fn estimate(&self) -> u64 {
        if self.hashes.is_empty() {
            return 0;
        }
        let theta = self.theta as f64 / MAX_THETA as f64;
        (self.hashes.len() as f64 / theta).round().max(0.0) as u64
    }

    pub fn theta(&self) -> u64 {
        self.theta
    }

    pub fn num_retained(&self) -> usize {
        self.hashes.len()
    }

    /// Serialise in the `serialise_theta_body` layout.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.hashes.len() * 8);
        for h in &self.hashes {
            out.extend_from_slice(&h.to_le_bytes());
        }
        out
    }

    /// Package as a writable blob, recording theta for the next decode.
    pub fn into_blob(self, field_id: i32, name: &str) -> NdvBlob {
        let mut properties = HashMap::new();
        properties.insert(PROP_THETA.to_string(), self.theta.to_string());
        NdvBlob {
            field_id,
            name: name.to_string(),
            ndv: self.estimate(),
            body: self.encode(),
            properties,
        }
    }
}

/// Recover theta for a body written before `sqe.theta` existed. Exact mode
/// (`retained >= ndv`) is `MAX_THETA`; otherwise `estimate = retained /
/// (theta / MAX_THETA)` inverted.
fn derive_theta(retained: usize, ndv: u64) -> u64 {
    if ndv == 0 || retained == 0 || retained as u64 >= ndv {
        return MAX_THETA;
    }
    let fraction = retained as f64 / ndv as f64;
    ((MAX_THETA as f64) * fraction).max(1.0) as u64
}

/// Statistics of the snapshot a commit replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviousStats {
    pub snapshot_id: i64,
    pub blobs: Vec<NdvBlob>,
}

fn mark_carried(blob: &mut NdvBlob, from_snapshot: i64, upper_bound: bool) {
    blob.properties
        .insert(PROP_CARRIED_FROM.to_string(), from_snapshot.to_string());
    if upper_bound {
        blob.properties
            .insert(PROP_NDV_UPPER_BOUND.to_string(), "true".to_string());
    }
}

/// The decision matrix from the module docs, as a pure function over blobs.
/// An empty result means "nothing to register".
pub fn merge_blobs(
    op: CarryOp,
    previous: Option<&PreviousStats>,
    new_sketches: Option<&ColumnSketches>,
) -> Vec<NdvBlob> {
    match op {
        CarryOp::Rewrite | CarryOp::RowDelta => {
            let Some(prev) = previous else {
                return Vec::new();
            };
            let upper_bound = op == CarryOp::RowDelta;
            prev.blobs
                .iter()
                .cloned()
                .map(|mut b| {
                    mark_carried(&mut b, prev.snapshot_id, upper_bound);
                    b
                })
                .collect()
        }
        CarryOp::InsertOverwrite => {
            // Union like an INSERT, then flag every blob: dropped partitions
            // may have held the only occurrences of some values.
            let mut out = merge_blobs(CarryOp::Insert, previous, new_sketches);
            if previous.is_some() {
                for b in &mut out {
                    b.properties
                        .insert(PROP_NDV_UPPER_BOUND.to_string(), "true".to_string());
                }
            }
            out
        }
        CarryOp::Insert => {
            let fresh: Vec<NdvBlob> = new_sketches.map(sketches_to_blobs).unwrap_or_default();
            let Some(prev) = previous else {
                return fresh;
            };
            let mut fresh_by_field: HashMap<i32, (NdvBlob, Option<RawThetaSketch>)> = fresh
                .into_iter()
                .map(|b| {
                    let raw = RawThetaSketch::decode(&b);
                    (b.field_id, (b, raw))
                })
                .collect();
            // Preserve the previous file's column order, then append columns
            // that only the new write sketched.
            let mut new_only_order: Vec<i32> = new_sketches
                .map(|s| s.iter().map(|(id, _, _, _)| *id).collect())
                .unwrap_or_default();
            let mut out = Vec::with_capacity(prev.blobs.len() + fresh_by_field.len());
            for old in &prev.blobs {
                let Some((new_blob, new_raw)) = fresh_by_field.remove(&old.field_id) else {
                    let mut carried = old.clone();
                    mark_carried(&mut carried, prev.snapshot_id, false);
                    out.push(carried);
                    continue;
                };
                new_only_order.retain(|id| *id != old.field_id);
                let merged = match (RawThetaSketch::decode(old), new_raw) {
                    (Some(old_raw), Some(new_raw)) => {
                        let mut merged = old_raw
                            .union(&new_raw, NOMINAL_ENTRIES)
                            .into_blob(old.field_id, &new_blob.name);
                        mark_carried(&mut merged, prev.snapshot_id, false);
                        merged
                    }
                    _ => {
                        // Body unavailable or undecodable on one side: keep
                        // the new body (it is at least a sketch of the new
                        // rows) and publish the only bound we can defend.
                        let mut fallback = new_blob;
                        fallback.ndv = old.ndv.saturating_add(fallback.ndv);
                        mark_carried(&mut fallback, prev.snapshot_id, true);
                        fallback
                    }
                };
                out.push(merged);
            }
            for id in new_only_order {
                if let Some((b, _)) = fresh_by_field.remove(&id) {
                    out.push(b);
                }
            }
            out
        }
    }
}

/// Commit a `StatisticsFile` through the shared optimistic-concurrency retry
/// loop. Each attempt reloads the table and rebuilds the `UpdateStatistics`
/// action; the statistics file is bound to a snapshot id, so it stays valid
/// even when the table has moved on since the data commit.
pub async fn commit_statistics_with_retry(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    stats_file: StatisticsFile,
) -> Result<IcebergTable, iceberg::Error> {
    commit_with_retry(catalog, table_ident, "update-statistics", move |fresh| {
        let stats_file = stats_file.clone();
        async move {
            let tx = Transaction::new(&fresh);
            let tx = tx
                .update_statistics()
                .set_statistics(stats_file)
                .apply(tx)?;
            tx.commit(catalog).await
        }
    })
    .await
}

fn record(metrics: Option<&MetricsRegistry>, op: CarryOp, outcome: &'static str) -> &'static str {
    if let Some(m) = metrics {
        m.puffin_sidecar_total
            .with_label_values(&[op.label(), outcome])
            .inc();
    }
    outcome
}

/// Register NDV statistics for the snapshot `committed` points at, derived
/// from the parent snapshot's statistics and/or `new_sketches` according to
/// `op`. Returns the outcome label that was recorded in
/// `sqe_puffin_sidecar_total`.
///
/// `committed` is the table as returned by the data commit; its current
/// snapshot is the one to annotate and that snapshot's parent is the one to
/// carry from. No reload happens here, so a concurrent commit landing after
/// ours cannot make us annotate the wrong snapshot.
pub async fn carry_forward_statistics(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    committed: &IcebergTable,
    op: CarryOp,
    new_sketches: Option<&ColumnSketches>,
    metrics: Option<&MetricsRegistry>,
) -> &'static str {
    let metadata = committed.metadata();
    if !puffin_stats_enabled(metadata.properties()) {
        return "disabled";
    }
    let Some(snapshot) = metadata.current_snapshot() else {
        return record(metrics, op, "skipped_no_snapshot");
    };
    let snapshot_id = snapshot.snapshot_id();
    if metadata.statistics_for_snapshot(snapshot_id).is_some() {
        return record(metrics, op, "skipped_already_present");
    }
    let sequence_number = snapshot.sequence_number();

    let previous = match snapshot
        .parent_snapshot_id()
        .and_then(|parent| metadata.statistics_for_snapshot(parent))
    {
        Some(prev_file) => {
            let blobs = match read_ndv_blobs(committed.file_io(), prev_file).await {
                Ok(blobs) => blobs,
                Err(e) => {
                    warn!(
                        table = %table_ident,
                        path = %prev_file.statistics_path,
                        error = %e,
                        "puffin: previous sidecar unreadable; carrying ndv as an upper bound"
                    );
                    ndv_blobs_from_metadata(prev_file)
                        .into_iter()
                        .map(|mut b| {
                            b.properties
                                .insert(PROP_NDV_UPPER_BOUND.to_string(), "true".to_string());
                            b
                        })
                        .collect()
                }
            };
            Some(PreviousStats {
                snapshot_id: prev_file.snapshot_id,
                blobs,
            })
        }
        None => None,
    };

    let blobs = merge_blobs(op, previous.as_ref(), new_sketches);
    if blobs.is_empty() {
        debug!(table = %table_ident, op = op.label(), "puffin: nothing to carry forward");
        return record(metrics, op, "skipped_no_previous");
    }

    let base_dir = match committed.metadata_location() {
        Some(loc) => loc
            .rsplit_once('/')
            .map(|(head, _)| head)
            .unwrap_or(loc)
            .to_string(),
        None => format!("{}/metadata", metadata.location().trim_end_matches('/')),
    };

    let stats_file = match write_puffin_sidecar_from_blobs(
        committed.file_io(),
        &base_dir,
        &blobs,
        snapshot_id,
        sequence_number,
    )
    .await
    {
        Ok(f) => f,
        Err(e) => {
            warn!(table = %table_ident, error = %e, "puffin: sidecar write failed");
            return record(metrics, op, "write_failed");
        }
    };

    match commit_statistics_with_retry(catalog, table_ident, stats_file.clone()).await {
        Ok(_) => {
            info!(
                table = %table_ident,
                op = op.label(),
                snapshot_id,
                blobs = blobs.len(),
                "puffin: statistics registered"
            );
            record(metrics, op, "registered")
        }
        Err(e) => {
            warn!(
                table = %table_ident,
                error = %e,
                path = %stats_file.statistics_path,
                "puffin: statistics commit failed; deleting staged sidecar"
            );
            if let Err(del) = committed
                .file_io()
                .delete(&stats_file.statistics_path)
                .await
            {
                warn!(
                    path = %stats_file.statistics_path,
                    error = %del,
                    "puffin: orphan sidecar delete failed; requires manual cleanup"
                );
            }
            record(metrics, op, "commit_failed")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sketch_of(range: std::ops::Range<u64>) -> ThetaSketch {
        let mut s = ThetaSketch::builder().build();
        for v in range {
            s.update(v);
        }
        s
    }

    fn column(field_id: i32, name: &str, range: std::ops::Range<u64>) -> ColumnSketches {
        let s = sketch_of(range);
        let ndv = s.estimate().round() as u64;
        vec![(field_id, name.to_string(), s, ndv)]
    }

    fn blob_of(field_id: i32, name: &str, range: std::ops::Range<u64>) -> NdvBlob {
        let s = sketch_of(range);
        let ndv = s.estimate().round() as u64;
        NdvBlob::from_sketch(field_id, name, &s, ndv)
    }

    fn within(actual: u64, expected: u64, pct: f64) -> bool {
        let tol = expected as f64 * pct;
        (actual as f64 - expected as f64).abs() <= tol
    }

    // -- RawThetaSketch -----------------------------------------------------

    #[test]
    fn exact_mode_union_counts_the_true_distinct_set() {
        let a = RawThetaSketch::from_sketch(&sketch_of(0..1000));
        let b = RawThetaSketch::from_sketch(&sketch_of(500..1500));
        let u = a.union(&b, NOMINAL_ENTRIES);
        assert_eq!(u.theta(), MAX_THETA, "still exact: 1500 < k");
        assert_eq!(u.num_retained(), 1500);
        assert_eq!(u.estimate(), 1500);
    }

    #[test]
    fn union_is_commutative_and_idempotent() {
        let a = RawThetaSketch::from_sketch(&sketch_of(0..3000));
        let b = RawThetaSketch::from_sketch(&sketch_of(2000..5000));
        assert_eq!(a.union(&b, NOMINAL_ENTRIES), b.union(&a, NOMINAL_ENTRIES));
        assert_eq!(a.union(&a, NOMINAL_ENTRIES), a);
    }

    #[test]
    fn estimation_mode_union_stays_within_tolerance_and_nominal_size() {
        let a = RawThetaSketch::from_sketch(&sketch_of(0..100_000));
        let b = RawThetaSketch::from_sketch(&sketch_of(50_000..150_000));
        let u = a.union(&b, NOMINAL_ENTRIES);
        assert!(u.num_retained() <= NOMINAL_ENTRIES);
        assert!(u.theta() < MAX_THETA);
        assert!(
            within(u.estimate(), 150_000, 0.05),
            "estimate {} not within 5% of 150000",
            u.estimate()
        );
        // Union never estimates below the larger input.
        assert!(u.estimate() >= a.estimate().min(b.estimate()));
    }

    #[test]
    fn blob_round_trip_is_lossless_with_theta_property() {
        let live = sketch_of(0..200_000);
        let raw = RawThetaSketch::from_sketch(&live);
        let blob = raw.clone().into_blob(7, "x");
        assert_eq!(
            blob.properties.get(PROP_THETA),
            Some(&raw.theta().to_string())
        );
        let decoded = RawThetaSketch::decode(&blob).expect("decodes");
        assert_eq!(decoded, raw);
        assert_eq!(blob.ndv, raw.estimate());
        // And matches the live sketch's own estimate.
        assert_eq!(blob.ndv, live.estimate().round() as u64);
    }

    #[test]
    fn decode_without_theta_property_derives_it_from_ndv() {
        // Exact mode: retained == ndv -> MAX_THETA.
        let mut exact = blob_of(1, "a", 0..100);
        exact.properties.remove(PROP_THETA);
        let d = RawThetaSketch::decode(&exact).expect("decodes");
        assert_eq!(d.theta(), MAX_THETA);
        assert_eq!(d.estimate(), 100);

        // Estimation mode: theta recovered from retained / ndv.
        let mut est = blob_of(2, "b", 0..300_000);
        est.properties.remove(PROP_THETA);
        let d = RawThetaSketch::decode(&est).expect("decodes");
        assert!(d.theta() < MAX_THETA);
        assert!(
            (d.estimate() as i64 - est.ndv as i64).abs() <= 1,
            "derived {} vs stored {}",
            d.estimate(),
            est.ndv
        );
    }

    #[test]
    fn decode_rejects_undecodable_bodies() {
        let mut odd = blob_of(1, "a", 0..10);
        odd.body.push(0xff);
        assert!(
            RawThetaSketch::decode(&odd).is_none(),
            "not a multiple of 8"
        );

        let metadata_only = NdvBlob {
            field_id: 1,
            name: "a".into(),
            body: Vec::new(),
            ndv: 42,
            properties: HashMap::new(),
        };
        assert!(
            RawThetaSketch::decode(&metadata_only).is_none(),
            "empty body with ndv > 0 has nothing to union"
        );

        let empty = NdvBlob {
            ndv: 0,
            ..metadata_only
        };
        let d = RawThetaSketch::decode(&empty).expect("an empty sketch is valid");
        assert_eq!(d.estimate(), 0);
    }

    #[test]
    fn new_normalises_hashes_against_theta() {
        let raw = RawThetaSketch::new(100, vec![300, 5, 5, 99, 100, 1]);
        assert_eq!(raw.num_retained(), 3);
        assert_eq!(raw.encode().len(), 24);
        let decoded: Vec<u64> = raw
            .encode()
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(decoded, vec![1, 5, 99]);
    }

    // -- merge_blobs decision matrix ----------------------------------------

    fn previous(snapshot_id: i64, blobs: Vec<NdvBlob>) -> PreviousStats {
        PreviousStats { snapshot_id, blobs }
    }

    #[test]
    fn rewrite_without_previous_emits_nothing() {
        assert!(merge_blobs(CarryOp::Rewrite, None, None).is_empty());
        assert!(merge_blobs(CarryOp::RowDelta, None, None).is_empty());
    }

    #[test]
    fn rewrite_carries_previous_blobs_exactly() {
        let prev = previous(
            11,
            vec![blob_of(1, "a", 0..500), blob_of(2, "b", 0..70_000)],
        );
        let out = merge_blobs(CarryOp::Rewrite, Some(&prev), None);
        assert_eq!(out.len(), 2);
        for (o, p) in out.iter().zip(prev.blobs.iter()) {
            assert_eq!((o.field_id, o.ndv), (p.field_id, p.ndv));
            assert_eq!(o.body, p.body, "body carried byte-for-byte");
            assert_eq!(o.properties.get(PROP_CARRIED_FROM), Some(&"11".to_string()));
            assert!(
                !o.properties.contains_key(PROP_NDV_UPPER_BOUND),
                "compaction does not change row identity: ndv stays exact"
            );
        }
    }

    #[test]
    fn row_delta_carries_previous_ndv_as_upper_bound() {
        let prev = previous(5, vec![blob_of(1, "a", 0..500)]);
        let out = merge_blobs(CarryOp::RowDelta, Some(&prev), None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ndv, 500);
        assert_eq!(
            out[0].properties.get(PROP_NDV_UPPER_BOUND),
            Some(&"true".to_string())
        );
        assert_eq!(
            out[0].properties.get(PROP_CARRIED_FROM),
            Some(&"5".to_string())
        );
    }

    #[test]
    fn insert_without_previous_is_the_fresh_sketches() {
        let new = column(1, "a", 0..300);
        let out = merge_blobs(CarryOp::Insert, None, Some(&new));
        assert_eq!(out, sketches_to_blobs(&new));
        assert!(merge_blobs(CarryOp::Insert, None, None).is_empty());
    }

    #[test]
    fn insert_unions_previous_body_with_new_sketch() {
        let prev = previous(3, vec![blob_of(1, "a", 0..1000)]);
        let new = column(1, "a", 500..1500);
        let out = merge_blobs(CarryOp::Insert, Some(&prev), Some(&new));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ndv, 1500, "exact union of overlapping ranges");
        assert!(!out[0].properties.contains_key(PROP_NDV_UPPER_BOUND));
        assert_eq!(
            out[0].properties.get(PROP_CARRIED_FROM),
            Some(&"3".to_string())
        );
        assert!(out[0].properties.contains_key(PROP_THETA));
        // The merged body decodes again, so the next INSERT can union too.
        let again = RawThetaSketch::decode(&out[0]).expect("merged body decodes");
        assert_eq!(again.estimate(), 1500);
    }

    #[test]
    fn insert_with_undecodable_previous_body_publishes_sum_as_upper_bound() {
        let mut broken = blob_of(1, "a", 0..1000);
        broken.body.truncate(3);
        let prev = previous(9, vec![broken]);
        let new = column(1, "a", 0..100);
        let out = merge_blobs(CarryOp::Insert, Some(&prev), Some(&new));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ndv, 1100, "old + new is the only defensible bound");
        assert_eq!(
            out[0].properties.get(PROP_NDV_UPPER_BOUND),
            Some(&"true".to_string())
        );
        // The body is the new sketch's, so it still decodes.
        assert!(RawThetaSketch::decode(&out[0]).is_some());
    }

    #[test]
    fn insert_keeps_previous_only_columns_and_appends_new_only_columns() {
        let prev = previous(2, vec![blob_of(1, "a", 0..10), blob_of(2, "b", 0..20)]);
        let mut new = column(2, "b", 20..25);
        new.extend(column(3, "c", 0..7));
        let out = merge_blobs(CarryOp::Insert, Some(&prev), Some(&new));
        let ids: Vec<i32> = out.iter().map(|b| b.field_id).collect();
        assert_eq!(ids, vec![1, 2, 3], "previous order first, then new-only");
        assert_eq!(out[0].ndv, 10);
        assert_eq!(
            out[0].properties.get(PROP_CARRIED_FROM),
            Some(&"2".to_string())
        );
        assert_eq!(out[1].ndv, 25);
        assert_eq!(out[2].ndv, 7);
        assert!(!out[2].properties.contains_key(PROP_CARRIED_FROM));
    }

    #[test]
    fn carry_op_labels_are_stable_metric_values() {
        assert_eq!(CarryOp::Rewrite.label(), "rewrite");
        assert_eq!(CarryOp::Insert.label(), "insert");
        assert_eq!(CarryOp::InsertOverwrite.label(), "insert_overwrite");
        assert_eq!(CarryOp::RowDelta.label(), "row_delta");
    }

    #[test]
    fn insert_overwrite_unions_but_flags_the_result_as_an_upper_bound() {
        let prev = previous(4, vec![blob_of(1, "a", 0..1000)]);
        let new = column(1, "a", 500..1500);
        let out = merge_blobs(CarryOp::InsertOverwrite, Some(&prev), Some(&new));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ndv, 1500);
        assert_eq!(
            out[0].properties.get(PROP_NDV_UPPER_BOUND),
            Some(&"true".to_string())
        );
        // Without previous statistics it is a plain fresh sketch.
        let fresh = merge_blobs(CarryOp::InsertOverwrite, None, Some(&new));
        assert!(!fresh[0].properties.contains_key(PROP_NDV_UPPER_BOUND));
    }
}
