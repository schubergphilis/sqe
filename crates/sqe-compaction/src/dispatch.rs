//! Pure, catalog-free primitives for the coordinator's distributed
//! `compact_file_group` job runner (Phase 4c Task 4).
//!
//! Everything here is deterministic and network-free so it can be unit
//! tested without a live Flight server or catalog: worker placement
//! (largest-first, bounded by a per-worker cap), decoding a worker's
//! Avro-encoded `DataFile`s, and re-running the global `added_rows <=
//! removed_rows` invariant before the coordinator is allowed to commit.
//!
//! What stays OUT of this module: the actual Arrow Flight `do_action` RPC
//! (needs `tonic`/`arrow-flight`, which this crate does not depend on --
//! workers must not pull in a coordinator-shaped Flight *client*), the
//! `WorkerRegistry`/`WorkerLoadTracker` bookkeeping, and the
//! `RewriteFilesAction` commit. Those live in
//! `sqe_coordinator::compaction_dispatch` and
//! `sqe_coordinator::maintenance`, which call into this module with
//! already-resolved inputs (worker load snapshots, decoded responses).

use std::collections::HashSet;

use iceberg::spec::{DataFile, FormatVersion, Schema, StructType};
use sqe_core::SqeError;

use crate::wire::CompactGroupResponse;

/// A worker's current load, as seen by the coordinator right before a
/// placement decision. `in_flight` should come from
/// `WorkerLoadTracker::in_flight` (or an equivalent live count); this module
/// never talks to a registry itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerLoad {
    pub url: String,
    pub in_flight: usize,
}

/// One group's placement decision: which input index (into the caller's
/// group list) was assigned to which worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupPlacement {
    /// Index into the `sizes` slice passed to [`place_groups_largest_first`].
    pub group_index: usize,
    pub worker_url: String,
}

/// Result of one placement pass: `placed` groups (assigned this wave) plus
/// `deferred` groups (their index) that did not fit because every worker was
/// already at `max_inflight_per_worker`. Callers loop: dispatch `placed`,
/// wait for capacity to free up (a dispatch completing), then call again
/// with the updated load snapshot for `deferred`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlacementPlan {
    pub placed: Vec<GroupPlacement>,
    pub deferred: Vec<usize>,
}

/// Largest-first bin-packing placement, bounded by `max_inflight_per_worker`.
///
/// Mirrors `sqe_coordinator::scheduler::WeightedScheduler`'s heuristic
/// (sort work descending by cost, assign each to the currently
/// least-loaded worker) with one addition WeightedScheduler does not need
/// for scan fragments: a hard per-worker cap. A worker at or above the cap is
/// never chosen; if every worker is at cap, the group is deferred rather than
/// force-assigned, so compaction never saturates a worker beyond
/// `max_inflight_groups_per_worker`.
///
/// `workers` is assumed to already be filtered to healthy workers; this
/// function does not know about health. Ties in load are broken by worker
/// position (first wins), and ties in size preserve input order, so the
/// result is deterministic for a given input.
pub fn place_groups_largest_first(
    sizes: &[u64],
    workers: &[WorkerLoad],
    max_inflight_per_worker: usize,
) -> PlacementPlan {
    if workers.is_empty() || sizes.is_empty() {
        return PlacementPlan {
            placed: Vec::new(),
            deferred: (0..sizes.len()).collect(),
        };
    }

    let mut loads: Vec<usize> = workers.iter().map(|w| w.in_flight).collect();
    let mut order: Vec<usize> = (0..sizes.len()).collect();
    // Stable sort: equal sizes keep their original relative order.
    order.sort_by_key(|&i| std::cmp::Reverse(sizes[i]));

    let mut placed = Vec::new();
    let mut deferred = Vec::new();
    for i in order {
        let candidate = loads
            .iter()
            .enumerate()
            .filter(|(_, &load)| load < max_inflight_per_worker)
            .min_by_key(|(_, &load)| load)
            .map(|(idx, _)| idx);
        match candidate {
            Some(idx) => {
                loads[idx] += 1;
                placed.push(GroupPlacement {
                    group_index: i,
                    worker_url: workers[idx].url.clone(),
                });
            }
            None => deferred.push(i),
        }
    }

    // Restore ascending group-index order; the largest-first order above was
    // only needed for the assignment decision itself.
    placed.sort_by_key(|p| p.group_index);
    deferred.sort_unstable();
    PlacementPlan { placed, deferred }
}

/// Pick a single least-loaded worker under `max_inflight_per_worker`,
/// skipping any URL in `excluded`. Used for retry placement: a group that
/// already failed on one worker must land on a *different* healthy worker
/// (or defer, if none remain), which the batch
/// [`place_groups_largest_first`] cannot express per-group.
///
/// Returns `None` when no eligible worker exists (all excluded, or all at
/// cap).
pub fn least_loaded_worker(
    workers: &[WorkerLoad],
    max_inflight_per_worker: usize,
    excluded: &HashSet<String>,
) -> Option<String> {
    workers
        .iter()
        .filter(|w| !excluded.contains(&w.url) && w.in_flight < max_inflight_per_worker)
        .min_by_key(|w| w.in_flight)
        .map(|w| w.url.clone())
}

/// One pending group's inputs to the continuous-scheduler refill decision
/// [`next_group_assignment`]: its total size (for largest-first priority)
/// and its exclusion set (worker URLs it must not land on again, e.g. after
/// a prior attempt already failed on that worker).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSlot {
    pub total_bytes: u64,
    pub excluded: HashSet<String>,
}

/// Continuous-scheduler refill decision: pick exactly ONE (pending group,
/// worker) assignment for the next free slot, given every group still
/// waiting to be dispatched and each healthy worker's current in-flight
/// count.
///
/// Unlike [`place_groups_largest_first`], which plans a whole wave up front
/// and requires the caller to wait for the whole wave to drain before
/// planning the next one, this is meant to be called every time a slot
/// might have opened up (a dispatch just completed, or a fresh pass at
/// startup) so a worker that finishes early is refilled immediately instead
/// of idling until the slowest group in a shared wave finishes. The caller
/// re-snapshots `workers`' `in_flight` counts (and removes the assigned
/// group from `pending`) before calling again.
///
/// Priority rules, matching [`place_groups_largest_first`]/
/// [`least_loaded_worker`] exactly: the largest pending group is considered
/// first; among workers eligible for that group (under
/// `max_inflight_per_worker` and not in the group's `excluded` set), the
/// least-loaded one wins, ties broken by position. A group with no eligible
/// worker right now is skipped in favor of the next-largest group that does
/// have one (this is what lets a retried group's exclusion set coexist with
/// fresh groups still being placeable). Ties in size preserve input order
/// (stable sort), same as [`place_groups_largest_first`].
///
/// Returns `None` when no pending group can be placed on any worker right
/// now -- every eligible worker is at cap for every remaining group, or
/// every worker is excluded for every remaining group. The caller should
/// back off and retry once something changes (a slot frees, or the healthy
/// set changes).
pub fn next_group_assignment(
    pending: &[PendingSlot],
    workers: &[WorkerLoad],
    max_inflight_per_worker: usize,
) -> Option<(usize, String)> {
    let mut order: Vec<usize> = (0..pending.len()).collect();
    // Stable sort: equal sizes keep their original relative order, matching
    // place_groups_largest_first's tie-break.
    order.sort_by_key(|&i| std::cmp::Reverse(pending[i].total_bytes));
    for i in order {
        if let Some(url) =
            least_loaded_worker(workers, max_inflight_per_worker, &pending[i].excluded)
        {
            return Some((i, url));
        }
    }
    None
}

/// One group's decoded rewrite result: the worker's Avro-encoded
/// `DataFile`s, resolved into real `DataFile`s against the coordinator's own
/// view of the table (schema, partition type/spec id, format version -- none
/// of which rides in [`CompactGroupResponse`], see its doc comment).
#[derive(Debug, Clone)]
pub struct GroupOutcome {
    pub group_id: u32,
    pub new_files: Vec<DataFile>,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub uploaded_paths: Vec<String>,
}

/// Decode a worker's [`CompactGroupResponse`] into a [`GroupOutcome`].
///
/// `schema`/`partition_spec_id`/`partition_type`/`format_version` must come
/// from the SAME table load the coordinator planned the group against (its
/// `default_partition_type()`/`default_partition_spec_id()`/
/// `format_version()`/`current_schema()`), matching exactly what the worker
/// encoded with in `compact_pinned_table` (`sqe-worker/src/compaction.rs`).
pub fn decode_group_response(
    response: &CompactGroupResponse,
    schema: &Schema,
    partition_spec_id: i32,
    partition_type: &StructType,
    format_version: FormatVersion,
) -> sqe_core::Result<GroupOutcome> {
    let new_files = iceberg::spec::read_data_files_from_avro(
        &mut response.new_data_files_avro.as_slice(),
        schema,
        partition_spec_id,
        partition_type,
        format_version,
    )
    .map_err(|e| {
        SqeError::Execution(format!(
            "compact_file_group: failed to decode group {}'s data files from avro: {e}",
            response.group_id
        ))
    })?;
    Ok(GroupOutcome {
        group_id: response.group_id,
        new_files,
        rows_written: response.rows_written,
        bytes_written: response.bytes_written,
        uploaded_paths: response.uploaded_paths.clone(),
    })
}

/// Every group's decoded outcome combined into the single set of `DataFile`s
/// the coordinator commits, plus the aggregate counts needed for the
/// `RewriteOutcome` summary and the row invariant below.
#[derive(Debug, Clone, Default)]
pub struct AggregatedRewrite {
    pub new_files: Vec<DataFile>,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub added_rows: u64,
    pub removed_rows: u64,
    pub uploaded_paths: Vec<String>,
}

/// Combine every group's [`GroupOutcome`] into one [`AggregatedRewrite`],
/// then re-run the global row-count invariant that
/// `sqe_coordinator::maintenance::rewrite_data_files_once` enforces for the
/// coordinator-local path: a rewrite can never manufacture rows, so
/// `added_rows` (summed over every new file) must never exceed `removed_rows`
/// (summed over every OLD file being replaced, i.e. `old_files`, the union of
/// every dispatched group's input data files).
///
/// The per-group `expected_rows_after_deletes` cross-check already ran on
/// the WORKER inside `rewrite_group` before it returned; this is the
/// coordinator-side backstop for the whole job, run once against everything
/// before the single atomic commit -- an error here must abort before any
/// commit is attempted.
pub fn aggregate_group_outcomes(
    outcomes: Vec<GroupOutcome>,
    old_files: &[DataFile],
) -> sqe_core::Result<AggregatedRewrite> {
    let mut new_files = Vec::new();
    let mut rows_written = 0u64;
    let mut bytes_written = 0u64;
    let mut uploaded_paths = Vec::new();
    for outcome in outcomes {
        rows_written += outcome.rows_written;
        bytes_written += outcome.bytes_written;
        uploaded_paths.extend(outcome.uploaded_paths);
        new_files.extend(outcome.new_files);
    }

    let removed_rows: u64 = old_files.iter().map(|f| f.record_count()).sum();
    let added_rows: u64 = new_files.iter().map(|f| f.record_count()).sum();
    if added_rows > removed_rows {
        return Err(SqeError::Execution(format!(
            "distributed compaction row-count invariant violated: added={added_rows} exceeds \
             removed={removed_rows} (rows_written across groups={rows_written}); a rewrite \
             cannot increase row count; aborting before commit"
        )));
    }

    Ok(AggregatedRewrite {
        new_files,
        rows_written,
        bytes_written,
        added_rows,
        removed_rows,
        uploaded_paths,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{
        write_data_files_to_avro, DataContentType, DataFileBuilder, DataFileFormat, NestedField,
        PrimitiveType, Struct,
    };

    fn worker(url: &str, in_flight: usize) -> WorkerLoad {
        WorkerLoad {
            url: url.to_string(),
            in_flight,
        }
    }

    // ---- place_groups_largest_first ------------------------------------

    #[test]
    fn placement_assigns_largest_groups_first_and_defers_the_rest_under_cap() {
        // 3 workers, cap 1 each => capacity for exactly 3 groups this wave.
        let workers = vec![worker("w1", 0), worker("w2", 0), worker("w3", 0)];
        let sizes = [50u64, 40, 30, 20, 10];
        let plan = place_groups_largest_first(&sizes, &workers, 1);

        assert_eq!(plan.placed.len(), 3, "capacity for exactly 3 groups");
        assert_eq!(plan.deferred, vec![3, 4], "the two smallest groups defer");

        let placed_indices: HashSet<usize> = plan.placed.iter().map(|p| p.group_index).collect();
        assert_eq!(
            placed_indices,
            HashSet::from([0, 1, 2]),
            "the three largest groups (indices 0,1,2) are placed, not the smallest"
        );

        // Every worker got exactly one group: the cap was respected.
        let mut per_worker: std::collections::HashMap<&str, usize> =
            std::collections::HashMap::new();
        for p in &plan.placed {
            *per_worker.entry(p.worker_url.as_str()).or_default() += 1;
        }
        for count in per_worker.values() {
            assert_eq!(*count, 1, "no worker should exceed the cap of 1");
        }
        assert_eq!(
            per_worker.len(),
            3,
            "all three workers should have been used"
        );
    }

    #[test]
    fn placement_never_exceeds_cap_even_with_many_more_groups_than_capacity() {
        let workers = vec![worker("w1", 0), worker("w2", 0)];
        let sizes: Vec<u64> = (1..=10).collect();
        let plan = place_groups_largest_first(&sizes, &workers, 2);

        // Capacity is workers.len() * cap = 4.
        assert_eq!(plan.placed.len(), 4);
        assert_eq!(plan.deferred.len(), 6);

        let mut per_worker: std::collections::HashMap<&str, usize> =
            std::collections::HashMap::new();
        for p in &plan.placed {
            *per_worker.entry(p.worker_url.as_str()).or_default() += 1;
        }
        for count in per_worker.values() {
            assert!(
                *count <= 2,
                "no worker may exceed max_inflight_per_worker=2"
            );
        }
    }

    #[test]
    fn placement_accounts_for_pre_existing_in_flight_load() {
        // w1 already has 1 in flight (of cap 1): only w2 has room.
        let workers = vec![worker("w1", 1), worker("w2", 0)];
        let sizes = [100u64, 200];
        let plan = place_groups_largest_first(&sizes, &workers, 1);

        assert_eq!(plan.placed.len(), 1);
        assert_eq!(plan.placed[0].worker_url, "w2");
        // The larger group (index 1, size 200) is placed first; the smaller
        // one defers since w1 has no room and w2 is now full too.
        assert_eq!(plan.placed[0].group_index, 1);
        assert_eq!(plan.deferred, vec![0]);
    }

    #[test]
    fn placement_defers_everything_when_no_workers_are_available() {
        let plan = place_groups_largest_first(&[10, 20, 30], &[], 4);
        assert!(plan.placed.is_empty());
        assert_eq!(plan.deferred, vec![0, 1, 2]);
    }

    #[test]
    fn placement_is_a_noop_on_empty_group_list() {
        let workers = vec![worker("w1", 0)];
        let plan = place_groups_largest_first(&[], &workers, 1);
        assert!(plan.placed.is_empty());
        assert!(plan.deferred.is_empty());
    }

    // ---- least_loaded_worker --------------------------------------------

    #[test]
    fn least_loaded_worker_picks_the_lowest_load_under_cap() {
        let workers = vec![worker("w1", 2), worker("w2", 0), worker("w3", 1)];
        let picked = least_loaded_worker(&workers, 3, &HashSet::new());
        assert_eq!(picked, Some("w2".to_string()));
    }

    #[test]
    fn least_loaded_worker_skips_excluded_workers() {
        let workers = vec![worker("w1", 0), worker("w2", 1)];
        let mut excluded = HashSet::new();
        excluded.insert("w1".to_string());
        let picked = least_loaded_worker(&workers, 5, &excluded);
        assert_eq!(picked, Some("w2".to_string()));
    }

    #[test]
    fn least_loaded_worker_returns_none_when_all_are_saturated_or_excluded() {
        let workers = vec![worker("w1", 2), worker("w2", 1)];
        let mut excluded = HashSet::new();
        excluded.insert("w2".to_string());
        // w1 is at cap (2 >= max 2), w2 is excluded.
        let picked = least_loaded_worker(&workers, 2, &excluded);
        assert_eq!(picked, None);
    }

    // ---- next_group_assignment (continuous refill decision) -------------

    fn slot(total_bytes: u64) -> PendingSlot {
        PendingSlot {
            total_bytes,
            excluded: HashSet::new(),
        }
    }

    fn slot_excluding(total_bytes: u64, excluded: &[&str]) -> PendingSlot {
        PendingSlot {
            total_bytes,
            excluded: excluded.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn next_assignment_picks_the_largest_pending_group_for_a_free_worker() {
        let pending = vec![slot(100), slot(500), slot(200)];
        let workers = vec![worker("w1", 0), worker("w2", 0)];
        let (idx, url) = next_group_assignment(&pending, &workers, 1).unwrap();
        assert_eq!(idx, 1, "the 500-byte group is largest and goes first");
        assert_eq!(url, "w1", "ties in load broken by worker position");
    }

    #[test]
    fn next_assignment_skips_a_worker_at_cap() {
        let pending = vec![slot(100)];
        // w1 is already at the cap of 1; only w2 has room.
        let workers = vec![worker("w1", 1), worker("w2", 0)];
        let (idx, url) = next_group_assignment(&pending, &workers, 1).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(url, "w2");
    }

    #[test]
    fn next_assignment_returns_none_when_every_worker_is_at_cap() {
        let pending = vec![slot(100), slot(200)];
        let workers = vec![worker("w1", 2), worker("w2", 2)];
        assert_eq!(next_group_assignment(&pending, &workers, 2), None);
    }

    #[test]
    fn next_assignment_returns_none_on_an_empty_pending_list() {
        let workers = vec![worker("w1", 0)];
        assert_eq!(next_group_assignment(&[], &workers, 1), None);
    }

    #[test]
    fn next_assignment_returns_none_with_no_workers() {
        let pending = vec![slot(100)];
        assert_eq!(next_group_assignment(&pending, &[], 1), None);
    }

    #[test]
    fn next_assignment_falls_through_a_stuck_retry_to_a_smaller_placeable_group() {
        // The largest (retried) group is excluded from the only worker; the
        // continuous scheduler must not stall the whole queue on it -- it
        // should place the next-largest group that CAN go somewhere.
        let pending = vec![slot_excluding(500, &["w1"]), slot(100)];
        let workers = vec![worker("w1", 0)];
        let (idx, url) = next_group_assignment(&pending, &workers, 1).unwrap();
        assert_eq!(idx, 1, "falls through to the smaller, placeable group");
        assert_eq!(url, "w1");
    }

    #[test]
    fn next_assignment_returns_none_when_all_pending_groups_are_excluded_from_every_worker() {
        let pending = vec![slot_excluding(500, &["w1", "w2"])];
        let workers = vec![worker("w1", 0), worker("w2", 0)];
        assert_eq!(next_group_assignment(&pending, &workers, 1), None);
    }

    #[test]
    fn next_assignment_places_every_group_exactly_once_respecting_the_cap() {
        // Simulates the real usage pattern: call repeatedly, removing the
        // assigned group and bumping that worker's in-flight count each
        // time, until no more assignments are possible.
        let mut in_flight: std::collections::HashMap<String, usize> =
            [("w1".to_string(), 0), ("w2".to_string(), 0)]
                .into_iter()
                .collect();
        let sizes = [50u64, 40, 30, 20, 10, 5];
        let mut pending: Vec<PendingSlot> = sizes.iter().map(|&b| slot(b)).collect();
        let cap = 2;
        let mut placements: Vec<(u64, String)> = Vec::new();

        loop {
            let workers: Vec<WorkerLoad> = in_flight
                .iter()
                .map(|(url, &n)| WorkerLoad {
                    url: url.clone(),
                    in_flight: n,
                })
                .collect();
            match next_group_assignment(&pending, &workers, cap) {
                Some((idx, url)) => {
                    let placed = pending.remove(idx);
                    *in_flight.get_mut(&url).unwrap() += 1;
                    placements.push((placed.total_bytes, url));
                }
                None => break,
            }
        }

        // Capacity is workers.len() * cap = 4; the two smallest (10, 5)
        // never get placed this round.
        assert_eq!(placements.len(), 4);
        let mut placed_sizes: Vec<u64> = placements.iter().map(|(b, _)| *b).collect();
        placed_sizes.sort_unstable();
        assert_eq!(placed_sizes, vec![20, 30, 40, 50]);

        // No worker exceeded the cap, and (by construction, since `pending`
        // shrinks by one each time) no group was assigned twice.
        let mut per_worker: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (_, url) in &placements {
            *per_worker.entry(url.clone()).or_default() += 1;
        }
        for count in per_worker.values() {
            assert!(*count <= cap, "no worker may exceed the cap of {cap}");
        }
    }

    // ---- decode_group_response / aggregate_group_outcomes --------------

    fn test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![NestedField::required(
                1,
                "id",
                iceberg::spec::Type::Primitive(PrimitiveType::Long),
            )
            .into()])
            .build()
            .unwrap()
    }

    fn avro_encoded_file(path: &str, rows: u64, size: u64) -> Vec<u8> {
        let partition_type = StructType::new(vec![]);
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(rows)
            .file_size_in_bytes(size)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();
        let mut buf = Vec::new();
        write_data_files_to_avro(&mut buf, vec![file], &partition_type, FormatVersion::V2).unwrap();
        buf
    }

    fn old_data_file(path: &str, rows: u64, size: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(rows)
            .file_size_in_bytes(size)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    #[test]
    fn decode_group_response_round_trips_avro_data_files() {
        let avro = avro_encoded_file("s3://bucket/out-0.parquet", 42, 2048);
        let response = CompactGroupResponse {
            group_id: 7,
            new_data_files_avro: avro,
            rows_written: 42,
            bytes_written: 2048,
            uploaded_paths: vec!["s3://bucket/out-0.parquet".to_string()],
        };
        let schema = test_schema();
        let partition_type = StructType::new(vec![]);
        let outcome =
            decode_group_response(&response, &schema, 0, &partition_type, FormatVersion::V2)
                .unwrap();

        assert_eq!(outcome.group_id, 7);
        assert_eq!(outcome.new_files.len(), 1);
        assert_eq!(
            outcome.new_files[0].file_path(),
            "s3://bucket/out-0.parquet"
        );
        assert_eq!(outcome.new_files[0].record_count(), 42);
        assert_eq!(outcome.rows_written, 42);
        assert_eq!(outcome.bytes_written, 2048);
        assert_eq!(
            outcome.uploaded_paths,
            vec!["s3://bucket/out-0.parquet".to_string()]
        );
    }

    #[test]
    fn decode_group_response_rejects_corrupt_avro() {
        let response = CompactGroupResponse {
            group_id: 1,
            new_data_files_avro: vec![0xFF, 0x00, 0x01],
            rows_written: 0,
            bytes_written: 0,
            uploaded_paths: vec![],
        };
        let schema = test_schema();
        let partition_type = StructType::new(vec![]);
        let err = decode_group_response(&response, &schema, 0, &partition_type, FormatVersion::V2)
            .unwrap_err();
        assert!(err.to_string().contains("failed to decode"));
    }

    #[test]
    fn aggregate_group_outcomes_sums_rows_files_and_bytes_across_groups() {
        let g0 = GroupOutcome {
            group_id: 0,
            new_files: vec![old_data_file("s3://b/new-0.parquet", 40, 1000)],
            rows_written: 40,
            bytes_written: 1000,
            uploaded_paths: vec!["s3://b/new-0.parquet".to_string()],
        };
        let g1 = GroupOutcome {
            group_id: 1,
            new_files: vec![old_data_file("s3://b/new-1.parquet", 55, 1500)],
            rows_written: 55,
            bytes_written: 1500,
            uploaded_paths: vec!["s3://b/new-1.parquet".to_string()],
        };
        let old_files = vec![
            old_data_file("s3://b/old-0.parquet", 50, 1200),
            old_data_file("s3://b/old-1.parquet", 60, 1300),
        ];

        let agg = aggregate_group_outcomes(vec![g0, g1], &old_files).unwrap();
        assert_eq!(agg.new_files.len(), 2);
        assert_eq!(agg.rows_written, 95);
        assert_eq!(agg.bytes_written, 2500);
        assert_eq!(agg.added_rows, 95);
        assert_eq!(agg.removed_rows, 110);
        assert_eq!(agg.uploaded_paths.len(), 2);
    }

    #[test]
    fn aggregate_group_outcomes_rejects_added_rows_exceeding_removed_rows() {
        // Worker claims to have written more rows than the old files held:
        // this must abort before commit rather than silently resurrect rows.
        let g0 = GroupOutcome {
            group_id: 0,
            new_files: vec![old_data_file("s3://b/new-0.parquet", 200, 1000)],
            rows_written: 200,
            bytes_written: 1000,
            uploaded_paths: vec![],
        };
        let old_files = vec![old_data_file("s3://b/old-0.parquet", 100, 900)];

        let err = aggregate_group_outcomes(vec![g0], &old_files).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invariant violated"), "{msg}");
        assert!(
            msg.contains("added=200") && msg.contains("removed=100"),
            "{msg}"
        );
    }

    #[test]
    fn aggregate_group_outcomes_accepts_added_equal_to_removed() {
        // No deletes applied: added == removed is the boundary, not a
        // violation.
        let g0 = GroupOutcome {
            group_id: 0,
            new_files: vec![old_data_file("s3://b/new-0.parquet", 100, 1000)],
            rows_written: 100,
            bytes_written: 1000,
            uploaded_paths: vec![],
        };
        let old_files = vec![old_data_file("s3://b/old-0.parquet", 100, 900)];
        let agg = aggregate_group_outcomes(vec![g0], &old_files).unwrap();
        assert_eq!(agg.added_rows, 100);
        assert_eq!(agg.removed_rows, 100);
    }

    #[test]
    fn aggregate_group_outcomes_handles_empty_groups() {
        let agg = aggregate_group_outcomes(vec![], &[]).unwrap();
        assert!(agg.new_files.is_empty());
        assert_eq!(agg.added_rows, 0);
        assert_eq!(agg.removed_rows, 0);
    }
}
