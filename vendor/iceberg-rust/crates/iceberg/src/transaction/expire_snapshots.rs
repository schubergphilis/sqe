// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;

use crate::actions::maintenance::{DEFAULT_LOAD_CONCURRENCY, for_each_manifest_list};
use crate::error::invalid_data;
use crate::spec::{
    MAIN_BRANCH, SnapshotReference, SnapshotRetention, TableMetadata, TableProperties,
};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, Result, TableRequirement, TableUpdate};

/// A transaction action that removes snapshots from table metadata.
///
/// This only rewrites metadata; the now-unreferenced data and metadata files are left untouched.
/// Physical file cleanup is the responsibility of a higher-level maintenance operation built on
/// top of this action.
///
/// Selection follows Java `RemoveSnapshots`:
/// - Explicit ids ([`expire_snapshot_ids`](Self::expire_snapshot_ids)) and age-based expiry are
///   combined: a snapshot is expired if it is named explicitly *or* selected by age.
/// - Age-based expiry always runs. The cutoff is [`expire_older_than_ms`](Self::expire_older_than_ms)
///   when set, a per-branch `max_snapshot_age_ms` for that branch, otherwise
///   `now - history.expire.max-snapshot-age-ms` (default 5 days), matching Java's constructor default.
/// - Expiry is computed per branch along each branch's ancestry: each branch keeps its most recent
///   [`retain_last`](Self::retain_last) snapshots — defaulting to `history.expire.min-snapshots-to-keep`,
///   with a per-ref `min_snapshots_to_keep` overriding both — plus any ancestor newer than the cutoff,
///   so a shared ancestor reachable from a retained branch is never expired.
/// - Refs are aged out first: a non-`main` branch or tag whose head is older than its
///   `max_ref_age_ms` (defaulting to `history.expire.max-ref-age-ms`) is removed, and snapshots only
///   that ref retained then become expirable.
/// - Heads of retained refs (including the current snapshot) are never expired, and naming one
///   explicitly is an error, since
///   [`remove_snapshots`](crate::spec::TableMetadataBuilder::remove_snapshots) would otherwise
///   drop the ref silently.
pub struct ExpireSnapshotsAction {
    explicit_ids_to_remove: Vec<i64>,
    older_than_ms: Option<i64>,
    retain_last: Option<usize>,
    clear_expired_meta_data: bool,
}

impl ExpireSnapshotsAction {
    pub(crate) fn new() -> Self {
        Self {
            explicit_ids_to_remove: vec![],
            older_than_ms: None,
            retain_last: None,
            clear_expired_meta_data: false,
        }
    }

    /// Expire these snapshot ids in addition to any age-based selection.
    ///
    /// Age-based expiry runs by default (see the type-level docs), so a call that only names ids
    /// still expires snapshots older than `history.expire.max-snapshot-age-ms`. Pin
    /// [`expire_older_than_ms`](Self::expire_older_than_ms) to a very old timestamp to expire by id
    /// alone.
    ///
    /// Ids accumulate across calls (like [`add_data_files`](crate::transaction::Transaction::fast_append)).
    /// An id that is still referenced by a branch or tag cannot be expired and causes commits to fail.
    pub fn expire_snapshot_ids(mut self, snapshot_ids: impl IntoIterator<Item = i64>) -> Self {
        self.explicit_ids_to_remove.extend(snapshot_ids);
        self
    }

    /// Expire snapshots whose timestamp is strictly older than `older_than_ms`.
    pub fn expire_older_than_ms(mut self, older_than_ms: i64) -> Self {
        self.older_than_ms = Some(older_than_ms);
        self
    }

    /// Keep at least the `retain_last` most recent snapshots of each branch when expiring by age
    /// (defaults to the table's `history.expire.min-snapshots-to-keep`, must be at least 1).
    ///
    /// This only bounds the age cutoff; it does not protect snapshots named via
    /// [`expire_snapshot_ids`](Self::expire_snapshot_ids). Setting it to 0 makes commit fail.
    pub fn retain_last(mut self, retain_last: usize) -> Self {
        self.retain_last = Some(retain_last);
        self
    }

    /// Remove schemas and partition specs that are no longer referenced after snapshot expiry.
    ///
    /// This is disabled by default. Enabling it reads the manifest lists of every retained
    /// snapshot so partition specs used by historical data remain reachable. The current schema
    /// and default partition spec are always retained.
    pub fn clear_expired_meta_data(mut self, clear_expired_meta_data: bool) -> Self {
        self.clear_expired_meta_data = clear_expired_meta_data;
        self
    }

    /// Resolves the snapshots and refs to remove, following Java `RemoveSnapshots.internalApply`.
    fn plan(&self, table: &Table, properties: &TableProperties<'_>) -> Result<ExpirePlan> {
        // Matches Java `RemoveSnapshots.retainLast`, which requires at least one snapshot.
        if self.retain_last == Some(0) {
            return Err(invalid_data!(
                "Number of snapshots to retain must be at least 1"
            ));
        }

        let metadata = table.metadata();
        let now = Utc::now().timestamp_millis();
        // When a knob is not set explicitly, fall back to the table's `history.expire.*` properties,
        // matching Java `RemoveSnapshots`' constructor. With the default `max-snapshot-age-ms` (5
        // days) the age path always runs, so even an explicit-id-only call applies the default cutoff.
        let default_cutoff = match self.older_than_ms {
            Some(older_than_ms) => older_than_ms,
            None => now.saturating_sub(properties.max_snapshot_age_ms()?),
        };
        let default_min_to_keep = match self.retain_last {
            Some(retain_last) => retain_last,
            None => properties.min_snapshots_to_keep()?,
        };

        // Ref aging: `main` is always kept; any other ref whose head is older than its
        // `max_ref_age_ms` (defaulting to `history.expire.max-ref-age-ms`) is dropped, like Java's
        // `computeRetainedRefs`.
        let default_max_ref_age_ms = properties.max_ref_age_ms()?;
        let mut removed_ref_names: Vec<String> = vec![];
        let mut retained_refs: Vec<&SnapshotReference> = vec![];
        for (ref_name, snapshot_ref) in &metadata.refs {
            if ref_name == MAIN_BRANCH
                || !Self::ref_aged_out(metadata, snapshot_ref, now, default_max_ref_age_ms)
            {
                retained_refs.push(snapshot_ref);
            } else {
                removed_ref_names.push(ref_name.clone());
            }
        }

        // Heads of retained refs (plus the current snapshot) are never expired; naming one
        // explicitly is an error, since `remove_snapshots` would otherwise drop the ref silently.
        let mut ref_head_ids: HashSet<i64> = retained_refs.iter().map(|r| r.snapshot_id).collect();
        if let Some(current_id) = metadata.current_snapshot_id() {
            ref_head_ids.insert(current_id);
        }

        let existing_ids: HashSet<i64> = metadata.snapshots().map(|s| s.snapshot_id()).collect();
        let mut expiring_ids: HashSet<i64> = HashSet::new();
        for id in &self.explicit_ids_to_remove {
            if ref_head_ids.contains(id) {
                return Err(Self::reference_error(metadata, *id));
            }
            if existing_ids.contains(id) {
                expiring_ids.insert(*id);
            }
        }

        // Per-branch retention: keep each branch's most recent `min_to_keep` ancestors plus any
        // newer than the branch cutoff. The current snapshot is treated as a branch (default policy)
        // so its lineage is protected even when there is no explicit `main` ref.
        let mut retained_ids = ref_head_ids.clone();
        let mut referenced_ids = ref_head_ids.clone();
        let mut branches: Vec<(i64, usize, i64)> = vec![];
        for snapshot_ref in &retained_refs {
            match &snapshot_ref.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep,
                    max_snapshot_age_ms,
                    ..
                } => {
                    let min_to_keep =
                        min_snapshots_to_keep.map_or(default_min_to_keep, |m| m as usize);
                    let cutoff =
                        max_snapshot_age_ms.map_or(default_cutoff, |age| now.saturating_sub(age));
                    branches.push((snapshot_ref.snapshot_id, min_to_keep, cutoff));
                }
                SnapshotRetention::Tag { .. } => {
                    referenced_ids.insert(snapshot_ref.snapshot_id);
                }
            }
        }
        if let Some(current_id) = metadata.current_snapshot_id()
            && !branches
                .iter()
                .any(|(head_id, _, _)| *head_id == current_id)
        {
            branches.push((current_id, default_min_to_keep, default_cutoff));
        }
        for (head_id, min_to_keep, cutoff) in branches {
            Self::retain_branch(
                metadata,
                head_id,
                min_to_keep,
                cutoff,
                &mut retained_ids,
                &mut referenced_ids,
            );
        }

        // Unreferenced snapshots newer than the default cutoff are kept (Java's
        // `unreferencedSnapshotsToRetain`); everything else not retained is expired.
        for snapshot in metadata.snapshots() {
            let id = snapshot.snapshot_id();
            if !referenced_ids.contains(&id) && snapshot.timestamp_ms() >= default_cutoff {
                retained_ids.insert(id);
            }
        }
        for snapshot in metadata.snapshots() {
            if !retained_ids.contains(&snapshot.snapshot_id()) {
                expiring_ids.insert(snapshot.snapshot_id());
            }
        }

        let mut ids_to_remove: Vec<i64> = expiring_ids.into_iter().collect();
        ids_to_remove.sort_unstable();
        removed_ref_names.sort();
        Ok(ExpirePlan {
            ids_to_remove,
            refs_to_remove: removed_ref_names,
        })
    }

    /// Whether a non-main ref should be dropped because its head is older than its `max_ref_age_ms`,
    /// defaulting to `default_max_ref_age_ms` (`history.expire.max-ref-age-ms`) when the ref sets no
    /// window of its own. The default `i64::MAX` effectively never ages a ref out.
    fn ref_aged_out(
        metadata: &TableMetadata,
        snapshot_ref: &SnapshotReference,
        now: i64,
        default_max_ref_age_ms: i64,
    ) -> bool {
        let max_ref_age_ms = match snapshot_ref.retention {
            SnapshotRetention::Branch { max_ref_age_ms, .. }
            | SnapshotRetention::Tag { max_ref_age_ms } => max_ref_age_ms,
        }
        .unwrap_or(default_max_ref_age_ms);
        match metadata.snapshot_by_id(snapshot_ref.snapshot_id) {
            Some(snapshot) => now.saturating_sub(snapshot.timestamp_ms()) > max_ref_age_ms,
            None => false,
        }
    }

    /// Walks a branch's ancestry (Java's `computeBranchSnapshotsToRetain`), retaining each ancestor
    /// while fewer than `min` are kept or it is newer than `cutoff`, and recording every ancestor as
    /// referenced. Ancestry timestamps decrease monotonically, so this needs no early break.
    fn retain_branch(
        metadata: &TableMetadata,
        head_id: i64,
        min_to_keep: usize,
        cutoff: i64,
        retained_ids: &mut HashSet<i64>,
        referenced_ids: &mut HashSet<i64>,
    ) {
        let mut kept_count = 0usize;
        for ancestor_id in Self::ancestors(metadata, head_id) {
            referenced_ids.insert(ancestor_id);
            let timestamp = metadata
                .snapshot_by_id(ancestor_id)
                .map_or(i64::MIN, |snapshot| snapshot.timestamp_ms());
            if kept_count < min_to_keep || timestamp >= cutoff {
                retained_ids.insert(ancestor_id);
                kept_count += 1;
            }
        }
    }

    /// Iterates a snapshot and its ancestors, newest first, following `parent_snapshot_id`.
    fn ancestors(metadata: &TableMetadata, head_id: i64) -> impl Iterator<Item = i64> + '_ {
        let mut next_id = Some(head_id);
        std::iter::from_fn(move || {
            let id = next_id?;
            next_id = metadata
                .snapshot_by_id(id)
                .and_then(|snapshot| snapshot.parent_snapshot_id());
            Some(id)
        })
    }

    fn reference_error(metadata: &TableMetadata, snapshot_id: i64) -> Error {
        if metadata.current_snapshot_id() == Some(snapshot_id) {
            return invalid_data!("Cannot expire the current snapshot");
        }
        let ref_names: Vec<&str> = metadata
            .refs
            .iter()
            .filter(|(_, snapshot_ref)| snapshot_ref.snapshot_id == snapshot_id)
            .map(|(ref_name, _)| ref_name.as_str())
            .collect();
        invalid_data!("Cannot expire snapshot {snapshot_id}: still referenced by {ref_names:?}")
    }

    async fn expired_meta_data_updates(
        &self,
        table: &Table,
        ids_to_remove: &HashSet<i64>,
    ) -> Result<Vec<TableUpdate>> {
        let metadata = table.metadata();
        let retained_snapshots: Vec<_> = metadata
            .snapshots()
            .filter(|snapshot| !ids_to_remove.contains(&snapshot.snapshot_id()))
            .cloned()
            .collect();

        let mut reachable_schema_ids = HashSet::from([metadata.current_schema_id()]);
        let has_snapshot_without_schema_id = retained_snapshots.iter().any(|snapshot| {
            if let Some(schema_id) = snapshot.schema_id() {
                reachable_schema_ids.insert(schema_id);
                false
            } else {
                true
            }
        });
        // Older snapshots may omit `schema-id`. In that case there is no safe way to identify the
        // historical schema from table metadata alone, so retain every schema conservatively.
        if has_snapshot_without_schema_id {
            reachable_schema_ids.extend(metadata.schemas_iter().map(|schema| schema.schema_id()));
        }

        let mut reachable_spec_ids = HashSet::from([metadata.default_partition_spec_id()]);
        if metadata.partition_specs_iter().count() > 1 {
            for_each_manifest_list(
                table,
                retained_snapshots,
                DEFAULT_LOAD_CONCURRENCY,
                |manifest_list| {
                    reachable_spec_ids.extend(
                        manifest_list
                            .entries()
                            .iter()
                            .map(|manifest| manifest.partition_spec_id),
                    );
                },
            )
            .await?;
        }

        let mut spec_ids_to_remove: Vec<_> = metadata
            .partition_specs_iter()
            .map(|spec| spec.spec_id())
            .filter(|spec_id| !reachable_spec_ids.contains(spec_id))
            .collect();
        spec_ids_to_remove.sort_unstable();

        let mut schema_ids_to_remove: Vec<_> = metadata
            .schemas_iter()
            .map(|schema| schema.schema_id())
            .filter(|schema_id| !reachable_schema_ids.contains(schema_id))
            .collect();
        schema_ids_to_remove.sort_unstable();

        let mut updates = vec![];
        if !spec_ids_to_remove.is_empty() {
            updates.push(TableUpdate::RemovePartitionSpecs {
                spec_ids: spec_ids_to_remove,
            });
        }
        if !schema_ids_to_remove.is_empty() {
            updates.push(TableUpdate::RemoveSchemas {
                schema_ids: schema_ids_to_remove,
            });
        }
        Ok(updates)
    }
}

/// Snapshots and refs an [`ExpireSnapshotsAction`] resolves to remove.
struct ExpirePlan {
    ids_to_remove: Vec<i64>,
    refs_to_remove: Vec<String>,
}

#[async_trait]
impl TransactionAction for ExpireSnapshotsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        let properties = metadata.table_properties();

        // Expiring metadata defeats a user's explicit decision to disable GC (Java refuses too).
        if !properties.gc_enabled()? {
            return Err(invalid_data!(
                "Cannot expire snapshots: gc.enabled is false"
            ));
        }

        let plan = self.plan(table, &properties)?;

        if plan.ids_to_remove.is_empty()
            && plan.refs_to_remove.is_empty()
            && !self.clear_expired_meta_data
        {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let ids_to_remove: HashSet<_> = plan.ids_to_remove.iter().copied().collect();

        // Drop aged-out refs first, then the snapshots no ref retains anymore.
        let mut updates: Vec<TableUpdate> = plan
            .refs_to_remove
            .iter()
            .cloned()
            .map(|ref_name| TableUpdate::RemoveSnapshotRef { ref_name })
            .collect();

        // Drop statistics metadata for expired snapshots.
        // This only updates metadata; puffin files are cleaned up separately.
        let mut stats_updates: Vec<TableUpdate> = vec![];
        for &snapshot_id in &plan.ids_to_remove {
            stats_updates.extend(
                metadata
                    .statistics_for_snapshot(snapshot_id)
                    .is_some()
                    .then_some(TableUpdate::RemoveStatistics { snapshot_id }),
            );
            stats_updates.extend(
                metadata
                    .partition_statistics_for_snapshot(snapshot_id)
                    .is_some()
                    .then_some(TableUpdate::RemovePartitionStatistics { snapshot_id }),
            );
        }

        if !plan.ids_to_remove.is_empty() {
            updates.push(TableUpdate::RemoveSnapshots {
                snapshot_ids: plan.ids_to_remove.clone(),
            });
        }
        updates.extend(stats_updates);

        if self.clear_expired_meta_data {
            updates.extend(
                self.expired_meta_data_updates(table, &ids_to_remove)
                    .await?,
            );
        }

        if updates.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // The ref assertion closes the race where a concurrent writer advances `main` between
        // selection and commit, which could orphan a snapshot whose parent we are about to remove.
        Ok(ActionCommit::new(updates, vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: metadata.current_snapshot_id(),
            },
        ]))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::Utc;

    use crate::spec::{
        MAIN_BRANCH, ManifestContentType, ManifestFile, ManifestListWriter, NestedField, Operation,
        PartitionStatisticsFile, PrimitiveType, Snapshot, SnapshotReference, SnapshotRetention,
        StatisticsFile, Summary, Transform, Type, UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::transaction::action::{ApplyTransactionAction, TransactionAction};
    use crate::transaction::expire_snapshots::ExpireSnapshotsAction;
    use crate::transaction::tests::{make_v2_minimal_table, make_v2_table};
    use crate::{TableRequirement, TableUpdate};

    // `make_v2_table` carries an older snapshot (ts 1515100955770) and a current
    // snapshot (ts 1555100955770).
    const OLD_SNAPSHOT: i64 = 3051729675574597004;
    const CURRENT_SNAPSHOT: i64 = 3055729675574597004;
    // Well after the minimal table's last-updated-ms, so synthetic snapshots pass timestamp checks.
    const TS: i64 = 1_700_000_000_000;
    // A cutoff at the epoch makes age-based expiry a no-op (every snapshot is newer), isolating the
    // explicit-id behavior under test now that the default cutoff (`now - 5 days`) always runs.
    const NO_AGE_EXPIRY: i64 = 0;

    fn action() -> ExpireSnapshotsAction {
        ExpireSnapshotsAction::new()
    }

    async fn removed_ids(action: ExpireSnapshotsAction) -> Vec<i64> {
        expired(&make_v2_table(), action).await
    }

    async fn updates_of(table: &Table, action: ExpireSnapshotsAction) -> Vec<TableUpdate> {
        Arc::new(action).commit(table).await.unwrap().take_updates()
    }

    async fn expired(table: &Table, action: ExpireSnapshotsAction) -> Vec<i64> {
        updates_of(table, action)
            .await
            .into_iter()
            .find_map(|update| match update {
                TableUpdate::RemoveSnapshots { snapshot_ids } => Some(snapshot_ids),
                _ => None,
            })
            .unwrap_or_default()
    }

    fn removed_refs(updates: &[TableUpdate]) -> Vec<String> {
        let mut refs: Vec<String> = updates
            .iter()
            .filter_map(|update| match update {
                TableUpdate::RemoveSnapshotRef { ref_name } => Some(ref_name.clone()),
                _ => None,
            })
            .collect();
        refs.sort();
        refs
    }

    fn snapshot(id: i64, parent: Option<i64>, sequence_number: i64, timestamp_ms: i64) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(timestamp_ms)
            .with_schema_id(0)
            .with_manifest_list(format!("/snap-{id}.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }

    fn branch(snapshot_id: i64, min_snapshots_to_keep: Option<i32>) -> SnapshotReference {
        branch_with(snapshot_id, min_snapshots_to_keep, None, None)
    }

    fn branch_with(
        snapshot_id: i64,
        min_snapshots_to_keep: Option<i32>,
        max_snapshot_age_ms: Option<i64>,
        max_ref_age_ms: Option<i64>,
    ) -> SnapshotReference {
        SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep,
                max_snapshot_age_ms,
                max_ref_age_ms,
            },
        }
    }

    fn tag(snapshot_id: i64, max_ref_age_ms: Option<i64>) -> SnapshotReference {
        SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Tag { max_ref_age_ms },
        }
    }

    /// Builds a table from synthetic snapshots and refs on top of an empty base.
    fn table_with(snapshots: Vec<Snapshot>, refs: Vec<(&str, SnapshotReference)>) -> Table {
        table_with_props(snapshots, refs, HashMap::new())
    }

    /// Like [`table_with`], but also seeds table properties (e.g. `history.expire.*` defaults).
    fn table_with_props(
        snapshots: Vec<Snapshot>,
        refs: Vec<(&str, SnapshotReference)>,
        properties: HashMap<String, String>,
    ) -> Table {
        let base = make_v2_minimal_table();
        let mut builder = base
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(properties)
            .unwrap();
        for snapshot in snapshots {
            builder = builder.add_snapshot(snapshot).unwrap();
        }
        for (name, reference) in refs {
            builder = builder.set_ref(name, reference).unwrap();
        }
        base.with_metadata(Arc::new(builder.build().unwrap().metadata))
    }

    /// Like [`table_with`], but also attaches statistics and partition-statistics files. Reuses
    /// [`table_with`] for the snapshot/ref wiring so the two can't drift, then layers stats on top.
    fn table_with_stats(
        snapshots: Vec<Snapshot>,
        refs: Vec<(&str, SnapshotReference)>,
        statistics: Vec<StatisticsFile>,
        partition_statistics: Vec<PartitionStatisticsFile>,
    ) -> Table {
        let table = table_with(snapshots, refs);
        let mut builder = table.metadata().clone().into_builder(None);
        for stats in statistics {
            builder = builder.set_statistics(stats);
        }
        for stats in partition_statistics {
            builder = builder.set_partition_statistics(stats);
        }
        table.with_metadata(Arc::new(builder.build().unwrap().metadata))
    }

    async fn write_manifest_list(
        table: &Table,
        snapshot_id: i64,
        sequence_number: i64,
        path: &str,
        partition_spec_ids: &[i32],
    ) {
        let output = table
            .file_io()
            .new_output(path)
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut writer = ManifestListWriter::v2(output, snapshot_id, None, sequence_number);
        writer
            .add_manifests(
                partition_spec_ids
                    .iter()
                    .map(|partition_spec_id| ManifestFile {
                        manifest_path: format!(
                            "memory:///metadata/manifest-{snapshot_id}-{partition_spec_id}.avro"
                        ),
                        manifest_length: 0,
                        partition_spec_id: *partition_spec_id,
                        content: ManifestContentType::Data,
                        sequence_number,
                        min_sequence_number: sequence_number,
                        added_snapshot_id: snapshot_id,
                        added_files_count: Some(0),
                        existing_files_count: Some(0),
                        deleted_files_count: Some(0),
                        added_rows_count: Some(0),
                        existing_rows_count: Some(0),
                        deleted_rows_count: Some(0),
                        partitions: None,
                        key_metadata: None,
                        first_row_id: None,
                    }),
            )
            .unwrap();
        writer.close().await.unwrap();
    }

    async fn table_with_expired_schema_and_spec() -> Table {
        const OLD_MANIFEST_LIST: &str = "memory:///metadata/old-manifest-list.avro";
        const CURRENT_MANIFEST_LIST: &str = "memory:///metadata/current-manifest-list.avro";

        let base = make_v2_minimal_table();
        write_manifest_list(&base, 1, 1, OLD_MANIFEST_LIST, &[2]).await;
        write_manifest_list(&base, 2, 2, CURRENT_MANIFEST_LIST, &[1]).await;

        let schema_1 = base
            .metadata()
            .current_schema()
            .as_ref()
            .clone()
            .into_builder()
            .with_schema_id(1)
            .with_fields([Arc::new(NestedField::optional(
                4,
                "schema_1_only",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        let schema_2 = base
            .metadata()
            .current_schema()
            .as_ref()
            .clone()
            .into_builder()
            .with_schema_id(2)
            .with_fields([Arc::new(NestedField::optional(
                5,
                "schema_2_only",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        let spec_1 = UnboundPartitionSpec::builder()
            .with_spec_id(1)
            .add_partition_field(2, "y_bucket", Transform::Bucket(8))
            .unwrap()
            .build();
        let spec_2 = UnboundPartitionSpec::builder()
            .with_spec_id(2)
            .add_partition_field(3, "z_bucket", Transform::Bucket(8))
            .unwrap()
            .build();

        let old_snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(TS + 1)
            .with_schema_id(2)
            .with_manifest_list(OLD_MANIFEST_LIST)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();
        let current_snapshot = Snapshot::builder()
            .with_snapshot_id(2)
            .with_parent_snapshot_id(Some(1))
            .with_sequence_number(2)
            .with_timestamp_ms(TS + 2)
            .with_schema_id(1)
            .with_manifest_list(CURRENT_MANIFEST_LIST)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let metadata = base
            .metadata()
            .clone()
            .into_builder(None)
            .add_schema(schema_1)
            .unwrap()
            .add_schema(schema_2)
            .unwrap()
            .add_partition_spec(spec_1)
            .unwrap()
            .add_partition_spec(spec_2)
            .unwrap()
            .add_snapshot(old_snapshot)
            .unwrap()
            .add_snapshot(current_snapshot)
            .unwrap()
            .set_ref(MAIN_BRANCH, branch(2, None))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        base.with_metadata(Arc::new(metadata))
    }

    fn stats_file(snapshot_id: i64) -> StatisticsFile {
        StatisticsFile {
            snapshot_id,
            statistics_path: format!("/stats-{snapshot_id}.puffin"),
            file_size_in_bytes: 1,
            file_footer_size_in_bytes: 1,
            key_metadata: None,
            blob_metadata: vec![],
        }
    }

    fn partition_stats_file(snapshot_id: i64) -> PartitionStatisticsFile {
        PartitionStatisticsFile {
            snapshot_id,
            statistics_path: format!("/partition-stats-{snapshot_id}.puffin"),
            file_size_in_bytes: 1,
        }
    }

    fn removed_statistics(updates: &[TableUpdate]) -> Vec<i64> {
        updates
            .iter()
            .filter_map(|update| match update {
                TableUpdate::RemoveStatistics { snapshot_id } => Some(*snapshot_id),
                _ => None,
            })
            .collect()
    }

    fn removed_partition_statistics(updates: &[TableUpdate]) -> Vec<i64> {
        updates
            .iter()
            .filter_map(|update| match update {
                TableUpdate::RemovePartitionStatistics { snapshot_id } => Some(*snapshot_id),
                _ => None,
            })
            .collect()
    }

    fn removed_schema_ids(updates: &[TableUpdate]) -> Vec<i32> {
        updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::RemoveSchemas { schema_ids } => Some(schema_ids.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    fn removed_spec_ids(updates: &[TableUpdate]) -> Vec<i32> {
        updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::RemovePartitionSpecs { spec_ids } => Some(spec_ids.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn test_clear_expired_meta_data_without_expiring_snapshots() {
        let base = make_v2_minimal_table();
        let expired_schema = base
            .metadata()
            .current_schema()
            .as_ref()
            .clone()
            .into_builder()
            .with_schema_id(1)
            .with_fields([Arc::new(NestedField::optional(
                4,
                "expired",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        let expired_spec = UnboundPartitionSpec::builder()
            .with_spec_id(1)
            .add_partition_field(2, "y_bucket", Transform::Bucket(8))
            .unwrap()
            .build();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(None)
            .add_schema(expired_schema)
            .unwrap()
            .add_partition_spec(expired_spec)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = base.with_metadata(Arc::new(metadata));

        let updates = updates_of(&table, action().clear_expired_meta_data(true)).await;

        assert_eq!(removed_schema_ids(&updates), vec![1]);
        assert_eq!(removed_spec_ids(&updates), vec![1]);
    }

    #[tokio::test]
    async fn test_clear_expired_meta_data_preserves_retained_snapshot_metadata() {
        let table = table_with_expired_schema_and_spec().await;
        let expire = || action().retain_last(1).expire_older_than_ms(i64::MAX);

        let updates_without_cleanup = updates_of(&table, expire()).await;
        assert!(removed_schema_ids(&updates_without_cleanup).is_empty());
        assert!(removed_spec_ids(&updates_without_cleanup).is_empty());

        let updates = updates_of(&table, expire().clear_expired_meta_data(true)).await;

        assert!(updates.iter().any(
            |update| matches!(update, TableUpdate::RemoveSnapshots { snapshot_ids } if snapshot_ids == &[1])
        ));
        // Schema/spec 2 are referenced only by the expired snapshot. Schema/spec 1 remain
        // reachable through the retained snapshot, even though 0 are still current/default.
        assert_eq!(removed_schema_ids(&updates), vec![2]);
        assert_eq!(removed_spec_ids(&updates), vec![2]);
    }

    #[tokio::test]
    async fn test_clear_expired_meta_data_keeps_schemas_for_snapshot_without_schema_id() {
        let base = make_v2_minimal_table();
        let historical_schema = base
            .metadata()
            .current_schema()
            .as_ref()
            .clone()
            .into_builder()
            .with_schema_id(1)
            .with_fields([Arc::new(NestedField::optional(
                4,
                "historical",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        let retained_snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(TS + 1)
            .with_manifest_list("memory:///metadata/not-read-with-one-spec.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(None)
            .add_schema(historical_schema)
            .unwrap()
            .add_snapshot(retained_snapshot)
            .unwrap()
            .set_ref(MAIN_BRANCH, branch(1, None))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = base.with_metadata(Arc::new(metadata));

        let updates = updates_of(
            &table,
            action()
                .retain_last(1)
                .expire_older_than_ms(i64::MAX)
                .clear_expired_meta_data(true),
        )
        .await;

        assert!(removed_schema_ids(&updates).is_empty());
    }

    #[tokio::test]
    async fn test_expire_explicit_snapshot_id() {
        assert_eq!(
            removed_ids(
                action()
                    .expire_snapshot_ids(vec![OLD_SNAPSHOT])
                    .expire_older_than_ms(NO_AGE_EXPIRY)
            )
            .await,
            vec![OLD_SNAPSHOT]
        );
    }

    #[tokio::test]
    async fn test_explicit_unknown_id_is_ignored() {
        assert!(
            removed_ids(
                action()
                    .expire_snapshot_ids(vec![42])
                    .expire_older_than_ms(NO_AGE_EXPIRY)
            )
            .await
            .is_empty()
        );
    }

    #[tokio::test]
    async fn test_cannot_expire_current_snapshot() {
        let table = make_v2_table();
        let action = action().expire_snapshot_ids(vec![CURRENT_SNAPSHOT]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    /// `make_v2_table` with a tag pointing at the older snapshot.
    fn table_with_tag_on_old() -> Table {
        let table = make_v2_table();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_ref("history-tag", SnapshotReference {
                snapshot_id: OLD_SNAPSHOT,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        table.with_metadata(Arc::new(metadata))
    }

    #[tokio::test]
    async fn test_cannot_expire_tagged_snapshot_explicitly() {
        let table = table_with_tag_on_old();
        let action = action().expire_snapshot_ids(vec![OLD_SNAPSHOT]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_age_expiry_skips_tagged_snapshot() {
        let table = table_with_tag_on_old();
        let mut commit = Arc::new(action().expire_older_than_ms(i64::MAX))
            .commit(&table)
            .await
            .unwrap();
        // Both snapshots are referenced (current + tag), so nothing is expired.
        assert!(commit.take_updates().is_empty());
    }

    #[tokio::test]
    async fn test_retain_last_default_expires_older_non_current() {
        assert_eq!(
            removed_ids(action().expire_older_than_ms(i64::MAX)).await,
            vec![OLD_SNAPSHOT]
        );
    }

    #[tokio::test]
    async fn test_retain_last_noop_when_enough_retained() {
        assert!(removed_ids(action().retain_last(5)).await.is_empty());
    }

    #[tokio::test]
    async fn test_older_than_excludes_newer_snapshots() {
        // Threshold older than every snapshot -> nothing qualifies.
        assert!(
            removed_ids(action().expire_older_than_ms(1))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_apply_registers_action() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .expire_snapshots()
            .expire_snapshot_ids(vec![OLD_SNAPSHOT])
            .apply(tx)
            .unwrap();
        assert_eq!(tx.actions.len(), 1);
    }

    #[tokio::test]
    async fn test_per_branch_retention_protects_shared_ancestor() {
        // main: 1 -> 2 -> 3 ; branch `b`: 1 -> 2 -> 4. Snapshot 2 is a shared ancestor of both
        // branches but is not a ref head.
        let table = table_with(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
                snapshot(3, Some(2), 37, TS + 3),
                snapshot(4, Some(2), 38, TS + 4),
            ],
            vec![(MAIN_BRANCH, branch(3, None)), ("b", branch(4, None))],
        );

        // A global "newest 2" would expire 2 and orphan branch `b`; per-branch retention keeps it.
        let removed = expired(
            &table,
            action().retain_last(2).expire_older_than_ms(i64::MAX),
        )
        .await;
        assert_eq!(removed, vec![1]);
    }

    #[tokio::test]
    async fn test_per_ref_min_snapshots_to_keep_overrides_retain_last() {
        let table = table_with(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
                snapshot(3, Some(2), 37, TS + 3),
            ],
            vec![(MAIN_BRANCH, branch(3, Some(3)))],
        );

        // The branch's own min_snapshots_to_keep=3 wins over the action's retain_last(1).
        let removed = expired(
            &table,
            action().retain_last(1).expire_older_than_ms(i64::MAX),
        )
        .await;
        assert!(removed.is_empty());
    }

    #[tokio::test]
    async fn test_explicit_and_age_combine() {
        let table = table_with(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
                snapshot(3, Some(2), 37, TS + 3),
                snapshot(4, Some(3), 38, TS + 4),
            ],
            vec![(MAIN_BRANCH, branch(4, None))],
        );

        // Age expires 1 and 2 (older than the cutoff, beyond retain_last). 3 is newer than the
        // cutoff so age keeps it, but it is named explicitly, so all three are expired.
        let removed = expired(
            &table,
            action()
                .retain_last(1)
                .expire_older_than_ms(TS + 3)
                .expire_snapshot_ids(vec![3]),
        )
        .await;
        assert_eq!(removed, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_expire_snapshot_ids_accumulates() {
        let table = table_with(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
                snapshot(3, Some(2), 37, TS + 3),
            ],
            vec![(MAIN_BRANCH, branch(3, None))],
        );

        // Two separate calls both take effect (age expiry pinned off to isolate accumulation).
        let removed = expired(
            &table,
            action()
                .expire_snapshot_ids(vec![1])
                .expire_snapshot_ids(vec![2])
                .expire_older_than_ms(NO_AGE_EXPIRY),
        )
        .await;
        assert_eq!(removed, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_gc_disabled_errors() {
        let table = make_v2_table();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(HashMap::from([(
                "gc.enabled".to_string(),
                "false".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));

        let action = action().expire_snapshot_ids(vec![OLD_SNAPSHOT]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_commit_asserts_main_ref() {
        let table = make_v2_table();
        let mut commit = Arc::new(action().expire_snapshot_ids(vec![OLD_SNAPSHOT]))
            .commit(&table)
            .await
            .unwrap();
        assert!(
            commit
                .take_requirements()
                .iter()
                .any(|requirement| matches!(
                    requirement,
                    TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id }
                        if r#ref == MAIN_BRANCH && *snapshot_id == Some(CURRENT_SNAPSHOT)
                ))
        );
    }

    #[tokio::test]
    async fn test_ref_aging_drops_old_tag_and_expires_its_snapshot() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        // main: 2 (recent). Isolated snapshot 1 (old) is only kept alive by `old-tag`, whose own
        // age (10 days) exceeds its max-ref-age of 1 day.
        let table = table_with(
            vec![
                snapshot(1, None, 35, now - 10 * day_ms),
                snapshot(2, None, 36, now - 1000),
            ],
            // Set the tag before main so the builder's last-updated bookkeeping stays monotonic.
            vec![
                ("old-tag", tag(1, Some(day_ms))),
                (MAIN_BRANCH, branch(2, None)),
            ],
        );

        // An explicit cutoff lets the freed snapshot 1 actually expire once the tag is gone.
        let updates = updates_of(&table, action().expire_older_than_ms(now - 5 * day_ms)).await;
        // The tag is dropped, and snapshot 1 (now unreferenced and old) is expired with it.
        assert_eq!(removed_refs(&updates), vec!["old-tag".to_string()]);
        assert!(updates.iter().any(
            |u| matches!(u, TableUpdate::RemoveSnapshots { snapshot_ids } if snapshot_ids == &[1])
        ));
    }

    #[tokio::test]
    async fn test_ref_aging_keeps_recent_tag() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        let table = table_with(
            vec![
                snapshot(1, None, 35, now - 1000),
                snapshot(2, None, 36, now - 500),
            ],
            vec![
                ("fresh-tag", tag(1, Some(day_ms))),
                (MAIN_BRANCH, branch(2, None)),
            ],
        );

        let updates = updates_of(&table, action()).await;
        // The tag is younger than its max-ref-age, so neither it nor its snapshot is removed.
        assert!(removed_refs(&updates).is_empty());
        assert_eq!(expired(&table, action()).await, Vec::<i64>::new());
    }

    #[tokio::test]
    async fn test_per_ref_max_snapshot_age_overrides_default() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        // main: 1 (3 days old) -> 2 (1 day old) -> 3 (recent), with a 2-day per-ref window.
        let table = table_with(
            vec![
                snapshot(1, None, 35, now - 3 * day_ms),
                snapshot(2, Some(1), 36, now - day_ms),
                snapshot(3, Some(2), 37, now - 1000),
            ],
            vec![(MAIN_BRANCH, branch_with(3, Some(1), Some(2 * day_ms), None))],
        );

        // With no explicit cutoff nothing would expire, but main's 2-day window expires snapshot 1.
        let removed = expired(&table, action()).await;
        assert_eq!(removed, vec![1]);
    }

    #[tokio::test]
    async fn test_ref_aging_drops_old_branch() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        // A stale non-main branch (head 1, 10 days old) past its 1-day max-ref-age.
        let table = table_with(
            vec![
                snapshot(1, None, 35, now - 10 * day_ms),
                snapshot(2, None, 36, now - 1000),
            ],
            vec![
                ("stale", branch_with(1, None, None, Some(day_ms))),
                (MAIN_BRANCH, branch(2, None)),
            ],
        );

        let updates = updates_of(&table, action().expire_older_than_ms(now - 5 * day_ms)).await;
        // The stale branch is dropped and snapshot 1 (now unreferenced and old) is expired.
        assert_eq!(removed_refs(&updates), vec!["stale".to_string()]);
        assert!(updates.iter().any(
            |u| matches!(u, TableUpdate::RemoveSnapshots { snapshot_ids } if snapshot_ids == &[1])
        ));
    }

    #[tokio::test]
    async fn test_unreferenced_snapshots_retained_only_while_young() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        // Snapshot 1 is the main head; 2 and 3 are orphans (no ref, not ancestors). Add oldest
        // first so the builder's timestamp bookkeeping stays monotonic.
        let table = table_with(
            vec![
                snapshot(3, None, 35, now - 10 * day_ms), // old orphan
                snapshot(1, None, 36, now - 1000),        // main head
                snapshot(2, None, 37, now - 1000),        // young orphan
            ],
            vec![(MAIN_BRANCH, branch(1, None))],
        );

        // The young orphan (2) is kept; only the old orphan (3) is expired.
        let removed = expired(&table, action().expire_older_than_ms(now - 5 * day_ms)).await;
        assert_eq!(removed, vec![3]);
    }

    #[tokio::test]
    async fn test_retain_last_zero_errors() {
        let table = make_v2_table();
        assert!(
            Arc::new(action().retain_last(0))
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_cannot_expire_branch_head_explicitly() {
        let table = table_with(
            vec![snapshot(1, None, 35, TS + 1), snapshot(2, None, 36, TS + 2)],
            vec![(MAIN_BRANCH, branch(1, None)), ("branch", branch(2, None))],
        );
        // 2 is the head of a non-main branch, so it cannot be expired explicitly.
        let action = action().expire_snapshot_ids(vec![2]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_tag_does_not_protect_its_ancestry() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        // Chain 1 -> 2 -> 3 with main rewound to 1 and a tag on 3. Snapshot 2 (the tag target's
        // parent) is reachable from no ref's retained set, so it is expired.
        let table = table_with(
            vec![
                snapshot(1, None, 35, now - 10 * day_ms),
                snapshot(2, Some(1), 36, now - 8 * day_ms),
                snapshot(3, Some(2), 37, now - 1000),
            ],
            vec![(MAIN_BRANCH, branch(1, None)), ("tag", tag(3, None))],
        );

        let removed = expired(&table, action().expire_older_than_ms(now - 5 * day_ms)).await;
        assert_eq!(removed, vec![2]);
    }

    #[tokio::test]
    async fn test_branch_protects_its_ancestry() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        // Same topology as the tag case, but a branch (keeping its whole history) replaces the tag.
        // The branch's parent snapshot 2 is now reachable history and is not expired.
        let table = table_with(
            vec![
                snapshot(1, None, 35, now - 10 * day_ms),
                snapshot(2, Some(1), 36, now - 8 * day_ms),
                snapshot(3, Some(2), 37, now - 1000),
            ],
            vec![
                (MAIN_BRANCH, branch(1, None)),
                ("branch", branch_with(3, None, Some(i64::MAX), None)),
            ],
        );

        let removed = expired(&table, action().expire_older_than_ms(now - 5 * day_ms)).await;
        assert!(removed.is_empty());
    }

    #[tokio::test]
    async fn test_per_branch_max_snapshot_age_differs_across_branches() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        // main (1 -> 2) keeps 5 days; branch `keep` (3 -> 4) keeps 60 days. Snapshots 1 and 3 are
        // both 30 days old, but only main's short window expires its ancestor.
        let table = table_with(
            vec![
                snapshot(1, None, 35, now - 30 * day_ms),
                snapshot(3, None, 36, now - 30 * day_ms),
                snapshot(2, Some(1), 37, now - 2 * day_ms),
                snapshot(4, Some(3), 38, now - 2 * day_ms),
            ],
            vec![
                (MAIN_BRANCH, branch_with(2, None, Some(5 * day_ms), None)),
                ("keep", branch_with(4, None, Some(60 * day_ms), None)),
            ],
        );

        // main's 5-day window expires its old ancestor 1; keep's 60-day window retains its old
        // ancestor 3.
        let removed = expired(&table, action()).await;
        assert_eq!(removed, vec![1]);
    }

    #[tokio::test]
    async fn test_default_cutoff_expires_snapshots_older_than_max_age() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        let table = table_with(
            vec![
                snapshot(1, None, 35, now - 10 * day_ms), // older than the default 5-day cutoff
                snapshot(2, Some(1), 36, now - 1000),     // recent
            ],
            vec![(MAIN_BRANCH, branch(2, None))],
        );

        // No explicit cutoff: defaults to now - history.expire.max-snapshot-age-ms (5 days).
        let removed = expired(&table, action()).await;
        assert_eq!(removed, vec![1]);
    }

    #[tokio::test]
    async fn test_min_snapshots_to_keep_property_is_the_default_floor() {
        let table = table_with_props(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
                snapshot(3, Some(2), 37, TS + 3),
            ],
            vec![(MAIN_BRANCH, branch(3, None))],
            HashMap::from([(
                "history.expire.min-snapshots-to-keep".to_string(),
                "3".to_string(),
            )]),
        );

        // The snapshots predate the default cutoff, but the table's min-snapshots-to-keep=3 keeps
        // the whole chain.
        let removed = expired(&table, action()).await;
        assert!(removed.is_empty());
    }

    #[tokio::test]
    async fn test_max_snapshot_age_ms_property_sets_the_cutoff() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        // Snapshot 1 is 2 days old: the built-in 5-day default would keep it, but the table's
        // 1-day history.expire.max-snapshot-age-ms expires it. This pins the cutoff to the property
        // value rather than the hardcoded default.
        let table = table_with_props(
            vec![
                snapshot(1, None, 35, now - 2 * day_ms),
                snapshot(2, Some(1), 36, now - 1000),
            ],
            vec![(MAIN_BRANCH, branch(2, None))],
            HashMap::from([(
                "history.expire.max-snapshot-age-ms".to_string(),
                day_ms.to_string(),
            )]),
        );

        let removed = expired(&table, action()).await;
        assert_eq!(removed, vec![1]);
    }

    #[tokio::test]
    async fn test_max_ref_age_ms_property_ages_out_ref_without_its_own_window() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        // `old-tag` sets no max_ref_age_ms of its own, so it ages against the table's
        // history.expire.max-ref-age-ms (1 day); its head is 10 days old, so the tag is dropped.
        // Set the tag before main so the builder's last-updated bookkeeping stays monotonic.
        let table = table_with_props(
            vec![
                snapshot(1, None, 35, now - 10 * day_ms),
                snapshot(2, None, 36, now - 1000),
            ],
            vec![("old-tag", tag(1, None)), (MAIN_BRANCH, branch(2, None))],
            HashMap::from([(
                "history.expire.max-ref-age-ms".to_string(),
                day_ms.to_string(),
            )]),
        );

        let updates = updates_of(&table, action()).await;
        assert_eq!(removed_refs(&updates), vec!["old-tag".to_string()]);
        // Once the tag is gone, snapshot 1 (unreferenced and well past the default cutoff) expires.
        assert!(updates.iter().any(
            |u| matches!(u, TableUpdate::RemoveSnapshots { snapshot_ids } if snapshot_ids == &[1])
        ));
    }

    #[tokio::test]
    async fn test_expiring_snapshot_drops_its_statistics() {
        // main: 1 -> 2, both carrying statistics. retain_last(1) keeps only the head (2).
        let table = table_with_stats(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
            ],
            vec![(MAIN_BRANCH, branch(2, None))],
            vec![stats_file(1), stats_file(2)],
            vec![partition_stats_file(1), partition_stats_file(2)],
        );

        let updates = updates_of(
            &table,
            action().retain_last(1).expire_older_than_ms(i64::MAX),
        )
        .await;

        // Snapshot 1 expires, so its stats entries are dropped; the retained head 2 keeps its stats.
        assert_eq!(removed_statistics(&updates), vec![1]);
        assert_eq!(removed_partition_statistics(&updates), vec![1]);
    }

    #[tokio::test]
    async fn test_expiring_snapshot_without_statistics_emits_no_removal() {
        // Same expiry, but no statistics attached to any snapshot.
        let table = table_with(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
            ],
            vec![(MAIN_BRANCH, branch(2, None))],
        );

        let updates = updates_of(
            &table,
            action().retain_last(1).expire_older_than_ms(i64::MAX),
        )
        .await;

        assert!(removed_statistics(&updates).is_empty());
        assert!(removed_partition_statistics(&updates).is_empty());
    }

    #[tokio::test]
    async fn test_only_present_statistics_variant_is_removed() {
        // Snapshot 1 has statistics but no partition statistics.
        let table = table_with_stats(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
            ],
            vec![(MAIN_BRANCH, branch(2, None))],
            vec![stats_file(1)],
            vec![],
        );

        let updates = updates_of(
            &table,
            action().retain_last(1).expire_older_than_ms(i64::MAX),
        )
        .await;

        assert_eq!(removed_statistics(&updates), vec![1]);
        assert!(removed_partition_statistics(&updates).is_empty());
    }

    #[tokio::test]
    async fn test_ref_aging_expiry_drops_statistics() {
        let now = Utc::now().timestamp_millis();
        let day_ms = 24 * 60 * 60 * 1000;
        // Snapshot 1 is kept alive only by `old-tag`; once the tag ages out (1-day max-ref-age vs a
        // 10-day-old head) snapshot 1 becomes expirable and its statistics go with it. This reaches
        // the stats loop through the ref-aging branch of `plan()`, distinct from the retain/age path.
        let table = table_with_stats(
            vec![
                snapshot(1, None, 35, now - 10 * day_ms),
                snapshot(2, None, 36, now - 1000),
            ],
            vec![
                ("old-tag", tag(1, Some(day_ms))),
                (MAIN_BRANCH, branch(2, None)),
            ],
            vec![stats_file(1)],
            vec![partition_stats_file(1)],
        );

        let updates = updates_of(&table, action().expire_older_than_ms(now - 5 * day_ms)).await;
        assert_eq!(removed_refs(&updates), vec!["old-tag".to_string()]);
        assert_eq!(removed_statistics(&updates), vec![1]);
        assert_eq!(removed_partition_statistics(&updates), vec![1]);
    }

    #[tokio::test]
    async fn test_multiple_expired_snapshots_drop_their_statistics() {
        // Chain 1 -> 2 -> 3 on main; retain_last(1) keeps only head 3, expiring 1 and 2, both
        // carrying statistics.
        let table = table_with_stats(
            vec![
                snapshot(1, None, 35, TS + 1),
                snapshot(2, Some(1), 36, TS + 2),
                snapshot(3, Some(2), 37, TS + 3),
            ],
            vec![(MAIN_BRANCH, branch(3, None))],
            vec![stats_file(1), stats_file(2)],
            vec![partition_stats_file(1), partition_stats_file(2)],
        );

        let updates = updates_of(
            &table,
            action().retain_last(1).expire_older_than_ms(i64::MAX),
        )
        .await;

        assert_eq!(removed_statistics(&updates), vec![1, 2]);
        assert_eq!(removed_partition_statistics(&updates), vec![1, 2]);
    }
}
