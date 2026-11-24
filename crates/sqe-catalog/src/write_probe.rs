//! The update list of the catalog write probe (`SessionCatalog::check_write_access`).
//!
//! The probe sends the updates the real write would send, under an
//! `assert-table-uuid` that can never match, so the catalog authorizes the
//! same operations as the write and then commits nothing.

use std::collections::HashMap;

use iceberg::spec::{
    FormatVersion, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
    TableMetadata, MAIN_BRANCH,
};
use iceberg::TableUpdate;

/// The updates for the write probe on `metadata`.
///
/// - With a `main` ref: set `main` to the snapshot it already points at.
/// - Without one (a fresh table): what a first append sends, an
///   `add-snapshot` and a `set-snapshot-ref` of `main` to it. Never a ref
///   removal, and never an empty list (authorized as a generic table update,
///   which a write grant does not confer).
pub fn write_probe_updates(metadata: &TableMetadata) -> Vec<TableUpdate> {
    if let Some((_, reference)) = metadata.refs_iter().find(|(name, _)| *name == MAIN_BRANCH) {
        return vec![TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: reference.clone(),
        }];
    }
    let snapshot_id = (uuid::Uuid::new_v4().as_u128() as i64).abs().max(1);
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_sequence_number(metadata.last_sequence_number() + 1)
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
        .with_manifest_list(format!(
            "{}/metadata/snap-{snapshot_id}-write-probe.avro",
            metadata.location()
        ))
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(metadata.current_schema_id());
    let snapshot = if metadata.format_version() >= FormatVersion::V3 {
        snapshot.with_row_range(metadata.next_row_id(), 0).build()
    } else {
        snapshot.build()
    };
    vec![
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ]
}
