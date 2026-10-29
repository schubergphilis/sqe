//! Resolve a time-travel clause (`FOR VERSION AS OF`, `FOR TIMESTAMP AS OF`,
//! `FOR SYSTEM_TIME AS OF`) to an Iceberg snapshot id.
//!
//! `sqe_sql::extract_time_travel_spec` strips the clause from the SQL text;
//! the coordinator and `sqe-cli --embedded` both resolve the captured
//! [`VersionRef`] here, so the two answer the same snapshot for the same
//! argument and fail with the same message.

use iceberg::spec::{SnapshotRetention, TableMetadata};
use sqe_core::{Result, SqeError, SqeErrorCode};
use sqe_sql::VersionRef;

/// The snapshot id `version` names on `table` (its name only feeds the
/// error messages).
///
/// - `SnapshotId(id)`: that snapshot, when the table has it.
/// - `Named(name)`: the branch or tag of that name.
/// - `Timestamp(raw)`: the latest snapshot at or before the time.
///
/// An open transaction's branch and the snapshots only it reaches read as
/// absent (#821): uncommitted writes are visible to no one but the
/// transaction itself, which reads them through its pin.
pub fn resolve_version_snapshot(
    metadata: &TableMetadata,
    version: &VersionRef,
    table: &str,
) -> Result<i64> {
    match version {
        VersionRef::SnapshotId(id) => {
            if metadata.snapshot_by_id(*id).is_none()
                || crate::txn::txn_private_snapshots(metadata).contains(id)
            {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("FOR VERSION AS OF {id}: snapshot id not found on {table}"),
                ));
            }
            Ok(*id)
        }
        VersionRef::Named(name) => {
            crate::txn::reject_txn_ref(name)?;
            let Some(reference) = metadata.reference_by_name(name) else {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!(
                        "FOR VERSION AS OF '{name}': no branch or tag with that name on {table}"
                    ),
                ));
            };
            // Iceberg keeps branch and tag names in one namespace, so the
            // name picks exactly one ref.
            let kind = match reference.retention {
                SnapshotRetention::Branch { .. } => "branch",
                SnapshotRetention::Tag { .. } => "tag",
            };
            tracing::debug!(
                table = %table,
                ref_name = %name,
                snapshot_id = reference.snapshot_id,
                kind,
                "FOR VERSION AS OF resolved a named ref"
            );
            Ok(reference.snapshot_id)
        }
        VersionRef::Timestamp(raw) => {
            find_snapshot_at_timestamp(metadata, sqe_sql::time_travel_millis(raw)?)
        }
    }
}

/// Find the latest Iceberg snapshot with `timestamp_ms <= target_ms`.
///
/// Returns an error when the table has no snapshot at or before the given timestamp.
pub fn find_snapshot_at_timestamp(metadata: &TableMetadata, target_ms: i64) -> Result<i64> {
    let mut best: Option<(i64, i64)> = None; // (snapshot_id, timestamp_ms)

    for snap in metadata.snapshots() {
        let snap_ts = snap.timestamp_ms();
        if snap_ts <= target_ms && best.is_none_or(|(_, ts)| snap_ts > ts) {
            best = Some((snap.snapshot_id(), snap_ts));
        }
    }

    best.map(|(id, _)| id).ok_or_else(|| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
                "No Iceberg snapshot found at or before timestamp {target_ms}ms. \
                 The table may not have existed yet at that point in time."
            ),
        )
    })
}
