//! Snapshot properties on user writes (change `write-snapshot-properties`,
//! section 3).
//!
//! `SET SNAPSHOT_PROPERTIES` (or the transaction's own set) rides the
//! statement's task-local scope ([`sqe_catalog::txn::snapshot_properties`]).
//! The write paths read it **once per statement**, before their commit loop,
//! and move the value into the retry closure: the task-local does not cross
//! a `tokio::spawn`, and a retry must stamp the same map on every attempt.
//!
//! Only user-write action sites call the helpers below. Maintenance commits
//! (compaction, Puffin carry, the ledger, snapshot expiry) never do, so a
//! session's properties never land on a snapshot the user did not write.
//!
//! The vendored snapshot producer merges the user's keys *under* the
//! computed summary (`additional_properties = user; extend(computed)`), so a
//! user key cannot shadow `operation` or a computed metric such as
//! `added-data-files`; the `commit.` / `write.` prefixes that the producer
//! reads for manifest merging are refused when the properties are set
//! (`SnapshotProperties::try_new`).

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::transaction::{
    ApplyTransactionAction, FastAppendAction, OverwriteFilesAction, RowDeltaAction, Transaction,
};
use iceberg::{Catalog, TableIdent};
use sqe_catalog::SessionCatalog;
use sqe_core::snapshot_properties::SnapshotProperties;
use sqe_core::{Session, SqeError, SqeErrorCode};

use super::WriteHandler;
use crate::commit::{commit_with_retry, txn_target_branch};

/// The properties a statement's commits carry; `None` is none.
pub(super) type Props = Option<Arc<SnapshotProperties>>;

/// The statement's snapshot properties. Read once, before the commit loop.
pub(super) fn current() -> Props {
    sqe_catalog::txn::snapshot_properties()
}

fn as_hash_map(props: &Props) -> Option<HashMap<String, String>> {
    props.as_deref().filter(|p| !p.is_empty()).map(|p| {
        p.as_map()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    })
}

/// Stamp an append. Does nothing without properties.
pub(super) fn stamp_append(action: FastAppendAction, props: &Props) -> FastAppendAction {
    match as_hash_map(props) {
        Some(map) => action.set_snapshot_properties(map),
        None => action,
    }
}

/// Stamp a row delta (merge-on-read DELETE, UPDATE, MERGE). Does nothing
/// without properties.
pub(super) fn stamp_row_delta(action: RowDeltaAction, props: &Props) -> RowDeltaAction {
    match as_hash_map(props) {
        Some(map) => action.set_snapshot_properties(map),
        None => action,
    }
}

/// Stamp a copy-on-write rewrite. Does nothing without properties.
pub(super) fn stamp_overwrite(action: &mut OverwriteFilesAction, props: &Props) {
    if let Some(map) = as_hash_map(props) {
        action.set_snapshot_properties(map);
    }
}

impl WriteHandler {
    /// The commit of a write that changed no rows.
    ///
    /// A statement that adds and removes no file would normally commit
    /// nothing. When it carries snapshot properties the user asked for a
    /// marked snapshot (a pipeline run id, a ticket), so this commits an
    /// empty `fast_append` that carries them: no data change, one snapshot
    /// whose summary holds the properties. Returns that snapshot's id, or
    /// `None` when there are no properties and nothing was committed
    /// (behaviour unchanged).
    ///
    /// Call it only *after* the statement's own write-access checks, so a
    /// principal who may not write the table cannot stamp a snapshot on it
    /// with `DELETE ... WHERE false`. It re-checks anyway (the check is
    /// cached), and routes to the transaction or session branch the way every
    /// other write does.
    pub(super) async fn commit_empty_snapshot(
        &self,
        session: &Session,
        catalog: &Arc<SessionCatalog>,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<Option<i64>> {
        let props = current();
        if as_hash_map(&props).is_none() {
            return Ok(None);
        }
        let bridge = catalog.as_catalog();
        let bridge_ref = bridge.as_ref();
        catalog.admit_txn_write(table_ident).await?;
        let table = bridge_ref
            .load_table(table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table {table_ident}: {e}")))?;
        catalog.check_write_access(table_ident, &table).await?;
        let target_branch = txn_target_branch(bridge_ref, table_ident).await?;
        let written_ref = target_branch.clone();
        let committed = commit_with_retry(bridge_ref, table_ident, "empty-write", move |fresh| {
            let props = props.clone();
            let target_branch = target_branch.clone();
            async move {
                let tx = Transaction::new(&fresh);
                let mut action = stamp_append(tx.fast_append(), &props);
                if let Some(branch) = target_branch {
                    action = action.set_target_branch(branch);
                }
                let tx = action.apply(tx)?;
                self.before_commit_seam().await;
                tx.commit(bridge_ref).await
            }
        })
        .await
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to commit snapshot properties of an empty write: {e}"),
            )
        })?;
        self.after_commit(session, catalog.as_ref(), table_ident)
            .await;
        let metadata = committed.metadata();
        Ok(match written_ref {
            Some(branch) => metadata.snapshot_for_ref(&branch).map(|s| s.snapshot_id()),
            None => metadata.current_snapshot_id(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, &str)]) -> Props {
        Some(Arc::new(
            SnapshotProperties::try_new(
                pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
            )
            .expect("valid properties"),
        ))
    }

    #[test]
    fn no_properties_is_no_map() {
        assert!(as_hash_map(&None).is_none());
        assert!(as_hash_map(&Some(Arc::new(SnapshotProperties::default()))).is_none());
    }

    #[test]
    fn properties_become_a_map_of_the_same_pairs() {
        let map = as_hash_map(&props(&[("run-id", "r1"), ("ticket", "T-9")])).expect("some");
        assert_eq!(map.len(), 2);
        assert_eq!(map["run-id"], "r1");
        assert_eq!(map["ticket"], "T-9");
    }
}
