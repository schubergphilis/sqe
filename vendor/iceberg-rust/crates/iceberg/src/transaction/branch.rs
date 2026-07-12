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

//! Transaction actions for managing named branches and tags on Iceberg tables.
//!
//! The Iceberg spec tracks named snapshot references in the table metadata.
//! Branches are mutable pointers that advance as new snapshots are committed.
//! Tags are immutable pointers to a specific snapshot.
//!
//! Both create and drop operations are exposed as `TransactionAction` implementations,
//! so they participate in the standard commit-with-retry pipeline.

use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableUpdate};

/// A transactional action that creates or updates a named branch.
///
/// If `snapshot_id` is `None`, the branch points at the table's current snapshot.
/// If the table has no snapshots yet, the action fails.
///
/// `retention` defaults to `SnapshotRetention::Branch` with all fields `None`,
/// which means the engine falls back to table-level expire properties.
pub struct CreateBranchAction {
    name: String,
    snapshot_id: Option<i64>,
    retention: Option<SnapshotRetention>,
    if_not_exists: bool,
}

impl CreateBranchAction {
    /// Creates a new branch action pointing at `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            snapshot_id: None,
            retention: None,
            if_not_exists: false,
        }
    }

    /// Pin the branch to a specific snapshot id (instead of the current snapshot).
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Override the default retention policy.
    pub fn with_retention(mut self, retention: SnapshotRetention) -> Self {
        self.retention = Some(retention);
        self
    }

    /// If true, a pre-existing reference with the same name is treated as success.
    /// If false (default), the commit will fail on a duplicate ref.
    pub fn if_not_exists(mut self, on: bool) -> Self {
        self.if_not_exists = on;
        self
    }
}

#[async_trait]
impl TransactionAction for CreateBranchAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Reject tag-style retention: branches must carry Branch retention.
        if let Some(SnapshotRetention::Tag { .. }) = &self.retention {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "CREATE BRANCH requires Branch retention, got Tag",
            ));
        }

        if self.if_not_exists && table.metadata().snapshot_for_ref(&self.name).is_some() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let snapshot_id = match self.snapshot_id {
            Some(id) => {
                if table.metadata().snapshot_by_id(id).is_none() {
                    return Err(Error::new(
                        ErrorKind::PreconditionFailed,
                        format!(
                            "CREATE BRANCH {}: snapshot id {id} not found in table history",
                            self.name
                        ),
                    ));
                }
                id
            }
            None => table.metadata().current_snapshot_id().ok_or_else(|| {
                Error::new(
                    ErrorKind::PreconditionFailed,
                    "CREATE BRANCH requires an existing snapshot; table has none",
                )
            })?,
        };

        let retention = self.retention.clone().unwrap_or(SnapshotRetention::Branch {
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        });

        let updates = vec![TableUpdate::SetSnapshotRef {
            ref_name: self.name.clone(),
            reference: SnapshotReference {
                snapshot_id,
                retention,
            },
        }];

        Ok(ActionCommit::new(updates, vec![]))
    }
}

/// A transactional action that creates or updates a tag.
///
/// Tags are immutable labels on a single snapshot. Repeated commits to the
/// same tag name with `create_or_replace = true` overwrite the previous tag.
pub struct CreateTagAction {
    name: String,
    snapshot_id: Option<i64>,
    max_ref_age_ms: Option<i64>,
    create_or_replace: bool,
}

impl CreateTagAction {
    /// Creates a new tag action targeting `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            snapshot_id: None,
            max_ref_age_ms: None,
            create_or_replace: false,
        }
    }

    /// Pin the tag to a specific snapshot id.
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Set the max reference age for the tag.
    pub fn with_max_ref_age_ms(mut self, ms: i64) -> Self {
        self.max_ref_age_ms = Some(ms);
        self
    }

    /// If true, replace an existing tag of the same name rather than failing.
    pub fn create_or_replace(mut self, on: bool) -> Self {
        self.create_or_replace = on;
        self
    }
}

#[async_trait]
impl TransactionAction for CreateTagAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let existing = table.metadata().snapshot_for_ref(&self.name);
        if existing.is_some() && !self.create_or_replace {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!(
                    "CREATE TAG {}: reference already exists (use CREATE OR REPLACE)",
                    self.name
                ),
            ));
        }

        let snapshot_id = match self.snapshot_id {
            Some(id) => {
                if table.metadata().snapshot_by_id(id).is_none() {
                    return Err(Error::new(
                        ErrorKind::PreconditionFailed,
                        format!(
                            "CREATE TAG {}: snapshot id {id} not found in table history",
                            self.name
                        ),
                    ));
                }
                id
            }
            None => table.metadata().current_snapshot_id().ok_or_else(|| {
                Error::new(
                    ErrorKind::PreconditionFailed,
                    "CREATE TAG requires an existing snapshot; table has none",
                )
            })?,
        };

        let retention = SnapshotRetention::Tag {
            max_ref_age_ms: self.max_ref_age_ms,
        };

        let updates = vec![TableUpdate::SetSnapshotRef {
            ref_name: self.name.clone(),
            reference: SnapshotReference {
                snapshot_id,
                retention,
            },
        }];

        Ok(ActionCommit::new(updates, vec![]))
    }
}

/// A transactional action that removes a named reference.
///
/// Removing the `main` branch is not allowed; the engine rejects it before
/// the catalog round-trip.
pub struct RemoveRefAction {
    name: String,
    if_exists: bool,
}

impl RemoveRefAction {
    /// Creates a drop action for `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            if_exists: false,
        }
    }

    /// If true, missing references are silently ignored.
    pub fn if_exists(mut self, on: bool) -> Self {
        self.if_exists = on;
        self
    }
}

#[async_trait]
impl TransactionAction for RemoveRefAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.name == MAIN_BRANCH {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "cannot drop the main branch",
            ));
        }

        if table.metadata().snapshot_for_ref(&self.name).is_none() {
            if self.if_exists {
                return Ok(ActionCommit::new(vec![], vec![]));
            }
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("reference {} does not exist", self.name),
            ));
        }

        let updates = vec![TableUpdate::RemoveSnapshotRef {
            ref_name: self.name.clone(),
        }];

        Ok(ActionCommit::new(updates, vec![]))
    }
}

impl Transaction {
    /// Creates a new `CreateBranchAction` for this transaction.
    pub fn create_branch(&self, name: impl Into<String>) -> CreateBranchAction {
        CreateBranchAction::new(name)
    }

    /// Creates a new `CreateTagAction` for this transaction.
    pub fn create_tag(&self, name: impl Into<String>) -> CreateTagAction {
        CreateTagAction::new(name)
    }

    /// Creates a drop-branch action.
    ///
    /// Dropping the `main` branch fails at commit time.
    pub fn drop_branch(&self, name: impl Into<String>) -> RemoveRefAction {
        RemoveRefAction::new(name)
    }

    /// Creates a drop-tag action.
    pub fn drop_tag(&self, name: impl Into<String>) -> RemoveRefAction {
        RemoveRefAction::new(name)
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::{MAIN_BRANCH, SnapshotRetention};
    use crate::transaction::branch::{CreateBranchAction, CreateTagAction, RemoveRefAction};

    #[test]
    fn test_create_branch_builder() {
        let action = CreateBranchAction::new("feature_x");
        assert_eq!(action.name, "feature_x");
        assert!(action.snapshot_id.is_none());
        assert!(action.retention.is_none());
        assert!(!action.if_not_exists);
    }

    #[test]
    fn test_create_branch_with_snapshot_id() {
        let action = CreateBranchAction::new("hist").with_snapshot_id(42);
        assert_eq!(action.snapshot_id, Some(42));
    }

    #[test]
    fn test_create_branch_with_retention() {
        let retention = SnapshotRetention::branch(Some(5), Some(1000), Some(2000));
        let action = CreateBranchAction::new("retained").with_retention(retention.clone());
        assert_eq!(action.retention, Some(retention));
    }

    #[test]
    fn test_create_branch_if_not_exists() {
        let action = CreateBranchAction::new("x").if_not_exists(true);
        assert!(action.if_not_exists);
    }

    #[test]
    fn test_create_tag_builder() {
        let action = CreateTagAction::new("v1")
            .with_snapshot_id(7)
            .with_max_ref_age_ms(86_400_000);
        assert_eq!(action.name, "v1");
        assert_eq!(action.snapshot_id, Some(7));
        assert_eq!(action.max_ref_age_ms, Some(86_400_000));
        assert!(!action.create_or_replace);
    }

    #[test]
    fn test_create_or_replace_tag_builder() {
        let action = CreateTagAction::new("v1").create_or_replace(true);
        assert!(action.create_or_replace);
    }

    #[test]
    fn test_remove_ref_builder() {
        let action = RemoveRefAction::new("stale").if_exists(true);
        assert_eq!(action.name, "stale");
        assert!(action.if_exists);
    }

    #[test]
    fn test_main_branch_constant() {
        assert_eq!(MAIN_BRANCH, "main");
    }
}
