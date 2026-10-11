//! Transactions on a per-transaction branch (#821, #823).
//!
//! An explicit transaction writes to its own Iceberg branch,
//! `sqe-txn-<id>`, of every table it writes. The coordinator runs every
//! statement of the transaction inside [`scope`]; while a scope is active:
//!
//! - the first write to a table claims it ([`target_branch`]) and the
//!   branch is created at the `main` snapshot that write read;
//! - every write to a claimed table commits to its branch;
//! - every load of a claimed table through the session catalog returns the
//!   branch read as `main` ([`iceberg::table::Table::pinned_to_ref`]), so a
//!   statement sees the writes of the statements before it;
//! - the session catalog refuses a commit that would move `main`, so a
//!   write path that forgot the branch fails instead of publishing early.
//!
//! `COMMIT` publishes every branch onto its `main` with
//! `iceberg::transaction::PublishBranchAction`, outside any scope. One table
//! is one `update_table`; several tables are one
//! [`Catalog::commit_transaction`] (REST `POST /v1/transactions/commit`),
//! so all of them move or none does. That needs every table in one catalog,
//! and a catalog that can commit several tables at once: the session catalog
//! refuses a write that would break either, before the write runs.
//!
//! The scope is a task-local, not session state: the session catalogs are
//! shared by every session of the same principal, and a pin stored on them
//! would leak branch reads into the user's other sessions.
//!
//! The table is matched by its Iceberg table UUID, not its name: two
//! catalogs may hold the same `namespace.table`, and the UUID is what every
//! loaded copy of the table carries.

use std::future::Future;
use std::sync::{Arc, Mutex};

use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use uuid::Uuid;

/// Prefix of every transaction branch. A sweep may remove refs with this
/// prefix whose transaction no coordinator knows.
pub const TXN_BRANCH_PREFIX: &str = "sqe-txn-";

/// Whether `name` is a transaction branch.
pub fn is_txn_ref(name: &str) -> bool {
    name.starts_with(TXN_BRANCH_PREFIX)
}

/// Refuse a statement that names a transaction branch: reading one
/// (`FOR VERSION AS OF`), writing to one (`SET WRITE_BRANCH`), or creating
/// or replacing one. An open transaction's writes are invisible to
/// everyone but that transaction until COMMIT; its branch is reachable only
/// through the transaction's own pin. Dropping one is allowed: it discloses
/// nothing, and it is how a branch left by a crashed coordinator is removed.
pub fn reject_txn_ref(name: &str) -> sqe_core::Result<()> {
    if is_txn_ref(name) {
        return Err(sqe_core::SqeError::with_code(
            sqe_core::SqeErrorCode::InvalidArguments,
            format!(
                "'{name}': branch names starting with '{TXN_BRANCH_PREFIX}' are reserved \
                 for transactions"
            ),
        ));
    }
    Ok(())
}

/// Snapshots only a transaction branch reaches: written by an open
/// transaction, not yet on `main` or any other branch or tag. Reads by
/// snapshot id and the snapshot listings hide them, so an uncommitted
/// transaction cannot be read by guessing or listing ids.
pub fn txn_private_snapshots(
    metadata: &iceberg::spec::TableMetadata,
) -> std::collections::HashSet<i64> {
    let ancestry = |head: i64| {
        std::iter::successors(metadata.snapshot_by_id(head), |s| {
            s.parent_snapshot_id()
                .and_then(|p| metadata.snapshot_by_id(p))
        })
        .map(|s| s.snapshot_id())
    };
    let (txn_refs, public_refs): (Vec<_>, Vec<_>) =
        metadata.refs_iter().partition(|(name, _)| is_txn_ref(name));
    if txn_refs.is_empty() {
        return Default::default();
    }
    let public: std::collections::HashSet<i64> = public_refs
        .iter()
        .flat_map(|(_, r)| ancestry(r.snapshot_id))
        .collect();
    txn_refs
        .iter()
        .flat_map(|(_, r)| ancestry(r.snapshot_id))
        .filter(|id| !public.contains(id))
        .collect()
}

/// A table a transaction writes, claimed by its first write to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnTable {
    pub table_uuid: Uuid,
    pub ident: TableIdent,
    /// `main` when the transaction first wrote; `None` for a table with
    /// no snapshot. `COMMIT` requires `main` to still be here.
    pub base_snapshot_id: Option<i64>,
    pub branch: String,
}

/// One explicit transaction's write state.
pub struct TxnScope {
    id: String,
    tables: Mutex<Vec<TxnTable>>,
    /// Identifies the catalog the branch commits went through (endpoint and
    /// warehouse), not the principal: every table of a transaction shares it.
    catalog_key: Mutex<Option<String>>,
    /// The catalog the last commit to a branch went through: COMMIT and
    /// ROLLBACK reach the tables the same way, as the same principal.
    catalog: Mutex<Option<Arc<dyn Catalog>>>,
}

impl std::fmt::Debug for TxnScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnScope")
            .field("id", &self.id)
            .field("tables", &self.tables())
            .field("catalog_key", &self.catalog_key())
            .finish()
    }
}

impl TxnScope {
    pub fn new(id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            id: id.into(),
            tables: Mutex::new(Vec::new()),
            catalog_key: Mutex::new(None),
            catalog: Mutex::new(None),
        })
    }

    /// Remember the catalog a branch commit went through. Called by the
    /// session catalog for every commit that sets this transaction's
    /// branch, so the latest bearer token wins. The session catalog refuses
    /// a branch commit to a second catalog before it lands ([`admit`]).
    pub fn bind_catalog(&self, key: impl Into<String>, catalog: Arc<dyn Catalog>) {
        self.bind_key(key);
        *self.catalog.lock().unwrap_or_else(|e| e.into_inner()) = Some(catalog);
    }

    fn bind_key(&self, key: impl Into<String>) {
        *self.catalog_key.lock().unwrap_or_else(|e| e.into_inner()) = Some(key.into());
    }

    /// The catalog to publish or drop the branches through; `None` when no
    /// branch commit ever landed.
    pub fn catalog(&self) -> Option<Arc<dyn Catalog>> {
        self.catalog
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The key of the catalog this transaction's branch commits went
    /// through, once one landed.
    pub fn catalog_key(&self) -> Option<String> {
        self.catalog_key
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// The branch this transaction writes to, the same name on every table.
    pub fn branch_name(&self) -> String {
        format!("{TXN_BRANCH_PREFIX}{}", self.id)
    }

    /// The tables this transaction wrote, in the order it first wrote them.
    pub fn tables(&self) -> Vec<TxnTable> {
        self.lock().clone()
    }

    /// The transaction's branch if it already claimed `table_uuid`.
    fn claimed_branch(&self, table_uuid: Uuid) -> Option<String> {
        self.claimed(table_uuid).map(|t| t.branch)
    }

    fn claimed(&self, table_uuid: Uuid) -> Option<TxnTable> {
        self.lock()
            .iter()
            .find(|t| t.table_uuid == table_uuid)
            .cloned()
    }

    fn record(&self, table: TxnTable) {
        let mut tables = self.lock();
        if !tables.iter().any(|t| t.table_uuid == table.table_uuid) {
            tables.push(table);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<TxnTable>> {
        // The guarded value is plain data; a panic while holding the lock
        // cannot leave it half-written.
        self.tables.lock().unwrap_or_else(|e| e.into_inner())
    }
}

tokio::task_local! {
    static CURRENT: Option<Arc<TxnScope>>;
}

/// Run `f` inside `txn`'s scope. `None` runs it outside any transaction,
/// even when the caller is inside one: COMMIT publishes the branch to
/// `main`, which the bridge refuses from inside a scope.
pub async fn scope<F: Future>(txn: Option<Arc<TxnScope>>, f: F) -> F::Output {
    CURRENT.scope(txn, f).await
}

/// The transaction the current task runs in, if any.
pub fn current() -> Option<Arc<TxnScope>> {
    CURRENT.try_with(Option::clone).ok().flatten()
}

/// `f` in the caller's transaction scope, captured now.
///
/// The scope is a task-local, which tokio keeps in a thread-local while the
/// task is polled, so a future driven on another thread
/// (`sqe_core::runtime::on_planning_stack`) runs outside it and plans its
/// table loads against `main`. Wrap such a future here, on the calling
/// thread.
pub fn carried<F: Future>(f: F) -> impl Future<Output = F::Output> {
    CURRENT.scope(current(), f)
}

/// Whether a write to `ident` through the catalog `catalog_key` may join
/// the current transaction. `atomic_multi_table` says whether that catalog
/// can commit several tables at once; it is only consulted when the
/// transaction already wrote another table.
///
/// Called by the session catalog before a statement writes any data file,
/// and again for every branch commit, so a transaction never holds tables
/// it cannot publish together.
pub fn admit(
    txn: &TxnScope,
    ident: &TableIdent,
    catalog_key: &str,
    atomic_multi_table: impl FnOnce() -> bool,
) -> sqe_core::Result<()> {
    if let Some(bound) = txn.catalog_key() {
        if bound != catalog_key {
            return Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::NotSupported,
                format!(
                    "A transaction cannot write tables in two catalogs: it already wrote \
                     through {bound}, and {ident} is in {catalog_key}"
                ),
            ));
        }
    }
    let other = txn.tables().into_iter().find(|t| &t.ident != ident);
    if let Some(other) = other {
        if !atomic_multi_table() {
            return Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::NotSupported,
                format!(
                    "Multi-table writes not supported in a single transaction on this \
                     catalog: it cannot commit several tables atomically. This transaction \
                     already wrote {}, not {ident}",
                    other.ident
                ),
            ));
        }
    }
    Ok(())
}

/// How a loaded table reads inside the current transaction.
#[derive(Debug)]
pub enum Pin {
    /// Not the transaction's table, or no transaction: read it as loaded.
    Unpinned(Table),
    /// The transaction's table, read at its branch.
    Pinned(Table),
    /// The transaction's table, but this copy has no branch ref: a cached
    /// copy from before the branch existed, or the branch was dropped.
    Stale { branch: String },
}

/// Read `table` at the current transaction's branch when the transaction
/// wrote it.
pub fn pin(table: Table) -> Pin {
    let Some(claimed) = current().and_then(|txn| txn.claimed(table.metadata().uuid())) else {
        return Pin::Unpinned(table);
    };
    match table.pinned_to_ref(&claimed.branch) {
        Some(pinned) => Pin::Pinned(pinned),
        // An empty table's branch is created by the transaction's first
        // write commit; until that lands, `main` (no snapshot) is the base.
        None if claimed.base_snapshot_id.is_none() => Pin::Unpinned(table),
        None => Pin::Stale {
            branch: claimed.branch,
        },
    }
}

/// The branch a write to `table` must target: `None` outside a
/// transaction, else the transaction branch.
///
/// The first write of a transaction to a table claims it and creates the
/// branch at the `main` snapshot `table` was loaded at, so the
/// transaction's base for that table is the state its first statement
/// read. A table without a snapshot gets no branch here: the first write
/// creates it with no parent.
///
/// `catalog` must be the session catalog bridge, so the branch commit
/// evicts every cached copy of the table and is checked by
/// [`admit`].
pub async fn target_branch(
    catalog: &dyn Catalog,
    table: &Table,
) -> sqe_core::Result<Option<String>> {
    let Some(txn) = current() else {
        return Ok(None);
    };
    let table_uuid = table.metadata().uuid();
    let ident = table.identifier();
    if let Some(branch) = txn.claimed_branch(table_uuid) {
        return Ok(Some(branch));
    }
    let branch = txn.branch_name();
    let base_snapshot_id = table.metadata().current_snapshot_id();
    if let Some(base) = base_snapshot_id {
        use iceberg::transaction::{ApplyTransactionAction, Transaction};
        let tx = Transaction::new(table);
        let started = tx
            .create_branch(branch.clone())
            .with_snapshot_id(base)
            .apply(tx)
            .map_err(|e| {
                sqe_core::SqeError::catalog_src(
                    format!("Failed to start transaction branch {branch} on {ident}"),
                    e,
                )
            })?;
        started.commit(catalog).await.map_err(|e| {
            sqe_core::SqeError::catalog_src(
                format!("Failed to create transaction branch {branch} on {ident}"),
                e,
            )
        })?;
    }
    txn.record(TxnTable {
        table_uuid,
        ident: ident.clone(),
        base_snapshot_id,
        branch: branch.clone(),
    });
    Ok(Some(branch))
}

/// COMMIT: move `main` of every table the transaction wrote to its branch
/// head and drop the branch, in one catalog commit. A transaction that
/// wrote nothing publishes nothing.
///
/// Fails with `TransactionConflict` when `main` of any table moved since
/// the transaction's first write to it; every branch is dropped then, so
/// the tables are left as if the transaction never ran. Any other failure
/// (catalog unreachable, token expired) keeps the branches: the transaction
/// is still open and COMMIT or ROLLBACK can be sent again. Run it outside
/// any [`scope`]: inside one, the session catalog refuses to move `main`.
pub async fn publish(txn: &TxnScope) -> sqe_core::Result<()> {
    let tables = txn.tables();
    let Some(catalog) = txn.catalog() else {
        // Nothing written, or the first write failed before its branch
        // commit landed.
        return Ok(());
    };
    let result = match tables.as_slice() {
        [] => return Ok(()),
        [table] => publish_branch(catalog.as_ref(), table).await,
        _ => publish_branches(catalog.as_ref(), &tables).await,
    };
    if result
        .as_ref()
        .is_err_and(|e| e.error_code() == sqe_core::SqeErrorCode::TransactionConflict)
    {
        discard(txn).await;
    }
    result
}

/// Several tables: one `PublishBranchAction` per table, prepared against
/// a fresh load of each, sent as one [`Catalog::commit_transaction`].
async fn publish_branches(catalog: &dyn Catalog, tables: &[TxnTable]) -> sqe_core::Result<()> {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    const MAX_ATTEMPTS: u32 = 4;
    let names = tables
        .iter()
        .map(|t| t.ident.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let conflict = |e: iceberg::Error| {
        sqe_core::SqeError::with_code(
            sqe_core::SqeErrorCode::TransactionConflict,
            format!("Transaction on {names} rolled back: {e}"),
        )
    };
    let mut attempt = 1;
    loop {
        let mut commits = Vec::with_capacity(tables.len());
        for TxnTable {
            ident,
            branch,
            base_snapshot_id,
            ..
        } in tables
        {
            let table = catalog.load_table(ident).await.map_err(|e| {
                sqe_core::SqeError::catalog_src(format!("Failed to load table {ident}"), e)
            })?;
            let metadata = table.metadata();
            if base_snapshot_id.is_none()
                && metadata.current_snapshot_id().is_none()
                && metadata.snapshot_for_ref(branch).is_none()
            {
                // An empty table whose first write failed before its commit
                // created the branch: nothing to publish.
                continue;
            }
            let tx = Transaction::new(&table);
            let tx = tx
                .publish_branch(branch.clone(), *base_snapshot_id)
                .apply(tx)
                .map_err(conflict)?;
            // The action checks `main` against the base and refuses a
            // missing branch: the transaction lost, no retry.
            let commit = tx.prepare_commit().await.map_err(|e| {
                if e.kind() == iceberg::ErrorKind::PreconditionFailed {
                    conflict(e)
                } else {
                    sqe_core::SqeError::catalog_src(
                        format!("Failed to prepare the commit of {ident}"),
                        e,
                    )
                }
            })?;
            commits.push(commit);
        }
        if commits.is_empty() {
            return Ok(());
        }
        match catalog.commit_transaction(commits).await {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == iceberg::ErrorKind::FeatureUnsupported => {
                return Err(sqe_core::SqeError::with_code(
                    sqe_core::SqeErrorCode::NotSupported,
                    format!("Cannot commit {names} atomically: {e}"),
                ));
            }
            // Another commit landed on one of the tables between the loads
            // and this commit. Reload: if a `main` moved, the action refuses
            // next time; if only something else changed, the commit goes
            // through.
            Err(e)
                if attempt < MAX_ATTEMPTS
                    && (e.kind() == iceberg::ErrorKind::CatalogCommitConflicts
                        || e.retryable()) =>
            {
                attempt += 1;
            }
            Err(e) if e.kind() == iceberg::ErrorKind::CatalogCommitConflicts || e.retryable() => {
                return Err(conflict(e));
            }
            Err(e) => {
                return Err(sqe_core::SqeError::catalog_src(
                    format!("Failed to commit transaction on {names}"),
                    e,
                ));
            }
        }
    }
}

async fn publish_branch(catalog: &dyn Catalog, txn_table: &TxnTable) -> sqe_core::Result<()> {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    const MAX_ATTEMPTS: u32 = 4;
    let TxnTable {
        ident,
        branch,
        base_snapshot_id,
        ..
    } = txn_table;
    let conflict = |e: iceberg::Error| {
        sqe_core::SqeError::with_code(
            sqe_core::SqeErrorCode::TransactionConflict,
            format!("Transaction on {ident} rolled back: {e}"),
        )
    };
    let mut attempt = 1;
    loop {
        let table = catalog.load_table(ident).await.map_err(|e| {
            sqe_core::SqeError::catalog_src(format!("Failed to load table {ident}"), e)
        })?;
        let tx = Transaction::new(&table);
        let tx = tx
            .publish_branch(branch.clone(), *base_snapshot_id)
            .apply(tx)
            .map_err(conflict)?;
        match tx.commit(catalog).await {
            Ok(_) => return Ok(()),
            // The action, run by the commit, checks `main` against the base
            // and refuses a missing branch: the transaction lost, no retry.
            Err(e) if e.kind() == iceberg::ErrorKind::PreconditionFailed => {
                return Err(conflict(e));
            }
            // The catalog saw another commit land between the load and this
            // one. Reload: if `main` moved, the action refuses next time; if
            // only something else changed, the publish goes through.
            Err(e)
                if attempt < MAX_ATTEMPTS
                    && (e.kind() == iceberg::ErrorKind::CatalogCommitConflicts
                        || e.retryable()) =>
            {
                attempt += 1;
            }
            Err(e) if e.kind() == iceberg::ErrorKind::CatalogCommitConflicts || e.retryable() => {
                return Err(conflict(e));
            }
            Err(e) => {
                return Err(sqe_core::SqeError::catalog_src(
                    format!("Failed to commit transaction on {ident}"),
                    e,
                ));
            }
        }
    }
}

/// ROLLBACK, and the end of a failed COMMIT: drop the transaction branch
/// of every table. Best effort, one table at a time: a branch left behind
/// holds no visible data, and its `sqe-txn-` name marks it for cleanup.
pub async fn discard(txn: &TxnScope) {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let Some(catalog) = txn.catalog() else {
        return;
    };
    for TxnTable { ident, branch, .. } in txn.tables() {
        let dropped = async {
            let table = catalog.load_table(&ident).await?;
            let tx = Transaction::new(&table);
            let tx = tx.drop_branch(branch.clone()).if_exists(true).apply(tx)?;
            tx.commit(catalog.as_ref()).await
        }
        .await;
        if let Err(e) = dropped {
            tracing::warn!(
                table = %ident,
                branch = %branch,
                error = %e,
                "could not drop transaction branch; it holds no visible data"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(name: &str) -> TableIdent {
        TableIdent::from_strs(["ns", name]).unwrap()
    }

    fn txn_on(uuid: Uuid) -> Arc<TxnScope> {
        let txn = TxnScope::new("tx-1");
        txn.record(TxnTable {
            table_uuid: uuid,
            ident: ident("t"),
            base_snapshot_id: Some(7),
            branch: txn.branch_name(),
        });
        txn
    }

    #[test]
    fn an_unclaimed_transaction_has_no_branch_and_pins_nothing() {
        let txn = TxnScope::new("tx-1");
        assert_eq!(txn.branch_name(), "sqe-txn-tx-1");
        assert_eq!(txn.claimed_branch(Uuid::from_u128(1)), None);
    }

    #[test]
    fn the_claimed_table_reuses_the_branch() {
        let t = Uuid::from_u128(1);
        let txn = txn_on(t);
        let branch = Some("sqe-txn-tx-1".to_string());
        assert_eq!(txn.claimed_branch(t), branch);
    }

    #[test]
    fn a_second_table_is_claimed_alongside_the_first() {
        let txn = txn_on(Uuid::from_u128(1));
        txn.record(TxnTable {
            table_uuid: Uuid::from_u128(2),
            ident: ident("u"),
            base_snapshot_id: None,
            branch: txn.branch_name(),
        });
        // Recording a table twice keeps its first base.
        txn.record(TxnTable {
            table_uuid: Uuid::from_u128(1),
            ident: ident("t"),
            base_snapshot_id: Some(99),
            branch: txn.branch_name(),
        });
        let tables = txn.tables();
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].base_snapshot_id, Some(7));
        assert_eq!(tables[1].ident, ident("u"));
        assert_eq!(
            txn.claimed_branch(Uuid::from_u128(2)).as_deref(),
            Some("sqe-txn-tx-1")
        );
    }

    #[test]
    fn a_second_table_needs_an_atomic_catalog() {
        let txn = txn_on(Uuid::from_u128(1));
        txn.bind_key("polaris|wh");
        // The claimed table itself never needs the multi-table commit.
        admit(&txn, &ident("t"), "polaris|wh", || panic!("not consulted")).unwrap();
        admit(&txn, &ident("u"), "polaris|wh", || true).unwrap();
        let err = admit(&txn, &ident("u"), "polaris|wh", || false).unwrap_err();
        assert_eq!(err.error_code(), sqe_core::SqeErrorCode::NotSupported);
        assert!(
            err.to_string()
                .contains("cannot commit several tables atomically"),
            "{err}"
        );
    }

    #[test]
    fn a_second_catalog_is_refused() {
        let txn = txn_on(Uuid::from_u128(1));
        txn.bind_key("polaris|wh");
        // Even the same name: `t` in another catalog is another table.
        for name in ["t", "u"] {
            let err = admit(&txn, &ident(name), "glue|other", || true).unwrap_err();
            assert_eq!(err.error_code(), sqe_core::SqeErrorCode::NotSupported);
            assert!(err.to_string().contains("two catalogs"), "{err}");
        }
    }

    /// Two snapshots, BASE <- HEAD, with refs pointing where the test says.
    fn metadata_with_refs(refs: &[(&str, i64)]) -> iceberg::spec::TableMetadata {
        use iceberg::spec::{SnapshotReference, SnapshotRetention, TableMetadata};
        let json = include_str!(
            "../../../vendor/iceberg-rust/crates/iceberg/testdata/table_metadata/TableMetadataV2Valid.json"
        );
        let mut builder = serde_json::from_str::<TableMetadata>(json)
            .unwrap()
            .into_builder(None);
        for (name, id) in refs {
            let retention = SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            };
            builder = builder
                .set_ref(name, SnapshotReference::new(*id, retention))
                .unwrap();
        }
        builder.build().unwrap().metadata
    }

    const BASE: i64 = 3051729675574597004;
    const HEAD: i64 = 3055729675574597004;

    #[test]
    fn snapshots_only_a_transaction_branch_reaches_are_private() {
        let open = metadata_with_refs(&[("main", BASE), ("sqe-txn-1", HEAD)]);
        assert_eq!(txn_private_snapshots(&open), [HEAD].into());

        // Published: main reaches HEAD, nothing is private.
        let published = metadata_with_refs(&[("main", HEAD), ("sqe-txn-1", HEAD)]);
        assert!(txn_private_snapshots(&published).is_empty());

        // Another ref on HEAD makes it public (a user's branch or tag).
        let shared = metadata_with_refs(&[("main", BASE), ("sqe-txn-1", HEAD), ("dev", HEAD)]);
        assert!(txn_private_snapshots(&shared).is_empty());

        // The transaction's own pinned view reads HEAD as main.
        let pinned = open.pinned_to_ref("sqe-txn-1").unwrap();
        assert!(txn_private_snapshots(&pinned).is_empty());
    }

    #[test]
    fn transaction_branch_names_are_reserved() {
        assert!(reject_txn_ref("sqe-txn-abc").is_err());
        assert!(reject_txn_ref("main").is_ok());
        assert!(reject_txn_ref("dev-sqe-txn-").is_ok());
    }

    #[tokio::test]
    async fn current_is_only_set_inside_the_scope() {
        assert!(current().is_none());
        let txn = TxnScope::new("tx-2");
        let seen = scope(Some(Arc::clone(&txn)), async {
            current().map(|t| t.id().to_string())
        })
        .await;
        assert_eq!(seen.as_deref(), Some("tx-2"));
        assert!(scope(None, async { current() }).await.is_none());
        assert!(current().is_none());
    }

    /// The planning stack polls on its own thread, where the task-local is
    /// unset; `carried` brings the scope along. Multi-thread flavor: on a
    /// current-thread runtime the planning stack runs inline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_scope_reaches_the_planning_stack_only_when_carried() {
        use sqe_core::runtime::on_planning_stack;
        let txn = TxnScope::new("tx-3");
        let (lost, carried_id) = scope(Some(txn), async {
            let lost = on_planning_stack(async { current() }).await;
            let carried_id = on_planning_stack(carried(async { current() }))
                .await
                .map(|t| t.id().to_string());
            (lost, carried_id)
        })
        .await;
        assert!(lost.is_none(), "an uncarried future runs outside the scope");
        assert_eq!(carried_id.as_deref(), Some("tx-3"));
    }
}
