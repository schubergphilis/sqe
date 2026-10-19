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

/// The longest a transaction stays open, however busy. The coordinator
/// rolls back a transaction this old on its next statement, which is what
/// lets [`stale_txn_refs`] call an older branch dead without asking every
/// coordinator.
pub const MAX_LIFETIME: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

/// Slack on top of [`MAX_LIFETIME`] for clock skew: a branch head's
/// timestamp comes from the clock of the coordinator that wrote it.
const STALE_MARGIN_MS: i64 = 60 * 60 * 1000;

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
    /// What a successful [`publish`] moved `main` to, per table, for the
    /// audit and lineage records of the COMMIT.
    published: Mutex<Vec<(TableIdent, i64)>>,
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
            published: Mutex::new(Vec::new()),
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

    /// The snapshot each table's `main` points at after a successful
    /// [`publish`]; empty before one, and for a transaction that wrote
    /// nothing.
    pub fn published(&self) -> Vec<(TableIdent, i64)> {
        self.published
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn set_published(&self, heads: Vec<(TableIdent, i64)>) {
        *self.published.lock().unwrap_or_else(|e| e.into_inner()) = heads;
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

/// What a statement runs in: an explicit transaction, or the session's
/// `WRITE_BRANCH` for a data write outside one (#829). Never both.
#[derive(Clone, Default)]
struct StatementScope {
    txn: Option<Arc<TxnScope>>,
    write_branch: Option<Arc<str>>,
}

tokio::task_local! {
    static CURRENT: StatementScope;
}

/// Run `f` inside `txn`'s scope. `None` runs it outside any transaction,
/// even when the caller is inside one: COMMIT publishes the branch to
/// `main`, which the bridge refuses from inside a scope.
pub async fn scope<F: Future>(txn: Option<Arc<TxnScope>>, f: F) -> F::Output {
    let scope = StatementScope {
        txn,
        write_branch: None,
    };
    CURRENT.scope(scope, f).await
}

/// Run the data write `f` on the session's `WRITE_BRANCH` (#829): inside
/// `txn` when there is one (the branch is then ignored; a transaction
/// refuses writes with a branch set), else on `write_branch`. While it
/// runs, every table that holds the branch is read at it
/// ([`pin`]), every write commits to it ([`target_branch`]), and the
/// session catalog refuses a commit that would move `main`.
pub async fn write_scope<F: Future>(
    txn: Option<Arc<TxnScope>>,
    write_branch: Option<String>,
    f: F,
) -> F::Output {
    let write_branch = match txn {
        Some(_) => None,
        None => write_branch.map(Arc::from),
    };
    CURRENT.scope(StatementScope { txn, write_branch }, f).await
}

/// The transaction the current task runs in, if any.
pub fn current() -> Option<Arc<TxnScope>> {
    CURRENT.try_with(|s| s.txn.clone()).ok().flatten()
}

/// The session branch the current data write commits to, outside a
/// transaction ([`write_scope`]).
pub fn write_branch() -> Option<Arc<str>> {
    CURRENT.try_with(|s| s.write_branch.clone()).ok().flatten()
}

/// `f` in the caller's statement scope, captured now.
///
/// The scope is a task-local, which tokio keeps in a thread-local while the
/// task is polled, so a future driven on another thread
/// (`sqe_core::runtime::on_planning_stack`) runs outside it and plans its
/// table loads against `main`. Wrap such a future here, on the calling
/// thread.
pub fn carried<F: Future>(f: F) -> impl Future<Output = F::Output> {
    let scope = CURRENT.try_with(StatementScope::clone).unwrap_or_default();
    CURRENT.scope(scope, f)
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
/// wrote it, or at the session's write branch when the table holds it
/// ([`write_scope`]). The second covers the target of a DELETE, UPDATE or
/// MERGE, which must rewrite the branch's files, not `main`'s, and any
/// source table staged on the same branch (Spark's `spark.wap.branch`
/// reads the same way).
pub fn pin(table: Table) -> Pin {
    if let Some(branch) = write_branch() {
        return match table.pinned_to_ref(&branch) {
            Some(pinned) => Pin::Pinned(pinned),
            None => Pin::Unpinned(table),
        };
    }
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

/// The branch a write to `table` must target: the transaction branch
/// inside a transaction, the session's write branch inside a
/// [`write_scope`], else `None` (`main`).
///
/// The first write of a transaction to a table claims it and creates the
/// branch at the `main` snapshot `table` was loaded at, so the
/// transaction's base for that table is the state its first statement
/// read. A table without a snapshot gets no branch here: the first write
/// creates it with no parent.
///
/// A session write branch must already exist on `table`: committing to a
/// missing ref would create it, so a typo in `SET WRITE_BRANCH` would
/// start a new branch instead of failing.
///
/// `catalog` must be the session catalog bridge, so the branch commit
/// evicts every cached copy of the table and is checked by
/// [`admit`].
pub async fn target_branch(
    catalog: &dyn Catalog,
    table: &Table,
) -> sqe_core::Result<Option<String>> {
    if let Some(branch) = write_branch() {
        return existing_write_branch(table, &branch).map(Some);
    }
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

/// `branch` if `table` holds it as a branch. A table without a snapshot
/// holds no branch, so a session branch cannot be written into an empty
/// table. A tag is refused: the commit would move it and rewrite its
/// retention as a branch's.
fn existing_write_branch(table: &Table, branch: &str) -> sqe_core::Result<String> {
    let ident = table.identifier();
    if let Some(reference) = table.metadata().reference_by_name(branch) {
        if !reference.is_branch() {
            return Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::InvalidArguments,
                format!(
                    "WRITE_BRANCH '{branch}' is a tag on {ident}; tags are read-only. \
                     Create a branch from it with ALTER TABLE {ident} CREATE BRANCH \
                     <name> AS OF VERSION <snapshot id>"
                ),
            ));
        }
    }
    if table.metadata().snapshot_for_ref(branch).is_some() {
        return Ok(branch.to_string());
    }
    Err(sqe_core::SqeError::with_code(
        sqe_core::SqeErrorCode::InvalidArguments,
        format!(
            "WRITE_BRANCH '{branch}' does not exist on {ident}; create it with \
             ALTER TABLE {ident} CREATE BRANCH {branch}, or RESET WRITE_BRANCH \
             to write to main"
        ),
    ))
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
        [table] => publish_branch(catalog.as_ref(), table)
            .await
            .map(|head| head.into_iter().collect()),
        _ => publish_branches(catalog.as_ref(), &tables).await,
    };
    match result {
        Ok(heads) => {
            txn.set_published(heads);
            Ok(())
        }
        Err(e) => {
            if e.error_code() == sqe_core::SqeErrorCode::TransactionConflict {
                discard(txn).await;
            }
            Err(e)
        }
    }
}

/// The snapshot `branch` of `table` points at: what publishing it moves
/// `main` to.
fn branch_head(table: &iceberg::table::Table, branch: &str) -> Option<i64> {
    table
        .metadata()
        .snapshot_for_ref(branch)
        .map(|s| s.snapshot_id())
}

/// Several tables: one `PublishBranchAction` per table, prepared against
/// a fresh load of each, sent as one [`Catalog::commit_transaction`].
async fn publish_branches(
    catalog: &dyn Catalog,
    tables: &[TxnTable],
) -> sqe_core::Result<Vec<(TableIdent, i64)>> {
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
        let mut heads = Vec::with_capacity(tables.len());
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
            heads.extend(branch_head(&table, branch).map(|id| (ident.clone(), id)));
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
            return Ok(heads);
        }
        match catalog.commit_transaction(commits).await {
            Ok(()) => return Ok(heads),
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

async fn publish_branch(
    catalog: &dyn Catalog,
    txn_table: &TxnTable,
) -> sqe_core::Result<Option<(TableIdent, i64)>> {
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
        let head = branch_head(&table, branch).map(|id| (ident.clone(), id));
        match tx.commit(catalog).await {
            Ok(_) => return Ok(head),
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
/// Returns how many branches could not be dropped.
pub async fn discard(txn: &TxnScope) -> usize {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let Some(catalog) = txn.catalog() else {
        return 0;
    };
    let mut failed = 0;
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
            failed += 1;
        }
    }
    failed
}

/// Transaction branches of `metadata` whose transaction has certainly
/// ended: the branch head is a snapshot the transaction wrote, and it is
/// older than [`MAX_LIFETIME`] plus a margin at `now_ms`. The transaction
/// wrote that snapshot after it opened, so it is older than the lifetime
/// too, and its coordinator rolled it back or lost it in a crash.
///
/// A branch still at its base snapshot is left alone: its head carries
/// `main`'s old timestamp, not the transaction's, and it stages no file.
/// `ALTER TABLE ... DROP BRANCH` removes one by hand.
///
/// Dropping a branch a live transaction still used would not lose data:
/// its next statement reads a stale pin and its COMMIT finds no branch,
/// both of which abort it.
pub fn stale_txn_refs(metadata: &iceberg::spec::TableMetadata, now_ms: i64) -> Vec<String> {
    let private = txn_private_snapshots(metadata);
    let cutoff_ms = now_ms
        .saturating_sub(MAX_LIFETIME.as_millis() as i64)
        .saturating_sub(STALE_MARGIN_MS);
    let mut stale: Vec<String> = metadata
        .refs_iter()
        .filter(|(name, _)| is_txn_ref(name))
        .filter(|(_, r)| private.contains(&r.snapshot_id))
        .filter(|(_, r)| {
            metadata
                .snapshot_by_id(r.snapshot_id)
                .is_some_and(|s| s.timestamp_ms() < cutoff_ms)
        })
        .map(|(name, _)| name.to_string())
        .collect();
    stale.sort();
    stale
}

/// Drop the [`stale_txn_refs`] of `ident` in one commit and return their
/// names. Their snapshots then expire like any other unreferenced
/// snapshot, and the files only they reached become orphans.
pub async fn drop_stale_branches(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    now_ms: i64,
) -> sqe_core::Result<Vec<String>> {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let table = catalog
        .load_table(ident)
        .await
        .map_err(|e| sqe_core::SqeError::catalog_src(format!("Failed to load table {ident}"), e))?;
    let stale = stale_txn_refs(table.metadata(), now_ms);
    if stale.is_empty() {
        return Ok(stale);
    }
    let mut tx = Transaction::new(&table);
    for branch in &stale {
        tx = tx
            .drop_branch(branch.clone())
            .if_exists(true)
            .apply(tx)
            .map_err(|e| {
                sqe_core::SqeError::catalog_src(format!("Failed to drop {branch} on {ident}"), e)
            })?;
    }
    tx.commit(catalog).await.map_err(|e| {
        sqe_core::SqeError::catalog_src(
            format!("Failed to drop stale transaction branches on {ident}"),
            e,
        )
    })?;
    tracing::info!(table = %ident, branches = ?stale, "dropped stale transaction branches");
    Ok(stale)
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
    /// `timestamp-ms` of HEAD in the fixture.
    const HEAD_MS: i64 = 1555100955770;

    #[test]
    fn only_old_branches_holding_a_private_head_are_stale() {
        let lifetime = MAX_LIFETIME.as_millis() as i64;
        let after_lifetime = HEAD_MS + lifetime + STALE_MARGIN_MS + 1;
        let open = metadata_with_refs(&[("main", BASE), ("sqe-txn-1", HEAD), ("dev", BASE)]);
        assert_eq!(stale_txn_refs(&open, after_lifetime), ["sqe-txn-1"]);
        // Within the lifetime plus margin the transaction may still be open.
        assert!(stale_txn_refs(&open, HEAD_MS + lifetime).is_empty());

        // A branch still at its base carries main's timestamp, not the
        // transaction's: never stale.
        let at_base = metadata_with_refs(&[("main", HEAD), ("sqe-txn-1", BASE)]);
        assert!(stale_txn_refs(&at_base, i64::MAX).is_empty());
        // A user branch on the head makes it public: not the transaction's.
        let shared = metadata_with_refs(&[("main", BASE), ("sqe-txn-1", HEAD), ("dev", HEAD)]);
        assert!(stale_txn_refs(&shared, i64::MAX).is_empty());
    }

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
