//! `SqliteRsCatalog`: the SQLite-backed Iceberg catalog over the pure-Rust
//! [`sqlite_rs`] engine instead of `sqlx` + `libsqlite3`.
//!
//! Same two-table schema as the vendored `iceberg-catalog-sql` crate
//! (`iceberg_tables`, `iceberg_namespace_properties`), same SQL statement for
//! statement, so an `sqe.db` written by either implementation opens under the
//! other. What differs is underneath:
//!
//! - **No pool.** `sqlite_rs::api::Connection` is `Send + Sync` over one
//!   worker thread and serialises statements. Lab271 measured that two
//!   connections to the same file in one process do not lock against each
//!   other (POSIX `fcntl` locks are per process), and a commit from one can
//!   silently drop the other's rows. This module therefore keeps exactly one
//!   `Connection` per canonical path, process-wide, and every catalog opened
//!   on that path shares it. The sqlx path's default pool of ten is the bug
//!   this rule exists to avoid.
//! - **No `CREATE TABLE IF NOT EXISTS`.** sqlite-rs #697: the guard is ignored
//!   and the second run corrupts the file. The schema step reads the table
//!   list through the facade and only creates what is absent.
//! - **Named unique indexes, not a declared composite `PRIMARY KEY`.**
//!   sqlite-rs #687: a table created *by sqlite-rs* with a composite PK gets no
//!   `sqlite_autoindex_*`, and stock `sqlite3` then reports the file malformed
//!   on the first write. A fresh file gets `CREATE UNIQUE INDEX` with the same
//!   key columns, which both engines maintain. A file the sqlx path created
//!   keeps its autoindexes; sqlite-rs #685 (merged) recovers and maintains
//!   those, so adoption is a reopen, not a migration.
//! - **Busy is retryable.** `Error::Busy` maps to an Iceberg error with
//!   `retryable = true`, the same shape `CatalogCommitConflicts` carries.
//!
//! Every engine call runs on `tokio::task::spawn_blocking`; the facade blocks
//! on a channel to its worker and must not sit on a runtime thread.
//!
//! ## Engine defects this module codes around (snapshot `e788241`)
//!
//! Found 2026-09-10 by running this catalog's statements through the facade;
//! each is pinned by a test here and reported upstream. Remove the workaround
//! when the corresponding fix lands in the vendored snapshot.
//!
//! 1. **`UPDATE` binds only its first parameter.** `UPDATE t SET v = ? WHERE
//!    k = ?` changes zero rows and reports zero; with one parameter in either
//!    clause it works. Every update here is therefore a `DELETE` + `INSERT`
//!    of the same key inside a `BEGIN IMMEDIATE` transaction
//!    ([`SqliteRsCatalog::replace_row`]), which binds correctly and is
//!    atomic. Rowids are not preserved; nothing reads them.
//! 2. **A covering-index `SELECT` miscounts parameters.** On a table with an
//!    autoindex, `SELECT table_name, table_namespace ... WHERE table_namespace
//!    = ? AND catalog_name = ?` fails with "wants 1 parameter(s) but 2 were
//!    bound"; the same query projecting one non-indexed column as well
//!    compiles. `list_tables` projects `metadata_location` it never reads.
//! 3. **`rowid` is not selectable** (`unknown column "rowid"`), which is why
//!    (1) cannot be a single-parameter `UPDATE ... WHERE rowid = N`.
//! 4. **`sqlite_master` is not queryable**; schema presence goes through
//!    `Connection::table_names`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, MetadataLocation, Namespace, NamespaceIdent, Result, TableCommit,
    TableCreation, TableIdent,
};
use sqlite_rs::api::{Connection, Error as SqliteError, Row, TransactionBehavior, Value};

/// Table holding one row per Iceberg table: the metadata pointer.
pub const TABLES: &str = "iceberg_tables";
/// Table holding one row per namespace property.
pub const NAMESPACE_PROPERTIES: &str = "iceberg_namespace_properties";
/// Name of the unique index a fresh file gets on `iceberg_tables`.
pub const TABLES_KEY_INDEX: &str = "iceberg_tables_pk";
/// Name of the unique index a fresh file gets on
/// `iceberg_namespace_properties`.
pub const NAMESPACE_PROPERTIES_KEY_INDEX: &str = "iceberg_namespace_properties_pk";
/// Namespace property the vendored SQL catalog also honours as the table
/// location root.
const NAMESPACE_LOCATION_PROPERTY_KEY: &str = "location";
const TABLE_RECORD_TYPE: &str = "TABLE";

/// Busy timeout applied to every connection this module opens. sqlx's SQLite
/// driver uses the same default.
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Iceberg catalog over a sqlite-rs `Connection`.
#[derive(Debug, Clone)]
pub struct SqliteRsCatalog {
    name: String,
    db_path: PathBuf,
    conn: Arc<Connection>,
    warehouse_location: String,
    fileio: FileIO,
}

impl SqliteRsCatalog {
    /// Open (creating if absent) the catalog database at `db_path`.
    ///
    /// `name` is the `catalog_name` row scope inside the file, exactly as the
    /// sqlx builder's `load(name, ..)` argument is; the embedded engine passes
    /// a fixed string so each file holds one coherent scope.
    /// `warehouse_location` is where new tables without an explicit location
    /// are created; its scheme selects the `FileIO` (`file://`, `s3://`, ...).
    pub async fn open(
        name: impl Into<String>,
        db_path: impl AsRef<Path>,
        warehouse_location: impl Into<String>,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Catalog name cannot be empty",
            ));
        }
        let warehouse_location = warehouse_location.into();
        let scheme = warehouse_location
            .split("://")
            .next()
            .filter(|s| !s.is_empty() && *s != warehouse_location.as_str())
            .unwrap_or("s3")
            .to_string();
        let fileio = FileIOBuilder::new(scheme).build()?;

        let db_path = canonical_db_path(db_path.as_ref())?;
        let conn = shared_connection(&db_path).await?;
        {
            let conn = Arc::clone(&conn);
            run_blocking(move || ensure_schema(&conn)).await?;
        }

        Ok(Self {
            name,
            db_path,
            conn,
            warehouse_location,
            fileio,
        })
    }

    /// The canonical database path this catalog is bound to.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// True when `other` runs on the same `Connection` (same worker thread).
    /// The process-wide one-connection-per-file rule is observable here.
    pub fn shares_connection_with(&self, other: &SqliteRsCatalog) -> bool {
        Arc::ptr_eq(&self.conn, &other.conn)
    }

    async fn fetch_rows(&self, sql: String, args: Vec<String>) -> Result<Vec<Row>> {
        let conn = Arc::clone(&self.conn);
        run_blocking(move || {
            conn.query_all_with(&sql, params(args))
                .map_err(from_sqlite_error)
        })
        .await
    }

    /// Run one write statement in autocommit; returns rows changed.
    async fn execute(&self, sql: String, args: Vec<String>) -> Result<u64> {
        let conn = Arc::clone(&self.conn);
        run_blocking(move || conn.execute_with(&sql, params(args)).map_err(from_sqlite_error)).await
    }

    /// The `UPDATE` stand-in (module docs, defect 1): inside one
    /// `BEGIN IMMEDIATE` transaction, run `select_sql` with `select_args`,
    /// hand the matching row (or `None`) to `decide`, and if it returns a
    /// replacement, `DELETE` with `delete_sql`/`delete_args` and `INSERT`
    /// the replacement row through `insert_sql`. `decide` returning an error
    /// rolls back and propagates it; returning `Ok(None)` commits nothing.
    ///
    /// `IMMEDIATE` takes the write lock before the read, so between the
    /// `SELECT` and the `INSERT` no other writer (this process or another)
    /// can slip a commit in; the compare-and-swap in `update_table` depends
    /// on that. Returns the number of rows the `DELETE` removed.
    async fn replace_row<D>(
        &self,
        select_sql: String,
        select_args: Vec<String>,
        decide: D,
        delete_sql: String,
        delete_args: Vec<String>,
        insert_sql: String,
    ) -> Result<u64>
    where
        D: FnOnce(Option<&Row>) -> Result<Option<Vec<Value>>> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        run_blocking(move || {
            let tx = conn
                .transaction_with(TransactionBehavior::Immediate)
                .map_err(from_sqlite_error)?;
            let rows = tx
                .query_all_with(&select_sql, params(select_args))
                .map_err(from_sqlite_error)?;
            let Some(replacement) = decide(rows.first())? else {
                tx.rollback().map_err(from_sqlite_error)?;
                return Ok(0);
            };
            let removed = tx
                .execute_with(&delete_sql, params(delete_args))
                .map_err(from_sqlite_error)?;
            tx.execute_with(&insert_sql, replacement)
                .map_err(from_sqlite_error)?;
            tx.commit().map_err(from_sqlite_error)?;
            Ok(removed)
        })
        .await
    }
}

fn params(args: Vec<String>) -> Vec<Value> {
    args.into_iter().map(|a| Value::Text(a.into())).collect()
}

fn text(row: &Row, index: usize) -> Result<String> {
    row.get::<String>(index).map_err(from_sqlite_error)
}

fn nullable_text(row: &Row, index: usize) -> Result<Option<String>> {
    row.get::<Option<String>>(index).map_err(from_sqlite_error)
}

fn text_value(s: impl AsRef<str>) -> Value {
    Value::Text(s.as_ref().into())
}

fn nullable_value(s: Option<String>) -> Value {
    s.map(text_value).unwrap_or(Value::Null)
}

fn from_sqlite_error(error: SqliteError) -> Error {
    let retryable = error.is_retryable();
    Error::new(
        ErrorKind::Unexpected,
        format!(
            "sqlite-rs catalog operation failed (sqlite code {}): {error}",
            error.extended_sqlite_code()
        ),
    )
    .with_retryable(retryable)
    .with_source(error)
}

fn no_such_namespace_err<T>(namespace: &NamespaceIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::Unexpected,
        format!("No such namespace: {namespace:?}"),
    ))
}

fn no_such_table_err<T>(table: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::Unexpected,
        format!("No such table: {table:?}"),
    ))
}

fn table_already_exists_err<T>(table: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::Unexpected,
        format!("Table {table:?} already exists"),
    ))
}

async fn run_blocking<T, F>(f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            format!("sqlite-rs catalog task did not complete: {e}"),
        )
    })?
}

/// Canonicalise a database path whose file may not exist yet: the parent
/// directory must exist and is canonicalised, the file name is kept.
fn canonical_db_path(path: &Path) -> Result<PathBuf> {
    if let Ok(existing) = path.canonicalize() {
        return Ok(existing);
    }
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let file_name = path.file_name().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("sqlite database path has no file name: {}", path.display()),
        )
    })?;
    let parent = parent.canonicalize().map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "sqlite database directory {} is not usable: {e}",
                parent.display()
            ),
        )
    })?;
    Ok(parent.join(file_name))
}

/// Process-wide registry: one live `Connection` per canonical path.
///
/// Holds `Weak` so the worker thread stops when the last catalog on that
/// path drops, and a later open starts a fresh one.
fn registry() -> &'static Mutex<HashMap<PathBuf, Weak<Connection>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<Connection>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn shared_connection(db_path: &Path) -> Result<Arc<Connection>> {
    // Fast path: a live connection already serves this file.
    if let Some(existing) = lock_registry().get(db_path).and_then(Weak::upgrade) {
        return Ok(existing);
    }
    // Open off the runtime: spawning the worker and opening the file block.
    let path = db_path.to_path_buf();
    let opened = run_blocking(move || {
        let conn = Connection::open(&path).map_err(from_sqlite_error)?;
        conn.set_busy_timeout(DEFAULT_BUSY_TIMEOUT)
            .map_err(from_sqlite_error)?;
        Ok(Arc::new(conn))
    })
    .await?;
    // Re-check under the lock: another task may have opened the same file
    // while we were off-thread. Prefer theirs and let ours drop, so the
    // one-connection rule holds even across a race.
    let mut reg = lock_registry();
    if let Some(existing) = reg.get(db_path).and_then(Weak::upgrade) {
        return Ok(existing);
    }
    reg.retain(|_, weak| weak.strong_count() > 0);
    reg.insert(db_path.to_path_buf(), Arc::downgrade(&opened));
    Ok(opened)
}

fn lock_registry() -> std::sync::MutexGuard<'static, HashMap<PathBuf, Weak<Connection>>> {
    // A poisoned registry only means a panic while holding the map; the
    // map itself holds nothing that can be half-written.
    registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Create whichever of the two tables is absent, each with its unique index.
///
/// Deliberately not `CREATE ... IF NOT EXISTS` (sqlite-rs #697) and
/// deliberately no declared composite `PRIMARY KEY` (sqlite-rs #687); see the
/// module docs. Presence is read through `Connection::table_names` because
/// the engine does not expose `sqlite_master` as a queryable table. A table
/// that already exists is left exactly as found, index and all: it carries
/// either the autoindex the sqlx path's PRIMARY KEY left behind or the named
/// index an earlier run of this function created. Table and index are
/// created in one transaction so a crash between them cannot leave a keyless
/// table for the next open to trust.
fn ensure_schema(conn: &Connection) -> Result<()> {
    let existing: HashSet<String> = conn
        .table_names()
        .map_err(from_sqlite_error)?
        .into_iter()
        .collect();

    if !existing.contains(TABLES) {
        create_keyed_table(
            conn,
            &format!(
                "CREATE TABLE {TABLES} (
                    catalog_name TEXT NOT NULL,
                    table_namespace TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    metadata_location TEXT,
                    previous_metadata_location TEXT,
                    iceberg_type TEXT)"
            ),
            &format!(
                "CREATE UNIQUE INDEX {TABLES_KEY_INDEX} ON {TABLES} (catalog_name, table_namespace, table_name)"
            ),
        )?;
    }
    if !existing.contains(NAMESPACE_PROPERTIES) {
        create_keyed_table(
            conn,
            &format!(
                "CREATE TABLE {NAMESPACE_PROPERTIES} (
                    catalog_name TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    property_key TEXT,
                    property_value TEXT)"
            ),
            &format!(
                "CREATE UNIQUE INDEX {NAMESPACE_PROPERTIES_KEY_INDEX} ON {NAMESPACE_PROPERTIES} (catalog_name, namespace, property_key)"
            ),
        )?;
    }
    Ok(())
}

fn create_keyed_table(conn: &Connection, create_table: &str, create_index: &str) -> Result<()> {
    let tx = conn.transaction().map_err(from_sqlite_error)?;
    tx.execute(create_table).map_err(from_sqlite_error)?;
    tx.execute(create_index).map_err(from_sqlite_error)?;
    tx.commit().map_err(from_sqlite_error)
}

#[async_trait]
impl Catalog for SqliteRsCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        // The sqlx path runs one UNION; two selects and a set are the same
        // result and keep each statement to a single-table shape.
        let mut rows = self
            .fetch_rows(
                format!("SELECT table_namespace FROM {TABLES} WHERE catalog_name = ?"),
                vec![self.name.clone()],
            )
            .await?;
        rows.extend(
            self.fetch_rows(
                format!("SELECT namespace FROM {NAMESPACE_PROPERTIES} WHERE catalog_name = ?"),
                vec![self.name.clone()],
            )
            .await?,
        );

        let mut namespaces = HashSet::<NamespaceIdent>::with_capacity(rows.len());
        if let Some(parent) = parent {
            if !self.namespace_exists(parent).await? {
                return no_such_namespace_err(parent);
            }
            let parent_str = parent.join(".");
            for row in &rows {
                let nsp = text(row, 0)?;
                // parent = a: only a.b, a.c come back, never a itself.
                if nsp != parent_str && nsp.starts_with(&parent_str) {
                    namespaces.insert(NamespaceIdent::from_strs(nsp.split('.'))?);
                }
            }
        } else {
            for row in &rows {
                let nsp = text(row, 0)?;
                if let Some(first) = nsp.split('.').next() {
                    namespaces.insert(NamespaceIdent::from_strs([first])?);
                }
            }
        }
        Ok(namespaces.into_iter().collect())
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        if self.namespace_exists(namespace).await? {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Namespace {namespace:?} already exists"),
            ));
        }
        let namespace_str = namespace.join(".");
        let insert = format!(
            "INSERT INTO {NAMESPACE_PROPERTIES} (catalog_name, namespace, property_key, property_value)
             VALUES (?, ?, ?, ?)"
        );

        if properties.is_empty() {
            self.execute(
                insert,
                vec![
                    self.name.clone(),
                    namespace_str,
                    "exists".to_string(),
                    "true".to_string(),
                ],
            )
            .await?;
            return Ok(Namespace::with_properties(namespace.clone(), properties));
        }

        let mut insert_properties = properties;
        insert_properties.insert("exists".to_string(), "true".to_string());
        // One statement per property, inside one transaction: sqlite-rs
        // rejects multi-row VALUES lists less predictably than one row at a
        // time, and a transaction keeps the namespace all-or-nothing.
        let conn = Arc::clone(&self.conn);
        let name = self.name.clone();
        let rows: Vec<(String, String)> = insert_properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        run_blocking(move || {
            let tx = conn.transaction().map_err(from_sqlite_error)?;
            for (key, value) in rows {
                tx.execute_with(
                    &insert,
                    params(vec![name.clone(), namespace_str.clone(), key, value]),
                )
                .map_err(from_sqlite_error)?;
            }
            tx.commit().map_err(from_sqlite_error)
        })
        .await?;
        Ok(Namespace::with_properties(
            namespace.clone(),
            insert_properties,
        ))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        if !self.namespace_exists(namespace).await? {
            return no_such_namespace_err(namespace);
        }
        let rows = self
            .fetch_rows(
                format!(
                    "SELECT namespace, property_key, property_value
                     FROM {NAMESPACE_PROPERTIES}
                     WHERE catalog_name = ? AND namespace = ?"
                ),
                vec![self.name.clone(), namespace.join(".")],
            )
            .await?;
        let mut properties = HashMap::with_capacity(rows.len());
        for row in &rows {
            properties.insert(text(row, 1)?, text(row, 2)?);
        }
        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let namespace_str = namespace.join(".");
        let in_tables = self
            .fetch_rows(
                format!(
                    "SELECT 1 FROM {TABLES}
                     WHERE catalog_name = ? AND table_namespace = ? LIMIT 1"
                ),
                vec![self.name.clone(), namespace_str.clone()],
            )
            .await?;
        if !in_tables.is_empty() {
            return Ok(true);
        }
        let in_props = self
            .fetch_rows(
                format!(
                    "SELECT 1 FROM {NAMESPACE_PROPERTIES}
                     WHERE catalog_name = ? AND namespace = ? LIMIT 1"
                ),
                vec![self.name.clone(), namespace_str],
            )
            .await?;
        Ok(!in_props.is_empty())
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        if !self.namespace_exists(namespace).await? {
            return no_such_namespace_err(namespace);
        }
        let existing = self.get_namespace(namespace).await?.properties().clone();
        let namespace_str = namespace.join(".");

        let mut updates = Vec::new();
        let mut inserts = Vec::new();
        for (key, value) in properties {
            match existing.get(&key) {
                Some(current) if current == &value => {}
                Some(_) => updates.push((key, value)),
                None => inserts.push((key, value)),
            }
        }
        if updates.is_empty() && inserts.is_empty() {
            return Ok(());
        }

        // An UPDATE with several bound parameters is a no-op on this engine
        // (module docs, defect 1): a changed property is deleted and
        // re-inserted, in the same transaction as the new ones.
        let delete_stmt = format!(
            "DELETE FROM {NAMESPACE_PROPERTIES}
             WHERE catalog_name = ? AND namespace = ? AND property_key = ?"
        );
        let insert_stmt = format!(
            "INSERT INTO {NAMESPACE_PROPERTIES} (catalog_name, namespace, property_key, property_value)
             VALUES (?, ?, ?, ?)"
        );
        let conn = Arc::clone(&self.conn);
        let name = self.name.clone();
        run_blocking(move || {
            let tx = conn
                .transaction_with(TransactionBehavior::Immediate)
                .map_err(from_sqlite_error)?;
            for (key, value) in updates {
                tx.execute_with(
                    &delete_stmt,
                    params(vec![name.clone(), namespace_str.clone(), key.clone()]),
                )
                .map_err(from_sqlite_error)?;
                tx.execute_with(
                    &insert_stmt,
                    params(vec![name.clone(), namespace_str.clone(), key, value]),
                )
                .map_err(from_sqlite_error)?;
            }
            for (key, value) in inserts {
                tx.execute_with(
                    &insert_stmt,
                    params(vec![name.clone(), namespace_str.clone(), key, value]),
                )
                .map_err(from_sqlite_error)?;
            }
            tx.commit().map_err(from_sqlite_error)
        })
        .await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        if !self.namespace_exists(namespace).await? {
            return no_such_namespace_err(namespace);
        }
        let tables = self.list_tables(namespace).await?;
        if !tables.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Namespace {:?} is not empty. {} tables exist.",
                    namespace,
                    tables.len()
                ),
            ));
        }
        self.execute(
            format!(
                "DELETE FROM {NAMESPACE_PROPERTIES}
                 WHERE namespace = ? AND catalog_name = ?"
            ),
            vec![namespace.join("."), self.name.clone()],
        )
        .await?;
        Ok(())
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        if !self.namespace_exists(namespace).await? {
            return no_such_namespace_err(namespace);
        }
        // `metadata_location` is projected and never read: with only index
        // columns in the projection the engine miscounts the parameters on
        // an autoindexed table (module docs, defect 2).
        let rows = self
            .fetch_rows(
                format!(
                    "SELECT table_name, table_namespace, metadata_location FROM {TABLES}
                     WHERE table_namespace = ? AND catalog_name = ?
                       AND (iceberg_type = '{TABLE_RECORD_TYPE}' OR iceberg_type IS NULL)"
                ),
                vec![namespace.join("."), self.name.clone()],
            )
            .await?;
        let mut tables = HashSet::<TableIdent>::with_capacity(rows.len());
        for row in &rows {
            let tbl = text(row, 0)?;
            let ns = NamespaceIdent::from_strs(text(row, 1)?.split('.'))?;
            tables.insert(TableIdent::new(ns, tbl));
        }
        Ok(tables.into_iter().collect())
    }

    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        let rows = self
            .fetch_rows(
                format!(
                    "SELECT 1 FROM {TABLES}
                     WHERE table_namespace = ? AND catalog_name = ? AND table_name = ?
                       AND (iceberg_type = '{TABLE_RECORD_TYPE}' OR iceberg_type IS NULL)"
                ),
                vec![
                    identifier.namespace().join("."),
                    self.name.clone(),
                    identifier.name().to_string(),
                ],
            )
            .await?;
        Ok(!rows.is_empty())
    }

    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        if !self.table_exists(identifier).await? {
            return no_such_table_err(identifier);
        }
        self.execute(
            format!(
                "DELETE FROM {TABLES}
                 WHERE catalog_name = ? AND table_name = ? AND table_namespace = ?
                   AND (iceberg_type = '{TABLE_RECORD_TYPE}' OR iceberg_type IS NULL)"
            ),
            vec![
                self.name.clone(),
                identifier.name().to_string(),
                identifier.namespace().join("."),
            ],
        )
        .await?;
        Ok(())
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        let rows = self
            .fetch_rows(
                format!(
                    "SELECT metadata_location FROM {TABLES}
                     WHERE catalog_name = ? AND table_name = ? AND table_namespace = ?
                       AND (iceberg_type = '{TABLE_RECORD_TYPE}' OR iceberg_type IS NULL)"
                ),
                vec![
                    self.name.clone(),
                    identifier.name().to_string(),
                    identifier.namespace().join("."),
                ],
            )
            .await?;
        let Some(row) = rows.first() else {
            return no_such_table_err(identifier);
        };
        let metadata_location = text(row, 0)?;
        let metadata = TableMetadata::read_from(&self.fileio, &metadata_location).await?;
        Table::builder()
            .file_io(self.fileio.clone())
            .identifier(identifier.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .build()
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        if !self.namespace_exists(namespace).await? {
            return no_such_namespace_err(namespace);
        }
        let tbl_name = creation.name.clone();
        let tbl_ident = TableIdent::new(namespace.clone(), tbl_name.clone());
        if self.table_exists(&tbl_ident).await? {
            return table_already_exists_err(&tbl_ident);
        }

        let (tbl_creation, location) = match creation.location.clone() {
            Some(location) => (creation, location),
            None => {
                // Namespace `location` property first, then the warehouse.
                let nsp_properties = self.get_namespace(namespace).await?.properties().clone();
                let nsp_location = match nsp_properties.get(NAMESPACE_LOCATION_PROPERTY_KEY) {
                    Some(location) => location.clone(),
                    None => format!("{}/{}", self.warehouse_location, namespace.join("/")),
                };
                let tbl_location = format!("{}/{}", nsp_location, tbl_ident.name());
                (
                    TableCreation {
                        location: Some(tbl_location.clone()),
                        ..creation
                    },
                    tbl_location,
                )
            }
        };

        let tbl_metadata = TableMetadataBuilder::from_table_creation(tbl_creation)?
            .build()?
            .metadata;
        let tbl_metadata_location =
            MetadataLocation::new_with_table_location(location).to_string();
        tbl_metadata
            .write_to(&self.fileio, &tbl_metadata_location)
            .await?;

        self.execute(
            format!(
                "INSERT INTO {TABLES}
                 (catalog_name, table_namespace, table_name, metadata_location, iceberg_type)
                 VALUES (?, ?, ?, ?, ?)"
            ),
            vec![
                self.name.clone(),
                namespace.join("."),
                tbl_name,
                tbl_metadata_location.clone(),
                TABLE_RECORD_TYPE.to_string(),
            ],
        )
        .await?;

        Table::builder()
            .file_io(self.fileio.clone())
            .metadata_location(tbl_metadata_location)
            .identifier(tbl_ident)
            .metadata(tbl_metadata)
            .build()
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        if src == dest {
            return Ok(());
        }
        if !self.table_exists(src).await? {
            return no_such_table_err(src);
        }
        if !self.namespace_exists(dest.namespace()).await? {
            return no_such_namespace_err(dest.namespace());
        }
        if self.table_exists(dest).await? {
            return table_already_exists_err(dest);
        }
        // Delete + insert stands in for UPDATE (module docs, defect 1); the
        // row's pointer columns are carried over unchanged.
        let key_where = format!(
            "WHERE catalog_name = ? AND table_name = ? AND table_namespace = ?
               AND (iceberg_type = '{TABLE_RECORD_TYPE}' OR iceberg_type IS NULL)"
        );
        let key_args = vec![
            self.name.clone(),
            src.name().to_string(),
            src.namespace().join("."),
        ];
        let (name, dest_ns, dest_name, src_for_err) = (
            self.name.clone(),
            dest.namespace().join("."),
            dest.name().to_string(),
            src.clone(),
        );
        let removed = self
            .replace_row(
                format!(
                    "SELECT metadata_location, previous_metadata_location, iceberg_type
                     FROM {TABLES} {key_where}"
                ),
                key_args.clone(),
                move |row| {
                    let Some(row) = row else {
                        return no_such_table_err(&src_for_err);
                    };
                    Ok(Some(vec![
                        text_value(&name),
                        text_value(&dest_ns),
                        text_value(&dest_name),
                        nullable_value(nullable_text(row, 0)?),
                        nullable_value(nullable_text(row, 1)?),
                        nullable_value(nullable_text(row, 2)?),
                    ]))
                },
                format!("DELETE FROM {TABLES} {key_where}"),
                key_args,
                format!(
                    "INSERT INTO {TABLES}
                     (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location, iceberg_type)
                     VALUES (?, ?, ?, ?, ?, ?)"
                ),
            )
            .await?;
        if removed == 0 {
            return no_such_table_err(src);
        }
        Ok(())
    }

    async fn register_table(
        &self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        if self.table_exists(table_ident).await? {
            return table_already_exists_err(table_ident);
        }
        let metadata = TableMetadata::read_from(&self.fileio, &metadata_location).await?;
        self.execute(
            format!(
                "INSERT INTO {TABLES}
                 (catalog_name, table_namespace, table_name, metadata_location, iceberg_type)
                 VALUES (?, ?, ?, ?, ?)"
            ),
            vec![
                self.name.clone(),
                table_ident.namespace().join("."),
                table_ident.name().to_string(),
                metadata_location.clone(),
                TABLE_RECORD_TYPE.to_string(),
            ],
        )
        .await?;
        Table::builder()
            .identifier(table_ident.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .file_io(self.fileio.clone())
            .build()
    }

    /// Compare-and-swap on `metadata_location`: zero rows changed means
    /// another writer committed first, reported as a retryable conflict.
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let table_ident = commit.identifier().clone();
        let current_table = self.load_table(&table_ident).await?;
        let current_metadata_location = current_table.metadata_location_result()?.to_string();

        let staged_table = commit.apply(current_table)?;
        let staged_metadata_location = staged_table.metadata_location_result()?.to_string();
        staged_table
            .metadata()
            .write_to(staged_table.file_io(), &staged_metadata_location)
            .await?;

        // The sqlx path is one conditional UPDATE (`... AND metadata_location
        // = ?`) whose zero-rows result is the conflict signal. Here the same
        // compare-and-swap is a read under BEGIN IMMEDIATE followed by
        // delete + insert (module docs, defect 1); `decide` is where the
        // pointer comparison happens, with the write lock already held.
        let key_where = format!(
            "WHERE catalog_name = ? AND table_name = ? AND table_namespace = ?
               AND (iceberg_type = '{TABLE_RECORD_TYPE}' OR iceberg_type IS NULL)"
        );
        let key_args = vec![
            self.name.clone(),
            table_ident.name().to_string(),
            table_ident.namespace().join("."),
        ];
        let conflict = || {
            Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!("Commit conflicted for table: {table_ident}"),
            )
            .with_retryable(true)
        };
        let (name, ns, tbl, expected, staged) = (
            self.name.clone(),
            table_ident.namespace().join("."),
            table_ident.name().to_string(),
            current_metadata_location.clone(),
            staged_metadata_location,
        );
        let ident_for_err = table_ident.clone();
        let removed = self
            .replace_row(
                format!("SELECT metadata_location, iceberg_type FROM {TABLES} {key_where}"),
                key_args.clone(),
                move |row| {
                    let Some(row) = row else {
                        return no_such_table_err(&ident_for_err);
                    };
                    if nullable_text(row, 0)?.as_deref() != Some(expected.as_str()) {
                        return Err(Error::new(
                            ErrorKind::CatalogCommitConflicts,
                            format!("Commit conflicted for table: {ident_for_err}"),
                        )
                        .with_retryable(true));
                    }
                    let record_type = nullable_text(row, 1)?
                        .unwrap_or_else(|| TABLE_RECORD_TYPE.to_string());
                    Ok(Some(vec![
                        text_value(&name),
                        text_value(&ns),
                        text_value(&tbl),
                        text_value(&staged),
                        text_value(&expected),
                        text_value(record_type),
                    ]))
                },
                format!("DELETE FROM {TABLES} {key_where}"),
                key_args,
                format!(
                    "INSERT INTO {TABLES}
                     (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location, iceberg_type)
                     VALUES (?, ?, ?, ?, ?, ?)"
                ),
            )
            .await?;
        if removed == 0 {
            return Err(conflict());
        }
        Ok(staged_table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fresh() -> (tempfile::TempDir, SqliteRsCatalog) {
        let dir = tempfile::TempDir::new().unwrap();
        let wh = format!("file://{}", dir.path().join("iceberg").display());
        let catalog = SqliteRsCatalog::open("t", dir.path().join("sqe.db"), wh)
            .await
            .unwrap();
        (dir, catalog)
    }

    fn key_where() -> String {
        format!(
            "WHERE catalog_name = ? AND table_name = ? AND table_namespace = ?
               AND (iceberg_type = '{TABLE_RECORD_TYPE}' OR iceberg_type IS NULL)"
        )
    }

    fn insert_sql() -> String {
        format!(
            "INSERT INTO {TABLES}
             (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location, iceberg_type)
             VALUES (?, ?, ?, ?, ?, ?)"
        )
    }

    /// The compare-and-swap that `update_table` relies on: when the stored
    /// pointer is not the one the caller loaded, `decide` refuses, nothing
    /// is deleted or inserted, and the error is the retryable conflict.
    #[tokio::test]
    async fn replace_row_refuses_when_the_stored_pointer_moved() {
        let (_dir, catalog) = fresh().await;
        catalog
            .execute(
                insert_sql(),
                vec![
                    "t".into(),
                    "sales".into(),
                    "orders".into(),
                    "file:///v1.json".into(),
                    String::new(),
                    TABLE_RECORD_TYPE.into(),
                ],
            )
            .await
            .unwrap();
        let key = vec!["t".to_string(), "orders".to_string(), "sales".to_string()];

        // A caller that loaded v0 (stale) must be refused.
        let err = catalog
            .replace_row(
                format!("SELECT metadata_location FROM {TABLES} {}", key_where()),
                key.clone(),
                |row| {
                    let stored = nullable_text(row.unwrap(), 0)?;
                    if stored.as_deref() != Some("file:///v0.json") {
                        return Err(Error::new(
                            ErrorKind::CatalogCommitConflicts,
                            "moved",
                        )
                        .with_retryable(true));
                    }
                    Ok(Some(Vec::new()))
                },
                format!("DELETE FROM {TABLES} {}", key_where()),
                key.clone(),
                insert_sql(),
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
        assert!(err.retryable());

        // Row untouched by the refused attempt.
        let rows = catalog
            .fetch_rows(
                format!("SELECT metadata_location FROM {TABLES} {}", key_where()),
                key.clone(),
            )
            .await
            .unwrap();
        assert_eq!(text(&rows[0], 0).unwrap(), "file:///v1.json");

        // A caller that loaded v1 swaps it, and the previous pointer is kept.
        let removed = catalog
            .replace_row(
                format!("SELECT metadata_location FROM {TABLES} {}", key_where()),
                key.clone(),
                |row| {
                    let stored = nullable_text(row.unwrap(), 0)?;
                    assert_eq!(stored.as_deref(), Some("file:///v1.json"));
                    Ok(Some(vec![
                        text_value("t"),
                        text_value("sales"),
                        text_value("orders"),
                        text_value("file:///v2.json"),
                        text_value("file:///v1.json"),
                        text_value(TABLE_RECORD_TYPE),
                    ]))
                },
                format!("DELETE FROM {TABLES} {}", key_where()),
                key.clone(),
                insert_sql(),
            )
            .await
            .unwrap();
        assert_eq!(removed, 1);
        let rows = catalog
            .fetch_rows(
                format!(
                    "SELECT metadata_location, previous_metadata_location FROM {TABLES} {}",
                    key_where()
                ),
                key,
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0], 0).unwrap(), "file:///v2.json");
        assert_eq!(text(&rows[0], 1).unwrap(), "file:///v1.json");
    }

    /// Pins engine defect 1 from the module docs so the workaround can be
    /// removed the day this starts passing: an UPDATE with two bound
    /// parameters must change one row, and today changes none.
    #[tokio::test]
    async fn engine_defect_update_binds_only_its_first_parameter() {
        let (_dir, catalog) = fresh().await;
        catalog
            .execute(
                insert_sql(),
                vec![
                    "t".into(),
                    "sales".into(),
                    "orders".into(),
                    "file:///v1.json".into(),
                    String::new(),
                    TABLE_RECORD_TYPE.into(),
                ],
            )
            .await
            .unwrap();
        let changed = catalog
            .execute(
                format!("UPDATE {TABLES} SET metadata_location = ? WHERE table_name = ?"),
                vec!["file:///v2.json".into(), "orders".into()],
            )
            .await
            .unwrap();
        assert_eq!(
            changed, 0,
            "sqlite-rs now binds every UPDATE parameter: drop replace_row and use UPDATE again"
        );
    }
}
