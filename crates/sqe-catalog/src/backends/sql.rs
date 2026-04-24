//! JDBC-style SQL catalog backend.
//!
//! The Iceberg SQL catalog stores table metadata in a relational database.
//! Two schemas exist in the ecosystem: `iceberg_tables` (columns: catalog_name,
//! table_namespace, table_name, metadata_location, previous_metadata_location)
//! and `iceberg_namespace_properties` (namespace properties). This backend
//! speaks the SQLite variant today; PostgreSQL and MySQL land when the
//! workspace adopts `iceberg-catalog-sql` from apache/iceberg-rust 0.8.0.
//!
//! Placeholder styles differ between drivers (`$N` for PostgreSQL, `?` for
//! MySQL/SQLite). The upstream `iceberg-catalog-sql` crate picks the right
//! one transparently; our SQLite-only implementation always uses `?`.

use rusqlite::{params, Connection, OpenFlags};
use tracing::{debug, instrument};

use sqe_core::{Result as SqeResult, SqeError};

/// A row in the `iceberg_tables` catalog table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlTableEntry {
    pub catalog_name: String,
    pub namespace: String,
    pub name: String,
    pub metadata_location: String,
    pub previous_metadata_location: Option<String>,
}

/// SQLite-backed catalog.
///
/// Each `SqlBackend` owns an open `rusqlite::Connection`. The connection is
/// not `Send`, so callers that need concurrency must wrap the backend in a
/// `Mutex`. Production workloads should prefer the REST catalog until the
/// PostgreSQL path arrives via the upstream crate.
pub struct SqlBackend {
    conn: Connection,
    catalog_name: String,
}

impl std::fmt::Debug for SqlBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlBackend")
            .field("catalog_name", &self.catalog_name)
            .finish()
    }
}

impl SqlBackend {
    /// Open or create a SQLite catalog database.
    ///
    /// If the file does not exist, the standard `iceberg_tables` and
    /// `iceberg_namespace_properties` schemas are initialised. Safe to call
    /// on an existing database; `CREATE TABLE IF NOT EXISTS` is used.
    pub fn open_sqlite(path: &str, catalog_name: &str) -> SqeResult<Self> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
        let conn = Connection::open_with_flags(path, flags)
            .map_err(|e| SqeError::Catalog(format!("open sqlite: {e}")))?;

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS iceberg_tables (
                catalog_name TEXT NOT NULL,
                table_namespace TEXT NOT NULL,
                table_name TEXT NOT NULL,
                metadata_location TEXT NOT NULL,
                previous_metadata_location TEXT,
                PRIMARY KEY (catalog_name, table_namespace, table_name)
            );
            CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
                catalog_name TEXT NOT NULL,
                namespace TEXT NOT NULL,
                property_key TEXT NOT NULL,
                property_value TEXT,
                PRIMARY KEY (catalog_name, namespace, property_key)
            );
            "#,
        )
        .map_err(|e| SqeError::Catalog(format!("init schema: {e}")))?;

        Ok(Self {
            conn,
            catalog_name: catalog_name.to_string(),
        })
    }

    /// List all tables in the given namespace.
    ///
    /// `namespace` is the dotted namespace path (e.g. `"sales.q1"`). Pass an
    /// empty string to match the root namespace (rare in practice but legal).
    #[instrument(skip(self))]
    pub fn list_tables(&self, namespace: &str) -> SqeResult<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT table_name FROM iceberg_tables \
                 WHERE catalog_name = ? AND table_namespace = ? \
                 ORDER BY table_name",
            )
            .map_err(|e| SqeError::Catalog(format!("prepare list_tables: {e}")))?;
        let rows = stmt
            .query_map(params![self.catalog_name, namespace], |row| row.get::<_, String>(0))
            .map_err(|e| SqeError::Catalog(format!("query list_tables: {e}")))?;
        let names: Result<Vec<String>, _> = rows.collect();
        names.map_err(|e| SqeError::Catalog(format!("collect list_tables: {e}")))
    }

    /// List all namespaces in this catalog.
    ///
    /// Namespaces are inferred from the `iceberg_tables.table_namespace` column
    /// (distinct). A real implementation also reads `iceberg_namespace_properties`
    /// for empty namespaces; this shortcut is enough for the integration test.
    pub fn list_namespaces(&self) -> SqeResult<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT table_namespace FROM iceberg_tables \
                 WHERE catalog_name = ? ORDER BY table_namespace",
            )
            .map_err(|e| SqeError::Catalog(format!("prepare list_namespaces: {e}")))?;
        let rows = stmt
            .query_map(params![self.catalog_name], |row| row.get::<_, String>(0))
            .map_err(|e| SqeError::Catalog(format!("query list_namespaces: {e}")))?;
        let names: Result<Vec<String>, _> = rows.collect();
        names.map_err(|e| SqeError::Catalog(format!("collect list_namespaces: {e}")))
    }

    /// Insert a new table entry.
    ///
    /// Real Iceberg SQL catalogs atomically swap `metadata_location` via an
    /// UPDATE guarded by the previous value. This helper is the simplest
    /// write path used by the integration test.
    #[instrument(skip(self))]
    pub fn create_table(
        &self,
        namespace: &str,
        name: &str,
        metadata_location: &str,
    ) -> SqeResult<()> {
        self.conn
            .execute(
                "INSERT INTO iceberg_tables \
                   (catalog_name, table_namespace, table_name, metadata_location) \
                 VALUES (?, ?, ?, ?)",
                params![self.catalog_name, namespace, name, metadata_location],
            )
            .map_err(|e| SqeError::Catalog(format!("insert table: {e}")))?;
        debug!(namespace, name, "SQL backend inserted table");
        Ok(())
    }

    /// Load a table by namespace and name.
    pub fn load_table(&self, namespace: &str, name: &str) -> SqeResult<Option<SqlTableEntry>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT catalog_name, table_namespace, table_name, metadata_location, \
                        previous_metadata_location \
                 FROM iceberg_tables \
                 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?",
            )
            .map_err(|e| SqeError::Catalog(format!("prepare load_table: {e}")))?;
        let mut rows = stmt
            .query(params![self.catalog_name, namespace, name])
            .map_err(|e| SqeError::Catalog(format!("query load_table: {e}")))?;

        if let Some(row) = rows
            .next()
            .map_err(|e| SqeError::Catalog(format!("fetch load_table: {e}")))?
        {
            Ok(Some(SqlTableEntry {
                catalog_name: row.get(0).map_err(|e| SqeError::Catalog(format!("col0: {e}")))?,
                namespace: row.get(1).map_err(|e| SqeError::Catalog(format!("col1: {e}")))?,
                name: row.get(2).map_err(|e| SqeError::Catalog(format!("col2: {e}")))?,
                metadata_location: row.get(3).map_err(|e| SqeError::Catalog(format!("col3: {e}")))?,
                previous_metadata_location: row
                    .get(4)
                    .map_err(|e| SqeError::Catalog(format!("col4: {e}")))?,
            }))
        } else {
            Ok(None)
        }
    }

    /// Drop a table entry by namespace and name.
    ///
    /// Returns `true` if a row was removed. Real Iceberg SQL catalogs also
    /// schedule the metadata file for deletion; SQE defers that to a future
    /// maintenance job.
    pub fn drop_table(&self, namespace: &str, name: &str) -> SqeResult<bool> {
        let n = self
            .conn
            .execute(
                "DELETE FROM iceberg_tables \
                 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?",
                params![self.catalog_name, namespace, name],
            )
            .map_err(|e| SqeError::Catalog(format!("delete table: {e}")))?;
        Ok(n > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_backend() -> SqlBackend {
        let dir = tempdir().unwrap();
        let path = dir.path().join("catalog.db");
        // Leak the tempdir so the file survives the test.
        std::mem::forget(dir);
        SqlBackend::open_sqlite(path.to_str().unwrap(), "sqe").unwrap()
    }

    #[test]
    fn open_and_list_empty_catalog() {
        let backend = make_backend();
        assert!(backend.list_tables("ns").unwrap().is_empty());
        assert!(backend.list_namespaces().unwrap().is_empty());
    }

    #[test]
    fn create_and_list_tables() {
        let backend = make_backend();
        backend
            .create_table("sales", "orders", "s3://lake/ns/orders/metadata/v1.metadata.json")
            .unwrap();
        backend
            .create_table("sales", "customers", "s3://lake/ns/customers/metadata/v1.metadata.json")
            .unwrap();
        let names = backend.list_tables("sales").unwrap();
        assert_eq!(names, vec!["customers", "orders"]);
        let ns = backend.list_namespaces().unwrap();
        assert_eq!(ns, vec!["sales".to_string()]);
    }

    #[test]
    fn load_returns_inserted_row() {
        let backend = make_backend();
        backend
            .create_table("sales", "orders", "s3://lake/ns/orders/metadata/v1.metadata.json")
            .unwrap();
        let row = backend.load_table("sales", "orders").unwrap().unwrap();
        assert_eq!(row.name, "orders");
        assert_eq!(row.namespace, "sales");
        assert_eq!(
            row.metadata_location,
            "s3://lake/ns/orders/metadata/v1.metadata.json"
        );
    }

    #[test]
    fn load_missing_returns_none() {
        let backend = make_backend();
        assert!(backend.load_table("sales", "missing").unwrap().is_none());
    }

    #[test]
    fn drop_removes_row() {
        let backend = make_backend();
        backend
            .create_table("sales", "orders", "s3://lake/ns/orders/metadata/v1.metadata.json")
            .unwrap();
        assert!(backend.drop_table("sales", "orders").unwrap());
        assert!(!backend.drop_table("sales", "orders").unwrap());
    }
}
