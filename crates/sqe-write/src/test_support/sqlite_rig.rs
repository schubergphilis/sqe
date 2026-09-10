//! In-process Iceberg write rig: a sqlite catalog over a tempdir warehouse,
//! reachable both as a bare `Arc<dyn Catalog>` and as the REAL
//! [`SessionCatalog`] the coordinator's write paths use (architecture review
//! 513).
//!
//! Before this module the sqlite rig lived three times over inside
//! `maintenance.rs`'s test modules and only maintenance could use it. Every
//! committing write path (`WriteHandler::handle_insert_streaming`,
//! `handle_ctas_streaming`, `handle_delete*`, `handle_update*`,
//! `handle_merge*`, `commit_written_files`, `commit_with_retry`,
//! `commit_rewrite_with_retry`) was covered only by `#[ignore]` docker e2e
//! tests that no CI job runs. This rig lets those paths commit against a real
//! Iceberg catalog with real Parquet on the local filesystem under plain
//! `cargo test`.
//!
//! # How the `SessionCatalog` is built
//!
//! `SessionCatalog` has no public constructor over an arbitrary
//! `Arc<dyn Catalog>` (its `for_test_with_backend` is `cfg(test)` inside
//! `sqe-catalog`). The one non-REST route that reaches a sqlite file is the
//! configured backend: [`Self::config`] carries
//! `CatalogBackend::Jdbc { url: "sqlite://<dir>/sqe.db?mode=rwc", warehouse:
//! "file://<dir>/iceberg" }`, so `SessionCatalog::for_session(&config, ..)`,
//! the exact call `WriteHandler::resolve_session_catalog` makes, yields a
//! session catalog over the same sqlite database this rig's raw
//! [`Self::catalog`] talks to. The raw handle is built through
//! `SessionCatalog::build_backend_catalog` too, so both sides share the SQL
//! catalog's row scope (`catalog_name`), which is what makes a table created
//! through one visible through the other. No fake session catalog is needed:
//! `WriteHandler::new(rig.config.clone())` is a production handler pointed at
//! the rig.
//!
//! The `sqlite:` Jdbc arm is served by the pure-Rust `sqe-catalog/sql-sqlite-rs`
//! engine, which the `test-sqlite` feature of this crate enables (see
//! `Cargo.toml`); no sqlx and no libsqlite3 are involved.
//!
//! # Running
//!
//! ```text
//! RUST_MIN_STACK=33554432 cargo test -p sqe-write --features test-sqlite -- write_handler::
//! RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --features test-sqlite -- maintenance::
//! ```
//!
//! CI's `aikido-test-rust` job runs `cargo test --workspace --all-features`
//! (with `RUST_MIN_STACK=33554432`), so every test gated on
//! `#[cfg(feature = "test-sqlite")]` runs on every MR without further wiring.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::Schema as ArrowSchema;
use chrono::Utc;
use futures::TryStreamExt;
use iceberg::spec::{DataFile, TableMetadataBuilder, Transform, UnboundPartitionSpec};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use sqe_catalog::SessionCatalog;
use sqe_core::config::CatalogBackend;
use sqe_core::{SecretString, Session, SqeConfig};
use sqlparser::ast::Statement;
use tempfile::TempDir;

use crate::writer::{new_upload_tracker, parse_parquet_compression, write_data_files};

/// Namespace every rig table lives in unless the caller qualifies otherwise.
pub const RIG_NAMESPACE: &str = "default";

/// Bearer the rig's [`Session`] carries. Non-REST backends only fingerprint
/// it for cache keys and logging; it never reaches the catalog.
pub const RIG_BEARER: &str = "rig-bearer-token";

/// One sqlite catalog + one filesystem warehouse, both under a tempdir that
/// lives as long as the rig.
pub struct SqliteRig {
    /// Owns the tempdir. Dropping the rig deletes the database and the
    /// warehouse.
    _dir: TempDir,
    /// The tempdir path.
    pub root: PathBuf,
    /// `sqlite://<root>/sqe.db?mode=rwc`.
    pub db_url: String,
    /// `file://<root>/iceberg`, the location new tables are created under.
    pub warehouse: String,
    /// Bare catalog over the sqlite database, for seeding and inspection.
    pub catalog: Arc<dyn Catalog>,
    /// A coordinator config whose `[catalog]` points at this rig. Hand it to
    /// `WriteHandler::new` / `MaintenanceHandler::new`.
    pub config: SqeConfig,
}

impl SqliteRig {
    /// Create a fresh, empty rig. Also creates the [`RIG_NAMESPACE`]
    /// namespace so callers can create tables right away.
    pub async fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().to_path_buf();
        let data_root = root.join("iceberg");
        std::fs::create_dir_all(&data_root).expect("create warehouse dir");
        let db_url = format!("sqlite://{}?mode=rwc", root.join("sqe.db").display());
        let warehouse = format!("file://{}", data_root.display());

        let backend = CatalogBackend::Jdbc {
            url: db_url.clone(),
            warehouse: warehouse.clone(),
        };
        let catalog = SessionCatalog::build_backend_catalog(&backend)
            .await
            .expect("sqlite (jdbc) catalog builds; needs sqe-catalog/sql-sqlite-rs");

        let config = rig_config(&db_url, &warehouse);

        let rig = Self {
            _dir: dir,
            root,
            db_url,
            warehouse,
            catalog,
            config,
        };
        rig.ensure_namespace(RIG_NAMESPACE).await;
        rig
    }

    /// A session whose default schema is [`RIG_NAMESPACE`]; the write
    /// handlers resolve unqualified table names against it.
    pub fn session(&self) -> Session {
        let mut session = Session::new(
            "rig".to_string(),
            SecretString::new(RIG_BEARER.to_string()),
            None,
            Utc::now() + chrono::Duration::hours(1),
            vec![],
        );
        session.default_schema = Some(RIG_NAMESPACE.to_string());
        session
    }

    /// The production `SessionCatalog` over this rig, built the same way
    /// `WriteHandler::resolve_session_catalog` builds it.
    pub async fn session_catalog(&self) -> Arc<SessionCatalog> {
        Arc::new(
            SessionCatalog::for_session(&self.config, None, RIG_BEARER)
                .await
                .expect("SessionCatalog::for_session over the rig's jdbc backend"),
        )
    }

    /// `default.<name>`.
    pub fn ident(&self, name: &str) -> TableIdent {
        TableIdent::new(
            NamespaceIdent::new(RIG_NAMESPACE.to_string()),
            name.to_string(),
        )
    }

    /// Create `namespace` if it does not exist yet.
    pub async fn ensure_namespace(&self, namespace: &str) {
        let ns = NamespaceIdent::new(namespace.to_string());
        if !self
            .catalog
            .namespace_exists(&ns)
            .await
            .expect("namespace_exists")
        {
            self.catalog
                .create_namespace(&ns, HashMap::new())
                .await
                .expect("create namespace");
        }
    }

    /// Create an empty table in [`RIG_NAMESPACE`] whose data lives on the
    /// local filesystem under [`Self::warehouse`]. Field ids are assigned
    /// 1..=n in `schema` order, so `spec` (if any) refers to them that way;
    /// see [`identity_spec`].
    pub async fn local_fs_table(
        &self,
        name: &str,
        schema: &ArrowSchema,
        spec: Option<UnboundPartitionSpec>,
    ) -> IcebergTable {
        self.local_fs_table_with_properties(name, schema, spec, HashMap::new())
            .await
    }

    /// [`Self::local_fs_table`] with table properties (for example
    /// `write.delete.mode = merge-on-read`).
    pub async fn local_fs_table_with_properties(
        &self,
        name: &str,
        schema: &ArrowSchema,
        spec: Option<UnboundPartitionSpec>,
        properties: HashMap<String, String>,
    ) -> IcebergTable {
        let iceberg_schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(schema)
            .expect("arrow schema converts to iceberg schema");
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(iceberg_schema)
            .properties(properties)
            .partition_spec_opt(spec)
            .build();
        self.catalog
            .create_table(&NamespaceIdent::new(RIG_NAMESPACE.to_string()), creation)
            .await
            .expect("create table")
    }

    /// Reload `ident` at its current snapshot.
    pub async fn load(&self, ident: &TableIdent) -> IcebergTable {
        self.catalog.load_table(ident).await.expect("load table")
    }

    /// Write `batches` (one data file each) and commit them as a single
    /// `fast_append` against the table's CURRENT snapshot (the table is
    /// reloaded first, so a stale `table` handle is fine). Returns the
    /// committed table.
    pub async fn commit_rows(
        &self,
        table: &IcebergTable,
        batches: Vec<RecordBatch>,
    ) -> IcebergTable {
        append_rows(&self.catalog, table.identifier(), batches).await
    }

    /// Live data files (content = data) of the table's current snapshot,
    /// after a reload.
    pub async fn live_files(&self, ident: &TableIdent) -> Vec<DataFile> {
        let table = self.load(ident).await;
        crate::writer::collect_live_data_files(&table)
            .await
            .expect("collect live data files")
    }

    /// Scan the whole table at its current snapshot through the Iceberg
    /// table scan, which APPLIES position and equality deletes. This is what
    /// a `SELECT *` sees; use it to assert on the logical contents of a
    /// merge-on-read table.
    pub async fn scan_all(&self, ident: &TableIdent) -> Vec<RecordBatch> {
        let table = self.load(ident).await;
        let scan = table.scan().select_all().build().expect("build table scan");
        scan.to_arrow()
            .await
            .expect("scan to arrow")
            .try_collect::<Vec<_>>()
            .await
            .expect("collect scan batches")
    }

    /// Sorted values of the `Int64` column `col` as a `SELECT` would see them.
    pub async fn scan_i64(&self, ident: &TableIdent, col: &str) -> Vec<i64> {
        let mut out = i64_column(&self.scan_all(ident).await, col);
        out.sort_unstable();
        out
    }

    /// Sorted `(id, v)` pairs of a table with an `Int64` `id` and a `Utf8`
    /// `v` column as a `SELECT` would see them.
    pub async fn scan_id_v(&self, ident: &TableIdent) -> Vec<(i64, Option<String>)> {
        let mut out = id_v_pairs(&self.scan_all(ident).await);
        out.sort();
        out
    }

    /// Evolve the table's default partition spec to `spec` (the `ALTER TABLE
    /// ... REPLACE PARTITION FIELD` outcome) while keeping every snapshot.
    ///
    /// The coordinator's own path (`SessionCatalog::commit_schema_update`) is
    /// REST-only, and the vendored `Transaction` has no partition-spec
    /// action, so this writes the evolved metadata file and re-registers the
    /// table at it: the SQL catalog's `drop_table` removes only the catalog
    /// row (no purge) and `register_table` points the row at the new file.
    pub async fn evolve_partition_spec(
        &self,
        ident: &TableIdent,
        spec: UnboundPartitionSpec,
    ) -> IcebergTable {
        let table = self.load(ident).await;
        let current_location = table.metadata_location().map(str::to_string);
        let built = TableMetadataBuilder::new_from_metadata(
            table.metadata().clone(),
            current_location.clone(),
        )
        .add_partition_spec(spec)
        .expect("add partition spec")
        .set_default_partition_spec(-1)
        .expect("set default partition spec (-1 = last added)")
        .build()
        .expect("build evolved metadata");
        // The SQL catalog derives the NEXT metadata file name from the current
        // one (`<version>-<uuid>.metadata.json`, see the vendored
        // `MetadataLocation::with_next_version`), so the evolved file must
        // keep that shape or the following commit fails while parsing the
        // version prefix. `MetadataLocation` itself is not exported.
        let next_version = current_location
            .as_deref()
            .and_then(|loc| loc.rsplit_once('/'))
            .and_then(|(_, file)| file.split_once('-'))
            .and_then(|(version, _)| version.parse::<i32>().ok())
            .map_or(0, |v| v + 1);
        let path = format!(
            "{}/metadata/{:0>5}-{}.metadata.json",
            table.metadata().location().trim_end_matches('/'),
            next_version,
            uuid::Uuid::now_v7()
        );
        let bytes = serde_json::to_vec(&built.metadata).expect("serialize metadata");
        table
            .file_io()
            .new_output(&path)
            .expect("new_output")
            .write(bytes.into())
            .await
            .expect("write evolved metadata");
        self.catalog
            .drop_table(ident)
            .await
            .expect("drop catalog row");
        self.catalog
            .register_table(ident, path)
            .await
            .expect("re-register at evolved metadata")
    }
}

/// Reload `ident` from `catalog`, write `batches` (one data file each) and
/// commit them as one `fast_append`. Free function so a race hook that only
/// holds an `Arc<dyn Catalog>` (no `&SqliteRig`) can land a concurrent
/// append.
pub async fn append_rows(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    batches: Vec<RecordBatch>,
) -> IcebergTable {
    let table = catalog.load_table(ident).await.expect("reload table");
    let compression = parse_parquet_compression("zstd");
    let mut files = Vec::new();
    for batch in batches {
        let written = write_data_files(
            &table,
            vec![batch],
            "rig",
            compression,
            new_upload_tracker(),
        )
        .await
        .expect("write data file");
        files.extend(written);
    }
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(files);
    let tx = action.apply(tx).expect("apply fast_append");
    tx.commit(catalog.as_ref())
        .await
        .expect("commit fast_append")
}

/// Minimal coordinator config whose `[catalog]` is the rig's jdbc/sqlite
/// backend. Built from JSON rather than TOML so the rig has no dependency
/// outside this crate's normal dependency set.
fn rig_config(db_url: &str, warehouse: &str) -> SqeConfig {
    let value = serde_json::json!({
        "coordinator": { "flight_sql_port": 0, "trino_http_port": 0 },
        "auth": {},
        "catalog": {
            "catalog_url": "",
            "warehouse": warehouse,
            "backend": { "type": "jdbc", "url": db_url, "warehouse": warehouse },
        },
    });
    serde_json::from_value(value).expect("rig SqeConfig parses")
}

/// An identity partition spec over the named columns of `schema`. Field ids
/// follow [`SqliteRig::local_fs_table`]'s 1..=n assignment.
pub fn identity_spec(schema: &ArrowSchema, columns: &[&str]) -> UnboundPartitionSpec {
    let mut builder = UnboundPartitionSpec::builder();
    for col in columns {
        let idx = schema
            .index_of(col)
            .unwrap_or_else(|_| panic!("column {col} not in schema"));
        let source_id = i32::try_from(idx + 1).expect("field id fits i32");
        builder = builder
            .add_partition_field(source_id, *col, Transform::Identity)
            .expect("add identity partition field");
    }
    builder.build()
}

/// Parse `sql` through the coordinator's own front door
/// (`sqe_sql::parse_and_classify`: same dialect, same identifier folding)
/// and return the single DDL/DML statement it yields.
pub fn parse_one(sql: &str) -> Statement {
    use sqe_sql::StatementKind;
    match sqe_sql::parse_and_classify(sql).expect("sql parses and classifies") {
        StatementKind::CreateTable(s)
        | StatementKind::Ctas(s)
        | StatementKind::Insert(s)
        | StatementKind::Merge(s)
        | StatementKind::Delete(s)
        | StatementKind::Update(s) => *s,
        other => panic!("not a write statement: {other:?}"),
    }
}

/// The source `SELECT` of an `INSERT`, rendered the way `query_handler` hands
/// it to `WriteHandler::handle_insert_streaming`.
pub fn insert_select_sql(stmt: &Statement) -> String {
    match stmt {
        Statement::Insert(ins) => {
            let q = ins.source.as_ref().expect("INSERT has a SELECT source");
            sqe_sql::rewrite_named_tvf_args(&format!("{q}"))
        }
        other => panic!("expected INSERT, got {other}"),
    }
}

/// The `SELECT` of a `CREATE TABLE ... AS SELECT`, rendered the way
/// `query_handler` hands it to `WriteHandler::handle_ctas_streaming`.
pub fn ctas_select_sql(stmt: &Statement) -> String {
    match stmt {
        Statement::CreateTable(ct) => {
            let q = ct.query.as_ref().expect("CTAS has a query");
            sqe_sql::rewrite_named_tvf_args(&format!("{q}"))
        }
        other => panic!("expected CREATE TABLE AS, got {other}"),
    }
}

/// The value of the one-row `rows_affected` result every DML handler returns.
pub fn rows_affected(batches: &[RecordBatch]) -> i64 {
    assert_eq!(
        batches.len(),
        1,
        "DML returns exactly one batch: {batches:?}"
    );
    let batch = &batches[0];
    let idx = batch
        .schema()
        .index_of("rows_affected")
        .expect("rows_affected column");
    let col = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("rows_affected is Int64");
    assert_eq!(col.len(), 1);
    col.value(0)
}

/// Every value of the `Int64` column `col` across `batches`, in scan order.
pub fn i64_column(batches: &[RecordBatch], col: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let idx = batch.schema().index_of(col).expect("column present");
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("{col} is Int64, got {:?}", batch.column(idx).data_type()));
        out.extend(arr.iter().map(|v| v.expect("non-null")));
    }
    out
}

/// `(id, v)` pairs of batches with an `Int64` `id` and a `Utf8` `v` column.
pub fn id_v_pairs(batches: &[RecordBatch]) -> Vec<(i64, Option<String>)> {
    let mut out = Vec::new();
    for batch in batches {
        let id_idx = batch.schema().index_of("id").expect("id column");
        let v_idx = batch.schema().index_of("v").expect("v column");
        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let vs = batch
            .column(v_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("v is Utf8, got {:?}", batch.column(v_idx).data_type()));
        for i in 0..batch.num_rows() {
            let v = if vs.is_null(i) {
                None
            } else {
                Some(vs.value(i).to_string())
            };
            out.push((ids.value(i), v));
        }
    }
    out
}

/// Schema `(id BIGINT NOT NULL, v VARCHAR)`, the shape most rig tests use.
pub fn id_v_schema() -> Arc<ArrowSchema> {
    use arrow_schema::{DataType, Field};
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
    ]))
}

/// One `(id, v)` batch over [`id_v_schema`].
pub fn id_v_batch(rows: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        id_v_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("build id/v batch")
}
