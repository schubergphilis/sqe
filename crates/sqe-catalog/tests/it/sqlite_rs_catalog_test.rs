#![cfg(feature = "sql-sqlite-rs")]
//! `SqliteRsCatalog` against a real file on disk, and against files the sqlx
//! path wrote. Stock `sqlite3` is used as an oracle for file validity when it
//! is on `PATH`; when it is not, those assertions are skipped with a note.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use sqe_catalog::sqlite_rs_catalog::{
    SqliteRsCatalog, NAMESPACE_PROPERTIES_KEY_INDEX, TABLES_KEY_INDEX,
};
use tempfile::TempDir;

const NAME: &str = "sqe-embedded";

fn warehouse(dir: &Path) -> String {
    let data = dir.join("iceberg");
    std::fs::create_dir_all(&data).unwrap();
    format!("file://{}", data.display())
}

fn db(dir: &Path) -> PathBuf {
    dir.join("sqe.db")
}

fn simple_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "amount", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()
        .unwrap()
}

fn creation(name: &str) -> TableCreation {
    TableCreation::builder()
        .name(name.to_string())
        .schema(simple_schema())
        .build()
}

/// Run `sqlite3 <db> "<sql>"` and return trimmed stdout, or `None` when the
/// oracle binary is not installed.
fn oracle(db: &Path, sql: &str) -> Option<String> {
    let out = Command::new("sqlite3").arg(db).arg(sql).output().ok()?;
    if !out.status.success() {
        panic!(
            "sqlite3 failed on {}: {}",
            db.display(),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn assert_oracle_ok(db: &Path) {
    match oracle(db, "PRAGMA integrity_check;") {
        Some(v) => assert_eq!(v, "ok", "stock sqlite3 integrity_check on {}", db.display()),
        None => eprintln!("sqlite3 not on PATH; skipping oracle integrity_check"),
    }
}

async fn open(dir: &Path) -> SqliteRsCatalog {
    SqliteRsCatalog::open(NAME, db(dir), warehouse(dir))
        .await
        .unwrap()
}

#[tokio::test]
async fn fresh_file_gets_named_unique_indexes_and_a_valid_file() {
    let dir = TempDir::new().unwrap();
    let catalog = open(dir.path()).await;

    // Second open on the same path must be idempotent: no IF NOT EXISTS, so
    // the schema step has to see what is already there.
    let again = open(dir.path()).await;
    assert!(catalog.shares_connection_with(&again));
    drop(again);

    if let Some(indexes) = oracle(
        &db(dir.path()),
        "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name;",
    ) {
        let names: Vec<&str> = indexes.lines().collect();
        assert_eq!(
            names,
            vec![NAMESPACE_PROPERTIES_KEY_INDEX, TABLES_KEY_INDEX],
            "a fresh file carries the two named unique indexes and no autoindex"
        );
        let pk = oracle(
            &db(dir.path()),
            "SELECT count(*) FROM sqlite_master WHERE sql LIKE '%PRIMARY KEY%';",
        )
        .unwrap();
        assert_eq!(pk, "0", "no declared composite PRIMARY KEY on a fresh file");
    }
    assert_oracle_ok(&db(dir.path()));
}

#[tokio::test]
async fn namespaces_and_tables_roundtrip_and_persist_across_reopen() {
    let dir = TempDir::new().unwrap();
    let ns = NamespaceIdent::new("sales".to_string());
    {
        let catalog = open(dir.path()).await;
        assert!(catalog.list_namespaces(None).await.unwrap().is_empty());

        let created = catalog
            .create_namespace(
                &ns,
                HashMap::from([("owner".to_string(), "data-eng".to_string())]),
            )
            .await
            .unwrap();
        assert_eq!(created.properties().get("exists").unwrap(), "true");
        assert_eq!(created.properties().get("owner").unwrap(), "data-eng");
        assert!(catalog.namespace_exists(&ns).await.unwrap());

        // Duplicate namespace is refused at the catalog layer.
        assert!(catalog.create_namespace(&ns, HashMap::new()).await.is_err());

        catalog
            .update_namespace(
                &ns,
                HashMap::from([
                    ("owner".to_string(), "finance".to_string()),
                    ("tier".to_string(), "gold".to_string()),
                ]),
            )
            .await
            .unwrap();
        let props = catalog.get_namespace(&ns).await.unwrap();
        assert_eq!(props.properties().get("owner").unwrap(), "finance");
        assert_eq!(props.properties().get("tier").unwrap(), "gold");

        let table = catalog.create_table(&ns, creation("orders")).await.unwrap();
        assert!(table.metadata_location().unwrap().starts_with("file://"));
        assert!(Path::new(
            table
                .metadata_location()
                .unwrap()
                .strip_prefix("file://")
                .unwrap()
        )
        .exists());

        let ident = TableIdent::new(ns.clone(), "orders".to_string());
        assert!(catalog.table_exists(&ident).await.unwrap());
        assert_eq!(catalog.list_tables(&ns).await.unwrap(), vec![ident.clone()]);
        assert!(catalog.create_table(&ns, creation("orders")).await.is_err());

        let loaded = catalog.load_table(&ident).await.unwrap();
        assert_eq!(loaded.metadata().current_schema().as_ref(), &simple_schema());

        let renamed = TableIdent::new(ns.clone(), "orders_v2".to_string());
        catalog.rename_table(&ident, &renamed).await.unwrap();
        assert!(!catalog.table_exists(&ident).await.unwrap());
        assert!(catalog.table_exists(&renamed).await.unwrap());

        // A namespace with tables cannot be dropped.
        assert!(catalog.drop_namespace(&ns).await.is_err());
    }

    // Fresh process-equivalent: the worker thread went away with the last
    // handle; a new open reads what was committed.
    let catalog = open(dir.path()).await;
    assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![ns.clone()]);
    let renamed = TableIdent::new(ns.clone(), "orders_v2".to_string());
    assert_eq!(catalog.list_tables(&ns).await.unwrap(), vec![renamed.clone()]);

    catalog.drop_table(&renamed).await.unwrap();
    assert!(matches!(
        catalog.load_table(&renamed).await,
        Err(e) if e.message().contains("No such table")
    ));
    catalog.drop_namespace(&ns).await.unwrap();
    assert!(!catalog.namespace_exists(&ns).await.unwrap());
    assert_oracle_ok(&db(dir.path()));
}

/// Two commits from the same loaded handle both land: iceberg's
/// `Transaction::commit` reloads the table inside `update_table` and applies
/// the change on top, so the compare-and-swap sees the current pointer each
/// time. What this checks is the swap itself: the pointer advances, the
/// previous pointer is recorded, the row count stays one, and the file the
/// delete + insert left behind is valid to stock sqlite3. (The race where the
/// stored pointer moves between the reload and the swap is exercised by the
/// module's unit test on `replace_row`.)
#[tokio::test]
async fn commit_swaps_the_pointer_and_records_the_previous_one() {
    let dir = TempDir::new().unwrap();
    let catalog = open(dir.path()).await;
    let ns = NamespaceIdent::new("sales".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
    let table = catalog.create_table(&ns, creation("orders")).await.unwrap();
    let ident = table.identifier().clone();
    let v0 = table.metadata_location().unwrap().to_string();

    let t1 = catalog.load_table(&ident).await.unwrap();
    let t2 = catalog.load_table(&ident).await.unwrap();

    let tx = Transaction::new(&t1);
    let tx = tx
        .update_table_properties()
        .set("writer".to_string(), "one".to_string())
        .apply(tx)
        .unwrap();
    let c1 = tx.commit(&catalog).await.unwrap();
    let v1 = c1.metadata_location().unwrap().to_string();
    assert_eq!(c1.metadata().properties().get("writer").unwrap(), "one");
    assert_ne!(v1, v0);

    let tx = Transaction::new(&t2);
    let tx = tx
        .update_table_properties()
        .set("reviewer".to_string(), "two".to_string())
        .apply(tx)
        .unwrap();
    let c2 = tx.commit(&catalog).await.unwrap();
    let v2 = c2.metadata_location().unwrap().to_string();
    assert_ne!(v2, v1);
    // Applied on top of the reloaded v1, so both properties are present.
    assert_eq!(c2.metadata().properties().get("writer").unwrap(), "one");
    assert_eq!(c2.metadata().properties().get("reviewer").unwrap(), "two");

    let reloaded = catalog.load_table(&ident).await.unwrap();
    assert_eq!(reloaded.metadata_location().unwrap(), v2);

    if let Some(row) = oracle(
        &db(dir.path()),
        "SELECT count(*) || '|' || metadata_location || '|' || previous_metadata_location FROM iceberg_tables;",
    ) {
        assert_eq!(row, format!("1|{v2}|{v1}"));
    }
    assert_oracle_ok(&db(dir.path()));
}

/// `commit_transaction` (#823) moves two tables in one call; each records its
/// previous pointer, and the file stays valid to stock sqlite3.
#[tokio::test]
async fn commit_transaction_moves_two_tables() {
    let dir = TempDir::new().unwrap();
    let catalog = open(dir.path()).await;
    assert!(catalog.supports_commit_transaction());
    let ns = NamespaceIdent::new("sales".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

    let mut before = Vec::new();
    let mut commits = Vec::new();
    for name in ["orders", "refunds"] {
        let table = catalog.create_table(&ns, creation(name)).await.unwrap();
        before.push(table.metadata_location().unwrap().to_string());
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set("batch".to_string(), "7".to_string())
            .apply(tx)
            .unwrap();
        commits.push(tx.prepare_commit().await.unwrap());
    }
    catalog.commit_transaction(commits).await.unwrap();

    for (name, previous) in ["orders", "refunds"].into_iter().zip(before) {
        let ident = TableIdent::new(ns.clone(), name.to_string());
        let table = catalog.load_table(&ident).await.unwrap();
        assert_eq!(table.metadata().properties().get("batch").unwrap(), "7");
        assert_ne!(table.metadata_location().unwrap(), previous);
        if let Some(prev) = oracle(
            &db(dir.path()),
            &format!(
                "SELECT previous_metadata_location FROM iceberg_tables WHERE table_name = '{name}';"
            ),
        ) {
            assert_eq!(prev, previous);
        }
    }
    assert_oracle_ok(&db(dir.path()));
}

#[tokio::test]
async fn one_connection_per_file_across_path_spellings() {
    let dir = TempDir::new().unwrap();
    let a = open(dir.path()).await;
    // Same file, non-canonical spelling.
    let spelled = dir.path().join(".").join("sqe.db");
    let b = SqliteRsCatalog::open(NAME, &spelled, warehouse(dir.path()))
        .await
        .unwrap();
    assert!(a.shares_connection_with(&b));
    assert_eq!(a.db_path(), b.db_path());

    // A different file is a different connection.
    let other = TempDir::new().unwrap();
    let c = open(other.path()).await;
    assert!(!a.shares_connection_with(&c));
}

/// The adoption case: a file the sqlx + libsqlite3 path created, with the
/// declared composite PRIMARY KEY and its `sqlite_autoindex_*`, opens under
/// sqlite-rs, takes a write, and still reads back under sqlx with a clean
/// oracle integrity check.
#[cfg(feature = "sql-sqlite")]
#[tokio::test]
async fn a_file_written_by_the_sqlx_path_roundtrips_through_sqlite_rs() {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_sql::{
        SqlCatalogBuilder, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE,
    };

    let dir = TempDir::new().unwrap();
    let ns = NamespaceIdent::new("sales".to_string());
    let via_sqlx = || async {
        let props = HashMap::from([
            (
                SQL_CATALOG_PROP_URI.to_string(),
                format!("sqlite://{}?mode=rwc", db(dir.path()).display()),
            ),
            (SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse(dir.path())),
        ]);
        SqlCatalogBuilder::default()
            .with_storage_factory(sqe_catalog::storage_factory())
            .load(NAME.to_string(), props)
            .await
            .unwrap()
    };

    // Phase 1: sqlx writes the file.
    {
        let sqlx_catalog = via_sqlx().await;
        sqlx_catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .unwrap();
        sqlx_catalog
            .create_table(&ns, creation("orders"))
            .await
            .unwrap();
    }
    if let Some(indexes) = oracle(
        &db(dir.path()),
        "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name;",
    ) {
        assert!(
            indexes.contains("sqlite_autoindex_iceberg_tables_1"),
            "precondition: the sqlx path leaves autoindexes: {indexes}"
        );
    }

    // Phase 2: sqlite-rs opens it, reads, and writes a second table.
    {
        let rs = open(dir.path()).await;
        assert_eq!(rs.list_namespaces(None).await.unwrap(), vec![ns.clone()]);
        let orders = TableIdent::new(ns.clone(), "orders".to_string());
        assert_eq!(rs.list_tables(&ns).await.unwrap(), vec![orders.clone()]);
        let loaded = rs.load_table(&orders).await.unwrap();
        assert_eq!(loaded.metadata().current_schema().as_ref(), &simple_schema());

        rs.create_table(&ns, creation("customers")).await.unwrap();
        // Uniqueness is enforced through the recovered autoindex.
        assert!(rs.create_table(&ns, creation("customers")).await.is_err());
    }
    if let Some(indexes) = oracle(
        &db(dir.path()),
        "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name;",
    ) {
        assert!(
            !indexes.contains(TABLES_KEY_INDEX),
            "an adopted file keeps its autoindexes and gets no second key index: {indexes}"
        );
    }
    assert_oracle_ok(&db(dir.path()));

    // Phase 3: sqlx reads back both tables.
    let sqlx_catalog = via_sqlx().await;
    let mut names: Vec<String> = sqlx_catalog
        .list_tables(&ns)
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["customers", "orders"]);
}

/// Many tasks, one connection. The facade's worker serialises statements but
/// not transactions, so without the catalog's per-connection gate two
/// concurrent commits interleave their `BEGIN`s ("cannot start a transaction
/// within a transaction", coordinator `maintenance_lease_test` on CI) and an
/// autocommit statement from a third task lands inside someone else's
/// transaction. Eight concurrent property commits from the same base plus
/// eight concurrent namespace creations with properties: every call must
/// return a real outcome (success or the retryable conflict), never an
/// engine error, and the end state must be consistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_commits_and_writes_on_one_connection_never_nest_transactions() {
    let dir = TempDir::new().unwrap();
    let catalog = open(dir.path()).await;
    let ns = NamespaceIdent::new("sales".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
    let table = catalog.create_table(&ns, creation("orders")).await.unwrap();
    let ident = table.identifier().clone();

    let mut commits = Vec::new();
    for i in 0..8 {
        let catalog = catalog.clone();
        let ident = ident.clone();
        commits.push(tokio::spawn(async move {
            let t = catalog.load_table(&ident).await.unwrap();
            let tx = Transaction::new(&t);
            let tx = tx
                .update_table_properties()
                .set(format!("writer-{i}"), "x".to_string())
                .apply(tx)
                .unwrap();
            tx.commit(&catalog).await
        }));
    }
    let mut namespaces = Vec::new();
    for i in 0..8 {
        let catalog = catalog.clone();
        namespaces.push(tokio::spawn(async move {
            catalog
                .create_namespace(
                    &NamespaceIdent::new(format!("ns{i}")),
                    HashMap::from([
                        ("owner".to_string(), format!("team-{i}")),
                        ("tier".to_string(), "gold".to_string()),
                    ]),
                )
                .await
        }));
    }

    let mut ok = 0;
    for c in commits {
        match c.await.unwrap() {
            Ok(_) => ok += 1,
            Err(e) => {
                assert_eq!(
                    e.kind(),
                    iceberg::ErrorKind::CatalogCommitConflicts,
                    "a concurrent commit may only fail with the retryable conflict: {e}"
                );
                assert!(e.retryable());
            }
        }
    }
    assert!(ok >= 1, "at least one commit must land");
    for n in namespaces {
        n.await.unwrap().unwrap();
    }

    // Consistent end state: one row per table, every namespace present with
    // both properties, the file valid to stock sqlite3.
    let final_table = catalog.load_table(&ident).await.unwrap();
    let landed = (0..8)
        .filter(|i| {
            final_table
                .metadata()
                .properties()
                .contains_key(&format!("writer-{i}"))
        })
        .count();
    assert_eq!(landed, ok, "every successful commit is visible, no more");
    for i in 0..8 {
        let got = catalog
            .get_namespace(&NamespaceIdent::new(format!("ns{i}")))
            .await
            .unwrap();
        assert_eq!(got.properties().get("owner").unwrap(), &format!("team-{i}"));
        assert_eq!(got.properties().get("tier").unwrap(), "gold");
    }
    if let Some(count) = oracle(&db(dir.path()), "SELECT count(*) FROM iceberg_tables;") {
        assert_eq!(count, "1");
    }
    assert_oracle_ok(&db(dir.path()));
}
