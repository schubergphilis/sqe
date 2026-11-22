//! Tests for the scoped `information_schema` walk. Every query runs twice:
//! through `plan_sql` (scoped) and through plain `ctx.sql` (DataFusion's
//! unscoped walk), and the rows must match wherever the qualifier does not
//! narrow them on purpose.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, SchemaProvider, TableProvider};
use datafusion::datasource::{MemTable, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{SessionConfig, SessionContext};
use sqe_core::{CatalogOp, SqeError};

use super::*;

/// What loading one table of a [`ProbeSchema`] does.
#[derive(Debug, Clone, Copy)]
enum Load {
    Ok,
    /// Polaris refuses the user: `CatalogHttp` 403.
    Denied,
    /// Polaris is down: `CatalogHttp` 503.
    Unavailable,
    /// The table loads but SQE cannot convert its schema.
    Unconvertible,
}

/// A schema whose tables load as [`Load`] says, wrapped the way
/// `SqeSchemaProvider::load_table_or_view` wraps a failure, and which records
/// every `table_names`, `table_type` and `table` call.
#[derive(Debug)]
struct ProbeSchema {
    path: String,
    tables: BTreeMap<String, Load>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl ProbeSchema {
    fn record(&self, call: &str, table: Option<&str>) {
        let entry = match table {
            Some(t) => format!("{call}:{}.{t}", self.path),
            None => format!("{call}:{}", self.path),
        };
        self.calls.lock().unwrap().push(entry);
    }

    fn load(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let Some(load) = self.tables.get(name) else {
            return Ok(None);
        };
        let http = |status| SqeError::CatalogHttp {
            status,
            op: CatalogOp::LoadTable,
            body: format!("{name}: status {status}"),
        };
        let error = match load {
            Load::Ok => {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("note", DataType::Utf8, true),
                ]));
                return Ok(Some(Arc::new(MemTable::try_new(schema, vec![vec![]])?)));
            }
            Load::Denied => http(403),
            Load::Unavailable => http(503),
            Load::Unconvertible => SqeError::catalog_src(
                format!("Failed to convert Iceberg schema to Arrow for {name}"),
                std::io::Error::other("unsupported type"),
            ),
        };
        Err(DataFusionError::External(Box::new(error)))
    }
}

#[async_trait]
impl SchemaProvider for ProbeSchema {
    fn table_names(&self) -> Vec<String> {
        self.record("list", None);
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.record("load", Some(name));
        self.load(name)
    }

    async fn table_type(&self, name: &str) -> DFResult<Option<TableType>> {
        self.record("type", Some(name));
        self.load(name).map(|t| t.map(|t| t.table_type()))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

struct Fixture {
    ctx: SessionContext,
    calls: Arc<Mutex<Vec<String>>>,
}

/// One schema: its name and its tables with how each loads.
type SchemaSpec<'a> = (&'a str, &'a [(&'a str, Load)]);

impl Fixture {
    /// Catalogs `lake` and `other`, each a map of schema to tables.
    fn new(catalogs: &[(&str, &[SchemaSpec<'_>])]) -> Self {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        let calls = Arc::new(Mutex::new(Vec::new()));
        for (catalog, schemas) in catalogs {
            let provider = MemoryCatalogProvider::new();
            for (schema, tables) in *schemas {
                let probe = ProbeSchema {
                    path: format!("{catalog}.{schema}"),
                    tables: tables.iter().map(|(t, l)| (t.to_string(), *l)).collect(),
                    calls: Arc::clone(&calls),
                };
                provider.register_schema(schema, Arc::new(probe)).unwrap();
            }
            ctx.register_catalog(*catalog, Arc::new(provider));
        }
        Self { ctx, calls }
    }

    fn lakes() -> Self {
        use Load::Ok as O;
        Self::new(&[
            (
                "lake",
                &[
                    ("sales", &[("orders", O), ("customers", O)]),
                    ("hr", &[("people", O)]),
                    ("x", &[("t", O)]),
                    ("y", &[("t", O)]),
                    ("z", &[("t", O)]),
                ],
            ),
            (
                "other",
                &[("sales", &[("orders", O)]), ("ops", &[("jobs", O)])],
            ),
        ])
    }

    async fn scoped(&self, sql: &str) -> DFResult<String> {
        let df = crate::file_tvf_prepare::plan_sql(&self.ctx, sql).await?;
        Ok(pretty_format_batches(&df.collect().await?)?.to_string())
    }

    async fn unscoped(&self, sql: &str) -> DFResult<String> {
        let df = self.ctx.sql(sql).await?;
        Ok(pretty_format_batches(&df.collect().await?)?.to_string())
    }

    fn take_calls(&self) -> Vec<String> {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }

    /// Run `sql` both ways, assert the rows match, and return the calls the
    /// scoped run made.
    async fn same_rows(&self, sql: &str) -> Vec<String> {
        let unscoped = self.unscoped(sql).await.unwrap();
        self.take_calls();
        let scoped = self.scoped(sql).await.unwrap();
        assert_eq!(scoped, unscoped, "{sql}");
        self.take_calls()
    }
}

fn touched(calls: &[String], prefix: &str) -> bool {
    calls.iter().any(|c| {
        c.split_once(':')
            .is_some_and(|(_, p)| p.starts_with(prefix))
    })
}

// ── dbt-sqe shapes (adapters/dbt-sqe/dbt/include/sqe/macros) ─────────────

const GET_CATALOG: &str = r#"
    SELECT
      c.table_catalog AS "table_database",
      c.table_schema AS "table_schema",
      c.table_name AS "table_name",
      CASE t.table_type WHEN 'VIEW' THEN 'view' ELSE 'table' END AS "table_type",
      NULL AS "table_comment",
      c.column_name AS "column_name",
      c.ordinal_position AS "column_index",
      c.data_type AS "column_type",
      NULL AS "column_comment"
    FROM "lake".information_schema.columns c
    JOIN "lake".information_schema.tables t
      ON t.table_catalog = c.table_catalog
     AND t.table_schema = c.table_schema
     AND t.table_name = c.table_name
    WHERE c.table_catalog = 'lake'
      AND c.table_schema IN ('sales', 'hr')
    ORDER BY c.table_schema, c.table_name, c.ordinal_position"#;

const GET_COLUMNS_IN_RELATION: &str = r#"
    SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
    FROM "lake".information_schema.columns
    WHERE table_name = 'orders'
      AND table_schema = 'sales'
      AND table_catalog = 'lake'
    ORDER BY ordinal_position"#;

const LIST_SCHEMAS: &str = r#"
    SELECT DISTINCT schema_name
    FROM "lake".information_schema.schemata
    WHERE catalog_name = 'lake'
    ORDER BY schema_name"#;

const LIST_RELATIONS: &str = r#"
    SELECT table_catalog AS "database", table_name AS "name", table_schema AS "schema",
      CASE table_type WHEN 'VIEW' THEN 'view' ELSE 'table' END AS "type"
    FROM "lake".information_schema.tables
    WHERE table_schema = 'sales'
      AND table_catalog = 'lake'
    ORDER BY "name""#;

#[tokio::test]
async fn dbt_shapes_keep_their_rows_and_never_walk_another_catalog() {
    let f = Fixture::lakes();
    for sql in [
        GET_CATALOG,
        GET_COLUMNS_IN_RELATION,
        LIST_SCHEMAS,
        LIST_RELATIONS,
    ] {
        let calls = f.same_rows(sql).await;
        assert!(!touched(&calls, "other."), "{sql}\n{calls:?}");
    }
}

#[tokio::test]
async fn get_columns_in_relation_loads_only_its_table() {
    let f = Fixture::lakes();
    let calls = f.same_rows(GET_COLUMNS_IN_RELATION).await;
    let loads: Vec<_> = calls.iter().filter(|c| c.starts_with("load:")).collect();
    assert_eq!(loads, ["load:lake.sales.orders"], "{calls:?}");
    assert_eq!(
        calls
            .iter()
            .filter(|c| c.starts_with("list:"))
            .collect::<Vec<_>>(),
        ["list:lake.sales"]
    );
}

#[tokio::test]
async fn get_catalog_walks_only_the_listed_schemas() {
    let f = Fixture::lakes();
    let calls = f.same_rows(GET_CATALOG).await;
    for schema in ["lake.x", "lake.y", "lake.z", "other."] {
        assert!(!touched(&calls, schema), "{schema}: {calls:?}");
    }
    assert!(touched(&calls, "lake.sales") && touched(&calls, "lake.hr"));
}

// ── narrowing ────────────────────────────────────────────────────────────

/// DataFusion's simplifier may turn a short IN list into an OR chain; every
/// length must still narrow.
#[tokio::test]
async fn schema_in_lists_of_any_length_narrow_the_walk() {
    let all = ["sales", "hr", "x", "y", "z"];
    for n in [1, 2, 3, 5] {
        let f = Fixture::lakes();
        let list = all[..n]
            .iter()
            .map(|s| format!("'{s}'"))
            .collect::<Vec<_>>();
        let sql = format!(
            "SELECT table_schema, table_name FROM \"lake\".information_schema.columns \
             WHERE table_catalog = 'lake' AND table_schema IN ({}) ORDER BY 1, 2",
            list.join(", ")
        );
        let calls = f.same_rows(&sql).await;
        for schema in &all[n..] {
            assert!(
                !touched(&calls, &format!("lake.{schema}")),
                "n={n}: {calls:?}"
            );
        }
        for schema in &all[..n] {
            assert!(
                touched(&calls, &format!("lake.{schema}")),
                "n={n}: {calls:?}"
            );
        }
    }
}

#[tokio::test]
async fn a_bare_read_narrows_by_its_catalog_conjunct_only() {
    let f = Fixture::lakes();
    let calls = f
        .same_rows(
            "SELECT table_schema, table_name FROM information_schema.tables \
             WHERE table_catalog = 'lake' ORDER BY 1, 2",
        )
        .await;
    assert!(!touched(&calls, "other."), "{calls:?}");

    let calls = f
        .same_rows(
            "SELECT table_catalog, table_schema, table_name FROM information_schema.tables \
             ORDER BY 1, 2, 3",
        )
        .await;
    assert!(
        touched(&calls, "other.") && touched(&calls, "lake."),
        "{calls:?}"
    );
}

#[tokio::test]
async fn the_qualifier_limits_rows_to_its_catalog() {
    let f = Fixture::lakes();
    let sql = "SELECT DISTINCT table_catalog FROM \"lake\".information_schema.tables ORDER BY 1";
    let unscoped = f.unscoped(sql).await.unwrap();
    assert!(unscoped.contains("other"), "{unscoped}");
    let scoped = f.scoped(sql).await.unwrap();
    assert!(
        scoped.contains("lake") && !scoped.contains("other"),
        "{scoped}"
    );

    // A qualifier that contradicts the catalog conjunct admits nothing.
    let f = Fixture::lakes();
    let sql = "SELECT table_name FROM \"lake\".information_schema.tables \
               WHERE table_catalog = 'other'";
    let scoped = f.scoped(sql).await.unwrap();
    assert!(
        !scoped.contains("orders") && !scoped.contains("jobs"),
        "{scoped}"
    );
    assert!(f.take_calls().is_empty());
}

#[tokio::test]
async fn other_information_schema_views_are_untouched() {
    let f = Fixture::lakes();
    f.same_rows("SELECT count(*) > 0 FROM \"lake\".information_schema.df_settings")
        .await;
}

// ── D6: access denied skips, everything else fails ───────────────────────

fn d6(load: Load) -> Fixture {
    Fixture::new(&[("lake", &[("sec", &[("open", Load::Ok), ("closed", load)])])])
}

const SEC_COLUMNS: &str =
    "SELECT table_name, column_name FROM \"lake\".information_schema.columns \
                           WHERE table_schema = 'sec' ORDER BY 1, 2";
const SEC_TABLES: &str = "SELECT table_name FROM \"lake\".information_schema.tables \
                          WHERE table_schema = 'sec' ORDER BY 1";

#[tokio::test]
async fn a_table_the_user_may_not_read_is_left_out_of_the_listing() {
    let f = d6(Load::Denied);
    // The unscoped walk fails on the first 403.
    assert!(f.unscoped(SEC_COLUMNS).await.is_err());
    for sql in [SEC_COLUMNS, SEC_TABLES] {
        let rows = f.scoped(sql).await.unwrap();
        assert!(
            rows.contains("open") && !rows.contains("closed"),
            "{sql}\n{rows}"
        );
    }
}

#[tokio::test]
async fn an_unavailable_catalog_fails_the_listing() {
    let f = d6(Load::Unavailable);
    for sql in [SEC_COLUMNS, SEC_TABLES] {
        let error = f.scoped(sql).await.unwrap_err().to_string();
        assert!(error.contains("503"), "{sql}: {error}");
    }
}

#[tokio::test]
async fn a_schema_that_cannot_be_converted_fails_the_listing() {
    let f = d6(Load::Unconvertible);
    for sql in [SEC_COLUMNS, SEC_TABLES] {
        let error = f.scoped(sql).await.unwrap_err().to_string();
        assert!(
            error.contains("Failed to convert Iceberg schema"),
            "{sql}: {error}"
        );
    }
}

#[test]
fn access_denied_is_found_under_any_wrapper() {
    let denied = || {
        DataFusionError::External(Box::new(SqeError::CatalogHttp {
            status: 403,
            op: CatalogOp::LoadTable,
            body: String::new(),
        }))
    };
    assert!(is_access_denied(&denied()));
    assert!(is_access_denied(&denied().context("loading t")));
    assert!(is_access_denied(&DataFusionError::Shared(Arc::new(
        denied()
    ))));
    assert!(is_access_denied(&DataFusionError::External(Box::new(
        denied()
    ))));
    let unavailable = DataFusionError::External(Box::new(SqeError::CatalogHttp {
        status: 503,
        op: CatalogOp::LoadTable,
        body: String::new(),
    }));
    assert!(!is_access_denied(&unavailable));
    assert!(!is_access_denied(&DataFusionError::Plan(
        "access denied".into()
    )));
}

#[tokio::test]
async fn the_enumeration_wrapper_forwards_what_it_does_not_change() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let probe = ProbeSchema {
        path: "lake.sec".into(),
        tables: [("a".to_string(), Load::Ok), ("b".to_string(), Load::Denied)].into(),
        calls,
    };
    let scope = MetaScope {
        table: Some(["a".to_string()].into()),
        ..MetaScope::default()
    };
    let wrapped = EnumerationSchemaProvider::new(Arc::new(probe), scope);
    assert_eq!(wrapped.table_names(), ["a"]);
    assert!(wrapped.table_exist("b"));
    assert!(wrapped.table("b").await.unwrap().is_none());
    assert_eq!(
        wrapped.table_type("a").await.unwrap(),
        Some(TableType::Base)
    );
}
