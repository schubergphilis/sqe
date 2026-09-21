//! Regression guard for issue #700: a FULL OUTER JOIN between two CTEs, one
//! carrying a window function, over partially overlapping keys must return
//! every key from both sides (NULLs on the missing side), not one side only.
//!
//! Runs the real `QueryHandler::execute` path (SQL rewrite, policy rewrite,
//! DataFusion optimizer, post-planning rule pipeline, local execution) over
//! two Hive external CSV tables, the same catalog kind the report came from.
//! A plain DataFusion `SessionContext` over `MemTable`s is the oracle.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use sqe_catalog::hive::provider::HiveMetadataCache;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_policy::plan_rewriter::HiveCatalogs;
use sqe_policy::PassthroughEnforcer;

/// a(k, x): keys 1,2,3.  b(k, y): keys 2,3,4.  FULL OUTER on k = 4 keys.
const A_ROWS: &[(i64, i64)] = &[(1, 10), (2, 20), (3, 30)];
const B_ROWS: &[(i64, i64)] = &[(2, 200), (3, 300), (4, 400)];
const EXPECTED_KEYS: &[i64] = &[1, 2, 3, 4];

/// `{a}` / `{b}` are substituted with the table references.
const SHAPES: &[(&str, &str)] = &[
    (
        "sum_over_left",
        "WITH c1 AS (SELECT k, x, SUM(x) OVER () AS total FROM {a}), \
              c2 AS (SELECT k, y FROM {b}) \
         SELECT COALESCE(c1.k, c2.k) AS k FROM c1 FULL OUTER JOIN c2 ON c1.k = c2.k",
    ),
    (
        "row_number_partition_left",
        "WITH c1 AS (SELECT k, x, ROW_NUMBER() OVER (PARTITION BY k ORDER BY x) AS rn FROM {a}), \
              c2 AS (SELECT k, y FROM {b}) \
         SELECT COALESCE(c1.k, c2.k) AS k FROM c1 FULL OUTER JOIN c2 ON c1.k = c2.k",
    ),
    (
        "sum_over_right",
        "WITH c1 AS (SELECT k, x FROM {a}), \
              c2 AS (SELECT k, y, SUM(y) OVER () AS total FROM {b}) \
         SELECT COALESCE(c1.k, c2.k) AS k FROM c1 FULL OUTER JOIN c2 ON c1.k = c2.k",
    ),
    (
        "share_then_group_by_left",
        "WITH w AS (SELECT k, CAST(x AS DOUBLE) / SUM(x) OVER (PARTITION BY k) AS share FROM {a}), \
              c1 AS (SELECT k, SUM(share) AS s FROM w GROUP BY k), \
              c2 AS (SELECT k, SUM(y) AS r FROM {b} GROUP BY k) \
         SELECT COALESCE(c1.k, c2.k) AS k, c1.s, c2.r FROM c1 FULL OUTER JOIN c2 ON c1.k = c2.k",
    ),
    (
        "order_by_limit",
        "WITH c1 AS (SELECT k, x, SUM(x) OVER () AS total FROM {a}), \
              c2 AS (SELECT k, y FROM {b}) \
         SELECT COALESCE(c1.k, c2.k) AS k FROM c1 FULL OUTER JOIN c2 ON c1.k = c2.k ORDER BY 1 LIMIT 10",
    ),
    (
        "select_star_both_sides",
        "WITH c1 AS (SELECT k, x, ROW_NUMBER() OVER (PARTITION BY k ORDER BY x) AS rn FROM {a}), \
              c2 AS (SELECT k, y FROM {b}) \
         SELECT COALESCE(c1.k, c2.k) AS k, c1.x, c1.rn, c2.y FROM c1 FULL OUTER JOIN c2 ON c1.k = c2.k",
    ),
    (
        "window_both_sides",
        "WITH c1 AS (SELECT k, SUM(x) OVER (PARTITION BY k) AS sx FROM {a}), \
              c2 AS (SELECT k, SUM(y) OVER (PARTITION BY k) AS sy FROM {b}) \
         SELECT COALESCE(c1.k, c2.k) AS k FROM c1 FULL OUTER JOIN c2 ON c1.k = c2.k",
    ),
];

fn keys(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().iter().flatten())
        .collect();
    out.sort_unstable();
    out
}

fn write_csv_table(dir: &std::path::Path, col: &str, rows: &[(i64, i64)]) {
    let partition = dir.join("dt=2026-01-01");
    std::fs::create_dir_all(&partition).expect("mkdir partition");
    let body: String = rows.iter().map(|(k, v)| format!("{k},{v}\n")).collect();
    std::fs::write(partition.join("a.csv"), format!("k,{col}\n{body}")).expect("write csv");
}

fn mem_table(col: &str, rows: &[(i64, i64)]) -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new(col, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
        ],
    )
    .unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

async fn mock_polaris() -> wiremock::MockServer {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/v1/config"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/v1/namespaces"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[]}"#))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn full_outer_join_over_window_cte_keeps_both_sides() {
    // Oracle: stock DataFusion over MemTables.
    let oracle = SessionContext::new();
    oracle.register_table("a", mem_table("x", A_ROWS)).unwrap();
    oracle.register_table("b", mem_table("y", B_ROWS)).unwrap();

    // System under test: the coordinator over Hive external CSV tables.
    let root_dir = tempfile::tempdir().expect("tempdir");
    let data_a = tempfile::tempdir().expect("tempdir");
    let data_b = tempfile::tempdir().expect("tempdir");
    write_csv_table(data_a.path(), "x", A_ROWS);
    write_csv_table(data_b.path(), "y", B_ROWS);
    let mock = mock_polaris().await;
    let toml_text = format!(
        "[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{}\"\n",
        mock.uri()
    );
    let mut config: SqeConfig = toml::from_str(&toml_text).expect("minimal config parses");
    config.storage.tvf.allow_local_paths = true;
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        None,
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
    .with_hive_registries(
        Arc::new(HiveCatalogs::new()),
        Arc::new(HiveMetadataCache::new()),
    );
    let session = Session::new(
        format!("tester-{}", uuid::Uuid::new_v4()),
        sqe_core::SecretString::new(uuid::Uuid::new_v4().to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        vec!["service_admin".to_string()],
    );
    let (handler, session) = (&handler, &session);
    let run = |sql: String| async move { handler.execute(session, &sql, None).await };
    run(format!(
        "ATTACH 'file://{}/wh' AS lake (TYPE hive_external)",
        root_dir.path().display()
    ))
    .await
    .expect("attach hive root");
    for (name, col, dir) in [("a", "x", &data_a), ("b", "y", &data_b)] {
        run(format!(
            "CREATE EXTERNAL TABLE lake.t.{name} (k BIGINT, {col} BIGINT) \
             PARTITIONED BY (dt string) \
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
             LOCATION 'file://{}' TBLPROPERTIES ('skip.header.line.count' = '1')",
            dir.path().display()
        ))
        .await
        .expect("create external table");
    }

    for (shape, template) in SHAPES {
        let oracle_sql = template.replace("{a}", "a").replace("{b}", "b");
        let oracle_keys = keys(
            &oracle
                .sql(&oracle_sql)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap(),
        );
        assert_eq!(oracle_keys, EXPECTED_KEYS, "oracle wrong for {shape}");

        let sqe_sql = template
            .replace("{a}", "lake.t.a")
            .replace("{b}", "lake.t.b");
        let batches = run(sqe_sql)
            .await
            .unwrap_or_else(|e| panic!("{shape}: {e}"));
        assert_eq!(
            keys(&batches),
            EXPECTED_KEYS,
            "SQE dropped a side for {shape}"
        );
        if *shape == "select_star_both_sides" {
            let nulls = |i: usize| {
                batches
                    .iter()
                    .map(|b| b.column(i).null_count())
                    .sum::<usize>()
            };
            assert_eq!(
                (nulls(1), nulls(3)),
                (1, 1),
                "{shape}: NULL padding on the missing side"
            );
        }
    }
}
