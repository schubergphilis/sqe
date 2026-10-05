//! Trino syntax DataFusion 55 handles natively, run on the engine session
//! config with no SQE rewrite applied. SQE used to rewrite each of these in
//! `sqe_sql::trino_compat`; if one of these tests starts failing after a
//! DataFusion upgrade, the rewrite has to come back.

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;

async fn rows(sql: &str) -> String {
    let ctx = SessionContext::new_with_config(sqe_catalog::engine_session_config());
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}

/// #348: the `WITH RECURSIVE t(n)` column list names the working table, so
/// the recursive term can reference `n`.
#[tokio::test]
async fn recursive_cte_column_list_names_the_working_table() {
    let out = rows(
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5) \
         SELECT count(*) AS c, max(n) AS m FROM t",
    )
    .await;
    assert!(out.contains("| 5 | 5 |"), "\n{out}");
}

#[tokio::test]
async fn recursive_cte_multi_column_list() {
    let out = rows(
        "WITH RECURSIVE r(a, b) AS (SELECT 1, 10 UNION ALL SELECT a + 1, b + 10 FROM r WHERE a < 3) \
         SELECT a, b FROM r ORDER BY a",
    )
    .await;
    let expected = "\
+---+----+
| a | b  |
+---+----+
| 1 | 10 |
| 2 | 20 |
| 3 | 30 |
+---+----+";
    assert_eq!(out, expected, "\n{out}");
}

/// The declared name wins over an alias already on the anchor.
#[tokio::test]
async fn recursive_cte_column_list_overrides_an_anchor_alias() {
    let out = rows(
        "WITH RECURSIVE t(n) AS (SELECT 1 AS x UNION ALL SELECT n + 1 FROM t WHERE n < 2) \
         SELECT n FROM t ORDER BY n",
    )
    .await;
    let expected = "\
+---+
| n |
+---+
| 1 |
| 2 |
+---+";
    assert_eq!(out, expected, "\n{out}");
}

/// Trino's `ROW(...)` constructor is DataFusion's `row()`, the same struct as
/// `struct(...)`: positional fields `c0`, `c1`, ... including nested rows.
#[tokio::test]
async fn row_constructor_is_a_positional_struct() {
    let out = rows(
        "SELECT ROW(1, 'a') = struct(1, 'a') AS same, \
         arrow_typeof(ROW(1, ROW(2, 'b'))) AS t",
    )
    .await;
    assert!(out.contains("| true "), "\n{out}");
    assert!(
        out.contains("Struct(\"c0\": Int64, \"c1\": Struct(\"c0\": Int64, \"c1\": Utf8))")
            || out.contains("Struct(c0 Int64, c1 Struct(c0 Int64, c1 Utf8))"),
        "\n{out}"
    );
}
