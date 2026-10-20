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

/// `CAST(x AS TIMESTAMP WITH TIME ZONE)` keeps the zone: the engine session
/// zone is UTC, so the value is zoned and an offset in the text is applied
/// (Trino keeps the offset; Arrow has one zone per column, so SQE normalizes
/// to UTC). With no session zone DataFusion typed it as a naive timestamp.
#[tokio::test]
async fn cast_with_time_zone_keeps_the_zone() {
    let sql = "SELECT arrow_typeof(CAST('2024-01-02 03:04:05' AS TIMESTAMP WITH TIME ZONE)) AS t, \
               CAST('2024-01-02 03:04:05+02:00' AS TIMESTAMP WITH TIME ZONE) AS v, \
               arrow_typeof(CAST('2024-01-02 03:04:05' AS TIMESTAMP)) AS naive";
    let out = rows(sql).await;
    assert!(out.contains("| Timestamp(ns, \"+00:00\") "), "\n{out}");
    assert!(out.contains("| 2024-01-02T01:04:05Z "), "\n{out}");
    assert!(out.contains("| Timestamp(ns) "), "\n{out}");
}

/// A zoned literal compared with a naive column is cast on the literal side,
/// so the column predicate still prunes files.
#[tokio::test]
async fn zoned_literal_filter_leaves_the_column_uncast() {
    let ctx = SessionContext::new_with_config(sqe_catalog::engine_session_config());
    let create = "CREATE TABLE tn (ts TIMESTAMP) AS VALUES (TIMESTAMP '2024-01-01 00:00:00')";
    ctx.sql(create).await.unwrap().collect().await.unwrap();
    for rhs in [
        "CAST('2023-06-01 00:00:00' AS TIMESTAMP WITH TIME ZONE)",
        "to_timestamp('2023-06-01 00:00:00')",
    ] {
        let sql = format!("EXPLAIN SELECT * FROM tn WHERE ts > {rhs}");
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let out = pretty_format_batches(&batches).unwrap().to_string();
        let expected = "Filter: tn.ts > TimestampNanosecond(1685577600000000000, None)";
        assert!(out.contains(expected), "{rhs}\n{out}");
    }
}
