//! Tokenizer-based table qualification (review 707), exercised from outside
//! the crate (review 706). The rewrite is opt-in (`--qualify-tables`) and must
//! never touch aliases, string literals, comments or CTE names.

use sqe_bench::query::{prefix_tables, qualify_tables, QualifyTables};

fn tpch() -> Vec<String> {
    [
        "lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

#[test]
fn column_alias_named_like_a_table_is_untouched() {
    let sql = "SELECT part AS p FROM lineitem";
    let out = qualify_tables(sql, "tpch_sf1", &tpch()).unwrap();
    assert_eq!(out, "SELECT part AS p FROM tpch_sf1.lineitem");
}

#[test]
fn scalar_subquery_after_between_is_qualified() {
    // TPC-C stock_level / TPC-DS q54 shape.
    let tables = vec!["district".to_string()];
    let sql = "SELECT 1 WHERE x BETWEEN (SELECT a FROM district) AND (SELECT b FROM district)";
    let out = qualify_tables(sql, "tpcc_sf1", &tables).unwrap();
    assert_eq!(
        out,
        "SELECT 1 WHERE x BETWEEN (SELECT a FROM tpcc_sf1.district) AND (SELECT b FROM tpcc_sf1.district)"
    );
}

#[test]
fn select_list_alias_in_group_and_order_by_is_untouched() {
    // TPC-DS q49 shape: `AS item` alias, then GROUP BY / ORDER BY on it.
    let tables: Vec<String> = ["item", "store_sales"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let sql = "SELECT 'store' AS channel, ss_item_sk AS item, SUM(ss_quantity) AS sales\n\
               FROM store_sales\nGROUP BY channel, item\nORDER BY channel, item";
    let out = qualify_tables(sql, "tpcds_sf1", &tables).unwrap();
    assert_eq!(
        out,
        "SELECT 'store' AS channel, ss_item_sk AS item, SUM(ss_quantity) AS sales\n\
         FROM tpcds_sf1.store_sales\nGROUP BY channel, item\nORDER BY channel, item"
    );
}

#[test]
fn projection_alias_named_like_a_table_in_window_is_untouched() {
    // TPC-DS q51 shape: `web_sales` / `store_sales` as column aliases.
    let tables: Vec<String> = ["web_sales", "store_sales"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let sql = "SELECT item_sk, web_sales, store_sales,\n\
               MAX(web_sales) OVER (PARTITION BY item_sk ORDER BY d_date) AS web_cumulative\n\
               FROM (SELECT web.cume_sales AS web_sales, store.cume_sales AS store_sales\n\
               FROM web_v1 web FULL OUTER JOIN store_v1 store ON web.item_sk = store.item_sk) x";
    let out = qualify_tables(sql, "tpcds_sf1", &tables).unwrap();
    assert_eq!(out, sql, "no base table is referenced; nothing may change");
}

#[test]
fn string_literals_and_comments_are_never_rewritten() {
    let tables = vec!["news_item".to_string(), "customer".to_string()];
    let sql = "-- credit the customer's balance for news_item\n\
               SELECT 'news_item' AS target, \"customer\" FROM news_item WHERE note = 'customer'";
    let out = qualify_tables(sql, "tpce_sf1", &tables).unwrap();
    assert_eq!(
        out,
        "-- credit the customer's balance for news_item\n\
         SELECT 'news_item' AS target, \"customer\" FROM tpce_sf1.news_item WHERE note = 'customer'"
    );
}

#[test]
fn comma_separated_from_list_with_aliases_and_subquery() {
    let sql = "SELECT * FROM\n    supplier,\n    lineitem l,\n    orders,\n    nation n1,\n    nation n2\n\
               WHERE ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE 1=1)";
    let out = qualify_tables(sql, "tpch_sf1", &tpch()).unwrap();
    assert_eq!(
        out,
        "SELECT * FROM\n    tpch_sf1.supplier,\n    tpch_sf1.lineitem l,\n    tpch_sf1.orders,\n    \
         tpch_sf1.nation n1,\n    tpch_sf1.nation n2\n\
         WHERE ps_suppkey NOT IN (SELECT s_suppkey FROM tpch_sf1.supplier WHERE 1=1)"
    );
}

#[test]
fn longest_name_first_is_not_needed_tokens_are_whole_words() {
    let sql = "SELECT * FROM partsupp, part WHERE ps_partkey = p_partkey";
    let out = qualify_tables(sql, "tpch_sf1", &tpch()).unwrap();
    assert_eq!(
        out,
        "SELECT * FROM tpch_sf1.partsupp, tpch_sf1.part WHERE ps_partkey = p_partkey"
    );
}

#[test]
fn joins_with_on_clauses_and_derived_tables() {
    let sql = "SELECT c.c_id FROM customer c\n\
               JOIN orders o ON o.o_c_id = c.c_id\n\
               LEFT OUTER JOIN (SELECT * FROM lineitem) li ON li.l_orderkey = o.o_id, nation\n\
               WHERE o.o_id = (SELECT MAX(o2.o_id) FROM orders o2 WHERE o2.o_c_id = c.c_id)";
    let out = qualify_tables(sql, "tpch_sf1", &tpch()).unwrap();
    assert_eq!(
        out,
        "SELECT c.c_id FROM tpch_sf1.customer c\n\
         JOIN tpch_sf1.orders o ON o.o_c_id = c.c_id\n\
         LEFT OUTER JOIN (SELECT * FROM tpch_sf1.lineitem) li ON li.l_orderkey = o.o_id, tpch_sf1.nation\n\
         WHERE o.o_id = (SELECT MAX(o2.o_id) FROM tpch_sf1.orders o2 WHERE o2.o_c_id = c.c_id)"
    );
}

#[test]
fn already_qualified_names_and_column_references_are_left_alone() {
    let sql = "SELECT orders.o_id FROM golden.tpch_sf1.orders WHERE orders.o_id > 1";
    let out = qualify_tables(sql, "tpch_sf1", &tpch()).unwrap();
    assert_eq!(out, sql);
}

#[test]
fn cte_named_like_a_table_is_not_qualified() {
    let sql = "WITH orders AS (SELECT * FROM lineitem), x AS (SELECT 1)\n\
               SELECT * FROM orders JOIN x ON true";
    let out = qualify_tables(sql, "tpch_sf1", &tpch()).unwrap();
    assert_eq!(
        out,
        "WITH orders AS (SELECT * FROM tpch_sf1.lineitem), x AS (SELECT 1)\n\
         SELECT * FROM orders JOIN x ON true"
    );
}

#[test]
fn dml_targets_are_qualified() {
    let tables = vec!["customer".to_string(), "district".to_string()];
    let sql = "-- Debit the customer's balance\n\
               UPDATE customer SET c_balance = c_balance - 1 WHERE c_id IN (SELECT d_id FROM district);\n\
               DELETE FROM district WHERE d_id = 1;\n\
               INSERT INTO customer (c_id) VALUES (1)";
    let out = qualify_tables(sql, "tpcc_sf1", &tables).unwrap();
    assert_eq!(
        out,
        "-- Debit the customer's balance\n\
         UPDATE tpcc_sf1.customer SET c_balance = c_balance - 1 WHERE c_id IN (SELECT d_id FROM tpcc_sf1.district);\n\
         DELETE FROM tpcc_sf1.district WHERE d_id = 1;\n\
         INSERT INTO tpcc_sf1.customer (c_id) VALUES (1)"
    );
}

#[test]
fn from_inside_function_arguments_is_not_a_table_introducer() {
    let tables = vec!["orders".to_string(), "part".to_string()];
    let sql =
        "SELECT EXTRACT(YEAR FROM part), SUBSTRING(x FROM 1 FOR 2), TRIM(BOTH ' ' FROM orders)\n\
               FROM orders";
    let out = qualify_tables(sql, "tpch_sf1", &tables).unwrap();
    assert_eq!(
        out,
        "SELECT EXTRACT(YEAR FROM part), SUBSTRING(x FROM 1 FOR 2), TRIM(BOTH ' ' FROM orders)\n\
         FROM tpch_sf1.orders"
    );
}

#[test]
fn drop_table_if_exists_and_create_table_as() {
    let sql = "DROP TABLE IF EXISTS orders; CREATE TABLE orders AS SELECT * FROM lineitem";
    let out = qualify_tables(sql, "tpch_sf1", &tpch()).unwrap();
    assert_eq!(
        out,
        "DROP TABLE IF EXISTS tpch_sf1.orders; CREATE TABLE tpch_sf1.orders AS SELECT * FROM tpch_sf1.lineitem"
    );
}

#[test]
fn quoted_identifier_table_keeps_its_quotes() {
    let tables = vec!["hits".to_string()];
    let sql = "SELECT \"UserID\" FROM \"hits\" WHERE \"AdvEngineID\" <> 0";
    let out = qualify_tables(sql, "clickbench_sf1", &tables).unwrap();
    assert_eq!(
        out,
        "SELECT \"UserID\" FROM clickbench_sf1.\"hits\" WHERE \"AdvEngineID\" <> 0"
    );
}

#[test]
fn multibyte_text_before_a_table_keeps_offsets_right() {
    let sql = "SELECT 'café', 'naïve' FROM orders";
    let out = qualify_tables(sql, "tpch_sf1", &tpch()).unwrap();
    assert_eq!(out, "SELECT 'café', 'naïve' FROM tpch_sf1.orders");
}

#[test]
fn prefix_tables_resolves_the_suite_table_list() {
    let out = prefix_tables("SELECT * FROM lineitem", "tpch_sf1", "tpch").unwrap();
    assert_eq!(out, "SELECT * FROM tpch_sf1.lineitem");
    // TPC-BB queries reference TPC-DS tables too.
    let out = prefix_tables(
        "SELECT * FROM web_clickstreams w JOIN item i ON w.wcs_item_sk = i.i_item_sk",
        "tpcds_sf1",
        "tpcbb",
    )
    .unwrap();
    assert_eq!(
        out,
        "SELECT * FROM tpcds_sf1.web_clickstreams w JOIN tpcds_sf1.item i ON w.wcs_item_sk = i.i_item_sk"
    );
    // Unknown suite: passthrough.
    assert_eq!(
        prefix_tables("SELECT * FROM t", "ns", "nope").unwrap(),
        "SELECT * FROM t"
    );
}

#[test]
fn qualify_mode_decides_per_client_default_schema() {
    assert!(
        QualifyTables::Auto.applies(None),
        "no schema on the wire: qualify"
    );
    assert!(
        !QualifyTables::Auto.applies(Some("tpch_sf1")),
        "X-Trino-Schema set: send the committed text"
    );
    assert!(QualifyTables::Always.applies(Some("tpch_sf1")));
    assert!(!QualifyTables::Never.applies(None));
}
