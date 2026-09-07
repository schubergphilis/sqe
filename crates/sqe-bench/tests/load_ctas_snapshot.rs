//! CTAS text the loader sends, snapshotted for one table per suite (review
//! 706: `load::load_benchmark` used to be unreachable from `tests/`).
//!
//! A recording `BenchClient` captures every `execute_update` statement; the
//! load never touches a network. The snapshots pin the clause order the
//! engine's parser requires (TBLPROPERTIES before PARTITIONED BY), the
//! partition specs, the sort-on-write keys, and the bloom / puffin levers.

use std::sync::Mutex;

use arrow_array::RecordBatch;
use sqe_bench::client::BenchClient;
use sqe_bench::load::{load_benchmark, LoadArgs, S3Args};

#[derive(Default)]
struct Recorder {
    statements: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl BenchClient for Recorder {
    async fn execute(&self, _sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        Ok(vec![])
    }
    async fn execute_update(&self, sql: &str) -> anyhow::Result<()> {
        self.statements.lock().unwrap().push(sql.to_string());
        Ok(())
    }
    fn protocol_name(&self) -> &str {
        "recorder"
    }
}

async fn statements(
    benchmark: &str,
    bloom: bool,
    puffin: bool,
    catalog: Option<&str>,
) -> Vec<String> {
    let rec = Recorder::default();
    let s3 = S3Args {
        access_key: None,
        secret_key: None,
        endpoint: None,
        region: "us-east-1".to_string(),
    };
    load_benchmark(
        &rec,
        &LoadArgs {
            benchmark,
            scale: 1.0,
            data_path: "data",
            s3_args: &s3,
            clean: false,
            catalog,
            namespace_override: None,
            bloom_filter: bloom,
            puffin_stats: puffin,
        },
    )
    .await
    .expect("load against recorder");
    rec.statements.into_inner().unwrap()
}

fn find<'a>(stmts: &'a [String], table: &str) -> &'a str {
    stmts
        .iter()
        .find(|s| {
            s.starts_with(&format!("CREATE TABLE {table}"))
                || s.starts_with(&format!("CREATE TABLE {table} "))
        })
        .unwrap_or_else(|| panic!("no CTAS for {table} in {stmts:#?}"))
}

#[tokio::test]
async fn tpch_lineitem_is_partitioned_and_not_sorted() {
    let stmts = statements("tpch", false, false, None).await;
    assert_eq!(stmts[0], "CREATE SCHEMA IF NOT EXISTS tpch_sf1");
    assert_eq!(
        find(&stmts, "tpch_sf1.lineitem"),
        "CREATE TABLE tpch_sf1.lineitem PARTITIONED BY (month(l_shipdate)) \
         AS SELECT * FROM read_parquet('data/tpch/sf1/lineitem/*.parquet', region => 'us-east-1')"
    );
}

#[tokio::test]
async fn tpch_bloom_and_puffin_share_one_tblproperties_before_partitioned_by() {
    let stmts = statements("tpch", true, true, Some("main")).await;
    assert_eq!(stmts[0], "CREATE SCHEMA IF NOT EXISTS main.tpch_sf1");
    assert_eq!(
        find(&stmts, "main.tpch_sf1.orders"),
        "CREATE TABLE main.tpch_sf1.orders \
         TBLPROPERTIES ('write.parquet.bloom-filter-columns' = 'o_orderkey,o_custkey', 'write.puffin.stats' = 'true') \
         PARTITIONED BY (month(o_orderdate)) \
         AS SELECT * FROM read_parquet('data/tpch/sf1/orders/*.parquet', region => 'us-east-1')"
    );
    // Unpartitioned dimension: TBLPROPERTIES only, no ORDER BY.
    assert_eq!(
        find(&stmts, "main.tpch_sf1.region"),
        "CREATE TABLE main.tpch_sf1.region \
         TBLPROPERTIES ('write.parquet.bloom-filter-columns' = 'r_regionkey', 'write.puffin.stats' = 'true') \
         AS SELECT * FROM read_parquet('data/tpch/sf1/region/*.parquet', region => 'us-east-1')"
    );
}

#[tokio::test]
async fn ssb_lineorder_is_sorted_on_write() {
    let stmts = statements("ssb", false, false, None).await;
    assert_eq!(
        find(&stmts, "ssb_sf1.lineorder"),
        "CREATE TABLE ssb_sf1.lineorder \
         AS SELECT * FROM read_parquet('data/ssb/sf1/lineorder/*.parquet', region => 'us-east-1') \
         ORDER BY lo_orderdate"
    );
}

#[tokio::test]
async fn tpcds_store_sales_is_sorted_by_date_key() {
    let stmts = statements("tpcds", false, false, None).await;
    assert_eq!(
        find(&stmts, "tpcds_sf1.store_sales"),
        "CREATE TABLE tpcds_sf1.store_sales \
         AS SELECT * FROM read_parquet('data/tpcds/sf1/store_sales/*.parquet', region => 'us-east-1') \
         ORDER BY ss_sold_date_sk"
    );
}

#[tokio::test]
async fn tpcc_district_is_partitioned_by_warehouse_and_district() {
    let stmts = statements("tpcc", false, false, None).await;
    assert_eq!(
        find(&stmts, "tpcc_sf1.district"),
        "CREATE TABLE tpcc_sf1.district PARTITIONED BY (d_w_id, d_id) \
         AS SELECT * FROM read_parquet('data/tpcc/sf1/district/*.parquet', region => 'us-east-1')"
    );
}

#[tokio::test]
async fn tpce_trade_is_plain() {
    let stmts = statements("tpce", false, false, None).await;
    assert_eq!(
        find(&stmts, "tpce_sf1.trade "),
        "CREATE TABLE tpce_sf1.trade \
         AS SELECT * FROM read_parquet('data/tpce/sf1/trade/*.parquet', region => 'us-east-1')"
    );
}

#[tokio::test]
async fn tpcbb_loads_into_the_tpcds_namespace() {
    let stmts = statements("tpcbb", false, false, None).await;
    assert_eq!(stmts[0], "CREATE SCHEMA IF NOT EXISTS tpcds_sf1");
    assert_eq!(
        find(&stmts, "tpcds_sf1.web_clickstreams"),
        "CREATE TABLE tpcds_sf1.web_clickstreams \
         AS SELECT * FROM read_parquet('data/tpcbb/sf1/web_clickstreams/*.parquet', region => 'us-east-1')"
    );
}

#[tokio::test]
async fn clickbench_hits_is_plain() {
    let stmts = statements("clickbench", false, false, None).await;
    assert_eq!(
        find(&stmts, "clickbench_sf1.hits"),
        "CREATE TABLE clickbench_sf1.hits \
         AS SELECT * FROM read_parquet('data/clickbench/sf1/hits/*.parquet', region => 'us-east-1')"
    );
}

#[tokio::test]
async fn bank_customer_is_plain() {
    let stmts = statements("bank", false, false, None).await;
    assert_eq!(
        find(&stmts, "bank_sf1.customer"),
        "CREATE TABLE bank_sf1.customer \
         AS SELECT * FROM read_parquet('data/bank/sf1/customer/*.parquet', region => 'us-east-1')"
    );
}
