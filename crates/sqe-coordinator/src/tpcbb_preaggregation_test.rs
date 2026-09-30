//! Verify the experimental query against the original, including multiplicity,
//! nullable paid values, absent matches and dimension groups spanning keys.

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::prelude::SessionContext;

#[tokio::test]
async fn preaggregation_preserves_fact_join_multiplicity_and_nulls() {
    let ctx = SessionContext::new();
    let clicks = RecordBatch::try_from_iter([
        (
            "wcs_item_sk",
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                Some(2),
                Some(2),
                Some(3),
                None,
                Some(4),
            ])) as ArrayRef,
        ),
        (
            "wcs_sales_sk",
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(4),
                Some(5),
                Some(6),
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let sales = RecordBatch::try_from_iter([
        (
            "ss_item_sk",
            Arc::new(Int32Array::from(vec![1, 1, 1, 2, 2, 3, 5])) as ArrayRef,
        ),
        (
            "ss_net_paid",
            Arc::new(Float64Array::from(vec![
                Some(10.0),
                Some(20.0),
                None,
                Some(5.0),
                None,
                None,
                Some(99.0),
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        "shared",
        "duplicate",
        "shared",
        "null-paid",
        "no-click",
    ]));
    let item = RecordBatch::try_from_iter([
        (
            "i_item_sk",
            Arc::new(Int32Array::from(vec![1, 1, 2, 3, 6])) as ArrayRef,
        ),
        ("i_item_id", strings.clone()),
        ("i_item_desc", strings.clone()),
        ("i_category", strings.clone()),
        ("i_class", strings),
    ])
    .unwrap();
    ctx.register_batch("web_clickstreams", clicks).unwrap();
    ctx.register_batch("store_sales", sales).unwrap();
    ctx.register_batch("item", item).unwrap();

    let original = include_str!("../../../benchmarks/queries/tpcbb/q01.sql");
    let candidate = include_str!("../../../benchmarks/experiments/tpcbb-q01-preaggregated.sql");
    let mut results = Vec::new();
    for sql in [original, candidate] {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut writer = arrow::json::ArrayWriter::new(Vec::new());
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
        let mut rows: Vec<serde_json::Value> =
            serde_json::from_slice(&writer.into_inner()).unwrap();
        rows.sort_by_key(|row| row["i_item_id"].as_str().unwrap().to_owned());
        results.push(rows);
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0].len(), 3);
    let shared = results[0]
        .iter()
        .find(|row| row["i_item_id"] == "shared")
        .unwrap();
    assert_eq!(shared["click_sales_count"], 8);
    assert_eq!(shared["total_net_revenue"], 65.0);
    assert_eq!(shared["avg_sale_amount"], 13.0);
}
