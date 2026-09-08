//! `SHOW STATS` under policy (architecture review 205, #558).
//!
//! `information_schema.columns` hides restricted columns, and `SHOW COLUMNS`,
//! `DESCRIBE` and `SHOW CREATE TABLE` read from it. `SHOW STATS` read the
//! Iceberg schema directly and listed every column, and reported the
//! snapshot's total row count regardless of the caller's row filter. Both
//! are now decided by the same effective policy the plan rewriter applies.
//! The MCP `get_table_stats` tool is a thin wrapper over this statement, so
//! this is also what an agent gets to see.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::Array;
use datafusion::logical_expr::{col, lit};
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::ResolvedPolicy;

/// `(column_name, row_count)` pairs from a SHOW STATS result.
fn stats_rows(batches: &[arrow_array::RecordBatch]) -> Vec<(String, Option<f64>)> {
    let mut out = Vec::new();
    for b in batches {
        let names = b.column(0).as_string::<i32>();
        let counts = b.column(4).as_primitive::<arrow_array::types::Float64Type>();
        for i in 0..b.num_rows() {
            out.push((
                names.value(i).to_string(),
                if counts.is_null(i) {
                    None
                } else {
                    Some(counts.value(i))
                },
            ));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn show_stats_hides_restricted_columns_and_filtered_row_counts() {
    // An unrestricted session creates the table and sees everything.
    let (session, plain) = crate::common::setup_handler().await;
    let _ = plain
        .execute(&session, "DROP TABLE IF EXISTS test_ns.stats_gov", None)
        .await;
    plain
        .execute(
            &session,
            "CREATE TABLE test_ns.stats_gov AS SELECT * FROM (VALUES \
             (1, 'a', 10.0), (2, 'b', 20.0), (3, 'c', 30.0)) AS t(id, secret, amount)",
            None,
        )
        .await
        .expect("CTAS");

    let all = stats_rows(
        &plain
            .execute(&session, "SHOW STATS FOR test_ns.stats_gov", None)
            .await
            .expect("SHOW STATS without policy"),
    );
    let all_names: Vec<&str> = all.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(all_names, vec!["id", "secret", "amount", ""], "{all:?}");
    let total = all.last().and_then(|(_, c)| *c);
    assert_eq!(total, Some(3.0), "unrestricted caller sees the snapshot row count: {all:?}");

    // The same user with a policy: `secret` restricted, rows filtered.
    let store = Arc::new(InMemoryPolicyStore::new());
    store
        .add_table_policy(
            "test_ns",
            "stats_gov",
            ResolvedPolicy {
                restricted_columns: vec!["secret".to_string()],
                row_filters: vec![col("id").gt(lit(1))],
                ..Default::default()
            },
        )
        .await;
    let (session, governed) = crate::common::setup_handler_with_policy_store(store).await;
    let seen = stats_rows(
        &governed
            .execute(&session, "SHOW STATS FOR test_ns.stats_gov", None)
            .await
            .expect("SHOW STATS under policy"),
    );
    let seen_names: Vec<&str> = seen.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        seen_names,
        vec!["id", "amount", ""],
        "the restricted column must be absent, not an error: {seen:?}"
    );
    assert_eq!(
        seen.last().and_then(|(_, c)| *c),
        None,
        "a row filter hides the snapshot's total row count: {seen:?}"
    );

    // And the enforcement matches what the same caller can actually read.
    let rows = governed
        .execute(&session, "SELECT count(*) FROM test_ns.stats_gov", None)
        .await
        .expect("filtered count");
    let n = rows[0]
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>()
        .value(0);
    assert_eq!(n, 2, "row filter id > 1 leaves two rows");

    let _ = plain
        .execute(&session, "DROP TABLE IF EXISTS test_ns.stats_gov", None)
        .await;
}
