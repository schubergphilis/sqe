//! End-to-end coverage for hidden-partitioning via `PARTITIONED BY (...)`.
//!
//! Phase M added SQL syntax for the six standard Iceberg transforms
//! (identity, year, month, day, hour, bucket, truncate, void). These
//! tests create tables with each transform, INSERT rows, and assert
//! the table is fully usable end-to-end through Polaris.
//!
//! Stack: `docker compose -f docker-compose.test.yml up -d` then
//! `cargo test -p sqe-coordinator --test partition_e2e -- --ignored
//! --test-threads=1`.


use arrow_array::{Array, Int64Array};

async fn row_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> i64 {
    let batches = handler
        .execute(session, &format!("SELECT COUNT(*) FROM {table}"))
        .await
        .expect("count select");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0)
}

async fn reset_table(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    fq_table: &str,
    create_sql: &str,
) {
    let _ = handler
        .execute(session, &format!("DROP TABLE IF EXISTS {fq_table}"))
        .await;
    handler
        .execute(session, create_sql)
        .await
        .expect("CREATE TABLE");
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:hidden-partitioning:v2 (identity transform on a string column)
//
// PARTITIONED BY (region) creates one identity partition. INSERT then
// SELECT rounds back through Polaris, proving the spec was accepted by
// the catalog and the writer routes records into the right partition.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn create_table_partitioned_by_identity() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "part_identity";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!(
            "CREATE TABLE {fq} (id BIGINT, region STRING, value BIGINT) \
             PARTITIONED BY (region)"
        ),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (1, 'eu', 10), (2, 'us', 20)"),
        )
        .await
        .expect("INSERT");

    let total = row_count(&handler, &session, &fq).await;
    assert_eq!(total, 2, "identity-partitioned table round-trips");

    let eu_count = row_count(
        &handler,
        &session,
        &format!("(SELECT * FROM {fq} WHERE region = 'eu')"),
    )
    .await;
    assert_eq!(eu_count, 1, "predicate on partition column returns 1 row");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"))
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:hidden-partitioning:v2 (day transform on TIMESTAMP)
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn create_table_partitioned_by_day_transform() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "part_day";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!(
            "CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP, value BIGINT) \
             PARTITIONED BY (day(ts))"
        ),
    )
    .await;

    handler
        .execute(
            &session,
            &format!(
                "INSERT INTO {fq} VALUES \
                 (1, TIMESTAMP '2026-04-26 10:00:00', 10), \
                 (2, TIMESTAMP '2026-04-27 11:00:00', 20), \
                 (3, TIMESTAMP '2026-04-26 23:30:00', 30)"
            ),
        )
        .await
        .expect("INSERT");

    let total = row_count(&handler, &session, &fq).await;
    assert_eq!(total, 3, "day-partitioned INSERT round-trip");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"))
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:hidden-partitioning:v2 (bucket transform with N=16)
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn create_table_partitioned_by_bucket_transform() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "part_bucket";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!(
            "CREATE TABLE {fq} (user_id BIGINT, event STRING) \
             PARTITIONED BY (bucket(16, user_id))"
        ),
    )
    .await;

    handler
        .execute(
            &session,
            &format!(
                "INSERT INTO {fq} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
            ),
        )
        .await
        .expect("INSERT");

    let total = row_count(&handler, &session, &fq).await;
    assert_eq!(total, 4, "bucket-partitioned INSERT round-trip");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"))
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:hidden-partitioning:v2 (truncate transform on STRING)
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn create_table_partitioned_by_truncate_transform() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "part_truncate";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!(
            "CREATE TABLE {fq} (name STRING, count BIGINT) \
             PARTITIONED BY (truncate(4, name))"
        ),
    )
    .await;

    handler
        .execute(
            &session,
            &format!(
                "INSERT INTO {fq} VALUES \
                 ('alpha', 1), ('alphabet', 2), ('beta', 3)"
            ),
        )
        .await
        .expect("INSERT");

    let total = row_count(&handler, &session, &fq).await;
    assert_eq!(total, 3, "truncate-partitioned INSERT round-trip");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"))
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:hidden-partitioning:v2 (composite multi-column spec)
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn create_table_partitioned_by_multiple_transforms() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "part_multi";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!(
            "CREATE TABLE {fq} (user_id BIGINT, ts TIMESTAMP, region STRING) \
             PARTITIONED BY (day(ts), bucket(8, user_id), region)"
        ),
    )
    .await;

    handler
        .execute(
            &session,
            &format!(
                "INSERT INTO {fq} VALUES \
                 (1, TIMESTAMP '2026-04-26 10:00:00', 'eu'), \
                 (2, TIMESTAMP '2026-04-26 11:00:00', 'us'), \
                 (3, TIMESTAMP '2026-04-27 12:00:00', 'eu')"
            ),
        )
        .await
        .expect("INSERT");

    let total = row_count(&handler, &session, &fq).await;
    assert_eq!(total, 3, "3-field composite partition spec round-trips");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"))
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:hidden-partitioning:v3 (V3 + nanosec timestamp partitioning)
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn create_table_partitioned_by_day_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "part_v3_day";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!(
            "CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9), value BIGINT) \
             PARTITIONED BY (day(ts))"
        ),
    )
    .await;

    handler
        .execute(
            &session,
            &format!(
                "INSERT INTO {fq} VALUES \
                 (1, TIMESTAMP '2026-04-26 10:00:00.123456789', 10), \
                 (2, TIMESTAMP '2026-04-27 11:00:00.987654321', 20)"
            ),
        )
        .await
        .expect("INSERT");

    let total = row_count(&handler, &session, &fq).await;
    assert_eq!(total, 2, "V3 partitioned-by-day INSERT round-trip");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"))
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Negative: unsupported transform raises a clear error pointing at the
// supported list.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn unsupported_partition_transform_returns_error() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "part_bad";
    let fq = format!("{ns}.{name}");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"))
        .await;

    let err = handler
        .execute(
            &session,
            &format!(
                "CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP) \
                 PARTITIONED BY (random(ts))"
            ),
        )
        .await
        .expect_err("random() is not a valid Iceberg transform");

    let msg = format!("{err}");
    assert!(
        msg.contains("PARTITIONED BY") && msg.to_lowercase().contains("random"),
        "error must name the unsupported transform clearly: {msg}"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Negative: PARTITIONED BY column that doesn't exist in the schema.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn unknown_partition_column_returns_error() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "part_unknown";
    let fq = format!("{ns}.{name}");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"))
        .await;

    let err = handler
        .execute(
            &session,
            &format!(
                "CREATE TABLE {fq} (id BIGINT) PARTITIONED BY (region)"
            ),
        )
        .await
        .expect_err("region column is not declared in the schema");

    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("region") && msg.to_lowercase().contains("unknown"),
        "error must name the unknown column: {msg}"
    );
}
