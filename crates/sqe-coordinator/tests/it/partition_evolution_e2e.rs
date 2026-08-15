//! End-to-end coverage for `ALTER TABLE ... ADD/DROP/REPLACE PARTITION FIELD`.
//!
//! Phase N adds Iceberg partition-spec evolution to the SQL surface:
//!
//! ```sql
//! ALTER TABLE ns.events ADD PARTITION FIELD year(ts)
//! ALTER TABLE ns.events DROP PARTITION FIELD region
//! ALTER TABLE ns.events REPLACE PARTITION FIELD region WITH bucket(8, region)
//! ```
//!
//! These tests create a table, evolve its partition spec, then INSERT
//! against the new spec to confirm the catalog accepted the change and
//! the writer routes records using the evolved spec.
//!
//! Stack: `docker compose -f docker-compose.test.yml up -d` then
//! `cargo test -p sqe-coordinator --test partition_evolution_e2e -- --ignored
//! --test-threads=1`.

use arrow_array::{Array, Int64Array};

async fn row_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> i64 {
    let batches = handler
        .execute(session, &format!("SELECT COUNT(*) FROM {table}"), None)
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
        .execute(session, &format!("DROP TABLE IF EXISTS {fq_table}"), None)
        .await;
    handler
        .execute(session, create_sql, None)
        .await
        .expect("CREATE TABLE");
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:partition-evolution:v2 (ADD PARTITION FIELD on identity)
//
// Start with an unpartitioned table, add an identity partition field, then
// INSERT and verify the new spec is in effect by reading the row back.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn add_identity_partition_field_to_unpartitioned_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.pe_add_identity";

    reset_table(
        &handler,
        &session,
        fq,
        &format!("CREATE TABLE {fq} (id BIGINT, region STRING, value BIGINT)"),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} ADD PARTITION FIELD region"),
            None,
        )
        .await
        .expect("ADD PARTITION FIELD region");

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (1, 'eu', 10), (2, 'us', 20)"),
            None,
        )
        .await
        .expect("INSERT after ADD PARTITION FIELD");

    let total = row_count(&handler, &session, fq).await;
    assert_eq!(total, 2, "round-trip after ADD PARTITION FIELD");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:partition-evolution:v2 (ADD PARTITION FIELD with day transform)
//
// Add a day(ts) transform. INSERT timestamps spanning two days; expect
// a successful round-trip on the new spec.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn add_day_partition_field_with_transform() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.pe_add_day";

    reset_table(
        &handler,
        &session,
        fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP, value BIGINT)"),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} ADD PARTITION FIELD day(ts)"),
            None,
        )
        .await
        .expect("ADD PARTITION FIELD day(ts)");

    handler
        .execute(
            &session,
            &format!(
                "INSERT INTO {fq} VALUES \
                 (1, TIMESTAMP '2026-04-26 10:00:00', 10), \
                 (2, TIMESTAMP '2026-04-27 11:00:00', 20)"
            ),
            None,
        )
        .await
        .expect("INSERT after ADD PARTITION FIELD day(ts)");

    let total = row_count(&handler, &session, fq).await;
    assert_eq!(total, 2, "round-trip after ADD PARTITION FIELD day(ts)");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:partition-evolution:v2 (DROP PARTITION FIELD)
//
// Start with an identity-partitioned table, drop the partition field,
// and verify INSERT works with the now-empty spec.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drop_partition_field_from_partitioned_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.pe_drop";

    reset_table(
        &handler,
        &session,
        fq,
        &format!(
            "CREATE TABLE {fq} (id BIGINT, region STRING, value BIGINT) \
             PARTITIONED BY (region)"
        ),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (1, 'eu', 10)"),
            None,
        )
        .await
        .expect("INSERT pre-DROP");

    handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} DROP PARTITION FIELD region"),
            None,
        )
        .await
        .expect("DROP PARTITION FIELD region");

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (2, 'us', 20)"),
            None,
        )
        .await
        .expect("INSERT after DROP PARTITION FIELD");

    let total = row_count(&handler, &session, fq).await;
    assert_eq!(total, 2, "rows from before+after DROP visible");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:partition-evolution:v2 (REPLACE PARTITION FIELD with bucket)
//
// Replace an identity field with bucket(8, region) on the same column.
// New writes should land under the bucketed spec; the row count includes
// rows written before the REPLACE.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn replace_partition_field_identity_to_bucket() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.pe_replace";

    reset_table(
        &handler,
        &session,
        fq,
        &format!(
            "CREATE TABLE {fq} (user_id BIGINT, region STRING, value BIGINT) \
             PARTITIONED BY (region)"
        ),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (1, 'eu', 10), (2, 'us', 20)"),
            None,
        )
        .await
        .expect("INSERT pre-REPLACE");

    handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} REPLACE PARTITION FIELD region WITH bucket(8, region)"),
            None,
        )
        .await
        .expect("REPLACE PARTITION FIELD");

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (3, 'eu', 30)"),
            None,
        )
        .await
        .expect("INSERT after REPLACE");

    let total = row_count(&handler, &session, fq).await;
    assert_eq!(total, 3, "rows from before+after REPLACE visible");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:partition-evolution:v3 (V3 + nanosec timestamp partitioning)
//
// Confirm partition evolution works against a V3 table with nanosec ts.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn add_partition_field_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.pe_v3_add";

    reset_table(
        &handler,
        &session,
        fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9), value BIGINT)"),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} ADD PARTITION FIELD day(ts)"),
            None,
        )
        .await
        .expect("ADD PARTITION FIELD on V3 table");

    handler
        .execute(
            &session,
            &format!(
                "INSERT INTO {fq} VALUES \
                 (1, TIMESTAMP '2026-04-26 10:00:00.123456789', 10), \
                 (2, TIMESTAMP '2026-04-27 11:00:00.987654321', 20)"
            ),
            None,
        )
        .await
        .expect("INSERT after ADD PARTITION FIELD on V3");

    let total = row_count(&handler, &session, fq).await;
    assert_eq!(total, 2, "V3 partition-evolution round-trip");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Negative: DROP PARTITION FIELD that does not exist returns a clear error.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drop_unknown_partition_field_returns_error() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.pe_drop_unknown";

    reset_table(
        &handler,
        &session,
        fq,
        &format!("CREATE TABLE {fq} (id BIGINT, region STRING)"),
    )
    .await;

    let err = handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} DROP PARTITION FIELD region"),
            None,
        )
        .await
        .expect_err("region is not currently a partition field");

    let msg = format!("{err}");
    assert!(
        msg.contains("no existing partition field matches"),
        "error must explain why DROP failed: {msg}"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Negative: ADD PARTITION FIELD on a column that does not exist.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn add_partition_field_on_unknown_column_returns_error() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.pe_add_unknown";

    reset_table(
        &handler,
        &session,
        fq,
        &format!("CREATE TABLE {fq} (id BIGINT)"),
    )
    .await;

    let err = handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} ADD PARTITION FIELD ghost"),
            None,
        )
        .await
        .expect_err("ghost column is not declared");

    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("ghost") && msg.to_lowercase().contains("not found"),
        "error must name the unknown column: {msg}"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}
