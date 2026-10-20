//! SQE-written merge-on-read deletes and V3 row lineage, read back by
//! Spark 4.1 with Iceberg 1.12 through the shared Polaris catalog (#853).
//! Each test asserts the snapshot summary first, so a copy-on-write
//! fallback cannot pass for merge-on-read. Needs the integration stack and
//! the Spark container (`./scripts/start-spark-interop.sh`); without the
//! container the tests skip with the reason.

use crate::common::spark_interop::{spark_sql, spark_stack_skip_reason};
use crate::v3_e2e::{latest_summary, lineage_by_id};
use crate::variant_spark_interop::{spark_rows, sqe_rows};

type Handler = sqe_coordinator::QueryHandler;
type Session = sqe_core::Session;

/// Spark creates `default.<name>` with `id` as the identifier field and
/// every write mode merge-on-read. SQE has no DDL for identifier fields,
/// and without one UPDATE and MERGE fall back to copy-on-write.
fn spark_create_keyed_table(name: &str, format_version: u8) {
    let sql = format!(
        "DROP TABLE IF EXISTS rest.default.{name}; \
         CREATE TABLE rest.default.{name} (id BIGINT NOT NULL, v BIGINT) USING iceberg \
         TBLPROPERTIES ('format-version' = '{format_version}', \
         'write.delete.mode' = 'merge-on-read', \
         'write.update.mode' = 'merge-on-read', \
         'write.merge.mode' = 'merge-on-read'); \
         ALTER TABLE rest.default.{name} SET IDENTIFIER FIELDS id"
    );
    spark_sql(&sql).unwrap_or_else(|e| panic!("spark: {sql}: {e}"));
}

async fn run(handler: &Handler, session: &Session, sql: &str) {
    handler.execute(session, sql, None).await.expect(sql);
}

async fn drop_table(handler: &Handler, session: &Session, name: &str) {
    let sql = format!("DROP TABLE IF EXISTS default.{name}");
    let _ = handler.execute(session, &sql, None).await;
}

async fn insert_three_rows(handler: &Handler, session: &Session, name: &str) {
    let sql = format!("INSERT INTO default.{name} VALUES (1, 10), (2, 20), (3, 30)");
    run(handler, session, &sql).await;
}

/// The rows Spark and SQE return for `SELECT id, v ... ORDER BY id`.
async fn both_engines(
    handler: &Handler,
    session: &Session,
    name: &str,
) -> (Vec<String>, Vec<String>) {
    let spark = spark_rows(&format!(
        "SELECT id, v FROM rest.default.{name} ORDER BY id"
    ));
    let sql = format!("SELECT id, v FROM default.{name} ORDER BY id");
    (spark, sqe_rows(handler, session, &sql).await)
}

fn skip(test: &str) -> bool {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping {test}: {reason}");
        return true;
    }
    false
}

/// MoR DELETE twice on the same data file: V3 writes a deletion vector and
/// then replaces it, V2 writes position-delete files.
async fn mor_delete_case(name: &str, format_version: u8) {
    let (session, handler) = crate::common::setup_handler().await;
    drop_table(&handler, &session, name).await;
    let create = format!(
        "CREATE TABLE default.{name} (id BIGINT, v BIGINT) \
         TBLPROPERTIES ('format-version' = '{format_version}', \
         'write.delete.mode' = 'merge-on-read')"
    );
    run(&handler, &session, &create).await;
    insert_three_rows(&handler, &session, name).await;

    run(
        &handler,
        &session,
        &format!("DELETE FROM default.{name} WHERE id = 2"),
    )
    .await;
    let summary = latest_summary(&handler, &session, "default", name).await;
    if format_version >= 3 {
        assert!(summary.contains("\"added-dvs\":\"1\""), "one DV: {summary}");
    } else {
        assert!(
            summary.contains("added-position-delete-files"),
            "position deletes: {summary}"
        );
    }
    run(
        &handler,
        &session,
        &format!("DELETE FROM default.{name} WHERE id = 3"),
    )
    .await;
    let summary = latest_summary(&handler, &session, "default", name).await;
    if format_version >= 3 {
        assert!(
            summary.contains("\"removed-dvs\":\"1\""),
            "old DV retired: {summary}"
        );
    }
    assert!(
        !summary.contains("deleted-data-files"),
        "MoR rewrote a data file: {summary}"
    );

    let (spark, sqe) = both_engines(&handler, &session, name).await;
    drop_table(&handler, &session, name).await;
    assert_eq!(spark, vec!["1|10"], "Spark's rows after SQE's MoR DELETE");
    assert_eq!(sqe, spark, "SQE and Spark disagree");
}

/// MoR DELETE on a keyed table writes an equality delete instead of a
/// position delete or deletion vector.
async fn mor_keyed_delete_case(name: &str, format_version: u8) {
    let (session, handler) = crate::common::setup_handler().await;
    spark_create_keyed_table(name, format_version);
    insert_three_rows(&handler, &session, name).await;
    run(
        &handler,
        &session,
        &format!("DELETE FROM default.{name} WHERE id = 2"),
    )
    .await;
    let summary = latest_summary(&handler, &session, "default", name).await;
    assert!(
        summary.contains("added-equality-delete-files"),
        "keyed DELETE must write an equality delete: {summary}"
    );

    let (spark, sqe) = both_engines(&handler, &session, name).await;
    drop_table(&handler, &session, name).await;
    assert_eq!(
        spark,
        vec!["1|10", "3|30"],
        "Spark's rows after SQE's keyed DELETE"
    );
    assert_eq!(sqe, spark, "SQE and Spark disagree");
}

/// MoR UPDATE on a keyed table writes a data file and an equality delete
/// in one commit; Spark applies the equality delete.
async fn mor_update_case(name: &str, format_version: u8) {
    let (session, handler) = crate::common::setup_handler().await;
    spark_create_keyed_table(name, format_version);
    insert_three_rows(&handler, &session, name).await;
    run(
        &handler,
        &session,
        &format!("UPDATE default.{name} SET v = 99 WHERE id = 2"),
    )
    .await;
    let summary = latest_summary(&handler, &session, "default", name).await;
    assert!(
        summary.contains("added-equality-delete-files"),
        "UPDATE must be merge-on-read, not copy-on-write: {summary}"
    );

    let (spark, sqe) = both_engines(&handler, &session, name).await;
    drop_table(&handler, &session, name).await;
    assert_eq!(
        spark,
        vec!["1|10", "2|99", "3|30"],
        "Spark's rows after SQE's UPDATE"
    );
    assert_eq!(sqe, spark, "SQE and Spark disagree");
}

/// MoR MERGE with a matched DELETE, a matched UPDATE and a not-matched
/// INSERT commits one RowDelta; Spark sees the merged rows.
async fn mor_merge_case(name: &str, format_version: u8) {
    let (session, handler) = crate::common::setup_handler().await;
    spark_create_keyed_table(name, format_version);
    insert_three_rows(&handler, &session, name).await;
    let merge = format!(
        "MERGE INTO default.{name} t \
         USING (SELECT 1 AS id, 11 AS v UNION ALL SELECT 2, 0 UNION ALL SELECT 4, 40) s \
         ON t.id = s.id \
         WHEN MATCHED AND s.v = 0 THEN DELETE \
         WHEN MATCHED THEN UPDATE SET v = s.v \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    );
    run(&handler, &session, &merge).await;
    let summary = latest_summary(&handler, &session, "default", name).await;
    assert!(
        summary.contains("added-equality-delete-files"),
        "MERGE must be merge-on-read, not copy-on-write: {summary}"
    );

    let (spark, sqe) = both_engines(&handler, &session, name).await;
    drop_table(&handler, &session, name).await;
    assert_eq!(
        spark,
        vec!["1|11", "3|30", "4|40"],
        "Spark's rows after SQE's MERGE"
    );
    assert_eq!(sqe, spark, "SQE and Spark disagree");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn mor_spark_interop_reads_sqe_v3_deletion_vectors() {
    if !skip("mor_spark_interop_reads_sqe_v3_deletion_vectors") {
        mor_delete_case("mor_si_v3_dv", 3).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn mor_spark_interop_reads_sqe_v2_position_deletes() {
    if !skip("mor_spark_interop_reads_sqe_v2_position_deletes") {
        mor_delete_case("mor_si_v2_pos", 2).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn mor_spark_interop_reads_sqe_v3_equality_delete() {
    if !skip("mor_spark_interop_reads_sqe_v3_equality_delete") {
        mor_keyed_delete_case("mor_si_v3_eq_delete", 3).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn mor_spark_interop_reads_sqe_v2_equality_delete() {
    if !skip("mor_spark_interop_reads_sqe_v2_equality_delete") {
        mor_keyed_delete_case("mor_si_v2_eq_delete", 2).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn mor_spark_interop_reads_sqe_v3_equality_delete_update() {
    if !skip("mor_spark_interop_reads_sqe_v3_equality_delete_update") {
        mor_update_case("mor_si_v3_update", 3).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn mor_spark_interop_reads_sqe_v2_equality_delete_update() {
    if !skip("mor_spark_interop_reads_sqe_v2_equality_delete_update") {
        mor_update_case("mor_si_v2_update", 2).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn mor_spark_interop_reads_sqe_v3_merge() {
    if !skip("mor_spark_interop_reads_sqe_v3_merge") {
        mor_merge_case("mor_si_v3_merge", 3).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn mor_spark_interop_reads_sqe_v2_merge() {
    if !skip("mor_spark_interop_reads_sqe_v2_merge") {
        mor_merge_case("mor_si_v2_merge", 2).await;
    }
}

/// Row lineage written by SQE (insert, merge-on-read UPDATE and MERGE):
/// updated rows keep `_row_id` and get a newer sequence number, untouched
/// rows keep both, and Spark reads the same `_row_id` and
/// `_last_updated_sequence_number`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn mor_spark_interop_reads_sqe_v3_row_lineage() {
    if skip("mor_spark_interop_reads_sqe_v3_row_lineage") {
        return;
    }
    let (session, handler) = crate::common::setup_handler().await;
    let name = "mor_si_v3_lineage";
    spark_create_keyed_table(name, 3);
    insert_three_rows(&handler, &session, name).await;
    let start = lineage_by_id(&handler, &session, "default", name).await;

    let update = format!("UPDATE default.{name} SET v = 99 WHERE id = 2");
    run(&handler, &session, &update).await;
    let summary = latest_summary(&handler, &session, "default", name).await;
    assert!(
        summary.contains("added-equality-delete-files"),
        "MoR UPDATE: {summary}"
    );
    let after_update = lineage_by_id(&handler, &session, "default", name).await;
    assert_eq!(
        after_update[&2].0, start[&2].0,
        "MoR UPDATE must keep _row_id"
    );
    assert!(
        after_update[&2].1 > start[&2].1,
        "MoR UPDATE needs a newer sequence number"
    );
    assert_eq!(after_update[&1], start[&1]);

    let merge = format!(
        "MERGE INTO default.{name} t USING (SELECT 3 AS id, 33 AS v) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET v = s.v"
    );
    run(&handler, &session, &merge).await;
    let summary = latest_summary(&handler, &session, "default", name).await;
    assert!(
        summary.contains("added-equality-delete-files"),
        "MoR MERGE: {summary}"
    );
    let after_merge = lineage_by_id(&handler, &session, "default", name).await;
    assert_eq!(
        after_merge[&3].0, start[&3].0,
        "MoR MERGE must keep _row_id"
    );
    assert!(
        after_merge[&3].1 > start[&3].1,
        "MoR MERGE needs a newer sequence number"
    );
    assert_eq!(after_merge[&1], start[&1]);
    assert_eq!(after_merge[&2], after_update[&2]);

    let sqe: Vec<String> = after_merge
        .into_iter()
        .map(|(id, (row_id, seq))| format!("{id}|{row_id}|{seq}"))
        .collect();
    let spark = spark_rows(&format!(
        "SELECT id, _row_id, _last_updated_sequence_number \
         FROM rest.default.{name} ORDER BY id"
    ));
    drop_table(&handler, &session, name).await;
    assert_eq!(sqe.len(), 3, "three live rows: {sqe:?}");
    assert_eq!(spark, sqe, "Spark's lineage differs from SQE's");
}
