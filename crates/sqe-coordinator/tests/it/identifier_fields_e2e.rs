//! `ALTER TABLE ... SET | DROP IDENTIFIER FIELDS` against the live stack.
//!
//! Identifier fields decide how a merge-on-read UPDATE is written: with
//! them it writes equality deletes, without them it falls back to
//! copy-on-write. The snapshot summary shows which one ran, so these tests
//! assert the DDL through that behaviour.

use crate::v3_e2e::latest_summary;

async fn reset(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, fq: &str) {
    let _ = handler
        .execute(session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    let create = format!(
        "CREATE TABLE {fq} (id BIGINT NOT NULL, v BIGINT, d DOUBLE NOT NULL) \
         TBLPROPERTIES ('write.update.mode' = 'merge-on-read', \
         'write.delete.mode' = 'merge-on-read')"
    );
    handler
        .execute(session, &create, None)
        .await
        .expect("CREATE TABLE");
    let insert = format!("INSERT INTO {fq} VALUES (1, 10, 1.0), (2, 20, 2.0), (3, 30, 3.0)");
    handler
        .execute(session, &insert, None)
        .await
        .expect("INSERT");
}

async fn exec(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, sql: &str) {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// UPDATE `id = 1` and report whether the commit wrote equality deletes.
async fn update_writes_equality_deletes(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    name: &str,
    v: i64,
) -> bool {
    exec(
        handler,
        session,
        &format!("UPDATE default.{name} SET v = {v} WHERE id = 1"),
    )
    .await;
    let summary = latest_summary(handler, session, "default", name).await;
    summary.contains("added-equality-delete-files")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn identifier_fields_set_drop_switch_update_mode() {
    let (session, handler) = crate::common::setup_handler().await;
    let name = "identifier_fields_set_drop";
    let fq = format!("default.{name}");
    reset(&handler, &session, &fq).await;

    assert!(
        !update_writes_equality_deletes(&handler, &session, name, 11).await,
        "no identifier fields: UPDATE falls back to copy-on-write"
    );

    exec(
        &handler,
        &session,
        &format!("ALTER TABLE {fq} SET IDENTIFIER FIELDS id"),
    )
    .await;
    assert!(
        update_writes_equality_deletes(&handler, &session, name, 12).await,
        "SET IDENTIFIER FIELDS id: MoR UPDATE writes equality deletes"
    );

    exec(
        &handler,
        &session,
        &format!("ALTER TABLE {fq} DROP IDENTIFIER FIELDS id"),
    )
    .await;
    assert!(
        !update_writes_equality_deletes(&handler, &session, name, 13).await,
        "DROP IDENTIFIER FIELDS id: back to copy-on-write"
    );

    // The schema with `id` as identifier already exists: the catalog reuses
    // it, so this SET selects it by id instead of adding a new one.
    exec(
        &handler,
        &session,
        &format!("ALTER TABLE {fq} SET IDENTIFIER FIELDS (id)"),
    )
    .await;
    // Setting the current set again is a no-op, not an error.
    exec(
        &handler,
        &session,
        &format!("ALTER TABLE {fq} SET IDENTIFIER FIELDS ID"),
    )
    .await;
    assert!(
        update_writes_equality_deletes(&handler, &session, name, 14).await,
        "SET again after DROP restores equality deletes"
    );

    let batches = handler
        .execute(
            &session,
            &format!("SELECT id, v FROM {fq} ORDER BY id"),
            None,
        )
        .await
        .expect("select");
    let rows = arrow::util::pretty::pretty_format_batches(&batches)
        .expect("format")
        .to_string();
    for row in ["| 1  | 14 |", "| 2  | 20 |", "| 3  | 30 |"] {
        assert!(rows.contains(row), "missing {row} in\n{rows}");
    }

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn identifier_fields_invalid_columns_are_refused() {
    let (session, handler) = crate::common::setup_handler().await;
    let name = "identifier_fields_invalid";
    let fq = format!("default.{name}");
    reset(&handler, &session, &fq).await;

    for (cols, why) in [
        ("v", "nullable column"),
        ("d", "double column"),
        ("nope", "unknown column"),
    ] {
        let sql = format!("ALTER TABLE {fq} SET IDENTIFIER FIELDS {cols}");
        let err = handler.execute(&session, &sql, None).await;
        assert!(err.is_err(), "{why}: {sql} must fail");
    }
    let sql = format!("ALTER TABLE {fq} DROP IDENTIFIER FIELDS id");
    let err = handler
        .execute(&session, &sql, None)
        .await
        .expect_err("DROP of a non-identifier column must fail");
    assert!(
        err.to_string().contains("is not an identifier field"),
        "unexpected error: {err}"
    );

    // A refused statement leaves the table without identifier fields.
    assert!(
        !update_writes_equality_deletes(&handler, &session, name, 15).await,
        "refused DDL must not change the identifier set"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}
