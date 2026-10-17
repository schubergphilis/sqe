//! Iceberg V3 `VARIANT` columns against the live Polaris stack (#849).
//!
//! A `VARIANT` column makes the table format-version 3, takes JSON text on
//! INSERT, and comes back as variant storage that renders as JSON. CTAS keeps
//! the type, and `ALTER TABLE ADD COLUMN ... VARIANT` needs a v3 table.

use arrow::array::AsArray;
use arrow_array::Array;

/// Every row of `sql` as `col|col|...`, a variant cell as its JSON text and
/// NULL as `NULL`.
async fn rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<String> {
    let batches = handler.execute(session, sql, None).await.expect(sql);
    let mut out = Vec::new();
    for batch in &batches {
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| {
                if sqe_core::variant::is_variant_storage(c.data_type()) {
                    sqe_core::variant::to_json(c.as_ref()).expect("variant to JSON")
                } else {
                    c.clone()
                }
            })
            .collect();
        for r in 0..batch.num_rows() {
            let cells: Vec<String> = columns
                .iter()
                .map(|c| {
                    if c.is_null(r) {
                        "NULL".to_string()
                    } else {
                        arrow::util::display::array_value_to_string(c, r).expect("render")
                    }
                })
                .collect();
            out.push(cells.join("|"));
        }
    }
    out
}

async fn show_create(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> String {
    let sql = format!("SHOW CREATE TABLE {table}");
    let batches = handler.execute(session, &sql, None).await.expect(&sql);
    batches[0].column(0).as_string::<i32>().value(0).to_string()
}

async fn drop(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, table: &str) {
    let _ = handler
        .execute(session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn variant_column_stores_json_and_reads_it_back() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.variant_round_trip";
    drop(&handler, &session, fq).await;
    handler
        .execute(
            &session,
            &format!("CREATE TABLE {fq} (id INT, v VARIANT)"),
            None,
        )
        .await
        .expect("CREATE TABLE with VARIANT");
    handler
        .execute(
            &session,
            &format!(
                r#"INSERT INTO {fq} VALUES (1, '{{"a": 1, "b": [true, null, "x"]}}'),
                   (2, NULL), (3, '42'), (4, '"text"'), (5, 'null')"#
            ),
            None,
        )
        .await
        .expect("INSERT JSON text into VARIANT");

    assert_eq!(
        rows(
            &handler,
            &session,
            &format!("SELECT id, v FROM {fq} ORDER BY id")
        )
        .await,
        vec![
            r#"1|{"a":1,"b":[true,null,"x"]}"#,
            "2|NULL",
            "3|42",
            r#"4|"text""#,
            "5|null",
        ]
    );
    let ddl = show_create(&handler, &session, fq).await;
    assert!(ddl.contains("v VARIANT"), "SHOW CREATE TABLE {fq}: {ddl}");

    // Invalid JSON fails the write; it is never stored as NULL.
    let err = handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (6, '{{oops')"),
            None,
        )
        .await
        .expect_err("invalid JSON must fail the INSERT");
    assert!(format!("{err}").contains("as VARIANT"), "err: {err}");
    assert_eq!(
        rows(&handler, &session, &format!("SELECT count(*) FROM {fq}")).await,
        vec!["5"]
    );

    drop(&handler, &session, fq).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn variant_survives_ctas_and_insert_select() {
    let (session, handler) = crate::common::setup_handler().await;
    let src = "default.variant_src";
    let dst = "default.variant_ctas";
    drop(&handler, &session, src).await;
    drop(&handler, &session, dst).await;
    handler
        .execute(
            &session,
            &format!("CREATE TABLE {src} (id INT, v VARIANT)"),
            None,
        )
        .await
        .expect("CREATE TABLE");
    handler
        .execute(
            &session,
            &format!(r#"INSERT INTO {src} VALUES (1, '{{"k": [1, 2]}}')"#),
            None,
        )
        .await
        .expect("INSERT");

    handler
        .execute(
            &session,
            &format!("CREATE TABLE {dst} AS SELECT id, v FROM {src}"),
            None,
        )
        .await
        .expect("CTAS from a VARIANT column");
    handler
        .execute(
            &session,
            &format!("INSERT INTO {dst} SELECT id + 1, v FROM {src}"),
            None,
        )
        .await
        .expect("INSERT SELECT of a VARIANT column");

    // ADD COLUMN VARIANT is refused on a v2 table, so it succeeding here
    // shows the CTAS table is v3.
    handler
        .execute(
            &session,
            &format!("ALTER TABLE {dst} ADD COLUMN w VARIANT"),
            None,
        )
        .await
        .expect("ADD COLUMN VARIANT on the v3 CTAS table");
    handler
        .execute(
            &session,
            &format!("INSERT INTO {dst} VALUES (3, '[]', '{{}}')"),
            None,
        )
        .await
        .expect("INSERT into the added column");

    assert_eq!(
        rows(
            &handler,
            &session,
            &format!("SELECT id, v, w FROM {dst} ORDER BY id")
        )
        .await,
        vec![r#"1|{"k":[1,2]}|NULL"#, r#"2|{"k":[1,2]}|NULL"#, "3|[]|{}",]
    );
    // After the SELECT: SHOW CREATE TABLE on a CTAS table lists no columns
    // until a query has read it, variant or not (#850).
    let ddl = show_create(&handler, &session, dst).await;
    assert!(
        ddl.contains("v VARIANT") && ddl.contains("w VARIANT"),
        "SHOW CREATE TABLE {dst}: {ddl}"
    );

    drop(&handler, &session, src).await;
    drop(&handler, &session, dst).await;
}

/// The documented way to convert a JSON-as-text column: Iceberg refuses
/// evolving `string` to `variant`, so the text is copied into a VARIANT table.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn json_text_column_converts_by_insert_select() {
    let (session, handler) = crate::common::setup_handler().await;
    let text = "default.variant_from_text";
    let dst = "default.variant_converted";
    drop(&handler, &session, text).await;
    drop(&handler, &session, dst).await;
    for sql in [
        format!("CREATE TABLE {text} (id INT, doc VARCHAR)"),
        format!(r#"INSERT INTO {text} VALUES (1, '{{"n": 1.5}}'), (2, NULL)"#),
        format!("CREATE TABLE {dst} (id INT, doc VARIANT)"),
        format!("INSERT INTO {dst} SELECT id, doc FROM {text}"),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    assert_eq!(
        rows(
            &handler,
            &session,
            &format!("SELECT id, doc FROM {dst} ORDER BY id")
        )
        .await,
        vec![r#"1|{"n":1.5}"#, "2|NULL"]
    );
    drop(&handler, &session, text).await;
    drop(&handler, &session, dst).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn add_variant_column_to_v2_table_is_refused() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.variant_on_v2";
    drop(&handler, &session, fq).await;
    handler
        .execute(
            &session,
            &format!("CREATE TABLE {fq} (id INT) WITH ('format-version' = '2')"),
            None,
        )
        .await
        .expect("CREATE TABLE");
    let err = handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} ADD COLUMN v VARIANT"),
            None,
        )
        .await
        .expect_err("a v2 table cannot hold a variant");
    assert!(
        format!("{err}").contains("variant needs format v3"),
        "err: {err}"
    );
    drop(&handler, &session, fq).await;
}

/// On a format v3 table `JSON` is stored as a variant, also when added
/// later; on a v2 table it stays a string column.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn json_column_is_a_variant_on_a_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let v3 = "default.json_on_v3";
    let v2 = "default.json_on_v2";
    drop(&handler, &session, v3).await;
    drop(&handler, &session, v2).await;
    for sql in [
        format!("CREATE TABLE {v3} (id INT, j JSON) WITH ('format-version' = '3')"),
        format!("ALTER TABLE {v3} ADD COLUMN k JSON"),
        format!(r#"INSERT INTO {v3} VALUES (1, '{{"a": {{"b": 2}}}}', '[1]'), (2, NULL, 'true')"#),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    assert_eq!(
        rows(
            &handler,
            &session,
            &format!("SELECT id, j, k FROM {v3} ORDER BY id")
        )
        .await,
        vec![r#"1|{"a":{"b":2}}|[1]"#, "2|NULL|true"]
    );
    let ddl = show_create(&handler, &session, v3).await;
    assert!(
        ddl.contains("j VARIANT") && ddl.contains("k VARIANT"),
        "SHOW CREATE TABLE {v3}: {ddl}"
    );
    // A variant takes only JSON.
    let err = handler
        .execute(
            &session,
            &format!("INSERT INTO {v3} VALUES (3, 'oops', NULL)"),
            None,
        )
        .await
        .expect_err("invalid JSON must fail on a v3 JSON column");
    assert!(format!("{err}").contains("as VARIANT"), "err: {err}");

    for sql in [
        format!("CREATE TABLE {v2} (id INT, j JSON) WITH ('format-version' = '2')"),
        format!("INSERT INTO {v2} VALUES (1, 'not json at all')"),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    assert_eq!(
        rows(&handler, &session, &format!("SELECT j FROM {v2}")).await,
        vec!["not json at all"]
    );

    drop(&handler, &session, v3).await;
    drop(&handler, &session, v2).await;
}

/// The JSON functions take a variant column of a v3 table as they take
/// JSON text.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn json_functions_read_a_v3_json_column() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.json_fns_on_v3";
    drop(&handler, &session, fq).await;
    for sql in [
        format!("CREATE TABLE {fq} (id INT, j JSON) WITH ('format-version' = '3')"),
        format!(r#"INSERT INTO {fq} VALUES (1, '{{"a": {{"b": 2}}, "s": "x"}}')"#),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    assert_eq!(
        rows(
            &handler,
            &session,
            &format!(
                "SELECT json_extract(j, '$.a'), json_extract_scalar(j, '$.s'), \
                 json_get_int(j, 'a', 'b') FROM {fq}"
            )
        )
        .await,
        vec![r#"{"b":2}|x|2"#]
    );
    drop(&handler, &session, fq).await;
}
