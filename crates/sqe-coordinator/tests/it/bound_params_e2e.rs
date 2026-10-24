//! Bound parameters reach the plan as typed values, never as SQL text
//! (finding 406, portal request §1).
//!
//! Every statement here runs through `execute_bound_with_id`, the path both
//! Flight SQL prepared statements and Trino `EXECUTE ... USING` take. The
//! region value is an injection string: if any path spliced it into SQL it
//! would break the statement or leak into the query log.

use datafusion::scalar::ScalarValue;

const SENTINEL: &str = "it's'; DROP TABLE default.bound_param_t; --";

async fn exec(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, sql: &str) {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Run `sql` with `params` bound; return the batches and the query id.
async fn bound(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
    params: Vec<ScalarValue>,
) -> (Vec<arrow_array::RecordBatch>, uuid::Uuid) {
    let id = uuid::Uuid::new_v4();
    let batches = handler
        .execute_bound_with_id(session, sql, params, None, Some(id))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    (batches, id)
}

fn utf8(s: &str) -> ScalarValue {
    ScalarValue::Utf8(Some(s.to_string()))
}

fn int(v: i64) -> ScalarValue {
    ScalarValue::Int64(Some(v))
}

/// `id|region|value` rows of `t`, ordered by id.
async fn rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    t: &str,
) -> Vec<String> {
    let sql = format!("SELECT id, region, value FROM {t} ORDER BY id");
    let batches = handler.execute(session, &sql, None).await.expect("select");
    let mut out = Vec::new();
    for b in &batches {
        for r in 0..b.num_rows() {
            let cells: Vec<String> = (0..3)
                .map(|c| crate::common::fmt_val(b.column(c).as_ref(), r))
                .collect();
            out.push(cells.join("|"));
        }
    }
    out
}

/// The query log holds the template with its markers, never a value.
fn assert_logged_as_template(handler: &sqe_coordinator::QueryHandler, id: &uuid::Uuid) {
    let record = handler
        .query_tracker()
        .get(id)
        .unwrap_or_else(|| panic!("query {id} not tracked"));
    assert!(record.sql.contains("$1"), "logged sql: {}", record.sql);
    assert!(
        !record.sql.contains(SENTINEL),
        "value logged: {}",
        record.sql
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn bound_params_drive_every_write_path() {
    let (session, handler) = crate::common::setup_handler().await;
    let t = "default.bound_param_t";
    let src = "default.bound_param_src";
    let ctas = "default.bound_param_ctas";
    for fq in [t, src, ctas] {
        exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    }
    let create = format!(
        "CREATE TABLE {t} (id BIGINT, region STRING, value BIGINT) PARTITIONED BY (region)"
    );
    exec(&handler, &session, &create).await;
    let create_src = format!("CREATE TABLE {src} (id BIGINT, region STRING, value BIGINT)");
    exec(&handler, &session, &create_src).await;

    let mut ids = Vec::new();

    // VALUES: a JDBC driver sends "10" as a string for a BIGINT column.
    let sql = format!("INSERT INTO {src} VALUES ($1, $2, $3), ($4, $5, $6)");
    let params = vec![
        utf8("10"),
        utf8(SENTINEL),
        int(100),
        int(11),
        utf8("EU"),
        int(110),
    ];
    ids.push(bound(&handler, &session, &sql, params).await.1);

    // INSERT ... SELECT filtered on the injection string itself.
    let sql = format!("INSERT INTO {t} SELECT * FROM {src} WHERE region = $1");
    ids.push(
        bound(&handler, &session, &sql, vec![utf8(SENTINEL)])
            .await
            .1,
    );
    let sql = format!("INSERT INTO {t} VALUES ($1, $2, $3), ($4, $5, $6)");
    let params = vec![int(20), utf8("EU"), int(200), int(30), utf8("US"), int(300)];
    ids.push(bound(&handler, &session, &sql, params).await.1);

    let sql = format!("UPDATE {t} SET value = $1 WHERE id = $2");
    ids.push(
        bound(&handler, &session, &sql, vec![int(201), int(20)])
            .await
            .1,
    );

    // A DELETE on the partition column prunes on the bound value.
    let sql = format!("DELETE FROM {t} WHERE region = $1");
    ids.push(bound(&handler, &session, &sql, vec![utf8("US")]).await.1);

    let sql = format!(
        "MERGE INTO {t} AS t USING {src} AS s ON t.id = s.id AND t.value < $1 \
         WHEN MATCHED THEN UPDATE SET value = $2 \
         WHEN NOT MATCHED THEN INSERT (id, region, value) VALUES (s.id, s.region, $3)"
    );
    ids.push(
        bound(&handler, &session, &sql, vec![int(1000), int(999), int(7)])
            .await
            .1,
    );

    assert_eq!(
        rows(&handler, &session, t).await,
        vec![
            format!("10|{SENTINEL}|999"),
            "11|EU|7".to_string(),
            "20|EU|201".to_string(),
        ]
    );

    let sql = format!("CREATE TABLE {ctas} AS SELECT * FROM {t} WHERE value > $1");
    ids.push(bound(&handler, &session, &sql, vec![int(100)]).await.1);
    assert_eq!(
        rows(&handler, &session, ctas).await,
        vec![format!("10|{SENTINEL}|999"), "20|EU|201".to_string()]
    );

    let sql = format!("SELECT id, region, value FROM {t} WHERE region = $1");
    let (batches, id) = bound(&handler, &session, &sql, vec![utf8(SENTINEL)]).await;
    ids.push(id);
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "only the row holding the injection string");

    for id in &ids {
        assert_logged_as_template(&handler, id);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn bound_param_of_the_wrong_type_is_refused_without_echoing_it() {
    let (session, handler) = crate::common::setup_handler().await;
    let t = "default.bound_param_refuse";
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {t}")).await;
    let create = format!("CREATE TABLE {t} (id BIGINT, region STRING, value BIGINT)");
    exec(&handler, &session, &create).await;
    exec(
        &handler,
        &session,
        &format!("INSERT INTO {t} VALUES (1, 'EU', 10)"),
    )
    .await;

    let secret = "secret-value-xyz";
    let id = uuid::Uuid::new_v4();
    let sql = format!("UPDATE {t} SET value = $1 WHERE id = $2");
    let err = handler
        .execute_bound_with_id(&session, &sql, vec![utf8(secret), int(1)], None, Some(id))
        .await
        .expect_err("a string that is not a number does not bind to BIGINT");
    let msg = err.to_string();
    assert!(
        msg.contains("parameter $1"),
        "error names the position: {msg}"
    );
    assert!(!msg.contains(secret), "error echoes the value: {msg}");
    if let Some(record) = handler.query_tracker().get(&id) {
        let logged = record.error_message.clone().unwrap_or_default();
        assert!(
            !logged.contains(secret),
            "query log echoes the value: {logged}"
        );
        assert!(!record.sql.contains(secret), "query log echoes the value");
    }

    assert_eq!(
        rows(&handler, &session, t).await,
        vec!["1|EU|10".to_string()]
    );
}
