//! A struct field reached by dot after a subscript, `l[1].b`.
//!
//! DataFusion plans a dot only in the leading `t.s.a` run of a field access,
//! so `l[1].b` failed with "Dot access not supported for non-string expr",
//! while Trino and Spark accept it. The front door rewrites it to
//! `l[1]['b']`; every form must return the subscript form's rows.
//!
//! Stack: `make test-integration FILTER=subscript_dot_access`.

/// Every row of `sql` as `col|col|...` text, NULL rendered as `NULL`.
async fn rendered_rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Result<Vec<String>, String> {
    let batches = handler
        .execute(session, sql, None)
        .await
        .map_err(|e| format!("{sql}: {e}"))?;
    let mut rows = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|c| {
                    if c.is_null(r) {
                        "NULL".to_string()
                    } else {
                        arrow::util::display::array_value_to_string(c, r).expect("render")
                    }
                })
                .collect();
            rows.push(cells.join("|"));
        }
    }
    Ok(rows)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn dot_after_a_subscript_reads_the_struct_field() {
    let table = "iceberg.test_ns.subscript_dot";
    let (session, handler) = crate::common::setup_handler().await;
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    for sql in [
        format!(
            "CREATE TABLE {table} \
             (id INT, l ARRAY<STRUCT<b INT, c STRUCT<d STRING>>>, s STRUCT<a INT>)"
        ),
        format!(
            "INSERT INTO {table} VALUES \
             (1, [named_struct('b', 10, 'c', named_struct('d', 'x'))], named_struct('a', 7)), \
             (2, [named_struct('b', 20, 'c', named_struct('d', 'y'))], named_struct('a', 8))"
        ),
    ] {
        handler
            .execute(&session, &sql, None)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let mut failures = Vec::new();
    for (dotted, subscript) in [
        ("l[1].b", "l[1]['b']"),
        ("l[1].c.d", "l[1]['c']['d']"),
        ("l[1].C.D", "l[1]['c']['d']"),
        ("t.l[1].b", "l[1]['b']"),
        ("(s).a", "s['a']"),
        ("named_struct('z', id).z", "id + 0"),
    ] {
        let q = |e: &str| format!("SELECT id, {e} FROM {table} t ORDER BY id");
        let want = rendered_rows(&handler, &session, &q(subscript)).await;
        let got = rendered_rows(&handler, &session, &q(dotted)).await;
        if want.is_err() || got != want {
            failures.push(format!("{dotted}: got {got:?}, want {want:?}"));
        }
    }
    // The rewritten form also filters.
    let filtered = rendered_rows(
        &handler,
        &session,
        &format!("SELECT id FROM {table} WHERE l[1].b = 20"),
    )
    .await;
    if filtered != Ok(vec!["2".to_string()]) {
        failures.push(format!("WHERE l[1].b = 20: {filtered:?}"));
    }

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    assert!(failures.is_empty(), "{failures:#?}");
}
