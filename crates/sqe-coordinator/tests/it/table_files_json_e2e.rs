//! `table_files` renders `partition`, `lower_bounds` and `upper_bounds` as
//! JSON for every column type. Date, timestamp and binary bounds used to come
//! out unquoted, string bounds unescaped, and the partition tuple in Rust's
//! Debug form, so none of them parsed.

use arrow_array::{Array, StringArray};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn table_files_columns_are_json_for_every_type() {
    let (session, handler) = crate::common::setup_handler().await;
    let table = "default.table_files_json";
    let exec = |sql: String| {
        let (handler, session) = (&handler, &session);
        async move {
            handler
                .execute(session, &sql, None)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
        }
    };
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    exec(format!(
        "CREATE TABLE {table} (id BIGINT, d DATE, s STRING, b BYTEA, ts TIMESTAMP) \
         PARTITIONED BY (d)"
    ))
    .await;
    exec(format!(
        "INSERT INTO {table} VALUES \
         (1, DATE '2026-10-26', 'a\"b\\c', X'00AB', TIMESTAMP '2026-10-26 08:00:00')"
    ))
    .await;

    let sql = "SELECT partition, lower_bounds, upper_bounds \
               FROM table_files('default', 'table_files_json')";
    let batches = exec(sql.to_string()).await;
    let cell = |col: usize| {
        let a = batches[0].column(col);
        let a = a.as_any().downcast_ref::<StringArray>().expect("Utf8");
        assert_eq!(a.len(), 1, "one data file");
        let text = a.value(0).to_string();
        serde_json::from_str::<serde_json::Value>(&text)
            .unwrap_or_else(|e| panic!("column {col} is not JSON: {text}: {e}"))
    };
    let (partition, lower, upper) = (cell(0), cell(1), cell(2));

    // Field ids: 1 id, 2 d, 3 s, 4 b, 5 ts.
    assert_eq!(partition, serde_json::json!(["2026-10-26"]));
    for bounds in [&lower, &upper] {
        assert_eq!(bounds["1"], serde_json::json!(1), "{bounds}");
        assert_eq!(bounds["2"], serde_json::json!("2026-10-26"), "{bounds}");
        assert_eq!(bounds["3"], serde_json::json!("a\"b\\c"), "{bounds}");
        assert_eq!(bounds["4"], serde_json::json!("00AB"), "{bounds}");
        let ts = bounds["5"].as_str().unwrap_or_default();
        assert!(ts.starts_with("2026-10-26"), "{bounds}");
    }

    exec(format!("DROP TABLE {table}")).await;
}
