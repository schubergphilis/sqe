//! Iceberg V3 `VARIANT` columns against the live Polaris stack (#849).
//!
//! A `VARIANT` column makes the table format-version 3, takes JSON text on
//! INSERT, and comes back as variant storage that renders as JSON. CTAS keeps
//! the type, and `ALTER TABLE ADD COLUMN ... VARIANT` needs a v3 table.

use arrow::array::AsArray;
use arrow_array::Array;
use object_store::ObjectStoreExt;

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
    assert!(ddl.contains("v variant"), "SHOW CREATE TABLE {fq}: {ddl}");
    // typeof names the variant, not its struct storage, and a column by its
    // Trino type name (#867).
    let typeof_sql = format!("SELECT typeof(v), typeof(id) FROM {fq} WHERE id = 1");
    assert_eq!(
        rows(&handler, &session, &typeof_sql).await,
        vec!["variant|integer"]
    );

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

    let ddl = show_create(&handler, &session, dst).await;
    assert!(
        ddl.contains("v variant") && ddl.contains("w variant"),
        "SHOW CREATE TABLE {dst}: {ddl}"
    );
    assert_eq!(
        rows(
            &handler,
            &session,
            &format!("SELECT id, v, w FROM {dst} ORDER BY id")
        )
        .await,
        vec![r#"1|{"k":[1,2]}|NULL"#, r#"2|{"k":[1,2]}|NULL"#, "3|[]|{}",]
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
        ddl.contains("j variant") && ddl.contains("k variant"),
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

/// `CAST(.. AS JSON)` of a v3 JSON column embeds the stored value, also
/// inside an array, as Trino does (#848).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn cast_as_json_embeds_a_v3_json_column() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.cast_as_json_v3";
    drop(&handler, &session, fq).await;
    for sql in [
        format!("CREATE TABLE {fq} (id INT, j JSON) WITH ('format-version' = '3')"),
        format!(r#"INSERT INTO {fq} VALUES (1, '{{"a": 1}}'), (2, '"s"'), (3, NULL)"#),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    let select = format!("SELECT id, CAST(j AS JSON), to_json(j) FROM {fq} ORDER BY id");
    assert_eq!(
        rows(&handler, &session, &select).await,
        vec![r#"1|{"a":1}|{"a":1}"#, r#"2|"s"|"s""#, "3|NULL|NULL"]
    );
    let nested = format!("SELECT CAST(ARRAY[j, j] AS JSON) FROM {fq} WHERE id = 1");
    assert_eq!(
        rows(&handler, &session, &nested).await,
        vec![r#"[{"a":1},{"a":1}]"#]
    );
    drop(&handler, &session, fq).await;
}

/// A JSON value from `json_parse` / `json_extract` keeps its JSON marker
/// through a subquery, so `CAST(.. AS JSON)` of it is the identity, and an
/// array of JSON values embeds them (#848). The marked field writes like
/// any string (CTAS, INSERT, UNION with a plain column). A stored V2 text
/// column has no marker, so its cast stays a JSON string.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn cast_as_json_of_a_json_value_is_the_identity() {
    let (session, handler) = crate::common::setup_handler().await;
    let src = "default.json_value_cast_src";
    let dst = "default.json_value_cast_dst";
    drop(&handler, &session, src).await;
    drop(&handler, &session, dst).await;
    for sql in [
        format!("CREATE TABLE {src} (id INT, doc VARCHAR)"),
        format!(r#"INSERT INTO {src} VALUES (1, '{{"n": 1.5}}'), (2, NULL)"#),
        format!("CREATE TABLE {dst} AS SELECT id, json_parse(doc) AS p FROM {src}"),
        format!("INSERT INTO {dst} SELECT id + 10, json_extract(doc, '$') FROM {src}"),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    let cases = [
        (
            format!("SELECT id, CAST(p AS JSON), typeof(p) FROM (SELECT id, json_parse(doc) AS p FROM {src}) s ORDER BY id"),
            vec![r#"1|{"n":1.5}|json"#, "2|NULL|json"],
        ),
        (
            format!("SELECT CAST(ARRAY[json_parse(doc), NULL] AS JSON) FROM {src} ORDER BY id"),
            vec![r#"[{"n":1.5},null]"#, "[null,null]"],
        ),
        // CASE and coalesce over JSON values stay JSON.
        (
            format!(
                "SELECT id, CAST(CASE WHEN id = 1 THEN json_parse(doc) END AS JSON), \
                 CAST(coalesce(json_parse(doc), json_parse('[]')) AS JSON) FROM {src} ORDER BY id"
            ),
            vec![r#"1|{"n":1.5}|{"n":1.5}"#, "2|NULL|[]"],
        ),
        (
            format!("SELECT id, p, CAST(p AS JSON) FROM {dst} ORDER BY id"),
            vec![
                r#"1|{"n":1.5}|"{\"n\":1.5}""#,
                "2|NULL|NULL",
                r#"11|{"n":1.5}|"{\"n\":1.5}""#,
                "12|NULL|NULL",
            ],
        ),
        (
            format!(
                "SELECT v FROM (SELECT json_parse(doc) AS v FROM {src} WHERE id = 1 \
                 UNION ALL SELECT doc FROM {src} WHERE id = 1) u ORDER BY v"
            ),
            vec![r#"{"n": 1.5}"#, r#"{"n":1.5}"#],
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&handler, &session, &sql).await, want, "{sql}");
    }
    drop(&handler, &session, src).await;
    drop(&handler, &session, dst).await;
}

/// `variant_get` reads typed fields of a stored variant column, a missing
/// path is NULL, and without a type the result is a variant (#857).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn variant_get_reads_typed_fields() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.variant_get_fields";
    drop(&handler, &session, fq).await;
    for sql in [
        format!("CREATE TABLE {fq} (id INT, v VARIANT)"),
        format!(
            r#"INSERT INTO {fq} VALUES
               (1, '{{"n": 7, "s": "x", "d": 1.25, "a": {{"b": [10, 20]}}}}'),
               (2, '{{"n": 8}}'), (3, NULL)"#
        ),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    assert_eq!(
        rows(
            &handler,
            &session,
            &format!(
                "SELECT id, variant_get(v, '$.n', 'int') + 1, variant_get(v, '$.s', 'string'), \
                 variant_get(v, '$.d', 'decimal(5,2)'), variant_get(v, '$.a.b[1]', 'bigint'), \
                 variant_get(v, '$.a') FROM {fq} ORDER BY id"
            )
        )
        .await,
        vec![
            r#"1|8|x|1.25|20|{"b":[10,20]}"#,
            "2|9|NULL|NULL|NULL|NULL",
            "3|NULL|NULL|NULL|NULL|NULL",
        ]
    );
    // In a predicate, too.
    assert_eq!(
        rows(
            &handler,
            &session,
            &format!("SELECT id FROM {fq} WHERE variant_get(v, '$.n', 'int') > 7")
        )
        .await,
        vec!["2"]
    );
    // A value that does not convert errors; try_variant_get gives NULL.
    handler
        .execute(
            &session,
            &format!("SELECT variant_get(v, '$.s', 'int') FROM {fq}"),
            None,
        )
        .await
        .expect_err("'x' is not an int");
    assert_eq!(
        rows(
            &handler,
            &session,
            &format!("SELECT try_variant_get(v, '$.s', 'int') FROM {fq} WHERE id = 1")
        )
        .await,
        vec!["NULL"]
    );
    drop(&handler, &session, fq).await;
}

/// `rewrite_data_files` merges the small files of a variant table and every
/// value reads back the same, as JSON and through `variant_get` (#857).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn rewrite_data_files_preserves_variant_values() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.variant_compaction";
    drop(&handler, &session, fq).await;
    handler
        .execute(
            &session,
            &format!("CREATE TABLE {fq} (id INT, v VARIANT)"),
            None,
        )
        .await
        .expect("CREATE TABLE with VARIANT");
    // One file per INSERT; a mix of objects, arrays, scalars and NULLs.
    let values = [
        r#"'{"k": 0, "tags": ["a", "b"]}'"#,
        "'[1, 2.5, null]'",
        "'42'",
        r#"'"text"'"#,
        "NULL",
        r#"'{"k": 5, "nested": {"deep": true}}'"#,
    ];
    for (i, value) in values.iter().enumerate() {
        handler
            .execute(
                &session,
                &format!("INSERT INTO {fq} VALUES ({i}, {value})"),
                None,
            )
            .await
            .expect("INSERT");
    }
    let select = format!("SELECT id, v, variant_get(v, '$.k', 'int') FROM {fq} ORDER BY id");
    let before = rows(&handler, &session, &select).await;
    assert_eq!(
        before,
        vec![
            r#"0|{"k":0,"tags":["a","b"]}|0"#,
            "1|[1,2.5,null]|NULL",
            "2|42|NULL",
            r#"3|"text"|NULL"#,
            "4|NULL|NULL",
            r#"5|{"k":5,"nested":{"deep":true}}|5"#,
        ]
    );
    let files =
        "SELECT COUNT(*) FROM table_files('default', 'variant_compaction') WHERE content = 0";
    assert_eq!(rows(&handler, &session, files).await, vec!["6"]);

    handler
        .execute(
            &session,
            &format!("CALL system.rewrite_data_files(table => '{fq}', min_input_files => 2)"),
            None,
        )
        .await
        .expect("rewrite_data_files on a variant table");

    assert_eq!(
        rows(&handler, &session, files).await,
        vec!["1"],
        "the six files merge into one"
    );
    assert_eq!(rows(&handler, &session, &select).await, before);
    let ddl = show_create(&handler, &session, fq).await;
    assert!(ddl.contains("v variant"), "still a variant column: {ddl}");
    drop(&handler, &session, fq).await;
}

/// The row groups of every data file of `table` (in `default`), and the
/// largest file's size.
pub(crate) async fn row_groups(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> (usize, i64) {
    let sql =
        format!("SELECT file_path, file_size_in_bytes FROM table_files('default', '{table}') WHERE content = 0");
    let mut groups = 0;
    let mut largest = 0;
    for row in rows(handler, session, &sql).await {
        let (path, size) = row.split_once('|').expect("path|size");
        let (bucket, key) = crate::variant_spark_interop::split_s3(path);
        let bytes = crate::variant_spark_interop::test_store(&bucket)
            .get(&object_store::path::Path::from(key.as_str()))
            .await
            .expect("get data file")
            .bytes()
            .await
            .expect("read data file");
        let reader =
            parquet::file::serialized_reader::SerializedFileReader::new(bytes).expect("footer");
        groups += parquet::file::reader::FileReader::metadata(&reader).num_row_groups();
        largest = largest.max(size.parse::<i64>().expect("size"));
    }
    (groups, largest)
}

/// The scan's output rows in `EXPLAIN ANALYZE sql`.
pub(crate) async fn scan_output_rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> i64 {
    let analyze = format!("EXPLAIN ANALYZE {sql}");
    let batches = handler
        .execute(session, &analyze, None)
        .await
        .expect(&analyze);
    let mut out = None;
    for batch in &batches {
        let ops = batch
            .column_by_name("operation")
            .expect("operation")
            .as_string::<i32>();
        let output = batch.column_by_name("output_rows").expect("output_rows");
        let output = output.as_primitive::<arrow::datatypes::Int64Type>();
        for r in 0..batch.num_rows() {
            if ops.value(r) == "IcebergScanExec" {
                assert!(out.is_none(), "one scan in {sql}");
                out = Some(output.value(r));
            }
        }
    }
    out.unwrap_or_else(|| panic!("no IcebergScanExec in EXPLAIN ANALYZE {sql}"))
}

/// A comparison on a shredded variant field skips the row groups whose
/// statistics exclude it (#870), on the small-file direct read and on the
/// vendored reader, and returns what the unpruned scan returns.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn prunes_shredded_row_groups() {
    let (session, handler) = crate::common::setup_handler().await;
    // The small table is read whole into memory; the large one's file is
    // over the 3 MiB direct-read threshold and goes through the vendored
    // reader. The padding is incompressible so the file really is large.
    for (table, n) in [
        ("variant_prune_small", 20_000_i64),
        ("variant_prune_large", 300_000),
    ] {
        let fq = format!("default.{table}");
        drop(&handler, &session, &fq).await;
        let create = format!(
            "CREATE TABLE {fq} (id BIGINT, v VARIANT) \
             WITH ('write.parquet.shred-variants' = 'true', \
                   'write.parquet.row-group-size-bytes' = '262144')"
        );
        handler
            .execute(&session, &create, None)
            .await
            .expect(&create);
        let insert = format!(
            r#"INSERT INTO {fq} SELECT value, '{{"a": ' || value || ', "pad": "' || md5(CAST(value AS VARCHAR)) || '"}}' FROM range(0, {n})"#
        );
        handler
            .execute(&session, &insert, None)
            .await
            .expect(&insert);

        let (groups, largest) = row_groups(&handler, &session, table).await;
        assert!(
            groups > 2,
            "{table}: several row groups to prune, got {groups}"
        );
        let direct = largest < 3 * 1024 * 1024;
        assert_eq!(direct, n == 20_000, "{table}: largest file {largest} bytes");

        let cut = n - 100;
        let pruned = format!(
            "SELECT count(*), min(id) FROM {fq} WHERE variant_get(v, '$.a', 'bigint') >= {cut}"
        );
        // `+ 0` is not a shape the scan prunes on: the full-scan baseline.
        let baseline = format!(
            "SELECT count(*), min(id) FROM {fq} WHERE variant_get(v, '$.a', 'bigint') + 0 >= {cut}"
        );
        let want = vec![format!("100|{cut}")];
        assert_eq!(
            rows(&handler, &session, &pruned).await,
            want,
            "{table}: pruned"
        );
        assert_eq!(
            rows(&handler, &session, &baseline).await,
            want,
            "{table}: baseline"
        );

        assert_eq!(
            scan_output_rows(&handler, &session, &baseline).await,
            n,
            "{table}: baseline reads every row"
        );
        let read = scan_output_rows(&handler, &session, &pruned).await;
        assert!(read < n / 2, "{table}: pruned scan read {read} of {n} rows");

        // The literal on the left and `try_variant_get` prune the same way.
        let flipped =
            format!("SELECT count(*) FROM {fq} WHERE {cut} <= try_variant_get(v, '$.a', 'bigint')");
        assert_eq!(
            rows(&handler, &session, &flipped).await,
            vec!["100"],
            "{table}: flipped"
        );
        assert!(
            scan_output_rows(&handler, &session, &flipped).await < n / 2,
            "{table}: flipped prunes"
        );
        drop(&handler, &session, &fq).await;
    }
}

/// Write `fq` (in `default`, dropped first) with `n` shredded rows plus a
/// second file that does not shred `b`, then check that reading fields
/// through `variant_get` (#870), which skips the other shredded fields,
/// returns what a whole-column read returns.
///
/// The last three rows of the first file come after its first batch, which
/// fixed the shredding schema: one holds `a` as a string, so the writer
/// puts that whole object in the residual `value`; one is an array; one is
/// NULL.
pub(crate) async fn check_field_reads(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    fq: &str,
    n: i64,
) {
    let _ = handler
        .execute(session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    let (odd, array, null) = (n - 3, n - 2, n - 1);
    for sql in [
        format!(
            "CREATE TABLE {fq} (id BIGINT, v VARIANT) \
             WITH ('write.parquet.shred-variants' = 'true')"
        ),
        format!(
            r#"INSERT INTO {fq} SELECT value, CASE
                 WHEN value = {odd} THEN '{{"a": "s", "b": "odd", "pad": "p"}}'
                 WHEN value = {array} THEN '[1, 2]'
                 WHEN value = {null} THEN NULL
                 ELSE '{{"a": ' || value || ', "b": "x' || value || '", "pad": "' || md5(CAST(value AS VARCHAR)) || '"}}'
               END FROM range(0, {n})"#
        ),
        format!(
            r#"INSERT INTO {fq} SELECT value, '{{"a": ' || value || ', "c": true}}' FROM range({n}, {n} + 10)"#
        ),
    ] {
        handler.execute(session, &sql, None).await.expect(&sql);
    }

    let ids = format!("0, 1, {odd}, {array}, {null}, {n}");
    let fields = "variant_get(v, '$.b', 'string'), try_variant_get(v, '$.a', 'bigint'), \
                  try_variant_get(v, '$.c', 'boolean')";
    let narrowed = format!("SELECT id, {fields} FROM {fq} WHERE id IN ({ids}) ORDER BY id");
    // `$[0]` reads the column whole: the baseline. Its cell is dropped.
    let whole = format!(
        "SELECT id, {fields}, try_variant_get(v, '$[0]', 'bigint') FROM {fq} \
         WHERE id IN ({ids}) ORDER BY id"
    );
    let got = rows(handler, session, &narrowed).await;
    let want: Vec<String> = rows(handler, session, &whole)
        .await
        .into_iter()
        .map(|r| r.rsplit_once('|').expect("baseline cell").0.to_string())
        .collect();
    assert_eq!(got, want, "{fq}: narrowed read vs whole read");
    assert_eq!(
        got,
        vec![
            "0|x0|0|NULL".to_string(),
            "1|x1|1|NULL".to_string(),
            format!("{odd}|odd|NULL|NULL"),
            format!("{array}|NULL|NULL|NULL"),
            format!("{null}|NULL|NULL|NULL"),
            format!("{n}|NULL|{n}|true"),
        ],
        "{fq}"
    );

    let count = format!("SELECT count(variant_get(v, '$.b', 'string')) FROM {fq}");
    assert_eq!(
        rows(handler, session, &count).await,
        vec![(n - 2).to_string()],
        "{fq}: every row but the array and NULL has b"
    );
}

/// The plan text of `EXPLAIN sql`.
async fn explain(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> String {
    rows(handler, session, &format!("EXPLAIN {sql}")).await.join("\n")
}

/// Reading shredded variant fields through `variant_get` reads only those
/// fields (#870), on the small-file direct read and on the vendored reader,
/// and returns what a whole-column read returns.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn reads_only_the_shredded_fields_a_query_uses() {
    let (session, handler) = crate::common::setup_handler().await;
    // The large table's file is over the 3 MiB direct-read threshold.
    for (table, n) in [("variant_fields_small", 20_000_i64), ("variant_fields_large", 300_000)] {
        let fq = format!("default.{table}");
        check_field_reads(&handler, &session, &fq, n).await;
        let (_, largest) = row_groups(&handler, &session, table).await;
        assert_eq!(largest < 3 * 1024 * 1024, n == 20_000, "{table}: largest file {largest} bytes");

        let narrowed = format!("SELECT variant_get(v, '$.b', 'string') FROM {fq}");
        let plan = explain(&handler, &session, &narrowed).await;
        assert!(plan.contains("variant_fields={2:[b]}"), "{table}: {plan}");
        let whole = format!("SELECT variant_get(v, '$.b', 'string'), v FROM {fq}");
        let plan = explain(&handler, &session, &whole).await;
        assert!(!plan.contains("variant_fields="), "{table}: {plan}");
        drop(&handler, &session, &fq).await;
    }
}
