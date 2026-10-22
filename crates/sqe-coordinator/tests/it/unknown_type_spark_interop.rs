//! Iceberg V3 `unknown` columns across engines: Spark 4.1 with Iceberg 1.12
//! declares a `VOID` column, SQE reads it as all-NULL, inserts around it and
//! promotes it to a concrete type without rewriting the existing files.
//! Needs the integration stack and the Spark container
//! (`./scripts/start-spark-interop.sh`); without the container the test skips
//! with the reason.

use crate::common::spark_interop::{spark_sql, spark_stack_skip_reason};
use crate::variant_spark_interop::{spark_rows, sqe_rows};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn unknown_column_reads_writes_and_promotes() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping unknown_column_reads_writes_and_promotes: {reason}");
        return;
    }
    let fq = "default.unknown_from_spark";
    spark_sql(&format!(
        r#"DROP TABLE IF EXISTS rest.{fq};
           CREATE TABLE rest.{fq} (id INT, u VOID) USING iceberg
             TBLPROPERTIES ('format-version' = '3');
           INSERT INTO rest.{fq} VALUES (1, NULL), (2, NULL);"#
    ))
    .expect("spark writes the unknown-column table");

    let (session, handler) = crate::common::setup_handler().await;
    let declared = sqe_rows(
        &handler,
        &session,
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_schema = 'default' AND table_name = 'unknown_from_spark' \
         ORDER BY ordinal_position",
    )
    .await;
    let before = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id, u, u IS NULL FROM {fq} ORDER BY id"),
    )
    .await;
    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (3, NULL)"),
            None,
        )
        .await
        .expect("SQE inserts around the unknown column");
    handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} ALTER COLUMN u SET DATA TYPE INT"),
            None,
        )
        .await
        .expect("SQE promotes unknown to int");
    handler
        .execute(&session, &format!("INSERT INTO {fq} VALUES (4, 40)"), None)
        .await
        .expect("SQE inserts into the promoted column");
    let after = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id, u FROM {fq} ORDER BY id"),
    )
    .await;
    let spark = spark_rows(&format!(
        "SELECT id, u, typeof(u) FROM rest.{fq} ORDER BY id"
    ));
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));

    // information_schema keeps Arrow names, except unknown (Arrow Null).
    assert_eq!(declared, vec!["id|Int32", "u|unknown"], "declared types");
    assert_eq!(before, vec!["1|NULL|true", "2|NULL|true"], "Spark's rows");
    assert_eq!(
        after,
        vec!["1|NULL", "2|NULL", "3|NULL", "4|40"],
        "rows after the promotion"
    );
    assert_eq!(
        spark,
        vec!["1|NULL|int", "2|NULL|int", "3|NULL|int", "4|40|int"],
        "Spark's rows after SQE promoted the column"
    );
}

/// SQE declares the `unknown` column itself (`UNKNOWN`, or Spark's `VOID`),
/// writes rows around it, then promotes it; Spark reads every stage.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn sqe_declares_an_unknown_column_spark_reads_it() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping sqe_declares_an_unknown_column_spark_reads_it: {reason}");
        return;
    }
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.unknown_to_spark";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    for sql in [
        format!("CREATE TABLE {fq} (id INT, u UNKNOWN, w VOID)"),
        format!("INSERT INTO {fq} VALUES (1, NULL, NULL), (2, NULL, NULL)"),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    let declared = spark_rows(&format!(
        "SELECT id, typeof(u), typeof(w) FROM rest.{fq} ORDER BY id"
    ));
    for sql in [
        format!("ALTER TABLE {fq} ALTER COLUMN u SET DATA TYPE VARCHAR"),
        format!("INSERT INTO {fq} VALUES (3, 'x', NULL)"),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    let promoted = spark_rows(&format!("SELECT id, u, w FROM rest.{fq} ORDER BY id"));
    let sqe = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id, u, w FROM {fq} ORDER BY id"),
    )
    .await;
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;

    assert_eq!(
        declared,
        vec!["1|void|void", "2|void|void"],
        "Spark's types"
    );
    assert_eq!(
        promoted,
        vec!["1|NULL|NULL", "2|NULL|NULL", "3|x|NULL"],
        "Spark's rows after the promotion"
    );
    assert_eq!(sqe, promoted, "SQE's rows match Spark's");
}

/// An `unknown` field nested in a struct, and in a list of structs (#859):
/// Spark writes it, SQE reads it as NULL and writes rows around it, and
/// Spark reads SQE's rows. No data file stores the nested field.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn nested_unknown_struct_spark_writes_sqe_reads() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping nested_unknown_struct_spark_writes_sqe_reads: {reason}");
        return;
    }
    let fq = "default.nested_unknown_from_spark";
    spark_sql(&format!(
        r#"DROP TABLE IF EXISTS rest.{fq};
           CREATE TABLE rest.{fq}
             (id INT, s STRUCT<a: INT, u: VOID>, l ARRAY<STRUCT<b: INT, v: VOID>>)
             USING iceberg TBLPROPERTIES ('format-version' = '3');
           INSERT INTO rest.{fq} VALUES
             (1, named_struct('a', 10, 'u', NULL), array(named_struct('b', 100, 'v', NULL)));"#
    ))
    .expect("spark writes the nested unknown table");

    let (session, handler) = crate::common::setup_handler().await;
    let select =
        format!("SELECT id, s.a, s.u IS NULL, l[1]['b'], l[1]['v'] IS NULL FROM {fq} ORDER BY id");
    let before = sqe_rows(&handler, &session, &select).await;
    let insert = format!(
        "INSERT INTO {fq} VALUES (2, named_struct('a', 20, 'u', NULL), \
         [named_struct('b', 200, 'v', NULL)])"
    );
    handler
        .execute(&session, &insert, None)
        .await
        .expect("SQE writes around the nested unknown fields");
    let after = sqe_rows(&handler, &session, &select).await;
    let spark = spark_rows(&format!(
        "SELECT id, s.a, s.u, l[0].b, l[0].v FROM rest.{fq} ORDER BY id"
    ));
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));

    assert_eq!(before, vec!["1|10|true|100|true"], "Spark's rows");
    assert_eq!(
        after,
        vec!["1|10|true|100|true", "2|20|true|200|true"],
        "rows after SQE's insert"
    );
    assert_eq!(
        spark,
        vec!["1|10|NULL|100|NULL", "2|20|NULL|200|NULL"],
        "Spark reads SQE's rows"
    );
}

/// SQE declares the nested `unknown` field itself and writes rows; Spark
/// reads them (#859).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn nested_unknown_struct_sqe_creates_spark_reads() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping nested_unknown_struct_sqe_creates_spark_reads: {reason}");
        return;
    }
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.nested_unknown_to_spark";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    for sql in [
        format!("CREATE TABLE {fq} (id INT, s STRUCT<a INT, u UNKNOWN>)"),
        format!("INSERT INTO {fq} VALUES (1, named_struct('a', 10, 'u', NULL))"),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    let spark = spark_rows(&format!(
        "SELECT id, s.a, s.u, typeof(s) FROM rest.{fq} ORDER BY id"
    ));
    let sqe = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id, s.a, s.u IS NULL FROM {fq} ORDER BY id"),
    )
    .await;
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;

    assert_eq!(
        spark,
        vec!["1|10|NULL|struct<a:int,u:void>"],
        "Spark's rows"
    );
    assert_eq!(sqe, vec!["1|10|true"], "SQE's rows");
}

/// A list of unknown, a map with an unknown value and a struct whose members
/// are all unknown can be declared and read, but no engine can write a row:
/// Parquet has no column for them. SQE refuses the INSERT with a sentence
/// naming the column, and Iceberg Java refuses it too.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn unknown_list_map_and_struct_are_declared_not_written() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping unknown_list_map_and_struct_are_declared_not_written: {reason}");
        return;
    }
    let (session, handler) = crate::common::setup_handler().await;
    let mut outcomes = Vec::new();
    for (name, column, value) in [
        ("unknown_list", "c ARRAY<UNKNOWN>", "[NULL]"),
        ("unknown_map", "c MAP<VARCHAR, UNKNOWN>", "MAP {'k': NULL}"),
        (
            "unknown_struct",
            "c STRUCT<u UNKNOWN>",
            "named_struct('u', NULL)",
        ),
    ] {
        let fq = format!("default.{name}");
        let _ = handler
            .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
            .await;
        let create = format!("CREATE TABLE {fq} (id INT, {column})");
        handler
            .execute(&session, &create, None)
            .await
            .expect(&create);
        let empty = sqe_rows(&handler, &session, &format!("SELECT count(*) FROM {fq}")).await;
        let insert = format!("INSERT INTO {fq} VALUES (1, {value})");
        let sqe = match handler.execute(&session, &insert, None).await {
            Ok(_) => "accepted".to_string(),
            Err(e) => e.to_string(),
        };
        let spark_count = spark_rows(&format!("SELECT count(*) FROM rest.{fq}"));
        let spark_insert = spark_sql(&format!("INSERT INTO rest.{fq} (id) VALUES (1)"));
        let _ = handler
            .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
            .await;
        outcomes.push(format!(
            "{name}: sqe {} refuses={} spark {} refuses={}",
            empty.join(","),
            sqe.contains("Column `c` has") && sqe.contains("cannot be written to a data file"),
            spark_count.join(","),
            spark_insert.is_err(),
        ));
    }
    assert_eq!(
        outcomes,
        [
            "unknown_list: sqe 0 refuses=true spark 0 refuses=true",
            "unknown_map: sqe 0 refuses=true spark 0 refuses=true",
            "unknown_struct: sqe 0 refuses=true spark 0 refuses=true",
        ]
    );
}

/// `ALTER COLUMN s.a SET DATA TYPE` promotes a struct member, an `unknown`
/// one included, keeping its field id: rows written before the change read
/// widened in SQE and in Spark, and an illegal change is refused (#859).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn nested_type_promotion_reads_in_sqe_and_spark() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping nested_type_promotion_reads_in_sqe_and_spark: {reason}");
        return;
    }
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.nested_type_promotion";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    let columns = "id INT, s STRUCT<a INT, f REAL, u UNKNOWN, d DECIMAL(10,2)>";
    for sql in [
        format!("CREATE TABLE {fq} ({columns})"),
        format!(
            "INSERT INTO {fq} VALUES (1, named_struct('a', 1, 'f', 1.5, 'u', NULL, 'd', 12.25))"
        ),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }

    let mut refused = Vec::new();
    for (column, to) in [
        ("s.a", "VARCHAR"),
        ("s.d", "DECIMAL(12,3)"),
        ("s.nope", "BIGINT"),
        ("id.x", "BIGINT"),
    ] {
        let alter = format!("ALTER TABLE {fq} ALTER COLUMN {column} SET DATA TYPE {to}");
        match handler.execute(&session, &alter, None).await {
            Ok(_) => refused.push(format!("{column} -> {to}: accepted")),
            Err(e) => refused.push(format!(
                "{column} -> {to}: {}",
                e.to_string().contains(column)
            )),
        }
    }
    for (column, to) in [
        ("s.a", "BIGINT"),
        ("s.f", "DOUBLE"),
        ("s.u", "INT"),
        ("s.d", "DECIMAL(12,2)"),
    ] {
        let alter = format!("ALTER TABLE {fq} ALTER COLUMN {column} SET DATA TYPE {to}");
        handler.execute(&session, &alter, None).await.expect(&alter);
    }
    let insert = format!(
        "INSERT INTO {fq} VALUES \
         (2, named_struct('a', 5000000000, 'f', 2.25, 'u', 7, 'd', 1234567890.12))"
    );
    handler
        .execute(&session, &insert, None)
        .await
        .expect(&insert);

    // The row helper renders neither decimals nor full float precision.
    let select = format!(
        "SELECT id, s.a, CAST(s.f AS VARCHAR), s.u, CAST(s.d AS VARCHAR), \
         arrow_typeof(s.f), arrow_typeof(s.d) FROM {fq} ORDER BY id"
    );
    let sqe = sqe_rows(&handler, &session, &select).await;
    let spark = spark_rows(&format!(
        "SELECT id, s.a, s.f, s.u, s.d, typeof(s) FROM rest.{fq} ORDER BY id"
    ));
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;

    assert_eq!(
        refused,
        [
            "s.a -> VARCHAR: true",
            "s.d -> DECIMAL(12,3): true",
            "s.nope -> BIGINT: true",
            "id.x -> BIGINT: true",
        ],
        "refused type changes"
    );
    assert_eq!(
        sqe,
        [
            "1|1|1.5|NULL|12.25|Float64|Decimal128(12, 2)",
            "2|5000000000|2.25|7|1234567890.12|Float64|Decimal128(12, 2)",
        ],
        "SQE's rows"
    );
    let typ = "struct<a:bigint,f:double,u:int,d:decimal(12,2)>";
    assert_eq!(
        spark,
        [
            format!("1|1|1.5|NULL|12.25|{typ}"),
            format!("2|5000000000|2.25|7|1234567890.12|{typ}"),
        ],
        "Spark's rows"
    );
}

/// `ALTER COLUMN l.element` / `m.value` promote a list element and a map
/// value, keeping their field ids: rows written before the change read
/// widened in SQE and in Spark. A map key cannot change type (#869).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn list_element_and_map_value_promotion_reads_in_sqe_and_spark() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping list_element_and_map_value_promotion_reads_in_sqe_and_spark: {reason}");
        return;
    }
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.list_map_type_promotion";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    for sql in [
        format!("CREATE TABLE {fq} (id INT, l ARRAY<INT>, m MAP<INT, REAL>)"),
        format!("INSERT INTO {fq} VALUES (1, make_array(1, 2), MAP {{1: 1.5}})"),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }

    let mut refused = Vec::new();
    for (column, to) in [
        ("l.element", "VARCHAR"),
        ("m.key", "BIGINT"),
        ("l.item", "BIGINT"),
    ] {
        let alter = format!("ALTER TABLE {fq} ALTER COLUMN {column} SET DATA TYPE {to}");
        match handler.execute(&session, &alter, None).await {
            Ok(_) => refused.push(format!("{column} -> {to}: accepted")),
            Err(e) => refused.push(format!(
                "{column} -> {to}: {}",
                e.to_string().contains(column)
            )),
        }
    }
    for (column, to) in [("l.element", "BIGINT"), ("m.value", "DOUBLE")] {
        let alter = format!("ALTER TABLE {fq} ALTER COLUMN {column} SET DATA TYPE {to}");
        handler.execute(&session, &alter, None).await.expect(&alter);
    }
    let insert = format!("INSERT INTO {fq} VALUES (2, make_array(5000000000), MAP {{2: 2.25}})");
    handler
        .execute(&session, &insert, None)
        .await
        .expect(&insert);

    // DataFusion indexes a map only with a literal key; each map holds one entry.
    let select = format!(
        "SELECT id, l[1], CAST(map_values(m)[1] AS VARCHAR), arrow_typeof(l), arrow_typeof(m) \
         FROM {fq} ORDER BY id"
    );
    let sqe = sqe_rows(&handler, &session, &select).await;
    let spark = spark_rows(&format!(
        "SELECT id, l[0], m[id], typeof(l), typeof(m) FROM rest.{fq} ORDER BY id"
    ));
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;

    assert_eq!(
        refused,
        [
            "l.element -> VARCHAR: true",
            "m.key -> BIGINT: true",
            "l.item -> BIGINT: true",
        ],
        "refused type changes"
    );
    assert_eq!(sqe.len(), 2, "SQE's rows: {sqe:?}");
    for (row, prefix) in sqe.iter().zip(["1|1|1.5|", "2|5000000000|2.25|"]) {
        assert!(row.starts_with(prefix), "SQE row {row}");
        assert!(
            row.contains("Int64") && row.contains("Float64"),
            "SQE row {row}"
        );
    }
    assert_eq!(
        spark,
        [
            "1|1|1.5|array<bigint>|map<int,double>",
            "2|5000000000|2.25|array<bigint>|map<int,double>",
        ],
        "Spark's rows"
    );
}
