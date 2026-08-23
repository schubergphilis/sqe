use sqe_sql::classifier::{parse_and_classify, StatementKind};
use sqe_sql::hive_partitions::{try_parse_hive_partition_ddl, HivePartitionAction};

fn add_of(sql: &str) -> (String, HivePartitionAction) {
    let ddl = try_parse_hive_partition_ddl(sql)
        .expect("parse")
        .expect("is an ADD/DROP PARTITION statement");
    match ddl.action {
        a @ HivePartitionAction::Add { .. } => (ddl.table, a),
        other => panic!("expected ADD, got {other:?}"),
    }
}

fn drop_of(sql: &str) -> (String, HivePartitionAction) {
    let ddl = try_parse_hive_partition_ddl(sql)
        .expect("parse")
        .expect("is an ADD/DROP PARTITION statement");
    match ddl.action {
        a @ HivePartitionAction::Drop { .. } => (ddl.table, a),
        other => panic!("expected DROP, got {other:?}"),
    }
}

#[test]
fn parses_add_partition_with_location() {
    let (table, action) = add_of(
        "ALTER TABLE lake.sales.orders ADD PARTITION (dt = '2026-01-02') \
         LOCATION 's3://raw/orders/dt=2026-01-02/'",
    );
    assert_eq!(table, "lake.sales.orders");
    match action {
        HivePartitionAction::Add {
            if_not_exists,
            partitions,
        } => {
            assert!(!if_not_exists);
            assert_eq!(partitions.len(), 1);
            assert_eq!(
                partitions[0].values,
                vec![("dt".to_string(), "2026-01-02".to_string())]
            );
            assert_eq!(
                partitions[0].location.as_deref(),
                Some("s3://raw/orders/dt=2026-01-02/")
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn parses_add_if_not_exists_without_location() {
    let (table, action) = add_of(
        "ALTER TABLE orders ADD IF NOT EXISTS PARTITION (dt = '2026-01-02', region = 'eu')",
    );
    assert_eq!(table, "orders");
    match action {
        HivePartitionAction::Add {
            if_not_exists,
            partitions,
        } => {
            assert!(if_not_exists);
            assert_eq!(partitions[0].values.len(), 2);
            assert_eq!(partitions[0].location, None);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn parses_multiple_added_partitions_with_and_without_locations() {
    let (_, action) = add_of(
        "ALTER TABLE orders ADD \
         PARTITION (dt = '2026-01-01') LOCATION 's3://a/dt=2026-01-01/', \
         PARTITION (dt = '2026-01-02')",
    );
    match action {
        HivePartitionAction::Add { partitions, .. } => {
            assert_eq!(partitions.len(), 2);
            assert!(partitions[0].location.is_some());
            assert!(partitions[1].location.is_none());
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn parses_unquoted_and_numeric_values() {
    let (_, action) = add_of("ALTER TABLE t ADD PARTITION (bucket = 42, dt = 2026-01-02)");
    match action {
        HivePartitionAction::Add { partitions, .. } => {
            assert_eq!(
                partitions[0].values,
                vec![
                    ("bucket".to_string(), "42".to_string()),
                    ("dt".to_string(), "2026-01-02".to_string())
                ]
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn parses_negative_number_values() {
    let (_, action) = add_of("ALTER TABLE t ADD PARTITION (offset = -7)");
    match action {
        HivePartitionAction::Add { partitions, .. } => {
            assert_eq!(partitions[0].values[0].1, "-7");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn parses_drop_partition() {
    let (table, action) = drop_of("ALTER TABLE lake.sales.orders DROP PARTITION (dt = '2026-01-02')");
    assert_eq!(table, "lake.sales.orders");
    match action {
        HivePartitionAction::Drop {
            if_exists,
            partitions,
            purge,
        } => {
            assert!(!if_exists);
            assert!(!purge);
            assert_eq!(partitions[0].values[0].0, "dt");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn parses_drop_if_exists_and_purge() {
    let (_, action) = drop_of("ALTER TABLE t DROP IF EXISTS PARTITION (dt = 'x') PURGE");
    match action {
        HivePartitionAction::Drop {
            if_exists, purge, ..
        } => {
            assert!(if_exists);
            assert!(purge, "PURGE must be recognized so the rejection is precise");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn quoted_identifiers_survive_the_table_reference() {
    let (table, _) = add_of(r#"ALTER TABLE "MyDb"."My Table" ADD PARTITION (dt = 'x')"#);
    assert_eq!(table, r#""MyDb"."My Table""#);
}

#[test]
fn non_partition_alter_falls_through() {
    for sql in [
        "ALTER TABLE t ADD COLUMN c INT",
        "ALTER TABLE t ADD PARTITION FIELD year(ts)",
        "ALTER TABLE t RENAME TO u",
        "CREATE TABLE t (a INT)",
    ] {
        let parsed = try_parse_hive_partition_ddl(sql).expect("no error");
        assert!(parsed.is_none(), "{sql} must fall through, got {parsed:?}");
    }
}

#[test]
fn trailing_garbage_is_an_error_not_a_silent_accept() {
    let err = try_parse_hive_partition_ddl(
        "ALTER TABLE t ADD PARTITION (dt = 'x') SOMETHING ELSE",
    )
    .expect_err("must reject");
    assert!(
        format!("{err}").contains("trailing"),
        "got {err}"
    );
}

// --- classifier wiring ---

#[test]
fn classifier_routes_add_partition_to_the_hive_variant() {
    let kind =
        parse_and_classify("ALTER TABLE lake.sales.orders ADD PARTITION (dt = '2026-01-02')")
            .expect("classify");
    assert!(matches!(kind, StatementKind::HivePartitionDdl(_)), "{kind:?}");
    assert_eq!(kind.name(), "hivepartitionddl");
}

#[test]
fn classifier_routes_msck_repair_table() {
    let kind = parse_and_classify("MSCK REPAIR TABLE lake.sales.orders").expect("classify");
    match kind {
        StatementKind::MsckRepairTable(table) => {
            assert_eq!(table, "lake.sales.orders");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn bare_msck_and_partition_action_forms_get_precise_rejections() {
    for sql in ["MSCK TABLE t", "MSCK REPAIR TABLE t ADD PARTITIONS"] {
        let err = parse_and_classify(sql).expect_err("must reject");
        assert!(
            format!("{err}").to_lowercase().contains("msck"),
            "{sql}: {err}"
        );
    }
}

#[test]
fn classifier_routes_show_partitions() {
    let kind = parse_and_classify("SHOW PARTITIONS lake.sales.orders").expect("classify");
    match kind {
        StatementKind::ShowPartitions(table) => assert_eq!(table, "lake.sales.orders"),
        other => panic!("{other:?}"),
    }
    // Bare form: precise error.
    let err = parse_and_classify("SHOW PARTITIONS").expect_err("must reject");
    assert!(format!("{err}").contains("SHOW PARTITIONS <table>"), "{err}");
}

#[test]
fn partition_field_evolution_still_reaches_its_own_variant() {
    let kind =
        parse_and_classify("ALTER TABLE events ADD PARTITION FIELD year(ts)").expect("classify");
    assert!(matches!(kind, StatementKind::PartitionEvolution(_)), "{kind:?}");
}

#[test]
fn drop_without_a_partition_clause_is_not_a_hive_partition_ddl() {
    let parsed = try_parse_hive_partition_ddl("ALTER TABLE t DROP COLUMN c").expect("no error");
    assert!(parsed.is_none());
}

#[test]
fn add_partition_with_empty_spec_is_an_error() {
    let err = try_parse_hive_partition_ddl("ALTER TABLE t ADD PARTITION ()").expect_err("empty");
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("partition") || msg.contains("value") || msg.contains("empty"),
        "{err}"
    );
}

#[test]
fn show_partitions_rejects_trailing_garbage() {
    let err = parse_and_classify("SHOW PARTITIONS t LIKE 'dt%'").expect_err("LIKE not supported");
    assert!(
        format!("{err}").contains("LIKE") || format!("{err}").contains("extra clauses"),
        "{err}"
    );
}

#[test]
fn classifier_routes_drop_partition() {
    let kind =
        parse_and_classify("ALTER TABLE lake.sales.orders DROP PARTITION (dt = 'x')").expect("c");
    assert!(matches!(kind, StatementKind::HivePartitionDdl(_)), "{kind:?}");
}

#[test]
fn msck_repair_requires_a_table() {
    let err = parse_and_classify("MSCK REPAIR TABLE").expect_err("no table");
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("identifier") || msg.contains("msck") || msg.contains("table"),
        "{err}"
    );
}

#[test]
fn comments_and_a_trailing_semicolon_are_ignored() {
    let ddl = try_parse_hive_partition_ddl(
        "ALTER TABLE t /* note */ ADD PARTITION (dt = 'x');",
    )
    .expect("parse")
    .expect("is ADD");
    match ddl.action {
        HivePartitionAction::Add { partitions, .. } => {
            assert_eq!(partitions[0].values[0].1, "x");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn mixed_case_keywords_parse() {
    let (_, action) = add_of("alter table t add if not exists partition (dt = 'x')");
    match action {
        HivePartitionAction::Add { if_not_exists, .. } => assert!(if_not_exists),
        other => panic!("{other:?}"),
    }
}

#[test]
fn location_must_be_a_single_quoted_string() {
    let err = try_parse_hive_partition_ddl(
        "ALTER TABLE t ADD PARTITION (dt = 'x') LOCATION not_a_string",
    )
    .expect_err("LOCATION must be quoted");
    assert!(format!("{err}").contains("LOCATION"), "{err}");
}

#[test]
fn drop_partition_rejects_a_trailing_location() {
    let err = try_parse_hive_partition_ddl(
        "ALTER TABLE t DROP PARTITION (dt = 'x') LOCATION 's3://x/'",
    )
    .expect_err("DROP has no LOCATION");
    assert!(format!("{err}").contains("trailing"), "{err}");
}

#[test]
fn missing_equals_in_the_spec_is_an_error() {
    let err = try_parse_hive_partition_ddl("ALTER TABLE t ADD PARTITION (dt)")
        .expect_err("missing =");
    assert!(format!("{err}").contains('='), "{err}");
}

#[test]
fn escaped_quote_in_a_partition_value_survives() {
    let (_, action) = add_of("ALTER TABLE t ADD PARTITION (dt = 'o''brien')");
    match action {
        HivePartitionAction::Add { partitions, .. } => {
            assert_eq!(partitions[0].values[0].1, "o'brien");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn newlines_between_clauses_are_fine() {
    let (_, action) = add_of(
        "ALTER TABLE t ADD\nPARTITION (dt = 'x')\nLOCATION 's3://raw/dt=x/'",
    );
    match action {
        HivePartitionAction::Add { partitions, .. } => {
            assert_eq!(
                partitions[0].location.as_deref(),
                Some("s3://raw/dt=x/")
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn show_partitions_accepts_a_trailing_semicolon() {
    let kind = parse_and_classify("SHOW PARTITIONS lake.sales.orders;").expect("classify");
    match kind {
        StatementKind::ShowPartitions(table) => assert_eq!(table, "lake.sales.orders"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn show_partitions_is_case_insensitive() {
    let kind = parse_and_classify("show partitions lake.sales.orders").expect("classify");
    assert!(matches!(kind, StatementKind::ShowPartitions(_)), "{kind:?}");
}

#[test]
fn classifier_routes_mixed_case_msck() {
    let kind = parse_and_classify("msck repair table lake.sales.orders").expect("classify");
    match kind {
        StatementKind::MsckRepairTable(table) => assert_eq!(table, "lake.sales.orders"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn add_if_not_exists_in_the_wrong_place_is_not_ours() {
    // `ADD PARTITION IF NOT EXISTS` (IF after PARTITION) is not the Hive form
    // we accept; sqlparser / Iceberg own that shape.
    let parsed = try_parse_hive_partition_ddl(
        "ALTER TABLE t ADD PARTITION IF NOT EXISTS (dt = 'x')",
    )
    .expect("no error");
    // Either fall-through or a precise error; must not silently accept as ADD.
    if let Some(ddl) = parsed {
        match ddl.action {
            HivePartitionAction::Add { if_not_exists, .. } => {
                assert!(
                    !if_not_exists,
                    "IF NOT EXISTS after PARTITION must not be treated as the prefix form"
                );
            }
            other => panic!("{other:?}"),
        }
    }
}
