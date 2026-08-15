//! Integration tests for the ATTACH/DETACH/CREATE SECRET/DROP SECRET/SHOW SECRETS
//! post-parse hooks in `parse_and_classify`.
//!
//! Spec: docs/superpowers/specs/2026-05-09-attach-catalog-and-secrets-design.md.

use sqe_sql::{parse_and_classify, CatalogKind, OptionValue, SecretKind, StatementKind};

// ---------------------------------------------------------------------------
// ATTACH
// ---------------------------------------------------------------------------

#[test]
fn attach_iceberg_rest_with_warehouse() {
    let kind = parse_and_classify(
        "ATTACH 'https://polaris.example.com/api/catalog' AS polaris \
         (TYPE iceberg_rest, WAREHOUSE 'my_wh')",
    )
    .expect("parses");
    let attach = match kind {
        StatementKind::Attach(a) => *a,
        other => panic!("expected Attach, got {other:?}"),
    };
    assert_eq!(attach.name, "polaris");
    assert_eq!(attach.location, "https://polaris.example.com/api/catalog");
    assert_eq!(attach.kind, CatalogKind::IcebergRest);
    assert_eq!(
        attach.options.get("WAREHOUSE"),
        Some(&OptionValue::String("my_wh".to_string()))
    );
    // TYPE was consumed by the classifier; it should not surface as a generic option.
    assert!(!attach.options.contains_key("TYPE"));
}

#[test]
fn attach_glue_with_region_and_secret() {
    let kind = parse_and_classify(
        "ATTACH 'arn:aws:glue:us-east-1:123:catalog/sales' AS glue \
         (TYPE glue, REGION 'us-east-1', SECRET aws_prod)",
    )
    .expect("parses");
    let attach = match kind {
        StatementKind::Attach(a) => *a,
        other => panic!("expected Attach, got {other:?}"),
    };
    assert_eq!(attach.name, "glue");
    assert_eq!(attach.kind, CatalogKind::Glue);
    assert_eq!(
        attach.options.get("REGION"),
        Some(&OptionValue::String("us-east-1".to_string()))
    );
    assert_eq!(
        attach.options.get("SECRET"),
        Some(&OptionValue::SecretRef("aws_prod".to_string()))
    );
}

#[test]
fn attach_glue_without_extra_options() {
    let kind = parse_and_classify(
        "ATTACH 'arn:aws:glue:us-east-1:123:catalog/default' AS glue (TYPE glue)",
    )
    .expect("parses");
    let attach = match kind {
        StatementKind::Attach(a) => *a,
        other => panic!("expected Attach, got {other:?}"),
    };
    assert_eq!(attach.kind, CatalogKind::Glue);
    assert!(attach.options.is_empty(), "options: {:?}", attach.options);
}

#[test]
fn attach_classifier_name_is_attach() {
    let kind = parse_and_classify(
        "ATTACH 'sqlite:///tmp/local.db' AS local (TYPE sqlite, WAREHOUSE '/tmp/wh')",
    )
    .expect("parses");
    assert_eq!(kind.name(), "attach");
}

#[test]
fn attach_option_keys_are_case_insensitive() {
    // Both `type=...` and `TYPE=...` must classify the same way.
    let kind_lower =
        parse_and_classify("ATTACH 'https://polaris/api' AS p (type iceberg_rest, warehouse 'wh')")
            .expect("lowercase parses");
    let kind_upper =
        parse_and_classify("ATTACH 'https://polaris/api' AS p (TYPE iceberg_rest, WAREHOUSE 'wh')")
            .expect("uppercase parses");
    let lower = match kind_lower {
        StatementKind::Attach(a) => *a,
        _ => panic!("expected Attach"),
    };
    let upper = match kind_upper {
        StatementKind::Attach(a) => *a,
        _ => panic!("expected Attach"),
    };
    assert_eq!(lower, upper);
    assert_eq!(
        lower.options.get("WAREHOUSE"),
        Some(&OptionValue::String("wh".to_string()))
    );
}

#[test]
fn attach_with_unknown_type_is_rejected() {
    let result = parse_and_classify(
        "ATTACH 'arn:aws:glue:us-east-1:123:catalog/x' AS x (TYPE not_a_real_backend)",
    );
    assert!(result.is_err(), "expected error for unknown TYPE");
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.to_ascii_lowercase().contains("type"),
        "error should mention TYPE: {err}"
    );
}

#[test]
fn attach_without_type_option_is_rejected() {
    let result = parse_and_classify("ATTACH 'https://polaris/api' AS polaris (WAREHOUSE 'my_wh')");
    assert!(result.is_err(), "expected error for missing TYPE");
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.to_ascii_lowercase().contains("type"),
        "error should mention TYPE: {err}"
    );
}

#[test]
fn attach_legacy_sqlite_form_does_not_classify_as_attach() {
    // Plain SQLite-style `ATTACH '<file>' AS <name>` (no SQE option list)
    // must not be hijacked by our pre-scan. It should fall through to the
    // existing default behaviour. Today that default is `NotImplemented`,
    // because sqlparser produces `Statement::AttachDatabase` and the
    // classifier has no arm for it. The exact error type is not the point
    // here; the important contract is: not StatementKind::Attach.
    let result = parse_and_classify("ATTACH 'foo.db' AS foo");
    if let Ok(StatementKind::Attach(_)) = result {
        panic!("legacy ATTACH must not be classified as the SQE Attach variant");
    }
}

#[test]
fn attach_trailing_semicolon_is_accepted() {
    let kind = parse_and_classify(
        "ATTACH 'https://polaris/api' AS polaris (TYPE iceberg_rest, WAREHOUSE 'wh');",
    )
    .expect("trailing semicolon parses");
    assert!(matches!(kind, StatementKind::Attach(_)));
}

// ---------------------------------------------------------------------------
// DETACH
// ---------------------------------------------------------------------------

#[test]
fn detach_extracts_name() {
    let kind = parse_and_classify("DETACH polaris").expect("parses");
    let detach = match kind {
        StatementKind::Detach(d) => *d,
        other => panic!("expected Detach, got {other:?}"),
    };
    assert_eq!(detach.name, "polaris");
}

#[test]
fn detach_classifier_name_is_detach() {
    let kind = parse_and_classify("DETACH polaris").expect("parses");
    assert_eq!(kind.name(), "detach");
}

#[test]
fn detach_trailing_semicolon_is_accepted() {
    let kind = parse_and_classify("DETACH polaris;").expect("parses");
    if let StatementKind::Detach(d) = kind {
        assert_eq!(d.name, "polaris");
    } else {
        panic!("expected Detach");
    }
}

// ---------------------------------------------------------------------------
// CREATE SECRET
// ---------------------------------------------------------------------------

#[test]
fn create_secret_aws_with_explicit_credentials() {
    let kind = parse_and_classify(
        "CREATE SECRET aws_prod (TYPE aws, ACCESS_KEY 'AKIA...', \
         SECRET_KEY 'shh', REGION 'us-east-1')",
    )
    .expect("parses");
    let stmt = match kind {
        StatementKind::CreateSecret(s) => *s,
        other => panic!("expected CreateSecret, got {other:?}"),
    };
    assert_eq!(stmt.name, "aws_prod");
    assert_eq!(stmt.kind, SecretKind::Aws);
    assert_eq!(
        stmt.options.get("ACCESS_KEY"),
        Some(&OptionValue::String("AKIA...".to_string()))
    );
    assert_eq!(
        stmt.options.get("SECRET_KEY"),
        Some(&OptionValue::String("shh".to_string()))
    );
    assert_eq!(
        stmt.options.get("REGION"),
        Some(&OptionValue::String("us-east-1".to_string()))
    );
    // TYPE keyword was consumed; it must not appear as an option.
    assert!(!stmt.options.contains_key("TYPE"));
}

#[test]
fn create_secret_bearer_with_token() {
    let kind =
        parse_and_classify("CREATE SECRET polaris_jwt (TYPE bearer, TOKEN 'xyz')").expect("parses");
    let stmt = match kind {
        StatementKind::CreateSecret(s) => *s,
        other => panic!("expected CreateSecret, got {other:?}"),
    };
    assert_eq!(stmt.kind, SecretKind::Bearer);
    assert_eq!(
        stmt.options.get("TOKEN"),
        Some(&OptionValue::String("xyz".to_string()))
    );
}

#[test]
fn create_secret_basic_with_username_password() {
    let kind = parse_and_classify(
        "CREATE SECRET hms_basic (TYPE basic, USERNAME 'alice', PASSWORD 'p@ss')",
    )
    .expect("parses");
    let stmt = match kind {
        StatementKind::CreateSecret(s) => *s,
        other => panic!("expected CreateSecret, got {other:?}"),
    };
    assert_eq!(stmt.kind, SecretKind::Basic);
    assert_eq!(
        stmt.options.get("USERNAME"),
        Some(&OptionValue::String("alice".to_string()))
    );
    assert_eq!(
        stmt.options.get("PASSWORD"),
        Some(&OptionValue::String("p@ss".to_string()))
    );
}

#[test]
fn create_secret_classifier_name_is_create_secret() {
    let kind = parse_and_classify("CREATE SECRET t (TYPE bearer, TOKEN 'x')").expect("parses");
    assert_eq!(kind.name(), "create_secret");
}

#[test]
fn create_secret_unknown_type_is_rejected() {
    let result = parse_and_classify("CREATE SECRET t (TYPE oauth, TOKEN 'x')");
    assert!(result.is_err(), "unknown secret kind should error");
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.to_ascii_lowercase().contains("type") || err.to_ascii_lowercase().contains("oauth"),
        "error should mention TYPE/oauth: {err}"
    );
}

#[test]
fn create_secret_missing_type_is_rejected() {
    let result = parse_and_classify("CREATE SECRET t (TOKEN 'x')");
    assert!(result.is_err(), "missing TYPE should error");
}

#[test]
fn create_secret_string_value_with_single_quotes() {
    // Confirm the value extractor strips outer quotes and keeps inner content verbatim.
    let kind = parse_and_classify("CREATE SECRET t (TYPE bearer, TOKEN 'a/b/c=')").expect("parses");
    if let StatementKind::CreateSecret(stmt) = kind {
        assert_eq!(
            stmt.options.get("TOKEN"),
            Some(&OptionValue::String("a/b/c=".to_string()))
        );
    } else {
        panic!("expected CreateSecret");
    }
}

// ---------------------------------------------------------------------------
// DROP SECRET
// ---------------------------------------------------------------------------

#[test]
fn drop_secret_extracts_name() {
    let kind = parse_and_classify("DROP SECRET aws_prod").expect("parses");
    let stmt = match kind {
        StatementKind::DropSecret(d) => *d,
        other => panic!("expected DropSecret, got {other:?}"),
    };
    assert_eq!(stmt.name, "aws_prod");
}

#[test]
fn drop_secret_classifier_name_is_drop_secret() {
    let kind = parse_and_classify("DROP SECRET aws_prod").expect("parses");
    assert_eq!(kind.name(), "drop_secret");
}

#[test]
fn drop_secret_trailing_semicolon_is_accepted() {
    let kind = parse_and_classify("DROP SECRET aws_prod;").expect("parses");
    assert!(matches!(kind, StatementKind::DropSecret(_)));
}

// ---------------------------------------------------------------------------
// SHOW SECRETS
// ---------------------------------------------------------------------------

#[test]
fn show_secrets_classifies() {
    let kind = parse_and_classify("SHOW SECRETS").expect("parses");
    assert!(matches!(kind, StatementKind::ShowSecrets));
}

#[test]
fn show_secrets_classifier_name_is_show_secrets() {
    let kind = parse_and_classify("SHOW SECRETS").expect("parses");
    assert_eq!(kind.name(), "show_secrets");
}

#[test]
fn show_secrets_case_insensitive() {
    let kind = parse_and_classify("show secrets").expect("parses");
    assert!(matches!(kind, StatementKind::ShowSecrets));
}

#[test]
fn show_secrets_trailing_semicolon_is_accepted() {
    let kind = parse_and_classify("SHOW SECRETS;").expect("parses");
    assert!(matches!(kind, StatementKind::ShowSecrets));
}

// ---------------------------------------------------------------------------
// Regression: pre-existing SHOW classifications must not break
// ---------------------------------------------------------------------------

#[test]
fn show_catalogs_still_classifies() {
    // SHOW CATALOGS is intercepted before sqlparser. SHOW SECRETS lives in the
    // same neighbourhood; make sure we did not steal it.
    let kind = parse_and_classify("SHOW CATALOGS").expect("parses");
    assert!(matches!(kind, StatementKind::ShowCatalogs));
}

#[test]
fn show_grants_still_classifies() {
    let kind = parse_and_classify("SHOW GRANTS ON cat.ns.tbl").expect("SHOW GRANTS still parses");
    assert!(matches!(kind, StatementKind::ShowGrants(_)));
}
