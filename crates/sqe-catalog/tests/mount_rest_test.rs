//! Integration tests for `sqe_catalog::mount::build_catalog` with
//! `CatalogKind::IcebergRest`.
//!
//! These tests stand up a wiremock HTTP server, mount the resulting
//! catalog through the public `build_catalog` entry point, and assert
//! the wire calls (`/v1/config`, `/v1/namespaces`) carry the expected
//! shape including the bearer header.

#![cfg(feature = "rest")]

use std::collections::BTreeMap;

use sqe_catalog::build_catalog;
use sqe_core::{Secret, SecretStore};
use sqe_sql::{CatalogKind, OptionValue};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Standard `/v1/config` body the iceberg-rest catalog reads on first
/// touch. Empty `overrides`/`defaults` is the simplest valid response.
const CONFIG_BODY: &str = r#"{"overrides":{},"defaults":{}}"#;

/// Empty namespace list response shape.
const EMPTY_NAMESPACES: &str = r#"{"namespaces":[]}"#;

#[tokio::test]
async fn build_iceberg_rest_against_wiremock() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CONFIG_BODY))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_string(EMPTY_NAMESPACES))
        .mount(&server)
        .await;

    let secrets = SecretStore::new();
    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("my_wh".to_string()),
    );

    let catalog = build_catalog(&server.uri(), CatalogKind::IcebergRest, &options, &secrets)
        .await
        .expect("build_catalog should succeed against wiremock");

    let namespaces = catalog
        .list_namespaces(None)
        .await
        .expect("list_namespaces should round-trip through wiremock");
    assert!(namespaces.is_empty(), "wiremock returned an empty list");
}

#[tokio::test]
async fn build_iceberg_rest_with_bearer_secret() {
    let server = MockServer::start().await;

    // The bearer header must reach the server. Both the config and
    // namespaces endpoints should see `Authorization: Bearer <token>`.
    let token = "test-bearer-token-1234";
    let auth_value = format!("Bearer {token}");

    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .and(header("Authorization", auth_value.as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_string(CONFIG_BODY))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .and(header("Authorization", auth_value.as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_string(EMPTY_NAMESPACES))
        .mount(&server)
        .await;

    let secrets = SecretStore::new();
    secrets
        .create(
            "my_token",
            Secret::Bearer {
                token: token.to_string(),
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("my_wh".to_string()),
    );
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("my_token".to_string()),
    );

    let catalog = build_catalog(&server.uri(), CatalogKind::IcebergRest, &options, &secrets)
        .await
        .expect("build_catalog with bearer secret");

    // If the bearer header was missing, the wiremock matcher would
    // reject the request and list_namespaces would error.
    catalog
        .list_namespaces(None)
        .await
        .expect("list_namespaces with bearer secret");
}

#[tokio::test]
async fn build_iceberg_rest_with_inline_token() {
    let server = MockServer::start().await;

    let token = "inline-token-9876";
    let auth_value = format!("Bearer {token}");

    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .and(header("Authorization", auth_value.as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_string(CONFIG_BODY))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .and(header("Authorization", auth_value.as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_string(EMPTY_NAMESPACES))
        .mount(&server)
        .await;

    let secrets = SecretStore::new();
    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("my_wh".to_string()),
    );
    options.insert("TOKEN".to_string(), OptionValue::String(token.to_string()));

    let catalog = build_catalog(&server.uri(), CatalogKind::IcebergRest, &options, &secrets)
        .await
        .expect("build_catalog with inline TOKEN");

    catalog
        .list_namespaces(None)
        .await
        .expect("list_namespaces with inline TOKEN");
}

#[tokio::test]
async fn build_iceberg_rest_rejects_wrong_secret_kind() {
    let secrets = SecretStore::new();
    secrets
        .create(
            "aws_creds",
            Secret::Aws {
                access_key: Some("AKIA".to_string()),
                secret_key: Some("sk".to_string()),
                session_token: None,
                region: None,
                profile: None,
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("my_wh".to_string()),
    );
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("aws_creds".to_string()),
    );

    let err = build_catalog(
        "http://localhost:9999",
        CatalogKind::IcebergRest,
        &options,
        &secrets,
    )
    .await
    .expect_err("AWS secret must be rejected for iceberg_rest");
    assert!(
        err.contains("type aws") && err.contains("bearer"),
        "expected kind-mismatch message, got: {err}"
    );
}

#[tokio::test]
async fn build_iceberg_rest_requires_warehouse() {
    let secrets = SecretStore::new();
    let options: BTreeMap<String, OptionValue> = BTreeMap::new();
    let err = build_catalog(
        "http://localhost:9999",
        CatalogKind::IcebergRest,
        &options,
        &secrets,
    )
    .await
    .expect_err("missing WAREHOUSE must error");
    assert!(
        err.contains("WAREHOUSE"),
        "expected WAREHOUSE error, got: {err}"
    );
}

#[tokio::test]
async fn build_iceberg_rest_rejects_empty_location() {
    let secrets = SecretStore::new();
    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("my_wh".to_string()),
    );
    let err = build_catalog("", CatalogKind::IcebergRest, &options, &secrets)
        .await
        .expect_err("empty location must error");
    assert!(
        err.contains("location must not be empty"),
        "expected empty-location error, got: {err}"
    );
}
