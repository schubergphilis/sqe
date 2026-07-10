//! Tests for [`sqe_catalog::aws_config::build_aws_config`].
//!
//! Each test sets `AWS_EC2_METADATA_DISABLED=true` so that the credential
//! chain does not block waiting on a (probably absent) IMDS endpoint when
//! the test does not provide explicit credentials. Real CI runners
//! sometimes have an IMDS server, sometimes do not; turning IMDS off
//! makes the tests deterministic on either kind of host.

#![cfg(all(feature = "glue", feature = "s3tables"))]

use std::collections::BTreeMap;

use aws_credential_types::provider::ProvideCredentials;
use sqe_catalog::aws_config::build_aws_config;
use sqe_core::{Secret, SecretStore};
use sqe_sql::OptionValue;

/// Disable IMDS / EC2 metadata service lookups for the duration of a
/// test. Some hosts respond to IMDS, others time out for several seconds;
/// pinning this off keeps the tests fast and deterministic. Safety: the
/// env var is process-global, but tests in this file all want it set, so
/// the simplest path is to set it in every test rather than coordinate.
fn disable_imds() {
    // Safety: we are in a test binary; concurrent test threads inside
    // the same process all want this var set, so a duplicated set is a
    // no-op rather than a race.
    unsafe {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
}

#[tokio::test]
async fn explicit_secret_uses_those_creds() {
    disable_imds();
    let secrets = SecretStore::new();
    secrets
        .create(
            "aws_test",
            Secret::Aws {
                access_key: Some("AKIAFAKEKEY12345".to_string()),
                secret_key: Some("fake/secret/value".to_string()),
                session_token: None,
                region: Some("us-east-1".to_string()),
                profile: None,
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("aws_test".to_string()),
    );

    let cfg = build_aws_config(&options, &secrets)
        .await
        .expect("build_aws_config");
    // Region from the secret should win.
    assert_eq!(cfg.region().map(|r| r.as_ref()), Some("us-east-1"));
    // Resolve the layered credentials and confirm they match the secret.
    let provider = cfg
        .credentials_provider()
        .expect("explicit credentials provider attached");
    let creds = provider
        .provide_credentials()
        .await
        .expect("provider returns explicit creds without I/O");
    assert_eq!(creds.access_key_id(), "AKIAFAKEKEY12345");
    assert_eq!(creds.secret_access_key(), "fake/secret/value");
    assert_eq!(creds.session_token(), None);
}

#[tokio::test]
async fn region_option_overrides_secret_region() {
    disable_imds();
    let secrets = SecretStore::new();
    secrets
        .create(
            "aws_west",
            Secret::Aws {
                access_key: Some("AKIA-X".to_string()),
                secret_key: Some("secret-x".to_string()),
                session_token: None,
                region: Some("us-west-2".to_string()),
                profile: None,
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("aws_west".to_string()),
    );
    options.insert(
        "REGION".to_string(),
        OptionValue::String("eu-west-1".to_string()),
    );

    let cfg = build_aws_config(&options, &secrets)
        .await
        .expect("build_aws_config");
    // The REGION option overrides the secret-supplied region.
    assert_eq!(cfg.region().map(|r| r.as_ref()), Some("eu-west-1"));
}

#[tokio::test]
async fn no_secret_falls_through_to_chain() {
    disable_imds();
    let secrets = SecretStore::new();
    let options: BTreeMap<String, OptionValue> = BTreeMap::new();
    // Just check the call returns an SdkConfig without panicking. With
    // IMDS disabled and no explicit secret / env / profile, the loader
    // produces a config with no resolvable credentials, but the load
    // itself succeeds and we get a usable handle back.
    let _cfg = build_aws_config(&options, &secrets)
        .await
        .expect("build_aws_config loads without explicit creds");
}

#[tokio::test]
async fn wrong_secret_kind_errors() {
    disable_imds();
    let secrets = SecretStore::new();
    secrets
        .create(
            "bad",
            Secret::Bearer {
                token: "totally-not-aws".to_string(),
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("bad".to_string()),
    );

    let err = build_aws_config(&options, &secrets)
        .await
        .expect_err("Bearer secret must be rejected for AWS layering");
    assert!(
        err.contains("not of type aws"),
        "error should call out the kind mismatch, got: {err}"
    );
}
