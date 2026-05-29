//! Integration tests for `sqe_coordinator::RuntimeCatalogRegistry`.
//!
//! Each test attaches a real SQLite-backed Iceberg catalog over a
//! tempdir warehouse. Using `CatalogKind::Sqlite` keeps the wiring
//! end to end (parser AST -> mount::build_catalog -> sqlx ->
//! WritableIcebergCatalog) without standing up a REST endpoint or AWS
//! fixture. The registry itself no longer touches DataFusion. Every
//! attached catalog's `CatalogProvider` is exposed via `providers()`
//! and re-registered into each new `SessionContext` by
//! `create_session_context`.
//!
//! Run with `cargo test -p sqe-coordinator --features test-sqlite`.

#![cfg(feature = "test-sqlite")]

use std::collections::BTreeMap;

use sqe_coordinator::RuntimeCatalogRegistry;
use sqe_core::SecretStore;
use sqe_sql::{AttachStatement, CatalogKind, OptionValue};
use tempfile::TempDir;

/// Build an `AttachStatement` for a SQLite catalog rooted at `dir`.
fn sqlite_attach(name: &str, dir: &TempDir) -> AttachStatement {
    AttachStatement {
        name: name.to_string(),
        location: dir
            .path()
            .to_str()
            .expect("tempdir path is UTF-8")
            .to_string(),
        kind: CatalogKind::Sqlite,
        options: BTreeMap::new(),
    }
}

#[tokio::test]
async fn attach_then_list_returns_name() {
    let registry = RuntimeCatalogRegistry::new();
    let secrets = SecretStore::new();
    let dir = tempfile::tempdir().expect("tempdir");

    registry
        .attach(&sqlite_attach("foo", &dir), &secrets)
        .await
        .expect("first attach succeeds");

    assert_eq!(registry.list().unwrap(), vec!["foo".to_string()]);
}

#[tokio::test]
async fn attach_duplicate_name_errors() {
    let registry = RuntimeCatalogRegistry::new();
    let secrets = SecretStore::new();
    let dir = tempfile::tempdir().expect("tempdir");

    registry
        .attach(&sqlite_attach("foo", &dir), &secrets)
        .await
        .expect("first attach succeeds");

    let err = registry
        .attach(&sqlite_attach("foo", &dir), &secrets)
        .await
        .expect_err("duplicate attach should error");
    assert!(
        err.contains("already attached"),
        "expected duplicate-name error, got: {err}"
    );
    // Registry state must not have changed.
    assert_eq!(registry.list().unwrap(), vec!["foo".to_string()]);
}

#[tokio::test]
async fn detach_unknown_errors() {
    let registry = RuntimeCatalogRegistry::new();

    let err = registry
        .detach("nope")
        .expect_err("detaching an unknown catalog should error");
    assert!(
        err.contains("not attached"),
        "expected not-attached error, got: {err}"
    );
}

#[tokio::test]
async fn detach_then_attach_again_works() {
    let registry = RuntimeCatalogRegistry::new();
    let secrets = SecretStore::new();
    let dir = tempfile::tempdir().expect("tempdir");

    registry
        .attach(&sqlite_attach("foo", &dir), &secrets)
        .await
        .expect("first attach succeeds");

    registry.detach("foo").expect("first detach succeeds");
    assert!(registry.list().unwrap().is_empty());

    registry
        .attach(&sqlite_attach("foo", &dir), &secrets)
        .await
        .expect("re-attach after detach should succeed");

    assert_eq!(registry.list().unwrap(), vec!["foo".to_string()]);
}

#[tokio::test]
async fn referenced_secrets_lists_consumers() {
    let registry = RuntimeCatalogRegistry::new();
    let secrets = SecretStore::new();
    let dir_a = tempfile::tempdir().expect("tempdir");
    let dir_b = tempfile::tempdir().expect("tempdir");

    // Two attaches, both flagged as consumers of the same secret.
    // We don't need the secret to actually exist for this test —
    // SQLite doesn't read it — only the `SECRET <name>` option in
    // the AST so the registry records the dependency edge.
    let mut stmt_a = sqlite_attach("cat_a", &dir_a);
    stmt_a.options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("shared_creds".to_string()),
    );
    let mut stmt_b = sqlite_attach("cat_b", &dir_b);
    stmt_b.options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("shared_creds".to_string()),
    );

    registry
        .attach(&stmt_a, &secrets)
        .await
        .expect("attach cat_a");
    registry
        .attach(&stmt_b, &secrets)
        .await
        .expect("attach cat_b");

    let mut consumers = registry.referenced_secrets("shared_creds").unwrap();
    consumers.sort();
    assert_eq!(
        consumers,
        vec!["cat_a".to_string(), "cat_b".to_string()],
        "both catalogs should be reported as consumers of shared_creds"
    );
}

#[tokio::test]
async fn referenced_secrets_empty_when_no_consumers() {
    let registry = RuntimeCatalogRegistry::new();
    let secrets = SecretStore::new();
    let dir = tempfile::tempdir().expect("tempdir");

    // Attach without any SECRET option.
    registry
        .attach(&sqlite_attach("plain", &dir), &secrets)
        .await
        .expect("attach without secret");

    assert!(registry.referenced_secrets("anything").unwrap().is_empty());
    assert!(registry.referenced_secrets("plain").unwrap().is_empty());
}

#[tokio::test]
async fn providers_returns_attached_catalogs() {
    let registry = RuntimeCatalogRegistry::new();
    let secrets = SecretStore::new();
    let dir = tempfile::tempdir().expect("tempdir");

    assert!(
        registry.providers().unwrap().is_empty(),
        "fresh registry should expose no providers"
    );

    registry
        .attach(&sqlite_attach("foo", &dir), &secrets)
        .await
        .expect("attach succeeds");

    let providers = registry.providers().unwrap();
    assert_eq!(providers.len(), 1, "one catalog attached");
    assert_eq!(providers[0].0, "foo");
    // The default fresh SQLite catalog has no namespaces, so
    // `schema_names` returns an empty list. Calling it exercises the
    // wired `WritableIcebergCatalog` end to end.
    assert!(providers[0].1.schema_names().is_empty());
}
