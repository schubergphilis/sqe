//! Integration tests for `sqe_coordinator::RuntimeCatalogRegistry`.
//!
//! Each test attaches a real SQLite-backed Iceberg catalog over a
//! tempdir warehouse. Using `CatalogKind::Sqlite` keeps the wiring
//! end to end (parser AST -> mount::build_catalog -> sqlx ->
//! WritableIcebergCatalog -> SessionContext) without standing up a
//! REST endpoint or AWS fixture.

use std::collections::BTreeMap;

use datafusion::execution::context::SessionContext;
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
    let ctx = SessionContext::new();
    let dir = tempfile::tempdir().expect("tempdir");

    registry
        .attach(&sqlite_attach("foo", &dir), &secrets, &ctx)
        .await
        .expect("first attach succeeds");

    assert_eq!(registry.list(), vec!["foo".to_string()]);
}

#[tokio::test]
async fn attach_duplicate_name_errors() {
    let registry = RuntimeCatalogRegistry::new();
    let secrets = SecretStore::new();
    let ctx = SessionContext::new();
    let dir = tempfile::tempdir().expect("tempdir");

    registry
        .attach(&sqlite_attach("foo", &dir), &secrets, &ctx)
        .await
        .expect("first attach succeeds");

    let err = registry
        .attach(&sqlite_attach("foo", &dir), &secrets, &ctx)
        .await
        .expect_err("duplicate attach should error");
    assert!(
        err.contains("already attached"),
        "expected duplicate-name error, got: {err}"
    );
    // Registry state must not have changed.
    assert_eq!(registry.list(), vec!["foo".to_string()]);
}

#[tokio::test]
async fn detach_unknown_errors() {
    let registry = RuntimeCatalogRegistry::new();
    let ctx = SessionContext::new();

    let err = registry
        .detach("nope", &ctx)
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
    let ctx = SessionContext::new();
    let dir = tempfile::tempdir().expect("tempdir");

    registry
        .attach(&sqlite_attach("foo", &dir), &secrets, &ctx)
        .await
        .expect("first attach succeeds");

    registry
        .detach("foo", &ctx)
        .expect("first detach succeeds");
    assert!(registry.list().is_empty());

    // The fresh `SessionContext::new()` uses a `MemoryCatalogProviderList`
    // directly (no `enable_url_table` wrapping), so the detach above
    // also cleared DataFusion's view. A re-attach must not collide.
    registry
        .attach(&sqlite_attach("foo", &dir), &secrets, &ctx)
        .await
        .expect("re-attach after detach should succeed");

    assert_eq!(registry.list(), vec!["foo".to_string()]);
}

#[tokio::test]
async fn referenced_secrets_lists_consumers() {
    let registry = RuntimeCatalogRegistry::new();
    let secrets = SecretStore::new();
    let ctx = SessionContext::new();
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
        .attach(&stmt_a, &secrets, &ctx)
        .await
        .expect("attach cat_a");
    registry
        .attach(&stmt_b, &secrets, &ctx)
        .await
        .expect("attach cat_b");

    let mut consumers = registry.referenced_secrets("shared_creds");
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
    let ctx = SessionContext::new();
    let dir = tempfile::tempdir().expect("tempdir");

    // Attach without any SECRET option.
    registry
        .attach(&sqlite_attach("plain", &dir), &secrets, &ctx)
        .await
        .expect("attach without secret");

    assert!(registry.referenced_secrets("anything").is_empty());
    assert!(registry.referenced_secrets("plain").is_empty());
}

#[tokio::test]
async fn attach_registers_with_session_context() {
    let registry = RuntimeCatalogRegistry::new();
    let secrets = SecretStore::new();
    let ctx = SessionContext::new();
    let dir = tempfile::tempdir().expect("tempdir");

    // Sanity check: name not present before attach.
    assert!(
        ctx.catalog("foo").is_none(),
        "fresh SessionContext should not know about 'foo' yet"
    );

    registry
        .attach(&sqlite_attach("foo", &dir), &secrets, &ctx)
        .await
        .expect("attach succeeds");

    let provider = ctx.catalog("foo").expect("catalog 'foo' is registered");
    // The default fresh SQLite catalog has no namespaces, so
    // `schema_names` returns an empty list. Calling it exercises the
    // wired `WritableIcebergCatalog` end to end.
    assert!(provider.schema_names().is_empty());
}
