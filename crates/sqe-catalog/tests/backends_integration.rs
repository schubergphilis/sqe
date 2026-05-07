//! Integration tests for catalog backends.
//!
//! Tests that require external services (live AWS, HMS Thrift, Unity Catalog)
//! are marked `#[ignore]` and run manually with documented credentials. Tests
//! that use in-process fixtures (SQLite, in-memory object store) run by
//! default under their respective Cargo features.
//!
//! Cargo invocation examples:
//!
//! ```bash
//! cargo test -p sqe-catalog --features sql backends_integration
//! cargo test -p sqe-catalog --features hadoop backends_integration
//! cargo test -p sqe-catalog --features glue backends_integration -- --ignored
//! ```

// -- Glue (AWS) -------------------------------------------------------------

#[cfg(feature = "glue")]
mod glue {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::io::OpenDalStorageFactory;
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
    use iceberg_catalog_glue::{
        AWS_REGION_NAME, GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalogBuilder,
    };

    /// Live AWS Glue round-trip: create_namespace -> list_namespaces ->
    /// drop_namespace, against the user's account in eu-central-1 (or
    /// whichever region they configured).
    ///
    /// Reads credentials from the standard AWS provider chain. Use
    /// `.env` (template at `.env.example`) to avoid putting profile
    /// names in shell history:
    ///
    /// ```bash
    /// cp .env.example .env  # then edit AWS_PROFILE/AWS_REGION/warehouse
    /// set -a; source .env; set +a
    /// cargo test -p sqe-catalog --features glue backends_integration -- \
    ///     --ignored glue::live_glue_namespace_round_trip
    /// ```
    ///
    /// The test panics with a clear message if `SQE_TEST_GLUE_WAREHOUSE`
    /// is not set; that's the signal that the operator opted out of the
    /// live AWS path. Glue databases are regional and need a real S3
    /// bucket to exist for the LocationUri.
    #[tokio::test]
    #[ignore = "requires AWS credentials + a pre-created S3 bucket; run with --ignored"]
    async fn live_glue_namespace_round_trip() {
        let warehouse = std::env::var("SQE_TEST_GLUE_WAREHOUSE")
            .expect(
                "SQE_TEST_GLUE_WAREHOUSE must be set to a pre-created \
                 s3:// path, e.g. s3://sqe-glue-it-eu-central-1/wh/. \
                 Copy .env.example to .env and source it.",
            );
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "eu-central-1".into());

        // Glue stores warehouse paths but the namespace round-trip
        // doesn't write Parquet, so the Fs storage factory is enough
        // to satisfy `with_storage_factory`. Same pattern as HMS.
        let storage_factory: Arc<dyn iceberg::io::StorageFactory> =
            Arc::new(OpenDalStorageFactory::Fs);

        // Build the upstream GlueCatalog directly through the loader
        // builder pattern — same path the iceberg-catalog-loader uses
        // when SQE's `[catalog.backend] type = "glue"` is selected.
        let mut props = HashMap::new();
        props.insert(GLUE_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
        props.insert(AWS_REGION_NAME.to_string(), region.clone());
        let catalog = GlueCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load("sqe-glue-it", props)
            .await
            .expect("GlueCatalog builds with the configured AWS profile");

        // Glue database names are restricted to lowercase ASCII +
        // underscore + digits, max 255 chars. UUID hex chunk fits.
        let ns_name = format!("sqe_glue_ci_{}", uuid::Uuid::new_v4().simple());
        let ns = NamespaceIdent::new(ns_name.clone());

        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .unwrap_or_else(|e| panic!("create_namespace({ns_name}) failed: {e}"));

        let listed = catalog
            .list_namespaces(None)
            .await
            .expect("list_namespaces");
        let listed_names: Vec<String> =
            listed.iter().map(|n| n.as_ref().join(".")).collect();
        assert!(
            listed_names.iter().any(|n| n == &ns_name),
            "namespace {ns_name} should appear in list after create. Got first 20: {:?}",
            listed_names.iter().take(20).collect::<Vec<_>>()
        );

        catalog
            .drop_namespace(&ns)
            .await
            .unwrap_or_else(|e| panic!("drop_namespace({ns_name}) failed: {e}"));
    }

    /// Pure-config smoke test: builds a GlueCatalogBuilder without
    /// touching AWS. Verifies the warehouse + region props flow
    /// through the loader path the same way the live test uses them.
    #[tokio::test]
    async fn glue_builder_rejects_empty_warehouse() {
        let storage_factory: Arc<dyn iceberg::io::StorageFactory> =
            Arc::new(OpenDalStorageFactory::Fs);
        // Empty warehouse must fail fast at builder.load() rather than
        // surfacing a confusing AWS error later.
        let res = GlueCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load("sqe-glue-it", HashMap::new())
            .await;
        assert!(
            res.is_err(),
            "empty warehouse should fail fast, got: {res:?}"
        );
    }
}

// -- HMS (Hive Metastore) ---------------------------------------------------

#[cfg(feature = "hms")]
mod hms {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::io::OpenDalStorageFactory;
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
    use iceberg_catalog_hms::{
        HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE, HmsCatalogBuilder,
    };

    /// Live HMS round-trip: create_namespace -> namespace_exists -> drop_namespace.
    ///
    /// Brings a real Thrift round-trip against a Hive metastore container,
    /// proving the vendored `iceberg-catalog-hms` crate (Phase K) works
    /// end-to-end on this fork. Marked `#[ignore]` because it needs the
    /// `docker-compose.hms.yml` stack running:
    ///
    /// ```bash
    /// docker compose -f docker-compose.test.yml \
    ///                -f docker-compose.hms.yml up -d
    /// cargo test -p sqe-catalog --features hms backends_integration -- \
    ///     --ignored hms::
    /// ```
    ///
    /// The test uses a unique namespace name per run (`sqe_hms_ci_<uuid>`)
    /// so concurrent runs don't collide, and cleans up on success.
    /// Failure leaves the namespace behind for inspection.
    #[tokio::test]
    #[ignore = "requires docker-compose HMS stack; run with --ignored"]
    async fn hms_namespace_round_trip() {
        // Upstream HmsCatalog calls `to_socket_addrs()` directly on the
        // configured address, so it wants `host:port` (no scheme prefix).
        // Force IPv4 because Docker port forwarding on macOS doesn't
        // always answer on the IPv6 loopback that `localhost` resolves
        // to first.
        let uri = std::env::var("SQE_TEST_HMS_URI")
            .unwrap_or_else(|_| "127.0.0.1:19083".into());
        let warehouse = std::env::var("SQE_TEST_HMS_WAREHOUSE")
            .unwrap_or_else(|_| "s3a://warehouse/hms/".into());

        // Storage factory: HMS stores warehouse paths but our test
        // doesn't actually open Parquet files. The `Fs` factory
        // satisfies `with_storage_factory` without pulling in S3
        // credentials. (Same pattern the existing iceberg-catalog-sql
        // test below uses on line ~209.)
        let storage_factory: Arc<dyn iceberg::io::StorageFactory> =
            Arc::new(OpenDalStorageFactory::Fs);

        // Build the upstream HmsCatalog directly through the builder
        // pattern — same path iceberg-catalog-loader uses when SQE's
        // `[catalog.backend] type = "hms"` is selected.
        let mut props = HashMap::new();
        props.insert(HMS_CATALOG_PROP_URI.to_string(), uri.clone());
        props.insert(HMS_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
        let catalog = HmsCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load("sqe-hms-it", props)
            .await
            .expect("HmsCatalog builds against the docker stack");

        // Unique-per-run namespace so parallel tests / re-runs don't collide.
        let ns_name = format!("sqe_hms_ci_{}", uuid::Uuid::new_v4().simple());
        let ns = NamespaceIdent::new(ns_name.clone());

        // create_namespace -> list_namespaces -> drop_namespace round-trip.
        // We list (rather than `namespace_exists`) so any name-mangling
        // upstream is visible in the assertion failure rather than
        // hidden behind a bare false.
        catalog
            .create_namespace(&ns, std::collections::HashMap::new())
            .await
            .unwrap_or_else(|e| panic!("create_namespace({ns_name}) failed: {e}"));

        let listed = catalog
            .list_namespaces(None)
            .await
            .expect("list_namespaces");
        let listed_names: Vec<String> = listed
            .iter()
            .map(|n| n.as_ref().join("."))
            .collect();
        assert!(
            listed_names.iter().any(|n| n == &ns_name),
            "namespace {ns_name} should appear in list after create. Got: {listed_names:?}"
        );

        catalog
            .drop_namespace(&ns)
            .await
            .unwrap_or_else(|e| panic!("drop_namespace({ns_name}) failed: {e}"));
    }
}

// -- JDBC / SQL (SQLite) ----------------------------------------------------

#[cfg(feature = "sql")]
mod sql {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::CatalogBuilder;
    use iceberg::io::OpenDalStorageFactory;
    use iceberg_catalog_sql::{
        SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
    };
    use tempfile::tempdir;

    /// Builder smoke test: the vendored `iceberg-catalog-sql` crate
    /// uses sqlx's `Any` driver, which only speaks the database
    /// engines whose drivers are compiled in. The default-features
    /// build of SQE pulls in PostgreSQL via the `sql-postgres` feature;
    /// SQLite is not enabled by default. This test verifies the
    /// builder rejects inputs without a registered driver, surfacing
    /// the same fast-fail path the loader takes when SQE's
    /// `[catalog.backend] type = "jdbc"` is selected with an
    /// unsupported URL scheme.
    ///
    /// The full live SQL round-trip lives in `sql_postgres::*` and
    /// runs against the docker-compose postgres stack.
    #[tokio::test]
    async fn jdbc_sqlite_builder_rejects_unsupported_driver() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("catalog.db");
        let warehouse_path = dir.path().join("warehouse");
        std::fs::create_dir_all(&warehouse_path).unwrap();
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let warehouse = format!("file://{}", warehouse_path.display());

        let mut props = HashMap::new();
        props.insert(SQL_CATALOG_PROP_URI.to_string(), url);
        props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse);

        let storage_factory: Arc<dyn iceberg::io::StorageFactory> =
            Arc::new(OpenDalStorageFactory::Fs);

        let res = SqlCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load("sqe-sqlite-it", props)
            .await;
        // Without `sqlx/sqlite` enabled, sqlx::any reports
        // "no driver found for URL scheme \"sqlite\"". The builder
        // surfaces this as an iceberg::Error rather than panicking,
        // which is what we want for the `type = jdbc` config path.
        let err = res.expect_err(
            "SqlCatalogBuilder should reject sqlite:// without sqlx/sqlite \
             enabled (default SQE build only enables sqlx/postgres)",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("sqlite") || msg.contains("driver"),
            "expected driver-missing error, got: {msg}"
        );
    }
}

// -- iceberg-catalog-sql + Postgres (vendored upstream) ---------------------

#[cfg(feature = "sql-postgres")]
mod sql_postgres {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::io::OpenDalStorageFactory;
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
    use iceberg_catalog_sql::{
        SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
    };

    /// Smoke test: connect to a live Postgres instance, build the vendored
    /// `iceberg-catalog-sql` catalog, create + list + drop a namespace.
    ///
    /// Stack: `docker compose -f docker-compose.test.yml up -d postgres`
    /// then `cargo test -p sqe-catalog --features sql-postgres -- --ignored
    /// jdbc_postgres_namespace_roundtrip`.
    ///
    /// Default `SQE_TEST_PG_URL` matches the docker-compose service:
    /// `postgres://iceberg:iceberg@localhost:15432/iceberg_catalog`.
    #[tokio::test]
    #[ignore = "requires live Postgres; run with --ignored against docker-compose postgres"]
    async fn jdbc_postgres_namespace_roundtrip() {
        let url = std::env::var("SQE_TEST_PG_URL").unwrap_or_else(|_| {
            "postgres://iceberg:iceberg@localhost:15432/iceberg_catalog".to_string()
        });
        // Warehouse path: any reachable file:// dir works for the namespace
        // smoke test because we never call create_table here.
        let warehouse = std::env::var("SQE_TEST_PG_WAREHOUSE")
            .unwrap_or_else(|_| "/tmp/sqe-pg-jdbc-test-warehouse".to_string());

        let mut props = HashMap::new();
        props.insert(SQL_CATALOG_PROP_URI.to_string(), url.clone());
        props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse);

        let catalog = SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::Fs))
            .load("postgres-jdbc-test", props)
            .await
            .expect("SqlCatalog should build against live Postgres");

        let ns = NamespaceIdent::new(format!(
            "sqe_test_ns_{}",
            uuid::Uuid::new_v4().simple()
        ));

        // Best-effort cleanup of any leftover namespace from a previous run.
        let _ = catalog.drop_namespace(&ns).await;

        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .expect("create_namespace should succeed against Postgres");

        let listed = catalog
            .list_namespaces(None)
            .await
            .expect("list_namespaces should succeed");
        assert!(
            listed.iter().any(|n| n == &ns),
            "newly created namespace {ns:?} must appear in list_namespaces"
        );

        catalog
            .drop_namespace(&ns)
            .await
            .expect("drop_namespace should succeed");

        let after = catalog
            .list_namespaces(None)
            .await
            .expect("list_namespaces after drop");
        assert!(
            !after.iter().any(|n| n == &ns),
            "namespace {ns:?} must be gone after drop"
        );
    }

    /// Phase O+ step 2: prove the engine session manager can route
    /// through a JDBC backend end-to-end. Builds an `SqeConfig` with
    /// `catalog.backend = Jdbc { url, warehouse }`, calls
    /// `SessionCatalog::for_session`, and asserts that:
    ///
    ///   1. The trait-only methods (`list_namespaces`, namespace
    ///      round-trip) succeed against live Postgres.
    ///   2. REST-only methods (`create_view`) error with a clear
    ///      "requires the REST catalog backend" message rather than
    ///      silently making an HTTP call to an empty URL.
    ///
    /// Closes the engine-wiring caveat on `sqe:jdbc-catalog:v2`. The
    /// existing `jdbc_postgres_namespace_roundtrip` proves the
    /// library layer; this proves the SQE engine dispatcher.
    #[tokio::test]
    #[ignore = "requires live Postgres; run with --ignored against docker-compose postgres"]
    async fn for_session_dispatches_through_jdbc_backend() {
        use std::sync::Arc;

        use iceberg::Catalog;
        use sqe_catalog::SessionCatalog;
        use sqe_core::config::CatalogBackend;

        let url = std::env::var("SQE_TEST_PG_URL").unwrap_or_else(|_| {
            "postgres://iceberg:iceberg@localhost:15432/iceberg_catalog".to_string()
        });
        let warehouse = std::env::var("SQE_TEST_PG_WAREHOUSE")
            .unwrap_or_else(|_| "/tmp/sqe-pg-jdbc-engine-test-warehouse".to_string());

        // Parse a minimal `SqeConfig` from inline TOML and override
        // the catalog block with the JDBC backend selector. Going
        // through the parser exercises the same path operators take
        // when starting `sqe-server`, and dodges the "all 14 fields
        // by hand" tedium.
        let toml = format!(
            r#"
[coordinator]
flight_sql_port = 50051

[auth]
client_id = "sqe-client"

[catalog]
polaris_url = ""
warehouse = "{warehouse}"

[catalog.backend]
type = "jdbc"
url = "{url}"
warehouse = "{warehouse}"
"#,
            url = url,
            warehouse = warehouse,
        );
        let mut config: sqe_core::config::SqeConfig =
            toml::from_str(&toml).expect("config TOML parses");
        // The `catalog.backend = { type = "jdbc", ... }` shape lands
        // as `CatalogBackend::Jdbc` after deserialisation; sanity
        // check before we use it.
        assert!(matches!(config.catalog.backend, CatalogBackend::Jdbc { .. }));
        // Tighten timeouts to keep the test fast.
        config.catalog.metadata_cache_ttl_secs = 30;

        let session_catalog =
            SessionCatalog::for_session(&config, None, "irrelevant-bearer-for-jdbc")
                .await
                .expect("SessionCatalog::for_session should build through JDBC backend");

        let session_catalog = Arc::new(session_catalog);
        let bridge: Arc<dyn Catalog> = session_catalog.as_catalog();

        // Round-trip a unique namespace to prove writes go through.
        let ns = NamespaceIdent::new(format!(
            "sqe_engine_jdbc_{}",
            uuid::Uuid::new_v4().simple()
        ));
        let _ = bridge.drop_namespace(&ns).await;
        bridge
            .create_namespace(&ns, HashMap::new())
            .await
            .expect("create_namespace through JDBC backend");
        let after_create = bridge
            .list_namespaces(None)
            .await
            .expect("list_namespaces after create");
        assert!(
            after_create.iter().any(|n| n == &ns),
            "newly created namespace {ns:?} should appear in JDBC list"
        );
        bridge
            .drop_namespace(&ns)
            .await
            .expect("drop_namespace through JDBC backend");

        // REST-only path: create_view should fail fast with a clear
        // "requires REST" error rather than an opaque HTTP failure
        // against an empty catalog_url. The TOML at the top of this
        // test deliberately uses the legacy `polaris_url` field name
        // so toml::from_str exercises the serde alias on the parser
        // path. The dispatch path itself does not read catalog_url
        // for the JDBC backend — alias coverage in dispatch is by
        // serde, not by code in catalog_ops.
        let view_err = session_catalog
            .create_view(&ns, "should_not_create", "SELECT 1", &serde_json::json!({}))
            .await
            .expect_err("create_view should error on JDBC backend");
        let msg = format!("{view_err}");
        assert!(
            msg.contains("REST catalog backend") || msg.contains("create_view"),
            "expected REST-backend error mentioning create_view; got: {msg}"
        );
    }

    /// Live verification that the JDBC backend persists Iceberg
    /// format-version 3 metadata round-trip. Closes the matrix caveat
    /// on `sqe:jdbc-catalog:v3`: Phase O+ already proved the dispatcher
    /// + V2 table round-trip; this test creates a V3 table, drops the
    /// catalog handle, reloads, and asserts the metadata still reports
    /// `format-version: 3`.
    ///
    /// The vendored `iceberg-catalog-sql` is format-version-agnostic;
    /// SQLite, PostgreSQL, and MySQL all accept V3 metadata. We pick
    /// PostgreSQL because the docker-compose stack already runs it for
    /// the V2 round-trip, so this test rides the same infrastructure.
    #[tokio::test]
    #[ignore = "requires live Postgres; run with --ignored against docker-compose postgres"]
    async fn jdbc_postgres_v3_table_format_version_roundtrip() {
        use iceberg::TableCreation;
        use iceberg::spec::{
            NestedField, PrimitiveType, Schema as IcebergSchema, Type as IcebergType,
        };

        let url = std::env::var("SQE_TEST_PG_URL").unwrap_or_else(|_| {
            "postgres://iceberg:iceberg@localhost:15432/iceberg_catalog".to_string()
        });
        let warehouse = std::env::var("SQE_TEST_PG_WAREHOUSE")
            .unwrap_or_else(|_| "/tmp/sqe-pg-jdbc-v3-warehouse".to_string());

        let mut props = HashMap::new();
        props.insert(SQL_CATALOG_PROP_URI.to_string(), url.clone());
        props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());

        let catalog = SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::Fs))
            .load("postgres-jdbc-v3-test", props)
            .await
            .expect("SqlCatalog should build against live Postgres");

        // Unique namespace per run so retries don't conflict on the
        // shared docker-compose volume.
        let ns = NamespaceIdent::new(format!(
            "sqe_v3_jdbc_{}",
            uuid::Uuid::new_v4().simple()
        ));
        let _ = catalog.drop_namespace(&ns).await;
        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .expect("create_namespace through JDBC backend");

        // Schema with a single int column. We don't need a V3-only
        // type here; the `format-version=3` table property is what
        // iceberg-rust round-trips through the catalog.
        let schema = IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(
                    1,
                    "id",
                    IcebergType::Primitive(PrimitiveType::Long),
                )
                .into(),
            ])
            .build()
            .expect("schema builder");

        let mut table_props = HashMap::new();
        table_props.insert("format-version".to_string(), "3".to_string());

        let table_name = format!("v3_table_{}", uuid::Uuid::new_v4().simple());
        let creation = TableCreation::builder()
            .name(table_name.clone())
            .schema(schema)
            .properties(table_props)
            .build();

        // create_table writes the manifest under the warehouse root.
        let created = catalog
            .create_table(&ns, creation)
            .await
            .expect("create_table with format-version=3 through JDBC");
        let v_at_create = created.metadata().format_version();

        // Round-trip: drop the in-memory handle and reload via the
        // catalog. If the catalog persisted V3 metadata correctly, the
        // reloaded table reports the same format-version.
        drop(created);
        let table_ident = iceberg::TableIdent::new(ns.clone(), table_name.clone());
        let reloaded = catalog
            .load_table(&table_ident)
            .await
            .expect("load_table after JDBC create");
        let v_at_reload = reloaded.metadata().format_version();

        // Best-effort cleanup before assertions so a failure still
        // leaves the database tidy for the next run.
        let _ = catalog.drop_table(&table_ident).await;
        let _ = catalog.drop_namespace(&ns).await;

        assert_eq!(
            v_at_create,
            iceberg::spec::FormatVersion::V3,
            "create_table should yield format-version=3"
        );
        assert_eq!(
            v_at_reload,
            iceberg::spec::FormatVersion::V3,
            "JDBC reload must preserve format-version=3"
        );
    }
}

// -- Hadoop (storage-only) --------------------------------------------------

#[cfg(feature = "hadoop")]
mod hadoop {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
    use sqe_catalog::backends::hadoop::HadoopBackend;

    #[tokio::test]
    async fn hadoop_backend_auto_discovery() {
        let store = InMemory::new();
        for (path, body) in [
            ("warehouse/sales/orders/metadata/v00001.metadata.json", "{}"),
            ("warehouse/sales/orders/metadata/v00002.metadata.json", "{}"),
            ("warehouse/ops/events/metadata/v00001.metadata.json", "{}"),
        ] {
            let p = ObjectPath::from(path);
            store
                .put(&p, PutPayload::from(bytes::Bytes::from(body.to_string())))
                .await
                .unwrap();
        }
        let arc_store: Arc<dyn ObjectStore> = Arc::new(store);
        let backend = HadoopBackend::new(arc_store, ObjectPath::from("warehouse"));

        let tables = backend.list_tables().await.unwrap();
        assert_eq!(tables.len(), 2);

        let orders = backend.find_table("sales", "orders").await.unwrap().unwrap();
        assert_eq!(orders.version, 2);
        assert_eq!(orders.namespace, vec!["sales".to_string()]);
    }

    #[tokio::test]
    #[ignore = "requires MinIO-backed warehouse; run with --ignored and SQE_TEST_MINIO_URL set"]
    async fn hadoop_backend_minio_integration() {
        // Future: wire to a running MinIO instance and discover real tables.
        unimplemented!("MinIO integration arrives with task 2.16");
    }
}

// -- Nessie (Iceberg REST) --------------------------------------------------

#[cfg(feature = "rest")]
mod nessie {
    use std::collections::HashMap;

    // CatalogBuilder is the trait that adds `.load(name, props)` onto
    // `RestCatalogBuilder::default()`. Without it the call resolves to
    // nothing on the bare struct.
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
    use iceberg_catalog_rest::RestCatalogBuilder;

    /// Live Nessie round-trip via the Iceberg REST adapter.
    ///
    /// Nessie speaks Iceberg REST, so this exercises the same client SQE
    /// uses against Polaris. Proving it works against Nessie's adapter
    /// flips the matrix cell from "REST library compiles" to "verified
    /// against the actual server."
    ///
    /// Stack:
    /// ```bash
    /// docker compose -f docker-compose.test.yml \
    ///                -f docker-compose.nessie.yml up -d
    /// cargo test -p sqe-catalog backends_integration -- \
    ///     --ignored nessie::
    /// ```
    ///
    /// The `prefix` Nessie returns from `/iceberg/v1/config` is
    /// `main|warehouse` (URL-encoded as `main%7Cwarehouse`). The
    /// upstream `RestCatalog` client reads it from the config response
    /// and prepends it to subsequent paths automatically; we don't have
    /// to thread it through.
    #[tokio::test]
    #[ignore = "requires docker-compose Nessie stack; run with --ignored"]
    async fn nessie_namespace_round_trip() {
        // `/iceberg/` is the Iceberg REST mount point Nessie reports
        // back via the config response. Trailing slash matters: the
        // client appends `v1/config?warehouse=...` directly.
        let uri = std::env::var("SQE_TEST_NESSIE_URI")
            .unwrap_or_else(|_| "http://127.0.0.1:19121/iceberg/".into());
        let warehouse = std::env::var("SQE_TEST_NESSIE_WAREHOUSE")
            .unwrap_or_else(|_| "warehouse".into());

        let mut props = HashMap::new();
        props.insert("uri".to_string(), uri);
        props.insert("warehouse".to_string(), warehouse);

        let catalog = RestCatalogBuilder::default()
            .load("sqe-nessie-test".to_string(), props)
            .await
            .expect("RestCatalog builds against Nessie");

        // Unique-per-run namespace so parallel runs don't collide.
        let ns_name = format!("sqe_nessie_ci_{}", uuid::Uuid::new_v4().simple());
        let ns = NamespaceIdent::new(ns_name.clone());

        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .unwrap_or_else(|e| panic!("create_namespace({ns_name}) failed: {e}"));

        let listed = catalog
            .list_namespaces(None)
            .await
            .expect("list_namespaces");
        let listed_names: Vec<String> =
            listed.iter().map(|n| n.as_ref().join(".")).collect();
        assert!(
            listed_names.iter().any(|n| n == &ns_name),
            "namespace {ns_name} should appear in list after create. Got: {listed_names:?}"
        );

        catalog
            .drop_namespace(&ns)
            .await
            .unwrap_or_else(|e| panic!("drop_namespace({ns_name}) failed: {e}"));
    }
}

// -- AWS S3 Tables (Iceberg REST + SigV4) -----------------------------------

#[cfg(feature = "rest")]
mod s3_tables {
    use std::collections::HashMap;

    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
    use iceberg_catalog_rest::RestCatalogBuilder;

    /// Live AWS S3 Tables read-only smoke test through the federated
    /// Glue Iceberg REST endpoint.
    ///
    /// AWS publishes two Iceberg REST surfaces in front of S3 Tables:
    ///
    ///   - `https://glue.<region>.amazonaws.com/iceberg` (federated)
    ///   - `https://s3tables.<region>.amazonaws.com/iceberg` (per-bucket)
    ///
    /// Both speak the standard Iceberg REST protocol but require AWS
    /// SigV4 on every request. Phase P added a `aws-sigv4` feature to
    /// the vendored `iceberg-catalog-rest` crate that swaps the
    /// OAuth/Bearer authenticator for a SigV4 signer when the user
    /// (or the server's `/v1/config` defaults) advertises
    /// `rest.sigv4-enabled=true`.
    ///
    /// This test exercises the smallest end-to-end shape: list the
    /// namespaces visible to the configured AWS principal in the
    /// pre-existing test bucket, then list tables in the first
    /// namespace. No DDL or DML — namespace creation through this
    /// federated endpoint requires Lake Formation grants we don't
    /// own here.
    ///
    /// Run:
    /// ```bash
    /// set -a; source .env; set +a   # AWS_PROFILE, AWS_REGION
    /// cargo test -p sqe-catalog backends_integration -- \
    ///     --ignored s3_tables::list_namespaces_via_glue_rest
    /// ```
    ///
    /// Fails fast with a clear message if `SQE_TEST_S3TABLES_WAREHOUSE`
    /// isn't set; that's the signal that the operator opted out of the
    /// AWS path. The warehouse format is the value AWS returns from
    /// the `/v1/config` `prefix` override:
    /// `<account-id>:s3tablescatalog/<bucket-name>`.
    #[tokio::test]
    #[ignore = "requires AWS credentials + a pre-existing S3 Tables bucket; run with --ignored"]
    async fn list_namespaces_via_glue_rest() {
        let warehouse = std::env::var("SQE_TEST_S3TABLES_WAREHOUSE").expect(
            "SQE_TEST_S3TABLES_WAREHOUSE must be set, e.g. \
             311141556126:s3tablescatalog/iceberg-demo-table-iceberg-data",
        );
        let region =
            std::env::var("AWS_REGION").unwrap_or_else(|_| "eu-central-1".into());
        // Default to the federated Glue endpoint; per-bucket
        // `s3tables.<region>.amazonaws.com/iceberg` is also valid but
        // requires `rest.signing-name=s3tables`.
        let uri = std::env::var("SQE_TEST_S3TABLES_URI").unwrap_or_else(|_| {
            format!("https://glue.{region}.amazonaws.com/iceberg")
        });
        let signing_name =
            std::env::var("SQE_TEST_S3TABLES_SIGNING_NAME").unwrap_or_else(|_| "glue".into());

        let mut props = HashMap::new();
        props.insert("uri".to_string(), uri);
        props.insert("warehouse".to_string(), warehouse);
        // Opt the client into SigV4 mode. AWS also advertises these
        // in the server's `/v1/config` defaults, but we have to set
        // them on the user config so that the very first request
        // (the config fetch itself) is signed.
        props.insert("rest.sigv4-enabled".to_string(), "true".to_string());
        props.insert("rest.signing-name".to_string(), signing_name);
        props.insert("rest.signing-region".to_string(), region.clone());

        let catalog = RestCatalogBuilder::default()
            .load("sqe-s3tables-test".to_string(), props)
            .await
            .expect("RestCatalog builds with SigV4 against AWS Glue Iceberg REST");

        // Listing must succeed. The expected response shape is a
        // single-element vec with at least one namespace; AWS returns
        // an empty list when no namespaces exist, which we treat as a
        // skip rather than a failure (a fresh bucket is a valid
        // state).
        let listed = catalog
            .list_namespaces(None)
            .await
            .expect("list_namespaces should succeed under SigV4");

        if listed.is_empty() {
            eprintln!(
                "S3 Tables warehouse has no namespaces; the SigV4 round-trip \
                 reached the server but there's nothing to enumerate. \
                 Configure a namespace via `aws s3tables` or skip this run."
            );
            return;
        }

        // Drill into the first namespace and list tables. We only
        // assert the call doesn't error; a freshly created namespace
        // legitimately has zero tables.
        let first = listed.first().expect("checked non-empty above").clone();
        let tables = catalog
            .list_tables(&first)
            .await
            .unwrap_or_else(|e| panic!("list_tables({first:?}) failed: {e}"));

        let ns_str = first.as_ref().join(".");
        let table_names: Vec<String> = tables
            .iter()
            .map(|t| format!("{}.{}", t.namespace.as_ref().join("."), t.name))
            .collect();
        eprintln!(
            "S3 Tables round-trip ok: namespace={ns_str} tables={table_names:?}"
        );

        // Sanity check: at least the namespace listing came back
        // without an Authorization-Bearer 403, which is the failure
        // mode if the SigV4 path didn't actually engage.
        assert!(
            !ns_str.is_empty(),
            "namespace string parsed from listing should be non-empty"
        );
        // We deliberately don't assert a specific namespace name
        // because the bucket contents change over time. The presence
        // of any successfully-enumerated namespace plus a clean
        // `list_tables` call is enough to prove the SigV4 wiring.
        let _ = NamespaceIdent::new(ns_str);
    }
}

// -- Unity Catalog (OIDC M2M auth provider) --------------------------------

#[cfg(feature = "rest")]
mod unity_catalog {
    use std::collections::HashMap;

    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
    use iceberg_catalog_rest::RestCatalogBuilder;
    use sqe_auth::{OidcM2mConfig, OidcM2mProvider};

    /// OIDC M2M auth provider smoke test. Verifies the auth path SQE
    /// uses against a Databricks-hosted Unity Catalog with bearer auth
    /// turned on. Independent of the read-only Iceberg REST smoke
    /// below.
    #[tokio::test]
    #[ignore = "requires Unity Catalog M2M credentials; run with --ignored"]
    async fn unity_catalog_m2m_auth_obtains_token() {
        let endpoint = std::env::var("SQE_TEST_UC_TOKEN_ENDPOINT")
            .expect("SQE_TEST_UC_TOKEN_ENDPOINT must be set");
        let client_id = std::env::var("SQE_TEST_UC_CLIENT_ID")
            .expect("SQE_TEST_UC_CLIENT_ID must be set");
        let client_secret = std::env::var("SQE_TEST_UC_CLIENT_SECRET")
            .expect("SQE_TEST_UC_CLIENT_SECRET must be set");

        let cfg = OidcM2mConfig::new(endpoint, client_id, client_secret);
        let provider = OidcM2mProvider::new(cfg).unwrap();

        let token = provider.get_token().await.unwrap();
        assert!(!token.is_empty(), "Unity Catalog returned an empty token");
    }

    /// Live Unity Catalog OSS round-trip via the Iceberg REST adapter.
    ///
    /// Read-only smoke: Unity OSS only implements list/load endpoints
    /// (GET namespaces, GET tables, HEAD table). Create / drop /
    /// commit are not implemented yet (unitycatalog/unitycatalog#3),
    /// so this test asserts what we *can* observe: the bundled
    /// `unity.default.marksheet_uniform` table that ships with the
    /// image.
    ///
    /// Stack:
    /// ```bash
    /// docker compose -f docker-compose.test.yml \
    ///                -f docker-compose.unity.yml up -d
    /// cargo test -p sqe-catalog backends_integration -- \
    ///     --ignored unity_catalog::list_via_unity_rest
    /// ```
    ///
    /// Auth is disabled by default in the unity OSS image, so the
    /// REST client connects without a bearer token. The bearer-auth
    /// path against a Databricks-hosted Unity is exercised separately
    /// by `unity_catalog_m2m_auth_obtains_token`.
    #[tokio::test]
    #[ignore = "requires docker-compose Unity stack; run with --ignored"]
    async fn list_via_unity_rest() {
        // Unity exposes its Iceberg REST surface under
        // /api/2.1/unity-catalog/iceberg/. The trailing slash is
        // optional; the REST client appends `v1/config?warehouse=...`
        // either way.
        let uri = std::env::var("SQE_TEST_UNITY_URI").unwrap_or_else(|_| {
            "http://127.0.0.1:18080/api/2.1/unity-catalog/iceberg".into()
        });
        // `warehouse` is the catalog name in Unity's model, not an
        // S3 URI. The seeded image ships with a catalog called
        // `unity` containing a `default` schema.
        let warehouse =
            std::env::var("SQE_TEST_UNITY_WAREHOUSE").unwrap_or_else(|_| "unity".into());

        let mut props = HashMap::new();
        props.insert("uri".to_string(), uri);
        props.insert("warehouse".to_string(), warehouse);

        let catalog = RestCatalogBuilder::default()
            .load("sqe-unity-test".to_string(), props)
            .await
            .expect("RestCatalog builds against Unity Catalog OSS");

        // The seeded image always carries a `default` namespace.
        let listed = catalog
            .list_namespaces(None)
            .await
            .expect("list_namespaces should succeed against Unity OSS");
        let listed_names: Vec<String> =
            listed.iter().map(|n| n.as_ref().join(".")).collect();
        assert!(
            listed_names.iter().any(|n| n == "default"),
            "expected `default` namespace in Unity OSS seed; got {listed_names:?}"
        );

        // The seeded namespace ships with `marksheet_uniform`. That
        // gives us a stable read-only target without a setup script.
        let default_ns = NamespaceIdent::new("default".to_string());
        let tables = catalog
            .list_tables(&default_ns)
            .await
            .expect("list_tables(default) should succeed against Unity OSS");
        let table_names: Vec<String> = tables.iter().map(|t| t.name.clone()).collect();
        assert!(
            table_names.iter().any(|n| n == "marksheet_uniform"),
            "expected seeded `marksheet_uniform` table in Unity OSS; got {table_names:?}"
        );
    }
}
