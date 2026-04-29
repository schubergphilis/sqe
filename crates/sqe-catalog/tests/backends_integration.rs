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
    use iceberg::{Catalog, NamespaceIdent};
    use sqe_catalog::backends::glue::{GlueBackend, GlueConfig};

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

        let backend = GlueBackend::new(GlueConfig::new(region, warehouse));
        let catalog = backend
            .build_catalog(storage_factory)
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

    /// Pure-config smoke test: exercises the wrapper struct without
    /// touching AWS. Useful in CI where the live test is gated.
    #[test]
    fn glue_config_constructs() {
        let cfg = GlueConfig::new("eu-west-1", "s3://lake/wh");
        assert_eq!(cfg.region, "eu-west-1");
    }
}

// -- HMS (Hive Metastore) ---------------------------------------------------

#[cfg(feature = "hms")]
mod hms {
    use std::sync::Arc;

    use iceberg::io::OpenDalStorageFactory;
    use iceberg::{Catalog, NamespaceIdent};
    use sqe_catalog::backends::hms::{HmsBackend, HmsConfig};

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

        let backend = HmsBackend::new(HmsConfig::new(uri, warehouse));
        let catalog = backend
            .build_catalog(storage_factory)
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
    use sqe_catalog::backends::sql::SqlBackend;
    use tempfile::tempdir;

    #[test]
    fn jdbc_backend_sqlite_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("catalog.db");
        let backend = SqlBackend::open_sqlite(path.to_str().unwrap(), "sqe").unwrap();

        // Start empty.
        assert!(backend.list_namespaces().unwrap().is_empty());

        // Insert two tables across two namespaces.
        backend
            .create_table("sales", "orders", "s3://lake/sales/orders/metadata/v1.metadata.json")
            .unwrap();
        backend
            .create_table("sales", "customers", "s3://lake/sales/customers/metadata/v1.metadata.json")
            .unwrap();
        backend
            .create_table("ops", "events", "s3://lake/ops/events/metadata/v1.metadata.json")
            .unwrap();

        let namespaces = backend.list_namespaces().unwrap();
        assert_eq!(namespaces, vec!["ops".to_string(), "sales".to_string()]);

        let sales = backend.list_tables("sales").unwrap();
        assert_eq!(sales, vec!["customers", "orders"]);

        let ops = backend.list_tables("ops").unwrap();
        assert_eq!(ops, vec!["events"]);

        // Load and drop.
        let entry = backend.load_table("sales", "orders").unwrap().unwrap();
        assert_eq!(entry.name, "orders");

        assert!(backend.drop_table("sales", "orders").unwrap());
        assert_eq!(backend.list_tables("sales").unwrap(), vec!["customers"]);
    }

    #[test]
    #[ignore = "requires PostgreSQL; run with --ignored and SQE_TEST_PG_URL set"]
    fn jdbc_backend_postgres_roundtrip() {
        // Postgres support arrives when the upstream `iceberg-catalog-sql` crate
        // is adopted (task 2.11). Today we document the shape of the test.
        let _url = std::env::var("SQE_TEST_PG_URL")
            .expect("SQE_TEST_PG_URL must be set for this test");
        unimplemented!("PostgreSQL path arrives via upstream iceberg-catalog-sql in task 2.11");
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

// -- Unity Catalog (OIDC M2M) -----------------------------------------------

#[cfg(feature = "rest")]
mod unity_catalog {
    use sqe_auth::{OidcM2mConfig, OidcM2mProvider};

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
}
