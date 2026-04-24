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
    use sqe_catalog::backends::glue::{GlueBackend, GlueConfig};

    /// Requires live AWS credentials and a pre-provisioned Glue database.
    /// Skipped by default because it touches real AWS.
    #[tokio::test]
    #[ignore = "requires live AWS credentials; run with --ignored"]
    async fn glue_backend_lists_databases() {
        let region = std::env::var("SQE_TEST_GLUE_REGION").unwrap_or_else(|_| "eu-west-1".into());
        let warehouse = std::env::var("SQE_TEST_GLUE_WAREHOUSE")
            .expect("SQE_TEST_GLUE_WAREHOUSE must be set for this test");
        let backend = GlueBackend::new(GlueConfig::new(region, warehouse));
        let result = backend.list_databases().await;
        // When the real implementation lands (task 2.3) this should succeed.
        // Today the stub returns an error pointing at the task list.
        assert!(result.is_err(), "expected stub error until task 2.3 lands");
    }

    #[test]
    fn glue_config_constructs() {
        let cfg = GlueConfig::new("eu-west-1", "s3://lake/wh");
        assert_eq!(cfg.region, "eu-west-1");
    }
}

// -- HMS (Hive Metastore) ---------------------------------------------------

#[cfg(feature = "hms")]
mod hms {
    use sqe_catalog::backends::hms::{HmsBackend, HmsConfig};

    #[tokio::test]
    #[ignore = "requires docker-compose HMS stack; run with --ignored"]
    async fn hms_backend_lists_tables() {
        let uri = std::env::var("SQE_TEST_HMS_URI")
            .unwrap_or_else(|_| "thrift://localhost:9083".into());
        let warehouse = std::env::var("SQE_TEST_HMS_WAREHOUSE")
            .unwrap_or_else(|_| "s3://lake/warehouse".into());
        let backend = HmsBackend::new(HmsConfig::new(uri, warehouse));
        let result = backend.list_tables("default").await;
        assert!(result.is_err(), "expected stub error until task 2.7 lands");
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
