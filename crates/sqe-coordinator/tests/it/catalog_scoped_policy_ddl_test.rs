//! `CREATE POLICY ... ON TAG` under `[policy.ranger] catalog_scoped` (#919).
//!
//! A bare tag in SQL governs the session's catalog only: the store receives
//! `<catalog>.<tag>`, never a bare key that would apply in every catalog. A
//! dotted tag names its catalog and passes through, and without
//! `catalog_scoped` the statement is unchanged.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SessionUser, SqeConfig};
use sqe_policy::{PassthroughEnforcer, PolicyStore, ResolvedPolicy};
use sqe_sql::policy_ddl::{CreatePolicyStatement, PolicyTarget};

/// Records each `CREATE POLICY` the coordinator hands to the backend.
struct Recording {
    scoped: bool,
    created: Mutex<Vec<CreatePolicyStatement>>,
}

#[async_trait]
impl PolicyStore for Recording {
    async fn resolve(
        &self,
        _user: &SessionUser,
        _table_name: &str,
        _namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        Ok(ResolvedPolicy::default())
    }

    fn invalidate_all(&self) {}

    fn catalog_scoped(&self) -> bool {
        self.scoped
    }

    async fn create_policy(&self, stmt: &CreatePolicyStatement) -> sqe_core::Result<()> {
        self.created.lock().unwrap().push(stmt.clone());
        Ok(())
    }
}

fn handler(store: Arc<Recording>) -> QueryHandler {
    let config: SqeConfig = toml::from_str(
        "[coordinator]\n[auth]\n[catalog]\ncatalog_url = \"http://localhost:59999\"\n",
    )
    .expect("minimal config parses");
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        Some(store),
        config,
        None,
        None,
        None,
        None,
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
}

fn admin_in(catalog: &str) -> Session {
    let mut session = Session::new(
        "admin".to_string(),
        sqe_core::SecretString::new("tok".to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        vec!["service_admin".to_string()],
    );
    session.default_catalog = Some(catalog.to_string());
    session
}

async fn created_tags(scoped: bool, catalog: &str, sql: &str) -> Vec<String> {
    let store = Arc::new(Recording {
        scoped,
        created: Mutex::new(Vec::new()),
    });
    handler(store.clone())
        .execute(&admin_in(catalog), sql, None)
        .await
        .expect(sql);
    let created = store.created.lock().unwrap();
    created
        .iter()
        .map(|c| match &c.target {
            PolicyTarget::Tag(tag) => tag.clone(),
            PolicyTarget::Table(_) => panic!("expected a tag policy"),
        })
        .collect()
}

#[tokio::test]
async fn bare_on_tag_is_keyed_to_the_session_catalog() {
    let sql = "CREATE POLICY p ON TAG pii COLUMN MASK MASK_NULL TO GROUP analysts";
    assert_eq!(
        created_tags(true, "sales_wh", sql).await,
        vec!["sales_wh.pii"],
        "a bare tag must not become a key that applies in every catalog"
    );
}

#[tokio::test]
async fn qualified_on_tag_keeps_its_catalog() {
    let sql = "CREATE POLICY p ON TAG ops_wh.pii COLUMN MASK MASK_NULL TO GROUP analysts";
    assert_eq!(
        created_tags(true, "sales_wh", sql).await,
        vec!["ops_wh.pii"]
    );
}

#[tokio::test]
async fn without_catalog_scoped_on_tag_is_unchanged() {
    let sql = "CREATE POLICY p ON TAG pii COLUMN MASK MASK_NULL TO GROUP analysts";
    assert_eq!(created_tags(false, "sales_wh", sql).await, vec!["pii"]);
}
