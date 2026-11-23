//! Engine credential identity, REST side (#934).
//!
//! A catalog with `[catalogs.<name>.credentials]` reads in two steps. The
//! person's own `loadTable` (no delegation header) proves the person may see
//! the table's metadata. Then, only if the policy engine allows the person
//! `select` ([`ReadGate`]), the engine's `client_credentials` token fetches
//! the table's read keys from `/credentials`. Writes never use those keys:
//! they load through a second client that still sends the delegation header
//! as the person.
//!
//! This module holds everything that is specific to that mode so
//! `rest_catalog.rs` only gains the hooks that call into it.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::FileIOBuilder;
use iceberg::table::Table;
use iceberg::TableIdent;
use iceberg_catalog_rest::{
    is_vended_credential_key, select_storage_credential, table_file_io_props, RestCatalog,
    StorageCredential,
};
use serde::Deserialize;
use uuid::Uuid;

use sqe_core::{SqeError, SqeErrorCode};
use sqe_metrics::propagation::trace_context_http_headers;

use super::{
    authenticate_raw_request, namespace_path_segment, CatalogHandle, SessionCatalog,
    ACCESS_DELEGATION_HEADER_PROP, REST_CATALOG_CACHE,
};
use crate::engine_identity::{
    EngineFetchEvent, EngineIdentity, ENGINE_CREDENTIALS_ERROR_TYPE, KEY_SOURCE_ENGINE,
    KEY_SOURCE_PROP, READ_DENIED_PROP,
};

/// Internal catalog property: this session reads through the engine
/// identity. Set by `for_session_with` for a catalog with `credentials`,
/// removed before the properties reach the REST client, and carried to the
/// session through `extra_props`. Never sent anywhere.
pub(crate) const ENGINE_READ_MODE_PROP: &str = "sqe.engine-identity-read";

/// Suffix of the cache "warehouse" segment for tables loaded by the
/// delegated write client, so a write-purpose table (person-vended keys)
/// never serves a read and the reverse.
pub(super) const WRITE_CACHE_TAG: &str = "#write";

/// The storage key entries of one `/credentials` answer. Its `Debug` prints
/// the entry count only, so no log line or `{:?}` can carry a key.
#[derive(Clone)]
pub struct VendedKeys(HashMap<String, String>);

impl VendedKeys {
    /// Keep only the credential entries of `config`. Endpoint, region and
    /// other location properties stay with the person-loaded FileIO.
    pub fn from_config(config: HashMap<String, String>) -> Self {
        Self(
            config
                .into_iter()
                .filter(|(k, _)| is_vended_credential_key(k))
                .collect(),
        )
    }

    /// True when the answer held no key entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn into_inner(self) -> HashMap<String, String> {
        self.0
    }
}

impl std::fmt::Debug for VendedKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VendedKeys(<{} redacted entries>)", self.0.len())
    }
}

#[derive(Deserialize)]
struct CredentialsResponse {
    #[serde(rename = "storage-credentials", default)]
    storage_credentials: Vec<StorageCredential>,
}

/// Rebuild `table` over a FileIO holding `props`, keeping everything else.
/// The vendored `Table` has no way to swap its FileIO. A KMS client held by
/// the original table is not carried over (it is private to the catalog), so
/// an encrypted table loaded this way cannot decrypt.
fn rebuild_with_props(table: &Table, props: HashMap<String, String>) -> sqe_core::Result<Table> {
    let file_io = FileIOBuilder::new(crate::storage_factory())
        .with_props(props)
        .build();
    let mut builder = Table::builder()
        .identifier(table.identifier().clone())
        .file_io(file_io)
        .metadata(table.metadata_ref())
        .readonly(table.readonly())
        .runtime(iceberg::Runtime::current());
    if let Some(location) = table.metadata_location() {
        builder = builder.metadata_location(location);
    }
    builder
        .build()
        .map_err(|e| SqeError::catalog_src(format!("Failed to rebuild table: {e}"), e))
}

/// Build (or take from the shared cache) a REST client for `props`.
pub(super) async fn cached_rest_catalog(
    cache_key: String,
    token_fingerprint: &str,
    props: HashMap<String, String>,
) -> sqe_core::Result<Arc<RestCatalog>> {
    use iceberg::CatalogBuilder;
    if let Some(cached) = REST_CATALOG_CACHE.get(&cache_key).await {
        return Ok(cached);
    }
    let catalog = iceberg_catalog_rest::RestCatalogBuilder::default()
        .with_client(super::shared_vendored_http_client())
        .with_storage_factory(crate::storage_factory())
        .load(format!("sqe-session-{token_fingerprint}"), props)
        .await
        .map_err(|e| SqeError::catalog_src(format!("Failed to create REST catalog: {e}"), e))?;
    let catalog = Arc::new(catalog);
    REST_CATALOG_CACHE.insert(cache_key, catalog.clone()).await;
    Ok(catalog)
}

/// Properties of the delegated client that serves writes in engine mode:
/// the read client's, plus the delegation header.
pub(super) fn delegated_write_props(
    read_props: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut props = read_props.clone();
    props.insert(
        ACCESS_DELEGATION_HEADER_PROP.to_string(),
        "vended-credentials".to_string(),
    );
    props
}

impl SessionCatalog {
    /// Attach the engine identity (token source, select gate, audit sink).
    /// A catalog in engine mode without one refuses every read load.
    pub fn with_engine_identity(mut self, identity: EngineIdentity) -> Self {
        self.engine = Some(identity);
        self
    }

    /// True when this catalog reads through the engine identity.
    pub fn uses_engine_identity(&self) -> bool {
        self.extra_props.contains_key(ENGINE_READ_MODE_PROP)
    }

    /// The identity, or the refusal that stops a load before any key is
    /// fetched: engine mode without a gate must never grant keys.
    fn engine_identity_or_refuse(&self) -> sqe_core::Result<&EngineIdentity> {
        self.engine.as_ref().ok_or_else(|| {
            SqeError::Execution(
                "this catalog reads with the engine credential identity but no read gate is \
                 wired for the session; refusing to load the table"
                    .to_string(),
            )
        })
    }

    /// The client that serves writes and DDL: the delegated one in engine
    /// mode, otherwise the only one.
    pub(super) fn write_handle(&self) -> &CatalogHandle {
        self.write_inner.as_ref().unwrap_or(&self.inner)
    }

    /// A copy of this session that loads tables for a write: it uses the
    /// delegated client, a separate table cache key and no engine identity.
    /// `None` outside engine mode, where one client serves both purposes.
    fn write_view(&self) -> Option<SessionCatalog> {
        let write_inner = self.write_inner.as_ref()?;
        let mut extra_props = self.extra_props.clone();
        extra_props.remove(ENGINE_READ_MODE_PROP);
        Some(SessionCatalog {
            inner: match write_inner {
                CatalogHandle::Rest(c) => CatalogHandle::Rest(c.clone()),
                CatalogHandle::Other(c) => CatalogHandle::Other(c.clone()),
            },
            catalog_url: self.catalog_url.clone(),
            warehouse: self.warehouse.clone(),
            bearer_token: self.bearer_token.clone(),
            token_fingerprint: self.token_fingerprint.clone(),
            storage_config: self.storage_config.clone(),
            http_client: self.http_client.clone(),
            sigv4: self.sigv4.clone(),
            extra_props,
            circuit_breaker: self.circuit_breaker.clone(),
            require_vended_credentials: self.require_vended_credentials,
            table_cache: self.table_cache.clone(),
            scope_guard: self.scope_guard.clone(),
            inherit_schema_tags: self.inherit_schema_tags,
            inherit_catalog_tags: self.inherit_catalog_tags,
            engine: None,
            write_inner: None,
            cache_tag: WRITE_CACHE_TAG,
        })
    }

    /// The session for a write path (the write handler, DDL, maintenance
    /// procedures): in engine mode a copy on the delegated client, so every
    /// load on it carries the person's credentials and none the engine's.
    /// Unchanged outside engine mode.
    pub fn into_write_catalog(self) -> Self {
        match self.write_view() {
            Some(view) => view,
            None => self,
        }
    }

    /// Load `table_ident` for a write (INSERT, UPDATE, DELETE, MERGE, DDL,
    /// maintenance): with the person's delegated credentials, never the
    /// engine's read keys. Identical to [`Self::load_table`] outside engine
    /// mode.
    pub async fn load_table_for_write(&self, table_ident: &TableIdent) -> sqe_core::Result<Table> {
        match self.write_view() {
            Some(view) => view.load_table(table_ident).await,
            None => self.load_table(table_ident).await,
        }
    }

    /// Re-run the gate for a cached engine-keyed table before it is served.
    /// A person whose grant was revoked since the table was cached must not
    /// keep reading with the keys cached for them. `Ok(true)` outside engine
    /// mode.
    pub(super) async fn engine_cached_still_readable(
        &self,
        table_ident: &TableIdent,
        loaded: &Table,
    ) -> sqe_core::Result<bool> {
        if !self.uses_engine_identity() {
            return Ok(true);
        }
        self.gate_may_read(table_ident, loaded).await
    }

    /// Ask the select gate, with the schema's tags when the policy inherits
    /// them (#924). Unreadable schema tags fail the load: no keys on a
    /// policy that cannot be read.
    async fn gate_may_read(
        &self,
        table_ident: &TableIdent,
        loaded: &Table,
    ) -> sqe_core::Result<bool> {
        use crate::schema_tags::SchemaTagState;
        let identity = self.engine_identity_or_refuse()?;
        let schema_tags = match self.schema_tag_state(table_ident.namespace()).await {
            SchemaTagState::NotInherited => Vec::new(),
            SchemaTagState::Known(tags) => tags,
            SchemaTagState::Unknown => {
                return Err(sqe_core::SqeError::Execution(format!(
                    "engine credentials: schema tags of {table_ident} could not be read; tag state unknown"
                )));
            }
        };
        let Some(catalog_tags) = self.catalog_tag_state().await.into_tags() else {
            return Err(sqe_core::SqeError::Execution(format!(
                "engine credentials: catalog tags of {table_ident} could not be read; tag state unknown"
            )));
        };
        identity
            .gate
            .may_read(table_ident, loaded, &schema_tags, &catalog_tags)
            .await
    }

    /// Turn the person's plain `loadTable` answer into the table a read
    /// uses: unchanged outside engine mode; with the gate's yes, over a
    /// FileIO holding the engine's keys for this table; with its no, over a
    /// FileIO with no keys and the read-denied marker.
    pub(super) async fn engine_read_table(
        &self,
        table_ident: &TableIdent,
        table: Table,
    ) -> sqe_core::Result<Table> {
        if !self.uses_engine_identity() {
            return Ok(table);
        }
        let identity = self.engine_identity_or_refuse()?;
        let person_props = table.file_io().config().props().clone();
        if !self.gate_may_read(table_ident, &table).await? {
            let mut props = person_props;
            props.retain(|k, _| !is_vended_credential_key(k));
            props.insert(READ_DENIED_PROP.to_string(), "true".to_string());
            return rebuild_with_props(&table, props);
        }
        let location = table.metadata().location().to_string();
        let (prefix, keys) = self
            .fetch_engine_credentials(identity, table_ident, &location)
            .await?;
        let credential = StorageCredential {
            prefix,
            config: keys.into_inner(),
        };
        let mut props = table_file_io_props(
            &person_props,
            Some(std::slice::from_ref(&credential)),
            &location,
            &HashMap::new(),
        );
        props.insert(KEY_SOURCE_PROP.to_string(), KEY_SOURCE_ENGINE.to_string());
        rebuild_with_props(&table, props)
    }

    /// Fetch the table's read keys as the engine and audit the attempt. Any
    /// failure is an error: there is no fallback to the person's own keys.
    async fn fetch_engine_credentials(
        &self,
        identity: &EngineIdentity,
        table_ident: &TableIdent,
        location: &str,
    ) -> sqe_core::Result<(String, VendedKeys)> {
        let result = self
            .request_engine_credentials(identity, table_ident, location)
            .await;
        identity.audit.record(EngineFetchEvent {
            table: table_ident.clone(),
            client_id: identity.token.client_id().to_string(),
            failure: result
                .as_ref()
                .err()
                .map(|e| format!("{ENGINE_CREDENTIALS_ERROR_TYPE}: {e}")),
        });
        result
    }

    async fn request_engine_credentials(
        &self,
        identity: &EngineIdentity,
        table_ident: &TableIdent,
        location: &str,
    ) -> sqe_core::Result<(String, VendedKeys)> {
        let fail = |detail: String| {
            SqeError::with_code(
                SqeErrorCode::CatalogError,
                format!("engine credentials for {table_ident}: {detail}"),
            )
        };
        let token = identity.token.token().await?;
        let url = self
            .rest_url(&[
                "namespaces",
                &namespace_path_segment(table_ident.namespace()),
                "tables",
                table_ident.name(),
                "credentials",
            ])
            .await;
        let mut req = self
            .http_client
            .get(url)
            .header("X-Request-ID", Uuid::new_v4().to_string());
        for (k, v) in trace_context_http_headers() {
            req = req.header(k, v);
        }
        let request = authenticate_raw_request(req, self.sigv4.as_deref(), &token).await?;
        let response = self
            .http_client
            .execute(request)
            .await
            .map_err(|e| fail(format!("request failed: {e}")))?;
        let status = response.status();
        if !status.is_success() {
            return Err(fail(format!("the catalog answered {}", status.as_u16())));
        }
        let body: CredentialsResponse = response
            .json()
            .await
            .map_err(|e| fail(format!("unreadable answer: {e}")))?;
        let chosen = select_storage_credential(Some(&body.storage_credentials), location)
            .ok_or_else(|| fail(format!("no storage credential covers {location}")))?;
        let keys = VendedKeys::from_config(chosen.config.clone());
        if keys.is_empty() {
            return Err(fail("the selected credential holds no keys".to_string()));
        }
        Ok((chosen.prefix.clone(), keys))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vended_keys_debug_holds_no_key_material() {
        let keys = VendedKeys::from_config(HashMap::from([
            (
                "s3.access-key-id".to_string(),
                "AKIAENGINESECRET".to_string(),
            ),
            (
                "s3.secret-access-key".to_string(),
                "sekrit-value".to_string(),
            ),
            ("s3.session-token".to_string(), "tok-value".to_string()),
            ("s3.endpoint".to_string(), "http://dropped".to_string()),
        ]));
        let shown = format!("{keys:?} {:#?}", keys);
        for needle in ["AKIAENGINESECRET", "sekrit-value", "tok-value", "s3.secret"] {
            assert!(!shown.contains(needle), "{needle} leaked in {shown}");
        }
        assert!(shown.contains("3 redacted"), "{shown}");
    }

    // ---- wiremock: the engine read path (#934) ----

    use crate::engine_identity::{
        is_engine_keyed, is_read_denied, refuse_engine_keyed, EngineAudit, EngineFetchEvent,
        EngineIdentity, EngineTokenSource, ReadGate,
    };
    use iceberg::table::Table;
    use iceberg::NamespaceIdent;
    use std::sync::Mutex;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const LOAD_TABLE_RESPONSE: &str = include_str!(
        "../../../../vendor/iceberg-rust/crates/catalog/rest/testdata/load_table_response.json"
    );
    const ORDERS_PATH: &str = "/v1/wh/namespaces/sales/tables/orders";
    const CREDENTIALS_PATH: &str = "/v1/wh/namespaces/sales/tables/orders/credentials";

    fn orders() -> TableIdent {
        TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "orders".to_string(),
        )
    }

    struct FixedGate(bool);

    #[async_trait::async_trait]
    impl ReadGate for FixedGate {
        async fn may_read(
            &self,
            _: &TableIdent,
            _: &Table,
            _: &[sqe_core::column_tags::ColumnTag],
            _: &[sqe_core::column_tags::ColumnTag],
        ) -> sqe_core::Result<bool> {
            Ok(self.0)
        }
    }

    struct FixedToken;

    #[async_trait::async_trait]
    impl EngineTokenSource for FixedToken {
        fn client_id(&self) -> &str {
            "sqe-engine"
        }
        async fn token(&self) -> sqe_core::Result<String> {
            Ok("engine-token".to_string())
        }
    }

    #[derive(Default)]
    struct Recorder(Mutex<Vec<EngineFetchEvent>>);

    impl EngineAudit for Arc<Recorder> {
        fn record(&self, event: EngineFetchEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    /// `/v1/config`, the person's `loadTable` (any headers) and the engine's
    /// `/credentials` answer.
    async fn mock_catalog(credentials: ResponseTemplate) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "defaults": {}, "overrides": {"prefix": "wh"}
            })))
            .mount(&server)
            .await;
        let mut body: serde_json::Value = serde_json::from_str(LOAD_TABLE_RESPONSE).unwrap();
        body["config"] = serde_json::json!({
            "s3.access-key-id": "PERSON", "s3.secret-access-key": "person-secret"
        });
        Mock::given(method("GET"))
            .and(path(ORDERS_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(CREDENTIALS_PATH))
            .respond_with(credentials)
            .mount(&server)
            .await;
        server
    }

    fn engine_keys(prefix: &str) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "storage-credentials": [{
                "prefix": prefix,
                "config": {
                    "s3.access-key-id": "ENGINE",
                    "s3.secret-access-key": "engine-secret",
                    "s3.session-token": "engine-token-key",
                }
            }]
        }))
    }

    async fn engine_session(
        server: &MockServer,
        gate: Option<bool>,
    ) -> (SessionCatalog, Arc<Recorder>) {
        let cfg: sqe_core::config::CatalogConfig = toml::from_str(&format!(
            "catalog_url = \"{}\"\nwarehouse = \"wh\"\nrequire_vended_credentials = true\n\
             [credentials]\ntype = \"client_credentials\"\ntoken_endpoint = \"http://idp/token\"\n\
             client_id = \"sqe-engine\"\nclient_secret = \"s\"\n",
            server.uri()
        ))
        .unwrap();
        let recorder = Arc::new(Recorder::default());
        let mut session =
            SessionCatalog::for_session_with(&cfg, &Default::default(), None, "person")
                .await
                .unwrap();
        if let Some(allow) = gate {
            session = session.with_engine_identity(EngineIdentity::new(
                Arc::new(FixedToken),
                Arc::new(FixedGate(allow)),
                Arc::new(Arc::clone(&recorder)),
            ));
        }
        (session, recorder)
    }

    async fn hits(server: &MockServer, p: &str) -> Vec<wiremock::Request> {
        server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.url.path() == p)
            .collect()
    }

    #[tokio::test]
    async fn engine_credential_read_sends_no_delegation_and_uses_engine_keys() {
        let server = mock_catalog(engine_keys("s3://warehouse/database")).await;
        let (session, audit) = engine_session(&server, Some(true)).await;
        let table = session.load_table(&orders()).await.expect("engine read");
        let props = table.file_io().config().props();
        assert_eq!(props["s3.access-key-id"], "ENGINE");
        assert_eq!(props["s3.secret-access-key"], "engine-secret");
        assert!(is_engine_keyed(&table));
        assert!(!is_read_denied(&table));
        for r in server.received_requests().await.unwrap() {
            assert!(
                r.headers.get("x-iceberg-access-delegation").is_none(),
                "{} carried the delegation header",
                r.url.path()
            );
        }
        let creds = hits(&server, CREDENTIALS_PATH).await;
        assert_eq!(creds.len(), 1);
        assert_eq!(
            creds[0].headers.get("authorization").unwrap(),
            "Bearer engine-token"
        );
        let events = audit.0.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].failure.is_none());
        assert_eq!(events[0].client_id, "sqe-engine");
    }

    #[tokio::test]
    async fn engine_credential_denied_table_gets_no_keys_and_no_fetch() {
        let server = mock_catalog(engine_keys("s3://warehouse/database")).await;
        let (session, audit) = engine_session(&server, Some(false)).await;
        let table = session
            .load_table(&orders())
            .await
            .expect("denied is not an error");
        let props = table.file_io().config().props();
        assert!(is_read_denied(&table));
        assert!(!is_engine_keyed(&table));
        assert!(
            !props.contains_key("s3.access-key-id"),
            "no person keys either"
        );
        assert!(
            hits(&server, CREDENTIALS_PATH).await.is_empty(),
            "no engine fetch"
        );
        assert!(audit.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn engine_credential_without_a_gate_fails_the_load() {
        let server = mock_catalog(engine_keys("s3://warehouse/database")).await;
        let (session, _) = engine_session(&server, None).await;
        assert!(session.load_table(&orders()).await.is_err());
        assert!(hits(&server, CREDENTIALS_PATH).await.is_empty());
    }

    #[tokio::test]
    async fn engine_credential_no_prefix_match_fails_and_is_audited() {
        let server = mock_catalog(engine_keys("s3://elsewhere/")).await;
        let (session, audit) = engine_session(&server, Some(true)).await;
        let err = session
            .load_table(&orders())
            .await
            .expect_err("no covering credential");
        assert!(
            err.to_string().contains("no storage credential covers"),
            "{err}"
        );
        let events = audit.0.lock().unwrap();
        assert_eq!(events.len(), 1);
        let failure = events[0].failure.as_deref().unwrap();
        assert!(failure.starts_with("engine_credentials:"), "{failure}");
    }

    #[tokio::test]
    async fn engine_credential_catalog_refusal_has_no_fallback() {
        for status in [403, 404] {
            let server = mock_catalog(ResponseTemplate::new(status)).await;
            let (session, audit) = engine_session(&server, Some(true)).await;
            assert!(session.load_table(&orders()).await.is_err(), "{status}");
            assert_eq!(audit.0.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn engine_credential_write_load_uses_the_delegated_client() {
        let server = mock_catalog(engine_keys("s3://warehouse/database")).await;
        let (session, _) = engine_session(&server, Some(true)).await;
        let table = session
            .load_table_for_write(&orders())
            .await
            .expect("write load");
        assert!(
            !is_engine_keyed(&table),
            "a write table never holds engine keys"
        );
        assert!(refuse_engine_keyed(&table, "write").is_ok());
        let loads = hits(&server, ORDERS_PATH).await;
        assert!(
            loads
                .iter()
                .any(|r| r.headers.get("x-iceberg-access-delegation").is_some()),
            "the write load asks for vended keys"
        );
        assert!(hits(&server, CREDENTIALS_PATH).await.is_empty());
    }

    #[tokio::test]
    async fn engine_credential_reloader_path_fetches_fresh_engine_keys() {
        let server = mock_catalog(engine_keys("s3://warehouse/database")).await;
        let (session, _) = engine_session(&server, Some(true)).await;
        session.load_table(&orders()).await.unwrap();
        let table = session
            .load_table_uncached(&orders())
            .await
            .expect("reload");
        assert!(is_engine_keyed(&table));
        assert_eq!(
            table.file_io().config().props()["s3.access-key-id"],
            "ENGINE"
        );
        assert_eq!(
            hits(&server, CREDENTIALS_PATH).await.len(),
            2,
            "a key refresh per reload"
        );
        for r in hits(&server, ORDERS_PATH).await {
            assert!(r.headers.get("x-iceberg-access-delegation").is_none());
        }
    }

    #[tokio::test]
    async fn engine_credential_keyed_table_is_refused_for_writes() {
        let server = mock_catalog(engine_keys("s3://warehouse/database")).await;
        let (session, _) = engine_session(&server, Some(true)).await;
        let table = session.load_table(&orders()).await.unwrap();
        let err = session
            .check_write_access(&orders(), &table)
            .await
            .expect_err("engine-keyed table must not be written");
        assert!(err.to_string().to_lowercase().contains("engine"), "{err}");
    }

    #[test]
    fn vended_keys_drop_location_entries() {
        let keys = VendedKeys::from_config(HashMap::from([(
            "s3.endpoint".to_string(),
            "http://x".to_string(),
        )]));
        assert!(keys.is_empty());
    }
}
