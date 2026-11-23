//! One property of the Polaris catalog itself, read and written through the
//! Polaris management API (`GET|PUT /api/management/v1/catalogs/{name}`) on
//! the caller's own token. Catalog tags (#922) live there.
//!
//! Polaris authorizes the PUT as `UPDATE_CATALOG`, so only a principal with
//! catalog-properties write on that catalog can change a key; SQE adds no
//! check of its own. The PUT replaces the whole property map and carries the
//! entity version it read, so a concurrent change answers 409 and the
//! read-modify-write is retried on the new version.

use std::collections::HashMap;

use sqe_core::{CatalogOp, SqeError, SqeErrorCode};
use uuid::Uuid;

use super::{record_breaker_class, transport_failure_class, CatalogHandle, SessionCatalog};
use sqe_core::config::BreakerFailureClass;
use sqe_metrics::propagation::trace_context_http_headers;

/// Version conflicts tolerated before the update gives up.
const MAX_ATTEMPTS: usize = 3;

/// The management API base for a Polaris REST catalog URL:
/// `https://host/api/catalog[/v1]` becomes `https://host/api/management/v1`.
/// `None` for a URL with no `/api/catalog` suffix, which is not Polaris.
pub(super) fn management_base(catalog_url: &str) -> Option<String> {
    let url = catalog_url.trim_end_matches('/');
    let url = url.strip_suffix("/v1").unwrap_or(url);
    url.strip_suffix("/api/catalog")
        .map(|host| format!("{host}/api/management/v1"))
}

/// The parts of a Polaris catalog entity the update needs.
struct CatalogEntity {
    properties: HashMap<String, String>,
    version: i64,
}

fn parse_entity(body: &serde_json::Value) -> Option<CatalogEntity> {
    let version = body.get("entityVersion")?.as_i64()?;
    let properties = body
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                .collect()
        })
        .unwrap_or_default();
    Some(CatalogEntity {
        properties,
        version,
    })
}

impl SessionCatalog {
    /// The properties of this session's Polaris catalog, as the caller may
    /// read them (a principal without catalog-properties read gets 403).
    pub async fn catalog_properties(&self) -> sqe_core::Result<HashMap<String, String>> {
        let url = self.catalog_entity_url()?;
        Ok(self.load_catalog_entity(&url).await?.properties)
    }

    /// Replace catalog property `key` with what `next` makes of its current
    /// value (`None` removes the key). `next` may run more than once: each
    /// version conflict re-reads the catalog and calls it again.
    pub async fn update_catalog_property(
        &self,
        key: &str,
        next: impl Fn(Option<&str>) -> sqe_core::Result<Option<String>>,
    ) -> sqe_core::Result<()> {
        self.ensure_writable()?;
        let url = self.catalog_entity_url()?;
        for _ in 0..MAX_ATTEMPTS {
            let entity = self.load_catalog_entity(&url).await?;
            let mut properties = entity.properties;
            match next(properties.get(key).map(String::as_str))? {
                Some(value) => properties.insert(key.to_string(), value),
                None => properties.remove(key),
            };
            let body = serde_json::json!({
                "currentEntityVersion": entity.version,
                "properties": properties,
            });
            match self
                .send_catalog_request(self.http_client.put(&url).json(&body))
                .await?
            {
                (status, _) if status.is_success() => return Ok(()),
                (reqwest::StatusCode::CONFLICT, _) => continue,
                (status, text) => return Err(self.catalog_entity_error(status, &text)),
            }
        }
        Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
                "catalog '{}' changed {MAX_ATTEMPTS} times while {key} was being written; \
                 retry the statement",
                self.warehouse
            ),
        ))
    }

    /// `{management}/catalogs/{warehouse}`, or a refusal for a catalog that
    /// is not Polaris REST.
    fn catalog_entity_url(&self) -> sqe_core::Result<String> {
        let not_polaris = || {
            SqeError::NotImplemented(format!(
                "catalog '{}' is not a Polaris REST catalog; catalog properties are written \
                 through the Polaris management API",
                self.warehouse
            ))
        };
        if !matches!(self.inner, CatalogHandle::Rest(_)) || self.sigv4.is_some() {
            return Err(not_polaris());
        }
        let base = management_base(&self.catalog_url).ok_or_else(not_polaris)?;
        let mut url = reqwest::Url::parse(&base).map_err(|e| {
            SqeError::catalog_src(format!("bad catalog management URL '{base}': {e}"), e)
        })?;
        url.path_segments_mut()
            .map_err(|()| not_polaris())?
            .push("catalogs")
            .push(&self.warehouse);
        Ok(url.to_string())
    }

    async fn load_catalog_entity(&self, url: &str) -> sqe_core::Result<CatalogEntity> {
        let (status, text) = self.send_catalog_request(self.http_client.get(url)).await?;
        if !status.is_success() {
            return Err(self.catalog_entity_error(status, &text));
        }
        serde_json::from_str(&text)
            .ok()
            .as_ref()
            .and_then(parse_entity)
            .ok_or_else(|| {
                SqeError::Catalog(format!(
                    "Polaris answered catalog '{}' without an entity version",
                    self.warehouse
                ))
            })
    }

    /// Send one management request on the caller's identity, with the
    /// breaker and trace headers every raw catalog call carries.
    async fn send_catalog_request(
        &self,
        mut req: reqwest::RequestBuilder,
    ) -> sqe_core::Result<(reqwest::StatusCode, String)> {
        self.circuit_breaker.check().map_err(SqeError::Catalog)?;
        req = req.header("X-Request-ID", Uuid::new_v4().to_string());
        for (k, v) in trace_context_http_headers() {
            req = req.header(k, v);
        }
        let response = self.execute_raw(req).await?.map_err(|e| {
            record_breaker_class(&self.circuit_breaker, transport_failure_class(&e));
            SqeError::catalog_src(
                format!(
                    "Failed to reach the management API of catalog '{}': {e}",
                    self.warehouse
                ),
                e,
            )
        })?;
        let status = response.status();
        // `from_http_status` classifies error statuses only: a 2xx would
        // read as `ServerError` and lock the caller's token out.
        if status.is_success() {
            self.circuit_breaker.record_success();
        } else {
            record_breaker_class(
                &self.circuit_breaker,
                BreakerFailureClass::from_http_status(status.as_u16()),
            );
        }
        let text = response.text().await.unwrap_or_default();
        Ok((status, text))
    }

    fn catalog_entity_error(&self, status: reqwest::StatusCode, text: &str) -> SqeError {
        let reason = serde_json::from_str::<serde_json::Value>(text)
            .ok()
            .and_then(|v| v.pointer("/error/message")?.as_str().map(str::to_string))
            .unwrap_or_else(|| status.canonical_reason().unwrap_or("").to_string());
        SqeError::CatalogHttp {
            status: status.as_u16(),
            op: CatalogOp::Other,
            body: format!("catalog '{}': {reason}", self.warehouse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn management_base_follows_the_catalog_url() {
        for url in [
            "http://polaris:8181/api/catalog",
            "http://polaris:8181/api/catalog/",
            "http://polaris:8181/api/catalog/v1",
        ] {
            assert_eq!(
                management_base(url).as_deref(),
                Some("http://polaris:8181/api/management/v1"),
                "{url}"
            );
        }
        assert_eq!(management_base("https://glue.example/iceberg"), None);
    }

    fn entity(version: i64, props: serde_json::Value) -> serde_json::Value {
        serde_json::json!({"type": "INTERNAL", "name": "wh", "entityVersion": version, "properties": props})
    }

    async fn polaris() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/catalog/v1/config"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"defaults": {}, "overrides": {}})),
            )
            .mount(&server)
            .await;
        server
    }

    async fn session(server: &MockServer) -> SessionCatalog {
        let cfg: sqe_core::config::CatalogConfig = toml::from_str(&format!(
            "catalog_url = \"{}/api/catalog\"\nwarehouse = \"wh\"",
            server.uri()
        ))
        .unwrap();
        SessionCatalog::for_session_with(&cfg, &Default::default(), None, "tok-cat")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn the_whole_map_goes_back_with_the_version_it_was_read_at() {
        let server = polaris().await;
        Mock::given(method("GET"))
            .and(path("/api/management/v1/catalogs/wh"))
            .and(header("authorization", "Bearer tok-cat"))
            .respond_with(ResponseTemplate::new(200).set_body_json(entity(
                4,
                serde_json::json!({"default-base-location": "s3://b/wh/", "keep": "1"}),
            )))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/api/management/v1/catalogs/wh"))
            .and(header("authorization", "Bearer tok-cat"))
            .and(body_partial_json(serde_json::json!({
                "currentEntityVersion": 4,
                "properties": {"default-base-location": "s3://b/wh/", "keep": "1", "k": "[\"gold\"]"}
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(entity(5, serde_json::json!({}))))
            .expect(1)
            .mount(&server)
            .await;
        session(&server)
            .await
            .update_catalog_property("k", |cur| {
                assert_eq!(cur, None);
                Ok(Some("[\"gold\"]".into()))
            })
            .await
            .unwrap();
        server.verify().await;
    }

    /// A successful answer closes the breaker rather than counting toward
    /// opening it. Found live: five successful reads locked a catalog
    /// admin's token out for 30 seconds.
    #[tokio::test]
    async fn successful_calls_never_open_the_breaker() {
        let server = polaris().await;
        Mock::given(method("GET"))
            .and(path("/api/management/v1/catalogs/wh"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(entity(1, serde_json::json!({"k": "[]"}))),
            )
            .mount(&server)
            .await;
        let catalog = session(&server).await;
        for _ in 0..8 {
            let props = catalog.catalog_properties().await.unwrap();
            assert_eq!(props.get("k").map(String::as_str), Some("[]"));
        }
    }

    #[tokio::test]
    async fn a_version_conflict_rereads_and_retries() {
        let server = polaris().await;
        Mock::given(method("GET"))
            .and(path("/api/management/v1/catalogs/wh"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(entity(1, serde_json::json!({"k": "[\"a\"]"}))),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/management/v1/catalogs/wh"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(entity(2, serde_json::json!({"k": "[\"b\"]"}))),
            )
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(body_partial_json(
                serde_json::json!({"currentEntityVersion": 1}),
            ))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "error": {"message": "currentEntityVersion '2', expected '1'", "code": 409}
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(body_partial_json(
                serde_json::json!({"currentEntityVersion": 2, "properties": {"k": "[\"b\",\"c\"]"}}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(entity(3, serde_json::json!({}))))
            .expect(1)
            .mount(&server)
            .await;
        session(&server)
            .await
            .update_catalog_property("k", |cur| {
                let base = cur.unwrap().trim_end_matches(']').to_string();
                Ok(Some(format!("{base},\"c\"]")))
            })
            .await
            .unwrap();
        server.verify().await;
    }

    #[tokio::test]
    async fn a_polaris_denial_is_returned_with_its_reason() {
        let server = polaris().await;
        Mock::given(method("GET"))
            .and(path("/api/management/v1/catalogs/wh"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(entity(1, serde_json::json!({}))),
            )
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": {"message": "Principal 'alice' is not authorized for op UPDATE_CATALOG", "code": 403}
            })))
            .mount(&server)
            .await;
        let err = session(&server)
            .await
            .update_catalog_property("k", |_| Ok(None))
            .await
            .unwrap_err();
        match err {
            SqeError::CatalogHttp { status, body, .. } => {
                assert_eq!(status, 403);
                assert!(body.contains("UPDATE_CATALOG"), "{body}");
            }
            other => panic!("expected CatalogHttp, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_catalog_url_that_is_not_polaris_is_refused_before_any_request() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"defaults": {}, "overrides": {}})),
            )
            .mount(&server)
            .await;
        let cfg: sqe_core::config::CatalogConfig = toml::from_str(&format!(
            "catalog_url = \"{}\"\nwarehouse = \"wh\"",
            server.uri()
        ))
        .unwrap();
        let err = SessionCatalog::for_session_with(&cfg, &Default::default(), None, "tok")
            .await
            .unwrap()
            .update_catalog_property("k", |_| Ok(None))
            .await
            .unwrap_err();
        assert!(matches!(err, SqeError::NotImplemented(_)), "{err:?}");
    }

    /// #924: with the flag off the catalog entity is never requested; on, the
    /// tags come from it and a refusal is `Unknown` (never an empty list).
    #[tokio::test]
    async fn catalog_tag_state_reads_the_entity_only_when_inheriting() {
        use crate::catalog_tags::CatalogTagState;
        let server = polaris().await;
        Mock::given(method("GET"))
            .and(path("/api/management/v1/catalogs/wh"))
            .respond_with(ResponseTemplate::new(200).set_body_json(entity(
                1,
                serde_json::json!({"sqe.catalog-tags": "[\"pii\"]"}),
            )))
            .expect(1)
            .mount(&server)
            .await;

        let off = session(&server).await;
        assert!(matches!(
            off.catalog_tag_state().await,
            CatalogTagState::NotInherited
        ));
        assert_eq!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|r| r.url.path().starts_with("/api/management"))
                .count(),
            0,
            "flag off: no catalog call"
        );

        let on = session(&server).await.with_inherit_catalog_tags(true);
        match on.catalog_tag_state().await {
            CatalogTagState::Known(tags) => assert_eq!(tags.len(), 1),
            other => panic!("{other:?}"),
        }
        server.verify().await;

        let denied = polaris().await;
        Mock::given(method("GET"))
            .and(path("/api/management/v1/catalogs/wh"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&denied)
            .await;
        let on = session(&denied).await.with_inherit_catalog_tags(true);
        assert!(matches!(
            on.catalog_tag_state().await,
            CatalogTagState::Unknown
        ));
    }
}
