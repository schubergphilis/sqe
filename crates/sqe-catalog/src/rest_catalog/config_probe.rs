//! `GET /v1/config?warehouse=W` with a chosen bearer: the prefix the REST
//! catalog routes a warehouse to for that caller. The coordinator asks it
//! once for the person and once for the engine before a discovered warehouse
//! may read with the engine's keys (`[query] discovery_engine_read`).

use sqe_core::config::CatalogConfig;
use sqe_core::SqeError;

/// The `overrides.prefix` the REST catalog at `catalog.catalog_url` answers
/// for `warehouse` when called with `bearer`. `Ok(None)`: the answer names no
/// prefix (absent, empty or not a string). `Err`: transport failure, a
/// non-2xx status or a body that is not JSON.
pub async fn warehouse_config_prefix(
    catalog: &CatalogConfig,
    warehouse: &str,
    bearer: &str,
) -> sqe_core::Result<Option<String>> {
    let url = format!("{}/v1/config", catalog.catalog_url.trim_end_matches('/'));
    let response = super::shared_http_client_for(catalog)
        .get(&url)
        .query(&[("warehouse", warehouse)])
        .bearer_auth(bearer)
        .send()
        .await
        .map_err(|e| SqeError::Catalog(format!("GET /v1/config for '{warehouse}': {e}")))?;
    let status = response.status();
    if !status.is_success() {
        return Err(SqeError::Catalog(format!(
            "GET /v1/config for '{warehouse}': HTTP {status}"
        )));
    }
    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| SqeError::Catalog(format!("GET /v1/config for '{warehouse}': {e}")))?;
    Ok(body
        .pointer("/overrides/prefix")
        .and_then(serde_json::Value::as_str)
        .filter(|p| !p.is_empty())
        .map(str::to_owned))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn catalog(url: &str) -> CatalogConfig {
        toml::from_str(&format!("catalog_url = \"{url}/\"\nwarehouse = \"t\"")).expect("catalog")
    }

    #[tokio::test]
    async fn config_prefix_sends_the_warehouse_and_bearer() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .and(query_param("warehouse", "ws 1"))
            .and(header("authorization", "Bearer tok"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({"defaults": {}, "overrides": {"prefix": "ws1"}}),
                ),
            )
            .expect(1)
            .mount(&server)
            .await;
        let prefix = warehouse_config_prefix(&catalog(&server.uri()), "ws 1", "tok")
            .await
            .unwrap();
        assert_eq!(prefix.as_deref(), Some("ws1"));
    }

    #[tokio::test]
    async fn config_prefix_absent_or_empty_is_none() {
        for body in [
            serde_json::json!({"defaults": {}, "overrides": {}}),
            serde_json::json!({"overrides": {"prefix": ""}}),
            serde_json::json!({"overrides": {"prefix": 7}}),
        ] {
            let server = MockServer::start().await;
            Mock::given(path("/v1/config"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body.clone()))
                .mount(&server)
                .await;
            let got = warehouse_config_prefix(&catalog(&server.uri()), "w", "t")
                .await
                .unwrap();
            assert_eq!(got, None, "{body}");
        }
    }

    #[tokio::test]
    async fn config_prefix_refusal_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(path("/v1/config"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let err = warehouse_config_prefix(&catalog(&server.uri()), "w", "t")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("403"), "{err}");
    }
}
