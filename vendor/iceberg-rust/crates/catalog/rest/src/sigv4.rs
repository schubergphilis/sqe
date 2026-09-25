// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
//! AWS SigV4 signing for the Iceberg REST endpoints AWS publishes:
//! `https://glue.<region>.amazonaws.com/iceberg` (which federates
//! S3 Tables, Lake Formation, and Glue Iceberg tables) and the
//! per-bucket `https://s3tables.<region>.amazonaws.com/iceberg`
//! variant.
//!
//! The AWS endpoints declare themselves to the client through the
//! standard Iceberg REST `/v1/config` response with three properties:
//!
//!   "rest.sigv4-enabled":  "true"
//!   "rest.signing-name":   "glue" | "s3tables"
//!   "rest.signing-region": "eu-central-1"
//!
//! The Apache Iceberg Java reference implementation switches its auth
//! manager when those land. We do the same here: when `sigv4-enabled`
//! is true, the per-request authenticator skips the OAuth/Bearer path
//! and signs the outgoing `reqwest::Request` with SigV4 instead.
//!
//! Credentials come from the standard AWS provider chain, resolved
//! once and cached per `RestCatalog` instance, unless the catalog
//! properties carry static `s3.access-key-id` / `s3.secret-access-key`
//! keys (SQE: S3-compatible servers such as RustFS sign their REST
//! catalog with the same static keys as the object store). Token
//! refresh is handled by the SDK's credential cache; signing itself is
//! a pure synchronous transformation of the request.
//!
//! Compiled in only when the `aws-sigv4` cargo feature is on, so a
//! REST-only build (Polaris / Nessie / Lakekeeper) doesn't pull in
//! `aws-config`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, SignableBody, SignableRequest, SigningSettings, sign,
};
use aws_sigv4::sign::v4;
use http::HeaderValue;
use iceberg::{Error, ErrorKind, Result};
use tokio::sync::OnceCell;

use crate::auth::{AuthManager, AuthSession};
use crate::client::HttpClient;
use crate::request::HttpRequest;

/// `rest.auth.type` value selecting SigV4 signing (Iceberg Java parity).
pub const AUTH_TYPE_SIGV4: &str = "sigv4";

fn sigv4_enabled(props: &HashMap<String, String>) -> bool {
    props
        .get("rest.sigv4-enabled")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

fn non_empty<'a>(props: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    props.get(key).map(String::as_str).filter(|v| !v.is_empty())
}

/// Wraps the auth manager `rest.auth.type` resolved and swaps in SigV4
/// signing when AWS-style signing applies. AWS's Iceberg REST endpoints
/// (federated `glue`, per-bucket `s3tables`) advertise it in `/v1/config`
/// through `rest.sigv4-enabled`, `rest.signing-name` and
/// `rest.signing-region`; the user config can set the same properties, or
/// `rest.auth.type=sigv4`.
///
/// A server-advertised `rest.sigv4-enabled` never replaces a configured
/// bearer token or credential (SQE issue 748): that would swap the caller's
/// per-user identity for the process's AWS identity on every request (a
/// confused deputy). Only SigV4 the user config itself asked for is used
/// alongside a bearer.
#[derive(Debug)]
pub struct SigV4Manager {
    fallback: Arc<dyn AuthManager>,
    user_requested: bool,
    bearer_bound: bool,
}

impl SigV4Manager {
    /// Wraps `fallback`, deciding from the user-supplied `props`. `forced`
    /// is `rest.auth.type=sigv4`.
    pub fn wrap(
        fallback: Arc<dyn AuthManager>,
        props: &HashMap<String, String>,
        forced: bool,
    ) -> Self {
        Self {
            fallback,
            user_requested: forced || sigv4_enabled(props),
            bearer_bound: non_empty(props, "token").is_some()
                || non_empty(props, "credential").is_some(),
        }
    }

    /// The signer for a session over `props`, or `None` to use the fallback.
    fn signer(&self, props: &HashMap<String, String>) -> Option<SigV4Signer> {
        let wanted = self.user_requested || (sigv4_enabled(props) && !self.bearer_bound);
        if !wanted {
            return None;
        }
        let signer = SigV4Signer::new(
            non_empty(props, "rest.signing-region")?,
            non_empty(props, "rest.signing-name")?,
        );
        // S3-compatible servers that sign their Iceberg REST catalog
        // (RustFS S3 Tables) authenticate it with the object-store keys, so
        // the same `s3.*` keys serve both. Without them the AWS SDK provider
        // chain resolves the identity.
        Some(
            match (
                non_empty(props, "s3.access-key-id"),
                non_empty(props, "s3.secret-access-key"),
            ) {
                (Some(key), Some(secret)) => signer.with_static_credentials(
                    key,
                    secret,
                    props.get("s3.session-token").cloned(),
                ),
                _ => signer,
            },
        )
    }
}

#[async_trait]
impl AuthManager for SigV4Manager {
    async fn init_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        match self.signer(props) {
            Some(signer) => Ok(Box::new(SigV4Session(signer))),
            None => self.fallback.init_session(client, props).await,
        }
    }

    async fn catalog_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        match self.signer(props) {
            Some(signer) => Ok(Arc::new(SigV4Session(signer))),
            None => self.fallback.catalog_session(client, props).await,
        }
    }
}

/// [`AuthSession`] signing every request with SigV4.
#[derive(Debug)]
struct SigV4Session(SigV4Signer);

#[async_trait]
impl AuthSession for SigV4Session {
    async fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
        self.0.sign(request).await
    }
}

/// Resolves AWS credentials lazily on first use. The default chain
/// (env vars, shared config, IMDS, etc.) is what every other AWS SDK
/// client in the same process uses, so behaviour is consistent.
///
/// Public (SQE) so an embedding engine can sign its own raw HTTP calls
/// to the same catalog with the same identity; see
/// [`SigV4Signer::sign_headers`].
pub struct SigV4Signer {
    region: String,
    name: String,
    /// Static keys from the catalog properties. When set, the SDK
    /// provider chain is never consulted.
    static_credentials: Option<Credentials>,
    /// Cached credentials provider. The SDK provider already does its
    /// own caching internally; we just hold the resolved chain.
    provider: OnceCell<aws_credential_types::provider::SharedCredentialsProvider>,
}

impl std::fmt::Debug for SigV4Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigV4Signer")
            .field("region", &self.region)
            .field("name", &self.name)
            .finish()
    }
}

impl SigV4Signer {
    /// A signer for `name` (the SigV4 service name, e.g. `glue`,
    /// `s3tables`, or `s3`) in `region`, resolving credentials through
    /// the AWS SDK default provider chain.
    pub fn new(region: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            name: name.into(),
            static_credentials: None,
            provider: OnceCell::new(),
        }
    }

    /// Sign with fixed keys instead of the SDK provider chain.
    pub fn with_static_credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        self.static_credentials = Some(Credentials::new(
            access_key_id,
            secret_access_key,
            session_token,
            None,
            "iceberg-rest-static",
        ));
        self
    }

    async fn credentials(&self) -> Result<Credentials> {
        if let Some(creds) = &self.static_credentials {
            return Ok(creds.clone());
        }
        // Lazily build the provider on first request. `load_defaults`
        // is the non-deprecated entry point in aws-config 1.x; we pin
        // a stable BehaviorVersion so SDK upgrades don't silently
        // change credential resolution semantics.
        let provider = self
            .provider
            .get_or_try_init(|| async {
                // SQE: supply an explicit rustls (ring) HTTPS client. The
                // `aws-config` dependency has `default-https-client` off
                // because that feature hardcodes the aws-lc provider (and
                // its C build) into every binary; the credential chain
                // still needs HTTP for IMDS, STS and SSO, so it gets the
                // same ring client the vendored Glue/S3 Tables crates use.
                let http_client = aws_smithy_http_client::Builder::new()
                    .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                        aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
                    ))
                    .build_https();
                let conf = aws_config::defaults(aws_config::BehaviorVersion::v2026_01_12())
                    .http_client(http_client)
                    .load()
                    .await;
                conf.credentials_provider().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "AWS credentials provider chain returned no provider",
                    )
                })
            })
            .await?;

        provider.provide_credentials().await.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Failed to resolve AWS credentials for SigV4 signing",
            )
            .with_source(e)
        })
    }

    /// Sign `req` in place, mutating its headers with the SigV4
    /// `Authorization`, `x-amz-date`, and (when applicable)
    /// `x-amz-security-token` lines.
    ///
    /// The signed bytes have to match what the wire carries, so the
    /// body is hashed as-is; a streaming body cannot be signed.
    pub(crate) async fn sign(&self, req: &mut HttpRequest) -> Result<()> {
        let body = req.body().as_bytes().map(<[u8]>::to_vec).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "SigV4 cannot sign a request with a streaming body",
            )
        })?;
        let url = req.url_str().to_string();
        let method = req.method().as_str().to_string();
        self.sign_headers(&method, &url, req.headers_mut(), &body)
            .await
    }

    /// Sign one request given as its parts, adding the SigV4
    /// `Authorization`, `x-amz-date`, `x-amz-content-sha256` (for the
    /// `s3` service) and, when applicable, `x-amz-security-token`
    /// headers to `headers`.
    ///
    /// Works on an `http::HeaderMap`, so callers on any `reqwest` major
    /// (or none) can sign with the same identity the catalog client
    /// uses. `body` must be the exact bytes that go on the wire.
    pub async fn sign_headers(
        &self,
        method: &str,
        url: &str,
        headers: &mut http::HeaderMap,
        body: &[u8],
    ) -> Result<()> {
        let creds = self.credentials().await?;
        let identity = creds.into();

        let mut signing_settings = SigningSettings::default();
        // SQE: servers that authenticate the catalog with S3-style
        // SigV4 (RustFS S3 Tables signs the REST catalog under the
        // `s3` service name) reject requests without the
        // `x-amz-content-sha256` payload hash header ("missing header:
        // x-amz-content-sha256", HTTP 400). The AWS S3 signer always
        // sends it, so mirror that whenever the signing name is `s3`.
        // Glue and Amazon S3 Tables (`glue`, `s3tables`) keep the
        // default header set.
        if self.name == "s3" {
            signing_settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        }
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(&self.name)
            .time(SystemTime::now())
            .settings(signing_settings)
            .build()
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to build AWS SigV4 signing params",
                )
                .with_source(e)
            })?
            .into();

        // aws-sigv4 wants `(name, value)` string slices for headers.
        let header_iter: Vec<(&str, &str)> = headers
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.as_str(), v)))
            .collect();

        let signable = SignableRequest::new(
            method,
            url,
            header_iter.iter().copied(),
            SignableBody::Bytes(body),
        )
        .map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Failed to build SigV4 signable request",
            )
            .with_source(e)
        })?;

        let signing_output = sign(signable, &signing_params).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "AWS SigV4 signing failed").with_source(e)
        })?;

        let (signing_instructions, _signature) = signing_output.into_parts();

        // Apply the signing headers/params. `aws-sigv4`'s `Header`
        // exposes `name()`/`value()` borrows (no into_parts/into_raw
        // helper in 1.x); we copy them out and splice into the
        // request headers.
        let (header_inst, _query_inst) = signing_instructions.into_parts();
        for header in header_inst.iter() {
            let name_str = header.name();
            let value_str = header.value();
            let value = HeaderValue::from_str(value_str).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("SigV4 produced an invalid header value for {name_str}"),
                )
                .with_source(e)
            })?;
            // SAFETY: `name_str` is one of `authorization`,
            // `x-amz-date`, etc., all valid header names by
            // construction.
            let header_name = http::HeaderName::from_bytes(name_str.as_bytes())
                .map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("SigV4 produced an invalid header name {name_str}"),
                    )
                    .with_source(e)
                })?;
            headers.insert(header_name, value);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::NoopAuthManager;

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    const ADVERTISED: [(&str, &str); 3] = [
        ("rest.sigv4-enabled", "true"),
        ("rest.signing-name", "glue"),
        ("rest.signing-region", "eu-central-1"),
    ];

    /// SQE issue 748: a `/v1/config` that advertises `rest.sigv4-enabled`
    /// must not swap a per-user bearer for the process's AWS identity.
    #[test]
    fn server_advertised_sigv4_does_not_replace_a_bearer() {
        let mut merged_with_bearer = props(&ADVERTISED);
        merged_with_bearer.insert("token".to_string(), "user-bearer".to_string());

        let bearer = SigV4Manager::wrap(
            Arc::new(NoopAuthManager),
            &props(&[("token", "user-bearer")]),
            false,
        );
        assert!(
            bearer.signer(&merged_with_bearer).is_none(),
            "server-advertised SigV4 replaced a bearer"
        );

        // No bearer anywhere: the advertised signer is adopted.
        let anonymous = SigV4Manager::wrap(Arc::new(NoopAuthManager), &props(&[]), false);
        assert!(anonymous.signer(&props(&ADVERTISED)).is_some());

        // SigV4 the user asked for is honoured even with a bearer.
        let forced = SigV4Manager::wrap(
            Arc::new(NoopAuthManager),
            &props(&[("token", "user-bearer")]),
            true,
        );
        assert!(forced.signer(&merged_with_bearer).is_some());
    }

    #[test]
    fn sigv4_without_signing_scope_falls_back() {
        let manager = SigV4Manager::wrap(
            Arc::new(NoopAuthManager),
            &props(&[("rest.sigv4-enabled", "true")]),
            false,
        );
        assert!(manager.signer(&props(&[("rest.sigv4-enabled", "true")])).is_none());
    }
}
