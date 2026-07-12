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
//! once and cached per `RestCatalog` instance. Token refresh is
//! handled by the SDK's credential cache; signing itself is a pure
//! synchronous transformation of the request.
//!
//! Compiled in only when the `aws-sigv4` cargo feature is on, so a
//! REST-only build (Polaris / Nessie / Lakekeeper) doesn't pull in
//! `aws-config`.

use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use bytes::Bytes;
use http::HeaderValue;
use iceberg::{Error, ErrorKind, Result};
use reqwest::Request;
use tokio::sync::OnceCell;

/// Resolves AWS credentials lazily on first use. The default chain
/// (env vars, shared config, IMDS, etc.) is what every other AWS SDK
/// client in the same process uses, so behaviour is consistent.
pub(crate) struct SigV4Signer {
    region: String,
    name: String,
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
    pub(crate) fn new(region: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            name: name.into(),
            provider: OnceCell::new(),
        }
    }

    async fn credentials(&self) -> Result<Credentials> {
        // Lazily build the provider on first request. `load_defaults`
        // is the non-deprecated entry point in aws-config 1.x; we pin
        // a stable BehaviorVersion so SDK upgrades don't silently
        // change credential resolution semantics.
        let provider = self
            .provider
            .get_or_try_init(|| async {
                let conf = aws_config::defaults(aws_config::BehaviorVersion::v2026_01_12())
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
    /// The signed bytes have to match what the wire actually carries,
    /// so we read the request body via `req.body()` and feed it to the
    /// signer as `SignableBody::Bytes`. `reqwest::Request` bodies are
    /// always materialised before a request is built (the catalog
    /// calls `RequestBuilder::body(serde_json::to_vec(...))`), so this
    /// is safe.
    pub(crate) async fn sign(&self, req: &mut Request) -> Result<()> {
        let creds = self.credentials().await?;
        let identity = creds.into();

        let signing_settings = SigningSettings::default();
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

        // `aws-sigv4` wants the body bytes as `SignableBody::Bytes(&[u8])`
        // (so it can hash them into the canonical request). Pull the
        // body out of the reqwest::Request, reattach it after signing.
        let body_bytes: Bytes = match req.body().and_then(|b| b.as_bytes()) {
            Some(b) => Bytes::copy_from_slice(b),
            None => Bytes::new(),
        };

        let url = req.url().to_string();
        let method = req.method().as_str().to_string();

        // aws-sigv4 wants `(name, value)` string slices for headers.
        let header_iter: Vec<(&str, &str)> = req
            .headers()
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.as_str(), v)))
            .collect();

        let signable = SignableRequest::new(
            &method,
            &url,
            header_iter.iter().copied(),
            SignableBody::Bytes(&body_bytes),
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
            let header_name = http::HeaderName::from_bytes(name_str.as_bytes()).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("SigV4 produced an invalid header name {name_str}"),
                )
                .with_source(e)
            })?;
            req.headers_mut().insert(header_name, value);
        }

        Ok(())
    }
}
