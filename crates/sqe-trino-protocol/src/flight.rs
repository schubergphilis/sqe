//! `FlightSqlClient`: the shared wrapper over `FlightSqlServiceClient`.
//!
//! `sqe-cli` and `sqe-bench` each carried their own channel builder,
//! handshake and `do_get` loop (review 705). This module owns those three
//! steps: build a tonic channel (TLS for `https://`), authenticate (Basic
//! handshake or a pre-issued bearer), and run a statement either collecting
//! every batch or streaming them.
//!
//! It lives in the Trino-protocol crate behind the `flight` feature rather
//! than in a fourth crate: both binaries already depend on this crate for
//! the HTTP client, and the extra dependency weight (`arrow-flight`, `tonic`)
//! is one they both carry regardless.

use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use tonic::transport::{Channel, ClientTlsConfig};

#[derive(Debug, thiserror::Error)]
pub enum FlightClientError {
    #[error("invalid Flight endpoint {url:?}: {message}")]
    InvalidEndpoint { url: String, message: String },
    #[error("TLS configuration for {url} failed: {source}")]
    Tls {
        url: String,
        #[source]
        source: tonic::transport::Error,
    },
    #[error("failed to connect to {url}: {source}")]
    Connect {
        url: String,
        #[source]
        source: tonic::transport::Error,
    },
    #[error("authentication failed: {0}")]
    Handshake(#[source] FlightError),
    #[error("handshake returned a token that is not UTF-8")]
    TokenNotUtf8,
    #[error("query failed: {0}")]
    Query(#[source] FlightError),
    #[error("failed to fetch results: {0}")]
    Fetch(#[source] FlightError),
    #[error("result stream failed: {0}")]
    Stream(#[from] FlightError),
    #[error("Flight endpoint returned no ticket")]
    NoTicket,
}

/// Channel tuning. `Default` sets nothing beyond TLS-for-https, which is what
/// an interactive CLI wants; the bench sets keep-alives and a long timeout.
#[derive(Debug, Clone, Default)]
pub struct ChannelOptions {
    /// Per-request deadline.
    pub timeout: Option<Duration>,
    pub connect_timeout: Option<Duration>,
    pub http2_keep_alive_interval: Option<Duration>,
    pub keep_alive_timeout: Option<Duration>,
    pub keep_alive_while_idle: bool,
}

/// Normalise a host or URL to something `Channel::from_shared` accepts:
/// `host:port` becomes `http://host:port`.
pub fn normalise_endpoint(url: &str) -> String {
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_string()
    } else {
        format!("http://{url}")
    }
}

/// Build and connect a tonic channel. `https://` endpoints get TLS with the
/// root store the workspace `tonic` features enable (finding 603).
pub async fn connect_channel(
    url: &str,
    opts: &ChannelOptions,
) -> Result<Channel, FlightClientError> {
    let url = normalise_endpoint(url);
    let mut endpoint =
        Channel::from_shared(url.clone()).map_err(|e| FlightClientError::InvalidEndpoint {
            url: url.clone(),
            message: e.to_string(),
        })?;
    if url.starts_with("https://") {
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .map_err(|source| FlightClientError::Tls {
                url: url.clone(),
                source,
            })?;
    }
    if let Some(t) = opts.timeout {
        endpoint = endpoint.timeout(t);
    }
    if let Some(t) = opts.connect_timeout {
        endpoint = endpoint.connect_timeout(t);
    }
    if let Some(t) = opts.http2_keep_alive_interval {
        endpoint = endpoint.http2_keep_alive_interval(t);
    }
    if let Some(t) = opts.keep_alive_timeout {
        endpoint = endpoint.keep_alive_timeout(t);
    }
    if opts.keep_alive_while_idle {
        endpoint = endpoint.keep_alive_while_idle(true);
    }
    endpoint
        .connect()
        .await
        .map_err(|source| FlightClientError::Connect { url, source })
}

/// A connected Flight SQL client.
pub struct FlightSqlClient {
    inner: FlightSqlServiceClient<Channel>,
}

impl FlightSqlClient {
    /// Connect without credentials (anonymous session).
    pub async fn connect(url: &str, opts: &ChannelOptions) -> Result<Self, FlightClientError> {
        let channel = connect_channel(url, opts).await?;
        Ok(Self {
            inner: FlightSqlServiceClient::new(channel),
        })
    }

    /// Connect and run the Basic handshake; the returned bearer is attached to
    /// every subsequent request.
    pub async fn connect_with_handshake(
        url: &str,
        username: &str,
        password: &str,
        opts: &ChannelOptions,
    ) -> Result<Self, FlightClientError> {
        let mut client = Self::connect(url, opts).await?;
        let token = client
            .inner
            .handshake(username, password)
            .await
            .map_err(FlightClientError::Handshake)?;
        let token =
            String::from_utf8(token.to_vec()).map_err(|_| FlightClientError::TokenNotUtf8)?;
        client.inner.set_token(token);
        Ok(client)
    }

    /// Connect with a pre-issued bearer token.
    pub async fn connect_with_token(
        url: &str,
        token: &str,
        opts: &ChannelOptions,
    ) -> Result<Self, FlightClientError> {
        let mut client = Self::connect(url, opts).await?;
        client.inner.set_token(token.to_string());
        Ok(client)
    }

    /// The bearer this client attaches to requests, if any.
    pub fn token(&self) -> Option<&str> {
        self.inner.token().map(String::as_str)
    }

    pub fn set_token(&mut self, token: impl Into<String>) {
        self.inner.set_token(token.into());
    }

    /// Run a statement and collect every batch from every endpoint.
    pub async fn execute_collect(
        &mut self,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, FlightClientError> {
        let stream = self.execute_stream(sql).await?;
        stream.try_collect().await
    }

    /// Run a statement and stream its batches, endpoint by endpoint in order.
    pub async fn execute_stream(
        &mut self,
        sql: &str,
    ) -> Result<BoxStream<'static, Result<RecordBatch, FlightClientError>>, FlightClientError> {
        let info = self
            .inner
            .execute(sql.to_string(), None)
            .await
            .map_err(FlightClientError::Query)?;
        let mut streams = Vec::with_capacity(info.endpoint.len());
        for endpoint in info.endpoint {
            let ticket = endpoint.ticket.ok_or(FlightClientError::NoTicket)?;
            let stream = self
                .inner
                .do_get(ticket)
                .await
                .map_err(FlightClientError::Fetch)?;
            streams.push(stream);
        }
        Ok(futures::stream::iter(streams)
            .flatten()
            .map_err(FlightClientError::from)
            .boxed())
    }

    /// Run a statement that returns no rows (DDL / DML) and report the
    /// server's affected-row count.
    pub async fn execute_update(&mut self, sql: &str) -> Result<i64, FlightClientError> {
        self.inner
            .execute_update(sql.to_string(), None)
            .await
            .map_err(FlightClientError::Query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalise_endpoint_adds_scheme_only_when_missing() {
        assert_eq!(
            normalise_endpoint("localhost:50051"),
            "http://localhost:50051"
        );
        assert_eq!(normalise_endpoint("http://h:1"), "http://h:1");
        assert_eq!(normalise_endpoint("https://h:1"), "https://h:1");
    }

    #[tokio::test]
    async fn connect_to_unroutable_endpoint_is_a_connect_error() {
        let opts = ChannelOptions {
            connect_timeout: Some(Duration::from_millis(200)),
            ..ChannelOptions::default()
        };
        // Port 9 (discard) on localhost is not listening in any test rig.
        let err = FlightSqlClient::connect("http://127.0.0.1:9", &opts)
            .await
            .err()
            .expect("must not connect");
        assert!(matches!(err, FlightClientError::Connect { .. }), "{err}");
    }
}
