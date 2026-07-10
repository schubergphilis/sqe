//! Synchronous Quack RPC client.

use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use sqe_quack_wire::arrow_bridge::{data_chunk_to_record_batch, logical_schema_to_arrow};
use sqe_quack_wire::message::{
    decode_message, encode_message, ConnectionRequest, FetchRequest, MessageHeader, MessageType,
    PrepareRequest, QuackMessage,
};
use thiserror::Error;
use url::Url;

const CONTENT_TYPE: &str = "application/vnd.duckdb";
const CLIENT_DUCKDB_VERSION: &str = "v1.5.3";
const CLIENT_PLATFORM: &str = "sqe-quack-client";
/// Quack protocol version we negotiate. DuckDB 1.5.x speaks version 1.
const QUACK_VERSION: u64 = 1;

/// Errors surfaced by the client. Wraps the underlying transport, codec, and
/// server-side `ErrorResponse` cases.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid quack URI: {0}")]
    InvalidUri(String),
    #[error("HTTP transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("HTTP {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("wire codec: {0}")]
    Wire(#[from] sqe_quack_wire::WireError),
    #[error("unexpected message type from server: {0:?}")]
    UnexpectedMessage(MessageType),
    #[error("server error: {0}")]
    ServerError(String),
    #[error("not connected — call connect() first")]
    NotConnected,
}

/// Synchronous, single-connection Quack client. Holds the `connection_id`
/// returned by the server handshake and reuses it across queries until
/// [`QuackClient::disconnect`] or drop.
pub struct QuackClient {
    http: reqwest::blocking::Client,
    endpoint: Url,
    connection_id: String,
    next_query_id: u64,
}

impl QuackClient {
    /// Open a Quack connection to `uri` (form `quack:host[:port]`, optional
    /// `quacks:` for TLS). `token` is sent as the `auth_string` in the
    /// initial handshake and is whatever the server's auth provider expects
    /// (typically a bearer JWT or an opaque secret).
    pub fn connect(uri: &str, token: Option<&str>) -> Result<Self, ClientError> {
        let endpoint = parse_quack_uri(uri)?;
        let http = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;
        let mut client = QuackClient {
            http,
            endpoint,
            connection_id: String::new(),
            next_query_id: 1,
        };
        client.handshake(token.unwrap_or(""))?;
        Ok(client)
    }

    fn handshake(&mut self, token: &str) -> Result<(), ClientError> {
        let header = MessageHeader {
            r#type: MessageType::ConnectionRequest,
            connection_id: String::new(),
            client_query_id: None,
        };
        let body = QuackMessage::ConnectionRequest(ConnectionRequest {
            auth_string: token.to_string(),
            client_duckdb_version: CLIENT_DUCKDB_VERSION.to_string(),
            client_platform: CLIENT_PLATFORM.to_string(),
            min_supported_quack_version: QUACK_VERSION,
            max_supported_quack_version: QUACK_VERSION,
        });
        let (resp_header, resp_body) = self.send(&header, &body)?;
        match resp_body {
            QuackMessage::ConnectionResponse(_) => {
                self.connection_id = resp_header.connection_id;
                Ok(())
            }
            QuackMessage::ErrorResponse(e) => Err(ClientError::ServerError(e.message)),
            other => Err(ClientError::UnexpectedMessage(other.message_type())),
        }
    }

    /// Run a SQL query, returning every `RecordBatch` the server produces.
    /// Drives the `PrepareRequest` -> drain `FetchRequest`s loop until
    /// `needs_more_fetch` flips false.
    pub fn execute(&mut self, sql: &str) -> Result<ExecuteResult, ClientError> {
        if self.connection_id.is_empty() {
            return Err(ClientError::NotConnected);
        }
        let qid = self.next_query_id;
        self.next_query_id += 1;
        let header = MessageHeader {
            r#type: MessageType::PrepareRequest,
            connection_id: self.connection_id.clone(),
            client_query_id: Some(qid),
        };
        let body = QuackMessage::PrepareRequest(PrepareRequest {
            sql_query: sql.to_string(),
        });
        let (_h, prepare_body) = self.send(&header, &body)?;
        let prepare = match prepare_body {
            QuackMessage::PrepareResponse(r) => r,
            QuackMessage::ErrorResponse(e) => return Err(ClientError::ServerError(e.message)),
            other => return Err(ClientError::UnexpectedMessage(other.message_type())),
        };

        let names = prepare.result_names.clone();
        let schema = logical_schema_to_arrow(&names, &prepare.result_types)?;
        let mut batches: Vec<RecordBatch> = Vec::new();
        for chunk in &prepare.results {
            batches.push(data_chunk_to_record_batch(&names, chunk)?);
        }

        let mut needs_more = prepare.needs_more_fetch;
        let uuid = prepare.result_uuid;
        while needs_more {
            let fetch_header = MessageHeader {
                r#type: MessageType::FetchRequest,
                connection_id: self.connection_id.clone(),
                client_query_id: Some(qid),
            };
            let fetch_body = QuackMessage::FetchRequest(FetchRequest { uuid });
            let (_h, body) = self.send(&fetch_header, &fetch_body)?;
            let fetch = match body {
                QuackMessage::FetchResponse(r) => r,
                QuackMessage::ErrorResponse(e) => return Err(ClientError::ServerError(e.message)),
                other => return Err(ClientError::UnexpectedMessage(other.message_type())),
            };
            for chunk in &fetch.results {
                batches.push(data_chunk_to_record_batch(&names, chunk)?);
            }
            // The server flips needs_more_fetch off implicitly via batch_index
            // being None — or it never sets it in the first place for a small
            // result set. The simplest signal: if the fetch yielded zero
            // chunks, we're done.
            needs_more = !fetch.results.is_empty();
        }

        Ok(ExecuteResult {
            names: prepare.result_names,
            schema,
            batches,
        })
    }

    /// Send a `DisconnectMessage` to politely close the session. Drops the
    /// connection_id; further calls to [`Self::execute`] will return
    /// [`ClientError::NotConnected`].
    pub fn disconnect(&mut self) -> Result<(), ClientError> {
        if self.connection_id.is_empty() {
            return Ok(());
        }
        let header = MessageHeader {
            r#type: MessageType::DisconnectMessage,
            connection_id: std::mem::take(&mut self.connection_id),
            client_query_id: None,
        };
        let body = QuackMessage::DisconnectMessage;
        // Best-effort: ignore the response shape — the server replies with
        // either Success or Error, and we don't care which once we're tearing
        // down.
        let _ = self.send(&header, &body)?;
        Ok(())
    }

    fn send(
        &self,
        header: &MessageHeader,
        body: &QuackMessage,
    ) -> Result<(MessageHeader, QuackMessage), ClientError> {
        let bytes = encode_message(header, body);
        let resp = self
            .http
            .post(self.endpoint.clone())
            .header(reqwest::header::CONTENT_TYPE, CONTENT_TYPE)
            .body(bytes)
            .send()?;
        let status = resp.status();
        let payload = resp.bytes()?.to_vec();
        if !status.is_success() {
            return Err(ClientError::HttpStatus {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&payload).into_owned(),
            });
        }
        let decoded = decode_message(&payload)?;
        Ok(decoded)
    }
}

impl Drop for QuackClient {
    fn drop(&mut self) {
        let _ = self.disconnect();
    }
}

/// Result of [`QuackClient::execute`]: column names + the Arrow [`SchemaRef`]
/// derived from `PrepareResponse.result_types` + every batch the server
/// streamed back. The schema is set even when `batches` is empty — useful
/// for downstream consumers (e.g. `QuackTableProvider`) that need to expose
/// a schema before any rows arrive.
#[derive(Debug, Clone)]
pub struct ExecuteResult {
    pub names: Vec<String>,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

fn parse_quack_uri(input: &str) -> Result<Url, ClientError> {
    // Accept `quack:host`, `quack:host:port`, `quacks:host[:port]`, or a
    // fully-qualified `http://host:port/quack` URL. The first three forms
    // are DuckDB's canonical wire URI; the last is what the live server
    // exposes via `quack_serve()`.
    if let Ok(parsed) = Url::parse(input) {
        if parsed.scheme().starts_with("http") {
            return Ok(parsed);
        }
    }
    let (scheme_https, rest) = match input.strip_prefix("quacks:") {
        Some(r) => (true, r),
        None => match input.strip_prefix("quack:") {
            Some(r) => (false, r),
            None => return Err(ClientError::InvalidUri(input.to_string())),
        },
    };
    let (host_port, default_port) = if rest.contains(':') {
        (rest.to_string(), None)
    } else {
        let port = if scheme_https { 443u16 } else { 9494u16 };
        (format!("{rest}:{port}"), Some(port))
    };
    let scheme = if scheme_https { "https" } else { "http" };
    let raw = format!("{scheme}://{host_port}/quack");
    Url::parse(&raw).map_err(|_| {
        ClientError::InvalidUri(format!(
            "could not normalise {input} -> {raw} (default port {default_port:?})"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_quack_uri_strips_scheme_and_defaults_port() {
        let u = parse_quack_uri("quack:localhost").unwrap();
        assert_eq!(u.as_str(), "http://localhost:9494/quack");
    }

    #[test]
    fn parse_quack_uri_keeps_explicit_port() {
        let u = parse_quack_uri("quack:127.0.0.1:9495").unwrap();
        assert_eq!(u.as_str(), "http://127.0.0.1:9495/quack");
    }

    #[test]
    fn parse_quack_uri_accepts_https_via_quacks_scheme() {
        // The `url` crate normalises the explicit default port (:443 for https)
        // out of the serialised form.
        let u = parse_quack_uri("quacks:example.com").unwrap();
        assert_eq!(u.scheme(), "https");
        assert_eq!(u.host_str(), Some("example.com"));
        assert_eq!(u.port_or_known_default(), Some(443));
        assert_eq!(u.path(), "/quack");
    }

    #[test]
    fn parse_quack_uri_accepts_full_http_url_unchanged() {
        let u = parse_quack_uri("http://localhost:9494/quack").unwrap();
        assert_eq!(u.as_str(), "http://localhost:9494/quack");
    }

    #[test]
    fn parse_quack_uri_rejects_garbage() {
        assert!(parse_quack_uri("not-a-uri").is_err());
    }
}
