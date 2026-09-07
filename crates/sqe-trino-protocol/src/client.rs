//! `TrinoHttpClient`: the one Trino `/v1/statement` client for SQE binaries.
//!
//! Protocol rules this client enforces (each was wrong in at least one of the
//! clients it replaces, review 705):
//!
//! - `error` is checked before `data` on every page; an error page never
//!   reaches the caller's page callback.
//! - Pagination is unbounded. There is no silent cap: the optional
//!   [`TrinoHttpClientBuilder::max_pages`] guard fails the query instead of
//!   returning a truncated result as success.
//! - `nextUri` is only followed when it shares the origin (scheme, host,
//!   port) of the configured base URL. A foreign origin is an error (SSRF).
//! - The final `nextUri` is always fetched, even when `stats.state` already
//!   says `FINISHED`; the server releases the result cursor on that fetch.
//! - Dropping an in-flight query (callback error, page-limit error, or the
//!   future being cancelled) issues `DELETE nextUri` so the server can abort
//!   the query instead of running it to completion for nobody.
//! - HTTP 503 means "retry the same request later" in the Trino protocol and
//!   is retried with backoff; every other non-2xx status is an error.

use std::time::Duration;

use url::Url;

use crate::types::{TrinoColumn, TrinoError, TrinoResponse, TrinoStats};

/// How many bytes of an unparseable or failing response body to keep in the
/// error message.
const BODY_SNIPPET_BYTES: usize = 2048;

#[derive(Debug, thiserror::Error)]
pub enum TrinoClientError {
    #[error("invalid Trino base URL {url:?}: {source}")]
    InvalidUrl {
        url: String,
        #[source]
        source: url::ParseError,
    },
    #[error("HTTP client could not be built: {0}")]
    Build(#[source] reqwest::Error),
    #[error("HTTP request to {url} failed: {source}")]
    Http {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("HTTP {status} from {url}: {body}")]
    Status {
        status: u16,
        url: String,
        body: String,
    },
    #[error("cannot parse Trino response from {url}: {source}\nBody: {body}")]
    Parse {
        url: String,
        #[source]
        source: serde_json::Error,
        body: String,
    },
    /// The server reported a query failure in the `error` field.
    #[error("query failed: {0}")]
    Query(Box<TrinoError>),
    #[error(
        "server returned nextUri {next} whose origin differs from {base}; refusing to follow it"
    )]
    ForeignNextUri { base: String, next: String },
    #[error(
        "query produced more than {limit} pages (the configured page limit); \
         the result is incomplete and has been discarded"
    )]
    PageLimitExceeded { limit: usize },
    #[error("server answered HTTP 503 on {attempts} consecutive attempts for {url}")]
    Unavailable { url: String, attempts: usize },
}

#[derive(Clone, Debug)]
enum Auth {
    Basic { username: String, password: String },
    Bearer(String),
}

impl Auth {
    fn apply(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self {
            Auth::Basic { username, password } => req.basic_auth(username, Some(password)),
            Auth::Bearer(token) => req.bearer_auth(token),
        }
    }
}

/// Builder for [`TrinoHttpClient`]. Start with [`TrinoHttpClient::builder`].
#[derive(Debug)]
pub struct TrinoHttpClientBuilder {
    base_url: String,
    auth: Option<Auth>,
    user: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
    source: Option<String>,
    accept_invalid_certs: bool,
    timeout: Option<Duration>,
    max_pages: Option<usize>,
    unavailable_retries: usize,
}

impl TrinoHttpClientBuilder {
    /// HTTP Basic credentials (`Authorization: Basic ...`).
    #[must_use]
    pub fn basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Some(Auth::Basic {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    /// Bearer token (`Authorization: Bearer ...`).
    #[must_use]
    pub fn bearer_auth(mut self, token: impl Into<String>) -> Self {
        self.auth = Some(Auth::Bearer(token.into()));
        self
    }

    /// `X-Trino-User`. Required by an unauthenticated Trino; SQE derives the
    /// user from the credential and ignores it.
    #[must_use]
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Default catalog for unqualified names (`X-Trino-Catalog`).
    #[must_use]
    pub fn catalog(mut self, catalog: impl Into<String>) -> Self {
        self.catalog = Some(catalog.into());
        self
    }

    /// Default schema for unqualified names (`X-Trino-Schema`).
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// `X-Trino-Source`, shown in the server's query listing.
    #[must_use]
    pub fn source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Skip TLS certificate verification (the CLI's `--insecure`).
    #[must_use]
    pub fn accept_invalid_certs(mut self, yes: bool) -> Self {
        self.accept_invalid_certs = yes;
        self
    }

    /// Per-request timeout. None (the default) means reqwest's default of no
    /// timeout, which is what a long-running page fetch needs.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Optional safety guard on the number of pages a single query may
    /// return. Exceeding it is an error ([`TrinoClientError::PageLimitExceeded`]);
    /// the client never truncates silently. Default: unbounded.
    #[must_use]
    pub fn max_pages(mut self, limit: usize) -> Self {
        self.max_pages = Some(limit);
        self
    }

    /// How many consecutive HTTP 503 answers to retry before giving up.
    /// Default 10 (about 12 s of backoff in total).
    #[must_use]
    pub fn unavailable_retries(mut self, retries: usize) -> Self {
        self.unavailable_retries = retries;
        self
    }

    pub fn build(self) -> Result<TrinoHttpClient, TrinoClientError> {
        let trimmed = self.base_url.trim_end_matches('/').to_string();
        let base = Url::parse(&trimmed).map_err(|source| TrinoClientError::InvalidUrl {
            url: self.base_url.clone(),
            source,
        })?;
        let statement_url = Url::parse(&format!("{trimmed}/v1/statement")).map_err(|source| {
            TrinoClientError::InvalidUrl {
                url: self.base_url.clone(),
                source,
            }
        })?;
        let mut builder =
            reqwest::Client::builder().danger_accept_invalid_certs(self.accept_invalid_certs);
        if let Some(t) = self.timeout {
            builder = builder.timeout(t);
        }
        let http = builder.build().map_err(TrinoClientError::Build)?;
        Ok(TrinoHttpClient {
            http,
            base,
            statement_url,
            auth: self.auth,
            user: self.user,
            catalog: self.catalog,
            schema: self.schema,
            source: self.source,
            max_pages: self.max_pages,
            unavailable_retries: self.unavailable_retries,
        })
    }
}

/// Trino `/v1/statement` client. Cheap to clone (shares the connection pool).
#[derive(Clone, Debug)]
pub struct TrinoHttpClient {
    http: reqwest::Client,
    base: Url,
    statement_url: Url,
    auth: Option<Auth>,
    user: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
    source: Option<String>,
    max_pages: Option<usize>,
    unavailable_retries: usize,
}

/// What a completed query amounted to, independent of how pages were consumed.
#[derive(Debug, Clone, Default)]
pub struct QuerySummary {
    pub id: String,
    /// Column metadata from the first page that carried it; `None` for
    /// statements without a result set.
    pub columns: Option<Vec<TrinoColumn>>,
    pub update_type: Option<String>,
    pub update_count: Option<i64>,
    pub pages: usize,
    /// Total rows across all pages.
    pub rows: usize,
    /// Stats from the last page.
    pub stats: TrinoStats,
}

/// A fully buffered result: every row of every page.
#[derive(Debug, Clone, Default)]
pub struct CollectedQuery {
    pub id: String,
    pub columns: Vec<TrinoColumn>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub update_type: Option<String>,
    pub update_count: Option<i64>,
    pub stats: TrinoStats,
}

impl TrinoHttpClient {
    /// Start building a client for `base_url` (e.g. `http://localhost:8080`).
    pub fn builder(base_url: impl Into<String>) -> TrinoHttpClientBuilder {
        TrinoHttpClientBuilder {
            base_url: base_url.into(),
            auth: None,
            user: None,
            catalog: None,
            schema: None,
            source: None,
            accept_invalid_certs: false,
            timeout: None,
            max_pages: None,
            unavailable_retries: 10,
        }
    }

    pub fn base_url(&self) -> &Url {
        &self.base
    }

    pub fn catalog(&self) -> Option<&str> {
        self.catalog.as_deref()
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Change the default catalog sent on subsequent statements.
    pub fn set_catalog(&mut self, catalog: Option<String>) {
        self.catalog = catalog;
    }

    /// Change the default schema sent on subsequent statements.
    pub fn set_schema(&mut self, schema: Option<String>) {
        self.schema = schema;
    }

    /// Run `sql`, invoking `on_page` for every page in order, and return the
    /// summary. Pages that carry `error` are turned into
    /// [`TrinoClientError::Query`] before the callback sees them. If the
    /// callback returns an error the query is cancelled on the server
    /// (`DELETE nextUri`) and that error is returned.
    pub async fn execute<F>(
        &self,
        sql: &str,
        mut on_page: F,
    ) -> Result<QuerySummary, TrinoClientError>
    where
        F: FnMut(&TrinoResponse) -> Result<(), TrinoClientError>,
    {
        let mut page = self.post_statement(sql).await?;
        let mut cancel = CancelGuard::new(self.http.clone(), self.auth.clone(), self.user.clone());
        let mut summary = QuerySummary {
            id: page.id.clone(),
            ..QuerySummary::default()
        };

        loop {
            if let Some(err) = page.error.take() {
                // Terminal page: nothing left to cancel.
                cancel.disarm();
                return Err(TrinoClientError::Query(Box::new(err)));
            }

            summary.pages += 1;
            if summary.columns.is_none() {
                summary.columns = page.columns.clone();
            }
            if let Some(data) = &page.data {
                summary.rows += data.len();
            }
            if page.update_type.is_some() {
                summary.update_type = page.update_type.clone();
            }
            if page.update_count.is_some() {
                summary.update_count = page.update_count;
            }
            summary.stats = page.stats.clone();

            let next = page.next_uri.clone();
            cancel.arm(next.clone());

            on_page(&page)?;

            let Some(next) = next else {
                cancel.disarm();
                return Ok(summary);
            };

            if let Some(limit) = self.max_pages {
                if summary.pages >= limit {
                    // `cancel` is still armed with `next`: dropping it issues
                    // the DELETE. Truncation is never reported as success.
                    return Err(TrinoClientError::PageLimitExceeded { limit });
                }
            }

            self.check_same_origin(&next)?;
            page = self.get_page(&next).await?;
        }
    }

    /// Run `sql` and buffer every row.
    pub async fn collect(&self, sql: &str) -> Result<CollectedQuery, TrinoClientError> {
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        let summary = self
            .execute(sql, |page| {
                if let Some(data) = &page.data {
                    rows.extend(data.iter().cloned());
                }
                Ok(())
            })
            .await?;
        Ok(CollectedQuery {
            id: summary.id,
            columns: summary.columns.unwrap_or_default(),
            rows,
            update_type: summary.update_type,
            update_count: summary.update_count,
            stats: summary.stats,
        })
    }

    fn check_same_origin(&self, next: &str) -> Result<(), TrinoClientError> {
        let foreign = || TrinoClientError::ForeignNextUri {
            base: self.base.to_string(),
            next: next.to_string(),
        };
        let next_url = Url::parse(next).map_err(|_| foreign())?;
        if next_url.origin() != self.base.origin() {
            return Err(foreign());
        }
        Ok(())
    }

    fn decorate(&self, mut req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(auth) = &self.auth {
            req = auth.apply(req);
        }
        if let Some(user) = &self.user {
            req = req.header("X-Trino-User", user);
        }
        req
    }

    async fn post_statement(&self, sql: &str) -> Result<TrinoResponse, TrinoClientError> {
        let url = self.statement_url.clone();
        let build = || {
            let mut req = self
                .http
                .post(url.clone())
                .header("Content-Type", "text/plain")
                .body(sql.to_string());
            req = self.decorate(req);
            if let Some(catalog) = &self.catalog {
                req = req.header("X-Trino-Catalog", catalog);
            }
            if let Some(schema) = &self.schema {
                req = req.header("X-Trino-Schema", schema);
            }
            if let Some(source) = &self.source {
                req = req.header("X-Trino-Source", source);
            }
            req
        };
        self.send_with_503_retry(url.as_str(), build).await
    }

    async fn get_page(&self, next: &str) -> Result<TrinoResponse, TrinoClientError> {
        let build = || self.decorate(self.http.get(next));
        self.send_with_503_retry(next, build).await
    }

    async fn send_with_503_retry<B>(
        &self,
        url: &str,
        build: B,
    ) -> Result<TrinoResponse, TrinoClientError>
    where
        B: Fn() -> reqwest::RequestBuilder,
    {
        let mut attempt = 0usize;
        loop {
            let resp = build()
                .send()
                .await
                .map_err(|source| TrinoClientError::Http {
                    url: url.to_string(),
                    source,
                })?;
            let status = resp.status();
            if status.as_u16() == 503 {
                attempt += 1;
                if attempt > self.unavailable_retries {
                    return Err(TrinoClientError::Unavailable {
                        url: url.to_string(),
                        attempts: attempt,
                    });
                }
                let backoff = Duration::from_millis((50u64 << attempt.min(5)).min(2000));
                tokio::time::sleep(backoff).await;
                continue;
            }
            let body = resp.text().await.map_err(|source| TrinoClientError::Http {
                url: url.to_string(),
                source,
            })?;
            if !status.is_success() {
                return Err(TrinoClientError::Status {
                    status: status.as_u16(),
                    url: url.to_string(),
                    body: snippet(&body),
                });
            }
            return serde_json::from_str::<TrinoResponse>(&body).map_err(|source| {
                TrinoClientError::Parse {
                    url: url.to_string(),
                    source,
                    body: snippet(&body),
                }
            });
        }
    }
}

fn snippet(body: &str) -> String {
    if body.len() <= BODY_SNIPPET_BYTES {
        return body.to_string();
    }
    let mut end = BODY_SNIPPET_BYTES;
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}... ({} bytes total)", &body[..end], body.len())
}

/// Cancels the in-flight query (`DELETE nextUri`) when dropped while armed.
///
/// Armed with the latest `nextUri` before the page callback runs and after
/// each successful fetch; disarmed on the terminal page. So any early exit --
/// callback error, page limit, or the caller dropping the `execute` future --
/// tells the server to abort instead of leaving the query running for nobody.
struct CancelGuard {
    http: reqwest::Client,
    auth: Option<Auth>,
    user: Option<String>,
    uri: Option<String>,
}

impl CancelGuard {
    fn new(http: reqwest::Client, auth: Option<Auth>, user: Option<String>) -> Self {
        Self {
            http,
            auth,
            user,
            uri: None,
        }
    }

    fn arm(&mut self, uri: Option<String>) {
        self.uri = uri;
    }

    fn disarm(&mut self) {
        self.uri = None;
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        let Some(uri) = self.uri.take() else {
            return;
        };
        // Drop cannot await. Spawn the DELETE on the current runtime when
        // there is one; without a runtime there is nothing to send it on.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut req = self.http.delete(uri);
        if let Some(auth) = &self.auth {
            req = auth.apply(req);
        }
        if let Some(user) = &self.user {
            req = req.header("X-Trino-User", user);
        }
        handle.spawn(async move {
            let _ = req.send().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_normalises_trailing_slash_and_builds_statement_url() {
        let c = TrinoHttpClient::builder("http://localhost:8080/")
            .build()
            .unwrap();
        assert_eq!(
            c.statement_url.as_str(),
            "http://localhost:8080/v1/statement"
        );
        assert_eq!(c.base_url().as_str(), "http://localhost:8080/");
    }

    #[test]
    fn builder_rejects_garbage_url() {
        let err = TrinoHttpClient::builder("not a url").build().err().unwrap();
        assert!(matches!(err, TrinoClientError::InvalidUrl { .. }), "{err}");
    }

    #[test]
    fn same_origin_check_compares_scheme_host_port() {
        let c = TrinoHttpClient::builder("http://localhost:8080")
            .build()
            .unwrap();
        assert!(c
            .check_same_origin("http://localhost:8080/v1/statement/x/1")
            .is_ok());
        assert!(c
            .check_same_origin("http://localhost:8081/v1/statement/x/1")
            .is_err());
        assert!(c
            .check_same_origin("https://localhost:8080/v1/statement/x/1")
            .is_err());
        assert!(c.check_same_origin("http://evil.example/x").is_err());
        assert!(c.check_same_origin("::not a url::").is_err());
    }

    #[test]
    fn snippet_truncates_on_char_boundary() {
        let long = "é".repeat(BODY_SNIPPET_BYTES);
        let s = snippet(&long);
        assert!(s.ends_with("bytes total)"));
        assert!(s.len() < long.len());
    }
}
