//! Cached HuggingFace tree-API client.
//!
//! Future V12.2 work plugs this into a custom `HfObjectStore` so
//! `SELECT * FROM 'hf://datasets/foo/bar/**/*.parquet'` (glob patterns)
//! enumerates files via HuggingFace's tree API instead of attempting a
//! plain HTTP `LIST` (which the protocol does not support).
//!
//! The cache layer lives separately from the object store implementation
//! because:
//!
//! 1. The cache is the only piece that touches HuggingFace's API surface;
//!    isolating it keeps the API shape testable without an `ObjectStore`
//!    impl harness.
//! 2. A future `HfObjectStore` swaps `HttpClient` for a mock at test time.
//! 3. The same cache pattern can also serve a future Hub-aware completion
//!    UX in the CLI (suggesting file paths from a typed dataset id).
//!
//! Endpoints (anonymous read access works for public datasets):
//!
//! ```text
//! GET https://huggingface.co/api/datasets/<owner>/<name>/tree/<branch>?recursive=true
//! GET https://huggingface.co/api/models/<owner>/<name>/tree/<branch>?recursive=true
//! GET https://huggingface.co/api/spaces/<owner>/<name>/tree/<branch>?recursive=true
//! ```
//!
//! Pagination: HuggingFace caps responses around 1000 entries and returns a
//! `Link: <next>; rel="next"` header for further pages. We follow links
//! transparently so the cache value is the full file list.
//!
//! TTL: 5 minutes by default. Tree contents on a stable branch change
//! rarely; a 5-minute miss-once-then-cache pattern is the standard tradeoff
//! between staleness and API hits.

use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;
use reqwest::header::HeaderMap;
use serde::Deserialize;
use sqe_core::{Result as SqeResult, SqeError};

const HF_API_BASE: &str = "https://huggingface.co/api";
const DEFAULT_TTL_SECS: u64 = 300;
const DEFAULT_MAX_ENTRIES: u64 = 256;

/// Repo kind on HuggingFace Hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HfRepoKind {
    Datasets,
    Models,
    Spaces,
}

impl HfRepoKind {
    /// Path segment used in the HF API URL.
    pub fn api_segment(&self) -> &'static str {
        match self {
            HfRepoKind::Datasets => "datasets",
            HfRepoKind::Models => "models",
            HfRepoKind::Spaces => "spaces",
        }
    }

    /// Parse from the `hf://<kind>/...` scheme.
    pub fn from_segment(s: &str) -> Option<Self> {
        match s {
            "datasets" => Some(HfRepoKind::Datasets),
            "models" => Some(HfRepoKind::Models),
            "spaces" => Some(HfRepoKind::Spaces),
            _ => None,
        }
    }
}

/// Cache key for a single tree-API response.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TreeKey {
    pub kind: HfRepoKind,
    pub owner: String,
    pub name: String,
    pub branch: String,
}

impl TreeKey {
    pub fn new(kind: HfRepoKind, owner: &str, name: &str, branch: &str) -> Self {
        Self {
            kind,
            owner: owner.to_string(),
            name: name.to_string(),
            branch: branch.to_string(),
        }
    }
}

/// One entry in a HuggingFace tree response.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct TreeEntry {
    /// `"file"` or `"directory"`.
    #[serde(rename = "type")]
    pub kind: String,
    /// In-repo path, relative to the repo root.
    pub path: String,
    /// File size in bytes; absent for directories.
    #[serde(default)]
    pub size: Option<u64>,
    /// Git OID (commit SHA for the blob); absent for directories.
    #[serde(default)]
    pub oid: Option<String>,
}

impl TreeEntry {
    /// `true` if this entry represents a regular file.
    pub fn is_file(&self) -> bool {
        self.kind == "file"
    }
}

/// Pluggable HTTP backend so tests can substitute an in-memory transport.
#[async_trait::async_trait]
pub trait HfHttpClient: Send + Sync + std::fmt::Debug {
    /// Fetch one URL, return body as bytes plus the response headers
    /// (so the caller can follow `Link: <next>; rel="next"`).
    async fn fetch(&self, url: &str) -> SqeResult<(bytes::Bytes, HeaderMap)>;
}

#[derive(Debug)]
pub struct ReqwestClient {
    client: reqwest::Client,
    /// Optional bearer token. Set from HF_TOKEN env at session start when
    /// private datasets are needed.
    bearer: Option<String>,
}

impl ReqwestClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            bearer: std::env::var("HF_TOKEN").ok().filter(|s| !s.is_empty()),
        }
    }

    #[must_use = "with_bearer consumes self; bind the returned client"]
    pub fn with_bearer(mut self, token: impl Into<String>) -> Self {
        self.bearer = Some(token.into());
        self
    }
}

impl Default for ReqwestClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl HfHttpClient for ReqwestClient {
    async fn fetch(&self, url: &str) -> SqeResult<(bytes::Bytes, HeaderMap)> {
        let mut req = self.client.get(url);
        if let Some(token) = &self.bearer {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SqeError::catalog_src(format!("HF API GET {url} failed: {e}"), e))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(SqeError::Catalog(format!(
                "HF API GET {url} returned {status}"
            )));
        }
        let headers = resp.headers().clone();
        let body = resp
            .bytes()
            .await
            .map_err(|e| SqeError::catalog_src(format!("HF API read body failed: {e}"), e))?;
        Ok((body, headers))
    }
}

/// TTL-cached HuggingFace tree API client. One instance per session.
#[derive(Clone)]
pub struct HfTreeCache {
    http: Arc<dyn HfHttpClient>,
    cache: Cache<TreeKey, Arc<Vec<TreeEntry>>>,
}

impl std::fmt::Debug for HfTreeCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HfTreeCache")
            .field("entries", &self.cache.entry_count())
            .finish_non_exhaustive()
    }
}

impl HfTreeCache {
    /// Build a cache backed by `reqwest`. Reads `HF_TOKEN` from the
    /// environment for private dataset access.
    pub fn new() -> Self {
        Self::with_client(Arc::new(ReqwestClient::new()))
    }

    /// Build a cache with a caller-supplied HTTP client. Tests pass a
    /// mock; production passes [`ReqwestClient`].
    pub fn with_client(http: Arc<dyn HfHttpClient>) -> Self {
        let cache = Cache::builder()
            .max_capacity(DEFAULT_MAX_ENTRIES)
            .time_to_live(Duration::from_secs(DEFAULT_TTL_SECS))
            .build();
        Self { http, cache }
    }

    /// Override the default 5-minute TTL.
    #[must_use = "with_ttl consumes self; bind the returned cache"]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        let http = self.http.clone();
        self.cache = Cache::builder()
            .max_capacity(DEFAULT_MAX_ENTRIES)
            .time_to_live(ttl)
            .build();
        self.http = http;
        self
    }

    /// Fetch and cache the recursive file list for one repo + branch.
    /// Subsequent calls within the TTL hit the cache and skip the API.
    pub async fn list(&self, key: &TreeKey) -> SqeResult<Arc<Vec<TreeEntry>>> {
        if let Some(hit) = self.cache.get(key).await {
            return Ok(hit);
        }
        let fetched = self.fetch_all_pages(key).await?;
        let value = Arc::new(fetched);
        self.cache.insert(key.clone(), value.clone()).await;
        Ok(value)
    }

    /// `true` if the tree is currently cached for this key.
    pub async fn is_cached(&self, key: &TreeKey) -> bool {
        self.cache.get(key).await.is_some()
    }

    /// Drop a single cache entry, e.g. after a known write to the repo.
    pub async fn invalidate(&self, key: &TreeKey) {
        self.cache.invalidate(key).await;
    }

    /// Drop the entire cache.
    pub async fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Fetch every page of the tree API for one key, following `Link:
    /// <next>; rel="next"` headers until exhausted.
    async fn fetch_all_pages(&self, key: &TreeKey) -> SqeResult<Vec<TreeEntry>> {
        let mut url = format!(
            "{HF_API_BASE}/{kind}/{owner}/{name}/tree/{branch}?recursive=true",
            kind = key.kind.api_segment(),
            owner = url_encode_segment(&key.owner),
            name = url_encode_segment(&key.name),
            branch = url_encode_segment(&key.branch),
        );

        let mut all = Vec::new();
        let mut page = 0usize;
        loop {
            let (body, headers) = self.http.fetch(&url).await?;
            let entries: Vec<TreeEntry> = serde_json::from_slice(&body).map_err(|e| {
                SqeError::catalog_src(format!("HF tree response parse failed for {url}: {e}"), e)
            })?;
            all.extend(entries);
            page += 1;
            // Cap pagination so a misbehaving response cannot loop us.
            if page >= 64 {
                return Err(SqeError::Catalog(format!(
                    "HF tree pagination exceeded 64 pages for {url}"
                )));
            }
            match next_link(&headers) {
                Some(next) => url = next,
                None => break,
            }
        }
        Ok(all)
    }
}

impl Default for HfTreeCache {
    fn default() -> Self {
        Self::new()
    }
}

/// URL-encode one path segment. Slashes get percent-encoded so the
/// segment lands as one path component.
fn url_encode_segment(seg: &str) -> String {
    let mut out = String::with_capacity(seg.len());
    for c in seg.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(c),
            _ => {
                let mut buf = [0u8; 4];
                for &b in c.encode_utf8(&mut buf).as_bytes() {
                    out.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    out
}

/// Parse a `Link` header for the URL marked `rel="next"`. Returns `None`
/// when no next link is present (the standard end-of-pagination signal).
fn next_link(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(reqwest::header::LINK)?.to_str().ok()?;
    // `Link: <https://...>; rel="next", <https://...>; rel="prev"`
    for entry in value.split(',') {
        let entry = entry.trim();
        if !entry.contains(r#"rel="next""#) {
            continue;
        }
        let url_start = entry.find('<')? + 1;
        let url_end = entry[url_start..].find('>')? + url_start;
        return Some(entry[url_start..url_end].to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// `(url, body_bytes, headers)` for a mock HTTP response.
    type MockResponse = (String, Vec<u8>, Vec<(String, String)>);

    /// Simple in-memory mock keyed by URL. Records every call so tests
    /// can assert cache behaviour.
    #[derive(Debug, Default)]
    struct MockHttp {
        responses: Mutex<Vec<MockResponse>>,
        calls: Mutex<Vec<String>>,
    }

    impl MockHttp {
        fn enqueue(&self, url: &str, body: &str, headers: Vec<(&str, &str)>) {
            self.responses.lock().unwrap().push((
                url.to_string(),
                body.as_bytes().to_vec(),
                headers
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ));
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl HfHttpClient for MockHttp {
        async fn fetch(&self, url: &str) -> SqeResult<(bytes::Bytes, HeaderMap)> {
            self.calls.lock().unwrap().push(url.to_string());
            let mut responses = self.responses.lock().unwrap();
            let pos = responses
                .iter()
                .position(|(u, _, _)| u == url)
                .ok_or_else(|| SqeError::Catalog(format!("mock: no response for {url}")))?;
            let (_, body, headers) = responses.remove(pos);
            let mut hm = HeaderMap::new();
            for (k, v) in headers {
                hm.insert(
                    k.parse::<reqwest::header::HeaderName>().unwrap(),
                    v.parse().unwrap(),
                );
            }
            Ok((bytes::Bytes::from(body), hm))
        }
    }

    fn datasets_url(owner: &str, name: &str, branch: &str) -> String {
        format!("{HF_API_BASE}/datasets/{owner}/{name}/tree/{branch}?recursive=true")
    }

    #[tokio::test]
    async fn list_caches_response_within_ttl() {
        let mock = Arc::new(MockHttp::default());
        let body = r#"[
            {"type": "file", "path": "data.csv", "size": 100, "oid": "abc"},
            {"type": "directory", "path": "subdir"}
        ]"#;
        mock.enqueue(&datasets_url("foo", "bar", "main"), body, vec![]);

        let cache = HfTreeCache::with_client(mock.clone());
        let key = TreeKey::new(HfRepoKind::Datasets, "foo", "bar", "main");

        let first = cache.list(&key).await.unwrap();
        let second = cache.list(&key).await.unwrap();

        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        assert_eq!(mock.call_count(), 1, "second list must hit cache");
        assert!(first[0].is_file());
        assert!(!first[1].is_file());
    }

    #[tokio::test]
    async fn list_separate_keys_fetch_separately() {
        let mock = Arc::new(MockHttp::default());
        mock.enqueue(
            &datasets_url("foo", "bar", "main"),
            r#"[{"type": "file", "path": "a.csv"}]"#,
            vec![],
        );
        mock.enqueue(
            &datasets_url("foo", "bar", "v1"),
            r#"[{"type": "file", "path": "a-v1.csv"}]"#,
            vec![],
        );

        let cache = HfTreeCache::with_client(mock.clone());
        let main = cache
            .list(&TreeKey::new(HfRepoKind::Datasets, "foo", "bar", "main"))
            .await
            .unwrap();
        let v1 = cache
            .list(&TreeKey::new(HfRepoKind::Datasets, "foo", "bar", "v1"))
            .await
            .unwrap();

        assert_eq!(main[0].path, "a.csv");
        assert_eq!(v1[0].path, "a-v1.csv");
        assert_eq!(
            mock.call_count(),
            2,
            "different branches = separate fetches"
        );
    }

    #[tokio::test]
    async fn list_follows_link_header_pagination() {
        let mock = Arc::new(MockHttp::default());
        let url1 = datasets_url("foo", "bar", "main");
        let url2 = format!("{url1}&cursor=page2");
        mock.enqueue(
            &url1,
            r#"[{"type": "file", "path": "a.csv"}]"#,
            vec![("link", &format!(r#"<{url2}>; rel="next""#))],
        );
        mock.enqueue(&url2, r#"[{"type": "file", "path": "b.csv"}]"#, vec![]);

        let cache = HfTreeCache::with_client(mock.clone());
        let entries = cache
            .list(&TreeKey::new(HfRepoKind::Datasets, "foo", "bar", "main"))
            .await
            .unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "a.csv");
        assert_eq!(entries[1].path, "b.csv");
        assert_eq!(mock.call_count(), 2);
    }

    #[tokio::test]
    async fn invalidate_drops_cache_entry() {
        let mock = Arc::new(MockHttp::default());
        mock.enqueue(
            &datasets_url("foo", "bar", "main"),
            r#"[{"type": "file", "path": "a.csv"}]"#,
            vec![],
        );
        mock.enqueue(
            &datasets_url("foo", "bar", "main"),
            r#"[{"type": "file", "path": "a.csv"}, {"type": "file", "path": "b.csv"}]"#,
            vec![],
        );

        let cache = HfTreeCache::with_client(mock.clone());
        let key = TreeKey::new(HfRepoKind::Datasets, "foo", "bar", "main");

        let first = cache.list(&key).await.unwrap();
        assert_eq!(first.len(), 1);

        cache.invalidate(&key).await;

        let second = cache.list(&key).await.unwrap();
        assert_eq!(second.len(), 2, "post-invalidate must re-fetch");
        assert_eq!(mock.call_count(), 2);
    }

    #[tokio::test]
    async fn list_url_encodes_owner_and_name() {
        // A repo whose owner / name contain spaces or unicode lands on
        // the URL with percent-encoded segments. The encode helper is
        // conservative, matching what HuggingFace expects.
        let mock = Arc::new(MockHttp::default());
        let url = format!("{HF_API_BASE}/datasets/foo%20bar/baz/tree/main?recursive=true");
        mock.enqueue(&url, "[]", vec![]);

        let cache = HfTreeCache::with_client(mock.clone());
        let _entries = cache
            .list(&TreeKey::new(
                HfRepoKind::Datasets,
                "foo bar",
                "baz",
                "main",
            ))
            .await
            .unwrap();
        assert_eq!(mock.call_count(), 1);
    }

    #[tokio::test]
    async fn list_propagates_parse_errors() {
        let mock = Arc::new(MockHttp::default());
        mock.enqueue(
            &datasets_url("foo", "bar", "main"),
            "this is not json",
            vec![],
        );
        let cache = HfTreeCache::with_client(mock.clone());
        let err = cache
            .list(&TreeKey::new(HfRepoKind::Datasets, "foo", "bar", "main"))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("parse failed"));
    }

    #[tokio::test]
    async fn list_propagates_http_errors() {
        let mock = Arc::new(MockHttp::default());
        // No enqueue; the mock returns "no response for ..." which the
        // cache surfaces as a Catalog error.
        let cache = HfTreeCache::with_client(mock);
        let err = cache
            .list(&TreeKey::new(HfRepoKind::Datasets, "foo", "bar", "main"))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("mock: no response"));
    }

    #[tokio::test]
    async fn pagination_cap_prevents_infinite_loop() {
        let mock = Arc::new(MockHttp::default());
        // Each page points at itself — without the cap we would loop.
        let url = datasets_url("foo", "bar", "main");
        for _ in 0..70 {
            mock.enqueue(
                &url,
                "[]",
                vec![("link", &format!(r#"<{url}>; rel="next""#))],
            );
        }
        let cache = HfTreeCache::with_client(mock);
        let err = cache
            .list(&TreeKey::new(HfRepoKind::Datasets, "foo", "bar", "main"))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("pagination exceeded"));
    }

    #[test]
    fn next_link_finds_rel_next_among_multiple() {
        let mut hm = HeaderMap::new();
        hm.insert(
            reqwest::header::LINK,
            r#"<https://a/p1>; rel="prev", <https://a/p2>; rel="next""#
                .parse()
                .unwrap(),
        );
        assert_eq!(next_link(&hm), Some("https://a/p2".to_string()));
    }

    #[test]
    fn next_link_returns_none_without_next() {
        let mut hm = HeaderMap::new();
        hm.insert(
            reqwest::header::LINK,
            r#"<https://a/p1>; rel="prev""#.parse().unwrap(),
        );
        assert_eq!(next_link(&hm), None);
    }

    #[test]
    fn repo_kind_parsing() {
        assert_eq!(
            HfRepoKind::from_segment("datasets"),
            Some(HfRepoKind::Datasets)
        );
        assert_eq!(HfRepoKind::from_segment("models"), Some(HfRepoKind::Models));
        assert_eq!(HfRepoKind::from_segment("spaces"), Some(HfRepoKind::Spaces));
        assert_eq!(HfRepoKind::from_segment("foo"), None);
    }

    #[test]
    fn url_encode_segment_keeps_unreserved() {
        assert_eq!(url_encode_segment("foo-bar_baz.qux"), "foo-bar_baz.qux");
        assert_eq!(url_encode_segment("foo bar"), "foo%20bar");
        assert_eq!(url_encode_segment("foo/bar"), "foo%2Fbar");
        // utf-8 byte-level encoding for non-ascii
        assert_eq!(url_encode_segment("ñ"), "%C3%B1");
    }
}
