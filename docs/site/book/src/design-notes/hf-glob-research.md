# HuggingFace glob expansion: research notes

Goal: support `SELECT * FROM 'hf://datasets/foo/bar@~parquet/**/*.parquet'` so DuckDB-style glob URLs work end-to-end through SQE.

## What blocks globs today

After V12.1 (this MR), the SQL rewriter resolves the hf:// URL to its HTTPS form and DataFusion's `enable_url_table()` builds a `ListingTable` against that URL. For globs to expand, two layers must agree:

1. **`object_store::list(prefix)` on the underlying store.** DataFusion's `ListingTableUrl::list_all_files` calls `list` to enumerate files matching the glob. The default `HttpStore` from `object_store::http` *cannot* enumerate; HTTP has no standard directory-listing protocol. The store returns an empty iterator (or an error) and DataFusion sees "no files match".

2. **Glob parsing.** DataFusion accepts `**/*.ext` syntax in `ListingTableUrl` and dispatches to `list_all_files`. That part already works; it's the upstream `list` that returns nothing.

So the gap is: HuggingFace HTTPS URLs need a working `list()` implementation. HuggingFace's tree API gives us exactly that, just not through WebDAV.

## HuggingFace tree API

```
GET https://huggingface.co/api/datasets/<owner>/<name>/tree/<branch>?recursive=true
```

Returns JSON:

```json
[
  {"type": "file", "path": "default/train/0000.parquet", "size": 12345, "oid": "sha"},
  {"type": "file", "path": "default/train/0001.parquet", "size": 67890, "oid": "sha"},
  {"type": "directory", "path": "default/test", "oid": null}
]
```

For models and spaces the prefix changes: `/api/models/<owner>/<name>/tree/<branch>` and `/api/spaces/<owner>/<name>/tree/<branch>`. The `<branch>` segment accepts URL-encoded refs, including `refs%2Fconvert%2Fparquet` for the auto-generated parquet view.

Auth: anonymous for public datasets. Private datasets need `Authorization: Bearer $HF_TOKEN`.

Rate limits: HuggingFace publishes 1000 requests / 5 minutes per IP for the API. Tree calls are cached server-side; consecutive calls for the same dataset are cheap.

## Three approaches

### Option A: SQL pre-rewriter expands globs

Extend `rewrite_hf_urls_in_sql` to detect glob characters and replace the query with a UNION of resolved URLs.

```sql
-- Input
SELECT col FROM 'hf://datasets/foo/bar@~parquet/**/*.parquet';

-- Rewritten
SELECT col FROM (
  SELECT * FROM 'https://huggingface.co/datasets/foo/bar/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet'
  UNION ALL
  SELECT * FROM 'https://huggingface.co/datasets/foo/bar/resolve/refs%2Fconvert%2Fparquet/default/train/0001.parquet'
);
```

**Pros:**
- No new TableProvider; reuses the same V12 SQL-rewrite hook.
- Works for both URL-table auto-detect (`SELECT * FROM 'hf://...'`) and TVFs (`read_parquet('hf://...')`).
- Easy to test deterministically: fake the HTTP client.

**Cons:**
- N HTTPS calls become N ListingTable entries. Each opens a separate Parquet reader. DataFusion's ParquetExec already handles multi-file scans, but the SQL rewrite introduces them as a UNION instead, which is less efficient (the planner sees them as separate sources, not one table).
- Glob expansion happens at SQL parse time, which means the file list gets baked into the query text. Subsequent runs that re-fetch a stale list would not see new files. Cache invalidation left to the user.
- The rewritten SQL gets very long (a 1000-file dataset becomes 1000 UNION arms).

**Verdict:** workable for small datasets, brittle at scale. Not recommended as the primary path.

### Option B: Custom `HfObjectStore` with working `list()`

Wrap HuggingFace's tree API in an `object_store::ObjectStore` implementation. DataFusion's `ListingTable` then uses the default glob path with no other changes.

```rust
pub struct HfObjectStore {
    inner_http: Arc<dyn ObjectStore>,  // for actual file reads
    api_client: reqwest::Client,        // for tree API listing
    base: String,                       // "https://huggingface.co"
}

#[async_trait]
impl ObjectStore for HfObjectStore {
    async fn list(&self, prefix: Option<&Path>) -> ... {
        // 1. Parse `prefix` -> (owner, name, branch, in-repo path)
        // 2. GET /api/<kind>/<owner>/<name>/tree/<branch>?recursive=true
        // 3. Filter results by in-repo path prefix
        // 4. Yield ObjectMeta entries
    }

    async fn get(&self, location: &Path) -> ... {
        // Delegate to inner_http after rewriting hf:// to https://
        self.inner_http.get(location).await
    }

    // ... head, list_with_delimiter, etc.
}
```

**Pros:**
- Single TableProvider handles arbitrary globs at any depth.
- No SQL surgery. The user's exact URL flows through unchanged.
- Lists once per query, not once per parse. Stat caching can live in the store.
- Works for `read_parquet`, `read_csv`, `read_json`, and URL-table auto-detect uniformly.

**Cons:**
- More code: ~400 lines for the store impl, plus tests.
- Needs registration with the lazy registry: `LazyHttpObjectStoreRegistry` would gain a branch for `hf://` schemes that builds an `HfObjectStore` instead of a plain `HttpStore`.
- Has to handle pagination if HF caps results (the `tree` endpoint returns up to 1000 entries per call; pagination via `?cursor=` per HF Hub API docs).

**Verdict:** the right architecture. Aligns with how V10 handles HTTPS via a lazy-built store. Worth building.

### Option C: DataFusion `TableFactory` for hf://

Register a custom `TableFactory` that DataFusion's `DynamicFileCatalog` calls when it sees `'hf://...'`. The factory:
1. Parses the URL
2. Calls HF tree API directly
3. Builds a `ListingTable` with the expanded file list

**Pros:**
- Cleaner separation: hf:// handling lives in one place.
- No object_store gymnastics.

**Cons:**
- DynamicFileCatalog's extension API (per DataFusion 53) is private. `register_factory` is not public. Would require either a fork patch or upstreaming.
- Doesn't help the TVF path (`read_parquet('hf://**/*.parquet')`); that goes through different machinery.

**Verdict:** blocked on upstream API exposure. Skip.

## Recommended path

**Implement Option B.** Concretely:

1. New module `crates/sqe-catalog/src/hf_object_store.rs` (~300-400 lines).
2. Implements `ObjectStore` trait. Constructor: `HfObjectStore::new(repo_kind, owner, name, branch, http_client)`.
3. `get`, `get_range`, `head`: rewrite `Path` against the resolved HTTPS form, delegate to inner `HttpStore`.
4. `list`, `list_with_delimiter`: call HF tree API, parse JSON, filter by prefix, yield `ObjectMeta`.
5. `LazyHttpObjectStoreRegistry::get_store` learns to detect `hf://` scheme and build an `HfObjectStore` instead of an `HttpStore`. Cache per `(repo_kind, owner, name, branch)`.
6. Tests:
   - Unit tests with `wiremock` faking the tree API: glob expansion, prefix filtering, pagination, branch refs with slashes (`refs/convert/parquet`).
   - Integration test against a known small HuggingFace dataset (`#[ignore]` so CI does not hit network unprompted).

## SQL changes after Option B lands

The V12 SQL rewriter (this MR) translates `hf://` to `https://huggingface.co/...` so DataFusion sees an https URL. With Option B, we want DataFusion to see the original `hf://` URL so it picks the `HfObjectStore` from the registry.

Two cleanup options:

- **Stop rewriting hf:// in SQL when Option B lands.** The HfObjectStore handles the URL natively. Remove `rewrite_hf_urls_in_sql`. The TVFs keep their internal `rewrite_hf_path_in_place` (they need the resolved URL because they manually call `register_http_store_if_needed`).
- **Keep both paths.** SQL rewrite stays as a fallback when `LazyHttpObjectStoreRegistry` is not active (e.g., a harness-only test context).

Recommendation: drop the SQL rewrite once Option B lands. The lazy registry is the natural integration point; keeping both is dual maintenance.

## Effort estimate

| Task | Lines | Effort |
|---|---:|---|
| `hf_object_store.rs` | ~350 | M |
| `LazyHttpObjectStoreRegistry` integration | ~50 | S |
| Tests (wiremock + ignored network) | ~250 | M |
| `rewrite_hf_urls_in_sql` removal | ~30 | S |
| Docs | ~50 | S |

Roughly a 1-2 day MR. Slot as V12.2 once V12 (this MR) and V12.1 (the `@~parquet` extension landing in this same commit) merge.

## What we are NOT doing

- **Caching the tree response across queries.** First query pays the API call; if the user runs the same glob twice in the same session, the second query hits the server again. Adding a TTL cache is a small follow-up.
- **HF_TOKEN env var pickup for private datasets.** V10 documented this as deferred. Public datasets work today; private datasets need an explicit auth provider, which lands separately.
- **DuckDB's `?ext=parquet` shortcut.** Their httpfs accepts `hf://...?ext=parquet` to skip the glob and let HF route to whichever file matches. We do not implement this; the auto-generated `~parquet` view + glob is the equivalent path.

## References

- HuggingFace Hub API: https://huggingface.co/docs/hub/api
- DuckDB `hf://` extension blog: https://duckdb.org/2024/05/29/access-150k-plus-datasets-from-hugging-face-with-duckdb
- DataFusion ListingTableUrl: https://datafusion.apache.org/library-user-guide/working-with-data-sources/datasource.html
- object_store ObjectStore trait: https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html
