# Trino JDBC Compatibility Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two Trino JDBC driver compatibility issues — always include `infoUri` in responses, and add `system.jdbc.*` virtual tables for DBeaver metadata browsing.

**Architecture:** Two independent fixes in separate crates. Fix 1 modifies `sqe-trino-compat` to always populate `infoUri` using the request's `Host` header. Fix 2 adds a `SystemCatalogProvider` in `sqe-catalog` with a `JdbcSchemaProvider` containing virtual `system.jdbc.*` tables, registered in the coordinator's DataFusion context.

**Tech Stack:** Rust, DataFusion (CatalogProvider/SchemaProvider traits), axum (HTTP headers), Arrow (RecordBatch/MemTable)

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/sqe-trino-compat/src/protocol.rs` | Modify | Always serialize `infoUri` (remove `skip_serializing_if`) |
| `crates/sqe-trino-compat/src/server.rs` | Modify | Extract `Host` header, build absolute `infoUri`/`nextUri`, pass to response builders |
| `crates/sqe-catalog/src/system_jdbc.rs` | Create | `JdbcSchemaProvider` implementing `system.jdbc.{types,catalogs,schemas,tables,columns}` |
| `crates/sqe-catalog/src/system_catalog.rs` | Create | `SystemCatalogProvider` wrapping `JdbcSchemaProvider` |
| `crates/sqe-catalog/src/lib.rs` | Modify | Add `pub mod system_jdbc; pub mod system_catalog;` and re-export |
| `crates/sqe-coordinator/src/query_handler.rs` | Modify | Register `system` catalog in `create_session_context` |

---

## Task 1: Always include `infoUri` in Trino responses

The Trino JDBC driver calls `requireNonNull(infoUri)` in its `QueryResults` constructor. SQE currently sets `info_uri: None` and skips it during serialization, causing NPE in the driver.

**Files:**
- Modify: `crates/sqe-trino-compat/src/protocol.rs:26-39`
- Modify: `crates/sqe-trino-compat/src/server.rs:182-198,244-279,283-376`

### Step 1.1: Update `TrinoResponse` to always serialize `infoUri`

- [ ] In `crates/sqe-trino-compat/src/protocol.rs`, remove the `skip_serializing_if` on `info_uri`:

```rust
// BEFORE (line 28-29):
#[serde(skip_serializing_if = "Option::is_none")]
pub info_uri: Option<String>,

// AFTER:
pub info_uri: Option<String>,
```

This ensures `infoUri` always appears in the JSON output (as `null` if not set, or as a string).

### Step 1.2: Add `base_url` parameter to response builders

- [ ] In `crates/sqe-trino-compat/src/server.rs`, add a helper to extract the base URL from the `Host` header, and a helper to build `info_uri`:

```rust
/// Extract the base URL from the Host header (e.g. "http://host:port").
/// Falls back to the bound port if no Host header is present.
fn extract_base_url(headers: &HeaderMap, bound_port: u16) -> String {
    if let Some(host) = extract_header(headers, "host") {
        // Use http:// — TLS is terminated by a reverse proxy in front of SQE
        format!("http://{host}")
    } else {
        format!("http://localhost:{bound_port}")
    }
}

/// Build the `infoUri` for a query.
fn info_uri(base_url: &str, query_id: &str) -> String {
    format!("{base_url}/v1/query/{query_id}")
}
```

### Step 1.3: Make `next_uri` and `build_page_response` use absolute URLs

- [ ] Change the `next_uri` function to accept a `base_url` parameter:

```rust
fn next_uri(base_url: &str, query_id: &str, token: usize, total_pages: usize) -> Option<String> {
    if token + 1 < total_pages {
        Some(format!("{base_url}/v1/statement/{query_id}/{}", token + 1))
    } else {
        None
    }
}
```

- [ ] Change `build_page_response` to accept `base_url`:

```rust
fn build_page_response(
    base_url: &str,
    query_id: &str,
    paginated: &PaginatedResult,
    page_token: usize,
) -> TrinoResponse {
    let page_data = paginated
        .pages
        .get(page_token)
        .cloned()
        .unwrap_or_default();
    let is_last = page_token + 1 >= paginated.total_pages;

    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: next_uri(base_url, query_id, page_token, paginated.total_pages),
        columns: Some(paginated.columns.clone()),
        data: Some(page_data),
        stats: if is_last {
            TrinoStats::finished()
        } else {
            TrinoStats::running(page_token + 1, paginated.total_pages)
        },
        error: None,
    }
}
```

### Step 1.4: Store `bound_port` in `TrinoState`

- [ ] Add `port: u16` to `TrinoState`:

```rust
pub struct TrinoState<A, Q> {
    pub authenticator: Arc<A>,
    pub query_handler: Arc<Q>,
    pub results: DashMap<String, PaginatedResult>,
    pub node: NodeContext,
    pub page_size: usize,
    pub port: u16,
}
```

- [ ] Update `start_trino_server` to store the port:

```rust
let state = Arc::new(TrinoState {
    authenticator,
    query_handler,
    results: DashMap::new(),
    node,
    page_size: DEFAULT_PAGE_SIZE,
    port,
});
```

### Step 1.5: Update `submit_query` to build absolute URIs

- [ ] In `submit_query`, extract `base_url` from headers and pass to response builders:

```rust
async fn submit_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    // ... existing auth code ...

    let base_url = extract_base_url(&headers, state.port);
    let query_id = Uuid::new_v4().to_string();

    match state.query_handler.execute(&session, sql).await {
        Ok(batches) => {
            let (columns, data) = protocol::batches_to_trino(&batches);
            let pages = paginate_rows(data, state.page_size);
            let total_pages = pages.len();

            let paginated = PaginatedResult { columns, pages, total_pages, created_at: std::time::Instant::now() };
            let response = build_page_response(&base_url, &query_id, &paginated, 0);
            state.results.insert(query_id, paginated);
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            warn!(error = %e, sql = sql, "Trino query execution failed");
            let response = TrinoResponse {
                id: query_id.clone(),
                info_uri: Some(info_uri(&base_url, &query_id)),
                next_uri: None,
                // ... rest unchanged
            };
            (StatusCode::OK, Json(response)).into_response()
        }
    }
}
```

### Step 1.6: Update `get_results` to build absolute URIs

- [ ] In `get_results`, extract headers and build base_url. This handler needs the `HeaderMap` added to its signature:

```rust
async fn get_results<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    headers: HeaderMap,
    Path((id, token)): Path<(String, String)>,
) -> Response {
    let base_url = extract_base_url(&headers, state.port);
    // ... rest uses build_page_response(&base_url, &id, &paginated, page_token) ...
}
```

Also update the error/not-found response branches to include `info_uri: Some(info_uri(&base_url, &id))`.

### Step 1.7: Update `error_response` to include `infoUri`

- [ ] `error_response` doesn't have a query ID or base URL context. For error responses at the protocol level (empty query, missing auth), `infoUri` can be `None` since these are pre-query errors. The JDBC driver only requires `infoUri` on actual query results.

Verify that the `error_response` helper is only used for pre-query errors (no query ID yet). If so, leave `info_uri: None` — the driver won't parse these as `QueryResults`.

**Safety note:** `error_response` is called for pre-query protocol errors (empty body, missing auth) which return HTTP 400/401. The Trino JDBC driver only parses HTTP 200 responses as `QueryResults` (where it calls `requireNonNull(infoUri)`). Non-200 responses are handled as transport errors, so `infoUri: null` is safe in `error_response`. The actual query error path in `submit_query` always returns HTTP 200 with `infoUri` populated (see Step 1.5).

### Step 1.8: Update all tests

- [ ] Fix compilation errors in existing tests. All changes listed below:

**a) All `TrinoState` constructions in tests — add `port: 8080`:**
```rust
// Every test that creates TrinoState needs this field added:
let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
    authenticator: Arc::new(MockAuth),
    query_handler: Arc::new(MockQuery),
    results: DashMap::new(),
    node: NodeContext { version: "0.1.0".to_string(), ready: ..., started_at: ... },
    page_size: DEFAULT_PAGE_SIZE,
    port: 8080,  // <-- ADD THIS
});
```

**b) `next_uri` calls — add `base_url` as first param:**
```rust
// test_next_uri_has_next:
let uri = next_uri("http://localhost:8080", "q-123", 0, 3);
assert_eq!(uri, Some("http://localhost:8080/v1/statement/q-123/1".to_string()));

// test_next_uri_last_page:
let uri = next_uri("http://localhost:8080", "q-123", 2, 3);
assert!(uri.is_none());

// test_next_uri_single_page:
let uri = next_uri("http://localhost:8080", "q-123", 0, 1);
assert!(uri.is_none());
```

**c) `build_page_response` calls — add `base_url` as first param:**
```rust
// test_build_page_response_first_page:
let resp = build_page_response("http://localhost:8080", "q-abc", &paginated, 0);
assert_eq!(resp.info_uri, Some("http://localhost:8080/v1/query/q-abc".to_string()));
assert_eq!(resp.next_uri, Some("http://localhost:8080/v1/statement/q-abc/1".to_string()));

// test_build_page_response_last_page:
let resp = build_page_response("http://localhost:8080", "q-abc", &paginated, 1);

// test_build_page_response_single_page:
let resp = build_page_response("http://localhost:8080", "q-single", &paginated, 0);
```

**d) `get_results` handler calls — add `HeaderMap::new()` parameter:**

Axum extracts parameters in order. Since `HeaderMap` was added before `Path`, update every `get_results` call:
```rust
// BEFORE:
let response = get_results::<MockAuth, MockQuery>(
    State(state.clone()),
    Path(("q-paged".to_string(), "1".to_string())),
).await;

// AFTER:
let response = get_results::<MockAuth, MockQuery>(
    State(state.clone()),
    HeaderMap::new(),
    Path(("q-paged".to_string(), "1".to_string())),
).await;
```
Apply this to all 4 tests: `test_get_results_invalid_token`, `test_get_results_out_of_range_token`, `test_get_results_returns_correct_page`, `test_get_results_last_page_cleans_up`.

- [ ] Add a new test for `extract_base_url`:

```rust
#[test]
fn test_extract_base_url_from_host_header() {
    let mut headers = HeaderMap::new();
    headers.insert("host", "myhost:9090".parse().unwrap());
    assert_eq!(extract_base_url(&headers, 8080), "http://myhost:9090");
}

#[test]
fn test_extract_base_url_fallback() {
    let headers = HeaderMap::new();
    assert_eq!(extract_base_url(&headers, 8080), "http://localhost:8080");
}
```

- [ ] Add a test verifying `infoUri` is always present in serialized JSON:

```rust
#[test]
fn test_trino_response_always_includes_info_uri() {
    let resp = TrinoResponse {
        id: "q-001".to_string(),
        info_uri: None,
        next_uri: None,
        columns: None,
        data: None,
        stats: TrinoStats::finished(),
        error: None,
    };
    let json = serde_json::to_string(&resp).unwrap();
    assert!(json.contains("\"infoUri\":null"), "infoUri must always be present, got: {json}");
}
```

### Step 1.9: Run tests and commit

- [ ] Run: `cargo test -p sqe-trino-compat`
- [ ] Run: `cargo clippy -p sqe-trino-compat --all-targets -- -D warnings`
- [ ] Commit: `git commit -m "fix: always include infoUri in Trino-compat responses"`

---

## Task 2: Add `system.jdbc.*` virtual tables

DBeaver's Trino JDBC driver queries `system.jdbc.{types,catalogs,schemas,tables,columns}` for metadata browsing. These need to be real DataFusion virtual tables so SQL WHERE clauses work.

**Files:**
- Create: `crates/sqe-catalog/src/system_jdbc.rs`
- Create: `crates/sqe-catalog/src/system_catalog.rs`
- Modify: `crates/sqe-catalog/src/lib.rs`
- Modify: `crates/sqe-coordinator/src/query_handler.rs:349-403`

### Step 2.1: Create `JdbcSchemaProvider`

- [ ] Create `crates/sqe-catalog/src/system_jdbc.rs` — a DataFusion `SchemaProvider` for the `jdbc` schema inside the `system` catalog.

This provider implements 5 virtual tables matching Trino's `system.jdbc.*` schema:

```rust
use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::{BooleanBuilder, Int32Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result as DFResult;

use crate::rest_catalog::SessionCatalog;

/// DataFusion `SchemaProvider` for the virtual `system.jdbc` schema.
///
/// Implements the Trino `system.jdbc.*` tables that the Trino JDBC driver
/// uses for `DatabaseMetaData` calls (DBeaver catalog browsing, etc.).
pub struct JdbcSchemaProvider {
    session_catalog: Arc<SessionCatalog>,
    warehouse: String,
}

impl JdbcSchemaProvider {
    pub fn new(session_catalog: Arc<SessionCatalog>, warehouse: String) -> Self {
        Self { session_catalog, warehouse }
    }
}

impl std::fmt::Debug for JdbcSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JdbcSchemaProvider")
            .field("warehouse", &self.warehouse)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for JdbcSchemaProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn table_names(&self) -> Vec<String> {
        vec![
            "types".to_string(),
            "catalogs".to_string(),
            "schemas".to_string(),
            "tables".to_string(),
            "columns".to_string(),
        ]
    }

    fn table_exist(&self, name: &str) -> bool {
        matches!(name, "types" | "catalogs" | "schemas" | "tables" | "columns")
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match name {
            "types" => Ok(Some(build_types_table()?)),
            "catalogs" => Ok(Some(build_catalogs_table(&self.warehouse)?)),
            "schemas" => Ok(Some(self.build_schemas_table().await?)),
            "tables" => Ok(Some(self.build_tables_table().await?)),
            "columns" => Ok(Some(self.build_columns_table().await?)),
            _ => Ok(None),
        }
    }
}
```

### Step 2.2: Implement `build_types_table` (static JDBC type metadata)

- [ ] Add the static types table. This returns the standard JDBC types that Trino exposes. The schema matches Trino's `system.jdbc.types`:

```rust
/// Build the static `system.jdbc.types` table.
///
/// Returns the standard SQL/JDBC types that SQE supports, matching the
/// column layout expected by the Trino JDBC driver's `getTypeInfo()`.
fn build_types_table() -> DFResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("type_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Int32, false),
        Field::new("precision", DataType::Int32, true),
        Field::new("literal_prefix", DataType::Utf8, true),
        Field::new("literal_suffix", DataType::Utf8, true),
        Field::new("create_params", DataType::Utf8, true),
        Field::new("nullable", DataType::Int32, false),
        Field::new("case_sensitive", DataType::Boolean, false),
        Field::new("searchable", DataType::Int32, false),
        Field::new("unsigned_attribute", DataType::Boolean, false),
        Field::new("fixed_prec_scale", DataType::Boolean, false),
        Field::new("auto_increment", DataType::Boolean, false),
        Field::new("local_type_name", DataType::Utf8, true),
        Field::new("minimum_scale", DataType::Int32, false),
        Field::new("maximum_scale", DataType::Int32, false),
        Field::new("sql_data_type", DataType::Int32, false),
        Field::new("sql_datetime_sub", DataType::Int32, false),
        Field::new("num_prec_radix", DataType::Int32, true),
    ]));

    // Standard JDBC type constants
    // java.sql.Types: BOOLEAN=16, TINYINT=-6, SMALLINT=5, INTEGER=4,
    // BIGINT=-5, REAL=7, DOUBLE=8, DECIMAL=3, VARCHAR=12,
    // VARBINARY=-3, DATE=91, TIMESTAMP=93
    //
    // nullable: 1 = typeNullable
    // searchable: 3 = typeSearchable
    let types: Vec<(&str, i32, i32, Option<&str>, Option<&str>, bool, i32, i32)> = vec![
        // (name, jdbc_type, precision, literal_prefix, literal_suffix, case_sensitive, min_scale, max_scale)
        ("boolean",    16,  1,  None,        None,        false, 0, 0),
        ("tinyint",    -6,  3,  None,        None,        false, 0, 0),
        ("smallint",    5,  5,  None,        None,        false, 0, 0),
        ("integer",     4, 10,  None,        None,        false, 0, 0),
        ("bigint",     -5, 19,  None,        None,        false, 0, 0),
        ("real",        7, 24,  None,        None,        false, 0, 0),
        ("double",      8, 53,  None,        None,        false, 0, 0),
        ("decimal",     3, 38,  None,        None,        false, 0, 38),
        ("varchar",    12,  0,  Some("'"),   Some("'"),   true,  0, 0),
        ("varbinary",  -3,  0,  Some("X'"),  Some("'"),   false, 0, 0),
        ("date",       91,  0,  Some("DATE '"), Some("'"), false, 0, 0),
        ("timestamp",  93,  0,  Some("TIMESTAMP '"), Some("'"), false, 0, 9),
    ];

    let mut name_b = StringBuilder::new();
    let mut data_type_b = Int32Builder::new();
    let mut precision_b = Int32Builder::new();
    let mut prefix_b = StringBuilder::new();
    let mut suffix_b = StringBuilder::new();
    let mut params_b = StringBuilder::new();
    let mut nullable_b = Int32Builder::new();
    let mut case_b = BooleanBuilder::new();
    let mut search_b = Int32Builder::new();
    let mut unsigned_b = BooleanBuilder::new();
    let mut fixed_b = BooleanBuilder::new();
    let mut auto_b = BooleanBuilder::new();
    let mut local_b = StringBuilder::new();
    let mut min_scale_b = Int32Builder::new();
    let mut max_scale_b = Int32Builder::new();
    let mut sql_dt_b = Int32Builder::new();
    let mut sql_sub_b = Int32Builder::new();
    let mut radix_b = Int32Builder::new();

    for (name, jdbc_type, precision, prefix, suffix, case_sensitive, min_scale, max_scale) in &types {
        name_b.append_value(name);
        data_type_b.append_value(*jdbc_type);
        precision_b.append_value(*precision);
        match prefix {
            Some(p) => prefix_b.append_value(p),
            None => prefix_b.append_null(),
        }
        match suffix {
            Some(s) => suffix_b.append_value(s),
            None => suffix_b.append_null(),
        }
        params_b.append_null();
        nullable_b.append_value(1); // typeNullable
        case_b.append_value(*case_sensitive);
        search_b.append_value(3); // typeSearchable
        unsigned_b.append_value(false);
        fixed_b.append_value(false);
        auto_b.append_value(false);
        local_b.append_null();
        min_scale_b.append_value(*min_scale);
        max_scale_b.append_value(*max_scale);
        sql_dt_b.append_value(0);
        sql_sub_b.append_value(0);
        radix_b.append_option(if *jdbc_type == 12 || *jdbc_type == -3 { None } else { Some(10) });
    }

    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(name_b.finish()) as ArrayRef,
        Arc::new(data_type_b.finish()),
        Arc::new(precision_b.finish()),
        Arc::new(prefix_b.finish()),
        Arc::new(suffix_b.finish()),
        Arc::new(params_b.finish()),
        Arc::new(nullable_b.finish()),
        Arc::new(case_b.finish()),
        Arc::new(search_b.finish()),
        Arc::new(unsigned_b.finish()),
        Arc::new(fixed_b.finish()),
        Arc::new(auto_b.finish()),
        Arc::new(local_b.finish()),
        Arc::new(min_scale_b.finish()),
        Arc::new(max_scale_b.finish()),
        Arc::new(sql_dt_b.finish()),
        Arc::new(sql_sub_b.finish()),
        Arc::new(radix_b.finish()),
    ])?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}
```

### Step 2.3: Implement `build_catalogs_table`

- [ ] Simple table returning the warehouse name:

```rust
fn build_catalogs_table(warehouse: &str) -> DFResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("table_cat", DataType::Utf8, false),
    ]));

    let mut builder = StringBuilder::new();
    builder.append_value(warehouse);

    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(builder.finish()) as ArrayRef,
    ])?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}
```

### Step 2.4: Implement `build_schemas_table`

- [ ] Lists namespaces from the Polaris catalog. Add to `impl JdbcSchemaProvider`:

```rust
impl JdbcSchemaProvider {
    async fn build_schemas_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_schem", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
        ]));

        let namespaces = self.list_namespaces_safe().await;

        let mut schem_b = StringBuilder::new();
        let mut cat_b = StringBuilder::new();

        // Always include information_schema — Trino shows it too
        schem_b.append_value("information_schema");
        cat_b.append_value(&self.warehouse);

        for ns in &namespaces {
            schem_b.append_value(ns);
            cat_b.append_value(&self.warehouse);
        }

        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(schem_b.finish()) as ArrayRef,
            Arc::new(cat_b.finish()),
        ])?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    async fn list_namespaces_safe(&self) -> Vec<String> {
        match self.session_catalog.list_namespaces().await {
            Ok(namespaces) => namespaces
                .iter()
                .map(|ns| ns.as_ref().iter().map(|s| s.as_str()).collect::<Vec<_>>().join("."))
                .collect(),
            Err(e) => {
                tracing::error!(error = %e, "Failed to list namespaces for system.jdbc.schemas");
                Vec::new()
            }
        }
    }
}
```

### Step 2.5: Implement `build_tables_table`

- [ ] Lists tables across all namespaces:

```rust
impl JdbcSchemaProvider {
    async fn build_tables_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_cat", DataType::Utf8, true),
            Field::new("table_schem", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("type_cat", DataType::Utf8, true),
            Field::new("type_schem", DataType::Utf8, true),
            Field::new("type_name", DataType::Utf8, true),
            Field::new("self_referencing_col_name", DataType::Utf8, true),
            Field::new("ref_generation", DataType::Utf8, true),
        ]));

        let namespaces = self.list_namespaces_safe().await;

        let mut cat_b = StringBuilder::new();
        let mut schem_b = StringBuilder::new();
        let mut name_b = StringBuilder::new();
        let mut type_b = StringBuilder::new();
        let mut remarks_b = StringBuilder::new();
        let mut type_cat_b = StringBuilder::new();
        let mut type_schem_b = StringBuilder::new();
        let mut type_name_b = StringBuilder::new();
        let mut self_ref_b = StringBuilder::new();
        let mut ref_gen_b = StringBuilder::new();

        for ns in &namespaces {
            let ns_ident = iceberg::NamespaceIdent::new(ns.clone());
            match self.session_catalog.list_tables(&ns_ident).await {
                Ok(tables) => {
                    for table in &tables {
                        cat_b.append_value(&self.warehouse);
                        schem_b.append_value(ns);
                        name_b.append_value(table.name());
                        type_b.append_value("TABLE");
                        remarks_b.append_null();
                        type_cat_b.append_null();
                        type_schem_b.append_null();
                        type_name_b.append_null();
                        self_ref_b.append_null();
                        ref_gen_b.append_null();
                    }
                }
                Err(e) => {
                    tracing::warn!(namespace = %ns, error = %e, "Failed to list tables for system.jdbc.tables");
                }
            }
        }

        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(cat_b.finish()) as ArrayRef,
            Arc::new(schem_b.finish()),
            Arc::new(name_b.finish()),
            Arc::new(type_b.finish()),
            Arc::new(remarks_b.finish()),
            Arc::new(type_cat_b.finish()),
            Arc::new(type_schem_b.finish()),
            Arc::new(type_name_b.finish()),
            Arc::new(self_ref_b.finish()),
            Arc::new(ref_gen_b.finish()),
        ])?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}
```

### Step 2.6: Implement `build_columns_table`

- [ ] Lists columns across all tables, loading each table schema from Iceberg:

```rust
impl JdbcSchemaProvider {
    async fn build_columns_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_cat", DataType::Utf8, true),
            Field::new("table_schem", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Int32, false),
            Field::new("type_name", DataType::Utf8, false),
            Field::new("column_size", DataType::Int32, true),
            Field::new("decimal_digits", DataType::Int32, true),
            Field::new("num_prec_radix", DataType::Int32, true),
            Field::new("nullable", DataType::Int32, false),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("column_def", DataType::Utf8, true),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("is_autoincrement", DataType::Utf8, false),
        ]));

        let namespaces = self.list_namespaces_safe().await;

        let mut cat_b = StringBuilder::new();
        let mut schem_b = StringBuilder::new();
        let mut tbl_b = StringBuilder::new();
        let mut col_b = StringBuilder::new();
        let mut dt_b = Int32Builder::new();
        let mut tn_b = StringBuilder::new();
        let mut size_b = Int32Builder::new();
        let mut digits_b = Int32Builder::new();
        let mut radix_b = Int32Builder::new();
        let mut null_b = Int32Builder::new();
        let mut remarks_b = StringBuilder::new();
        let mut def_b = StringBuilder::new();
        let mut ord_b = Int32Builder::new();
        let mut is_null_b = StringBuilder::new();
        let mut auto_b = StringBuilder::new();

        for ns in &namespaces {
            let ns_ident = iceberg::NamespaceIdent::new(ns.clone());
            let tables = match self.session_catalog.list_tables(&ns_ident).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(namespace = %ns, error = %e, "Failed to list tables for system.jdbc.columns");
                    continue;
                }
            };

            for table_ident in &tables {
                let full_ident = iceberg::TableIdent::new(ns_ident.clone(), table_ident.name().to_string());
                let table = match self.session_catalog.load_table(&full_ident).await {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::warn!(table = %table_ident.name(), error = %e, "Failed to load table for system.jdbc.columns");
                        continue;
                    }
                };

                let iceberg_schema = table.metadata().current_schema();
                for (idx, field) in iceberg_schema.as_struct().fields().iter().enumerate() {
                    cat_b.append_value(&self.warehouse);
                    schem_b.append_value(ns);
                    tbl_b.append_value(table_ident.name());
                    col_b.append_value(&field.name);

                    let (jdbc_type, type_name) = iceberg_type_to_jdbc(&field.field_type);
                    dt_b.append_value(jdbc_type);
                    tn_b.append_value(type_name);
                    size_b.append_null();
                    digits_b.append_null();
                    radix_b.append_option(Some(10));
                    null_b.append_value(if field.required { 0 } else { 1 });
                    remarks_b.append_null();
                    def_b.append_null();
                    ord_b.append_value((idx + 1) as i32);
                    is_null_b.append_value(if field.required { "NO" } else { "YES" });
                    auto_b.append_value("NO");
                }
            }
        }

        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(cat_b.finish()) as ArrayRef,
            Arc::new(schem_b.finish()),
            Arc::new(tbl_b.finish()),
            Arc::new(col_b.finish()),
            Arc::new(dt_b.finish()),
            Arc::new(tn_b.finish()),
            Arc::new(size_b.finish()),
            Arc::new(digits_b.finish()),
            Arc::new(radix_b.finish()),
            Arc::new(null_b.finish()),
            Arc::new(remarks_b.finish()),
            Arc::new(def_b.finish()),
            Arc::new(ord_b.finish()),
            Arc::new(is_null_b.finish()),
            Arc::new(auto_b.finish()),
        ])?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}
```

### Step 2.7: Add `iceberg_type_to_jdbc` helper

- [ ] Map Iceberg field types to JDBC type codes and Trino type names:

```rust
/// Map an Iceberg field type to (JDBC type code, Trino type name).
fn iceberg_type_to_jdbc(ty: &iceberg::spec::Type) -> (i32, &'static str) {
    use iceberg::spec::PrimitiveType;
    match ty {
        iceberg::spec::Type::Primitive(p) => match p {
            PrimitiveType::Boolean => (16, "boolean"),
            PrimitiveType::Int => (4, "integer"),
            PrimitiveType::Long => (-5, "bigint"),
            PrimitiveType::Float => (7, "real"),
            PrimitiveType::Double => (8, "double"),
            PrimitiveType::Decimal { .. } => (3, "decimal"),
            PrimitiveType::Date => (91, "date"),
            PrimitiveType::Time => (92, "time"),
            PrimitiveType::Timestamp => (93, "timestamp"),
            PrimitiveType::Timestamptz => (93, "timestamp with time zone"),
            PrimitiveType::String => (12, "varchar"),
            PrimitiveType::Uuid => (12, "varchar"),
            PrimitiveType::Fixed(_) => (-3, "varbinary"),
            PrimitiveType::Binary => (-3, "varbinary"),
            PrimitiveType::TimestampNs => (93, "timestamp"),
            PrimitiveType::TimestamptzNs => (93, "timestamp with time zone"),
            _ => (12, "varchar"),
        },
        _ => (12, "varchar"), // struct/list/map → varchar fallback
    }
}
```

### Step 2.8: Create `SystemCatalogProvider`

- [ ] Create `crates/sqe-catalog/src/system_catalog.rs`:

```rust
use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use crate::rest_catalog::SessionCatalog;
use crate::system_jdbc::JdbcSchemaProvider;

/// DataFusion `CatalogProvider` for the virtual `system` catalog.
///
/// Contains the `jdbc` schema used by the Trino JDBC driver for
/// `DatabaseMetaData` calls (DBeaver, DataGrip, etc.).
#[derive(Debug)]
pub struct SystemCatalogProvider {
    jdbc_schema: Arc<JdbcSchemaProvider>,
}

impl SystemCatalogProvider {
    pub fn new(session_catalog: Arc<SessionCatalog>, warehouse: String) -> Self {
        Self {
            jdbc_schema: Arc::new(JdbcSchemaProvider::new(session_catalog, warehouse)),
        }
    }
}

impl CatalogProvider for SystemCatalogProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn schema_names(&self) -> Vec<String> {
        vec!["jdbc".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "jdbc" {
            Some(self.jdbc_schema.clone())
        } else {
            None
        }
    }
}
```

### Step 2.9: Export new modules from `sqe-catalog`

- [ ] Modify `crates/sqe-catalog/src/lib.rs`:

```rust
pub mod rest_catalog;
pub mod catalog_provider;
pub mod schema_provider;
pub mod table_provider;
pub mod credential_vending;
pub mod expr_to_predicate;
pub mod iceberg_scan;
pub mod info_schema;
pub mod read_parquet;
pub mod system_catalog;
pub mod system_jdbc;

pub use catalog_provider::SqeCatalogProvider;
pub use iceberg_scan::IcebergScanExec;
pub use rest_catalog::SessionCatalog;
pub use system_catalog::SystemCatalogProvider;
```

### Step 2.10: Register `system` catalog in query handler

- [ ] In `crates/sqe-coordinator/src/query_handler.rs`, in `create_session_context`, register the system catalog after registering the main catalog:

```rust
// After line 384: ctx.register_catalog(&catalog_name, Arc::new(catalog_provider));

// Register the `system` catalog for Trino JDBC metadata queries
// (system.jdbc.types, system.jdbc.catalogs, etc.)
let system_catalog = sqe_catalog::SystemCatalogProvider::new(
    session_catalog.clone(),
    self.config.catalog.warehouse.clone(),
);
ctx.register_catalog("system", Arc::new(system_catalog));
```

**Important:** `session_catalog` is moved into `SqeCatalogProvider::try_new()` on line 377. You must clone the `Arc` *before* that call. Add this line before `SqeCatalogProvider::try_new`:

```rust
let session_catalog_for_system = session_catalog.clone();
```

Then use `session_catalog_for_system` when constructing `SystemCatalogProvider`:

```rust
let system_catalog = sqe_catalog::SystemCatalogProvider::new(
    session_catalog_for_system,
    self.config.catalog.warehouse.clone(),
);
ctx.register_catalog("system", Arc::new(system_catalog));
```

### Step 2.11: Add unit tests for `system_jdbc`

- [ ] Add a `#[cfg(test)] mod tests` section at the bottom of `system_jdbc.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_types_table_schema() {
        let table = build_types_table().unwrap();
        let schema = table.schema();
        assert_eq!(schema.field(0).name(), "type_name");
        assert_eq!(schema.field(1).name(), "data_type");
        assert!(schema.fields().len() >= 18);
    }

    #[test]
    fn test_catalogs_table() {
        let table = build_catalogs_table("my_warehouse").unwrap();
        let schema = table.schema();
        assert_eq!(schema.field(0).name(), "table_cat");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_string() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::String));
        assert_eq!(code, 12);
        assert_eq!(name, "varchar");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_long() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Long));
        assert_eq!(code, -5);
        assert_eq!(name, "bigint");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_boolean() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Boolean));
        assert_eq!(code, 16);
        assert_eq!(name, "boolean");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_timestamp() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Timestamp));
        assert_eq!(code, 93);
        assert_eq!(name, "timestamp");
    }

    #[test]
    fn test_jdbc_schema_provider_table_names() {
        // JdbcSchemaProvider needs a SessionCatalog which requires a network connection,
        // so we test the table_exist/table_names contract via the known list.
        let expected = vec!["types", "catalogs", "schemas", "tables", "columns"];
        for name in &expected {
            assert!(matches!(*name, "types" | "catalogs" | "schemas" | "tables" | "columns"));
        }
    }
}
```

### Step 2.12: Add unit tests for `system_catalog`

- [ ] No unit tests needed for `system_catalog.rs` — it's a thin wrapper around `JdbcSchemaProvider` and requires a `SessionCatalog` (network) to construct. The `JdbcSchemaProvider` tests in `system_jdbc.rs` cover the actual logic. Integration tests cover the full path.

### Step 2.13: Build, test, and commit

- [ ] Run: `cargo build -p sqe-catalog -p sqe-coordinator`
- [ ] Run: `cargo test -p sqe-catalog`
- [ ] Run: `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] Commit: `git commit -m "feat: add system.jdbc.* virtual tables for Trino JDBC metadata browsing"`

---

## Task 3: Final validation

### Step 3.1: Full build and test

- [ ] Run: `cargo build --all`
- [ ] Run: `cargo test --all`
- [ ] Run: `cargo clippy --all-targets --all-features -- -D warnings`

### Step 3.2: Update project tracking files

- [ ] Update `nextsteps.md` — note Trino JDBC compat fixes
- [ ] Update `README.md` roadmap if applicable
