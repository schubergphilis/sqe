# Attaching at Runtime {#sec:attach-runtime}

> The catalog list is not a config file.
> It is a process. The user changes it from SQL.

The previous chapter ended with six backends behind one dispatch. Operators wrote TOML. The engine connected. That story closed cleanly. Then the same teammate who pasted a HuggingFace path into the CLI asked the next obvious question.

"Can I attach a Glue catalog from SQL?"

Editing TOML and restarting the coordinator is the right answer when a deployment starts. It is the wrong answer when an analyst wants to point at a partner's catalog for ten minutes to compare two snapshots. DuckDB has a primitive for this. So does Trino, and Snowflake, and BigQuery. SQE did not.

This chapter is about closing that gap. Three SQL primitives. One registry. One lifecycle bug that took longer to diagnose than to fix.

## The DuckDB shape

We borrowed the surface from DuckDB because every analyst who has touched DuckDB already knows it.

```sql
ATTACH 'http://catalog.example.com:9090/api/catalog' AS partner
  (TYPE iceberg_rest, WAREHOUSE 'analytics');

DETACH partner;
```

The parser extension is a sqlparser post-parse rewrite. The standard parser sees `ATTACH '...'` and either chokes or treats it as something else; we run a second pass that recognises the keyword and the option list, then constructs a custom `AttachStatement` AST node. Same approach as the `MASKED WITH` and `ROWS WHERE` extensions from chapter 9. The standard parser does the heavy lifting; we lift only the sentences it does not understand.

```rust
pub struct AttachStatement {
    pub name: String,
    pub location: String,
    pub kind: CatalogKind,
    pub options: BTreeMap<String, OptionValue>,
}
```

The `CatalogKind` enum lists every backend the loader from chapter 6b knows about: `Sqlite`, `IcebergRest`, `Glue`, `S3tables`, `Hms`, `Jdbc`. The dispatch reuses the same `build_catalog` function the TOML path calls. One translation point. Two callers.

## Secrets need a home

ATTACH on its own is not enough. Real catalogs need credentials. Hardcoding a bearer token into the SQL is a non-starter; it ends up in query history, in dbt logs, in screenshots someone pastes into Slack. DuckDB solved this with `CREATE SECRET`. We borrowed that too.

```sql
CREATE SECRET partner_tok (TYPE bearer, TOKEN 'eyJ...');

ATTACH 'http://catalog.example.com/api/catalog' AS partner
  (TYPE iceberg_rest, WAREHOUSE 'analytics', SECRET partner_tok);
```

The secret never appears in the query the user sees. The plan history shows `SECRET partner_tok`. The catalog dispatch reads the bytes from the `SecretStore` at attach time and passes them to the upstream builder. Three secret kinds cover the cases we have: `bearer` for REST tokens, `aws` for access-key triplets, `basic` for username and password.

The store itself is small.

```rust
pub struct SecretStore {
    inner: Arc<RwLock<HashMap<String, Secret>>>,
}

pub enum Secret {
    Bearer { token: String },
    Aws { access_key, secret_key, session_token, region, profile },
    Basic { username: String, password: String },
}

impl Drop for Secret {
    fn drop(&mut self) {
        // Zeroize the bytes so the heap snapshot of a crashed
        // coordinator does not leak credentials.
    }
}
```

`zeroize` matters because process dumps land in places. A coordinator that segfaults under memory pressure writes a core dump. A core dump that contains live bearer tokens is an incident waiting to be reported. The `Drop` impl makes the leak window the lifetime of the secret, not the lifetime of the process.

The store also enforces a guard: `DROP SECRET` fails if any attached catalog still references the secret. The error names both the secret and the catalog so the user knows which DETACH to issue first.

```
DROP SECRET partner_tok;
ERROR: secret 'partner_tok' is still in use by catalog 'partner'.
       DETACH the catalog first.
```

The guard exists because the alternative (silently invalidating attached catalogs when their secret disappears) is the kind of bug that surfaces in production and not in tests.

## The registry

Two state stores. The `SecretStore` for credentials. The `RuntimeCatalogRegistry` for catalogs. Both are `Arc<RwLock<HashMap<...>>>` under the hood. Both are process-local. Both are wiped on coordinator restart.

Process-local is the right scope for the first version. Persistent ATTACH (where attached catalogs survive a restart) is a feature that operators ask for but most do not want once they think about it. A catalog attached at 9 AM on Monday is in the system at 3 AM on Sunday because someone forgot to DETACH it. The credentials behind it have rotated. The query against it returns 401. The on-call engineer wakes up to a query failure for a catalog they did not know existed. Static TOML catalogs are the right shape for "this is part of the deployment." ATTACH is the right shape for "this is part of this session."

Both can coexist. A coordinator with `[catalogs.polaris]` in its TOML and an analyst running `ATTACH '...' AS partner` sees both names in `SHOW CATALOGS`. The dispatch is uniform.

## The lifecycle bug

The first end-to-end test against a real REST endpoint failed.

```
Error: Failed to build session context: Catalog error:
  Failed to list namespaces: error sending request for url
  (http://localhost:59998/v1/config?warehouse=)
```

The fake REST server was on port 59999. The error said port 59998. That was the primary catalog from `[catalog]` in the test config. The ATTACH was hitting the wrong endpoint.

Reading the dispatch made the cause obvious.

```rust
StatementKind::Attach(stmt) => {
    let (ctx, _) = self.create_session_context(session).await?;
    self.handle_attach(stmt, &ctx).await
}
```

`create_session_context` connects to the primary catalog because that is what session contexts are for. Calling it inside `handle_attach` was a leftover from the first draft: I needed a `SessionContext` to register the new catalog into, and the easy way to get one was to ask the function that hands them out.

The leftover had two consequences. First, ATTACH could not run when the primary catalog was unreachable. That made testing the REST path impossible without standing up a fake primary too. Second, and worse, the catalog was registered onto a `SessionContext` that the handler returned out of. The next query built a fresh `SessionContext` from the same cache key, hit the same connection to the primary, and registered the same primary catalogs. Nothing in that path consulted the registry. The attached catalog vanished between the ATTACH statement and the next SELECT against it.

We had a SQL primitive that worked exactly once, on exactly the connection that ran it.

## The fix

Make the registry the source of truth. Make the `SessionContext` a derived view.

The registry stores the built `Arc<dyn CatalogProvider>` alongside the raw `Arc<dyn iceberg::Catalog>`. ATTACH builds the catalog, wraps it in `WritableIcebergCatalog`, and stashes both in the map. No `SessionContext` involved.

```rust
pub async fn attach(
    &self,
    stmt: &AttachStatement,
    secrets: &SecretStore,
) -> Result<(), String> {
    // 1. Refuse duplicate name.
    // 2. Build the iceberg::Catalog via the loader from chapter 6b.
    // 3. Wrap in WritableIcebergCatalog.
    // 4. Insert into the registry under a write lock.
}
```

`create_session_context` then takes a reference to the registry. After registering the primary catalogs, it iterates `registry.providers()` and registers every attached one too.

```rust
for (name, provider) in &attached_providers {
    ctx.register_catalog(name.clone(), Arc::clone(provider));
}
```

The handler invalidates the session-context cache after every ATTACH and DETACH. The next query builds a fresh context that includes the new catalog set. The catalog survives the handler's return because the registry holds the `Arc` independently.

This is the same pattern as policy stores from chapter 9. The plan rewriter does not store policies; it consults the policy store on every plan. The session builder does not store catalogs; it consults the registry on every build. The state lives in one place. The consumers are stateless.

## The downcast that did not work

DETACH had a separate problem. DataFusion 53.1 has `register_catalog` on the `CatalogProviderList` trait. It does not have `deregister_catalog`. The original code worked around this by downcasting to the default `MemoryCatalogProviderList` and removing the entry from its inner `DashMap`.

```rust
let list = state.catalog_list();
if let Some(memlist) = list.as_any().downcast_ref::<MemoryCatalogProviderList>() {
    memlist.catalogs.remove(name);
}
```

This worked in the unit tests. It failed in embedded mode. The embedded CLI calls `enable_url_table()` to make `SELECT * FROM 'file.parquet'` work, and `enable_url_table()` wraps the catalog list in a `DynamicFileCatalog`. The downcast to `MemoryCatalogProviderList` fails. The catalog stays registered in DataFusion even though our registry has forgotten it.

The fix made the downcast unnecessary. The registry-as-source-of-truth model means DETACH only removes from the registry. The next session-context rebuild does not register the detached catalog because it is no longer in `providers()`. DataFusion's view follows the registry, one cache invalidation behind.

We deleted the downcast and the warning that fired when it failed. Less code. Same behaviour. Better behaviour, in fact: the embedded path now works the same as the cluster path.

## Testing without a real REST endpoint

The REST catalog path needed integration tests. Standing up Polaris in CI for seven test cases is overkill. We used `wiremock`.

```rust
async fn mount_rest_fixture(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200)
            .set_body_string(r#"{"overrides":{},"defaults":{}}"#))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200)
            .set_body_string(r#"{"namespaces":[]}"#))
        .mount(server)
        .await;
}
```

Two endpoints. Two responses. The iceberg-rust REST client calls `/v1/config` to read overrides and `/v1/namespaces` to enumerate the catalog. Both are happy with empty payloads in the success-path test. The test asserts that ATTACH returns `Ok` with no rows.

A second fixture variant guards both endpoints with `header_exists("Authorization")` and adds a catch-all that returns 401 to any unauthenticated GET. That fixture proves the bearer token from `CREATE SECRET ... TYPE bearer` actually reaches the wire. If the dispatch ever stops forwarding the secret, the test fails with the right error.

The tests run in 7 seconds against the in-process wiremock server. Seven cases cover the lifecycle: ATTACH success, duplicate name, attach-detach-reattach, DETACH unknown, ATTACH with bearer, DROP SECRET in-use guard, DROP SECRET after DETACH.

## What changed in `SHOW CATALOGS`

The pre-flight check that rejects unknown 3-part qualifiers needed an update too. Without it, `SELECT * FROM partner.sales.orders` against a freshly attached `partner` catalog would error with "unknown catalog" because the check only looked at the static TOML list.

```rust
fn known_catalog_names(&self) -> Vec<String> {
    let mut names: Vec<String> = self.config.flattened_catalogs()
        .into_iter().map(|(n, _)| n).collect();
    names.push("system".to_string());
    names.push("datafusion".to_string());
    names.extend(self.runtime_catalogs.list());
    names.sort();
    names.dedup();
    names
}
```

The list is the union of TOML catalogs, the two coordinator-registered system catalogs, and the runtime registry. `SHOW CATALOGS` reads from the same union. The user sees what the engine sees. The pre-flight check accepts what the planner accepts.

## The lesson

Two lessons. One is small. One is large.

The small one is about leftovers. The first draft of `handle_attach` called `create_session_context` because that was the obvious way to get a `SessionContext`. The obvious way was wrong. The handler did not need a `SessionContext`; it needed to update a registry. Those are different operations. Reaching for the wrong primitive cost a day of debugging once the integration tests showed up. Read the call site twice. Ask whether the dependency is structural or accidental. Cut accidental ones.

The large lesson is about state stores. The book has two so far. The policy store from chapter 9 holds row filters and column masks. The runtime catalog registry from this chapter holds attached catalogs. Both are `Arc<RwLock<...>>`. Both are consulted on every relevant cache miss. Both are the source of truth for one slice of the engine's behaviour.

The shape repeats because the problem repeats. Whenever the engine has state that mutates between queries, we want a cheap-to-clone handle, a single owner, and a stateless consumer. DataFusion's `SessionContext` is the wrong place for that state because `SessionContext` is per-query and per-user. The state belongs in the coordinator. The session is just one of its readers.

Future state stores follow the same shape. The next one in the queue is a runtime grant cache for the GRANT/REVOKE chapters. Same pattern: registry as source of truth, session as derived view, cache invalidation on writes.

The engine is starting to look like a database, not a query parser. That is the right direction.
