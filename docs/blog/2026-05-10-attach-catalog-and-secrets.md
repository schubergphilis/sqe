---
title: "Mounting catalogs from SQL: ATTACH, DETACH, and the registry pattern"
description: "SQE now ships DuckDB-style ATTACH / DETACH and CREATE / DROP SECRET. The story of building it covers parser extension, credential hygiene, a lifecycle bug we found in the integration tests, and a state-store pattern that is starting to repeat."
pubDate: "2026-05-10"
author: "Jacob Verhoeks"
tags:
  - "iceberg"
  - "duckdb"
  - "datafusion"
  - "catalogs"
  - "developer-experience"
---



*May 10, 2026*

An analyst on our team asked the obvious question last week. SQE talks to six Iceberg catalog backends through TOML. Polaris, Glue, S3 Tables, HMS, JDBC, Hadoop. All wired up at coordinator startup. Their question:

"Can I attach a Glue catalog from SQL? I want to compare two snapshots from a partner's bucket for an hour and then go back to my own catalog."

Editing the deployment TOML and restarting the coordinator is the right answer at deployment time. It is the wrong answer when the use case is "for an hour." DuckDB has a one-line primitive for this. Trino has one. Snowflake has one. SQE did not.

This post is about closing that gap. Three SQL primitives. One registry. One lifecycle bug we found because we wrote the integration test before the implementation.

## The shape we shipped

```sql
CREATE SECRET partner_tok (TYPE bearer, TOKEN 'eyJhbGciOiJSUzI1...');

ATTACH 'http://catalog.example.com/api/catalog' AS partner
  (TYPE iceberg_rest, WAREHOUSE 'analytics', SECRET partner_tok);

SELECT * FROM partner.sales.orders LIMIT 10;

DETACH partner;
DROP SECRET partner_tok;
```

The same surface works for every backend. `TYPE iceberg_rest` for Polaris. `TYPE glue` for AWS Glue (with `SECRET aws_*` or the standard credential chain). `TYPE s3tables` for AWS S3 Tables. `TYPE hms` for Hive Metastore. `TYPE jdbc` for JDBC. `TYPE sqlite` for local prototyping without standing up Polaris.

`SHOW SECRETS` returns a name + type table with no values. `SHOW CATALOGS` includes the static TOML catalogs and the runtime ATTACH names, in one list.

## The parser extension

We did not fork sqlparser-rs to add the `ATTACH` keyword. sqlparser already knows about ATTACH for SQLite. We post-process the parsed AST: if the statement matches the SQLite shape but the option list mentions `TYPE`, we rewrite it into an `AttachStatement` of our own. Same approach we use for `MASKED WITH` and `ROWS WHERE` in the policy DDL.

The standard parser does the heavy lifting. We only lift the sentences it does not understand.

```rust
pub struct AttachStatement {
    pub name: String,
    pub location: String,
    pub kind: CatalogKind,
    pub options: BTreeMap<String, OptionValue>,
}

pub enum CatalogKind {
    IcebergRest, Glue, S3Tables, Hms, Jdbc, Sqlite, Hadoop,
}
```

`CREATE SECRET` and `DROP SECRET` are not in sqlparser at all. For those we hand-roll a tiny parser on the raw token stream: `CREATE SECRET <ident> (TYPE <kind>, KEY = '<value>', ...)`. About 90 lines including all three secret kinds (`bearer`, `basic`, `aws`).

## Secrets need their own home

ATTACH on its own is not enough. Real catalogs need credentials. Hardcoding a bearer into the SQL is a non-starter; it ends up in query history, in dbt logs, in screenshots someone pastes into Slack.

The store is small.

```rust
pub enum Secret {
    Bearer { token: String },
    Basic  { username: String, password: String },
    Aws    {
        access_key: Option<String>,
        secret_key: Option<String>,
        session_token: Option<String>,
        region: Option<String>,
        profile: Option<String>,
    },
}

impl Drop for Secret {
    fn drop(&mut self) {
        // zeroize::zeroize() on every byte
    }
}
```

The `Drop` impl matters. Process dumps land in places. A coordinator that segfaults under memory pressure writes a core dump. A core dump that contains live bearer tokens is an incident waiting to be reported. Zeroizing on drop makes the leak window the lifetime of the secret, not the lifetime of the process.

The store also enforces a guard: `DROP SECRET` fails if any attached catalog still references it. The error names both the secret and the catalog so the operator knows which DETACH to issue first.

```
DROP SECRET partner_tok;
ERROR: secret 'partner_tok' is referenced by attached catalogs: partner.
       DETACH the catalog first.
```

The alternative (silently invalidating attached catalogs when their secret disappears) is the kind of behaviour that surfaces in production and not in tests. We picked the loud failure.

## The lifecycle bug

This is where it gets interesting. The code passed unit tests. The handler did the right thing. The end-to-end test against a wiremock REST endpoint failed with a confusing error.

```
Error: Failed to build session context: Catalog error:
  Failed to list namespaces: error sending request for url
  (http://localhost:59998/v1/config?warehouse=)
```

The test's wiremock server was on port 59999. The error said port 59998. That was the primary catalog from `[catalog]` in the test config. The ATTACH was hitting the wrong endpoint.

Reading the dispatch made the cause obvious:

```rust
StatementKind::Attach(stmt) => {
    let (ctx, _) = self.create_session_context(session).await?;
    self.handle_attach(stmt, &ctx).await
}
```

`create_session_context` connects to the primary catalog. That is what session contexts are for. Calling it inside `handle_attach` was a leftover from the first draft. I needed a `SessionContext` to register the new catalog into, and the easy way to get one was to ask the function that hands them out.

The leftover had two consequences. The first was visible: ATTACH could not run when the primary catalog was unreachable. That made testing the REST path impossible without standing up a fake primary too.

The second was worse, and we only saw it because we were writing tests for the second statement after the ATTACH. The catalog was registered onto a `SessionContext` that the handler returned out of. The next query built a fresh `SessionContext` from the same cache key, hit the same connection to the primary, and registered the same primary catalogs. Nothing in that path consulted the registry.

The attached catalog vanished between the ATTACH statement and the next SELECT against it.

We had a SQL primitive that worked exactly once, on exactly the connection that ran it.

## The fix: registry as source of truth

Make the registry the single owner of attached state. Make the `SessionContext` a derived view that the cache rebuilds when the registry changes.

The registry stores the built `Arc<dyn CatalogProvider>` alongside the raw `Arc<dyn iceberg::Catalog>`. ATTACH builds the catalog, wraps it in our `WritableIcebergCatalog`, and stashes both in the map. No `SessionContext` involved.

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

`create_session_context` then takes a reference to the registry. After registering the primary catalogs from TOML, it iterates `registry.providers()` and registers every attached one too:

```rust
for (name, provider) in &attached_providers {
    ctx.register_catalog(name.clone(), Arc::clone(provider));
}
```

The handler invalidates the session-context cache after every ATTACH and DETACH. The next query builds a fresh context that includes the new catalog set. The catalog survives the handler's return because the registry holds the `Arc` independently of any `SessionContext`.

This is the same pattern we use for the policy store. The plan rewriter does not store policies; it consults the policy store on every plan. The session builder does not store catalogs; it consults the registry on every build. The state lives in one place. The consumers are stateless.

## Testing without a real REST endpoint

Standing up Polaris in CI for seven test cases is overkill. We used `wiremock`.

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

Two endpoints. Two responses. The iceberg-rust REST client calls `/v1/config` to read overrides and `/v1/namespaces` to enumerate the catalog. Both are happy with empty payloads in the success-path test.

A second fixture variant guards both endpoints with `header_exists("Authorization")` and adds a catch-all that returns 401 to any unauthenticated GET. That fixture proves the bearer token from `CREATE SECRET ... TYPE bearer` actually reaches the wire. If the dispatch ever stops forwarding the secret, the test fails with the right error.

The seven cases run in 7 seconds against the in-process wiremock server. ATTACH success. Duplicate name. Attach-detach-reattach. DETACH unknown. ATTACH with bearer SECRET. DROP SECRET in-use guard. DROP SECRET after DETACH.

## The DataFusion downcast that did not work

DETACH had a separate problem worth flagging if you build on DataFusion. Version 53.1 has `register_catalog` on the `CatalogProviderList` trait. It does not have `deregister_catalog`. The original code worked around this by downcasting to the default `MemoryCatalogProviderList` and removing the entry from its inner `DashMap` directly.

```rust
let list = state.catalog_list();
if let Some(memlist) = list.as_any().downcast_ref::<MemoryCatalogProviderList>() {
    memlist.catalogs.remove(name);
}
```

This worked in the unit tests. It failed in embedded mode. The embedded CLI calls `enable_url_table()` to make `SELECT * FROM 'file.parquet'` work, and `enable_url_table()` wraps the catalog list in DataFusion's `DynamicFileCatalog`. The downcast to `MemoryCatalogProviderList` fails. The catalog stayed registered in DataFusion even though our registry had forgotten it.

The fix made the downcast unnecessary. With registry-as-source-of-truth, DETACH only removes from the registry. The next session-context rebuild does not register the detached catalog because it is no longer in `providers()`. DataFusion's view follows the registry, one cache invalidation behind.

We deleted the downcast and the warning that fired when it failed. Less code. Better behaviour. The embedded path now works the same as the cluster path.

## Persistence and the on-call test

The runtime registry is process-local. Restart wipes it. There is no on-disk persistence in v1.

This is intentional. Persistent ATTACH (where catalogs survive a restart) is a feature operators ask for. Most do not actually want it once they think it through.

Picture the scenario. A catalog attached at 9 AM on Monday is in the system at 3 AM on Sunday because someone forgot to DETACH it. The credentials behind it have rotated. The query against it returns 401. The on-call engineer wakes up to a query failure for a catalog they did not know existed. They do not know who attached it. They do not know what it points to. They do not know whether deleting it breaks something.

Static TOML catalogs are the right shape for "this is part of the deployment." They are reviewed, version-controlled, and tied to deployment lifecycle. ATTACH is the right shape for "this is part of this session." It evaporates when the process exits.

Both can coexist. A coordinator with `[catalogs.polaris]` in its TOML and an analyst running `ATTACH '...' AS partner` sees both in `SHOW CATALOGS`. The dispatch is uniform.

## What we shipped

Five MRs over a week. Phase A through Phase G:

- **Phase A** (sqe-sql): parser AST + post-parse rewrite.
- **Phase B** (sqe-core): `SecretStore` with zeroize.
- **Phase C** (sqe-catalog): `mount::build_catalog` dispatch + AWS credential layering.
- **Phase D** (sqe-coordinator): `RuntimeCatalogRegistry`.
- **Phase E** (sqe-coordinator): coordinator handlers wired into `QueryHandler`.
- **Phase F** (sqe-cli): embedded mode wiring.
- **Phase G** (sqe-coordinator): wiremock REST integration tests + the lifecycle fix.

Then Phase H: the docs you are reading now. Operator reference at `docs/book/src/operations/catalogs.md`. Narrative chapter for the ebook at `docs/ebook/chapters/06c-attaching-at-runtime.md`. This blog post.

## The pattern that is starting to repeat

Two state stores in SQE so far. The policy store from chapter 9 holds row filters and column masks. The runtime catalog registry from this work holds attached catalogs. Both are `Arc<RwLock<HashMap<...>>>`. Both are consulted on every relevant cache miss. Both are the source of truth for one slice of the engine's behaviour.

The shape repeats because the problem repeats. Whenever the engine has state that mutates between queries, we want a cheap-to-clone handle, a single owner, and stateless consumers. DataFusion's `SessionContext` is the wrong place for that state. `SessionContext` is per-query and per-user. The state belongs in the coordinator. The session is just one of its readers.

The next state store on the queue is a runtime grant cache for OPA-mediated `GRANT` and `REVOKE`. Same pattern: registry as source of truth, session as derived view, cache invalidation on writes.

The engine is starting to look like a database, not a query parser.

That is the right direction.

## Trying it

```bash
sqe-cli --embedded
sqe> ATTACH '/tmp/sqe-dev' AS local (TYPE sqlite);
sqe> CREATE SCHEMA local.tutorial;
sqe> CREATE TABLE local.tutorial.events (id BIGINT, ts TIMESTAMP);
sqe> INSERT INTO local.tutorial.events VALUES (1, NOW());
sqe> SELECT * FROM local.tutorial.events;
```

That is the whole loop. Reference: [`docs/book/src/operations/catalogs.md`](https://github.com/sbp/sqe/blob/main/docs/book/src/operations/catalogs.md).
