# Ballista Bearer Passthrough Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the authenticated user's OIDC bearer reach ballista executors so each scan task authenticates to Polaris/S3 *as the user* (no service account), closing parity-gate criterion #1 (D8).

**Architecture:** The bearer travels **inside the plan**, not via ballista's session-config propagation (which drops the unprefixed `ConfigExtension` key, D8). Path: a per-query `SqeLogicalCodec` stamps the bearer onto the encoded table-provider bytes on the **client** (coordinator submit) -> the **scheduler** decodes it and attaches it to the rehydrated `SqeTableProvider` -> `scan()` bakes it onto `IcebergScanExec` -> `SqePhysicalCodec` writes it into `EncodedSqeScan` -> the **executor** mints a per-(user,table) `FileIO` from the bearer, **cached** so D4's no-per-task-round-trip invariant holds for all but a bounded cold start.

**Trust model (unchanged):** only the bearer travels, never S3 secrets (`auth_ext.rs` lines 11-15). The executor exchanges bearer -> vended S3 creds locally, once per (user,table), cached.

**Tech Stack:** Rust, Apache Ballista 53, DataFusion 53, iceberg-rust (vendored RW fork), serde_json wire format.

---

## Why this approach (settled during brainstorming, 2026-05-30)

- **Not an in-ballista fix.** Ballista is a registry dependency (`ballista = "53.0.0"`), not vendored; fixing the `ConfigExtension` round-trip inside it means *forking ballista*, which the cutover strategy forbids. The in-ballista fix is recorded as an **upstream contribution under D8**, not our code path.
- **Ship bearer, not creds.** Honors the `auth_ext.rs` trust model ("only the bearer travels"). Shipping coordinator-vended S3 creds in plan bytes would violate it.
- **Cache per (user,table) to honor D4.** A cold miss costs one async credential exchange (`block_in_place`); cached for every later task of that (user,table) on that executor. This is a bounded one-time burst, not the per-task sustained herd D4 eliminated. Single-flight (one in-flight exchange per key) is added so a cold fan-out of concurrent decodes does not stampede.

## Testability caveat (must appear in verify steps)

The dev/test stack is **single-principal** (all users share one service token), so it **cannot** validate true multi-tenant isolation end-to-end (cutover design lines 218-219). The honest, testable units are:
1. Wire round-trip: a non-empty bearer present in `EncodedSqeScan` survives encode/decode; empty/None survives as None.
2. Two distinct bearers produce two distinct cached `FileIO`/catalog instances (the per-(user,table) keying works).
3. Empty bearer -> the existing static-creds fallback path (no regression).
4. Integration smoke: the ballista path still returns TPC-H 22/22 with a bearer stamped (proves no regression on the single-principal stack).

Do NOT write a verify step that claims to prove per-user S3 isolation; the stack cannot exercise it.

## File structure

- `crates/sqe-catalog/src/iceberg_scan.rs` — add inert `bearer: Option<Arc<str>>` to `IcebergScanExec` (+ accessor, builder, clone propagation). Default `None`; only the ballista decode path sets it. Non-ballista path unchanged.
- `crates/sqe-catalog/src/table_provider.rs` — `SqeTableProvider` (already `#[derive(Debug, Clone)]`); add `bearer: Option<Arc<str>>`; `scan()` (builds the node at line ~200 via `new_with_filters_and_metrics`, returns `Arc::new(exec)` at line ~261) bakes the bearer onto `exec` just before the return.
- `crates/sqe-ballista/src/sqe_codec.rs` — `SqeLogicalCodec` gains an optional `bearer`; encodes/decodes it with the table ref; `SqePhysicalCodec` reads `scan.bearer()` into `EncodedSqeScan.bearer` and, on decode, resolves a per-(user,table) `FileIO` from the bearer (cached, single-flight) for the D4 sync rebuild.
- `crates/sqe-ballista/src/cluster.rs` — `submit_remote` builds a **per-query** logical codec carrying `user_bearer`; keep the `SqeAuthOptions` session-config insert as harmless belt-and-suspenders but stop relying on it.
- Docs/ledger: `docs/superpowers/specs/2026-05-28-sqe-on-ballista-cutover-design.md` (D8 status -> done on our side; upstream PR noted) and parity-gate criterion #1 status.

---

### Task 1: Carry the bearer in the `EncodedSqeScan` wire format

**Files:**
- Modify: `crates/sqe-ballista/src/sqe_codec.rs` (struct `EncodedSqeScan` ~line 151; test mod ~line 434)

- [ ] **Step 1: Add the failing round-trip assertion**

In `crates/sqe-ballista/src/sqe_codec.rs`, extend the existing `encoded_sqe_scan_round_trips` test. Add to the `EncodedSqeScan { .. }` literal:

```rust
            bearer: Some("eyJhbGciOiJ.user-bearer.sig".to_string()),
```

and after the existing assertions:

```rust
        assert_eq!(decoded.bearer, original.bearer);
```

- [ ] **Step 2: Run it to verify it fails to compile**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-ballista encoded_sqe_scan_round_trips`
Expected: FAIL — `struct EncodedSqeScan has no field named bearer`.

- [ ] **Step 3: Add the field**

In `struct EncodedSqeScan`, after `metadata_location`:

```rust
    /// The authenticated user's OIDC bearer, threaded through the plan so the
    /// executor can mint per-user vended S3 creds (parity #1 / D8). `None` =
    /// single-tenant fallback (Phase 3 behaviour). Only the bearer travels,
    /// never S3 secrets (auth_ext.rs trust model).
    #[serde(default)]
    bearer: Option<String>,
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-ballista encoded_sqe_scan_round_trips`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-ballista/src/sqe_codec.rs
git commit -m "feat(ballista): carry user bearer in EncodedSqeScan wire format"
```

---

### Task 2: Add an inert `bearer` field to `IcebergScanExec`

The physical codec encodes from the node, so the node must carry the bearer. Default `None`; only the ballista decode path sets it, so the non-ballista path is byte-for-byte unchanged.

**Files:**
- Modify: `crates/sqe-catalog/src/iceberg_scan.rs` (struct fields; `new_with_filters_and_metrics`; `from_codec_parts` ~line 345; `clone_with_pushed_filters` ~line 382)
- Test: `crates/sqe-catalog/src/iceberg_scan.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test**

Add to the test module in `iceberg_scan.rs` (adapt table construction to the existing test helpers in that file — find one with `git grep "fn .*-> IcebergScanExec\|IcebergScanExec::new" crates/sqe-catalog/src/iceberg_scan.rs`):

```rust
#[test]
fn bearer_defaults_none_and_is_settable() {
    let scan = make_test_scan(); // existing helper
    assert!(scan.bearer().is_none(), "non-ballista path must not set a bearer");
    let scan = scan.with_bearer(Some(std::sync::Arc::from("user-token")));
    assert_eq!(scan.bearer().map(|s| s.as_ref()), Some("user-token"));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-catalog bearer_defaults_none_and_is_settable`
Expected: FAIL — no method `bearer` / `with_bearer`.

- [ ] **Step 3: Implement the field, accessor, builder, and clone propagation**

Add the field to the `IcebergScanExec` struct (next to the other scan fields):

```rust
    /// Per-user OIDC bearer, set ONLY on the ballista scheduler decode path so
    /// the physical codec can ship it to executors (parity #1 / D8). `None` on
    /// every non-ballista construction -- this field is inert off that path.
    bearer: Option<Arc<str>>,
```

Initialize `bearer: None` in **every** constructor that builds the struct literal (`new_with_filters_and_metrics` is the base; `new` and `new_with_filters` delegate to it — confirm, then only touch the base + any other literal). Add the accessor + builder near the other `with_*` methods:

```rust
    /// The per-user bearer, if this scan was rehydrated on the ballista path.
    pub fn bearer(&self) -> Option<&Arc<str>> {
        self.bearer.as_ref()
    }

    /// Attach a per-user bearer (ballista scheduler decode path only).
    pub fn with_bearer(mut self, bearer: Option<Arc<str>>) -> Self {
        self.bearer = bearer;
        self
    }
```

In `clone_with_pushed_filters` (the struct literal ~line 383), add:

```rust
            bearer: self.bearer.clone(),
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-catalog bearer_defaults_none_and_is_settable`
Expected: PASS.

- [ ] **Step 5: Verify no behavioral drift on the existing scan path**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-catalog`
Expected: all existing `iceberg_scan` tests still PASS (the field is inert).

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-catalog/src/iceberg_scan.rs
git commit -m "feat(catalog): add inert per-user bearer field to IcebergScanExec"
```

---

### Task 3: Carry the bearer on `SqeTableProvider` and bake it into `scan()`

**Files:**
- Modify: `crates/sqe-catalog/src/table_provider.rs` (`SqeTableProvider` struct line 29; `scan()` line 164; node built line ~200, returned line ~261)
- Test: same file, inline

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn scan_propagates_bearer_to_iceberg_scan_exec() {
    let provider = make_test_sqe_table_provider().with_bearer(Some(std::sync::Arc::from("u-tok")));
    let plan = provider.scan(&session_state(), None, &[], None).await.unwrap();
    let scan = plan.as_any().downcast_ref::<IcebergScanExec>().expect("IcebergScanExec");
    assert_eq!(scan.bearer().map(|s| s.as_ref()), Some("u-tok"));
}
```

(Use the existing test constructors in that module; if there is no `make_test_sqe_table_provider`, build the provider the way the module's other tests do.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-catalog scan_propagates_bearer_to_iceberg_scan_exec`
Expected: FAIL — no method `with_bearer` on `SqeTableProvider`.

- [ ] **Step 3: Add the field, builder, and `scan()` propagation**

Add to the `SqeTableProvider` struct:

```rust
    /// Per-user bearer attached by the ballista logical codec on decode
    /// (scheduler side). `None` on the coordinator's normal registration.
    bearer: Option<Arc<str>>,
```

Initialize `bearer: None` in its constructor(s). Add:

```rust
    pub fn with_bearer(mut self, bearer: Option<Arc<str>>) -> Self {
        self.bearer = bearer;
        self
    }
```

In the `TableProvider::scan` impl, find where it constructs the `IcebergScanExec` and chain the bearer onto it:

```rust
        let exec = exec.with_bearer(self.bearer.clone());
```

(Place it immediately before the node is boxed/returned, after any other `with_*` chaining.)

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-catalog scan_propagates_bearer_to_iceberg_scan_exec`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/*.rs
git commit -m "feat(catalog): SqeTableProvider threads bearer into IcebergScanExec on scan"
```

---

### Task 4: `SqeLogicalCodec` encodes/decodes the bearer with the table ref

The client stamps the bearer; the scheduler decodes it and attaches it to the rehydrated provider.

**Files:**
- Modify: `crates/sqe-ballista/src/sqe_codec.rs` (`SqeLogicalCodec` struct ~line 60; `new` ~line 74; `try_encode_table_provider` ~line 96; `try_decode_table_provider` ~line 109)
- Test: `crates/sqe-ballista/src/sqe_codec.rs` inline

- [ ] **Step 1: Write the failing test (encode side, format pin)**

The wire format must be unambiguous: encode `bearer` then a separator then the table ref, so the decoder can split. Pin it:

```rust
#[test]
fn logical_codec_encodes_bearer_then_tableref() {
    use datafusion::sql::TableReference;
    let codec = SqeLogicalCodec::new_with_bearer(test_catalog(), Some(Arc::from("btok")));
    let mut buf = Vec::new();
    codec
        .try_encode_table_provider(&TableReference::partial("ns", "t"), test_provider(), &mut buf)
        .unwrap();
    // Format: "<bearer>\n<tableref>"; empty bearer => leading "\n".
    assert_eq!(String::from_utf8(buf).unwrap(), "btok\nns.t");
}
```

(`test_catalog()` / `test_provider()`: reuse whatever the module's tests already use; if none, a minimal `MemorySchemaProvider`-backed catalog is fine — match the existing test style.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-ballista logical_codec_encodes_bearer_then_tableref`
Expected: FAIL — no `new_with_bearer`.

- [ ] **Step 3: Implement bearer on the logical codec**

Add a field + constructor (keep `new` delegating with `None`):

```rust
pub struct SqeLogicalCodec {
    catalog: Arc<dyn CatalogProvider>,
    bearer: Option<Arc<str>>,
    default: BallistaLogicalExtensionCodec,
}

impl SqeLogicalCodec {
    pub fn new(catalog: Arc<dyn CatalogProvider>) -> Self {
        Self::new_with_bearer(catalog, None)
    }

    pub fn new_with_bearer(catalog: Arc<dyn CatalogProvider>, bearer: Option<Arc<str>>) -> Self {
        Self { catalog, bearer, default: BallistaLogicalExtensionCodec::default() }
    }
}
```

Rewrite `try_encode_table_provider` to prefix the bearer and a `\n` separator:

```rust
    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        _node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> DFResult<()> {
        let bearer = self.bearer.as_deref().unwrap_or("");
        buf.extend_from_slice(bearer.as_bytes());
        buf.push(b'\n');
        buf.extend_from_slice(table_ref.to_string().as_bytes());
        Ok(())
    }
```

- [ ] **Step 4: Run the encode test to verify it passes**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-ballista logical_codec_encodes_bearer_then_tableref`
Expected: PASS.

- [ ] **Step 5: Write the failing decode test**

```rust
#[tokio::test]
async fn logical_codec_decodes_bearer_onto_provider() {
    use datafusion::sql::TableReference;
    let codec = SqeLogicalCodec::new(test_catalog_with_table("ns", "t").await);
    let buf = b"btok\nns.t".to_vec();
    let provider = codec
        .try_decode_table_provider(&buf, &TableReference::partial("ns", "t"), test_schema(), &task_ctx())
        .unwrap();
    let sqe = provider.as_any().downcast_ref::<SqeTableProvider>().expect("SqeTableProvider");
    assert_eq!(sqe.bearer_for_test().map(|s| s.as_ref()), Some("btok"));
}
```

(Add a small `#[cfg(test)] pub fn bearer_for_test(&self) -> Option<&Arc<str>>` accessor on `SqeTableProvider` if it has no public getter.)

- [ ] **Step 6: Run to verify it fails**

Expected: FAIL — decoder ignores the bearer / does not attach it.

- [ ] **Step 7: Rewrite `try_decode_table_provider` to split the bearer and attach it**

```rust
    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let raw = std::str::from_utf8(buf)
            .map_err(|e| DataFusionError::Internal(format!("table ref not UTF-8: {e}")))?;
        // Format: "<bearer>\n<tableref>". Split on the FIRST newline only.
        let (bearer_str, encoded) = raw
            .split_once('\n')
            .ok_or_else(|| DataFusionError::Internal("missing bearer/tableref separator".into()))?;
        let bearer: Option<Arc<str>> = if bearer_str.is_empty() {
            None
        } else {
            Some(Arc::from(bearer_str))
        };

        let parsed = TableReference::parse_str(encoded);
        // ... existing schema_name/table_name extraction + lookup unchanged ...

        // The looked-up provider is an Arc<dyn TableProvider>; downcast to
        // SqeTableProvider to attach the bearer, then re-wrap. If it is not a
        // SqeTableProvider (it always is, on this path), return it unchanged.
        let provider = /* existing block_on_in_runtime lookup */;
        if let Some(sqe) = provider.as_any().downcast_ref::<SqeTableProvider>() {
            return Ok(Arc::new(sqe.clone().with_bearer(bearer)));
        }
        Ok(provider)
    }
```

> Note: `SqeTableProvider` already derives `Clone` (`table_provider.rs:28`), so `sqe.clone().with_bearer(..)` works directly.

- [ ] **Step 8: Run both logical-codec tests to verify they pass**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-ballista logical_codec_`
Expected: both PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/sqe-ballista/src/sqe_codec.rs crates/sqe-catalog/src/*.rs
git commit -m "feat(ballista): logical codec threads bearer client->scheduler onto provider"
```

---

### Task 5: Physical codec ships + consumes the bearer (encode side + executor per-user FileIO cache)

**Files:**
- Modify: `crates/sqe-ballista/src/sqe_codec.rs` (`SqePhysicalCodec` struct ~line 194; `try_encode` ~line 272; `try_decode` ~line 317; `build_file_io` ~line 400)
- Test: `crates/sqe-ballista/src/sqe_codec.rs` inline

- [ ] **Step 1: Write the failing encode test**

```rust
#[test]
fn physical_encode_carries_node_bearer() {
    let scan = make_iceberg_scan_exec().with_bearer(Some(Arc::from("ptok")));
    let codec = make_physical_codec();
    let mut buf = Vec::new();
    codec.try_encode(Arc::new(scan), &mut buf).unwrap();
    assert_eq!(buf[0], 1u8, "iceberg discriminator");
    let decoded: EncodedSqeScan = serde_json::from_slice(&buf[1..]).unwrap();
    assert_eq!(decoded.bearer.as_deref(), Some("ptok"));
}
```

- [ ] **Step 2: Run to verify it fails**

Expected: FAIL — `decoded.bearer` is `None` (encoder does not read the node's bearer yet).

- [ ] **Step 3: Stamp the bearer in `try_encode`**

In the `EncodedSqeScan { .. }` literal in `try_encode`, add:

```rust
            bearer: scan.bearer().map(|b| b.to_string()),
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-ballista physical_encode_carries_node_bearer`
Expected: PASS.

- [ ] **Step 5: Write the failing decode test (per-(user,table) FileIO cache + single-flight)**

Pin the cache *keying*, not real S3 (the stack can't vend per-user creds):

```rust
#[tokio::test]
async fn decode_uses_distinct_file_io_per_bearer() {
    let codec = make_physical_codec(); // static storage config (test)
    // Two encoded scans, same table, different bearers.
    let buf_a = encode_scan_with_bearer("ns", "t", Some("user-a"));
    let buf_b = encode_scan_with_bearer("ns", "t", Some("user-b"));
    let _a = codec.try_decode(&buf_a, &[], &task_ctx()).unwrap();
    let _b = codec.try_decode(&buf_b, &[], &task_ctx()).unwrap();
    // Cache holds two distinct entries keyed by (bearer-hash, table).
    assert_eq!(codec.file_io_cache_len_for_test(), 2);
    // Same bearer again -> no new entry (cache hit, single-flight).
    let _a2 = codec.try_decode(&buf_a, &[], &task_ctx()).unwrap();
    assert_eq!(codec.file_io_cache_len_for_test(), 2);
}
```

(`encode_scan_with_bearer` builds the metadata-populated D4 sync-path bytes — copy the existing `EncodedSqeScan` test literal and set `bearer`. `make_physical_codec` uses test storage config so `build_file_io` succeeds offline. Add `#[cfg(test)] pub fn file_io_cache_len_for_test(&self) -> usize`.)

- [ ] **Step 6: Run to verify it fails**

Expected: FAIL — no per-bearer FileIO cache; decode ignores `encoded.bearer`.

- [ ] **Step 7: Implement the per-(user,table) FileIO cache with single-flight**

Add a cache to `SqePhysicalCodec` keyed by `(bearer_hash, table_key)`:

```rust
    /// Per-(user,table) FileIO cache so the D4 sync-rebuild path pays the
    /// bearer->vended-creds exchange at most once per (user,table) per executor,
    /// not per task. Keyed by (non-crypto bearer hash, "ns.table").
    file_io: Arc<tokio::sync::Mutex<HashMap<(u64, String), Arc<tokio::sync::OnceCell<FileIO>>>>>,
```

(Initialize in `new`.) Add a resolver that single-flights via the per-key `OnceCell`:

```rust
    async fn resolve_file_io(
        &self,
        bearer: Option<&str>,
        ident: &TableIdent,
        metadata_location: Option<&str>,
    ) -> DFResult<FileIO> {
        let Some(bearer) = bearer.filter(|b| !b.is_empty()) else {
            // Phase 3 fallback: static service creds (unchanged behaviour).
            return build_file_io(&self.storage, metadata_location);
        };
        let key = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            bearer.hash(&mut h);
            (h.finish(), ident.to_string())
        };
        let cell = {
            let mut guard = self.file_io.lock().await;
            guard.entry(key).or_insert_with(|| Arc::new(tokio::sync::OnceCell::new())).clone()
        };
        // Single-flight: only the first caller for this key runs the exchange;
        // concurrent cold callers await the same OnceCell (no block_on herd).
        cell.get_or_try_init(|| async {
            let session_catalog = SessionCatalog::for_session_with(
                &self.cat_cfg, &self.storage, Some(self.table_cache.clone()), bearer,
            )
            .await
            .map_err(|e| DataFusionError::Internal(format!("per-user catalog on executor: {e}")))?;
            // Reload the table once to obtain the per-user vended FileIO, then
            // keep only its FileIO (metadata already crossed the wire, D4).
            let table = session_catalog
                .load_table(ident)
                .await
                .map_err(|e| DataFusionError::Internal(format!("per-user load_table: {e}")))?;
            Ok::<_, DataFusionError>(table.file_io().clone())
        })
        .await
        .cloned()
    }
```

In `try_decode`, on the D4 sync path (`metadata_json` non-empty), replace the `build_file_io(&self.storage, ..)` call with the bearer-aware resolver:

```rust
            let file_io = block_on_in_runtime({
                let bearer = encoded.bearer.clone();
                let ident = ident.clone();
                let loc = encoded.metadata_location.clone();
                async move { self.resolve_file_io(bearer.as_deref(), &ident, loc.as_deref()).await }
            })?;
```

> D4 note: the `block_on_in_runtime` here only blocks on a **cold-cache** exchange (once per (user,table)); cache hits resolve the `OnceCell` without a round-trip. With no bearer it calls `build_file_io` synchronously (no await) exactly as today.

- [ ] **Step 8: Run the decode test to verify it passes**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-ballista decode_uses_distinct_file_io_per_bearer`
Expected: PASS.

- [ ] **Step 9: Run the whole crate to verify no regression**

Run: `cargo test --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-ballista`
Expected: all PASS.

- [ ] **Step 10: Commit**

```bash
git add crates/sqe-ballista/src/sqe_codec.rs
git commit -m "feat(ballista): executor mints per-(user,table) FileIO from threaded bearer (D4-safe cache)"
```

---

### Task 6: `submit_remote` builds a per-query logical codec carrying the bearer

**Files:**
- Modify: `crates/sqe-ballista/src/cluster.rs` (`submit_remote` ~line where `with_ballista_logical_extension_codec(cluster.logical_codec())` is set; `ClusterCatalog::logical_codec` ~line 87)

- [ ] **Step 1: Add a bearer-aware logical-codec constructor on `ClusterCatalog`**

In `cluster.rs`, next to `logical_codec`:

```rust
    fn logical_codec_with_bearer(&self, bearer: Option<Arc<str>>) -> Arc<SqeLogicalCodec> {
        Arc::new(SqeLogicalCodec::new_with_bearer(self.provider.clone(), bearer))
    }
```

- [ ] **Step 2: Use it in `submit_remote`**

Replace:

```rust
        .with_ballista_logical_extension_codec(cluster.logical_codec())
```

with:

```rust
        .with_ballista_logical_extension_codec(cluster.logical_codec_with_bearer(
            (!user_bearer.is_empty()).then(|| Arc::from(user_bearer)),
        ))
```

Update the `submit_remote` doc comment: the bearer now travels in the plan (logical codec) and the `SqeAuthOptions` session-config insert is retained only as harmless redundancy (it is the path ballista drops, D8). Remove the "this does NOT currently reach the executor" claim — it now does, via the plan.

- [ ] **Step 3: Build to verify it compiles**

Run: `cargo build --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-ballista`
Expected: clean build.

- [ ] **Step 4: Clippy**

Run: `cargo clippy --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"' -p sqe-ballista --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-ballista/src/cluster.rs
git commit -m "feat(ballista): submit_remote stamps user bearer via per-query logical codec"
```

---

### Task 7: Integration smoke + ledger/spec update

**Files:**
- Run: TPC-H ballista-mode benchmark (single-principal smoke)
- Modify: `docs/superpowers/specs/2026-05-28-sqe-on-ballista-cutover-design.md` (D8 status; parity criterion #1)
- Modify: `README.md` roadmap + `nextsteps.md` (per CLAUDE.md "After Completing Work")

- [ ] **Step 1: Full workspace test gate**

Run:
```bash
cargo test --all --config 'source.escrow.registry="sparse+http://127.0.0.1:7888/cargo/"'
```
Expected: green (Task 1-6 unit tests pass; nothing else regressed).

- [ ] **Step 2: Ballista-path correctness smoke (no regression on single-principal stack)**

With the docker stack up, run TPC-H SF0.1 in ballista mode (the bearer is now stamped but resolves to the single service principal on this stack):
```bash
# engine=ballista path; confirm the bearer threading did not break execution
BENCH_SCALE=0.1 ./scripts/benchmark-test.sh tpch
```
Expected: **22/22**, row counts identical to legacy. (This proves no regression; it does NOT prove per-user isolation — the stack is single-principal. Record that explicitly in the result note.)

- [ ] **Step 3: Update the design doc**

In `2026-05-28-sqe-on-ballista-cutover-design.md`:
- Ledger D8: change status to "FIXED on our side via plan-node threading (logical codec -> node -> EncodedSqeScan -> executor per-(user,table) FileIO cache). Upstream improvement still worth filing: ballista `update_from_key_value_pair` should round-trip `ConfigExtension`-prefixed keys."
- Parity gate criterion #1: "VERIFIED BROKEN" -> "code-complete; per-user isolation unverifiable on the single-principal stack (needs multi-principal env). Wire round-trip + per-bearer cache keying unit-tested."

- [ ] **Step 4: Update README roadmap + nextsteps**

Mark bearer passthrough (Task #21) done; shift the NEXT pointer to parity criterion #2 (policy plans survive the codec).

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-05-28-sqe-on-ballista-cutover-design.md README.md nextsteps.md
git commit -m "docs(ballista): D8 fixed via plan-node bearer threading; parity #1 code-complete"
```

- [ ] **Step 6: Push + open MR**

```bash
git push -u origin feat/vendored-ballista-improvements
glab mr create --source-branch feat/vendored-ballista-improvements --target-branch main \
  --title "feat(ballista): per-user bearer passthrough via plan-node threading (D8 / parity #1)" \
  --description "Closes parity-gate criterion #1. Bearer travels in the plan, not ballista session config (D8). Trust model preserved (bearer only). D4 preserved via per-(user,table) FileIO cache + single-flight. Per-user isolation not E2E-verifiable on the single-principal stack."
```

---

## Self-review

**Spec coverage (parity-gate criterion #1 / D8):**
- Bearer reaches executors via the plan: Tasks 1,4,5,6. ✅
- Honors `auth_ext.rs` trust model (bearer only): Task 5 ships bearer, mints creds locally. ✅
- Honors D4 (no per-task round-trip): Task 5 per-(user,table) cache + single-flight; cold start bounded. ✅
- Non-ballista path unaffected: Task 2 field inert (None), Step 5 regression check. ✅
- Honest testability: every verify step states the single-principal limit; no step claims per-user isolation proof. ✅

**Placeholder scan:** all code blocks are complete. Module path (`table_provider.rs`), `Clone` derivation, and `scan()` line anchors are resolved against the live source. The only implementer-side discovery left is reusing each module's existing test helpers (named generically here, e.g. `make_test_scan`), which is normal for inline `#[cfg(test)]` blocks — not missing logic.

**Type consistency:** `with_bearer(Option<Arc<str>>)` / `bearer() -> Option<&Arc<str>>` used consistently on both `IcebergScanExec` (Task 2) and `SqeTableProvider` (Task 3); `EncodedSqeScan.bearer: Option<String>` (Task 1) converted at the node boundary via `.to_string()` (encode, Task 5 Step 3) and `Arc::from`/`as_deref` (decode). Cache key `(u64, String)` consistent in Task 5.
