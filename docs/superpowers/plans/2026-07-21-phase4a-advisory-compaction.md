# Phase 4a: Advisory Auto-Compaction + Service Principal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or subagent-driven-development) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Ship the read-only foundation of autonomous compaction: an opt-in maintenance service principal, a `[maintenance]` config section, a `CALL system.table_health` procedure, and an advisory scheduler loop that reports per-table compaction debt without mutating any table.

**Architecture:** Wire the already-written `OidcM2mProvider` (crates/sqe-auth/src/oidc_m2m.rs) behind a dedicated `[maintenance.principal]` config block owned by the maintenance subsystem, never added to the auth chain. A `spawn_supervised` loop discovers tables the principal can see that carry `sqe.maintenance.enabled=true`, runs the same health analysis `CALL system.table_health` exposes, and emits Prometheus gauges + audit events + optional `sqe_system.maintenance_log` rows. Nothing writes to a user table.

**Tech Stack:** Rust, DataFusion, vendored iceberg-rust, Arrow, moka, tokio, prometheus, serde/toml.

## Global Constraints (copied verbatim from the design)

- The interactive query path stays 100% user-identity. The maintenance principal provider is NEVER constructed on any auth-chain path (`factory.rs::build_auth_chain` is untouched; no `AuthProviderConfig::M2m` variant is added).
- Zero changes under `vendor/iceberg-rust`.
- Phase 4a mutates NO user table. Advisory only. The only writes are optional appends to `sqe_system.maintenance_log`, which the operator creates out-of-band; if it is absent, log writes are skipped with a warning, never an error.
- Config default is `maintenance.mode = "off"`. Advisory behavior requires `mode = "advisory"`.
- No emdash/endash/unicode arrows in any doc or comment. Use `->` in code only.
- Never push to main. Branch + MR via `glab`.

## File Structure

- `crates/sqe-core/src/config.rs` (modify): add `MaintenanceConfig` (+ `MaintenancePrincipalConfig`, `MaintenanceSchedulerConfig`, `MaintenanceCompactionConfig`, `MaintenanceDistributionConfig`) sub-structs following the `SessionConfig` idiom; add `pub maintenance: MaintenanceConfig` to `SqeConfig`; extend `validate()`.
- `crates/sqe-coordinator/src/maintenance_principal.rs` (create): `MaintenancePrincipal` newtype owning `OidcM2mProvider`, minting per-job ephemeral `Session`s. Not registered in `SessionManager`.
- `crates/sqe-coordinator/src/table_health.rs` (create): pure analysis `analyze_table_health(&IcebergTable, &MaintenanceCompactionConfig) -> TableHealth` + the Arrow `RecordBatch` shaping for the procedure.
- `crates/sqe-sql/src/procedures.rs` (modify): add `ProcedureCall::TableHealth { table }` variant + `parse_table_health`; register in the dispatch match and `name()`.
- `crates/sqe-coordinator/src/classifier.rs` (modify): route `table_health` to `StatementKind::Procedure` in the read-only classification path (no write privilege required).
- `crates/sqe-coordinator/src/maintenance.rs` (modify): add the `TableHealth` handler arm (read-only: NO `authorize_or_deny` write gate); reuse `collect_live_data_files`, `collect_live_delete_files`, `pack_file_groups_partition_aware`, `delete_heavy_files`.
- `crates/sqe-coordinator/src/maintenance_log.rs` (create): thin appender for `sqe_system.maintenance_log` rows; no-op-with-warning when the table is absent.
- `crates/sqe-coordinator/src/maintenance_scheduler.rs` (create): the advisory `spawn_supervised` loop + table discovery + per-table analysis + metrics/audit/log emission.
- `crates/sqe-coordinator/src/bin/sqe_server.rs` (modify): construct `MaintenancePrincipal` (if `mode != off` and a principal block is present) and register the scheduler `TaskGuard` into `_task_guards`.
- `crates/sqe-metrics/src/lib.rs` (modify): add the `sqe_maintenance_*` and `sqe_table_*` gauges/counters.
- `crates/sqe-coordinator/src/audit*` (modify): add `AuditKind::Maintenance`.
- `docs/site/book/src/sql-reference/procedures.md` (modify): document `table_health`; `docs/.../configuration.md`: document `[maintenance]`.

---

### Task 1: `[maintenance]` config section

**Files:**
- Modify: `crates/sqe-core/src/config.rs` (add structs near `SessionConfig` ~3025; add field to `SqeConfig` ~9-47; extend `validate` ~3400)
- Test: inline `#[cfg(test)]` in `config.rs`

**Interfaces produced:**
- `MaintenanceConfig { mode: MaintenanceMode, principal: Option<MaintenancePrincipalConfig>, scheduler: MaintenanceSchedulerConfig, compaction: MaintenanceCompactionConfig, distribution: MaintenanceDistributionConfig }`
- `enum MaintenanceMode { Off, Advisory, Active }` (serde rename lowercase; default `Off`)
- `MaintenancePrincipalConfig { token_endpoint, client_id, client_secret, scope: Option<String>, user_id, roles: Vec<String>, refresh_skew_secs: u64 }`
- `MaintenanceSchedulerConfig { enabled: bool, tick_secs: u64, schedule: String, jitter_secs: u64, max_concurrent_jobs: usize, lease: LeaseMode, lease_ttl_secs: u64, state_table: String, single_scheduler_acknowledged: bool }`
- `MaintenanceCompactionConfig { target_file_size_bytes: u64, min_input_files: usize, delete_file_threshold: usize, strategy: String }`
- `MaintenanceDistributionConfig { mode: DistributionMode, min_workers: usize, max_inflight_groups_per_worker: usize, group_attempts: usize, group_timeout_secs: u64, group_heartbeat_timeout_secs: u64 }`

- [ ] **Step 1: Write the failing test.** Add to `config.rs` tests: parse a TOML string with a full `[maintenance]` block and assert the fields; parse with NO `[maintenance]` and assert `mode == Off` and `maintenance` is present via `#[serde(default)]`; assert `validate()` errors when `mode != off` and `principal` is `None`; assert `validate()` errors on any zero interval (`tick_secs`, `lease_ttl_secs`) since they feed `tokio::time::interval`.

```rust
#[test]
fn maintenance_defaults_to_off_when_absent() {
    let cfg: SqeConfig = toml::from_str("").expect("empty config");
    assert_eq!(cfg.maintenance.mode, MaintenanceMode::Off);
    assert!(cfg.maintenance.principal.is_none());
}

#[test]
fn maintenance_active_without_principal_is_rejected() {
    let cfg: SqeConfig = toml::from_str("[maintenance]\nmode = \"advisory\"\n").unwrap();
    assert!(cfg.validate().is_err(), "advisory/active mode needs a principal block");
}

#[test]
fn maintenance_zero_tick_rejected() {
    let toml = "[maintenance]\nmode=\"off\"\n[maintenance.scheduler]\ntick_secs=0\n";
    let cfg: SqeConfig = toml::from_str(toml).unwrap();
    assert!(cfg.validate().is_err());
}
```

- [ ] **Step 2: Run to verify failure.** `cargo test -p sqe-core --lib maintenance_` -> FAIL (types missing).
- [ ] **Step 3: Implement the structs.** Follow the `SessionConfig` pattern exactly: `#[derive(Debug, Clone, Deserialize, Serialize)]`, `#[serde(default)]` on each struct, module-level `default_*` fns for every field, `Default` impls delegating to the `default_*` fns. Add `#[serde(default)] pub maintenance: MaintenanceConfig` to `SqeConfig`. Defaults per the design's TOML sketch (mode Off; tick 60; schedule "0 2 * * *"; jitter 900; lease "catalog"; lease_ttl 300; state_table "sqe_system.maintenance_log"; target 512MiB; min_input 5; delete_file_threshold 2; distribution mode "auto"; min_workers 2; group_timeout 3600; heartbeat 120).
- [ ] **Step 4: Extend `validate()`.** In `SqeConfig::validate`: if `maintenance.mode != Off` and `maintenance.principal.is_none()` -> error. If `scheduler.tick_secs == 0` or `lease_ttl_secs == 0` -> error. If `scheduler.enabled && lease == None && !single_scheduler_acknowledged` -> error. Warn (log) if `principal.client_id` equals any configured auth-provider client_id (best-effort scan of `auth`).
- [ ] **Step 5: Run tests.** `cargo test -p sqe-core --lib maintenance_` -> PASS.
- [ ] **Step 6: Commit.** `git commit -m "feat(maintenance): [maintenance] config section"`

---

### Task 2: Maintenance principal + ephemeral session minting

**Files:**
- Create: `crates/sqe-coordinator/src/maintenance_principal.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs` (add `pub mod maintenance_principal;`)
- Test: inline unit test (session isolation) + reuse for later tasks

**Interfaces consumed:** `MaintenancePrincipalConfig` (Task 1); `sqe_auth::{OidcM2mProvider, OidcM2mConfig}`; `sqe_auth::Identity`; `sqe_core::Session`; `crates/sqe-coordinator/src/session_manager.rs::identity_to_session` (mirror, do not register).

**Interfaces produced:**
- `struct MaintenancePrincipal { provider: OidcM2mProvider, user_id: String }`
- `MaintenancePrincipal::from_config(&MaintenancePrincipalConfig) -> Result<Self>`
- `async fn mint_session(&self, job_id: &str) -> Result<Session>` — authenticates via the provider, builds an ephemeral `Session` with `id = format!("maintenance-job-{job_id}")`, user = configured `user_id`, `access_token = identity.catalog_token`. NOT inserted into any `SessionManager`.
- `async fn refresh(&self, session: &Session) -> Result<()>` — pre-commit token refresh via `provider.refresh_catalog_token` + `session.rotate_credentials`.

- [ ] **Step 1: Write the failing test.** A unit test that constructs a `MaintenancePrincipal` from a config with a bogus endpoint, and asserts `from_config` succeeds (construction is lazy) but that the minted session id is prefixed `maintenance-job-` and carries the configured `user_id`. (Token fetch is not exercised offline; assert the shape, not the network call. Gate the network path behind the integration suite.)

```rust
#[test]
fn principal_from_config_builds() {
    let cfg = MaintenancePrincipalConfig {
        token_endpoint: "https://idp.example/token".into(),
        client_id: "sqe-maintenance".into(),
        client_secret: "x".into(),
        scope: None,
        user_id: "svc-sqe-maintenance".into(),
        roles: vec!["maintenance".into()],
        refresh_skew_secs: 60,
    };
    let p = MaintenancePrincipal::from_config(&cfg).expect("build");
    assert_eq!(p.user_id, "svc-sqe-maintenance");
}
```

- [ ] **Step 2: Run to verify failure.** `cargo test -p sqe-coordinator --lib principal_from_config_builds` -> FAIL.
- [ ] **Step 3: Implement.** `from_config` maps `MaintenancePrincipalConfig` -> `OidcM2mConfig` (set `user_id`, `roles`, `refresh_skew = Duration::from_secs(refresh_skew_secs)`), `OidcM2mProvider::new(...)`. `mint_session`: `let id = self.provider.authenticate(&FlightCredentials::default()).await?;` (authenticate ignores credentials), then build a `Session` mirroring `identity_to_session` but with the maintenance id and WITHOUT inserting into a manager. Read `session_manager.rs::identity_to_session` (line ~249) and copy the field mapping. Mark the session with an explicit internal maintenance flag if `Session` supports one (see Risk 1 in the design); otherwise leave a `// TODO(4b): explicit write-authority marker` and rely on Polaris for now.
- [ ] **Step 4: Run test.** PASS.
- [ ] **Step 5: Commit.** `git commit -m "feat(maintenance): service principal + ephemeral session minting"`

---

### Task 3: `CALL system.table_health` (read-only procedure)

**Files:**
- Modify: `crates/sqe-sql/src/procedures.rs` (variant + parse + name + doc header)
- Modify: `crates/sqe-coordinator/src/classifier.rs` (route to Procedure, read-only)
- Create: `crates/sqe-coordinator/src/table_health.rs` (pure analysis + batch shaping)
- Modify: `crates/sqe-coordinator/src/maintenance.rs` (handler arm, NO write gate)
- Modify: `crates/sqe-coordinator/src/lib.rs` (`pub mod table_health;`)
- Test: `crates/sqe-coordinator/tests/it/maintenance_procedures_test.rs` (classification) + a new `#[ignore]` e2e in `rewrite_data_files_real.rs` or a new `table_health_test.rs`

**Interfaces produced:**
- `ProcedureCall::TableHealth { table: TableRef }`
- `struct TableHealth { live_data_files: u64, small_files: u64, avg_file_bytes: u64, p50_file_bytes: u64, delete_files: u64, delete_heavy_files: u64, eligible_groups: u64, est_rewrite_bytes: u64, last_compaction_snapshot_ms: Option<i64>, maintenance_enabled: bool }`
- `fn analyze_table_health(data: &[DataFile], deletes: &[DataFile], read_plan: &DeleteAwareReadPlan, cfg: &MaintenanceCompactionConfig, props: &HashMap<String,String>) -> TableHealth` (pure; unit-testable)
- `fn table_health_batch(&TableHealth) -> RecordBatch`

- [ ] **Step 1 (parser): failing test.** In `procedures.rs` tests, `parses_table_health` for `CALL system.table_health(table => 'ns.t')` -> `ProcedureCall::TableHealth { table }`. Run -> FAIL.
- [ ] **Step 2 (parser): implement.** Add the variant; `parse_table_health(args)` = `take_table` + `expect_no_remaining`; register in `try_parse_call` dispatch (`"table_health" => parse_table_health(args).map(Some)`), in `name()` (`"table_health"`), and in the `table()` accessor. Mirror `SuggestBloomFilterColumns` which is the existing read-only procedure. Run -> PASS.
- [ ] **Step 3 (classifier): failing test.** In `maintenance_procedures_test.rs`, assert `parse_and_classify("CALL system.table_health(table => 'ns.t')")` is `StatementKind::Procedure`. Confirm it does not require write classification. Run -> FAIL if classifier does not know it.
- [ ] **Step 4 (classifier): implement.** Route `table_health` like `suggest_bloom_filter_columns` (read-only procedure) in `classifier.rs`. Run -> PASS.
- [ ] **Step 5 (analysis): failing unit test.** In `table_health.rs`, unit-test `analyze_table_health` on synthetic `DataFile`s (reuse the `data_file_of_size` / `data_file_part` builders pattern from maintenance tests): e.g. 10 files, 3 below target -> `small_files == 3`; delete-heavy count matches `delete_heavy_files` semantics; `eligible_groups` matches `pack_file_groups_partition_aware(...).filter(len >= min_input).count()`. Run -> FAIL.
- [ ] **Step 6 (analysis): implement.** Pure function over the already-collected file/delete/plan data. `small_files` = count `size < target`. `avg`/`p50` from sizes. `delete_files` = `deletes.len()`. `delete_heavy_files` = `delete_heavy_files(read_plan, cfg.delete_file_threshold).len()`. `eligible_groups` = eligible group count under bin-pack. `est_rewrite_bytes` = sum of bytes in eligible groups. `last_compaction_snapshot_ms` from the newest snapshot whose summary carries `sqe.maintenance.job-id` (None in 4a since nothing has stamped yet). `maintenance_enabled` from table props `sqe.maintenance.enabled == "true"`. Run -> PASS.
- [ ] **Step 7 (handler): implement + e2e.** Add the `ProcedureCall::TableHealth` arm in `maintenance.rs::handle` that does NOT call the write-gate `authorize_or_deny` write path (read-only; still audited). Load the table via the caller's session (`create_catalog_bridge`), collect data + delete files + read plan (reuse existing helpers), call `analyze_table_health`, return `table_health_batch`. Add an `#[ignore]` integration test (docker stack) that creates a table with several small files + a couple of MoR deletes and asserts the reported `live_data_files`, `small_files`, and `delete_files` are correct and that `SELECT`-ing it needs no write privilege.
- [ ] **Step 8: Run** unit tests (`cargo test -p sqe-coordinator --lib table_health`) -> PASS; integration behind the stack.
- [ ] **Step 9: Commit.** `git commit -m "feat(maintenance): CALL system.table_health read-only procedure"`

---

### Task 4: `sqe_system.maintenance_log` appender (best-effort)

**Files:**
- Create: `crates/sqe-coordinator/src/maintenance_log.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs`
- Test: inline unit (row shaping) + `#[ignore]` e2e append when the table exists

**Interfaces produced:**
- `struct MaintenanceLogRow { job_id, table, trigger, principal, started_at_ms, finished_at_ms, status, files_in, files_out, bytes_in, bytes_out, rows_removed, snapshot_id: Option<i64>, error: Option<String> }`
- `async fn append_row(catalog, state_table: &str, row: &MaintenanceLogRow) -> Result<()>` — appends via a normal Iceberg INSERT/append; if the state table does not exist, log a warning and return `Ok(())` (best-effort; never fails the caller in 4a).
- `fn advisory_row(table, principal, health: &TableHealth, ts_ms: i64) -> MaintenanceLogRow` (status `"advisory"`).

- [ ] **Step 1: failing unit test** for `advisory_row` field mapping (status == "advisory", files_in == health.live_data_files, etc.). Run -> FAIL.
- [ ] **Step 2: implement** the row types + `advisory_row`. Run -> PASS.
- [ ] **Step 3: implement `append_row`.** Resolve the `ns.table` from `state_table`; attempt `load_table`; on NotFound, `warn!` and return Ok. On success, append one row using the existing INSERT/append write path (reuse `write_handler` append or a direct fast-append of a one-row batch). Keep the schema fixed and documented at the top of the file.
- [ ] **Step 4: e2e (`#[ignore]`).** With the stack, `CREATE TABLE sqe_system.maintenance_log (...)`, call `append_row`, `SELECT` it back, assert the row. Also assert that with the table absent, `append_row` returns Ok and does not error.
- [ ] **Step 5: Commit.** `git commit -m "feat(maintenance): best-effort maintenance_log appender"`

---

### Task 5: Advisory scheduler loop

**Files:**
- Create: `crates/sqe-coordinator/src/maintenance_scheduler.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs`
- Modify: `crates/sqe-coordinator/src/bin/sqe_server.rs` (construct principal + register guard)
- Modify: `crates/sqe-metrics/src/lib.rs` (gauges/counters)
- Modify: audit (`AuditKind::Maintenance`)
- Test: unit (discovery filter, jitter determinism) + `#[ignore]` e2e (advisory tick mutates nothing)

**Interfaces consumed:** `MaintenanceConfig`, `MaintenancePrincipal` (Task 2), `analyze_table_health` (Task 3), `maintenance_log::append_row` (Task 4), `WorkerRegistry` (for the fleet gauge only in 4a), `spawn_supervised`, `MetricsRegistry`, audit sink.

**Interfaces produced:**
- `struct MaintenanceScheduler { cfg: MaintenanceConfig, principal: Arc<MaintenancePrincipal>, metrics, audit, catalog_factory }`
- `fn spawn(self) -> TaskGuard` (returns the supervised task guard; caller pushes into `_task_guards`).
- `fn table_due(ident: &str, schedule: &str, jitter_secs: u64, now_ms: i64) -> bool` (pure; deterministic per-table jitter = `hash(ident) % jitter_secs`).
- `async fn advisory_tick(&self) -> Result<()>` (discover -> filter -> analyze -> emit).

- [ ] **Step 1: failing unit tests.** (a) `table_due` is deterministic for a given ident + returns within the jitter window; (b) a discovery-filter unit: given a list of `(table, props)`, only those with `sqe.maintenance.enabled == "true"` are selected. Run -> FAIL.
- [ ] **Step 2: implement the pure helpers** (`table_due`, `select_enabled`). Run -> PASS.
- [ ] **Step 3: implement `advisory_tick`.** Mint an ephemeral session via the principal; list namespaces/tables visible to the principal token (single-catalog / default warehouse in 4a; multi-catalog is later); load each, read props, filter `select_enabled`; for each due table, run the same collection + `analyze_table_health` as Task 3; emit Prometheus gauges (`sqe_table_small_files`, `sqe_table_delete_files`, `sqe_maintenance_est_rewrite_bytes`), an `AuditKind::Maintenance` event (actor = principal user, session_id = a tick id), and a best-effort `maintenance_log::advisory_row`. NEVER mutate a user table.
- [ ] **Step 4: implement `spawn`.** `spawn_supervised("maintenance-scheduler", move |token| async move { let mut ticker = interval(Duration::from_secs(tick_secs)); loop { select! { _ = token.cancelled() => break, _ = ticker.tick() => { if cfg.mode == Advisory || cfg.mode == Active { if let Err(e) = self.advisory_tick().await { warn!(?e, "advisory tick failed"); metrics.maintenance_tick_errors.inc(); } } } } } })`. In `Off` mode the loop is not spawned at all.
- [ ] **Step 5: wire bootstrap.** In `sqe_server.rs` coordinator mode: if `config.maintenance.mode != Off`, construct `MaintenancePrincipal::from_config(config.maintenance.principal)` (error out clearly if `None`, but Task 1 validation already guarantees presence), build the `MaintenanceScheduler`, and `_task_guards.push(scheduler.spawn())`. In `Off` mode, construct nothing (principal provider never exists -> the isolation invariant).
- [ ] **Step 6: metrics + audit.** Add the `sqe_maintenance_*` / `sqe_table_*` families in `sqe-metrics`; add `AuditKind::Maintenance` and the event constructor. Unit-test that the metric names register without panic.
- [ ] **Step 7: e2e (`#[ignore]`).** Boot a handler with `mode="advisory"` and a stub principal pointing at the test IdP (or inject a pre-minted session for the test); create one opted-in table + one non-opted table; run one `advisory_tick`; assert the opted table's gauges are set, the non-opted table is untouched, and NO snapshot was added to either table (advisory mutates nothing). Assert the audit sink received a `Maintenance` event.
- [ ] **Step 8: Run** unit tests -> PASS; integration behind the stack.
- [ ] **Step 9: Commit.** `git commit -m "feat(maintenance): advisory scheduler loop (mutates nothing)"`

---

### Task 6: Docs + config example

**Files:**
- Modify: `docs/site/book/src/sql-reference/procedures.md` (add `table_health` reference row + example)
- Modify: `docs/site/book/src/deployment/configuration.md` (document `[maintenance]`, the three gates, advisory default, the operator-creates-`maintenance_log` note)
- Modify: a quickstart or `config.example.toml` if one exists (add a commented `[maintenance]` block)

- [ ] **Step 1:** Add `table_health` to the procedures reference table + a worked example (`CALL system.table_health(table => 'analytics.events')` with a sample output batch).
- [ ] **Step 2:** Document `[maintenance]`: the `mode` ladder (off/advisory/active), the principal block, the per-table `sqe.maintenance.enabled` opt-in, the least-privilege Polaris grant, and that `sqe_system.maintenance_log` is operator-created.
- [ ] **Step 3:** Run the forbidden-char scan: `grep -rn $'—\|–\|→' docs/site/book/src/sql-reference/procedures.md docs/site/book/src/deployment/configuration.md` -> zero hits.
- [ ] **Step 4: Commit.** `git commit -m "docs(maintenance): table_health + [maintenance] config"`

---

## Verification (whole phase)

- `cargo build --all` clean; `cargo clippy --all-targets --all-features -- -D warnings` clean.
- `cargo test -p sqe-core -p sqe-sql -p sqe-coordinator --lib` green.
- Integration (docker-compose.test.yml): `table_health` reports correct counts; advisory tick mutates nothing (assert snapshot count unchanged on all tables); non-opted tables never selected; `maintenance_log` append is best-effort.
- Manual smoke: with `mode="off"` (default), confirm no `maintenance-scheduler` task is spawned and no principal provider is constructed (grep logs / a debug assertion).
- Invariant check: `git diff --stat vendor/` is empty.

## Self-Review notes

- Spec coverage: 4a items from the design section 7 are all covered (principal, config, table_health, advisory loop, maintenance_log). Active mutation, worker distribution, and the HA lease are explicitly OUT (4b/4c/4d).
- The `session_has_write_privilege` marker (design Risk 1) is deferred to 4b (Task 2 leaves a TODO); 4a never mutates so the gate is not exercised.
- `maintenance_log` bootstrap authority (design Risk 2): 4a treats the table as operator-created and degrades to warn-and-skip, so no CREATE grant is needed.
