# Ranger / Polaris access control as a full integration test

**Date:** 2026-07-31
**Status:** design approved, implementation pending
**Scope:** new `access_control_e2e` module in the `sqe-coordinator` integration harness, a wrapper script, a make target, and one wiring refactor

## Summary

Access control is the only major subsystem in SQE whose end-to-end behavior is
asserted exclusively by a shell script. `quickstart/polaris-ranger-keycloak/test.sh`
covers ten scenarios against a live Polaris + Ranger + Keycloak stack, but it
classifies results by grepping CLI output, and several enforcement paths it does
not touch at all. This change ports that coverage into the Rust integration
tier, asserts on `RecordBatch` contents instead of text, and adds the paths that
have never run against a live Ranger: row filters, tag-based column masks, tag
row filters, tag fail-closed, and keyed hash masks.

The quickstart harness stays exactly as it is. It remains the demo, the
`scenario-test` CI job, and the base for the Spark cross-compare in
`parity-test.sh`.

## Motivation

What is covered today, and how:

| Path | Covered by | Assertion quality |
|---|---|---|
| GRANT enables, REVOKE disables | quickstart `test.sh` | greps for `Error:` |
| Ranger DENY precedence | quickstart `test.sh` | greps a denial-word list including `not found` |
| Resource column masks | quickstart `test.sh` | greps for absence of digits in the output |
| Admin gate on GRANT/REVOKE | `it/grant_dispatch_test.rs` | real, but against a recording stub |
| Cache flush on GRANT | `it/grant_introspection_gate_test.rs` | real, but against a recording stub |
| Row filters | nothing | not covered |
| Tag column masks, tag row filters | nothing | not covered |
| Tag fail-closed (unmappable CUSTOM) | nothing | not covered |
| Keyed hash mask (issue #37) | nothing | not covered |

Two findings drove the scope:

1. The demo deliberately drops its row-filter policy so the Spark mask
   cross-compare stays byte-comparable (`ranger/bootstrap-ranger.sh`, and the
   comment block at `test.sh` step 5). Row-level enforcement therefore has no
   end-to-end coverage at all.
2. The tag path has never seen a live Ranger bundle.
   `crates/sqe-policy/src/ranger_store.rs:27` carries
   `TODO(phase3): verify tagPolicies shape against a live tag-linked bundle`,
   `crates/sqe-policy/src/testdata/tag_bundle_live_sample.json` is a
   placeholder whose own text asks for a real capture, and
   `resolve_tag_policies_against_live_sample` is `#[ignore]`d waiting for it.
   The `tagPolicies` deserialization shape is, today, an educated guess.

A grep-based denial check is also weak in a specific way that matters here:
`is_denial()` matches `not found`, which is the same string a typo in a table
name produces. A test that cannot tell "correctly hidden" from "wrong
identifier" is not a security test.

## Non-goals

- Retiring or modifying the quickstart shell harness.
- The Spark / Kyuubi cross-compare. That stays in `parity-test.sh`.
- OPA and Cedar backends. `build_policy_enforcer` still bails on both.
- The Flight SQL server path. The coordinator runs in-process, matching every
  other test in the `it` harness.
- The "tag state unknown, therefore deny" branch, which needs a forced
  metadata-cache miss. Listed as a follow-up below.

## Architecture

### Coordinator: in-process

A new module `crates/sqe-coordinator/tests/it/access_control_e2e.rs`, registered
in `tests/it/main.rs`, follows the shape of `tests/it/common/mod.rs`:

1. Load `tests/sqe-ranger-test.toml`.
2. `policy_wiring::build_policy_enforcer(&config.policy, Some(table_cache), None)`
   returns the Ranger enforcer plus the `Arc<dyn PolicyStore>`.
3. `policy_wiring::build_grant_backend(...)` (see the refactor below) returns the
   Ranger grant backend.
4. `sqe_auth::Authenticator::authenticate(user, password)` performs the Keycloak
   ROPC exchange, so every query runs as a real federated identity with real
   realm roles.
5. `QueryHandler::new(...)` with the enforcer, store, and grant backend wired in.

The table metadata cache must be the same instance passed to
`build_policy_enforcer`, because `CacheTagSource` resolves column tags out of it.
A fresh cache per test would report tag state as unknown and fail closed.

### Stack: a subset of the existing quickstart

`scripts/access-control-test.sh` runs `docker compose up -d --wait` in
`quickstart/polaris-ranger-keycloak` naming only these services:

- `keycloak-config` (pulls in `keycloak`)
- `bucket-init` (pulls in `rustfs`)
- `ranger-setup` (pulls in `ranger-admin`, `ranger-db`)
- `polaris-setup` (pulls in `polaris`)

`sqe`, `data-seed`, and `spark` are not in any of those dependency chains, so
they never start. No SQE image is built and the demo's seeded tables and grants
are never created, which keeps the test's namespace clean.

Ranger Admin first boot takes two to four minutes. The script polls
`GET /service/plugins/services` with a bounded deadline (default 300s,
`AC_RANGER_TIMEOUT` overrides) and fails with the last HTTP status rather than
hanging.

### Config: `tests/sqe-ranger-test.toml`

Mirrors `quickstart/polaris-ranger-keycloak/sqe.toml` with host-published ports
from `.env.example`, and three deliberate differences:

```toml
[auth]
keycloak_url = "http://localhost:38080"
realm = "iceberg-ranger"
admin_roles = ["sqe_admin"]

[[auth.providers]]
type = "oidc_password"
token_url = "http://localhost:38080/realms/iceberg-ranger/protocol/openid-connect/token"
client_id = "sqe-client"
client_secret = "sqe-secret-change-me"
roles_claim = "realm_access.roles"

[catalogs.sales_wh]
polaris_url = "http://localhost:28181/api/catalog"
warehouse = "sales_wh"

[catalogs.ops_wh]
polaris_url = "http://localhost:28181/api/catalog"
warehouse = "ops_wh"

[storage]
s3_endpoint = "http://localhost:29000"
s3_path_style = true
s3_allow_http = true

[policy]
engine = "ranger"
mask_key = "sqe-ac-e2e-mask-key"       # difference 1: exercises keyed HMAC, issue #37

[policy.ranger]
url = "http://localhost:26080"
service-name = "sqe_ac_hive"           # difference 2: test-owned service, not the demo's `hive`
cache-ttl-secs = 2                     # difference 3: REST-created policies land in seconds

[access_control]
backend = "ranger"
url = "http://localhost:26080"

[access_control.ranger]
service-name = "polaris"               # shared: must match Polaris's authorizer config
realm = "*"
```

`realm = "*"` is not optional. Polaris sends a `root` resource on every
authorization request, and without a matching value every SQE-written policy is
accepted by Ranger and then silently never matches.

### Wiring refactor

`build_policy_enforcer` lives in `policy_wiring.rs` precisely so the two
coordinator binaries cannot drift. `build_grant_backend` does not: it is
duplicated as a private function in `crates/sqe-coordinator/src/main.rs:633` and
`crates/sqe-coordinator/src/bin/sqe_server.rs:802`. The test needs it, and a
third copy is the drift this module exists to prevent.

Move it into `policy_wiring.rs` as
`pub fn build_grant_backend(config: &SqeConfig) -> anyhow::Result<Option<Arc<dyn GrantBackend>>>`
and have both binaries call it. Behavior unchanged: the two copies were diffed
during design and are byte-identical (52 lines each), so the move carries no
reconciliation risk.

## Ranger fixture model

### Services

The test creates and owns two Ranger services, both idempotently, both over the
Admin REST API with basic auth:

| Service | Type | Purpose |
|---|---|---|
| `sqe_ac_hive` | `hive` | fine-grained resource policies: masks, row filters |
| `sqe_ac_tag` | `tag` | tag-based policies, linked to `sqe_ac_hive` via its `tagService` field |

Fine-grained policies deliberately do **not** go on the demo's shared `hive`
service. Linking a tag service mutates the downloaded bundle for everything that
reads `hive`, including the Spark path that `parity-test.sh` cross-compares
against. The cost of this isolation is that the test validates the same code
paths but not literally the same service instance the demo uses.

The coarse gate is unavoidably shared: `access_control.ranger.service-name` must
be `polaris` to match Polaris's embedded authorizer. GRANTs written there are
scoped to the test's own tables, so they cannot widen access to demo tables.

### Policies

Every policy the test creates is named with the prefix `sqe-ac-e2e-`. Setup
deletes every existing policy carrying that prefix before creating any, so a
crashed previous run cannot poison the next one. Teardown deletes them by name.

| Policy | Service | Shape |
|---|---|---|
| `sqe-ac-e2e-mask-amount` | `sqe_ac_hive` | datamask, `amount` -> MASK_NULL, role `engineer` |
| `sqe-ac-e2e-mask-ssn` | `sqe_ac_hive` | datamask, `ssn` -> MASK_SHOW_LAST_4, role `engineer` |
| `sqe-ac-e2e-mask-hash` | `sqe_ac_hive` | datamask, `email` -> MASK_HASH, role `engineer` |
| `sqe-ac-e2e-rowfilter` | `sqe_ac_hive` | rowfilter, `region = 'EU'`, role `engineer` |
| `sqe-ac-e2e-tag-mask-pii` | `sqe_ac_tag` | datamask on tag `PII` -> MASK_SHOW_LAST_4, role `engineer` |
| `sqe-ac-e2e-tag-rowfilter` | `sqe_ac_tag` | rowfilter on tag `RESTRICTED` -> `region = 'EU'`, role `engineer` |
| `sqe-ac-e2e-tag-mask-broken` | `sqe_ac_tag` | datamask on tag `SECRET` -> CUSTOM with no `valueExpr` |
| `sqe-ac-e2e-deny-audit` | `polaris` | deny on `ops_wh.ac.audit` for role `analyst` |

Ranger resolves tags for its own consumers through Atlas / tagsync. SQE does
not: it resolves column to tag from the Iceberg `sqe.column-tags` property via
`CacheTagSource` and asks Ranger only for the tag to mask rule. Nothing needs to
be installed or synced for the tag policies to apply.

### Data

carol (the only `sqe_admin`) creates the fixtures:

- `sales_wh.ac.orders (id BIGINT, region VARCHAR, amount DOUBLE, ssn VARCHAR, email VARCHAR)`
  with three rows: `(1,'EU',10.0,'111-11-1111','a@x')`, `(2,'US',20.0,'222-22-2222','b@x')`,
  `(3,'EU',30.0,'333-33-3333','c@x')`
- `ops_wh.ac.audit (id BIGINT, event VARCHAR)` with two rows

Namespace `ac` is used in both warehouses so nothing collides with the demo's
`sales` / `ops` namespaces.

Column tags are authored through SQL, not by writing the property directly, so
the DDL path is covered as well:

```sql
ALTER TABLE sales_wh.ac.orders MODIFY COLUMN ssn SET TAG PII = 'true';
ALTER TABLE sales_wh.ac.orders MODIFY COLUMN region SET TAG RESTRICTED = 'true';
```

## Test matrix

Every case asserts on decoded `RecordBatch` values. Denials assert a typed
error class, never a substring that a typo could also produce.

| # | Case | Assertion |
|---|---|---|
| 1 | Denied before grant | alice SELECT `sales_wh.ac.orders` fails; the error maps to a not-authorized or table-hidden class, and a control query on a table she can read still succeeds in the same session |
| 2 | GRANT enables | after carol grants SELECT to role `analyst`, alice sees exactly 3 rows, `region` = `EU,US,EU`, `amount` = `10.0,20.0,30.0` |
| 3 | Role vs user grant | bob reads via role `engineer`; a direct user grant on `ops_wh.ac.audit` to bob is honored |
| 4 | Write privileges | bob INSERT succeeds and the row is visible; alice INSERT and alice DROP are denied |
| 5 | Resource column masks | bob: `amount` is NULL in all rows, `ssn` is exactly `xxx-xx-1111` / `xxx-xx-2222` / `xxx-xx-3333`; alice: raw values |
| 6 | Keyed hash mask | bob's `email` equals the HMAC-SHA256 digest the test computes with `policy.mask_key`, proving the keyed path rather than bare SHA-256 |
| 7 | Resource row filter | bob sees exactly the 2 EU rows; alice sees 3 |
| 8 | Tag column mask | with `ssn` tagged `PII`, bob's `ssn` is `xxx-xx-1111`; alice's is raw. The mask comes from the tag policy, with the resource mask policy disabled for this case |
| 9 | Tag row filter | with `region` tagged `RESTRICTED`, bob sees exactly 2 rows |
| 10 | Tag fail-closed | with `email` tagged `SECRET` and a CUSTOM tag mask carrying no `valueExpr`, `email` is absent from bob's result schema entirely, and no raw value appears in any column |
| 11 | Ranger DENY precedence | deny on `ops_wh.ac.audit` for `analyst` overrides alice's allow; bob (engineer) still reads it |
| 12 | REVOKE | after REVOKE, alice is denied again without a fixed sleep, since GRANT/REVOKE flush the policy cache (issue #207) |
| 13 | SHOW GRANTS | the statement's Arrow output contains a row matching the grant the test made, asserted per column |
| 14 | CHECK ACCESS | allow for alice on a granted table, deny for dave (no role) |
| 15 | Live bundle capture | with `SQE_AC_CAPTURE=1`, `GET /service/plugins/policies/download/sqe_ac_hive` is written over `crates/sqe-policy/src/testdata/tag_bundle_live_sample.json` |

Case 15 is the payoff for the tag work: the captured bundle replaces the
placeholder, and the same change removes the `#[ignore]` from
`resolve_tag_policies_against_live_sample` and updates its asserted constants to
match the capture. If the live shape differs from what `ranger_store.rs`
deserializes, that is a real bug found by this test, and the `TODO(phase3)` at
line 27 gets deleted with evidence behind it.

Cases 5 and 8 both mask `ssn`. They must not run with both policies enabled, or
a passing tag case could be the resource policy doing the work. The fixture
enables exactly one of the two per case and asserts the other is absent.

## Gating, serialization, flake control

**Gating.** Every test is `#[ignore]`d *and* requires `SQE_AC_E2E=1`. The
distinction matters: `scripts/integration-test.sh` runs
`cargo test -p sqe-coordinator -- --ignored`, which would otherwise pick these
up against the wrong stack. When `SQE_AC_E2E` is unset the test returns early
with a one-line explanation. When it is set and the stack is unreachable, the
test **fails**. A gate that silently passes when the thing it tests never ran is
the failure mode already seen with the Trino comparison sweep, and it will not
be repeated here.

**Serialization.** A module-level `tokio::sync::Mutex` serializes the cases.
They share Ranger state, the policy cache, and the fixture tables. The wrapper
also passes `--test-threads=1`.

**Propagation.** `policy.ranger.cache-ttl-secs = 2` plus a bounded
`eventually(Duration, closure)` helper that retries until a deadline (default
30s) and, on timeout, reports the last observed value or error. No unconditional
sleeps.

**Stack size.** The wrapper exports `RUST_MIN_STACK=33554432`. The coordinator
write-path e2e tests SIGABRT on the default 2 MiB tokio stack, and this test
creates tables and inserts rows.

## Wiring

`scripts/access-control-test.sh`:

1. `cd quickstart/polaris-ranger-keycloak`, create `.env` from `.env.example` if
   absent, source it.
2. `docker compose up -d --wait keycloak-config bucket-init ranger-setup polaris-setup`.
3. Poll Ranger Admin, Polaris health, and the Keycloak realm endpoint with a
   bounded deadline.
4. `SQE_AC_E2E=1 RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --test it access_control -- --ignored --test-threads=1 --nocapture`.
5. Leave the stack up by default (matching `integration-test.sh`); `--down`
   tears it down.

Makefile:

```make
test-access-control:
	@scripts/access-control-test.sh
```

listed under the existing `Code:` help block, with a note that it brings up the
Ranger quickstart stack and that Ranger's first boot is slow.

CI: a job shaped like the existing `scenario-test` (docker-in-docker, generous
timeout, `changes:` on the policy crates plus this script). It only carries
signal once dind is healthy. Until then merge decisions on policy changes should
ride on a local run of this script, and the spec says so rather than letting a
green-but-empty pipeline imply coverage.

## Failure modes

| Failure | Behavior |
|---|---|
| Ranger Admin not up within the deadline | script exits non-zero with the last HTTP status |
| Stack unreachable while `SQE_AC_E2E=1` | test fails, never skips |
| Stale `sqe-ac-e2e-` policies from a crashed run | deleted during setup before anything is created |
| Fixture tables left behind | dropped best-effort in teardown; setup is `CREATE ... IF NOT EXISTS` plus a truncate-and-reinsert so a leftover table cannot skew row counts |
| A mask policy silently not applied | the case asserts exact masked values, so an unapplied mask shows up as a raw value, not as a passing "no digits found" grep |

## Verification plan

1. Run `scripts/access-control-test.sh` against a clean stack; all cases pass.
2. Mutation checks, each expected to turn a specific case red:
   delete `sqe-ac-e2e-mask-ssn` (case 5), delete the tag policy (case 8), drop
   the `SET TAG` statement (cases 8 to 10), clear `policy.mask_key` (case 6),
   point `policy.ranger.service-name` at the empty demo `hive` service (cases 5
   to 10).
3. Confirm `scripts/integration-test.sh` is unaffected: the AC cases do not run
   there, and its pass count is unchanged.
4. Confirm `quickstart/polaris-ranger-keycloak/run.sh --check` and
   `parity-test.sh` still pass after an AC run, proving fixture isolation.
5. Capture the live bundle, un-ignore `resolve_tag_policies_against_live_sample`,
   and confirm it passes against the real capture.

## Rollback

Self-contained: one new test module plus its `mod` line, one new config file, one
new script, one make target, one CI job. Reverting the commit removes all of it.
The only change to shipped code is the `build_grant_backend` extraction, which is
a pure move and independently revertable.

## Follow-ups

- Tag state unknown, therefore deny: needs a forced metadata-cache miss.
- Policy breaker: Ranger unreachable mid-query must fail closed. Needs
  stop/start of `ranger-admin` inside a test.
- Cache TTL expiry as its own case, distinct from the GRANT-time invalidation
  covered in case 12.
- A Flight SQL smoke test (one allow, one deny) so the server path is not
  entirely uncovered by the Rust tier.
