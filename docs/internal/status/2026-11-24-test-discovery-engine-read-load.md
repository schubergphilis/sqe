# test/discovery-engine-read-load

**NEXT:** run the catalog-discovery IT cases (openspec task 4.5) on the test
stack, then the live Ranger suite (4.6), before the opt-in is turned on
anywhere.

Follows `feat/discovery-engine-read`
(`openspec/changes/discovered-catalog-engine-read/`, task 4.4).

## Done

- `crates/sqe-coordinator/tests/it/discovery_engine_load.rs`: four wiremock
  tests that load a table through `discover_catalog_provider` and check what
  reaches the catalog.
  - Proved warehouse: engine keys, no `X-Iceberg-Access-Delegation` header,
    one `/credentials` call with the engine bearer, token form scope exactly
    `profile`.
  - Engine 403, a different engine prefix, a warehouse outside the allowlist:
    person's keys, `vended-credentials` on the load, no `/credentials` call.
  - Two proved warehouses: one token request.
  - A warehouse outside the session's catalog scope: no call at all.
- `discover_catalog_provider` is `pub` so the `it` harness can call it.
- `ENGINE_WIRING_LOCK` in `tests/it/common` serialises the tests that install
  the process-wide engine-identity policy store (`engine_credential_e2e` too).
- The tests live in `tests/it`, not `src/`, so the `sqe-coordinator` size
  budget needs no raise.

## Gotcha

The engine token cache and the REST client cache are process-wide, and
wiremock reuses ports. Each test uses its own client id and warehouse names,
or a token cached by an earlier test hides the token request.

The unit tests in `discovery_engine.rs` shared the client id `sqe-engine`, so
`two_warehouses_fetch_one_engine_token` failed in CI whenever another test had
cached a token on the same port. Each of them now takes its own client id.

## Tests run

- `cargo test -p sqe-coordinator --test it discovery_engine_load` (4 passed,
  three runs in a row)
- `cargo test -p sqe-coordinator --lib` (879 passed, 1 ignored)
- `cargo clippy -p sqe-coordinator --tests -- -D warnings`
- `cargo xtask budgets --check`

## Not run

- IT 4.5: the SQE test stack was not up, and starting it next to another
  running stack on a nearly full disk was not safe.
- Live Ranger suite 4.6: waits for a release.
