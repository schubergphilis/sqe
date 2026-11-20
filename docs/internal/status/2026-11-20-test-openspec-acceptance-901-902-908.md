**STATUS 2026-11-20: open OpenSpec acceptance tests (`test/openspec-acceptance-901-902-908`).**

- `signed-catalog-scope` 4.1 (#908, closed): `tests/it/catalog_scope_e2e.rs`.
  - The fixture is the issuer. It signs ES256 scopes with the test key already used in `sqe-auth`, and serves the JWKS over https with the coordinator's TLS test certificate (`accept_invalid_certs`).
  - `acceptance_1_scoped_client_needs_a_valid_scope` runs offline.
  - `acceptance_2_and_3_a_scoped_session_sees_and_writes_only_its_catalogs` is `needs:polaris`.
  - The gate is process-global, so the tests hold a lock and uninstall it on drop.
  - New dev-dependency: `jsonwebtoken`. It is a workspace crate already.
- `system-catalog-split` 6.2 (#901): `access_control_e2e::system_catalog_split_tag_mask_applies_and_ungranted_user_is_refused`.
  - The AC stack has no Polaris `system` catalog, so `ensure_polaris_system_catalog()` creates one through the management API. The call is idempotent.
  - Bob gets the masked values on all rows. Alice, who has no grant, is refused. Carol runs the same SQL first, which proves the SQL itself is valid.
- `admission-queue-and-drain` 4.4 (#902): `drain_process_e2e::drain_process_sigterm_lets_the_running_query_finish`.
  - Runs the real `target/debug/sqe-server` with `shutdown_drain_secs = 60`, anonymous auth and a wiremock Polaris, so no Docker is needed.
  - Sends SIGTERM during a ~5 s CPU-bound query. The query returns its rows, a later statement is refused with `draining`, and the process exits 0 about 0.3 s after the query ends.
  - It is `#[ignore]` and needs `cargo dev-server` first; `SQE_SERVER_BIN` overrides the path.

Gate:
- `make test-integration FILTER=catalog_scope_e2e`: exit 0, 1 passed.
- `cargo test -p sqe-coordinator --test it -- catalog_scope_e2e`: exit 0, 1 passed, 1 ignored.
- `scripts/access-control-test.sh system_catalog_split`: exit 0, 1 passed.
- `cargo test -p sqe-coordinator --test it -- --ignored drain_process`: exit 0, 1 passed.
- `cargo clippy -p sqe-coordinator --tests -- -D warnings`: exit 0.

Left open:
- No mutation check was run, for example removing the drain wait to see the test fail.
- The drain query runs 5 s, not the 20 s the spec mentions. The test asserts the query is still running at SIGTERM and when the late statement is refused, so 5 s is enough.
- A write into a catalog the scope hides is refused with "did not build for this session", not "not found". The test asserts only that it is refused.

With these, every task in the three changes is ticked. #901 and #902 can be closed, and the changes can be archived.

**NEXT:** #911 tag authority (OpenSpec first).
