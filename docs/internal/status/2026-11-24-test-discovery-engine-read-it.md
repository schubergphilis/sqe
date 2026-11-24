# test/discovery-engine-read-it

**NEXT:** run the live Ranger suite (openspec task 4.6) after the next
release, before the opt-in is turned on anywhere.

Follows `test/discovery-engine-read-load`
(`openspec/changes/discovered-catalog-engine-read/`, task 4.5).

## Done

- `crates/sqe-coordinator/tests/it/catalog_discovery_test.rs`, module
  `engine_read`: five live tests on the Polaris test stack. Each test makes its
  own principals, roles and `eng_*` warehouses through the management API, so
  runs share no grants or cached tokens.
  - A metadata-only person reads rows through the engine; with the opt-in
    off the same read fails. The audit has the proof event and a key fetch
    marked discovered.
  - A warehouse created after the handler and session exist is readable
    with no restart.
  - The engine holds no role: the read fails closed (see below).
  - An engine allowed to write does not carry the person's INSERT; the row
    count is unchanged.
  - A signed catalog scope without the warehouse refuses it before any proof
    event.
- `scripts/bootstrap-test.sh` creates the `warehouse-engine` bucket.

## Finding: `/v1/config` proves the catalog, not a grant

Polaris 1.9 answers `GET /v1/config?warehouse=W` with the catalog prefix for
any authenticated principal, including one with no role in `W`. The proof's
engine step therefore passes, the engine's `/credentials` call gets 403, and
the read fails with `ENGINE_CREDENTIALS_FAILED`. There is no fallback to the
person's keys. That fails closed and is kept. The design, spec, tasks and
`operations/catalogs.md` now say the engine principal needs a catalog role on
every allowlisted warehouse. A stronger proof and a retry on the person's
delegation are open options for the owner.

## Tests run

- `cargo test -p sqe-coordinator --test it catalog_discovery_test -- --ignored`
  on the test stack (18 passed, twice; the five new tests also passed on their
  own)
- `cargo clippy -p sqe-coordinator --tests -- -D warnings`

## Not run

- Live Ranger suite 4.6: waits for a release.
- `cargo test -p sqe-coordinator --lib`: no `src/` change.
