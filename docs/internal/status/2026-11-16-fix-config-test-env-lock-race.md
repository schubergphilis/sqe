**STATUS 2026-11-16: config test env race fixed (`fix/config-test-env-lock-race`).**

CI job `aikido-test-rust` on !1805 failed in `config::production::tests::telemetry_endpoints_need_https_unless_loopback`. The error was `unknown variant remote-signing in catalog.access_delegation (after env overrides SQE_CATALOG__ACCESS_DELEGATION)`.

Cause:
- `SqeConfig::load_from_str` applies the `SQE_*` env overlay.
- The access-delegation test in `config::tests` sets `SQE_CATALOG__ACCESS_DELEGATION=remote-signing` under `ENV_LOCK`.
- The production test loaded a config without taking that lock, so a parallel run could read the variable while it was set.
- The production test is the only config load under `config/` outside the lock. A scan of every `load_from_str` / `load_with_env` call in the test modules confirms it.

Fix:
- `ENV_LOCK` becomes `pub(super)`, and its doc comment now says every config load under `config/` must hold it.
- The production test takes the lock.
- Test-only; no changelog entry.

Gate:
- `cargo test -p sqe-core --lib`: 581 passed, in five consecutive runs.
- `cargo clippy -p sqe-core --all-targets -D warnings` and the rustfmt ratchet: exit 0.

**NEXT:** none for this fix. Any new test that loads a config under `config/` takes `ENV_LOCK`.
