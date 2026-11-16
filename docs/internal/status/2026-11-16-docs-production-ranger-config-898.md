**STATUS 2026-11-16: minimal production config with Ranger, and rule names in startup errors (`docs/production-ranger-config-898`).**

Part of #898 (Ask 4, and the rule-name half of acceptance test 3):
- `production-guide.md` gains "Minimal production config with Ranger". It is a whole `sqe.toml` that passes every `production_mode` rule with `policy.engine = "ranger"`.
- `production::violations()` prefixes each message with its rule name in brackets (`[rate-limit-enabled] ...`). The startup error therefore names the rule, which the guide's table lists.
- New unit test `production_guide_minimal_ranger_config_validates`. It extracts the block from the guide and runs it through `SqeConfig::validate()`, the same check startup uses, with the cert paths pointed at temp files. It then checks that the same file with `engine = "passthrough"` fails naming `[policy-engine-configured]`.

The first version of the block missed `[rate_limit] enabled = true` and left the default plaintext `trino_http_port` open. The test caught both before commit.

Gates:
- `cargo test -p sqe-core --lib config::`: 317 passed, 0 failed.
- `cargo clippy -p sqe-core --all-targets -- -D warnings`, `cargo xtask budgets --check` and the rustfmt ratchet on `production.rs`: exit 0.

Left open, waiting on a user decision about CI runners and the Harbor mirror:
- Ask 1: a multi-arch manifest list per release tag, built in CI.
- Ask 2: the manifest digest in the release notes.
- Ask 3: the release job fails when a platform is missing.
- Acceptance tests 1, 2 and 4: they need the published image.

**NEXT:** decide how CI builds arm64: arm64 runners, or `cargo-zigbuild` with `Dockerfile.ci` as `push-latest.sh` does. Confirm that the Harbor per-tag copy keeps the manifest list. Then change `aikido-release-image` (`.gitlab-ci.yml:188`) and have the release step print the digest.
