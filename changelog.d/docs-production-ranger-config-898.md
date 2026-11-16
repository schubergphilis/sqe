### Changed

- A `production_mode` startup error now names each rule it breaks, for example `[policy-engine-configured] production_mode: policy.engine is passthrough ...`, so an operator can look the rule up in the production guide

### Added

- Production guide: a minimal `sqe.toml` that starts with `production_mode = true` and Ranger enforcing row filters, masks and column restrictions (TLS-only Trino listener, rate limiting, group claims, vended credentials, `project-tags`, `catalog-scoped`). A unit test runs the documented block through startup validation, so it fails when a new rule makes it stale
