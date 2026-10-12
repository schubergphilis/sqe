**STATUS 2026-09-08 (evening): finding 212 (#565) on `fix/production-mode-policy-engine-212`: `production_mode` refuses the two governance defaults.**

`validate_production` now rejects `policy.engine = passthrough` unless
`security.allow_no_policy_engine = true`, and `access_control.backend = none`
unless `security.allow_no_grant_backend = true`, with messages naming the
real engines and the flag; `build_policy_enforcer` logs the disabled
enforcement at warn level when passthrough is selected, the same signal the
anonymous and bearer-passthrough auth providers emit. The Helm chart now
renders `[policy.ranger]` (engine = ranger only), `[access_control]`,
`[access_control.ranger]` (backend = ranger only) and the two `[security]`
flags from values; `values-production.yaml` selects the Ranger pairing for
both, with an empty `policy.ranger.url` the operator must fill (the engine
refuses to start otherwise). Book: deployment/kubernetes.md. Gates: sqe-core
425 (four new production-mode tests), policy_wiring 10, `helm lint` and a
rendered-TOML parse of both values files, clippy clean.
**NEXT:** 205 (#558, SHOW STATS restricted columns), 610 (#657), 627, #659
partials; the docker-capable CI runner for the `polaris` group (807 item 3).
