**STATUS 2026-09-05 (later): work package #659 (configuration, startup and code structure) on `fix/config-startup-code-structure`.**

Fail-closed `SQE_*` overlay, typed mode knobs, one tokio runtime builder,
`sqe-coordinator` binary removed in favour of `sqe-server`, worker
`--config` + `/healthz`, dead `deploy/k8s` removed, one Dockerfile with
`CATALOG_FEATURES`, embedded `--config` for mask key, buffered write
twins deleted. Partial: stacker for debug `RUST_MIN_STACK`, QueryHandler
split, dual field-stamping.
**NEXT:** remaining P1s in the combined index (202, 400, 503, 601/401/603).
