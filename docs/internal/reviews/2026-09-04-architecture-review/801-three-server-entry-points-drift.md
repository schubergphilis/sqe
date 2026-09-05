# 801: Three server entry points with different features, argument syntax and runtime stacks

- Scope: sqe-coordinator (`src/main.rs`, `src/bin/sqe_server.rs`), sqe-worker (`src/main.rs`), scripts, README
- Severity: high
- Category: architecture
- Effort: M
- Status: fixed (branch `fix/config-startup-code-structure`, 2026-09-05)

## Resolution (2026-09-05)

Fixed on `fix/config-startup-code-structure`. The `sqe-coordinator` binary is
removed. Scripts, README and QUICKSTART launch `sqe-server`. `sqe-worker`
uses the same `sqe_core::build_server_runtime` builder, clap-style `--config`,
and a `/healthz`+`/readyz` listener on `prometheus_port + 1`.


## Problem

The workspace builds three long-running server binaries that all load
`SqeConfig` but do not agree on what a server is.

| Binary | Source | Lines | Config arg | Runtime | Has /healthz | Used by |
|---|---|---|---|---|---|---|
| `sqe-server` | `crates/sqe-coordinator/src/bin/sqe_server.rs` | 3075 | clap `--config`, `--mode` | manual, 8 MiB stacks (`:762-766`) | yes (`:517-518`) | Dockerfile `:135`, Helm, compose |
| `sqe-coordinator` | `crates/sqe-coordinator/src/main.rs` | 685 | positional `args().nth(1)` (`:126-128`) | manual, 8 MiB stacks (`:110-114`) | no | README `:17`, `:209`; 8 scripts |
| `sqe-worker` | `crates/sqe-worker/src/main.rs` | 112 | positional (`:9-11`) | `#[tokio::main]` (`:7`), default 2 MiB stacks | no | `docker-compose.distributed.yml:33,47` |

Feature drift between the two coordinator binaries, by keyword hit count in
each file: `healthz` 0 vs 14, `maintenance` 0 vs 36, `web_ui` 0 vs 65,
`quack` 0 vs 29, `lineage` 1 vs 32, `production_mode` 0 vs 1, `hot_reload`
0 vs 1. The `sqe-coordinator` binary is a frozen snapshot of the server from
before maintenance, web UI, Quack, lineage and health endpoints existed, yet
it is the binary the README quick start tells new users to run and the one
`scripts/benchmark-test.sh:288`, `scripts/benchmark-matrix.sh:205`,
`scripts/benchmark.sh:109`, `scripts/benchmark-publish-iceberg.sh:189`,
`scripts/benchmark-load.sh:132`, `scripts/benchmark-split.sh:49` and
`scripts/ci/attach-parity-smoke.sh:82` launch.

Worker drift: Helm runs the worker as `sqe-server --mode worker`
(`deploy/helm/sqe/templates/worker-deployment.yaml:44`), so it gets the
8 MiB runtime threads, credential hot-reload wiring and health endpoints of
`sqe_server.rs:1801-1809`. Compose runs the standalone `sqe-worker` binary,
which builds its runtime with `#[tokio::main]` and therefore 2 MiB worker
stacks, the very size the integration notes say overflows on the write path
(`scripts/integration-test.sh:45-48`).

## Why it matters

- Benchmarks committed to `benchmarks/results/` are produced by a binary
  that is not the one shipped in the image. Any code path wired only in
  `sqe_server.rs` (maintenance scheduler, lineage emitter, web UI auth,
  production-mode gating) is absent from the benchmark process, so the
  committed numbers do not measure the deployed process.
- A user following the README gets a server with no `/healthz`, and the
  Helm probes documented next to it would fail against it.
- A deep-plan query that runs on a Helm worker can SIGSEGV on a compose
  worker because of the stack difference, with no configuration visible
  to explain it.
- Every bootstrap fix has to be made in two places; `main.rs` and
  `sqe_server.rs` already duplicate the runtime builder, the TLS warning
  and the audit logger setup.

## Proposed change

1. Delete `crates/sqe-coordinator/src/main.rs` and the `sqe-coordinator`
   `[[bin]]` entry (`crates/sqe-coordinator/Cargo.toml:26-28`). Replace with a
   `sqe-coordinator` symlink or a two-line shim that `exec`s
   `sqe-server --mode coordinator` if the name must survive for scripts.
2. Make `crates/sqe-worker/src/main.rs` call the same
   `sqe_worker::bootstrap::run(config, ...)` that `sqe_server.rs:1801-1809`
   uses, with the same manual runtime builder and stack size. Move the
   runtime builder into `sqe_core::runtime::build_server_runtime()` so there
   is exactly one.
3. Update the 8 scripts and README `:17`, `:209` to `sqe-server --config`.
4. Give both surviving binaries the same argument parser (clap) and accept
   `--config` everywhere; keep positional as a deprecated alias for one
   release with a `warn!`.

## Acceptance criteria

- `cargo metadata` lists exactly two server binaries: `sqe-server`, `sqe-worker`.
- `grep -rn 'target/[a-z-]*/sqe-coordinator' scripts README.md docs/site` returns nothing.
- `sqe-worker --config x.toml` and `sqe-server --mode worker --config x.toml`
  log the same startup banner, expose `/healthz` on the same port, and
  `ps -o rss` shows the same thread count after start.
- `PROFILE=dev-release BENCH_SCALE=1 ./scripts/benchmark-test.sh tpch`
  completes against `sqe-server`.

## Related

- 808 (stack-size dependence).
- 809 (deployment artifact drift).
- `nextsteps.md` benchmark baselines reference the coordinator-binary runs.
