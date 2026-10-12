**STATUS 2026-09-07 (late night): finding 807, items 1 and 2, on `test/ignore-groups-807`: every `#[ignore]` names its stack; `make test-integration GROUP=<stack>` runs one stack's worth.**

250 `#[ignore]` attributes now carry a reason whose first token is a fixed
vocabulary (`needs:polaris`, `needs:polaris+workers`, `needs:ranger`,
`needs:ranger+spark`, `needs:spark`, `needs:trino`, `needs:postgres`,
`needs:hms`, `needs:nessie`, `needs:unity`, `needs:minio`, `needs:aws`,
`bug:#n`, `bench`, `deferred`, `release-only`); 136 were bare before. New
`cargo xtask test-groups` lists the groups (polaris 158, ranger 43,
ranger+spark 19, aws 6, polaris+workers 5, ...), `--check` fails on a bare or
unknown reason and runs in `ci/aikido-rust-extra.sh`, `--names <group>`
feeds `scripts/integration-test.sh` so `make test-integration GROUP=polaris`
runs exactly those tests by libtest name (module-qualified for the `it`
harness; verified against the binary's own `--list --ignored`). Rules and
the open item (a docker-capable CI runner for the `polaris` group) in
`docs/internal/process/test-groups.md`.
Finding 411 (#591) landed on the same branch: `tests/it/common/chaos_worker.rs`
wraps a production `WorkerFlightService` in-process and cuts, hangs or slows
its `do_get` stream; `tests/it/distributed_failure_e2e.rs` runs the six cases
(refused connection, loss mid-stream, hang, `CancelQuery`, query deadline,
straggler) against the real registry, scheduler, retry loop, tracker and
streaming deadline. They need only the Polaris stack (`needs:polaris`), so
every `make test-integration` is now the gate for `distributed_scan.rs`,
`scheduler.rs`, `worker_registry.rs` and `streaming.rs`. Two test-side
lessons: the planner bin-packs small files into one fragment up to
`query.target_task_size`, so a spread over workers needs that knob low; and
`distribution_threshold` / `distribution_file_threshold` keep small tables
local unless zeroed.

Harness cross-talk fixed on the way (the MR's first CI run on the
aikido-rust image failed `trino_session_catalog_test` twice, deterministic,
passing locally): wiremock's `MockServer::start()` recycles pooled servers,
so a later test inherits an earlier test's `host:port`, and the
coordinator's process-wide catalog HTTP clients then reuse an idle
connection whose hyper driver died with the earlier `#[tokio::test]`
runtime ("error sending request", or a silently unresolved discovery).
Underneath, `trino_describe_test`, `audit_e2e_test` and `attach_rest_test`
used one fixed token for every test, so `SESSION_CONTEXT_CACHE`
(`username:token_hash:catalog:schema`) handed tests each other's contexts,
which the recycled ports had been masking. Fix: `common::mock_server()`
binds a fresh listener (no pool) at all 16 sites, and the three files use a
per-call token. Bisected with the 1.98 toolchain in a second target dir; the
toolchain was not the cause, the pairing `trino_session + trino_describe`
fails 6/6 on 1.97.1 too, and the full harness now passes 9/9 at 2, 4 and 8
threads.
**NEXT:** 806 (#638), 205/212 (#653), 610 (#657), 627, #659 partials; the
docker-capable CI runner for the `polaris` group (807 item 3).
