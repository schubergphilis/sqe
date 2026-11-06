**STATUS 2026-11-06: GitHub release-binaries workflow builds again (`fix/github-release-binaries-no-sqe-worker`).**

Every `release-binaries` run on the GitHub mirror since v0.50.0 failed in about a minute with
"no bin target named `sqe-worker`": a64546647 (2026-09-18) deleted the standalone worker binary,
and the workflow still listed it in `cargo build` and in the packaging loop. No GitHub Release
exists for v0.50.0 through v0.53.1. The workflow now builds `sqe-server`, `sqe-cli` and `sqe-bench`.

**NEXT:** once this reaches the GitHub mirror, backfill with
`gh workflow run release-binaries -R schubergphilis/sqe -f tag=v0.53.1` (the run uses the workflow
from main and checks out the tag); older tags can be backfilled the same way if wanted.
