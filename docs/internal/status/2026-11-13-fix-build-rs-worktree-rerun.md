**STATUS 2026-11-13: `sqe-server` no longer rebuilds on every cargo run in a worktree (`fix/build-rs-worktree-rerun`).**

`crates/sqe-server/build.rs` records the commit in the binary and asked cargo to rerun it when `<git dir>/<branch ref>` changed. A linked worktree keeps its refs in the main repository's git dir, and `git pack-refs` moves a ref into `packed-refs`. In both cases the watched file was missing, cargo treats a missing path as changed, and `sqe-server` was recompiled and relinked on every build: 4.5 minutes per architecture in a release build with nothing edited.

The script now asks git where each path lives (`git rev-parse --git-path`), watches only paths that exist, and watches `HEAD`, the `HEAD` reflog and either the loose branch ref or `packed-refs`.

Checked with the script compiled standalone:

- linked worktree: watches the worktree's `HEAD` and reflog and the branch ref in the main git dir
- loose ref, packed ref, detached `HEAD`: a commit, a checkout and a `reset --hard` each change a watched file; two runs with no git activity do not
- no git checkout: `SQE_GIT_SHA=unknown`, no watch lines
- in a worktree with a warm release cache: a second `cargo build` finished in 9 s and compiled nothing

**NEXT:** none for this fix. `scripts/seed-target.sh` second checkouts and agent worktrees get the saving without any change.
