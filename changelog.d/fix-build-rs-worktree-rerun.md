### Fixed

- `sqe-server` recompiled on every build in a git worktree

    `crates/sqe-server/build.rs` asked cargo to watch `<git dir>/<branch ref>`. In a linked worktree, and after `git pack-refs`, that file does not exist, and cargo treats a missing watched path as changed: the crate was rebuilt and relinked on every `cargo build`. The script now resolves paths with `git rev-parse --git-path`, watches only paths that exist, and watches the reflog so a new commit is still picked up.
