### Added

- `scripts/seed-target.sh` seeds a second checkout's `target/` from a built one

    A new git worktree or clone starts with an empty `target/` and pays the full cold
    build. The script clones `target/<profile>` from an existing checkout copy-on-write
    (APFS clonefile, reflink on Linux), which took 20 s for 45 GB and used no extra
    disk. A first `cargo check-server` in the seeded checkout then rebuilt 75 crates
    (the workspace and what sits downstream of `vendor/`) instead of about 720, in
    2m00. Each checkout keeps its own build lock, so two checkouts can build at once.
