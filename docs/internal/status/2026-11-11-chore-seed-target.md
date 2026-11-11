**STATUS 2026-11-11: a second checkout can start from a warm build cache (`chore/seed-target`).**

`.cargo/config.toml` keeps `target-dir = "target"`, so every extra git worktree
or clone began with a cold build of about 720 crates. `scripts/seed-target.sh
<built-checkout>` clones `target/debug` (`.fingerprint`, `build`, `deps`)
copy-on-write into the checkout it runs from.

Measured in a scratch worktree at the same commit as the source checkout:

| step | result |
|---|---|
| clone of 45 GB `target/debug` | 20 s, free disk unchanged |
| first `cargo check-server` | 75 crates rebuilt, 2m00 |

The 75 are the workspace crates, the vendored path crates, and the registry
crates that depend on a vendored one (`arrow`, `parquet`, every `datafusion-*`).
Their fingerprints include an absolute path, so they rebuild once per checkout.
The other registry crates are accepted as fresh.

Two alternatives were set aside. A shared `build.build-dir` works on this cargo
but puts every checkout behind one build lock. sccache stays off locally for
the reasons already in `.cargo/config.toml`.

Limits: the seed is a snapshot, so it helps most when taken from a checkout
built on a nearby commit with the same feature set. Copy-on-write needs both
checkouts on one APFS, btrfs or XFS volume; elsewhere `cp` falls back to a full
copy.

**NEXT:** unchanged.
