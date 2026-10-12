**STATUS 2026-09-07 (later): build-time lever A2 wired on `ci/sccache-compiler-cache`; #658 merged.**

The `target/` cache can never work here: the runner uploads `cache.zip`
with one presigned PUT and 5 GiB is the S3 single-object limit, while
`target/` is 37 GiB after one gate run. Dependency reuse now comes from
sccache: aikido v0.12.1 ships `.aikido_rust_build_env` (opt-in sccache to
S3 with protected read-write / unprotected read-only credentials, and
`dev`/`test` debuginfo at `line-tables-only`), both platform Rust jobs
use it, and this branch bumps the include refs to v0.12.1 and adds the
same block to `release-binaries`. The bucket is `sqe-cargocache` on the
acc StorageGRID; the settings and the read-write key pair are project CI
variables. Pending outside the repo: the 30-day lifecycle rule (the key
has no lifecycle permission; tenant admin), a read-only key pair so merge
requests read the cache, and whether the acc endpoint is reachable from
the production runners (the job log says `sccache: on` or why not).
Measured before: test job 27 min (7.5 setup + 12 compile + 2 tests + 3.8
clippy), release binaries 43.5 min (28 thin-LTO link). Next levers, in
order of minutes saved: release profile only on tags and a nightly
(~25 min per merge), a prebaked `aikido-rust` image (7.5 min per job),
gating on default features instead of `--all-features` (53 extra
packages, 41 of them AWS SDK), nextest archives, then B2 and A3.
**NEXT:** read the first main run's `sccache --show-stats` and `/cache`
probe; then the two one-line MRs (release profile on tags only, default
features gate); then the 411 + 807 follow-up and the review leftovers.
