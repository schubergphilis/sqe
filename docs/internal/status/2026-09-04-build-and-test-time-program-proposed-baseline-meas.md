**STATUS 2026-09-04: build and test time program proposed; baseline measured, nothing changed yet.**

`docs/internal/plans/2026-09-04-build-test-time-program.md` carries the
numbers. The short version: the MR pipeline is 72 minutes because two
serial jobs are 95 percent of it (`aikido-test-rust` 20 to 38 min,
`aikido-build` 38 to 67 min), the `target/` cache zip exceeds the 5 GiB
GitLab limit so every job spends 10 minutes archiving something that never
uploads, and the actual tests run for about 2 minutes. Locally, resolver-2
feature unification gives every cargo selection its own copy of the
DataFusion stack (38 copies found in `target/debug/deps`, 147 GB target on
a full disk): `cargo test -p sqe-coordinator --no-run` after a one-line
touch takes 5m50s where `--workspace` takes 31 s. 77 integration test
binaries cost more CPU than every first-party library together, and
`aws-lc-sys` (95 s of C, pulled by reqwest's `rustls` feature while `ring`
is already present) heads the cold critical path.

NEXT, in order: A1 run the image build in parallel with tests and only when
the Dockerfile or lockfile changes on MRs; A2 sccache to the GitLab S3
bucket instead of the `target/` zip; A3 ring instead of aws-lc; B1
cargo-hakari workspace-hack; B2 re-consolidate `tests/*.rs` into one
harness per crate; B3 nextest; B4 build binaries in a cargo job and make the
image job COPY-only. Each is its own MR with before/after numbers per the
protocol at the end of the plan.
