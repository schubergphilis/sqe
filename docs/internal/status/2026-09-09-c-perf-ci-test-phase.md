**STATUS 2026-09-09 (c): CI test job anatomy and the two cheap fixes, on `perf/ci-test-phase`.**

Pipeline 1609830's `aikido-test-rust` was 21m: 12m41 `cargo test --no-run`
(623 crates, cold: the cache key is per branch and holds only the crate
registry; `target/` is 33 GB against a 5 GiB cache limit; sccache is off
until the bucket exists on the prd endpoint), 2m42 test execution, 5m06
clippy serial after the suite, ~30 s the rest. Three levers, in payoff
order: (1) the sccache bucket (A2; platform team; removes most of the 12m41);
(2) clippy in its own parallel job: aikido !166 adds
`AIKIDO_CARGO_CLIPPY_JOB="1"` -> `aikido-clippy-rust` alongside the gate and
the gate skips clippy; consumer change here after the aikido release is the
include ref bump plus that variable (about 5 min off the wall clock); (3) the
test phase itself. Per-test timing (`RUSTC_BOOTSTRAP=1 cargo test -- -Zunstable-options --report-time`)
found two shapes: `sqe-worker`'s four signature/auth tests each waited 7 to
9 s for the expected downstream failure, because the fixtures carried
`WorkerDefault` credentials and no endpoint, so the S3 client ran the
instance-metadata probe to its timeout; static keys plus a closed local port
still paid the retry schedule, so the fixtures now fail at store
construction (bucket-less `s3:///` URL, `nowhere://` scheme for FileIO):
`flight_service::tests` 36 tests 0.26 s, the binary 38.8 s -> ~7 s on CI
cores. `sqe-bench`'s two row-count identity tests generated TPC-DS / TPC-BB
at SF1 in debug mode (7 s each); scale 0.01 says the same thing. The SF1
planted-coincidence tests stay (their constants are SF1 facts). Also still
there: 77 test binaries run serially by libtest; nextest would halve the
phase but the template does not use it.
**NEXT:** after aikido !166 releases, bump the include ref and set
`AIKIDO_CARGO_CLIPPY_JOB: "1"`; sccache bucket with the platform team; then
#659 partials (508/108 deletions), 807 item 3.
