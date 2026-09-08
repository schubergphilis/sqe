# Test groups: what an `#[ignore]` has to say, and how to run one stack's worth

Finding 807 of the 2026-09-04 architecture review (#639). Before this, 278
tests were `#[ignore]`d because they need a stack plain `cargo test` does not
have, 166 of them with no reason string, and the only way to run them was
`--ignored`: all of them, one stack or not, 32 MiB of thread stack and ten
minutes. The reason string is now the test's metadata.

## The rule

Every `#[ignore]` carries a reason whose first token names its group. Free
text may follow after `;`.

| token | meaning | run with |
|---|---|---|
| `needs:polaris` | docker-compose.test.yml: Polaris (in-memory) + RustFS | `make test-integration GROUP=polaris` |
| `needs:polaris+workers` | plus docker-compose.distributed.yml (two sqe-worker processes) | `make test-distributed GROUP=polaris+workers` |
| `needs:ranger` | quickstart/polaris-ranger-keycloak | `scripts/access-control-test.sh` |
| `needs:ranger+spark` | the same plus Spark | `scripts/spark-access-control-test.sh` |
| `needs:spark` | docker-compose.spark.yml + iceberg-spark-runtime | by hand |
| `needs:trino` | docker-compose.trino.yml + trino-iceberg-connector | by hand |
| `needs:postgres` | the `postgres` service of docker-compose.test.yml | by hand |
| `needs:hms`, `needs:nessie`, `needs:unity`, `needs:minio` | that catalog's compose stack or credentials | by hand |
| `needs:aws` | live AWS credentials: Glue, S3 bucket, S3 Tables | by hand |
| `bug:#<n>` | known defect with an issue; enable when fixed | never, until fixed |
| `bench` | informational benchmark, not a gate | by hand |
| `deferred` | stretch assertion kept for later | never, until promoted |
| `release-only` | only meaningful in an optimised build | `cargo test --release` |

The vocabulary lives in `xtask/src/test_groups.rs` (`STACKS`, `OTHER_TOKENS`).
Adding a stack is adding a row there; the checker rejects anything else so a
typo cannot create a group of one.

## The tooling

```
cargo xtask test-groups                  # table: group, count, how to run
cargo xtask test-groups --check          # CI: non-zero on a bare or unknown reason
cargo xtask test-groups --names polaris  # test names in a group, one per line
cargo xtask test-groups --json           # the inventory
```

`--check` runs in `ci/aikido-rust-extra.sh`, so a bare `#[ignore]` fails the
merge request. `make test-integration GROUP=<stack>` asks `--names` for the
list and passes it to `cargo test -- --ignored --exact`, so a group is the same
set of tests on every machine and does not drift from the attributes.

The `#[cfg_attr(debug_assertions, ignore = "release-only")]` form is read the
same way; the reason goes inside the `cfg_attr`.

## Still open from finding 807

- A mandatory CI job that runs `GROUP=polaris` against the compose stack. It
  needs a runner with Docker (runner 2433 with the `docker_socket` tag exists
  in the fleet; the shared runners are `docker` executors without a socket).
  Until then the gate for the write path, distributed dispatch and access
  control is `make test-integration` on a laptop, as CLAUDE.md says.
- Finding 411 (#591): the six distributed failure cases behind
  `make test-distributed`.
