# feat/keyless-s3-eks

**NEXT:** run the recipe on an EKS cluster with Pod Identity (and once with
IRSA): a TVF read under the allowlist succeeds with no key, a path outside it
is refused, a statement with its own keys uses them.

## Done

- Book recipe "Keyless S3 reads on EKS" in `deployment/kubernetes.md`. No
  engine change: with no static `[storage]` key, `sqe-server` already resolves
  the AWS SDK default chain for the file TVFs and `read_parquet`, gated per
  statement by `allowed_object_store_prefixes`.
- `keyless_bypass_warnings` in `sqe-server/src/main.rs`: a startup warning
  when there is no static key and `allow_all_object_stores` or
  `object_store_admin_roles` is set. Logged after `--allow-all-buckets` is
  applied. It lives in `sqe-server`, not `sqe-core`, because `sqe-core` is at
  its line budget.
- `sqe-server/src/main.rs` references `sqe_catalog::aws_config` in a `const`,
  so a server build without `glue` or `s3tables` fails to compile.

## Out of scope

- Hive roots and ATTACH mounts: they have their own credential settings.
- `sqe-cli`: its release build has no AWS SDK; without a key it reaches
  instance metadata only. The page says so.
- A separate on/off setting for the chain: it would refuse keyless reads that
  work today.

## Tests run

- `cargo test --no-default-features -p sqe-server --bin sqe-server keyless`
  (5 passed; the `const` pin compiled in that release-feature build)
- `cargo clippy --no-default-features -p sqe-server --bin sqe-server --tests -- -D warnings`
- `cargo xtask budgets --check`, `python3 scripts/changelog-collect.py --check`, `make leak-scan`

## Not run

- EKS acceptance (Pod Identity or IRSA).
- The negative build check (dropping `glue` from the server and watching the
  compile fail); checked by reading that `aws_config` is gated on
  `glue`/`s3tables` in `sqe-catalog/src/lib.rs`.
