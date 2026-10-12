**STATUS 2026-10-12: the dbt-sqe lock installs again (`fix/dbt-sqe-lock-hashes`).**

`pip install --require-hashes -r adapters/dbt-sqe/requirements-lock.txt` failed on main. Renovate's default `pip_requirements` manager matched the lock and edited its pins in place, which caused three faults:

- It kept the previous version's hashes. All nine packages bumped in `1a290d29` failed, and so did `charset-normalizer`, bumped in `3e38da17`. For example, the `agate==1.14.2` entry carried 1.9.1's hashes.
- The bumped pins could not be installed together: `dbt-common` 1.39 needs `agate<1.10`.
- It never added the new transitive dependencies of `dbt-core` 1.12.5 (`metricflow`, `sqlglot`, ...).

The fix:

- The lock is re-resolved from `setup.cfg` by a new `make dbt-sqe-lock`. It deletes the output first, because uv copies hashes from an existing output whenever the version matches; regenerating in place therefore kept the stale ones. `--exclude-newer` (today minus 7 days) applies the repo's release-age floor. Result: 61 packages, every hash matches what PyPI serves, and a clean `pip install --require-hashes` plus `dbt --version` succeeds.
- Renovate: `pip_requirements` is disabled for `**/requirements-lock.txt`. The `pip-compile` manager, which would re-hash, cannot read a `setup.cfg` source: a local Renovate 43 dry-run reported "support for manager is not yet implemented". So the lock is maintained by hand, like `Cargo.lock` with `make lock-maintenance`. `setup-cfg` still bumps the ranges in `setup.cfg` and now carries the 7-day `minimumReleaseAge`. `renovate-config-validator` passes, and a `--dry-run=lookup` shows all 61 lock deps `disabled`.

Versions that moved against the broken pins: importlib-metadata 9.0.1 and pathspec 1.0.4 up; down, as the resolution forces: agate 1.9.1, mashumaro 3.17, pydantic 2.13.5 / pydantic-core 2.46.5, networkx 3.6.1, opentelemetry-api 1.45.0, python-slugify 9.1.2, dbt-protos 1.0.565.

**NEXT:** dbt-sqe DML errors (unfetched INSERT / DELETE results are never read, so failures pass silently and a dropped cursor can cancel the write), then the delete+insert transaction (openspec `single-table-transactions` 5.4).
