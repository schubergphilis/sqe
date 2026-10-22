**STATUS 2026-10-21: dependency advisories (`fix/deps-emsign-certifi`).**

Three advisories published on 2026-10-20/21 failed `deps-check` and
`aikido-scan` on every open MR (!1496-!1502); main passed its last pipeline
only because it ran before they were published.

- `Cargo.lock`: `rustls-webpki` 0.103.15 -> 0.103.16 (RUSTSEC-2026-0382),
  `webpki-root-certs` 1.0.9 -> 1.0.10 (RUSTSEC-2026-0383), `webpki-roots`
  1.0.9 -> 1.0.10 (RUSTSEC-2026-0381; CI's cargo-deny run did not report it
  yet, the pre-commit `cargo audit` did). Lockfile only;
  `cargo deny check advisories` and `cargo audit` -> ok.
- `adapters/dbt-sqe/requirements-lock.txt`: `certifi` 2026.7.22 ->
  2026.10.19 (GHSA-6q48-6c7w-9gw2, critical in grype). `make dbt-sqe-lock`
  has a 7-day release-age floor that excludes the fixed release, so the lock
  was updated with `uv pip compile --upgrade-package certifi
  --exclude-newer 2026-10-21` against the existing file: only `certifi`
  moved.

**NEXT:** rebase the open MRs on main once this merges so their pipelines
pick up the fixed lockfiles.
