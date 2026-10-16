**STATUS 2026-10-16: shared CI pin bumped for the allocator-api2 license evidence (`fix/aikido-ref-allocator-api2`).**

`aikido-license-policy --strict` failed with `allocator-api2@0.2.21: license evidence missing`. The crate has been in `Cargo.lock` since 2026-03; nothing changed on our side. deps.dev returns 404 for the package itself, so the resolver can never fill its license. aikido !227 adds a `reviewed_evidence` entry pinned to 0.2.21, citing `LICENSE-MIT` at the publish commit `63cd7fcc`. This MR moves the five `include` refs and `AIKIDO_SHARED_REF` from `97e6f9e1` to `fb6d9b9d`. The other changes in that range are tool digest promotions and renovate image digests.

Verified: on a one-component SBOM, aikido's `sbom_license_check.py --strict` reports `review=1` with the old policy and `allowed=1` with the new one.

**NEXT:** the `ci-deps` image pin (`.gitlab-ci.yml` `aikido-test-rust` / `aikido-rust-extra`) predates the 2026-10-14 `vendor/` change. Every Rust job therefore rebuilds the vendored crates on top of the image's target dir, which ran a runner out of disk on !1445. Rebuild the image on main (manual `ci-deps-image`) and bump both digests.
