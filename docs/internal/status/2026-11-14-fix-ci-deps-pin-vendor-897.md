**STATUS 2026-11-14: CI dependency image pin follows the #897 vendor change (`fix/ci-deps-pin-vendor-897`).**

!1789 (#897) changed `vendor/iceberg-rust` (`ReplaceTableShapeAction::with_sort_order`). The `ci-deps-image` job on main (job 5858604) rebuilt the image from de95a8ed with rustc 1.99.1 and `vendor/` tree 2dc7f629: digest `sha256:306ae4af...`. Both Rust jobs now pin it.

Finding: this build and the one !1794 pinned (`sha256:e2965567...`, `vendor/` tree 61e1f3d7) carry the same tag, `ci-deps:lock-a73c967e88a4`; only `vendor/` differs between their source commits (74575db1, de95a8ed). The tag does not follow `vendor/`, so it is moved to the new build and names no single image. The comment in `.gitlab-ci.yml` now gives the digest next to the tag.

**NEXT:** have the shared `.aikido-rust-deps-image` template key the tag on `vendor/` and the toolchain as well as `Cargo.lock`; then automate the digest refresh (2026-11-11 status).
