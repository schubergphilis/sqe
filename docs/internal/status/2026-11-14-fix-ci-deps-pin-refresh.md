**STATUS 2026-11-14: CI dependency image pin refreshed again (`fix/ci-deps-pin-refresh`).**

After the dependency refresh merged on 2026-11-14 the `ci-deps-image` job on main rebuilt the image, and the digest pinned in `.gitlab-ci.yml` (`ci-deps:lock-ae6d789dfdd6`) was no longer in the registry. Both Rust jobs on every open merge request then failed at image pull with `manifest unknown`, unlike the silent full rebuild the 2026-11-11 status describes.

The pin now points at `ci-deps:lock-a73c967e88a4` (`sha256:e2965567...`), built by job 5857705 on main from 74575db1 with rustc 1.99.1 and `vendor/` tree 61e1f3d7. Main holds the same `Cargo.lock`, toolchain and `vendor/` today.

Open merge requests need a new pipeline once this lands.

**NEXT:** the #897 stack (!1789) changes `vendor/` (sort order on `ReplaceTableShapeAction`), so the pin goes stale again when it merges: refresh after its `ci-deps-image` run. The automation in the 2026-11-11 status (an MR bot for the digest) is still the real fix; old digests disappearing makes it more urgent.
