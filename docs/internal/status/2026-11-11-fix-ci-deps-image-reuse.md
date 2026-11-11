**STATUS 2026-11-11: CI dependency image pin refreshed (`fix/ci-deps-image-reuse`).**

The two Rust jobs on every merge request start from a `ci-deps` image that carries a compiled third-party `target/`. The pin in `.gitlab-ci.yml` dated from 2026-10-01 and pointed at an image built with rustc 1.98.1. The toolchain moved to 1.99.1 on 2026-10-18. Cargo reuses a baked crate only when rustc, package id and features match, so from that day both jobs rebuilt all 679 crates from `proc-macro2` and nothing turned red: the job log printed `vendor/ changed (9db9e713 -> 4b5d3bb0)` and carried on.

Measured on 2026-11-11 before the change: `aikido-test-rust` compile phase 11m50 on a fast runner and 24m09 on a slow one, of an 18 to 37 minute merge-request pipeline.

The pin now points at `ci-deps:lock-ae6d789dfdd6`, built by the `ci-deps-image` job on main from 1c639da2 with rustc 1.99.1 and `vendor/` tree 4b5d3bb0, which is what main holds today.

**NEXT:** make the refresh automatic. The `ci-deps-image` job rebuilds the image on main whenever the lock, toolchain or `vendor/` changes, but the reviewed digest pin is still moved by hand. Two things are needed: the image job should skip when its inputs are unchanged (today a `.gitlab-ci.yml` edit rebuilds it and moves the tag to a new digest for the same inputs), and a job on main should open a merge request with the new digest, which needs a project access token with API scope as a CI variable. Until then a consumer-side check can compare `rust-toolchain.toml` with the baked `.rustc_info.json` and say so loudly.
