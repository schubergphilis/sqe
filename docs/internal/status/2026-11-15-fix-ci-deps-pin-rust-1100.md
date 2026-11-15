**STATUS 2026-11-15: CI dependency image pin moves to the Rust 1.100 build (`fix/ci-deps-pin-rust-1100`).**

With the cargo 1.100 build-dir fix in the shared templates (aikido !262, sqlengine !1801) merged, `ci-deps-image` on main (job 5863372, 87a6f695) built and pushed `ci-deps:lock-eda03d32d84d`, digest `sha256:db44c576...` (rustc 1.100.0, `vendor/` tree 2dc7f629). `aikido-test-rust` and `aikido-rust-extra` now pin it instead of the 1.99.1 image `sha256:306ae4af...`.

**NEXT:** key the `lock-*` tag on `vendor/` and the toolchain as well as `Cargo.lock`, then automate the digest refresh.
