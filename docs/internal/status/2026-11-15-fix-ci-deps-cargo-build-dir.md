**STATUS 2026-11-15: CI dependency image builds on cargo 1.100's build-dir layout (`fix/ci-deps-cargo-build-dir`).**

After the Rust 1.100.0 merge (!1796), `ci-deps-image` on main (job 5862674, dcd1e310b) compiled in 10m32s and then failed on `find: '/opt/sqe-deps/target/debug/deps': No such file or directory`. Cargo 1.100 writes each unit to `target/debug/build/<pkg>/<hash>/{out,fingerprint,run}`; test binaries land in `out/`, and `debug/deps` is gone. No image was published, so the Rust jobs still run on the 1.99.1 image (`sha256:306ae4af...`) and rebuild every dependency.

Fix in the shared templates (aikido !262, af7e72c): the strip step also covers `debug/build/*/*/out` (keeping `build_script_*`), and `prune-workspace-artifacts.sh` removes a member's `debug/build/<name>` by exact name when `debug/deps` is absent. This branch moves all six template pins from 3911cfb to af7e72c, which also brings in the promoted tools and Rust image digests and the license-check change that landed on aikido main since 3911cfb.

Trial: the manual `ci-deps-image` job on this MR publishes `ci-deps:mr-<iid>` only.

**NEXT:** after both merges, play `ci-deps-image` on main and pin its digest in `aikido-test-rust` and `aikido-rust-extra`.
