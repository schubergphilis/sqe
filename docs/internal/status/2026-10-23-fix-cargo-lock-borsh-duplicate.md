**STATUS 2026-10-23: Cargo.lock on main parses again (`fix/cargo-lock-borsh-duplicate`).**

The renovate merge `d7d1dea5` combined two lockfile sides textually and left `borsh` / `borsh-derive` 1.8.1 in twice (the second `borsh-derive` on a `syn 3.0.6` that is not in the lockfile). Main pipeline #5166 failed in `deps-check`, `aikido-cargo-lock` and every other job that runs cargo. Removed the stale pair; `cargo metadata --locked` passes and cargo leaves the file unchanged. Also bumps `ctutils` 0.4.3 -> 0.4.5 (RUSTSEC-2026-0385), the advisory the pre-commit `cargo deny` / `cargo audit` hooks flagged once the lockfile parsed.

**NEXT:** unchanged
