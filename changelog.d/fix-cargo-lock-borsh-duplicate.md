### Fixed

- `Cargo.lock` on main parses again

    The renovate merge `d7d1dea5` (2026-10-23) produced a lockfile with `borsh` 1.8.1 and `borsh-derive` 1.8.1 listed twice, the second `borsh-derive` pointing at a `syn 3.0.6` entry that does not exist. Every cargo command on main failed with "package `borsh` is specified twice in the lockfile". The stale pair is removed; `cargo metadata --locked` passes.

### Security

- Bump `ctutils` 0.4.3 -> 0.4.5 for RUSTSEC-2026-0385

    `ctutils` (via `digest` -> `hmac`, used by SigV4 signing and SQE's own HMACs) generated branches in its constant-time mask functions before 0.4.4. `cargo deny` and `cargo audit` fail on it.
