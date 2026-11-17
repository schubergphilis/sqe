# fix/sast-release-script-uname

**Date:** 2026-11-17

The `aikido-sast` job reported `hooks-dns-exfiltration-generic` (high) at `scripts/ci-release-binaries.sh:41`. That line prints `uname -m` to stderr when the host is not x86_64, and it makes no network call. It now carries a `nosemgrep` marker with that reason, the same way `scripts/benchmark-test.sh` does.

The other high finding in that run, a JWT literal in `crates/sqe-coordinator/tests/it/flight_sql_smoke.rs`, is marked on its own branch (`feat/flight-bearer-handshake`).

Left open on main, and not part of this change:

- `deps-check` fails on three new unmaintained advisories: RUSTSEC-2026-0543 (chrono), RUSTSEC-2026-0555 (chrono-tz) and RUSTSEC-2026-0542 (humantime).
- `aikido-sast` still blocks on its 15 lower-severity findings.

Gate: `shellcheck scripts/ci-release-binaries.sh` and `pre-commit run`. This is a comment-only change, so no build was needed.

**NEXT:** after merge, check that the main pipeline's `aikido-sast` no longer lists this file.
