**STATUS 2026-10-14: rustls 0.23.45 -> 0.23.46 for RUSTSEC-2026-0346 to -0349 (`fix/rustls-advisories-2026-10-14`).**

The four advisories were published on 2026-10-14 and turned `deps-check` red on every open MR (`advisories FAILED`). `cargo update -p rustls` changes only `Cargo.lock`. `cargo deny check advisories` is ok, `cargo hakari generate --diff` is clean and the `sqe-server` bins check.

**NEXT:** unchanged
