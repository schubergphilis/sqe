**STATUS 2026-10-26: yanked `clipboard-win` bumped (`fix/yanked-clipboard-win`).**

`cargo deny check advisories` warned `detected yanked crate clipboard-win 5.4.1`. It comes only via `rustyline` 19 -> `sqe-cli` (Windows only). `cargo update -p clipboard-win --precise 5.4.3`: a lockfile-only change; 5.4.3 (2026-10-23) is not yanked.

Gates:
- `cargo deny check advisories`: the yanked warning is gone; only the zstd advisories (RUSTSEC-2026-0391..0410) remain, which fail on main too and get their own branch.
- `cargo check -p sqe-cli`: compiles (the crate is Windows-only, so macOS does not build it).

**NEXT:** zstd advisories via a vendored `zstd` 0.14 shim published as 0.13.4 (`[patch.crates-io]`), dropped when DataFusion 56 ships.
