### Fixed

- `clipboard-win` 5.4.1, which was yanked, is now 5.4.3

    `cargo deny` warned about the yanked crate. It comes in only through
    `rustyline` in `sqe-cli` (Windows clipboard support). Lockfile change only.
