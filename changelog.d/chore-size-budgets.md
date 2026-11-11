### Added

- Size and structure budgets, checked on every merge request (`cargo xtask budgets`)

    `.config/budgets.json` records a line ceiling per crate, a 3,000-line default per source
    file with a recorded ceiling for the 22 files already over it, the exact first-party
    dependencies of every crate, and ceilings on the `Cargo.lock` package count and on crate
    names locked at more than one version. `cargo xtask budgets --check` runs in the
    `aikido-rust-extra` job in both CI profiles and as a pre-push hook; it counts lines and
    reads `cargo metadata --no-deps`, so it compiles nothing. `--update` only lowers budgets;
    raising one, adding an edge or adding a crate is a hand edit of the file. Ceilings leave
    one to two steps of room (250 lines per file, 1,000 per crate) and the `Cargo.lock`
    ceilings are upper bounds only. Compile time itself is not budgeted here.
