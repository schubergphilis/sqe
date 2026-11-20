### Fixed

- Crates inherit the workspace `homepage` and `authors` fields

    `[workspace.package]` declared `homepage` and `authors`, but no crate
    inherited them, so every cargo run warned `unused field`. The 26 SQE
    crates now take both, which also puts them in `cargo metadata` and SBOMs.
