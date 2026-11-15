### Fixed

- The CI dependency image builds again with Rust 1.100

    Cargo 1.100 puts every build unit in `target/debug/build/<pkg>/<hash>/` and no longer
    creates `target/debug/deps`, so the image build failed after a successful compile and
    test jobs fell back to rebuilding every dependency. The shared CI templates now prune
    both layouts; this pins the template commit with that fix.
