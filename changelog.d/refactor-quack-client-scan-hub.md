### Changed

- `QuackTableProvider::scan` builds its plan synchronously (compile time only)

    `scan` was an `#[async_trait]` method that held `&dyn Session` and `&[Expr]` across an
    await, so rustc proved `Expr: Sync` for the whole crate. It is now written in
    `async_trait`'s desugared form and returns a ready future built from
    `MemorySourceConfig` + `DataSourceExec`, the same plan `MemTable::scan` produced.
    `scripts/compile-profile.sh sqe-quack-client`, two alternating runs each: `evaluate_obligation`
    self time 3.27 s / 3.20 s before, 15 ms / 15 ms after; crate pass time 3.6 s / 3.5 s before,
    about 0.36 s after. No runtime behaviour change.
