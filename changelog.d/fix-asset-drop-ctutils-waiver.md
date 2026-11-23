### Removed

- Expired `reputation-unresolved` waiver for `ctutils` dropped from `.asset`

    The waiver covered ctutils 0.4.5 while deps.dev had not yet indexed it. It
    expired on 2026-11-06 and only produced a warning. `Cargo.lock` now pins
    ctutils 0.4.8, which deps.dev resolves, and the dependency reputation check
    reports no unresolved package without the waiver.
