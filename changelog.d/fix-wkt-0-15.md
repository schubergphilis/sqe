### Fixed

- `sqe-geo` builds against `wkt` 0.15

    `wkt` 0.15 changed `Wkt::from_str` to return `wkt::ParseError` instead of
    `&str`, which broke `main` after the Renovate bump. The WKT parser now lets
    the error type be inferred, and the message still reads
    `<function>: invalid WKT: <reason>`.
