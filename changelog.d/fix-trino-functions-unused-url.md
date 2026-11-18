### Fixed

- Main's `deps-check` passes again

    `sqe-trino-functions` still declared the `url` crate after the URL functions
    moved to their own parser, and cargo-machete failed the job on main.
