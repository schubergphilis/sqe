### Security

- RUSTSEC-2026-0459 (C SQLite in libsqlite3-sys) recorded as an ignored advisory

    libsqlite3-sys 0.37.0 is only reachable through sqlx 0.9.0, which caps it
    below the fixed 0.39 and has no newer release. No shipped binary links it:
    released builds use the pure-Rust SQLite engine, and the sqlx SQLite
    driver stays behind the opt-in `sqlite-c` / `sql-sqlite` features. The
    ignore has a review date and goes once sqlx accepts libsqlite3-sys 0.39.
