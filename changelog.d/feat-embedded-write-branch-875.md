### Added

- `SET WRITE_BRANCH` in embedded mode (#875)

    `sqe-cli --embedded` now keeps a session write branch, as a cluster session does:
    INSERT, UPDATE, DELETE and MERGE on an Iceberg table read and commit at the branch,
    `main` does not move, a table without the branch is refused, and CTAS is refused while
    a branch is set.

- `RESET WRITE_BRANCH` (#875)

    Same as `SET WRITE_BRANCH = DEFAULT`. The refusals a write branch causes already told
    users to run it, but it did not parse.
