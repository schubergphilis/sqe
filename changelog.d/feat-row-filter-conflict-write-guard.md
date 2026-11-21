### Changed

- A tag write that would create a row filter conflict is refused under every mask precedence (#931)

    `SET TAGS`, `MODIFY COLUMN ... SET TAG` and a raw `sqe.column-tags` property write now fail with `tag change refused: it would create a row filter conflict on <table>` when the new tag gives some user a second row filter on the table. The write guard used to run only under `mask-precedence = "refuse"`, so in the default `tag` mode such a write was accepted and every later read of the table failed. Mask conflicts are still refused at write time only under `refuse`; the other modes resolve them by precedence when the table is read.
