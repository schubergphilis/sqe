### Added

- `[access_control] tag_names = "ranger-tagdefs"` refuses a tag name Ranger has
  no tag definition for (#926)

    A non-admin `SET TAGS` / `SET TAG`, and the tags a `CREATE TABLE` or a
    `sqe.column-tags` property write adds, fail with `tag 'x' is not defined
    in Ranger` when `GET /service/tags/tagdefs` does not list the name. The
    list is cached for `[policy.ranger] cache_ttl_secs` and re-read once
    before a name is refused. If it cannot be read the write is refused
    (fail closed). Under `catalog_scoped`, a definition `<catalog>.<tag>` also
    accepts `<tag>` on a table of that catalog. Admins are exempt, removing a
    tag is never refused, and the default `any` keeps the old behaviour.

### Fixed

- `ALTER TABLE t SET TAGS (col = ('k' = 'v'))` is refused instead of storing
  the tag name `k' = 'v`

    The native list form takes tag names only. A value there was unquoted into
    a junk key and stored without complaint. The error points to
    `ALTER TABLE t ALTER COLUMN col SET TAGS ('k' = 'v')`, which stores the
    value.
