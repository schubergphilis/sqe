### Added

- `ALTER CATALOG c SET|UNSET TAGS ('k' = 'v', ...)` tags a catalog (#922)

    Catalog tags are stored in the Polaris catalog property
    `sqe.catalog-tags`, in the column-tag JSON format, and written through the
    Polaris management API with the caller's token. Polaris authorizes the
    update (`UPDATE_CATALOG`), so only catalog admins can write, as Unity
    Catalog asks for ownership or `MANAGE`. The write sends back every other
    catalog property and the entity version it read, and retries when another
    writer got there first. Under `tag_authority = "admin-role"` only SQE
    admins pass; `tag_names` applies to the keys a `SET` adds. A catalog that
    is not Polaris REST, and a Hive catalog, are refused. Like table and
    schema tags, catalog tags are metadata only; tags inheriting into policy
    is tracked in #930.
