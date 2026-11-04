### Fixed

- `remove_orphan_files` and the file cleanup of `expire_snapshots` work on a credential-vending catalog

    Both commit first (a table property, or the expiry) and then read or
    delete files. They did that through the table the commit returned, but a
    REST commit's response carries no storage config, so that table had only
    the catalog's own storage properties: with vended credentials and no
    static warehouse key, a deleting `remove_orphan_files` failed with
    "failed to load signing credential" and an expiry's cleanup could not
    delete. The file step now runs on a reload of the table, which vends the
    caller's credential. If another writer committed in between, the file
    step is refused with `COMMIT_CONFLICT` and nothing is deleted.
