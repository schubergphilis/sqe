### Added

- `DROP POLICY [IF EXISTS] ON TABLE <t> FROM USER | ROLE | GROUP <name>` removes one grantee's column masks and row filter on a table

    Masks and row filters written by `GRANT SELECT ... MASKED WITH / ROWS WHERE` belong to the grantee and the table. `REVOKE` leaves them, because the grantee may still read the table another way. A later plain `GRANT SELECT` therefore reads through them again. This statement is the explicit way to remove them. It is admin-only and leaves the privilege and other grantees' items alone. It edits only the `sqe-grant-*` policies SQE created, deleting one that ends up with no items. A mask or filter policy written by hand in Ranger is never edited, even if it names the grantee.
