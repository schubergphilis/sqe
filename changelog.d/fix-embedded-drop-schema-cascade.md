### Fixed

- `DROP SCHEMA ... CASCADE` works on an Iceberg catalog attached to the embedded CLI

    The embedded CLI refused CASCADE with "not implemented". It now drops
    the schema's views (when the catalog has views) and tables, then the
    schema, as a coordinator does since #895. Everything is listed first,
    so a schema with child schemas is refused before anything is dropped.
    Table drops never purge data files. The cascade is not atomic: a failure
    part-way names what was dropped and what is still there.
