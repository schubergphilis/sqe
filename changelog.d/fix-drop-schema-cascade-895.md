### Fixed

- `DROP SCHEMA ... CASCADE` drops the schema's views and tables; `DROP SCHEMA a, b` drops both (#895)

    CASCADE was ignored, so a schema holding tables failed with the catalog's
    409 "Namespace is not empty". It now drops each view, then each table
    (a catalog drop, never a purge), with the caller's own token, so the
    catalog checks the drop privilege per object, then the schema. Everything
    is listed first: a schema with child schemas is refused before anything
    is dropped. The cascade is not atomic; a failure part-way names what was
    dropped and what is still there. Without CASCADE, a non-empty schema now
    says so and points at CASCADE. A multi-name `DROP SCHEMA` used to drop
    only the first name. The query result cache is cleared after a DROP
    SCHEMA.
