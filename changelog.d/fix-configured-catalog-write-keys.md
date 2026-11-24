### Fixed

- UPDATE, DELETE and MERGE into a named catalog with engine credentials no longer fail as "engine-keyed" (#934)

    A `[catalogs.<name>]` entry with `credentials` reads with engine-fetched keys. The write path built that catalog the same way as SELECT, so UPDATE, DELETE and MERGE (dbt incremental models among them) loaded their target with the engine's read keys, and the write check refused every one of them, also for a person allowed to write. The write path now builds such a catalog on the person's delegated keys, as it already did for the legacy `[catalog]` block. INSERT and CTAS were not affected. Covered by a mock-catalog test.
