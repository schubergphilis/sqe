**STATUS 2026-11-12: TVF prefix / Iceberg table overlap guard specified (`docs/tvf-prefix-overlap-spec`, openspec `tvf-prefix-table-overlap`).**

A copy job asked to `read_parquet` its staging files under the catalog's warehouse with no config change. File TVFs use the engine's storage credentials, so a prefix over table files bypasses policy. The approved alternative was a per-catalog staging prefix. It already exists as one global line, `s3://<bucket>/{catalog}/{user}/`, which works for discovered catalogs under the existing access proof. The change adds the missing guard instead of a new key.

Main decisions:
- Every allowed prefix is guarded against Iceberg table locations, with no new `staging_prefix` key.
- Creating an Iceberg table inside a prefix is refused before data is written.
- A loaded table that overlaps a prefix is recorded, and TVF reads under it are refused. Admin roles are refused too; inline credentials are exempt.
- An ancestor probe for `metadata/*.metadata.json` catches Iceberg tables SQE has never loaded.
- There is no catalog write-access probe. The `{user}` segment scopes the folder, and prefixes without `{user}` get a startup warning.
- Hive roots must be allowed prefixes, so they are out of scope; follow-up in #886.

The production guide recommended `s3://your-warehouse/` as a TVF prefix. That example is fixed in this MR.

**NEXT:** task 1.1, check whether the file TVF glob listing sees each object key; then #886.
