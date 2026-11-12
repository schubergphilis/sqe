### Security

- Production guide no longer recommends the warehouse bucket as a file TVF prefix

    File TVFs read with the engine's storage credentials, so `allowed_object_store_prefixes = ["s3://your-warehouse/"]` let any caller read every table's files raw, without row filters or masks. The examples now use a per-user landing prefix, and the guide says a TVF prefix never covers a warehouse. Deployments that copied the old example should replace that entry.

### Added

- OpenSpec `tvf-prefix-table-overlap`: guard file TVF prefixes against Iceberg table locations

    Refuses Iceberg tables created inside an allowed prefix, refuses TVF reads under a loaded table that overlaps one, and probes for Iceberg metadata above a TVF path. It documents `s3://<bucket>/{catalog}/{user}/` as the per-catalog staging area. Hive roots are out of scope (#886). Spec only, no code.
