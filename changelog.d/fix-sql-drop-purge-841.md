### Changed

- `DROP TABLE ... PURGE` on an Iceberg table now fails instead of succeeding without deleting data (#841)

    **Behaviour change.** Before, the statement removed the table from the
    catalog, left every data file in storage and reported success. It is now
    refused with `NotSupported`, and nothing is dropped. A script that drops
    with `PURGE` and then recreates the table stops at the drop, because the
    table is still there. To keep the old effect, drop the `PURGE` keyword.
    To delete the data as well, use
    `CALL system.drop_table(table => ..., purge => true)`. `DROP TABLE`
    without `PURGE` is unchanged, the dbt-sqe adapter never sends `PURGE`,
    and Hive tables already refused it. The DDL reference no longer claims
    `PURGE` deletes data files.
