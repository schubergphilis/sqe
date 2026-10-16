### Changed

- `DROP TABLE ... PURGE` on an Iceberg table now deletes the table's data files (#841)

    **Behaviour change.** Before, the statement removed the table from the
    catalog, left every data file in storage and reported success. It now
    runs the same purge as `CALL system.drop_table(table => ..., purge =>
    true)`: the catalog drop, then every file under the table's location.
    Like the CALL, it needs an admin role (`auth.admin_roles`, default
    `service_admin` and `catalog_admin`); a caller without one is refused
    with `AccessDenied` and nothing is dropped, so a job that ran
    `DROP TABLE ... PURGE` under a non-admin role now fails instead of
    keeping the files. It is also refused, with nothing dropped, when
    another table shares the location. `DROP TABLE` without `PURGE` is
    unchanged, the dbt-sqe adapter never sends `PURGE`, and Hive tables
    still refuse it.
