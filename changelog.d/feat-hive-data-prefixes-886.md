### Security

- File TVFs no longer read Hive external table data; Hive tables get their own location list (#886)

    Hive table locations were gated by the file TVF allow-list
    (`[storage.tvf] allowed_object_store_prefixes`), so every location a Hive
    table could live at was also a path any user could `read_parquet` /
    `read_csv` / `read_json` with the engine's credentials, bypassing the
    table's row filters and masks. Hive locations now use
    `[hive] allowed_data_prefixes` and a per-root `[[hive.roots]] data_prefixes`
    (same grammar, `{user}` and `{catalog}` placeholders). A file TVF read under
    a Hive data prefix, or under the location of any Hive table the coordinator
    has loaded, is refused for every caller, admins and
    `allow_all_object_stores` included; a call with complete inline credentials
    is not. Directories and globs above the data are refused too. Startup logs
    an error for each TVF prefix that overlaps a Hive prefix.

    Migration: with neither key set, Hive locations keep using the TVF list
    for one release and startup logs a deprecation warning; loaded tables are
    still refused to file TVFs. Existing file TVF reads of Hive table data
    stop working: query the Hive table instead. Loaded-table entries are per
    process, so after a restart a table is protected again from its first
    load; the configured prefixes have no such gap.
