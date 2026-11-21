### Fixed

- A tag steward can untag a column again (#936)

    Since the column tags were bound to field ids (#920), removing a column's last tag, and the rollback after a failed Ranger projection, removed the `sqe.column-tag-ids` table property. Polaris checks that as `REMOVE_TABLE_PROPERTIES`, which a steward holding only `table-properties-set` lacks, so the untag failed with a 403. Tag writes now only set properties: emptied ids are written as an empty id set bound to the new tag map, and a rollback writes back the values it read, with an absent property restored as its empty form.
