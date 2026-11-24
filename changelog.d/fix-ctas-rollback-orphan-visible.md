### Changed

- A failed CTAS names the table when its rollback cannot remove the catalog entry

    The rollback drops the new entry as the caller. When the drop is refused, the statement's error now says that the entry for that table stays registered with no data. The client and the failed query's audit event (`message`) both carry it; before, only the coordinator log did. The error code is unchanged.
