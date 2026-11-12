### Added

- Writes stamp `SET SNAPSHOT_PROPERTIES` into the snapshot they commit

    INSERT, CTAS, INSERT OVERWRITE, DELETE, UPDATE, MERGE, TRUNCATE and Flight SQL bulk ingest carry the transaction's properties inside a transaction and the session's outside one, in the same commit as their data. A write that changes nothing commits an empty snapshot with the properties, so a loader that copies an empty table still finds its marker. Without properties nothing changes. Maintenance commits never carry them.

- Audit: a data write's record lists the snapshot property keys it carried (OCSF `unmapped.snapshot_property_keys`), never the values

    Records without properties serialize exactly as before, so existing audit hash chains still verify.
