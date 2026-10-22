### Added

- `ALTER TABLE ... SET | DROP IDENTIFIER FIELDS`: SQE can declare an Iceberg table's row key itself

    Merge-on-read UPDATE and MERGE write equality deletes only on a table with identifier fields; without them they fall back to copy-on-write. SQE had no way to declare them, so a table needed Spark first, and the `write.identifier-field-ids` / `identifier_field_ids` table properties some tests used were never read: those tests ran copy-on-write. The new statement takes Spark's syntax (parentheses optional, `s.x` for a struct member), commits a new schema with the same fields, and reuses an existing schema when the set returns to an earlier one. A nullable, floating-point or unknown column is refused, and so is a DROP of a column that is not an identifier field. The four tests that set the dead property now use the DDL and assert `added-equality-delete-files`.
