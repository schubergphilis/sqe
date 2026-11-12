**STATUS 2026-11-12: ingest by column name specified (`docs/ingest-by-column-name-spec`, openspec `ingest-by-column-name`).**

The copy-job probe found that DoPut needs every column in table order. Ingest is positional: `apply_stamped_schema` checks arity and casts column `i` to field `i`. The change maps by name when every upload name resolves. Omitted columns get their `write-default` or NULL, and an omitted required column is refused. Uploads where no name resolves stay positional. Partly matching uploads are refused, which is the one BREAKING edge. Everything is decided from the peeked stream schema before any file is written. No code changed.

**NEXT:** task 1.1 (`ColumnMapping::build` with unit tests).
