### Fixed

- A `JSON` column on a V2 table casts to JSON unchanged (#848)

    Before format v3 a `JSON` column is stored as an Iceberg string, so
    `CAST(payload AS JSON)` quoted the text, `"{\"a\":1}"`, where Trino returns
    `{"a":1}`. SQE now lists a V2 table's JSON columns in the `sqe.json-columns`
    table property, and a scan marks those fields as JSON text: the cast returns
    the value canonical, and the Trino protocol types the column `json`.
    CREATE, CTAS, CREATE LIKE and ALTER ADD / RENAME / DROP COLUMN keep the
    property current in the same commit as the schema. A table written before
    the property opts in with `ALTER COLUMN .. SET DATA TYPE JSON`. DESCRIBE and
    `information_schema` still show `varchar`.
