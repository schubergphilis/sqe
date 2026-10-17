## ADDED Requirements

### Requirement: VARIANT column type
The system SHALL accept `VARIANT` as a column type and store it as the Iceberg v3 `variant` type.

#### Scenario: Create a table with a variant column
- **GIVEN** a session with CREATE privilege on the namespace
- **WHEN** the user issues `CREATE TABLE t (id INT, v VARIANT)`
- **THEN** the table is created with Iceberg format-version 3
- **AND** column `v` has the Iceberg type `variant`

#### Scenario: JSON text in, JSON text out
- **GIVEN** a table `t (id INT, v VARIANT)`
- **WHEN** the user inserts `'{"a": 1, "b": [true, null]}'` into `v` and selects it
- **THEN** the value comes back as the JSON text `{"a":1,"b":[true,null]}`
- **AND** Trino clients see the column type `json` and Flight SQL clients `Utf8`

#### Scenario: Invalid JSON is refused
- **GIVEN** a table with a variant column
- **WHEN** the user inserts text that is not valid JSON
- **THEN** the statement fails and no row is written

#### Scenario: Variant on a v2 table is refused
- **GIVEN** a format-version 2 table
- **WHEN** the user issues `ALTER TABLE t ADD COLUMN v VARIANT`
- **THEN** the statement fails with an error saying variant needs format v3

### Requirement: JSON follows the table format version
The system SHALL store a `JSON` column as an Iceberg `variant` on a format-version 3 table and as a string on a format-version 1 or 2 table.

#### Scenario: JSON on a table that asks for v3
- **GIVEN** a session
- **WHEN** the user issues `CREATE TABLE t (j JSON) WITH ('format-version' = '3')`, or the Trino form `WITH (format_version = 3)`, or `TBLPROPERTIES ('format-version' = '3')`
- **THEN** the table is format-version 3
- **AND** column `j` has the Iceberg type `variant`

#### Scenario: JSON beside another v3 column
- **GIVEN** a session
- **WHEN** the user issues `CREATE TABLE t (v VARIANT, j JSON, s STRUCT<x JSON>)`
- **THEN** `j` and `s.x` are both `variant`

#### Scenario: JSON under the default configuration
- **GIVEN** `default_table_format_version` is unset (default 3)
- **WHEN** the user issues `CREATE TABLE t (j JSON)`
- **THEN** the table is format-version 3 and `j` is `variant`

#### Scenario: Requested v2 wins over the v3 default
- **GIVEN** the default format version 3
- **WHEN** the user issues `CREATE TABLE t (j JSON) WITH ('format-version' = '2')`
- **THEN** the table is format-version 2 and `j` is a string column

#### Scenario: JSON on a v2 table stays a string
- **GIVEN** `default_table_format_version = 2` and no format-version option
- **WHEN** the user issues `CREATE TABLE t (j JSON)` and inserts `'not json'`
- **THEN** `j` is a string column and the insert succeeds

#### Scenario: ADD COLUMN JSON on a v3 table
- **GIVEN** a format-version 3 table
- **WHEN** the user issues `ALTER TABLE t ADD COLUMN k JSON`
- **THEN** `k` is `variant`

#### Scenario: Contradicting format version is refused
- **GIVEN** a session
- **WHEN** the user issues `CREATE TABLE t (v VARIANT) WITH ('format-version' = '2')`
- **THEN** the statement fails with an error saying the table needs format v3

#### Scenario: Unknown format version is refused
- **GIVEN** a session
- **WHEN** the user issues `CREATE TABLE t (id INT) WITH ('format-version' = '4')`
- **THEN** the statement fails and names the accepted values 1, 2 and 3

### Requirement: JSON functions read a variant
The system SHALL let every `json*` scalar function take a variant argument as its JSON text.

#### Scenario: Extract from a v3 JSON column
- **GIVEN** a v3 table whose `JSON` column `j` holds `{"a": {"b": 2}, "s": "x"}`
- **WHEN** the user selects `json_extract(j, '$.a')`, `json_extract_scalar(j, '$.s')` and `json_get_int(j, 'a', 'b')`
- **THEN** the results are `{"b":2}`, `x` and `2`, as for the same JSON stored as text

### Requirement: No in-place conversion
The system SHALL NOT change the type of an existing JSON-as-text column; conversion is by copy.

#### Scenario: Convert a text column by INSERT SELECT
- **GIVEN** a v2 table with a VARCHAR column holding JSON text and a v3 table with a VARIANT column
- **WHEN** the user issues `INSERT INTO v3_table SELECT id, doc FROM v2_table`
- **THEN** each text value is parsed into a variant and reads back as the same JSON
