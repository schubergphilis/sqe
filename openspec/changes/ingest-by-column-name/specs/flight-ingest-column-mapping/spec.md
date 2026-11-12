## ADDED Requirements

### Requirement: Ingest matches columns by name
When bulk ingest writes into an existing table and every column of the uploaded Arrow schema resolves to a table column, the server SHALL write each uploaded column into the table column of the same name, in any order. A name resolves by exact match first, then by unique ASCII case-insensitive match.

#### Scenario: Reordered columns
- **WHEN** a table has columns `(id BIGINT, name STRING, amount DOUBLE)` and the upload has `(amount, id, name)`
- **THEN** every row lands with each value in its own column

#### Scenario: Case differs
- **WHEN** the table column is `customer_id` and the upload field is `Customer_ID`
- **THEN** the field is written into `customer_id`

#### Scenario: Ambiguous case-insensitive match
- **WHEN** the table has columns `Id` and `ID` and the upload field is `id`
- **THEN** the request fails with InvalidArgument naming `id`, and no data file is written

#### Scenario: Two upload fields resolve to one column
- **WHEN** the upload has fields `Name` and `name` and the table has only `name`
- **THEN** the request fails with InvalidArgument, and no data file is written

### Requirement: Omitted columns are filled
In by-name mode, a table column the upload leaves out SHALL be written as its Iceberg `write-default`, or NULL when it has none. When an omitted column is required and has no `write-default`, the request SHALL fail before any data file is written, naming the column.

#### Scenario: Omitted nullable column
- **WHEN** the table has an optional `note STRING` with no default and the upload leaves it out
- **THEN** every ingested row has `note` NULL

#### Scenario: Omitted column with a write-default
- **WHEN** the table's `status` column has `write-default 'new'` and the upload leaves it out
- **THEN** every ingested row has `status = 'new'`

#### Scenario: Omitted required column
- **WHEN** the table's `id` column is required with no default and the upload leaves it out
- **THEN** the request fails with InvalidArgument naming `id`, and the table is unchanged

### Requirement: Positional ingest is kept for unnamed uploads
When no uploaded column name resolves to a table column and the column counts are equal, the server SHALL map columns by position, as before.

#### Scenario: Generic names
- **WHEN** a table has three columns and the upload has three fields named `f0`, `f1`, `f2`
- **THEN** the rows are written positionally

### Requirement: Partly matching uploads are refused
When some uploaded names resolve and others do not, or none resolve and the counts differ, the server SHALL fail the request with InvalidArgument before any data file is written. The message SHALL name the unresolved upload columns and list the table's columns.

#### Scenario: One unknown column
- **WHEN** the table has `(id, name)` and the upload has `(id, nmae)`
- **THEN** the request fails naming `nmae`, and the table is unchanged

### Requirement: A table created from the stream is unaffected
When ingest creates the table from the stream schema (`if_not_exist = CREATE` on a missing table, or REPLACE), the table SHALL take the stream's columns in stream order, as before.

#### Scenario: Create from stream
- **WHEN** a client ingests `(b, a)` into a missing table with `(CREATE, FAIL)`
- **THEN** the table is created with columns `(b, a)` in that order
