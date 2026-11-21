## ADDED Requirements

### Requirement: Tables and schemas carry tags
The system SHALL accept `ALTER TABLE <t> SET TAGS ('k' = 'v', ...)`, `ALTER TABLE <t> UNSET TAGS ('k', ...)`, and the same two forms on `ALTER SCHEMA` and `ALTER DATABASE`. A tag without a value, or with `''` as its value, SHALL be stored as a key only. SET SHALL add the named keys, or replace their values. UNSET SHALL remove the named keys whatever their value. Table tags SHALL be stored in the table property `sqe.table-tags`, and schema tags in the namespace property `sqe.schema-tags`. Both hold a JSON list of entries in the `sqe.column-tags` format.

#### Scenario: Table tag with a value
- **WHEN** an admin runs `ALTER TABLE ns.t SET TAGS ('owner' = 'finance', 'gold')`
- **THEN** `sqe.table-tags` on `ns.t` is `[{"owner":"finance"},"gold"]`

#### Scenario: Replace a value
- **WHEN** `ns.t` carries `owner = finance` and an admin sets `('owner' = 'risk')`
- **THEN** `ns.t` carries `owner = risk` once

#### Scenario: Unset the last tag
- **WHEN** an admin unsets every tag of `ns.t`
- **THEN** `sqe.table-tags` is `[]` and the table has no tags

#### Scenario: Schema tag
- **WHEN** an admin runs `ALTER SCHEMA ns SET TAGS ('domain' = 'sales')`
- **THEN** the namespace `ns` carries the property `sqe.schema-tags` = `[{"domain":"sales"}]`
- **AND** its other properties are unchanged

### Requirement: The column forms keep their meaning
A tag list that starts with an identifier SHALL be read as the native column form. A list that starts with a string literal SHALL be read as object tags. A list that mixes the two SHALL be refused without writing anything.

#### Scenario: Mixed list
- **WHEN** a user runs `ALTER TABLE t SET TAGS ('k' = 'v', email = ('pii'))`
- **THEN** the statement fails and no tag changes

### Requirement: Catalog tags are a Polaris catalog property
`ALTER CATALOG <c> SET TAGS (...)` and `UNSET TAGS (...)` SHALL read and write the property `sqe.catalog-tags` of the Polaris catalog through the management API, with the caller's token and the entity version it read. Every other catalog property SHALL be written back unchanged. A version conflict SHALL re-read and retry; a Polaris denial SHALL be returned as the error. A catalog that is not Polaris REST, and a Hive catalog, SHALL be refused.

#### Scenario: A catalog admin tags a catalog
- **WHEN** a principal with catalog-properties write on `sales_wh` runs `ALTER CATALOG sales_wh SET TAGS ('region' = 'eu')`
- **THEN** the catalog carries `sqe.catalog-tags` = `[{"region":"eu"}]`
- **AND** its other properties are unchanged

#### Scenario: A reader cannot tag a catalog
- **WHEN** a principal without catalog-properties write runs the same statement under `ranger-delegate`
- **THEN** Polaris refuses the update and the statement fails with its denial

### Requirement: Object tags are metadata only
Object tags SHALL NOT drive masks or row filters, and SHALL NOT be projected into the Ranger tag store.

#### Scenario: A table tagged like a masked column
- **WHEN** a tag policy masks `pii` and the table `ns.t` is tagged `pii`
- **THEN** a query of `ns.t` returns the same values as before the tag

### Requirement: Object tag writes follow the tag authority
Admins SHALL always pass. Under `tag_authority = "admin-role"`, or with no policy store, every other user SHALL be refused. Under `ranger-delegate`, a table tag write SHALL require the configured `tag_privilege` on the table. A schema tag write SHALL require it on the database's table `*`. A store error SHALL grant nothing. Under `tag_names = "ranger-tagdefs"`, a non-admin write SHALL be refused when a key it adds has no Ranger tag definition, under the same rules as column tags.

#### Scenario: Steward with alter on the table
- **WHEN** `tag_authority = "ranger-delegate"` and a user holding `alter` on `ns.t` sets a tag on `ns.t`
- **THEN** the tag is written

#### Scenario: Column grant is not a table grant
- **WHEN** the user holds `alter` on `ns.t.email` only
- **THEN** a table tag write on `ns.t` is refused with `403 Forbidden`

#### Scenario: Schema needs a database-wide grant
- **WHEN** the user holds `alter` on `ns.t` but on no item whose table resource is `*`
- **THEN** `ALTER SCHEMA ns SET TAGS (...)` is refused

### Requirement: The table tag property has no back door
A statement that writes `sqe.table-tags`, in any case and through any spelling (`SET TBLPROPERTIES`, Trino `extra_properties`, `CREATE [OR REPLACE] TABLE ... TBLPROPERTIES`), SHALL be refused unless the user is an admin.

#### Scenario: Raw write by a steward
- **WHEN** a user who may tag `ns.t` runs `ALTER TABLE ns.t SET TBLPROPERTIES ('sqe.table-tags' = '["x"]')`
- **THEN** the statement is refused and the property is unchanged
