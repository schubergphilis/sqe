## ADDED Requirements

### Requirement: The tag privilege is configurable
The system SHALL accept `[access_control] tag_privilege` with the values `alter` and `apply_tag` (alias `apply-tag`), defaulting to `alter`. An unrecognised value SHALL be a configuration error. Under `tag_authority = "ranger-delegate"`, a column tag write SHALL require the configured privilege on each column it changes. Under `admin-role` the setting SHALL have no effect.

#### Scenario: Default unchanged
- **WHEN** `tag_privilege` is not set and a user holding `alter` on `ns.t.ssn` runs `ALTER TABLE ns.t SET TAGS (ssn = ('PII'))` under `ranger-delegate`
- **THEN** the tag is written, as in 0.55

#### Scenario: Typo refused
- **WHEN** `tag_privilege = "applytag"`
- **THEN** the configuration fails to load

### Requirement: apply_tag is granted only by items that name it
The system SHALL grant `apply_tag` on a column only through a Ranger allow item (or deny exception) whose accesses name `apply_tag`. `alter`, write types and `all` SHALL NOT imply it. Deny items and allow exceptions naming `apply_tag` or `all` SHALL refuse it, and a deny SHALL win over an allow.

#### Scenario: Writer without apply_tag
- **WHEN** `tag_privilege = "apply_tag"` and a user holding `alter`, `update` and `all` on the column, but not `apply_tag`, tags it
- **THEN** the statement is refused with `403 Forbidden` naming `apply_tag` and the column, an audit record is written, and nothing changes

#### Scenario: Steward with apply_tag only
- **WHEN** a user granted only `apply_tag` on `ns.t.ssn` tags `ssn`
- **THEN** the tag is written and the mask applies on the next query
- **AND** the same user's `INSERT INTO ns.t` is refused

#### Scenario: Deny on all wins
- **WHEN** a user holds `apply_tag` on the column and a deny item for `all` covers it
- **THEN** the tag write is refused

### Requirement: Every writer of sqe.column-tags uses the same check
Under `ranger-delegate`, the system SHALL require the configured privilege on the columns a statement changes, for every statement that writes `sqe.column-tags`:
- `ALTER TABLE ... SET TBLPROPERTIES` and Trino `extra_properties`: the columns whose tag sets differ between the live map and the new one, removed columns included;
- `CREATE TABLE ... TBLPROPERTIES`: every column the map tags;
- `CREATE OR REPLACE TABLE ... TBLPROPERTIES`: the columns that differ from the live table, or every tagged column when it does not exist.
A statement whose value is not a JSON object of column to tag list, that spells the key in another case, or that would overwrite a live value that does not parse SHALL be refused unless the user is an admin. Admins SHALL always pass.

#### Scenario: Raw write touching one granted column
- **WHEN** a user granted the privilege on `ssn` only runs `SET TBLPROPERTIES` with a map that differs from the live one only in `ssn`
- **THEN** the map is written

#### Scenario: Raw write removing another column's tags
- **WHEN** the same user's map drops the tags of `email`
- **THEN** the statement is refused naming `email`, and nothing is written

#### Scenario: Concurrent change between check and write
- **WHEN** another write changes a column's tags after the check and before the `SET TBLPROPERTIES` commits, so the diff now reaches a column the check did not authorise
- **THEN** the statement is refused and asks to retry

#### Scenario: extra_properties
- **WHEN** a non-admin writes `sqe.column-tags` through `extra_properties` under `admin-role`
- **THEN** the statement is refused, as the plain key is

### Requirement: admin-role keeps the 0.55 gate
Under `tag_authority = "admin-role"` the system SHALL require an admin role for `SET TBLPROPERTIES` and `CREATE OR REPLACE` writing `sqe.column-tags`, and SHALL NOT gate a plain `CREATE TABLE`.

#### Scenario: SET TBLPROPERTIES by a non-admin
- **WHEN** a non-admin runs `ALTER TABLE t SET TBLPROPERTIES ('sqe.column-tags' = '{}')` under `admin-role`
- **THEN** the statement is refused with `403 Forbidden`

### Requirement: A tag write needs USE on the schema first
Under `ranger-delegate`, before any tag privilege is checked, the system SHALL require a non-admin writing column, table or schema tags to be able to use the schema the object sits in (#941). USE is a Ranger `select` (or `all`) allow on any table of that schema, decided as reads are: a `select` deny on the whole schema (`table: *`, every column) SHALL refuse whatever allows exist, while a deny on one table or column SHALL NOT. `apply_tag`, `remove_tag` and `alter` alone SHALL NOT count. Under `catalog_scoped` the schema key carries the catalog, so the check covers the catalog too; without it the catalog has no Ranger resource and Polaris decides it when the write loads the table or namespace on the caller's token. Catalog tags SHALL keep their own gate. A store that cannot answer SHALL refuse. Admins SHALL pass, and `admin-role` SHALL NOT ask.

#### Scenario: Steward with apply_tag only
- **WHEN** a user holds `apply_tag` on a column but no `select` anywhere in its schema
- **THEN** every tag write on that table is refused with `requires select (USE) on schema <ns>`

#### Scenario: select on another schema
- **WHEN** a user holds `select` on a table in another schema only
- **THEN** the tag write is refused

#### Scenario: Schema-wide deny
- **WHEN** a user holds `select` on a table and a `select` deny on the whole schema
- **THEN** the tag write is refused

#### Scenario: Ranger unreachable
- **WHEN** the USE check fails at the store
- **THEN** the tag write is refused
