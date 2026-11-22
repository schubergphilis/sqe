## ADDED Requirements

### Requirement: Tag removal authority is configurable
The system SHALL accept `[access_control] tag_removal` with the values `same`, `remove_tag` (alias `remove-tag`) and `admin`, defaulting to `same`. An unrecognised value SHALL be a configuration error. Under `tag_authority = "admin-role"` the setting SHALL have no effect.

#### Scenario: Default unchanged
- **WHEN** `tag_removal` is not set and a user holding the tag privilege on `ns.t.ssn` runs `ALTER TABLE ns.t UNSET TAGS (ssn)` under `ranger-delegate`
- **THEN** the tag is removed

#### Scenario: Typo refused
- **WHEN** `tag_removal = "removetag"`
- **THEN** the configuration fails to load

### Requirement: Removing a tag needs the removal right
Under `ranger-delegate` with `tag_removal = "remove_tag"`, a non-admin statement that removes a tag key from a column SHALL also require Ranger `remove_tag` on that column. With `tag_removal = "admin"`, it SHALL require an admin role. The removal right SHALL be checked in addition to the tag privilege, never instead of it. A refusal SHALL be `403 Forbidden` naming the right and the column, SHALL be logged, and SHALL change nothing.

#### Scenario: Steward may tag but not untag
- **WHEN** `tag_removal = "remove_tag"` and a user holding `apply_tag` but not `remove_tag` on `ssn` runs `UNSET TAGS (ssn)`
- **THEN** the statement is refused with `requires remove_tag on ns.t.ssn`
- **AND** the same user's `SET TAGS (ssn = ('PII'))` is written

#### Scenario: Unset masking policy is a removal, set is not
- **WHEN** the same user runs `ALTER TABLE ns.t ALTER COLUMN ssn UNSET MASKING POLICY`
- **THEN** it is refused before the policy store is consulted
- **AND** `SET MASKING POLICY` is not refused for `remove_tag`

#### Scenario: Raw write dropping a key
- **WHEN** a `SET TBLPROPERTIES ('sqe.column-tags' = ...)` drops a key from a column the user lacks `remove_tag` on
- **THEN** it is refused, while a write that only adds keys or changes a kept key's value is not

#### Scenario: Admin mode
- **WHEN** `tag_removal = "admin"` and a non-admin with every grant runs any removal
- **THEN** it is refused, and an admin's removal is written

#### Scenario: The removal right cannot be resolved
- **WHEN** `tag_removal = "remove_tag"` and the policy store fails to answer whether the user holds `remove_tag` (an unreachable Ranger, a servicedef without the type)
- **THEN** every removal, column or object, in either syntax, is refused as not granted
- **AND** a statement that only adds tags is not affected

#### Scenario: Databricks column syntax
- **WHEN** the removal is written `ALTER TABLE ns.t ALTER COLUMN ssn UNSET TAGS ('PII')`
- **THEN** it takes the same check as `UNSET TAGS (ssn)`

### Requirement: A removal is re-checked under the table lock
The locked re-check of a raw `sqe.column-tags` write SHALL refuse the write when, against the map read under the lock, it removes a key from a column the statement was not authorised to remove from, even when the column was authorised as a change.

#### Scenario: Concurrent tag turns an add into a removal
- **WHEN** a statement is authorised to change `phone` as an add, and before its commit another write tags `phone` with a key the statement's map lacks
- **THEN** the statement is refused and asked to retry

### Requirement: remove_tag is granted only by items that name it
The Ranger store SHALL grant `remove_tag` only through an allow item or deny exception naming it. `all`, `alter`, `apply_tag` and the write types SHALL NOT imply it. A deny naming `remove_tag` or `all` SHALL refuse it.

#### Scenario: Owner with all
- **WHEN** a user's allow item grants `all` on the column
- **THEN** `remove_tag` is not granted

### Requirement: Object tag removal takes the same check
An `ALTER TABLE|SCHEMA ... UNSET TAGS` on the object itself SHALL require `remove_tag` on the object's table-level key under `remove_tag`, and an admin under `admin`. An `ALTER CATALOG ... UNSET TAGS` SHALL require an admin under either setting.
