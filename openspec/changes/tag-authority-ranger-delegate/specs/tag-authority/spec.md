## ADDED Requirements

### Requirement: Tag authority defaults to the admin role
The system SHALL accept `[access_control] tag_authority` with the values `admin-role` and `ranger-delegate`, defaulting to `admin-role`. An unrecognised value SHALL be a configuration error. Under `admin-role`, SET/UNSET TAGS and SET/UNSET MASKING POLICY SHALL require an admin role exactly as before.

#### Scenario: Default unchanged
- **WHEN** `tag_authority` is not set and a non-admin runs `ALTER TABLE t UNSET TAGS (ssn)`
- **THEN** the statement is refused with `403 Forbidden` and no tag is written

### Requirement: Under ranger-delegate, a column alter grant authorises tagging that column
With `tag_authority = "ranger-delegate"`, the system SHALL let a non-admin SET or UNSET TAGS (and SET/UNSET MASKING POLICY) on a column when the Ranger access policies grant that user `alter` on that column, evaluated deny-first, with wildcards and exclusions on the `column` resource honoured. Every column of the statement SHALL pass before any write. Admins SHALL always pass.

#### Scenario: Column steward tags their column
- **WHEN** a user granted `alter` on `ns.t.ssn` only runs `ALTER TABLE ns.t SET TAGS (ssn = 'PII')`
- **THEN** the tag is written and the next query applies the tag's mask

#### Scenario: Grant on another column
- **WHEN** the same user tags `ns.t.email`
- **THEN** the statement is refused with `403 Forbidden`, and nothing is written

#### Scenario: Deny on the column
- **WHEN** a deny item for the user covers `ns.t.ssn` (or `*`), whatever the allow items say
- **THEN** the statement is refused

#### Scenario: Refusal is audited
- **WHEN** a statement is refused by the tag gate
- **THEN** an audit record for the failed statement is written

### Requirement: The checked resource is the written resource
The system SHALL key the check with the same catalog, namespace and table the write resolves, including a bare table name under `USE`, and with the store's `catalog_scoped` rule. A reference that cannot be keyed SHALL be refused.

#### Scenario: Bare name under USE
- **WHEN** a user granted `alter` only in namespace `a` runs `USE b` and then `ALTER TABLE t SET TAGS (...)`
- **THEN** the check is made against `b.t` and the statement is refused

### Requirement: Backends without a column privilege model grant nothing
A policy store that does not implement column privileges SHALL answer "not granted", so under `ranger-delegate` only admins can write tags with that backend. A store error SHALL be treated as not granted.
