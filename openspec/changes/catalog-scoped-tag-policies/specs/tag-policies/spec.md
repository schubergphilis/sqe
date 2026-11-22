## ADDED Requirements

### Requirement: A catalog-qualified tag policy governs its catalog only
With `catalog_scoped` on, the system SHALL resolve a column tag `t` of a table in catalog `c` against tag policies named `t` and `c.t`. A policy named `c.t` SHALL NOT apply to any other catalog.

#### Scenario: Policy on one catalog
- **WHEN** a mask policy names tag `a.pii` and catalogs `a` and `b` both hold a column tagged `pii`
- **THEN** the column is masked when read from `a` and raw when read from `b`

#### Scenario: Each catalog its own mask
- **WHEN** `a.pii` masks with NULL and `b.pii` masks with a hash
- **THEN** each catalog's column gets its own catalog's mask

### Requirement: Bare tag policies keep applying
With `catalog_scoped` on, a policy on bare tag `t` SHALL still apply in every catalog. When both `t` and `c.t` mask a column, the `c.t` mask SHALL win. Row filters from both SHALL apply. A tag the system cannot map under either name SHALL restrict the column.

#### Scenario: Turning the setting on unmasks nothing
- **WHEN** a mask policy names bare `pii` and `catalog_scoped` is turned on
- **THEN** every catalog's column tagged `pii` stays masked

### Requirement: Off means unchanged
With `catalog_scoped` off, or when a table's catalog is unknown, the system SHALL resolve tags by their bare names only, as before.

#### Scenario: Qualified policy without the setting
- **WHEN** a mask policy names `a.pii` and `catalog_scoped` is off
- **THEN** it masks nothing

### Requirement: Spark sees the same policies
When the system projects column tags into Ranger under `catalog_scoped`, it SHALL write both `t` and `c.t` on the column.

#### Scenario: Projection
- **WHEN** a column of catalog `a` is tagged `pii`
- **THEN** the projected resource carries tags `pii` and `a.pii`

### Requirement: A tag policy written in SQL is keyed to its catalog
With `catalog_scoped` on, `CREATE POLICY ... ON TAG t` with a bare `t` SHALL store the policy on `c.t`, where `c` is the session's catalog. The system SHALL NOT write a bare tag policy from SQL under `catalog_scoped`. A dotted tag SHALL be stored as written.

#### Scenario: Bare tag in SQL
- **WHEN** an admin with session catalog `c` runs `CREATE POLICY p ON TAG pii COLUMN MASK ...`
- **THEN** Ranger holds policy `p` on tag `c.pii`, and no policy on bare `pii`

#### Scenario: Setting off
- **WHEN** the same statement runs with `catalog_scoped` off
- **THEN** the policy is on bare `pii`, as before

### Requirement: Attaching a catalog's tag policy keeps the column tag bare
With `catalog_scoped` on, `SET MASKING POLICY p` for a column of catalog `c`, where `p` is on `c.t`, SHALL tag the column `t`.

#### Scenario: Attach
- **WHEN** policy `p` is on `c.pii` and an admin runs `ALTER TABLE c.s.t MODIFY COLUMN ssn SET MASKING POLICY p`
- **THEN** the column tag is `pii`, the projector writes `pii` and `c.pii`, and `p` masks the column

### Requirement: OR REPLACE never moves a tag policy
`CREATE OR REPLACE POLICY` SHALL refuse when the existing policy of that name is on a different tag than the new statement. The existing policy SHALL be left unchanged.

#### Scenario: Legacy bare policy
- **WHEN** policy `p` is on bare `pii` and an admin with session catalog `c` runs `CREATE OR REPLACE POLICY p ON TAG pii ...` under `catalog_scoped`
- **THEN** the statement is refused and `p` stays on `pii`

#### Scenario: Same tag
- **WHEN** policy `p` is on `c.pii` and the replacement is on `c.pii` with another mask
- **THEN** the policy is replaced

### Requirement: A legacy bare tag policy is never looser
A bare tag policy that still applies in every catalog SHALL be accepted only because it adds restrictions and never removes one. A policy on `c.t` SHALL NOT apply to a column of any catalog `d` other than `c` when that column is tagged `t`.

#### Scenario: Other catalog
- **WHEN** policies on `c.pii` (mask and row filter) exist and a column of catalog `d` is tagged `pii`
- **THEN** no part of the `c.pii` policies applies to it, and a bare `pii` policy still does

#### Scenario: Known exception (#943)
- **WHEN** a holder of `apply_tag` tags a column in catalog `d` with the literal name `c.pii`
- **THEN** `c`'s policy applies to it, and if that mask is weaker than the bare `pii` mask, the catalog's mask wins and the column is looser than under the bare policy alone. This is tracked in #943 and depends on the mask-precedence decision in #942.
