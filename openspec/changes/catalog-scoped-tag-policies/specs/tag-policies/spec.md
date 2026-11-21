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
