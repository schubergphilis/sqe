## ADDED Requirements

### Requirement: One row filter per table and user
When a tag row filter applies to a table for a user, the system SHALL refuse the statement with `row filter conflict on <table>` if more than one distinct row filter (resource or tag, under its bare or catalog-scoped name) resolves for that table and user, under every mask precedence. The same filter from two sources SHALL count once, and the deny-all sentinel SHALL NOT count.

#### Scenario: Tag and resource filter
- **WHEN** a resource row filter `id > 1` and a tag row filter `region = 'EU'` both apply to bob on a table
- **THEN** bob's SELECT fails with `row filter conflict on` and reads no row, whatever `mask-precedence` is

#### Scenario: Write target
- **WHEN** the same two filters apply and bob runs DELETE on the table
- **THEN** the DELETE fails with `row filter conflict on`

#### Scenario: Two tag filters
- **WHEN** two tags on the table each carry a different row filter for the user
- **THEN** the statement is refused

#### Scenario: The same filter twice
- **WHEN** the resource filter and the tag filter are the same expression
- **THEN** it applies once and the statement runs

#### Scenario: Resource filters only
- **WHEN** two resource row-filter policies apply and no tag filter does
- **THEN** both apply, ANDed, as before

## MODIFIED Requirements

### Requirement: Bare tag policies keep applying
With `catalog_scoped` on, a policy on bare tag `t` SHALL still apply in every catalog. When both `t` and `c.t` mask a column, the `c.t` mask SHALL win. When both carry a different row filter, the statement SHALL be refused (one row filter per table and user). A tag the system cannot map under either name SHALL restrict the column.

#### Scenario: Turning the setting on unmasks nothing
- **WHEN** a mask policy names bare `pii` and `catalog_scoped` is turned on
- **THEN** every catalog's column tagged `pii` stays masked
