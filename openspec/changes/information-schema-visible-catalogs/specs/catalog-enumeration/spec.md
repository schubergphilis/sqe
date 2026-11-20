## ADDED Requirements

### Requirement: Enumeration lists the catalogs the caller may see
When a statement reads `information_schema`, `SHOW CATALOGS` or `system.jdbc` / `system.metadata`, the system SHALL list every catalog drawn from these sources that has answered the caller's own bearer:
- the configured catalogs;
- the session catalog;
- the catalog scope;
- in an unscoped `polaris-auto` session, the Polaris management catalog list fetched as the caller.

The system SHALL use no service account and no shared credential.

#### Scenario: Scoped session lists its scope
- **GIVEN** `polaris-auto` and a session scoped to catalogs A and B, neither of them configured
- **WHEN** the session reads `information_schema.tables`
- **THEN** it returns rows for tables in both A and B
- **AND** `SHOW CATALOGS` lists A and B

#### Scenario: Management list when the caller may list catalogs
- **GIVEN** an unscoped `polaris-auto` session
- **AND** the caller may list catalogs in Polaris and has read access to catalog C, which is not configured
- **WHEN** the session reads `information_schema.schemata`
- **THEN** C's namespaces are in the result

#### Scenario: Management list denied
- **GIVEN** a caller for whom the management catalog list returns 403
- **WHEN** the session reads `information_schema.tables`
- **THEN** the statement succeeds
- **AND** it lists the configured catalogs, the session catalog and the scope catalogs, as before this change

### Requirement: A catalog the caller cannot reach is absent
The system SHALL leave out every candidate catalog whose probe with the caller's bearer fails, whatever the reason. In that case:
- the statement SHALL NOT fail;
- the catalog's name SHALL NOT appear in any result row;
- the catalog's name SHALL NOT appear in any log line.

A scoped session SHALL NOT list a catalog outside its scope, even when the management list returns that catalog.

#### Scenario: Catalog without a grant
- **GIVEN** the management list returns catalogs C and D
- **AND** the caller has no grant on D
- **WHEN** the session reads `information_schema.schemata`
- **THEN** it lists C's namespaces
- **AND** no row and no log line names D

### Requirement: Enumeration narrows to a pinned catalog
The system SHALL limit an enumeration to one catalog in these cases:
- the statement qualifies `information_schema` with that catalog;
- the statement filters on `table_catalog` or `catalog_name` equal to a literal.

For a pinned catalog, the system SHALL probe, refresh and walk no other catalog.

#### Scenario: Qualified reference
- **GIVEN** catalogs A and B are both visible
- **WHEN** the session reads `A.information_schema.tables`
- **THEN** every row has `table_catalog = 'A'`
- **AND** catalog B is not refreshed

#### Scenario: dbt-style predicate
- **WHEN** the session reads `information_schema.columns WHERE table_catalog = 'B' AND table_schema = 's'`
- **THEN** only catalog B is probed and walked

### Requirement: Enumeration is capped
For an unpinned enumeration, the system SHALL probe at most `[query] catalog_enumeration_max` candidates. When candidates are left out, the system SHALL:
- record a warning that carries counts and no names;
- increment `sqe_catalog_enumeration_truncated_total`.

#### Scenario: Over the cap
- **GIVEN** `catalog_enumeration_max = 2` and four visible candidates
- **WHEN** an unpinned `information_schema.tables` read runs
- **THEN** it lists the tables of at most two catalogs
- **AND** the truncation metric increments

### Requirement: Access-denied tables are skipped while enumerating
During an enumerating read, the system SHALL leave out of the result every table whose load returns access denied. The system SHALL still fail the read on any other load error: a 5xx, an open breaker, or a schema that cannot be converted. A statement that names a table directly SHALL keep reporting access denied for it.

#### Scenario: One denied table
- **GIVEN** namespace N holds tables T1 and T2
- **AND** the caller may list N and load T1, but loading T2 returns 403
- **WHEN** the session reads `information_schema.columns WHERE table_schema = 'N'`
- **THEN** T1's columns are returned and T2 is absent
- **AND** `SELECT * FROM N.T2` fails with access denied

### Requirement: Results are cached per token and scope
The system SHALL cache two things for `[query] catalog_list_ttl_secs`, keyed by the token fingerprint and the scope digest:
- the management-list result;
- negative probe results.

A different token or a different scope SHALL NOT reuse another key's entries.

#### Scenario: Repeat read within the TTL
- **WHEN** the same token reads `information_schema.tables` twice within the TTL
- **THEN** the management list is fetched once
- **AND** a denied catalog is probed once

### Requirement: Static mode and non-enumerating statements are unchanged
With `catalog_discovery = "static"`, or for a statement that does not enumerate, the system SHALL register no additional catalogs and SHALL make no additional Polaris calls.

#### Scenario: SELECT in polaris-auto
- **WHEN** a session runs `SELECT 1`, or a SELECT on a configured catalog's table
- **THEN** no management list call and no probe beyond today's are made
