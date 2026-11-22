## ADDED Requirements

### Requirement: The catalog scope has a startup mode
When `[session.catalog_scope]` is configured, the system SHALL read `mode` at startup, with values `binding` (the default), `union` or `off`, and SHALL log the active mode. The mode SHALL NOT be changeable per session.

- In `binding`, the system SHALL behave as the `signed-catalog-scope` requirements state.
- In `union`, the system SHALL still require and verify a scope for the configured client and audit it, and SHALL NOT narrow the session's catalogs or refuse writes because of it.
- In `off`, the system SHALL treat the configured client like any other client and ignore the scope header.

#### Scenario: binding is the default
- **GIVEN** `[session.catalog_scope]` without `mode`
- **WHEN** a runs-client session with a scope of A (`read_write`) and B (`read_only`) reads C
- **THEN** it gets "catalog not found", and writing B is refused

#### Scenario: union verifies but does not narrow
- **GIVEN** `mode = "union"`
- **WHEN** a runs-client session presents a valid scope of A
- **THEN** it can read C, which its grants allow
- **AND** a runs-client session without a scope is refused

#### Scenario: off ignores the scope
- **GIVEN** `mode = "off"`
- **WHEN** a runs-client session presents no scope
- **THEN** it is accepted and sees every catalog its grants allow

### Requirement: Scope refusals name their reason
In `binding`, the system SHALL record `catalog_not_in_workspace` as the audit `error_type` when a statement names a catalog outside the scope. It SHALL record `catalog_read_only` when a statement writes into a read-only catalog. The client SHALL still get "catalog not found" for a catalog outside the scope.

#### Scenario: Audited reasons
- **GIVEN** a scope of A (`read_write`) and B (`read_only`)
- **WHEN** the session reads C and inserts into B
- **THEN** the audit records `catalog_not_in_workspace` and `catalog_read_only`
- **AND** the client sees "Catalog 'c' not found" for C
