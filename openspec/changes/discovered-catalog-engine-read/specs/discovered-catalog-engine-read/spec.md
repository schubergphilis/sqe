## ADDED Requirements

### Requirement: Discovered warehouses read with the person's keys unless opted in
With `[query] discovery_engine_read` unset or `"off"`, a warehouse reached through polaris-auto discovery SHALL be read with the person's delegated keys, exactly as before this change.

#### Scenario: Opt-in off
- **WHEN** `discovery_engine_read` is `"off"` and the template catalog has an engine identity
- **THEN** a discovered warehouse's `loadTable` carries the delegation header with the person's token
- **AND** no engine `/config` or `/credentials` call is made for it

### Requirement: Engine identity only after the membership proof
With `discovery_engine_read = "template"`, SQE SHALL attach the template's engine identity to a discovered warehouse only when all of the following hold:
- the warehouse name matches `discovery_engine_warehouses`;
- it is not a configured catalog name, warehouse alias or Hive catalog;
- the person's `GET /config?warehouse=` at the template URL succeeds;
- the engine's `GET /config?warehouse=` at the same URL succeeds and returns the same catalog prefix.

Otherwise SQE SHALL read the warehouse with the person's delegated keys.

#### Scenario: Proof passes
- **WHEN** a metadata-only person queries an allowlisted discovered warehouse where the engine holds read-data and Ranger allows the person `select`
- **THEN** the rows are returned with masks and row filters applied
- **AND** no `sqe.toml` entry and no restart were needed for that warehouse

#### Scenario: Engine has no role in the warehouse
- **WHEN** the engine's `/config` for the warehouse answers 403
- **THEN** SQE reads with the person's delegated keys and makes no `/credentials` call

#### Scenario: Prefix mismatch
- **WHEN** the person's and the engine's `/config` responses carry different prefixes
- **THEN** SQE reads with the person's delegated keys and makes no `/credentials` call

#### Scenario: Outside the allowlist
- **WHEN** the warehouse name matches no entry in `discovery_engine_warehouses`
- **THEN** SQE reads with the person's delegated keys and makes no engine call

### Requirement: Default-deny select for every name that may carry the engine identity
With the opt-in on, the policy store SHALL deny `select` by default on every catalog name except the configured catalogs without `credentials`, from startup on.

#### Scenario: No select policy for a discovered warehouse
- **WHEN** no Ranger policy grants the person `select` on a table in a discovered warehouse
- **THEN** the person reads zero rows and no `/credentials` call is made

### Requirement: Writes to discovered warehouses stay delegated
SQE SHALL load every write target in a discovered warehouse with the person's delegated keys, in both modes.

#### Scenario: Insert into a discovered warehouse
- **WHEN** a person runs `INSERT` into a table of a discovered warehouse with the opt-in on
- **THEN** the target load carries the delegation header with the person's token and no engine keys are used

### Requirement: Startup refuses an unsafe opt-in
SQE SHALL refuse to start with `discovery_engine_read = "template"` unless all of the following hold:
- discovery is `polaris-auto`;
- the template catalog has a `client_credentials` engine identity with `require_vended_credentials`;
- the policy engine is Ranger with `catalog_scoped = true`;
- `discovery_engine_warehouses` is non-empty.

#### Scenario: Passthrough policy engine
- **WHEN** the opt-in is on and `policy.engine = "passthrough"`
- **THEN** startup fails with a message naming the missing requirement
