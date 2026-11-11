## ADDED Requirements

### Requirement: Shadowed built-in functions are declared
Every scalar, aggregate or window function that SQE registers under a name DataFusion's default registry also defines SHALL be listed with a reason.

#### Scenario: Undeclared shadow
- **WHEN** SQE registers a function whose name a default DataFusion session already defines, and the name is not in the list
- **THEN** the registry test fails and names the function

#### Scenario: DataFusion upgrade adds a built-in
- **WHEN** a DataFusion upgrade adds a built-in with the name of an existing SQE function
- **THEN** the registry test fails until the function is removed or declared

#### Scenario: Stale declaration
- **WHEN** a listed name is no longer shadowed
- **THEN** the registry test fails and asks for the entry to be removed

### Requirement: Shadowed functions are removed when equivalent
An SQE function that shadows a built-in SHALL be removed when the built-in passes SQE's existing tests for that function.

#### Scenario: Built-in is equivalent
- **WHEN** the SQE registration is disabled and the function's tests pass against the built-in
- **THEN** the SQE implementation and its list entry are deleted

#### Scenario: Built-in differs
- **WHEN** a test fails against the built-in
- **THEN** the SQE implementation stays and its list entry states the difference

### Requirement: Vendored patches are recorded
Every local change to a tree under `vendor/` SHALL have a ledger entry with its purpose and upstream status.

#### Scenario: Patch landed upstream
- **WHEN** a ledger entry is marked as released upstream
- **THEN** the patch is dropped at the next re-vendor of that tree

#### Scenario: New local patch
- **WHEN** a merge request changes a file under `vendor/` other than by re-vendoring
- **THEN** the same merge request adds or updates a ledger entry
