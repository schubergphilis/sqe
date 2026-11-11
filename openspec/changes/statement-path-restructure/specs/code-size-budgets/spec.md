## ADDED Requirements

### Requirement: Function-length budget
The budget check SHALL fail when a production function is longer than the default limit and is not recorded as an exception.

#### Scenario: New long function
- **WHEN** a merge request adds a production function longer than the default limit
- **THEN** `cargo xtask budgets` fails and names the function, its length and the limit

#### Scenario: Test code is exempt
- **WHEN** a function sits under `#[cfg(test)]`, in a `tests/` directory or in a bench target
- **THEN** it is not measured

### Requirement: Recorded exceptions only shrink
A function recorded as an exception SHALL NOT grow beyond its recorded length, and the record SHALL be lowered when the function shrinks.

#### Scenario: Recorded function grows
- **WHEN** a recorded function becomes longer than its recorded length
- **THEN** the check fails

#### Scenario: Recorded function shrinks
- **WHEN** a recorded function is shorter than its recorded length by more than the tolerance
- **THEN** the check fails and asks for the record to be lowered or removed

#### Scenario: Recorded function disappears
- **WHEN** a recorded function no longer exists
- **THEN** the check fails and asks for the record to be removed
