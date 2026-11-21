## ADDED Requirements

### Requirement: A tag write that creates a policy conflict is refused under refuse
With `policy.mask-precedence = "refuse"`, the system SHALL refuse a tag write (SET TAGS, SET MASKING POLICY, or a raw `sqe.column-tags` property write) when, for some principal named by a mask or row-filter policy that matches the table or one of its tags (or for `{PUBLIC}`), the table's tags after the write resolve a mask conflict on a column or a row-filter conflict on the table that the tags before the write did not. A refused write SHALL commit nothing and project nothing to Ranger.

#### Scenario: Tag mask conflicts with a resource mask
- **WHEN** a resource policy masks `orders.amount` with `MASK_HASH` for role `engineer`, a tag policy masks tag `t` with `MASK_NULL` for role `engineer`, and an admin runs `ALTER TABLE orders SET TAGS (amount = 't')`
- **THEN** the statement fails with `mask conflict on ...orders.amount` and the table's tags are unchanged

#### Scenario: Same mask from two sources
- **WHEN** the tag policy masks `t` with `MASK_HASH` instead
- **THEN** the tag is written

#### Scenario: Disjoint principals
- **WHEN** the tag policy binds role `analyst` and the resource policy binds role `engineer`, and no single role holds both
- **THEN** the tag is written

#### Scenario: Row filter conflict
- **WHEN** a resource row filter applies to `orders` for role `engineer` and a tag row-filter policy on tag `f` gives a different filter to `engineer`, and an admin tags a column of `orders` with `f`
- **THEN** the statement fails with `row filter conflict on ...orders`

#### Scenario: Raw property write
- **WHEN** an admin writes `sqe.column-tags` directly with the conflicting tag
- **THEN** the statement fails the same way

### Requirement: Writes that add no tag are not refused
The system SHALL NOT run the check for a tag write that adds no tag to any column.

#### Scenario: UNSET on a conflicted column
- **WHEN** a conflict exists on a column and an admin runs `ALTER TABLE orders UNSET TAGS (amount)`
- **THEN** the tag is removed

### Requirement: Other modes and backends are unchanged
Under `tag` and `resource` the system SHALL write tags as before. A policy store that cannot list the principals its policies name SHALL not refuse tag writes; the read-time refusal still applies.

#### Scenario: Tag mode
- **WHEN** `mask-precedence = "tag"` and the conflicting tag of the first scenario is written
- **THEN** the tag is written

### Requirement: The check fails closed
If the policy store cannot be read during the check, the system SHALL refuse the write.

### Requirement: Single principals only
The check SHALL evaluate one principal at a time. A conflict that needs two memberships of one user SHALL be allowed at write time and refused at read time for that user.

#### Scenario: Mixed membership
- **WHEN** the resource mask binds role `engineer`, the tag mask binds role `analyst`, and user bob holds both roles
- **THEN** the tag write succeeds and bob's read of the column fails with `mask conflict on ...`
