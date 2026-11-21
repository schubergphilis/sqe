### Added

- `policy.mask-precedence = "refuse"`: conflicting masks or row filters fail the statement instead of one winning (#918)

    Under `refuse`, when more than one distinct column mask resolves for a column and user (a resource mask and a tag mask, or two tag masks), the statement fails with `ACCESS_DENIED` "mask conflict on <catalog>.<schema>.<table>.<column>" before any row is read. When a tag row filter and a different row filter resolve for a table and user, it fails with "row filter conflict on <catalog>.<schema>.<table>". The same mask or filter from two sources counts once. This is Unity Catalog's ABAC rule. `tag` (the default) and `resource` are unchanged.
