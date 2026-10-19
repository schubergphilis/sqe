### Added

- `GRANT SELECT ... MASKED WITH (col = expr, ...)` and `... ROWS WHERE expr` attach column masks and a row filter to a grant

    The syntax was documented but never parsed. It now writes Ranger policies on the resource service (`[policy] engine = "ranger"`). Ranger allows one mask policy per column and one row-filter policy per table, so each grantee gets its own item in that one policy. A repeated GRANT replaces that grantee's item and puts it first. `REVOKE` leaves the masks and row filter in place: the grantee may still read the table through a wider grant, and lifting the mask would show it the real values. Admin-only, `SELECT` without a column list, one schema-qualified table, one grantee. Ranger policies written by hand with wildcards or value lists are never edited.
