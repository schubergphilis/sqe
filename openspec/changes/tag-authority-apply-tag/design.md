# Design: tagging as a grant of its own

## D1. A configurable access type, default `alter`

Supersedes D1 of `tag-authority-ranger-delegate`. `tag_privilege = "alter"` keeps 0.55 exactly. `"apply_tag"` asks for a custom access type that the portal's policies already use. A custom type means the `hive` servicedef must carry it: Ranger refuses a policy that names an unknown type. Other `hive` clients (Polaris's embedded authorizer, Spark/Kyuubi) never ask for it, so it changes no decision of theirs. The quickstart bootstrap adds it the way the portal's bootstrap does: next free `itemId`, no implied grants, GET by name then PUT by id, a no-op when present.

Spellings: `alter`, `apply_tag` (alias `apply-tag`). Anything else is a configuration error, so a typo cannot fall back to `alter`.

## D2. `all` does not imply `apply_tag`

In Ranger's `hive` servicedef, `all` implies every stock type. Taking that at face value would make every `all` grant (the usual owner grant) a tagging grant, which is the coupling `apply_tag` exists to remove. So on widening items (allow items, deny exceptions) only an item naming `apply_tag` grants it. On narrowing items (deny items, allow exceptions) `all` still covers it: a deny on `all` means "nothing", and treating it otherwise would let a denied user tag. The asymmetry errs towards refusing.

## D3. One check for every writer

Supersedes D5 of `tag-authority-ranger-delegate`. A different gate on a second spelling is a bypass or a needless refusal, so all writers of `sqe.column-tags` ask the same question: which columns does this statement change, and is each granted?

- Tag statements: the columns named.
- `SET TBLPROPERTIES`: the map replaces the live one, so the changed set is the diff. A column is changed when its tag set differs (order and duplicates ignored; an empty list counts as absent). A removed column counts: untagging is what removes a mask.
- `CREATE TABLE`: no live map; every column the map tags.
- `CREATE OR REPLACE`: diffed against the live table (missing = empty). Replacing without naming the key keeps the tags and needs nothing.

Under `admin-role` the 0.55 shape is kept (admin for ALTER and OR REPLACE, plain CREATE ungated). The plain `CREATE` stays ungated there, as in 0.55: a new table has no reader whose mask could be removed.

## D4. Read what will be written

The 0.55 gate looked for the key among raw `SqlOption`s. The write path resolves options through `TrinoTableOptions`, which also unpacks `extra_properties = MAP(ARRAY[...], ARRAY[...])`. So the gate (`tag_property_write`) uses the same resolver: the value checked is the value written, last exact key wins. An off-case key (`SQE.Column-Tags`) is refused for non-admins under delegate rather than guessed at, since the catalog stores it as a different key.

## D5. Static part first, columns after the catalog preflight

The part that needs no catalog (admin role, `admin-role` mode, off-case key, malformed JSON) runs before `preflight_resolve_catalogs`, so a refused caller learns nothing about which catalogs exist (as in 0.55). The per-column part needs the live map, so it runs after.

## D6. Check-then-write

`ALTER ... SET TBLPROPERTIES`: the coordinator diffs and checks, then hands the authorised column set to `set_table_properties_guarded`. That takes the per-table lock SET TAGS uses, reloads the table, re-diffs, and refuses if the diff now reaches a column outside the set ("changed while the statement was checked ... retry"). The lock is held to the commit. A concurrent SET TAGS cannot slip between check and write.

`CREATE OR REPLACE`: checked against the live table, then replaced through the CTAS path with no lock spanning both. A tag write landing in between is replaced by the statement's map. This is the window any replace has against a concurrent write, and it is documented rather than closed.

## D7. Refusals

`403 Forbidden: <statement> requires <privilege> on <ns>.<table>.<column>` (AccessDenied), plus a `warn!` line with user, roles, statement and privilege. `execute` writes the failed-statement audit record as for #911.

A live value that does not parse is refused for non-admins: there is no old map to diff, so the change cannot be bounded.
