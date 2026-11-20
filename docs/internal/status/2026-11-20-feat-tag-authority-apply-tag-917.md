**STATUS 2026-11-20: tagging as a grant of its own (`feat/tag-authority-apply-tag-917`, #917).**

- OpenSpec change `tag-authority-apply-tag` (proposal, design D1-D7, spec, tasks; all tasks ticked). Supersedes D1 and D5 of `tag-authority-ranger-delegate`.
- `[access_control] tag_privilege = "alter" (default) | "apply_tag"` (`TagPrivilege`, parse test). `TablePrivilege::ApplyTag`.
- Ranger: `apply_tag` is granted only by an item naming it; `all` on allow items does not grant it, `all` on deny items / allow exceptions still refuses it (`all_grants`).
- Gate moved to `query_handler/tag_authority.rs`. Under `ranger-delegate` every `sqe.column-tags` writer uses the per-column check: tag DDL (columns named), `SET TBLPROPERTIES` (diff live vs new, removed columns count, re-checked under the per-table lock in `set_table_properties_guarded`), `CREATE TABLE` (every tagged column), `CREATE OR REPLACE` (diff vs live). Malformed value, off-case key and a malformed live map stay admin-only. `admin-role` keeps the 0.55 shape.
- Found while building: the 0.55 gate matched raw option keys only, so `sqe.column-tags` via Trino `extra_properties` (ALTER or CREATE OR REPLACE) passed it under `admin-role`. The gate now reads options through `TrinoTableOptions`, like the write.
- `UNSET TBLPROPERTIES` does not parse (sqlparser 0.62), so there is no unset route.
- Quickstart `bootstrap-ranger.sh` adds `apply_tag` to the `hive` servicedef (idempotent; verified added then "already present" on re-run).
- Book "Who may tag a column", `sqe.toml.example`, README, roadmap.

Gate: see the MR description for the exact commands and counts.

Left open: refusing a tag that no tag policy defines (request item 3); a `select`/`USE` check on catalog and schema before a tag write; the `CREATE OR REPLACE` check-then-write window (D6).

**NEXT:** "tag must be defined" follow-up; then #912.
