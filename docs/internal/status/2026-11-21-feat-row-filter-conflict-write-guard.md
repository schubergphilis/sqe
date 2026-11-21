**STATUS 2026-11-21: tag write guard refuses row-filter conflicts in every mode (#931 follow-up, `feat/row-filter-conflict-write-guard`).**

- `TagConflictGuard` carries the session's `MaskPrecedence` (`with_precedence`, default `refuse`). A new `PolicyConflict::RowFilter` is refused under every precedence; a new `PolicyConflict::Mask` only under `refuse`, as before. The refusal text names the conflict kind.
- `QueryHandler::tag_conflict_guard` builds the guard in every mode. When the table has no tag policy key it still refuses under `refuse` and allows otherwise (no tag policy can be affected then).
- `set_table_properties_guarded`: a raw `sqe.column-tags` value that is not a tag map, written by an unrestricted writer (no column authorisation), skips the guard instead of failing the parse. Reads of such a value deny everything (#840), so there is no conflict to check; this is the path `unknown_tag_state_denies` uses to seed bad state, which the default-mode guard broke on the first full run.
- Follows TA-13 (!1899): reads already refuse a second row filter in every mode, so without this a `SET TAGS` in the default `tag` mode could break every later read of the table.
- OpenSpec `one-row-filter-per-table` task 4.1 ticked; book "Mask precedence" paragraph no longer says SET TAGS doesn't refuse.

Gate:
- `cargo test -p sqe-policy --lib tag_conflict_guard`: 12 passed (new: row-filter conflict refused under tag/resource/refuse; mask conflict allowed under tag/resource).
- `scripts/access-control-test.sh` (full), first run: 69 passed, 2 failed (`refuse_at_write_row_filter_conflict` reused `ctx.handler`, whose policy cache predated the test's policies; `unknown_tag_state_denies`, above). Both fixed.
- Full rerun after the fixes: 70 passed, 1 failed (exit 101): `show_schemas_describes_the_catalog_it_names` saw `orders_bad_tags`, left behind by the first run's failed `unknown_tag_state_denies` (it sorts later and drops the table itself). Rerun alone on the clean stack: 1 passed (exit 0).
- `cargo clippy -p sqe-policy -p sqe-coordinator -p sqe-write --all-targets -- -D warnings`: exit 0.

With !1886, !1897 and !1899 this covers every acceptance item of #918. Not in #918: one mask per column in every mode (`tag` and `resource` keep their precedence).

Open: nothing for 4.1. The guard's cost (one Ranger principal lookup per tag write) is now paid in every mode, not only `refuse`.

**NEXT:** #922 rest (`ALTER COLUMN ... SET TAGS ('k'='v')`) and #923 (keep tag values, SHOW TAGS with values).
