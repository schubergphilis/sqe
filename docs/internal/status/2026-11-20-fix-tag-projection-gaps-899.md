**STATUS 2026-11-20: e2e for catalog-scoped tag projection (`fix/tag-projection-gaps-899`, #899).**

- New access-control e2e `catalog_scoped_tag_projection_keys_the_catalog`:
  1. Sets a tag through the unscoped handler and asserts `database=ac`.
  2. Re-sets the tag through a second handler with `catalog_scoped = true` (same config, shared warm cache).
  3. Asserts `database=sales_wh.ac` is tagged and `database=ac` is no longer tagged.
  4. Unsets the tag and asserts no resource is left.
- The helper `ranger_column_tagged_in` matches on `database` too. `ranger_column_tagged` delegates to it, so existing callers are unchanged.
- Checked and left as is: `reproject_loaded_tags` skips a table whose tag map is empty, and that skip loses nothing.
  - The legacy cleanup can only delete the columns it is given (`previous` ∪ new). Ranger's import names a resource per tagged column.
  - Every SET/UNSET TAGS under `catalog_scoped` already clears the legacy resource from `previous`.
  - So a table with no tags in Iceberg has no legacy resource with tags left, unless the two stores drifted. Repairing drift would need a read of Ranger's tag store; no issue filed.

Gate:
- `scripts/access-control-test.sh catalog_scoped_tag`: exit 0, 1 passed.
- `scripts/access-control-test.sh tag`: exit 0, 4 passed (the existing tag tests).
- `cargo clippy -p sqe-coordinator --tests -- -D warnings`: exit 0.
- rustfmt ratchet: clean.

Left open on #899: the Spark read of `b.sales.orders` (acceptance 1, second half). It needs the portal's Spark image to send the scoped key (S10, out of scope in the issue).

**NEXT:** #902/#901/#908 open test tasks (`test/` branch).
