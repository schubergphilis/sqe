## Context

`tag_authority.rs` gates every writer of `sqe.column-tags`. Under `ranger-delegate`, `require_tag_columns` asks the store for the configured `tag_privilege` (`alter` or `apply_tag`) on each column that a statement changes. A raw property write is diffed against the live map (`changed_tag_columns`), and `set_table_properties_guarded` re-diffs under the per-table lock and refuses columns outside the authorised set.

## Decisions

### D1: A removal is a tag key that a column loses

`SET TAGS` merges (`apply_tag_ops`): it adds keys and replaces values but never drops a key. So:

- **SET/UNSET TAGS.** Every column with an `Unset` op is a removal. This is decided statically, before the catalog is read. An `UNSET` of a tag the column does not carry is still treated as a removal: answering otherwise would need the live map before the check, and the conservative answer leaks nothing.
- **SET/UNSET MASKING POLICY.** The gate comes from `MaskingPolicyOp.policy`: `None` (UNSET) is a removal and `Some` (SET) is an add. The synthetic gate statement used to be an `Unset` for both. It now carries the real action, and it still runs before the policy lookup.
- **Raw property writes and CREATE OR REPLACE.** The removed columns are `removed_tag_columns(old, new)`: the columns that hold a key in `old` that `new` does not give them. A value change on a kept key is not a removal, because masks resolve on keys.
- **Plain CREATE.** A new table removes nothing.

### D2: The check stacks on the tag privilege

A removal is also a change, so the removed columns still need `tag_privilege`. Under `remove_tag` they also need `remove_tag`, and under `admin` the statement needs an admin role. Admins always pass. `remove_tag` is checked after the tag privilege and before the `tag_names` check, so a refused caller learns nothing about the tag definitions.

### D3: The locked re-check carries the removed set

`authorise_tag_property` returns `TagWriteAuth { changed, removed }` instead of a bare `BTreeSet`. Under the lock, `set_table_properties_guarded` refuses when the re-diffed changed set has a column outside `changed`, or when the re-diffed removed set has a column outside `removed`. Without this, a column authorised as an add (it needed only `apply_tag`) could become a removal if a concurrent write tagged it in between.

### D4: `remove_tag` is never implied

The Ranger store's `all_grants` already excludes `apply_tag` from an allow item's `all`. It now excludes `remove_tag` too. A deny on `all` still denies `remove_tag`.

### D5: Object tags

Object tags (#922) do not drive masks today. Inheritance (#924/#930) will make them do so, so their UNSET takes the check now, against the same table-level key the object gate uses. A catalog's tags have no Ranger key that SQE checks, so under `remove_tag` or `admin` an UNSET on a catalog needs an admin.

### D6: Store errors grant nothing

`grants_column_privilege` errors are treated as not granted, as for `apply_tag`. Backends without a column privilege model (Polaris grants, in-memory) grant nothing, so under `remove_tag` only admins can remove a tag there.

## Risks

- **The default keeps the gap.** The parity test turns XPASS only on a deployment that sets `tag_removal`. The docs say so.
- **CREATE OR REPLACE.** It is still checked against the live table without a lock spanning the check and the replace. This is the window that #917's D6 documents.
