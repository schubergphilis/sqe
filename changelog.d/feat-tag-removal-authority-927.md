### Security

- `tag_removal` decides who may take a tag off a column, table or schema (#927)

    `access_control.tag_removal = "same"` (the default) keeps the old rule:
    the tag privilege covers adding and removing. `"remove_tag"` also asks
    for the Ranger `remove_tag` access type on each column or object that
    loses a tag key; `"admin"` reserves removal for admins. A removal is an
    `UNSET TAGS`, an `UNSET MASKING POLICY`, an object `UNSET TAGS`, or a raw
    `sqe.column-tags` write that drops a key; the commit re-checks the
    removed columns under the table lock. A catalog untag needs an admin
    under either stricter mode. Ranger's `all` does not grant `remove_tag`.
    Each authorised removal is logged with the user, table and columns. The
    quickstart bootstrap adds `remove_tag` to the `hive` service definition.
