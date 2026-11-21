# Design: refuse a conflicting tag at write time

## D1. Which principals are checked

A conflict is a property of one user, and a write has no reader. The check therefore asks the question for every principal the policies name: each user, role and group in an item of
- a mask or row-filter resource policy that matches the table (`matching_fine_grained_policies`, the matcher the read path uses), and
- a tag mask or row-filter policy on any tag in the table's tag map after the change,

plus one principal with no user name, roles or groups, which still matches `{PUBLIC}` items (`item_matches` treats the public group as matching everyone).

Each principal is a synthetic `SessionUser` holding that one user name, role or group. Its user name, when it is not a user principal, starts with a NUL byte so no `users:` item can match it. Resolving a synthetic user adds a cache entry under its own key (`cache_key` covers user name, roles and groups), so it cannot answer a real user's lookup.

**Limit: mixed memberships.** A real user holding two roles, one bound to a resource mask and the other to a different tag mask, has a conflict that neither role has alone. Checking every combination of principals grows exponentially, so single principals are checked and this case is left to the read-time refusal: the write is allowed, and that user's read fails with the named error. A test pins this.

## D2. Only new conflicts refuse

The check resolves each principal's policy once (`store.resolve`), then the tag side twice: once for the tags before the change, once for the tags after. Both are merged under `refuse` into their own copy of the resource policy. `resolve_tags` drops the tag key of a row filter, so one call over the union of the two tag sets could not be split afterwards. The result is two conflict sets. The write is refused when the after set holds a conflict the before set does not. Sets, not lists: the tag map is a `HashMap` and its order is not stable.

An existing conflict does not block an unrelated tag write. It already fails every read for that principal.

## D3. Writes that add nothing are not checked

Removing a tag can expose a conflict: an unmappable tag restricts its column, and taking it away un-restricts the column, so the column's other masks can now disagree. Refusing that would block the one statement that lets a steward undo a bad tag. The rule is therefore "a statement that adds no tag to any column is not checked", applied to the parsed ops of SET/UNSET TAGS and to the diff of a raw property write alike. A conflict exposed this way shows at read time.

## D4. Store failures refuse

`conflict_principals` returns `Result`; an error refuses the write. `resolve` errors propagate too: `resolve_effective_policy` turns them into a deny-all sentinel, which is right for a reader and wrong for this check. `resolve_tags` has no error channel and returns a `lit(false)` filter when the bundle will not load. The `refuse` merge never counts that deny-all sentinel as a row-filter conflict, so in that case the check finds no conflict and the write goes through; the read that follows still fails closed on the sentinel. The bundle was read moments earlier by `conflict_principals`, so the case is a race, not a mode.

## D5. Backends

`conflict_principals` defaults to `Ok(None)`: "this store cannot say". The guard then allows the write. The in-memory store resolves no tags, and Polaris grants and passthrough have no masks, so for them no tag write can create a conflict. Ranger is the only store that resolves tags.

## D6. Where it runs

`TagConflictGuard` (sqe-policy) carries the store, the policy key `(namespace, table)` and the table's display name. The coordinator builds it only when `mask_precedence = refuse` and a store is configured, with the key from the same helper the tag-authority gate uses (`resolve_policy_key_for_ident_scoped` with `store.catalog_scoped()`), so the policy that is checked is the one that is enforced. It also carries the table's catalog: under `catalog_scoped` (#919) tags resolve through `tag_scope::resolve_tags_in_catalog`, and `conflict_principals` gets both the bare and the `<catalog>.<tag>` names, so a catalog-scoped tag policy is checked for its own catalog only. `CatalogOps` takes it as an argument on each call, not as a field, so an unwired handler is a compile error rather than a silent no-op.

The guard runs inside the per-table lock, after the live map is read and the new map computed, before the commit. For the raw property path the lock is taken whenever the guard is present and the statement writes `sqe.column-tags`, not only under `ranger-delegate`; an admin's raw write is checked too.

## D7. Error

`AccessDenied`, `tag change refused: it would create a mask conflict on <table>.<column> under mask_precedence = refuse` (or `a row filter conflict on <table>`). The principal is not named: a column steward under `ranger-delegate` is not entitled to learn who a policy binds. Table and column are already known to the writer.
