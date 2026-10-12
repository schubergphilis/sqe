**STATUS 2026-09-07 (evening): coordinator crate split started; MR A (write-path decoupling, no file moves) on `refactor/write-path-decouple`.**

The compile-time measurement that decides the shape is in
`docs/internal/plans/2026-09-07-coordinator-crate-split.md`: after a touch
of sqe-core the whole first-party chain rebuilds in 154 s, of which the
coordinator library alone is 96 s (64 s of it type-checking) and its
binary 30 s more; every other crate is done by 44 s. Untangling the
policy/catalog and worker/coordinator edges (plan item C1) could move the
coordinator's start by at most 12 s, so it is dropped in favour of C2. A
chain of crates does not shorten a cold build; only sibling crates do, and
the coordinator's one big sibling pair is the write path and the
distribution path. MR A cuts every edge from the write path into the rest
of the crate without moving a file: the five identifier helpers move from
`catalog_ops` to a new `ident` module, `commit_with_retry` and
`is_conflict_message` to a new `commit` module, `collect_live_data_files`
from `maintenance` to `writer`, the two live-delete helpers are imported
from `sqe_compaction` directly, and the two `session_context` calls
(Polaris discovery, session-cache invalidation) go through a
`CatalogDiscovery` trait that `QueryHandler::new` installs as
`PolarisCatalogDiscovery`. Behaviour is unchanged when the trait is
installed; a `WriteHandler` without it (unit tests) treats a non-default
3-part target as an unknown catalog and skips session-cache invalidation.
The compiler cache is switched off for now: the acc StorageGRID endpoint
is unreachable from the production runners (probe MR !958, closed) and
there is no production bucket yet.
**NEXT:** MR B moves the write path into `crates/sqe-write`; MR C moves
the `sqe-server` binary into its own crate; re-measure after each with the
same `touch sqe-core` protocol. Then the review leftovers (411 + 807,
806, 205/212, 610, 627, #659 partials).
