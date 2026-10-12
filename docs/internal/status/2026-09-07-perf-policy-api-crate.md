**STATUS 2026-09-07 (late): build-time item C1, policy half, on `perf/policy-api-crate` (#662): `sqe-catalog` no longer waits for `sqe-policy`.**

New crate `sqe-policy-api` holds exactly the five things the catalog took
from the policy crate: the `PolicyStore` trait with `ResolvedPolicy`,
`MaskType`, `TagMaskSpec`, `MaskPolicyInfo`; the `GrantBackend` trait with
its statement types; and `HiveCatalogs`. Moved verbatim; `sqe-policy`
re-exports them under the old paths so no other crate changes;
`sqe-catalog` depends on the api crate (and on `sqe-policy` only as a
dev-dependency for `InMemoryPolicyStore`). Measured with
`CARGO_INCREMENTAL=0`, `touch sqe-core`: the catalog now starts at 10.6 s
instead of 28 s and compiles alongside the policy crate; the chain on a
main base goes 173 s -> 164 s, and the coordinator starts 12 s earlier. On
top of !961 (coordinator 68 s -> 18 s, chain 131 s) that should land near
120 s. Issues opened for both findings: #661 (async `Send` proofs, !961)
and #662 (this). MR B of the crate split is pushed as !962 at the user's
request, as a structural change for #636 only.
**NEXT:** merge !961, !962, this; re-measure the chain on main; then the
review leftovers (411 + 807, 806, 205/212, 610, 627, #659 partials). The
worker half of C1 (coordinator -> sqe-worker edge) stays parked: the
worker compiles in 13 s alongside the catalog and is off the critical path.
