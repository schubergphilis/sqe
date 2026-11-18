# fix/trino-cast-type-syntax

Part of the `trino-sql-parity` OpenSpec change, task 4.1 (done) and 4.2
(CAST half).

## What changed

- New `crates/sqe-sql/src/cast_types.rs`: a source-level rewrite of every
  `CAST(..)` / `TRY_CAST(..)` target, skipping string literals and quoted
  identifiers, reusing `ddl_types::rewrite_type`. `array(T)` becomes
  `ARRAY<T>`, `row(..)` becomes `STRUCT<..>` (a `ROW(..)` constructor operand
  keeps the #335 `named_struct` path), `map(K, V)` becomes
  `arrow_cast(x, '<arrow type>')` because DataFusion's SQL planner refuses a
  Map cast target, `time(p) [with time zone]` becomes `TIME`. Parse-gated:
  the rewrite is returned only when it parses.
- `front_door.rs`: runs on a statement that parses (map, time) and in the
  parse-failure fallback chain (array, nested row). The Flight, Trino and
  embedded paths all enter through `SqlFrontDoor::normalize`.
- `ddl_types.rs`: helpers made `pub(crate)`.

## Gate

- `cargo test -p sqe-sql --lib`: exit 0 (655 passed)
- `cargo test -p sqe-catalog --lib`: exit 0 (835 passed)
- `cargo clippy -p sqe-sql --all-targets -- -D warnings`: exit 0
- `cargo check-server`: exit 0
- `cargo xtask budgets --check`: exit 1 only on `sqe-catalog` (on main).

Not run: integration tests, parity run. Tests assert rewritten SQL; the
planned Arrow types were checked by hand.

## Open

- `TRY_CAST` to a map stays as written (`arrow_cast` raises).
- `CAST(json AS array(bigint))` plans but fails at run time: no JSON-to-array
  cast.
- `time(p)` nested inside an array or row type; DDL `time with time zone` is
  still refused (task 4.2 stays open).
- A statement mixing a nested row-constructor cast and an `array(..)` cast
  fails to parse: each fallback is gated on its own result.

**NEXT:** JSON-to-array/map cast; parity rerun on the stacked branches.
