**STATUS 2026-10-16: ISO 8601 functions and the query clock match Trino (`fix/trino-datetime-values`).**

Three wrong values from the Trino 476 side-by-side probe, none of them a type change:

- `from_iso8601_timestamp` returned NULL for any input with a zone designator (`Z`, `+02:00`, `+0200`, `+02`). It now reads the designator and returns that instant as UTC wall clock, and accepts `hh:mm` and a bare date, as Trino does. Without a designator the input is read as UTC, as before.
- `to_iso8601` on a timestamp without a zone appended `Z` (`2024-01-02T03:04:05.000Z`). It now has no designator (`2024-01-02T03:04:05`, Trino's output), and the fraction shows only when non-zero. Trino pads to the declared precision, which Arrow does not carry, so a stored `timestamp(6)` column still differs by trailing zeros. This changes the text existing queries see, so the changelog lists it under Changed.
- `now()`, `current_timestamp`, `localtimestamp` and `localtime` fold to the query start time in `simplify()`, so every clock call in a statement reads one instant. The constant evaluator runs before `simplify` and invoked the functions directly, reading the clock once per call, so `invoke_with_args` now refuses, as DataFusion's own `now()` does. The two physical-expression sites that skip the optimizer (`prune_data_files`, the worker's `build_physical_predicate`) only see filters the optimizer has already folded, and both skip pruning on an error rather than fail. On the rig, `WHERE ts < now()` on an Iceberg table pushes `predicate=[ts < <literal>]`.

`localtime` still uses the server's zone; that moves with the session-zone work. Probe re-run: of 95 expressions only these two values changed.

**NEXT:** `fix/trino-json-cast`: `CAST(ARRAY/MAP AS JSON)` returns a quoted display string instead of JSON. Then typeof names and exponent literals, then the session time zone (`timestamp with time zone` for the clock functions, zone-keeping literals, `AT TIME ZONE`).
