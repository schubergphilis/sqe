# 105: Pre-parse keyword scans uppercase with Unicode rules then slice the original string by ASCII byte length; some user inputs panic

- Scope: sqe-sql (classifier, tags, partition_evolution, time_travel)
- Severity: high
- Category: reliability
- Effort: S (hours)
- Status: open

## Problem

Several hand-written pre-scans compute `upper = s.to_uppercase()`, test
`upper.starts_with("<ASCII KEYWORD>")`, and then slice the original `s` at
`"<ASCII KEYWORD>".len()`. `str::to_uppercase` is Unicode-aware: `ſ` (U+017F,
two bytes) uppercases to `S`, `ı` (U+0131) to `I`, and the `ﬅ` ligature to
`ST`. When such a character sits at the end of the matched keyword, the byte
offset lands inside it and the slice panics with "byte index is not a char
boundary".

Reachable cases, verified by hand-computing the byte offsets:

- `crates/sqe-sql/src/classifier.rs:387` and `:429-430`:

  ```rust
  let upper = trimmed.to_uppercase();
  ...
  if upper.starts_with("SHOW PARTITIONS") {
      let rest = trimmed["SHOW PARTITIONS".len()..].trim();
  ```

  Input `SHOW PARTITIONſ` (16 bytes) uppercases to `SHOW PARTITIONS`, passes
  the test, and `trimmed[15..]` splits the two-byte `ſ`. Panic.

- `crates/sqe-sql/src/tags.rs:35-51`: `rest_upper = rest.to_uppercase()`, then
  `rest["SET TAGS".len()..]` and `rest["UNSET TAGS".len()..]`. Input
  `ALTER TABLE t SET TAGſ (...)` panics at `tags.rs:45`; `UNSET TAGſ` at `:50`.

Same pattern, not currently reachable because no multibyte character
uppercases to the last letter of the keyword, but one keyword edit away from
it: `tags.rs:39,56,58,68,80,122`, `partition_evolution.rs:55-62`
(`trimmed["ALTER TABLE ".len()..]`), `time_travel.rs:262-266`
(`trimmed[9..]` after `upper.starts_with("TIMESTAMP")`).

The crate already knows about this bug. `classifier.rs:313-321` documents the
fix ("The previous pre-scan tested prefix against `s.to_uppercase()` but
sliced the ORIGINAL string by the prefix's byte length") and provides a
correct `strip_prefix_ci`. There are three separate implementations of that
helper (`classifier.rs:321`, `attach.rs:373`, `policy_ddl.rs:254`), only the
`policy_ddl` one uses `s.get(..)` so it cannot panic, and the sites above
bypass all three.

The property test at `crates/sqe-sql/tests/parser_proptest.rs:33-57` only
draws `é` and `日` as non-ASCII characters; neither uppercases to ASCII, so the
existing "classifier does not panic" property cannot find this class.

## Why it matters

`parse_and_classify` runs on every statement before authentication of the
statement's target and before any policy check (`flight_sql.rs:876`,
`query_handler.rs:978`, `sqe-mcp/src/lib.rs:621`). Any authenticated client,
including the anonymous MCP mode, can crash the handler task with a 16-byte
string. Under tonic the panic is caught per request and surfaces as an
opaque transport error rather than a SQL error, so it also defeats the
structured error path in issue 104. A `panic = "abort"` profile or a future
move of classification onto a shared task would turn it into a process crash.

## Proposed change

1. One helper, one place: move a single `strip_prefix_ci(&str, &str) ->
   Option<&str>` into `crates/sqe-sql/src/lib.rs` (or a `text.rs`) built on
   `s.get(..prefix.len())` plus `eq_ignore_ascii_case`, and an
   `starts_with_ci` that never allocates. Delete the copies in `attach.rs` and
   `classifier.rs`.
2. Replace every `to_uppercase()` used for keyword detection with
   `to_ascii_uppercase()` or with the helper. `to_ascii_uppercase` never
   changes byte length, which makes all the existing slices safe by
   construction. SQL keywords are ASCII; there is no legitimate reason for
   Unicode case mapping here.
3. Add `clippy::string_slice` to the deny list for `sqe-sql` so new byte
   slicing on user text needs an explicit `#[allow]` with a justification.
4. Extend `nasty_string()` in `parser_proptest.rs` with `ſ`, `ı`, `ﬅ`, `ﬀ` and
   `İ`, and add the two concrete regressions above as plain unit tests.

## Acceptance criteria

- `cargo test -p sqe-sql` includes `show_partitions_long_s_does_not_panic`
  and `set_tags_long_s_does_not_panic`, both asserting `Err` or `Ok`, never a
  panic.
- `grep -rn 'to_uppercase()' crates/sqe-sql/src` returns zero hits outside
  tests.
- The proptest corpus includes the case-mapping characters listed above and
  passes 10,000 cases locally.
- `cargo clippy -p sqe-sql --all-targets -- -D warnings -D clippy::string_slice` passes.

## Related

- `107-classifier-prefix-dispatch-and-multi-statement-truncation.md` (the
  pre-scan layer these slices live in).
- Codebase analysis 2026-06-26 item 20 (proptest exists) explains why this was
  believed covered.
