# Findings — SQL parser, Trino compat/functions, Lineage (`sqe-sql`, `sqe-trino-*`, `sqe-lineage`)

**Scope:** Four crates with security + reliability + correctness weighting, treating all SQL/data as
attacker-controlled: `sqe-sql` (extended parser), `sqe-trino-functions` (Trino scalar UDFs), `sqe-trino-compat`
(Trino HTTP wire adapter), `sqe-lineage` (OpenLineage emitter/sinks). Verified the workspace panic strategy
(`panic = unwind`, no `catch_unwind` in worker/coordinator), traced attacker reachability for every panic, and
read the sqlparser 0.54 infix-parse loop to confirm the recursion-limit bypass. The Trino HTTP auth path (auth
enforced on all data routes, query-result ownership checks, restrictive CORS, version-gated `/v1/info`) and the
lineage channel/spool backpressure are sound and produced no finding.

---

### SQL-01 — high — Deep binary-operator chains bypass the parser recursion limit and stack-overflow the coordinator inside the Trino-compat AST walk

- **Dimension:** reliability
- **Status:** NEW surface (distinct, earlier, unguarded trigger site from the documented `in_subquery_or_stack_overflow` DataFusion-side reproduction; this one fires in `sqe-sql` before DataFusion is reached)
- **Location:** `crates/sqe-sql/src/trino_compat.rs:90` (the walk), reached from `crates/sqe-coordinator/src/query_handler.rs:1674` and `:1828`
- **Evidence:**
  ```rust
  // trino_compat.rs:89-90
  let mut visitor = TrinoCompatVisitor::default();
  let _ = statements.visit(&mut visitor);   // recursive VisitMut over the Expr tree
  ```
  ```rust
  // sqlparser-0.54.0 parse_subexpr: ONE recursion-counter decrement, then infix operators
  // consumed in a LOOP, building a depth-N left-leaning tree (A OR B OR C -> Or(Or(..),C)).
  let _guard = self.recursion_counter.try_decrease()?;
  let mut expr = self.parse_prefix()?;
  loop { ... expr = self.parse_infix(expr, next_precedence)?; }
  ```
- **Impact:** A flat chain like `a OR a OR ... OR a` of N terms parses cleanly. the recursion counter (default 50)
  is NOT consumed by the infix loop, so the depth-N tree is built without error. `rewrite_trino_compat` then walks
  it with sqlparser's derived recursive `VisitMut`, recursing N frames deep and overflowing the coordinator's
  8 MiB stack at roughly 16k-32k terms. A stack overflow is an OS-level abort, not a catchable panic, so it kills
  the whole coordinator process and every concurrent query on it. The fast-path skip only avoids the walk when the
  SQL contains none of `$`, `as json`, `rollup`/`cube`/`grouping sets`. an attacker simply embeds a `$` anywhere
  to force the recursive walk. A 4 MB Flight message holds ~800k `a OR ` terms, far past the overflow threshold.
- **Fix:** Before `statements.visit(...)`, enforce an explicit AST/expression depth bound (an iterative pre-pass
  that rejects expression trees deeper than a few hundred, or run the rewrite on a dedicated thread with a bounded
  stack and treat overflow as a clean parse error). The robust fix is a global expression-depth guard applied to
  all attacker-supplied SQL before any recursive visitor runs, since the same tree later also overflows
  DataFusion's analyzer.
- **Effort:** medium

---

### SQL-02 — medium — `from_base`/`to_base` panic or infinite-loop on out-of-range radix

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-trino-functions/src/trino_functions_ext.rs:425` (`from_base`), `:478-479` (`to_base`)
- **Evidence:**
  ```rust
  // from_base (line 420-425): radix taken from a scalar arg, NO range check
  let radix = match v { ScalarValue::Int64(Some(r)) => *r as u32, ... };
  let result = i64::from_str_radix(s, radix).ok();   // PANICS if radix not in 2..=36
  ```
  ```rust
  // to_base (line 458-479): same unchecked radix
  let d = (num % radix as u64) as usize;          // radix=0 -> modulo-by-zero PANIC
  s.push(digits.as_bytes()[d] as char);           // radix>36 -> index out of bounds PANIC
  num /= radix as u64;                             // radix=1 -> num never decreases -> INFINITE LOOP
  ```
- **Impact:** Reachable through scalar/literal arms (array arms return NULL), e.g. `from_base('10', 0)`,
  `to_base(5, 0)`, `to_base(5, 40)`, `to_base(5, 1)`. `i64::from_str_radix` panics for radix outside 2..=36;
  `to_base` panics on modulo-by-zero and index-out-of-bounds; radix 1 spins forever pinning a CPU core. A negative
  radix wraps via `*r as u32` to a huge value, also out of bounds. Unwind kills the running query/connection;
  radix 1 is a per-query CPU-pinning hang.
- **Fix:** Validate `radix` is in `2..=36` (reject the radix-1 infinite loop) and return a `DataFusionError::Plan`
  ("Radix must be between 2 and 36"), matching Trino.
- **Effort:** trivial

---

### SQL-03 — medium — `from_hex`/`from_utf8` panic on non-ASCII string data (char-boundary slice)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-trino-functions/src/trino_functions.rs:2139` (`from_hex`), `:2159` (`from_utf8`)
- **Evidence:**
  ```rust
  // step by 2 over BYTE length of a &str, then slice the &str -> panics on non-char-boundary
  let bytes: Vec<u8> = (0..s.len()).step_by(2)
      .filter_map(|i| if i + 2 <= s.len() { u8::from_str_radix(&s[i..i + 2], 16).ok() } else { None })
      .collect();
  ```
- **Impact:** `s` is a `&str` from a string column value (data-reachable). When the value contains a multi-byte
  UTF-8 character, `&s[i..i+2]` can land inside a character and panic with "byte index N is not a char boundary".
  Triggerable from any row whose value is non-ASCII; an attacker controlling table data or a literal panics the
  query per batch.
- **Fix:** Operate on `s.as_bytes()` and slice the byte slice (`&s.as_bytes()[i..i+2]`), or validate ASCII first.
- **Effort:** trivial

---

### SQL-04 — medium — Date/time extract UDFs `.unwrap()` on `date32_to_datetime`, panicking on extreme Date32 values

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-trino-functions/src/trino_functions.rs:212` (array), `:237` (scalar); same pattern at `:732`, `:862`
- **Evidence:**
  ```rust
  // days comes from a Date32 column value
  let date = temporal_conversions::date32_to_datetime(days).unwrap().date();   // None -> PANIC
  ```
- **Impact:** `date32_to_datetime` returns `Option` and yields `None` when the day count overflows the
  representable `NaiveDateTime` range. A Date32 value near `i32::MAX` (e.g. `year(CAST(2000000000 AS DATE))`)
  panics. `extract_component` backs every Trino date-part function (year/month/day/quarter/...) via the macro, so
  the panic is data-reachable across the whole family from one crafted value or table cell.
- **Fix:** Replace `.unwrap()` with a checked path: map `None` to a NULL output (or `DataFusionError::Plan`),
  consistent with how the Time64 arm at line 242 already handles `None`.
- **Effort:** small

---

### SQL-05 — medium — Per-row regex recompilation with no size limit or cache in `regexp_extract`/`_all`/`_split`

- **Dimension:** performance
- **Status:** NEW surface
- **Location:** `crates/sqe-trino-functions/src/trino_functions_ext.rs:810`, `:966` (compile inside the row loop)
- **Evidence:**
  ```rust
  // regexp_extract via str_transform_2: f(s, pattern) runs PER ROW, compiling each time
  let re = regex::Regex::new(pattern).ok()?;          // line 810
  // build_regex_list_array: compile inside for i in 0..n
  let re = regex::Regex::new(p).map_err(...)?;        // line 966
  ```
- **Impact:** Even when the pattern argument is a constant scalar (`regexp_extract(col, 'fixed')`), the regex is
  recompiled once per row because compilation lives inside the per-row closure/loop. On a 10M-row scan that is
  10M compiles of the same pattern. No `RegexBuilder::size_limit`/`dfa_size_limit` cap, so a single crafted
  pattern can compile near the `regex` crate's 10 MB default and make each of millions of compiles expensive.
  CPU/compute-cost amplification on the worker (not classic ReDoS. the `regex` crate is linear-time), but a
  malicious query can pin worker cores and inflate spend.
- **Fix:** Compile the pattern once outside the row loop when the pattern column is constant, or keep a small
  bounded LRU of compiled regexes keyed by pattern. Build via `RegexBuilder` with an explicit `size_limit`
  (e.g. 1 MB).
- **Effort:** small

---

### SQL-06 — medium — Lineage emits the RAW (unsanitized) error message to lineage sinks

- **Dimension:** security
- **Status:** REGRESSION of the resolved "error sanitization" finding, on the lineage path
- **Location:** `crates/sqe-coordinator/src/query_handler.rs:1383` (raw error in) -> `crates/sqe-lineage/src/emitter.rs:92`, `:111-124` (`ErrorMessageFacet.message`)
- **Evidence:**
  ```rust
  // query_handler.rs:1383 — RAW error to lineage, vs client_message() used everywhere else
  error_message: e.to_string(),
  ```
  For contrast, the tracker path sanitizes: `crates/sqe-coordinator/src/query_tracker.rs:214` uses
  `error.client_message()`.
- **Impact:** The resolved audit established that raw DataFusion/iceberg errors must never leave the server (only
  `client_message()` is exposed; details stay in server logs). The lineage emitter bypasses that: `e.to_string()`
  (raw error, which can carry file paths, S3 URIs, schema/column names, partition values, sometimes literal data
  fragments) is serialized into the OpenLineage `ErrorMessageFacet` and shipped to every configured sink (JSONL
  file, HTTP collector/Marquez). Anyone with read access to the lineage sink (a different trust boundary than the
  SQL client) sees internal error detail the sanitization layer was built to suppress.
- **Fix:** Pass `e.client_message()` (not `e.to_string()`) at `query_handler.rs:1383`, mirroring
  `query_tracker.rs:214`.
- **Effort:** trivial

---

### SQL-07 — low — Lineage SQL redaction is pattern-only; generic PII in literals reaches the sink

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-lineage/src/emitter.rs:138` -> `crates/sqe-metrics/src/audit.rs:24` (`redact_pii`)
- **Evidence:**
  ```rust
  // emitter.rs:138 — only regex-pattern redaction applied to the full SQL
  query: sqe_metrics::audit::redact_pii(sql),
  // audit.rs: redact_pii only matches email / SSN / phone / card / secret-keyword shapes
  ```
- **Impact:** The full query text is shipped to lineage sinks after `redact_pii`, which only catches
  email/SSN/phone/card/secret-keyword shapes. Free-form sensitive literals such as
  `WHERE patient_id = 'P-998877'` or `WHERE diagnosis = 'HIV positive'` pass through verbatim into the
  OpenLineage `SqlFacet.query` on every file/HTTP sink. Lower severity than SQL-06 because some redaction exists
  and the SQL hash is also emitted, but the residual PII leak to a separate trust boundary is real.
- **Fix:** For the lineage SQL facet, prefer emitting only the query hash plus literal-stripped SQL (replace
  string/number literals with placeholders), or make literal emission opt-in defaulting to off. Document that
  `redact_pii` is best-effort pattern matching, not a guarantee.
- **Effort:** small

---

### SQL-08 — low — Classifier prefix pre-scan slices the original string by the uppercased keyword length

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-sql/src/classifier.rs:222` (`let upper = trimmed.to_uppercase()`), used at `:31`, `:38`, `:246`, `:257`, `:269`, `:349`
- **Evidence:**
  ```rust
  let upper = trimmed.to_uppercase();
  if upper.starts_with("SHOW EFFECTIVE GRANTS FOR USER ") {
      let user = trimmed["SHOW EFFECTIVE GRANTS FOR USER ".len()..]  // slices ORIGINAL by keyword byte-len
  ```
- **Impact:** Match is tested on `upper` (uppercased) but the slice offset is applied to `trimmed` (original).
  `to_uppercase()` can change byte length for some Unicode code points, so the matched-prefix length on `upper`
  can differ from the byte span in `trimmed`, and slicing `trimmed[N..]` could land on a non-char-boundary and
  panic. In practice the keywords are pure ASCII prefixes, which constrains the leading bytes. no concrete crash
  was constructed, so this is defense-in-depth/correctness hardening.
- **Fix:** Compute the prefix test and the slice against the same string. Use `trimmed.get(N..)` (returns
  `Option`, no panic), or `strip_prefix` with a case-insensitive helper on `trimmed`.
- **Effort:** small
