# Access-control parity demo: EU retail bank fixture

Date: 2026-08-12
Scope: `scripts/access-control-parity-demo.sh` plus the two quickstart stack files
that define demo identities.

## Motivation

The parity demo proves that SQE and Spark/Kyuubi enforce the same Ranger policies
over the same Iceberg table. It does that on a three-row table with columns
`id, region, amount, ssn, email, phone`. The mechanics are complete; the fixture
reads like a unit test, so the transcript does not show what the mechanics are
for.

A regulated bank reviewing this stack wants to see its own vocabulary: a customer
register with national identifiers and IBANs, a payment ledger with cross-border
counterparties and AML alerts, GDPR data-residency scoping, and tags that carry a
governance meaning rather than the name `PII`.

## What changes

### Fixture: two tables, twelve and twenty-four rows

Namespace `sales_wh.acparity` gains two tables in place of `orders`.

```
customers(cust_id BIGINT, full_name VARCHAR, national_id VARCHAR, dob DATE,
          iban VARCHAR, nationality VARCHAR, residency_region VARCHAR,
          branch VARCHAR, consent_marketing BOOLEAN, pep_flag BOOLEAN,
          risk_score INT, phone VARCHAR)

payments(pay_id BIGINT, cust_id BIGINT, booked_at DATE, amount_eur DOUBLE,
         counterparty_iban VARCHAR, counterparty_country VARCHAR,
         channel VARCHAR, aml_alert BOOLEAN, mcc INT)
```

Row counts and the derived numbers every assertion depends on:

| set | rows |
|---|---|
| `customers` | 12 |
| `customers` with `residency_region = 'EU'` | 7 (cust_id 1-7) |
| `customers` with `pep_flag` | 2 (cust_id 6, 10) |
| `payments` | 24 |
| `payments` with `booked_at >= DATE '2019-01-01'` | 18 (pay_id 7-24) |
| `payments` with `aml_alert` | 4 (pay_id 3, 5, 11, 14) |
| `customers JOIN payments` restricted to EU customers | 15 |

No customer has a `dob` falling on 1 January. That is load-bearing: it lets
`MASK_DATE_SHOW_YEAR` be asserted as "every masked row now reports month 1 day 1,
and no unmasked row does" without pinning a date rendering.

### Existing policy targets map over

Sections 1-7 keep their probe count and change only vocabulary.

| old column | new column | role in the demo |
|---|---|---|
| `region` | `residency_region` (`EU` / `NON_EU`) | row filter, now reads as GDPR data residency |
| `amount` | `risk_score` INT | `MASK_NULL`: an internal model output |
| `ssn` | `national_id` (9 digits, no separators) | `MASK_SHOW_LAST_4` |
| `email` | `iban` | `MASK_HASH` as a pseudonymous account key |
| `phone` | `phone` | uncontested tag-mask target, unchanged |

`risk_score` is INT rather than DOUBLE, so no pinned expectation depends on float
rendering. `amount_eur` stays DOUBLE and is never asserted by value.

### Two new mask types, folded into section 3

`dob` gets `MASK_DATE_SHOW_YEAR` and `full_name` gets plain `MASK`. Both add one
comparison between them, not two, and both are asserted by semantics:

- date: two separate claims. `dob_leaks` counts rows still holding a seeded birth
  date and must be 0; `dob_year_only` counts rows reporting 1 January of the right
  year and must be 12. Written as `dob IN (DATE '...', ...)` rather than
  `month()`/`day()`, because those standalone date functions are not portable
  across both engines while a DATE literal is.
- name: masked names contain only `X`, `x`, `n`, and separators, so
  "no row still contains a vowel" holds under either engine's replacement chars.
  SQE leaves separators alone and Kyuubi maps them to `U`; the vowel test is blind
  to that difference, which is the point.

### Two new personas

| role | user | policy shape | story |
|---|---|---|---|
| `fraud_analyst` | erin | tag-driven masks, no row filter | data minimisation: identity hidden, every jurisdiction visible |
| `auditor` | frank | no masks, retention row filter on `payments` only | right of access over a seven-year window, no leak into `customers` |

Adding a persona touches five sites, and missing any of the last three produces
an authorization error indistinguishable from the policy denials this script
asserts deliberately:

1. `quickstart/polaris-ranger-keycloak/keycloak/realm-ranger.json` - realm role
   plus user, password `<username>123` (the convention `token_for` assumes).
2. `polaris/bootstrap-data.sh` - the principal ENTITY. Polaris federation
   resolves an existing principal by `preferred_username` and never creates one,
   so a realm-only user mints a token and then fails every read with 401
   "Failed to resolve principal".
3. `ranger/bootstrap-ranger.sh` - the `for u in root alice bob ...` loop, which
   creates the Ranger-side grantee reference.
4. `ranger/bootstrap-ranger.sh` - `mkrole`, the user-to-role membership Ranger
   actually resolves (Polaris ignores realm roles carried in the token).
5. `ranger/bootstrap-ranger.sh` - the `for role in analyst engineer` baseline
   traverse loop.

Sites 2, 3, and 4 fail in ways that need a preflight to be legible, so the script
carries `preflight_role` (the Ranger grant API's grantee-exists check) and
`preflight_principal` (the Polaris resolution error). Site 1 is already covered by
the startup token loop, and site 5 by the same preflights.

### Two new sections

**8. Data minimisation for the fraud desk.** One tag policy on `ACPARITY_ACCOUNT`
covers `customers.phone` and `payments.counterparty_iban`, proving a tag policy is
table-independent: one rule, two tables, no per-table authoring. `ACPARITY_SPI`
carries GDPR article 9 special-category data on `nationality`, and
`ACPARITY_IDENTITY` carries the direct identifiers `full_name` and `national_id`;
both resolve to `MASK_NULL`. Three tags rather than one because Ranger refuses a
second policy whose resource signature already belongs to another, so one tag
cannot carry two masks for two roles, and because the REASON a column is protected
is what an auditor asks about. Erin sees all 12 customers and all 24 payments with
identity removed. Alice stays the raw control. Three comparisons.

**9. Audit right of access with a retention window.** Frank reads `customers`
unmasked, which proves the mask policies are role-scoped, and reads `payments`
through `booked_at >= DATE '2019-01-01'`, which proves a filter on one table does
not reach the other. The third comparison is the join probe: Bob reads
`customers JOIN payments USING (cust_id)` and gets 15 rows with his customer-side
masks intact, proving policy survives a join. Three comparisons.

Total: seven new Spark probes, five to ten minutes of added wall clock.

### Tag vocabulary

`ACPARITY_PII` (engineer, masked to a fixed token), `ACPARITY_ACCOUNT` (fraud
desk, account identifiers across both tables), `ACPARITY_SPI` (fraud desk, article
9 special category), `ACPARITY_IDENTITY` (fraud desk, direct identifiers),
`ACPARITY_NULL` (the precedence probe, unchanged), `ACPARITY_NO_RULE` (the inert
probe, unchanged). The `ACPARITY_` prefix and the `acparity-demo-` policy prefix
stay: they are what makes teardown safe on a shared Ranger.

### Display panels

Two panels run one query as several users and print each result with the CLI's own
table rendering, through SQE only. A Spark probe costs a JVM start (~7 s) against
~200 ms for an SQE query, and parity is asserted elsewhere, so five perspectives
cost less than one extra comparison. Nothing in a panel asserts, so none can
produce a false green. Section 3 shows all five masks beside the unmasked control;
section 9 shows the same join five ways.

## Calibration: done, 43 of 43 green

Ran against a live quickstart stack on 2026-08-12. Every comparison passed on the
first pass, so each literal originally marked `# CALIBRATE:` was correct. Two
things kept that from having been a guess:

1. Every new assertion is an aggregate over semantics (`count(*)` plus
   `sum(CASE WHEN ...)`), the pattern already used at lines 558 and 587. No new
   pinned digest, float, or date rendering. An engine that does not implement a
   mask at all fails the aggregate loudly, which is the correct signal.
2. Every literal that could not be derived from an already-calibrated value
   carries a `# CALIBRATE:` comment naming what to confirm.

The SQE side of the `MASK_SHOW_LAST_4` rendering is confirmed from source rather
than assumed: `ranger_store.rs` maps it to `PartialMask{show_last: 4, digit: 'x'}`,
so a nine-digit identifier renders `xxxxx9103`. The Spark side (`nnnnn9103`) is
derived from the previously calibrated pair `xxx-xx-1111` / `nnnUnnU1111` and is
marked. `MASK_HASH` emitting 64 lowercase hex characters is likewise confirmed
(`sha256_udf.rs` `hex_lower`), which is why the digest-length divergence keeps its
pinned `64` / `32`.

Two inferences that could each have become a third documented divergence did not,
and both are now asserted rather than commented:

- `MASK_DATE_SHOW_YEAR`: Kyuubi truncates to 1 January exactly as SQE does. Both
  engines returned `dob_year_only = 12`.
- filter and mask over a join: both engines returned `15 | 15 | 15 | 0`, so Kyuubi
  applies a row filter AND column masks to a joined relation the way SQE does.

The derived Spark rendering `nnnnn9103` / `nnnnn0214` was exact.

The run also contradicted one of the design's own captions, which is what running
it is for. The five-way panel labelled carol "every row, every column"; carol is
`sqe_admin` AND `engineer` AND `analyst` in the realm, so the engineer policy
applies to her and she reads the same four masked EU rows Bob does. Being an admin
at the object gate is not an exemption from the data gate. Relabelled to say so.

`AC_PARITY_SECTIONS="3,9,10"` gates the comparisons only. Every `action` still
runs, so grants, policies, and tags that later sections depend on are always in
place; only the Spark JVM starts are skipped. That makes a calibration pass over
the new sections cost minutes instead of the full run.

## Known hazards recorded in the script

- Kyuubi Spark 3.5 raises `MISSING_ATTRIBUTES` (#6889) when a row filter reads a
  column the query does not project. New filter probes project the filtered
  column.
- The Ranger `database` resource is the namespace's last component, so both
  engines resolve `acparity`. Nothing in the fixture may rely on the catalog name.
- `MASK_NONE` is not usable as a break-glass exemption: `ranger_store.rs:497` maps
  it to `Ok(None)`, but masks from other matching policies are unioned in, so an
  exemption needs Ranger evaluation-order priorities that SQE does not implement.
  `access_control_e2e.rs:1747` records the same conclusion.
- Column restriction is not authorable. `restricted_columns` is populated only
  fail-closed, on an unsupported mask type, so "invisible denied columns" cannot
  be demonstrated through this surface.

## Out of scope

`scripts/access-control-demo.sh` keeps its three-row fixture. Extracting a shared
fixture library for both scripts is a separate change.

## Success criteria

- The transcript reads as a bank governance walkthrough: named columns, plausible
  values, tags that state why a column is protected.
- Probe count grows by seven; the two documented divergences stay two.
- `bash -n` clean, `shellcheck` no worse than before.
- Calibrated: 43 of 43 comparisons green on a live stack, first pass, exit 0.
