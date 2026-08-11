# Upstream upgrade feasibility, 2026-08-10

Read-only research. Every version claim below is quoted from a live fetch
(crates.io API, GitHub API, raw.githubusercontent.com) or from `cargo` output
on this checkout, on 2026-08-10. No training-data recall.

Method notes: all `cargo update` runs used `--dry-run` and wrote nothing.
`Cargo.lock` shows as modified in `git status`, but it was already modified
at session start (branch work in progress on
`test/access-control-policy-composition`); this analysis did not touch it.

## Verdict table

| Dependency | Current (pin / locked) | Candidate | Verdict | Blocker |
|---|---|---|---|---|
| datafusion | "54" / 54.1.0 | none | blocked | 54.1.0 is the newest DataFusion on crates.io (`max_stable_version` and `newest_version` both 54.1.0). There is nothing newer to move to. |
| arrow / arrow-flight / arrow-ipc / parquet | "58" / 58.4.0 | 59.2.0 | blocked | datafusion 54.1.0 requires `arrow ^58.3.0`, `parquet ^58.3.0`. Probe `cargo update -p arrow --precise 59.2.0 --dry-run` fails resolution. |
| object_store | "0.13" / 0.13.2 | 0.14.1 | blocked | parquet 58.4.0 and datafusion 54.1.0 both require `object_store ^0.13.2`. Probe `--precise 0.14.1` fails resolution. |
| sqlparser | "0.62" / 0.62.0 | none | blocked | 0.62.0 is the newest sqlparser on crates.io, and it is exactly what DF 54.1.0 requires (`^0.62.0`). Derived axis, already at ceiling. |
| datafusion-functions-json | "0.54" / 0.54.2 | none | blocked | 0.54.2 is the newest release on crates.io. No 0.55+ exists for any future DF major. |
| jiter [patch.crates-io] | vendor/jiter (0.15.0 + pyo3 0.29) | drop patch | blocked | datafusion-functions-json 0.54.2 still requires `jiter ^0.15.0`; upstream jiter 0.15.0 pins pyo3 0.28.2 (RUSTSEC-2026-0176). Patch must stay. |
| tonic | "0.14" / 0.14.6 | none | blocked | 0.14.6 is the newest tonic on crates.io. Already there. |
| prost | "0.14" / 0.14.4 | none | blocked | 0.14.4 is the newest prost on crates.io. Already there. |
| iceberg (vendor, RW fork) | dev_rebase_main_20260303 @ 813e544 + 3 backports | same-branch head 6bd8e98 (2026-08-03), or branch dev_rebase_main_20260807 @ 4cbd183 | published-only | Exists upstream; not resolution-verified (path dep, needs a vendor refresh + build). Neither candidate moves DF past 54. |
| toml | "0.9" / 0.9.12 | 1.1.4 | published-only | Semver-major; `cargo update` will not take it. Needs a Cargo.toml req change and API check. |
| jsonwebtoken | "9" / 9.3.1 | 11.0.0 | blocked | Held by documented decision in Cargo.toml (crypto-provider runtime panic, !738). Not a resolution blocker. |
| rand | "0.8" / 0.8.7 | 0.10.2 | blocked | Held by documented decision in Cargo.toml (seeded generator output stability, #390). Not a resolution blocker. |
| 64 transitive patch/minor updates | see gate 7 | latest compatible | published-and-resolves | Plain `cargo update --dry-run` locks all 64 with no conflict. |

Bottom line: the entire DataFusion orbit (DF, arrow, parquet, object_store,
sqlparser, datafusion-functions-json, tonic, prost) is already at the newest
published versions that exist and resolve together. There is no DF upgrade
available today from any direction. The only real upstream moves are a vendor
refresh of iceberg-rust (same DF) and a batch of low-risk patch/minor bumps.

## Gate 1: RisingWave iceberg-rust fork branches

`GET https://api.github.com/repos/risingwavelabs/iceberg-rust/branches?per_page=100`

Branches matching `dev_rebase_main_*` (non-archive):

| Branch | Head sha | Head date | Head message |
|---|---|---|---|
| dev_rebase_main_20250307 / 20250325 / 20250808 / 20251111 | older | older | not candidates |
| dev_rebase_main_20260303 (current vendor base) | 6bd8e98fe920 | 2026-08-03 | fix(arrow): propagate equality delete load failures (#196) |
| dev_rebase_main_20260728 | e1880986f749 | 2026-07-28 | feat(scan): complete engine compatibility APIs |
| dev_rebase_main_20260807 | 4cbd183953f8 | 2026-08-07 | fix(test): configure storage for manifest integration tests |

Pins quoted from each newer branch's root `Cargo.toml`
(raw.githubusercontent.com):

- dev_rebase_main_20260728: `datafusion = "54.1.0"`, `arrow = "58.4"` (and all
  arrow-* at 58.4), `parquet = "58.4"`. No `object_store` pin in the root
  Cargo.toml; iceberg-rust does storage IO through OpenDAL.
- dev_rebase_main_20260807: identical DF/arrow/parquet pins
  (`datafusion = "54.1.0"`, arrow/parquet 58.4). Workspace
  `version = "0.10.0"`, `rust-version = "1.94"`. The repo toolchain is 1.97.1
  (rust-toolchain.toml), so that is satisfied; note the SQE workspace still
  declares `rust-version = "1.85"`, which a 1.94-floor path dep would
  effectively raise.

So: newer branches exist, but no fork branch pins DataFusion past 54. The two
2026-07/08 rebases land on exactly the DF 54.1.0 / arrow 58.4 combo SQE
already uses. The fork does not open a DF 55 path because no DF 55 exists
(gate 4).

Nuance on the current vendor: the 20260303 branch's own root Cargo.toml (both
at the vendored commit 813e544 and at head 6bd8e98) pins
`datafusion = "53.0.0"`. The SQE vendored copy carries a local bump to
`datafusion = "54"` (documented in vendor/iceberg-rust/README.md). The
20260728/20260807 branches pin DF 54.1.0 natively, so a branch move would
retire that local divergence.

Vendor freshness: compare `813e544...dev_rebase_main_20260303` shows the
branch is 12 commits ahead of the vendored baseline. The vendor README's last
audit (2026-07-30, tip ac90a10d) already covers 11 of them and backported 3
(#187, #188, #190). One commit is new since that audit:
`6bd8e98f 2026-08-03 fix(arrow): propagate equality delete load failures
(#196)`. That one is unaudited.

## Gate 2: datafusion-functions-json

`GET https://crates.io/api/v1/crates/datafusion-functions-json`

Published versions (top of list): 0.54.2, 0.54.1, 0.54.0, 0.53.1, 0.53.0, ...

There is NO release beyond the 0.54 line. If any future fork branch moved to
a DF major past 54, datafusion-functions-json would block it until a matching
0.5x release appears, or the crate is dropped or vendored. Today this gate is
moot because no DF past 54 exists anywhere (gate 1 and gate 4), but it is the
first thing to re-check when DF 55 ships. SQE locks 0.54.2, which is the
newest.

## Gate 3: jiter patch

`GET https://crates.io/api/v1/crates/datafusion-functions-json/0.54.2/dependencies`
quotes `jiter ^0.15.0` (0.54.0 wanted `^0.13.0`; 0.54.2 tightened to 0.15).

jiter on crates.io: 0.16.0 and 0.15.0 both published, neither yanked. The
patch is not about availability. vendor/jiter/README.SQE.md documents the real
reason: upstream jiter 0.15.0 declares optional `pyo3 = "0.28.2"`, which lands
in Cargo.lock and trips scanners on RUSTSEC-2026-0176 (fixed in pyo3 >= 0.29).
jiter 0.16 uses pyo3 0.29 but does not satisfy `^0.15.0`. The vendored copy is
jiter 0.15.0 with the pyo3 pins moved to 0.29.0. `cargo tree -i pyo3` returns
"nothing to print": pyo3 is lock-only, never built.

Verdict: the `[patch.crates-io] jiter = { path = "vendor/jiter" }` block
cannot be dropped while datafusion-functions-json requires `^0.15.0`. Drop it
when that crate moves to jiter >= 0.16 (its own README says the same).

## Gate 4: latest published DF / arrow / parquet / object_store

crates.io `max_stable_version` on 2026-08-10:

- datafusion: 54.1.0 (newest_version also 54.1.0, so no pre-release either)
- arrow: 59.2.0
- parquet: 59.2.0
- object_store: 0.14.1
- arrow-flight: 59.2.0
- sqlparser: 0.62.0
- tonic: 0.14.6
- prost: 0.14.4

Cargo.lock already holds datafusion 54.1.0, arrow/parquet/arrow-flight 58.4.0,
object_store 0.13.2, sqlparser 0.62.0, tonic 0.14.6, prost 0.14.4.

Can the fork reach arrow 59 / object_store 0.14? No. The binding constraint is
DataFusion itself, quoted from
`https://crates.io/api/v1/crates/datafusion/54.1.0/dependencies`:
`arrow ^58.3.0`, `parquet ^58.3.0`, `object_store ^0.13.2`,
`sqlparser ^0.62.0`. Arrow 59 needs a DataFusion release built against it, and
none is published. Resolution probes confirm:

```
cargo update -p arrow --precise 59.2.0 --dry-run
error: failed to select a version for the requirement `arrow = "^58"`
cargo update -p object_store --precise 0.14.1 --dry-run
error: failed to select a version for the requirement `object_store = "^0.13"`
```

(Both probes fail first at SQE's own `^58` / `^0.13` workspace reqs; loosening
those would only move the failure to DF 54.1.0's `^58.3.0` / `^0.13.2` reqs
quoted above.)

## Gate 5: sqlparser (derived)

datafusion-sql 54.1.0 declares `sqlparser ^0.62.0` (crates.io dependencies
endpoint). SQE pins 0.62 and locks 0.62.0, which is also the newest sqlparser
on crates.io. Nothing to do; re-derive on the next DF bump, as the Cargo.toml
comment already instructs.

## Gate 6: the coupled bundle

The bundle moves as one unit, keyed on DF:

| Crate | Today (DF 54) | Next bundle (when a DF on arrow 59 ships) |
|---|---|---|
| datafusion / -expr / -proto | 54.1.0 | unpublished |
| arrow, arrow-flight, arrow-ipc, arrow-schema, arrow-array, arrow-buffer | 58.4.0 | 59.2.0 exists now |
| parquet | 58.4.0 | 59.2.0 exists now |
| object_store | 0.13.2 | 0.14.1 exists now |
| tonic / prost | 0.14.6 / 0.14.4 | unchanged: arrow-flight 59.2.0 still requires `tonic ^0.14.1`, `prost ^0.14.1` |
| sqlparser | 0.62.0 | whatever the new DF's datafusion-sql requires |
| datafusion-functions-json | 0.54.2 | must wait for a matching release (gate 2) |

Stale comment noted, not fixed: workspace Cargo.toml line 106 says
"arrow-flight 57 requires tonic 0.14" while the pin is 58. The constraint
itself is still accurate for 58 (arrow-flight 58.4.0 requires
`tonic ^0.14.1`); only the version number in the comment is stale.

## Gate 7: independent low-risk deps

`cargo update --dry-run` (plain, from repo root): "Locking 64 packages to
latest compatible versions", zero conflicts, all patch/minor. Highlights among
them: tokio 1.52.3 -> 1.53.1, regex 1.12.4 -> 1.13.1, moka 0.12.15 -> 0.12.16,
thiserror 2.0.19 -> 2.0.20, bytes 1.11.1 -> 1.12.1, http 1.4.2 -> 1.5.0,
hyper 1.10.1 -> 1.11.0, uuid 1.23.3 -> 1.24.0, zeroize 1.8.2 -> 1.9.0,
clap 4.6.4 -> 4.6.6, aws-sdk-glue 1.149 -> 1.159, aws-lc-rs 1.17 -> 1.18,
insta, wasm-bindgen family, rkyv, zerocopy. Verdict for the whole batch:
published-and-resolves.

Semver-major gaps that `cargo update` will NOT take, from the same output:

- toml 0.9.12 -> 1.1.4: the only unforced direct-dep major. published-only.
- jsonwebtoken 9.3.1 -> 11.0.0: held by documented decision (crypto provider
  footgun). Leave it.
- rand 0.8.7 -> 0.10.2: held by documented decision (seeded bench-data
  stability). Leave it.
- object_store 0.13.2 -> 0.14.1: DF-orbit, see gate 6.
- rusqlite 0.39.0 -> 0.40.2 and rustyline 15 -> 18: transitive
  (datafusion-cli orbit), not SQE-direct.
- bitvec 1.0.1 -> 1.1.1 and bstr, cc, object: transitive, follow their
  parents.

Direct deps confirmed already at their latest published major
(crates.io `max_stable_version`): reqwest 0.13.4, axum 0.8.9, sqlx 0.9.0,
moka 0.12.16, dashmap 6.2.1, governor 0.10.4, opentelemetry 0.32.0,
sha2 0.11.0, base64 0.23.1, zip 8.6.0, prometheus 0.14.0, wiremock 0.6.5.

## What can actually move today

1. `cargo update` for the 64 compatible bumps. published-and-resolves.
2. Vendor refresh: backport `6bd8e98f` (#196, equality delete load failure
   propagation) onto the vendored 20260303 snapshot, or evaluate a branch move
   to dev_rebase_main_20260807 (iceberg 0.10.0 base, DF 54.1 native,
   rust-version 1.94 floor). published-only; needs a build plus the write
   regression suite either way.
3. toml 0.9 -> 1.x if anyone wants it. published-only, unforced.
4. Nothing in the DF/arrow orbit. Re-check gates 1, 2, and 4 when DataFusion
   publishes a release on arrow 59.
