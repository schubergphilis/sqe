# DataFusion / Arrow / Iceberg dependency status

Living notes on the query-engine stack pins, Renovate behaviour, dual-version exceptions, and upstream work that may unblock upgrades.

Last reviewed: 2026-07-27 (against `origin/main` + public upstream).

---

## Current pins (SQE workspace)

| Piece | Workspace pin | Lock (approx.) | How managed |
|--------|---------------|----------------|-------------|
| **DataFusion** | `"54"` | **54.1.0** | crates.io, Renovate group `datafusion/arrow stack` |
| **Arrow / Parquet** | `"58"` | **58.4.0** | same group |
| **sqlparser** | `"0.62"` | 0.62.0 | same group (must match DF) |
| **datafusion-functions-json** | `"0.54"` | 0.54.x | must track DF major |
| **object_store** | `"0.14"` (workspace) | **0.14.1 + 0.13.2** | see [Exception: dual object_store](#exception-dual-object_store) |
| **Iceberg** | `path = vendor/iceberg-rust` | RisingWave fork + SQE patches | **not** Renovate-managed |

Source of truth for pins: root `Cargo.toml` `[workspace.dependencies]`. Iceberg vendor notes: `vendor/iceberg-rust/README.md`. Renovate: `renovate.json`.

---

## Renovate: is the stack “solved”?

### What works

- Renovate groups `datafusion`, `datafusion-*`, `arrow`, `arrow-*`, `parquet`, `object_store`, `sqlparser` into one MR (`groupName: datafusion/arrow stack`, label `datafusion-stack`, **no automerge**).
- Skewing those packages independently breaks the build; the group is intentional.
- Latest in-line bump merged as !703: DF **54.0 → 54.1**, Arrow/Parquet **58.3 → 58.4**, plus workspace `object_store` bumped toward **0.14**.
- Routine Rust minor/patch and container images are separate groups.

### What is intentionally out of Renovate

- **`vendor/**` is in `ignorePaths`.** The iceberg fork is path-vendored; bots do not bump it. Upgrades are manual rebases + re-apply SQE patches.
- Apache **Iceberg write APIs** (Overwrite / RowDelta / full CoW) are not on crates.io in a form that replaces the RisingWave fork yet.

### What Renovate still queues (not solved by grouping alone)

| Item | Status | Why blocked |
|------|--------|-------------|
| **Arrow stack major → v59** | Rate-limited on Dependency Dashboard (#379) | Published DF is still **54.x** on Arrow **58**. Arrow 59 alone cannot land. |
| DF 55 / next major | Not on crates.io yet | See [Upstream incoming](#upstream-incoming). |
| Other majors (jsonwebtoken 10, toml 1, zip 8, …) | Rate-limited | Unrelated to the DF stack. |
| Dashboard “Open” for !701 / !703 | Lag | Those MRs are **already merged**; ignore stale open checkboxes until Renovate refreshes. |

---

## Exception: dual `object_store`

**Not fully solved.**

After the datafusion/arrow stack MR:

| Consumer | `object_store` version |
|----------|-------------------------|
| SQE crates (`sqe-catalog`, `sqe-worker`, `sqe-bench`) | **0.14.x** (workspace pin) |
| DataFusion + parquet (transitive) | **0.13.2** |

Cargo keeps **two** copies. The tree often still compiles, but `ObjectStore` types are not interchangeable across the boundary.

**Clean options later:**

1. Re-pin workspace `object_store` to **0.13** until DF/parquet depend on 0.14, **or**
2. Wait for a DataFusion line that depends on 0.14 (upstream PR open: [apache/datafusion#23693](https://github.com/apache/datafusion/pull/23693)) and then bump as one stack.

Also note `deny.toml` / advisories: some `quick-xml` findings are pinned behind object_store’s version range; a real fix wants a newer object_store that accepts fixed quick-xml.

---

## Exception: Iceberg is a custom fork

Vendored from **RisingWave** `dev_rebase_main_20260303` @ **`813e54419b43`**, not stock `apache/iceberg-rust` crates.io.

Why fork (still true):

- Apache was missing production write pieces SQE uses: rewrite/overwrite transactions, position deletes, deletion vectors, etc.
- SQE-only patches on top (DynamicPredicate, DecodeGate #367, bloom SBBF #369, current-schema projection #358, REST sigv4, loader feature gates, …). See `vendor/iceberg-rust/README.md`.

**Exceptions for upgrades:**

- Renovate **will not** open iceberg MRs for the vendor tree.
- Bumping DF/Arrow on SQE without a matching vendor rebase fails.
- Dropping the fork requires Apache CoW/MoR APIs **plus** porting SQE patches.

Fork is **~6 commits ahead** of the vendored baseline (not yet backported; need write/conflict regression):

1. Unify overwrite/rewrite txn  
2. Rewrite-manifests reuse on conflict retry  
3. Stream manifest load in orphan removal  
4. DeleteVector traits  
5. Bound rewrite-manifests memory  

---

## Upstream incoming

### DataFusion / Arrow

| Track | Status | Meaning for SQE |
|--------|--------|-----------------|
| crates.io | DF **54.1.0**, Arrow **58.4** | What you ship today |
| **DF `main`** | Already **Arrow/Parquet 59.1.0**, package still **54.1.0** | [PR #23312](https://github.com/apache/datafusion/pull/23312) merged 2026-07-08 — Arrow 59 is on main before the next crates.io cut |
| **DF 54.2.0** | Release issue open ([#23805](https://github.com/apache/datafusion/issues/23805), 2026-07-25) | Patch/minor; checklist includes testing iceberg-rust |
| **DF 55.0.0** | Planned Jul/Aug 2026 ([#22393](https://github.com/apache/datafusion/issues/22393)) | Next major; early checklist |
| **object_store 0.14** | Open ([#23693](https://github.com/apache/datafusion/pull/23693)) | Would help the dual-version exception when published |

**Exception / caveat:** next DF publish that includes main’s Arrow 59 bump is the real unlock for Renovate’s “arrow → v59”. Until crates.io moves, Arrow 59 stays blocked even though DF main already uses it.

**Exception / caveat:** DF 54.1 already raised **MSRV** (around 1.88+; main higher). SQE workspace still advertises **1.85** — a DF bump may force a toolchain bump.

### Apache Iceberg Rust (official)

| Item | Status | Relevance |
|------|--------|-----------|
| **v0.10.0** | Released 2026-07-21 | Catalogs/IO; **not** a full drop-in for RW CoW/MoR writers |
| **main** | Still Arrow **58.4** + DF **54.1** | Lags DF main on Arrow 59 |
| **OverwriteAction CoW** | Open [#2185](https://github.com/apache/iceberg-rust/pull/2185) | Original “drop the fork” gate |
| **Core COW rewrite primitive** | Active [#2752](https://github.com/apache/iceberg-rust/pull/2752) (updated 2026-07-27) | Hottest CoW progress |
| **RowDelta / DVs** | Open [#2203](https://github.com/apache/iceberg-rust/pull/2203), [#2678](https://github.com/apache/iceberg-rust/pull/2678), [#2785](https://github.com/apache/iceberg-rust/pull/2785) | MoR still maturing |

**Exception:** Apache progress does **not** yet mean “switch off vendor this week.” Need Overwrite/RowDelta landings **and** SQE patch ports.

---

## Decision summary

| Question | Answer |
|----------|--------|
| Can Renovate bump DF/Arrow **within** 54/58 together? | **Yes** — grouped; 54.1 / 58.4 on main |
| Can Renovate bump Arrow **59** alone? | **No** — wait for DF crates.io that depends on Arrow 59 |
| Can Renovate bump Iceberg? | **No** — path vendor + `ignorePaths` |
| Is the stack fully single-versioned? | **Mostly** Arrow/DF; **exception: object_store 0.13 + 0.14** |
| Can we drop the iceberg fork? | **Not yet** — watch #2752 / Overwrite / RowDelta |
| Act this week on majors? | **No forced upgrade.** Optional: re-pin object_store 0.13, or cherry-pick RW fork commits after write tests |

---

## Watch list

1. DataFusion **54.2** (or next publish after main’s Arrow 59 bump) lands on crates.io  
2. DataFusion **object_store 0.14** PR merges and ships  
3. iceberg-rust **#2752** (COW rewrite) + **#2185** / RowDelta  
4. RisingWave fork commits after `813e544` — backport when write/conflict suite is ready  
5. SQE **MSRV** vs DF’s rust-version on next bump  

---

## Related files

- `Cargo.toml` — workspace pins  
- `renovate.json` — grouping and `vendor/**` ignore  
- `deny.toml` — advisory ignores and supply-chain notes for the fork  
- `vendor/iceberg-rust/README.md` — vendor baseline, SQE patches, upstream tracking  
