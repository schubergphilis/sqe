# Sovereign by Design: Whole-Book Editorial and Continuity Review

Consolidated from 27 per-chapter findings files and `analytics.md`. Selected claims
verified against the live chapter sources and adjacent docs (noted inline). This is a
working editorial document, not a book report.

---

## 1. Executive summary

The book is in strong shape and ready for a copy-edit pass, not a rewrite. Voice is the
least of the problems: every one of the 27 chapters grades A or A- against the voice guide,
mechanics are clean (no prose emdash/endash/unicode-arrow violations, no stray exclamation
marks in prose), and the signature rhythm and dead-end honesty are consistent throughout.
Voice is not the risk.

The risks are three, in order:

1. **A live AWS account id still leaks in the published-docs tree.** The ebook chapter that
   the review snapshot flagged (ch16c) has already been sanitized, but the real id
   `311141556126` plus live region/table names still sit in `docs/roadmap.md` and two
   `docs/book/` pages. Publication blocker.
2. **Two unfinished AI-Logbook placeholders** (ch07, ch10) and **one grammar-broken
   AI-Logbook block** (ch03 L602, damaged by a prior mechanical emdash-removal pass).
3. **Numeric and continuity drift** accumulated from months of incremental writing: a
   *systematic* "Chapter 8 vs Chapter 9" cross-reference off-by-one for the entire policy
   chapter, a JWT-validation contradiction in ch04, binary-size figures that disagree
   (47/80/180 MB), DataFusion version drift, and ~half a dozen intra-chapter counts that do
   not match their own tables.

Fix the leak and the placeholders, run the cross-reference and number-reconciliation
passes, and the manuscript is publishable.

---

## 2. Verdict at a glance

| Chapter | Grade | Voice issues | Continuity flags | Biggest issue |
|---|---|--:|--:|---|
| 00-preface | A- | 6 | 0 | "changed the game" idiom (L29); wall-of-text ack (L119) |
| 01-the-catalog-wars | A- | 5 | 1 | "see Chapter 8" for policy should be ch9 (L279) |
| 02-tables-made-of-files | A- | 6 | 0 | "Why Rust Changes the Game" header (L418); diluted close (L489) |
| 03-the-engine-you-already-have | A- | 5 | 4 | Grammar-broken AI-Logbook (L602); DF 53.1 vs 54 drift |
| 04-you-are-the-query | A- | 6 | 2 | JWT "no signature verification" (L158) vs "verifies" (L381) |
| 05-speaking-arrow | A | 3 | 2 | method count 24/25/"20+"; type count 13 vs "fifteen" |
| 06-the-catalog-is-the-api | A | 4 | 1 | "one line / two hours" debug anecdote variant (L332) |
| 06b-speaking-to-many-catalogs | A | 2 | 1 | 80 MB slim build vs ch16d's 180/70 MB |
| 06c-attaching-at-runtime | A | 4 | 1 | cites "chapter 9" correctly; verify numbering at build |
| 07-making-dbt-work | A- | 6 | 1 | **Unfinished `.ailog` placeholder (L148)** |
| 08-writing-is-a-contract | A- | 5 | 2 | "one day / three days to debug" (L222) vs "four hours" elsewhere |
| 09-what-you-cant-see | A- | 7 | 2 | Implementation-Status table "Implemented" vs prose "designed" |
| 09b-grant-that-polaris-enforces | A | 3 | 1 | INSERT "22 types" table vs "twenty-three" prose |
| 09c-one-policy-two-engines | A | 5 | 0 | "it is worth pinning" near-miss (L15) |
| 10-making-it-operable | A- | 7 | 2 | **Unfinished `.ailog` placeholder (L400)**; config keys 47 vs 45 |
| 11-why-distribute-at-all | A | 3 | 0 | meta throat-clear (L259); NP-hard clause (L299) |
| 12-standing-on-ballistas-shoulders | A- | 4 | 1 | "N lines / N days" beat 4x; "Nth lesson" listicle |
| 13-neither-trusts-the-other | A | 3 | 3 | coordinator "never touches S3" vs runs local scan |
| 14-failure-is-a-feature | A- | 8 | 1 | ~5 back-referring "This" openers; rhetorical-Q transition (L555) |
| 15-deploying-sovereignty | A- | 5 | 1 | duplicate epigraph bookend (L618); health-port 9091 vs 9090 |
| 16-benchmarks-dont-lie | A- | 6 | 6 | "40% faster / 30% slower" (L631) absent from all tables |
| 16b-the-matrix-and-the-quiet-bug | A | 3 | 2 | 63 cells vs 189 points; 11 vs 13 tests unreconciled |
| 16c-following-through | A- | 5 | 1 | **Live AWS account id in prose (sanitized in ch; leaks elsewhere)** |
| 16d-the-duckdb-drift | A- | 4 | 1 | binary 180 MB / 70 MB minimal vs ch06b 80 MB; garbled L196 |
| 16e-the-lineage-trail | A | 3 | 1 | "3 AM answer" motif slightly over-spent |
| 17-what-wed-do-differently | A- | 6 | 3 | "six benchmark suites" (L50) vs seven; "16 vs 15 chapters" (L7/L149) |
| 18-epilogue | A | 1 | 2 | "twelve crates" snapshot; display-formatter bug anchor |

Voice-issue counts are the number of itemized entries in each findings file (positives that
reviewers flagged as "keep" are excluded). Straight-A chapters: 05, 06, 06b, 06c, 09b, 09c,
11, 13, 16b, 16e, 18.

---

## 3. P0 — Publication blockers

Fix all four before any release.

### P0.1 Live AWS account id and live identifiers leak in the docs tree

**Status correction vs the review snapshot.** The dispatcher flagged ch16c L101. That line
has *already* been sanitized: `16c-following-through.md` L101 now reads account
`123456789012` (the AWS documentation placeholder). The findings file (16c L21/L56) captured
the pre-sanitization state. Good. **But the real id still leaks in three other files**,
verified by repo-wide grep:

| File:line | Leaked content |
|---|---|
| `docs/roadmap.md:60` | `live test against AWS account 311141556126 in eu-central-1` |
| `docs/book/src/features/iceberg.md:58` | `live-verified (2026-05-05) against AWS account 311141556126 in eu-central-1 and eu-west-1` |
| `docs/book/src/getting-started/catalogs.md:25, 244, 300` | account `311141556126`; ARN `arn:aws:s3tables:eu-west-1:311141556126:bucket/testtablebucket` |

**Problem:** `311141556126` is a real AWS account number. Coupled with the real region
(`eu-central-1`/`eu-west-1`), database (`iceberg_demo_analytics`), table
(`iceberg_user_events`), and bucket (`testtablebucket`), it is a publication leak and a
reconnaissance gift.

**Fix:** Replace every occurrence of `311141556126` with the AWS docs placeholder
`123456789012` (matching what ch16c, ch04 L426, ch06b L234 already use). Decide whether the
live database/table/bucket names should also be genericized; at minimum sanitize the account
id everywhere. Then re-run the book leak-scan so this class is caught next time (per the
known `leak-scan.sh` gap that misses bare account ids). **Do NOT touch** `123456789012`
occurrences (ch04 L426, ch06b L234) — those are already the safe placeholder.

### P0.2 Unfinished AI-Logbook placeholder — ch07

**File:** `07-making-dbt-work.md:148`
**Problem:** `*[To be completed by AI Logbook agent]*` — an empty `.ailog` callout shipped in
the chapter body. (The `.ailog` callout style itself is legitimate and appears throughout;
this specific block is unwritten.)
**Fix:** Write the AI-Logbook entry (the chapter's natural subject: the AI implementing the
UUID file-naming fix / the 17 Trino UDF aliases) or delete the empty callout.

### P0.3 Unfinished AI-Logbook placeholder — ch10

**File:** `10-making-it-operable.md:400`
**Problem:** `*[To be completed by AI Logbook agent]*` — same empty-callout issue.
**Fix:** Write it (subject: the config-validation / circuit-breaker / 43-finding-audit work)
or delete the callout.

These two (ch07, ch10) are the only real unfinished placeholders in the book.

### P0.4 Grammar-broken AI-Logbook block — ch03

**File:** `03-the-engine-you-already-have.md:602`
**Problem:** A prior mechanical emdash-removal pass turned an appositive into sentence
fragments. Current text:

> The `create_session_context` method. one `SessionContext` per user with per-session
> credentials. was specified by the human as the architectural constraint; ...

The two periods around the appositive were once emdashes; now the sentence is
ungrammatical.
**Fix:** Restore as commas:

> The `create_session_context` method, one `SessionContext` per user with per-session
> credentials, was specified by the human as the architectural constraint; the AI
> implemented it correctly because Rust's ownership model made the isolation boundaries
> explicit in the type signatures.

This is the only confirmed grammar casualty of the emdash-removal pass; worth a quick grep
for other "word. lowercase-word." fragments to be safe.

---

## 4. P1 — Factual / continuity contradictions (credibility risks)

Cross-chapter matrix mined from every "Key factual/numeric claims" and "Concepts" section,
then verified against source where load-bearing.

### P1.0 SYSTEMATIC: "Chapter 8" cross-references point at the wrong chapter (highest priority in P1)

`08-writing-is-a-contract.md` is the **write-path** chapter. `09-what-you-cant-see.md` is the
**policy / security** chapter (row filters, column masks, `PolicyPlanRewriter`). Every
back/forward reference to the *policy* material cites "Chapter 8" — it should be Chapter 9.
The tell: the later-written 09b/09c cite "chapter 9" for the *same* material. Verified by
grep against the chapter sources:

| Location | Current text | Should be |
|---|---|---|
| `01-the-catalog-wars.md:279` | "a custom policy engine ... (which is what we did in Chapter 8)" | Chapter 9 |
| `03-the-engine-you-already-have.md:59` | "More on this in Chapter 8." (policy filters in logical plan) | Chapter 9 |
| `04-you-are-the-query.md:158` | "policy enforcement (Chapter 8)" | Chapter 9 |
| `05-speaking-arrow.md:611` | "The security model (covered in Chapter 8)" | Chapter 9 |
| `13-neither-trusts-the-other.md:16` | "enforces policies (Chapter 8)" | Chapter 9 |
| `13-neither-trusts-the-other.md:95` | "Policy enforcement happens on the logical plan (Chapter 8)" | Chapter 9 |
| `17-what-wed-do-differently.md:324` | "Chapter 8 described the `PolicyPlanRewriter`" | Chapter 9 |

And the inverse, also confirmed:

| Location | Current text | Should be |
|---|---|---|
| `17-what-wed-do-differently.md:326` | "Chapter 7 described the Iceberg commit mechanism" | Chapter 8 (the commit mechanism is in `08-writing-is-a-contract.md`) |

**Recommended resolution:** One find-and-replace pass on these eight citations. If the build
uses lettered sub-chapters (09b/09c) and the author wants "Chapter 9" to mean the whole
policy cluster, that is fine — but the numbers above are simply wrong as written and send a
reader from a masks/row-filters promise to the write-path chapter. Do this before the number
reconciliations below; it is the cleanest high-impact fix in the book.

### P1.1 JWT signature: not validated vs validated (ch04)

`04-you-are-the-query.md` L158: roles are extracted via "a lightweight base64 decode of the
claims, **no signature verification**, because the OIDC provider already validated the
token," and the L164 antipattern callout says "Don't validate JWTs in the engine." But the
appended "Ten Ways" section, L381, says `BearerTokenProvider` "fetches the JWKS endpoint,
**verifies the signature**." Direct contradiction.
**Resolution:** State the real model once. Likely: the password-grant path trusts the token
it just minted (no re-verify), while the bearer-token provider (where the engine receives a
token it did not mint) does verify against JWKS. Make that distinction explicit and drop the
absolute "Don't validate JWTs in the engine" or scope it to the password path. This
contradiction is *caused by* the bolt-on section — see section 6.

### P1.2 Binary / image size: 47 vs 80 vs 180 MB (ch03, ch06b, ch15, ch16d)

Three different numbers attach to "the binary" across the book:

| Claim | Location |
|---|---|
| "Docker image is 47MB" | `03` L363; `15` L116 ("Final image 47MB; 98% reduction") |
| binaries (server+worker+cli) "~40 MB total" | `15` L17, L250 |
| slim build "started at 80 MB stayed at 80 MB" | `06b` L163-164 |
| "The binary is 180 MB to DuckDB's 30 ... could ship a 70 MB minimal build; we have not yet bothered" | `16d` L184 |

**Resolution:** These are plausibly different objects (multi-stage Docker *image* = 47 MB;
raw stripped *binary* ~40 MB; a "slim build" = 80 MB; the full embedded-mode binary with
delta/json/http features = 180 MB). As written a reader sees 47/80/180 for "the binary" and
distrusts all three. Label each occurrence with *which* artifact it measures, and reconcile
the 80 MB "slim build exists / size preserved" framing (ch06b) against the 70 MB "minimal
build, not yet bothered" framing (ch16d) — one says it ships, the other says it does not.

### P1.3 Implementation-Status table says "Implemented", prose says "designed" (ch09)

`09-what-you-cant-see.md` L519-530 table marks `PolicyPlanRewriter`, `PolicyStore`, the OPA
backend, and Collibra as **Implemented**, while the prose (L216, L404, L483, L526) describes
them as designed-but-not-yet-built (Phase 5, "OPA designed for"). Internal contradiction in
the chapter that owns the truth for this subsystem.
**Resolution:** Pick one. Project memory (Phase 5 status) and the surrounding prose suggest
the table rows should read "Designed / Phase 5," not "Implemented." Note this interacts with
ch17 L324, which says writing the chapter *forced* implementing the sha256 masking UDF — i.e.
parts genuinely landed later. Align the table with whatever was true at the chosen snapshot.

### P1.4 "One line / four hours" vs "three days" debugging anecdote (ch08, plus variants)

`08-writing-is-a-contract.md` L222 summary says the write path took "one day to implement and
**three days to debug**," contradicting the chapter's own opening (L6 "One day") and its
signature beat (L57 "Four hours of debugging," L230 "always four hours"). The "three days"
figure appears nowhere else.
**Resolution:** Commit to one debug figure in the summary or cut the number. Related variant
to watch: ch06 L332 "one line ... two hours"; ch02 L204 "three lines ... three days"; ch14
L876 "one day to write and three days to work through." The *pattern* is on-voice; the
specific numbers should not silently disagree inside a single chapter (the ch08 case is the
only true intra-chapter conflict — ch02/ch06/ch14 are distinct bugs and are fine).

### P1.5 DataFusion version drift: "DF 53.1" vs "DF 54" (ch03)

`03-the-engine-you-already-have.md` L507 parenthetical: "version numbers as of the work in
this book; we have since moved to DF 53.1." But the appended L607-625 section says the in-tree
port "runs on 54 while both apache main and the RisingWave rebase branch are still on 53."
The L507 "53.1" predates the DF 54 addition.
**Resolution:** Update the L507 parenthetical to "we have since moved to DF 54" (or "53.1,
then 54"). Cross-book the version timeline is otherwise coherent: ch12 L58 intentionally
shows DF 52 + datafusion-proto 53/53.1 coexisting "over the course of this book," and ch17
L282 traces "52 ... 53.0 ... 53.1." Only the ch03 L507 stale parenthetical needs the touch.

### P1.6 Coordinator "never touches S3" vs runs local scans (ch13)

`13-neither-trusts-the-other.md` L37 "The coordinator never touches S3" and L653 "In
distributed mode, it has no S3 connectivity (by design)." But the local-fallback path has the
coordinator perform the scan: L90 "the coordinator handles the scan itself," L355 "fall back
to local execution," L619 "the fragment runs on the coordinator itself." A coordinator doing
the scan fallback must reach S3.
**Resolution:** One clarifying sentence: the coordinator *has* S3 access but does not use it
while healthy workers handle all fragments; the absolute "never touches S3" is the
all-workers-healthy steady state, not the fallback path. (The credential-in-ScanTask and
STS-TTL sub-flags in the same findings file are not true contradictions; leave them.)

### P1.7 Intra-chapter counts that do not match their own tables

Clean, mechanical, individually small, collectively a credibility tax:

| Chapter | Claim vs reality | Fix |
|---|---|---|
| `05` L64/L82/L656 "20+" / table 25 rows / `.ailog` L669 "all 24" | three-way method-count mismatch; "all 24" also contradicts 7 explicitly-unimplemented methods | reconcile to one number; "all" cannot be true with Substrait/transactions omitted |
| `05` L429 "Fifteen type definitions" vs enumerated 13 (L409) | type count | recount; align prose to list |
| `09` / `17` "Twenty-six lines of Rust" trait vs shown trait = 9 lines | `PolicyEnforcer` line count | either say "26 lines with imports/attrs" or fix the number |
| `10` L378/L420 "45 keys" vs config table (L422-435) sums to 47 | config-key total | fix prose to 47 or correct the table |
| `16` L25 TPC-E 18 q vs L363 "11 queries" vs L710 matrix 18 | TPC-E count | state the read-subset count explicitly (as TPC-C does at L362) |
| `09b` table "22 types" vs prose "twenty-three" (L124) | INSERT access-type fan-out | reconciles as 22 + table-data-write = 23; add the bridge clause |
| `16b` "63 cells" vs "189 points" vs "11 vs 13 tests" | matrix cell/point vocabulary never stated | one sentence: a cell scores 0/partial/3, so 63 cells = 189 points; bridge the 11-vs-13 test count |
| `17` L50/L297 "six benchmark suites" vs seven established everywhere | suite count | fix to seven (TPC-H, TPC-DS, SSB, ClickBench, TPC-E, TPC-BB, TPC-C — confirmed in ch16 L31, ch16b L8, preface L103) |
| `17` L7 "Sixteen chapters" vs L149 "Fifteen chapters" | prior-chapter count | pick one against the final TOC |
| `16` L631 "40% faster / 30% slower" | absent from every table; tables show 2.5x-4.6x and q72 ~13x | replace with real figures (see section 5) |

### P1.8 Crate-count snapshot (NOT a contradiction — a snapshot-policy decision)

The book is internally consistent at **12 crates** (ch03 L382, ch17 L71/L97/L233/L345/L353,
ch18 L19/L46). Ground truth: the repo now has 17 crates (the quack-*, lineage, cli, bench
crates were added after the book's snapshot); CLAUDE.md's table says 10 (stale). So "12" is a
valid historical snapshot, **not** book drift. Do not "reconcile to 10." The action is an
*author decision*: (a) state a snapshot date for the crate count, or accept "12 at time of
writing"; and (b) fix the stale 10-crate table in `CLAUDE.md` so it stops contradicting the
book. The per-chapter reviewers who flagged "12 vs 10 — cross-check" were working without this
context; their flag is a false positive on the book and a true positive on CLAUDE.md.

---

## 5. P2 — Voice & editorial patterns (book-wide)

Voice is uniformly strong (all A/A-). These are recurring *patterns* worth a single sweep,
not per-chapter surgery.

**1. The "N lines of code / N hours-or-days of debugging" signature beat is over-used.**
It is genuinely on-voice (voice.md quotes "The fix was one line. The debugging was four
hours.") but it appears as a structural payoff in too many chapters: ch02 (L204), ch06
(L332), ch08 (L57 + L230, near-duplicate), ch12 (L404/L421/L623/L714 — four times in one
chapter), ch14 (L127/L874), ch16d (L36/L38/L75 "few hundred lines" variant). **Fix:** cap it
at roughly one payoff per chapter; in ch12 keep two and cut/vary the other two; in ch08 keep
L230 and vary L57.

**2. "change(s) the game" — forbidden idiom family.**
ch00 L29 "Then Apache Iceberg changed the game." and ch02 L418 header "Why Rust Changes the
Game." Both are the verb form of the banned "game-changer." **Fix:** ch00 -> "changed what
was possible"; ch02 -> "Why Rust Changes the Math."

**3. "it is worth pinning / dwelling / holding" — near-misses of the forbidden "it's worth
noting."**
ch09 L539 "This section is worth dwelling on because..."; ch09c L15 "so it is worth pinning
the difference down before anything else." (ch09b L70 noted in the same family.) **Fix:** cut
the throat-clear and lead with the thing: ch09 -> "The most subtle security property shows up
when a user predicate hits a masked column."; ch09c -> "Pin the difference down first."

**4. Sentences opening with back-referring "This" (CLAUDE.md forbids it explicitly).**
Most concentrated in ch14 (L227, L258, L589, L708, L116 — ~5 instances), also ch02 (L66,
L359, L450), ch03 (L44, L350, L505), ch15 (L514), ch16c (L65, L153, L285). **Fix:** name the
subject. ch14 L227 "This seems obvious" -> "The fix seems obvious"; ch16c L65 "This is the
spec feature" -> "Partition evolution is the spec feature". A find-for-`This is`
sentence-start sweep is the efficient way to catch them.

**5. Structural double-endings (couple these fixes with section 6).**
ch04 (clean close at L342, then the appended "Ten Ways" reopens and contradicts);
ch15 (L612 close, then epigraph repeated verbatim at L618);
ch16 (L621 "What We Learned" reads as finale, then four more sections follow);
ch17 (back third stacks five reflective sections relitigating one thesis).
**Fix:** see section 6 — these are pacing/structure, surfaced here because they read as voice
softness (a strong landing diluted by a second one).

**6. Walls of text (>5-sentence paragraphs).**
ch00 L119 (8+ sentence Rafael ack), ch04 L52 (8 sentences, "if your IdP is down" x3), ch07
L34 (7), ch10 L342 (7), ch06c L88 (9). **Fix:** split each at the natural seam noted in the
findings.

**7. Minor house-style: British vs US spelling.**
"realised/realise" mixes with "behavior/optimize" in ch10 (L189/L376/L596), ch16c L6 "per
cent" vs "68.3%" elsewhere. Not a voice-guide rule; pick one convention book-wide. Likely
intentional (Dutch author), so this is a consistency call, not a defect.

---

## 6. Structural / pacing notes

**Chapters that drag or double-end:**

- **ch04** — the appended "Ten Ways to Prove You're You" (L344-458) doubles the chapter
  after a clean thesis-landing at L342, *and* introduces the JWKS contradiction (P1.1). Fold
  the ten-provider material in *before* the L330 retrofit lesson, or mark it an explicit
  addendum, and reconcile the JWT-validation claim in the process. One fix, two problems.
- **ch16** — long (~796 lines) with two strong endings buried mid-chapter: "What We Learned"
  (L621) would be a natural finale, but four excellent sections follow it. Reorder so the
  recap is last, or demote L621 from finale cadence. The "40% / 30%" invented round numbers
  (P1.7/section 5) live here and clash with the chapter's own honesty ethos — fix them in the
  same pass.
- **ch17** — longest chapter; the back third (Open-Source Goal -> Where This Goes -> Build-vs-
  Buy -> Book That Found Bugs -> Hardest Lesson) restates "AI implements, human decides" five
  times. Tighten: fold "The Hardest Lesson" prose into its `.sovereignty` callout; cut one of
  the four "I said this elsewhere, here's the honest version" self-references (L16/L235/L257/
  L293). Also fix the duplicate-epigraph and double-close issues here.
- **ch15** — drop L618 (it repeats the epigraph and competes with the stronger L612 close).
- **ch13** — delete the duplicated epigraph line (L4-L5 both read "Neither trusts the other
  more than necessary.").

**Very short mini-chapters.** 06b (1,411 words), 06c (1,663), 16b (1,723), 16d (1,870), 16e
(2,119), 18 (516). These read as deliberate, focused interludes — each has a single thesis, a
clean hook, and an earned one-line close (06b "Check.", 16e "SQE emits OL.", 18 "Build what
matters. Leave the rest."). They are an asset, not a problem; the epilogue's brevity is
correct.

**The 16x sub-chapter sprawl (16, 16b, 16c, 16d, 16e).** This reads as *accretion*, not
design. ch16 is the benchmark chapter; 16b/16c are the capability-matrix arc; 16d is the
DuckDB-drift narrative; 16e is lineage. They are individually strong but they are four
distinct topics hung off one number because they were written later. **Recommendation:** at
the structural level, decide whether these become their own numbered chapters (17-20, shifting
the retrospective/epilogue) or are explicitly grouped as a "Part" with a short bridge. As
bare "16x" suffixes they signal "bolted on" to a reader scanning the TOC. This is the one
table-of-contents decision the author should make consciously before publication.

---

## 7. Analytics digest

Full table in `analytics.md`. Outliers worth acting on:

- **Total scope:** 100,776 prose words, 7,976 sentences, ~403 pages, ~503 min read. A
  substantial book; the length is in ch10/ch16/ch17 (each 7k+ words) and the four-part 16x
  cluster.
- **Long-sentence-heavy chapters** (>=30-word sentences, candidates to split per the
  "three-clauses-max" rule): ch16 (25), ch17 (25), ch10 (24), ch05 (21), ch04 (18). ch05 is
  notable: highest avg sentence length tie (14.3) with the most long sentences relative to its
  size — worth a split pass despite its A grade.
- **Code-ratio extremes.** Very high: ch12 (74%), ch15 (72%), ch13 (69%), ch14 (68%), ch06
  (65%), ch06c (60%), ch05 (59%) — these lean toward listing over argument; ch12 and ch05
  findings both flag long code stretches where prose nearly disappears (ch12 L271-358 codec
  deep-dive). Very low in how-it-works chapters: ch17 (8%) and ch18 (0%) are appropriately
  prose-only (retrospective/epilogue); ch00 (0%) correct for a preface; ch08 (13%) is low for
  a write-path chapter but the findings judge it "telling well," acceptable.
- **% short sentences** (Jacob's voice leans on these; <20% = not enough punch): lowest are
  ch06 (25%), ch09c (23%) — both still A-grade, but ch09c at 15.8 avg length / 23% short is the
  densest-reading chapter in the book; a few sentence-splits would lift it.
- **Exclamation marks in prose:** analytics flags non-zero counts for ch02 (2), ch14 (5),
  ch16 (2), and singles for 06/09/09c/10/11/16c — every one is a confirmed false positive in
  the findings (Rust `format!`/`warn!` macros, `!=` operators, image alt-text, NOTES.txt
  output). No prose exclamation marks exist. No action.
- **Dead-end callouts:** present and well-distributed (ch02 has 2, ch13 has 2, ch17 has 2) —
  the signature transparency device is used as the voice guide prescribes.

---

## 8. Prioritized next actions

Work straight down.

1. **Sanitize the AWS account id `311141556126`** -> `123456789012` in `docs/roadmap.md:60`,
   `docs/book/src/features/iceberg.md:58`, and `docs/book/src/getting-started/catalogs.md`
   (L25, L244, L300); decide on genericizing the live db/table/bucket names. Re-run the book
   leak-scan. (P0.1)
2. **Resolve the two AI-Logbook placeholders** (ch07 L148, ch10 L400): write or delete.
   (P0.2/3)
3. **Fix the grammar-broken AI-Logbook block** ch03 L602 (commas, not periods), and grep for
   other "word. lowercase." emdash-removal casualties. (P0.4)
4. **Run the Chapter 8 -> Chapter 9 cross-reference fix** (7 citations) and the one
   Chapter 7 -> Chapter 8 fix (ch17 L326). Single find-and-replace pass. (P1.0)
5. **Resolve the ch04 JWT-validation contradiction** while folding/marking the "Ten Ways"
   addendum (couples P1.1 with the ch04 double-ending). (P1.1 + section 6)
6. **Label every binary/image size** with which artifact it measures and reconcile the
   80-vs-70 MB "slim build exists vs not bothered" framing. (P1.2)
7. **Align the ch09 Implementation-Status table** with the prose (likely "Designed / Phase
   5"), accounting for what genuinely landed during book-writing per ch17 L324. (P1.3)
8. **Reconcile the intra-chapter counts** in one editing pass: ch08 debug figure, ch03 DF
   version parenthetical, ch13 coordinator-S3 clarification, and the P1.7 table (method
   count, type count, trait line count, config keys, TPC-E, INSERT types, matrix cells,
   suite count, chapter count, ch16 round percentages). (P1.4-7)
9. **Decide crate-count snapshot policy** and fix the stale 10-crate table in `CLAUDE.md`.
   (P1.8)
10. **Make the 16x table-of-contents decision** (renumber vs explicit Part grouping).
    (section 6)
11. **Voice sweep** (low priority, all A/A- already): the "N lines / N hours" beat cap, two
    "changes the game" idioms, three "it's worth X" near-misses, the back-referring "This"
    openers, and the five wall-of-text splits. (section 5)
12. **Structural close-tightening:** ch15 L618, ch13 L4-5 duplicate, ch16 reorder, ch17 back-
    third trim. (section 6)
