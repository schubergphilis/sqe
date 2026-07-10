# Findings: 17-what-wed-do-differently.md

## Thesis
A candid retrospective on building SQE in fifteen days with an AI coding agent: what the AI did well and poorly, which architectural decisions held up, which would change, and the "human architect, AI builder" division of labour that made the pace possible.

## Opening
> "This is the chapter where I stop saying "we" and start saying "I" more than usual. The architectural decisions were collaborative. The reflections are mine."
Verdict: strong hook. Sets the personal-reflection frame the voice guide reserves for ch17, then lands on the real question ("was this the right thing to do?"). Earns the "I".

## Closing
> "That ratio -- one human, one AI, clear roles -- is the thing I'd keep above all else."
Verdict: lands it. The body closes on the chapter's thesis sentence (callout boxes follow). The "Fifteen days. Three hundred and sixteen commits..." staccato run just before it is the strongest rhythm in the chapter.

## Voice & editorial issues
1. L65 -- "The truth is more interesting and more nuanced." Mild self-congratulatory throat-clear before the payload. The voice guide says state the thing. Rewrite: cut the sentence; let the "What the AI did well / poorly" headers carry it.
2. L138 -- "The AI is fast. The human is precise. The cycle is what produces code you can trust." Trailing summary of the section just delivered; restates "Spec, prompt, output, catch the bug, revise the prompt, verify the fix" from the prior sentence. Rewrite: end on that staccato list and drop the recap, or keep only "The AI is fast. The human is precise."
3. L235 -- "I mentioned this in Chapter 3, and I'm going to be more specific here because the retrospective earns specificity." Self-referential filler that echoes L16 ("the retrospective gets the honest version") and L293. Rewrite: "I mentioned compile times in Chapter 3. Here are the numbers."
4. L16 / L235 / L257 / L293 -- four variations of "I said this elsewhere, here's the honest/specific version." Good device, overused. Cut at least one (L235 or L293 is the weakest).
5. L295 -- "multiplied by whatever factor you want to assign to AI assistance" then immediately gives the factor ("roughly 10x ... roughly zero"). The hedge contradicts the directness that follows. Rewrite: drop the hedge, go straight to the 10x/zero split.
6. L299 -- "Enterprise-grade maturity." Borderline marketing-speak the voice guide's spirit resists; also redundant with "Battle-tested failure recovery" in the same line. Rewrite: "Production maturity."

## Mechanical violations (PROSE only)
none. The chapter uses `--` (double hyphen) throughout where an emdash would normally appear; grep for emdash/endash/arrows/emoji returns zero hits in prose.

## Exclamation marks in prose
none.

## Continuity data
### Concepts INTRODUCED / defined here
- ScanTask ceiling -> scans only, no aggregation
- PlanFragment (future) -> serialised physical subtrees to workers
- QueryLifecycle state machine -> explicit query-state transitions
- Speed multiplier -> 10-20x impl, 3-5x total
- "human architect, AI builder" -> division-of-labour model
- One Complete Cycle -> spec/prompt/output/review/revise mechanic
- semantic AI layer -> RDF/graph/vector future crates

### Concepts ASSUMED (used as if already known)
- bearer token passthrough (ch4, cited)
- PolicyPlanRewriter / PolicyEnforcer trait (ch8, sqe-policy)
- DistributedScanExec / ScanTask protocol (ch13/14)
- gRPC stream accumulation bug (ch14, cited)
- OpenSpec format
- Merge-on-Read position deletes, write.delete.mode property
- Polaris credential vending flow
- DataFusion pull model / HashJoinExec eager build (ch3)
- RustFS, Trino HTTP compat, OTel/Prometheus

### Key factual / numeric claims
- "316 commits (across all branches) in 15 days" (title L27; "Three hundred and sixteen commits" L353)
- "built ... in fifteen days" (L7, L24, L50, L60, L353)
- git log dates Mar 14 - Mar 29 (L31-48)
- "twelve crates" (L71, L97, L233, L345, L353) -- FLAG: CLAUDE.md crate table lists 10 crates; git log L34 says "All 6 crates scaffolded". Is it 10 or 12? cross-check against other chapters.
- "6 crates scaffolded: core, auth, policy, sql, catalog, coordinator" (L34)
- "37 integration tests" (L40, L73, L297)
- "PolicyEnforcer trait is twenty-six lines" (L69, L267)
- "borrow checker happy on the first try about 70% of the time" (L69)
- "keycloak_url to oidc_url" rename (L71, L261)
- "50 concurrent clients hung after about 30 queries" (L75)
- "distributed execution design went through three iterations" (L81, L92)
- "AI suggested a service account model twice" (L87)
- "twelve-minute security review/rejection" (L87, L313)
- multiplier "10-20x" impl, "3-5x" total, "30%" impl share (L107-111, L295)
- "plan_rewriter.rs ... lines 131-158 ... 160-181" (L134) -- VERIFY line numbers against current source
- "forty minutes" full cycle (L136)
- "Five decisions held up" (L143), lists 5 (L145-153)
- "TOML config came in version 0.3" (L168)
- "OSS security hardening pass -- Step 3 ... fifty-one tasks" (L261, L263)
- "RisingWave's iceberg-rust fork" (L182); "DataFusion 53 ... hash join dynamic filters and 40x faster planning" (L184); "upstream PR #2206" (L186); "issues #2185 and #2203" (L190); "vendor/iceberg-rust/ (4.6 MB)" (L190); "three crates" (L190)
- DataFusion "52 ... 53.0 ... 53.1" (L282)
- "clean build ... about eight minutes on an M3 MacBook Pro" (L237); "incremental ... 15-30 seconds"; "sqe-core ... 2-4 minutes"; "fifty builds a day"; "cargo check ... about 3x faster" (L242)
- "coordinator uses about 512MB of memory at idle" (L301)
- "six benchmark suites / six industry-standard benchmarks" (L50, L297) -- FLAG: L41-42 lists 7 benchmark generators (TPC-H, SSB, TPC-DS, ClickBench, TPC-E, TPC-BB, TPC-C). "six" vs seven named. cross-check.
- "writing the book produced 12 code fixes and 6 remaining TODO items" (L332)
- "two to three months for a senior Rust engineer, six months or more for a team" (L295)

### Cross-references
- "Sixteen chapters demonstrate that" (L7) vs "Fifteen chapters later" (L149) -- FLAG inconsistency: 16 vs 15 prior chapters.
- "Chapter 4 covers this in depth" (L149, bearer passthrough)
- "Chapter 10 shows the finished config surface" (L172)
- "Chapter 14" gRPC bug (L75)
- "Chapter 8 described the PolicyPlanRewriter" (L324)
- "Chapter 7 described the Iceberg commit mechanism" (L326)
- "Chapter 3 described DataFusion's pull model" / "mentioned this in Chapter 3" (L235, L328)
- "[Chapter 16b](#sec:matrix) and [Chapter 16c](#sec:punchlist)" (L284)
- "Art of Agents ... Use of Spies (Chapter 13)" (L270, the other book)
- Forward: "a question for the next version of this book" (L288)

## Pacing
Flows well overall; the AI-assessment section (did well / poorly / surprised / didn't surprise / speed multiplier) is the strongest stretch, well-chunked with bold leads. Longest chapter in the book and it shows: the back third (Open-Source Goal -> Where This Goes Next -> Build-vs-Buy -> Book That Found Bugs -> Hardest Lesson) stacks five reflective sections that each restate the human-vs-AI thesis. No single wall-of-text paragraph, but cumulative repetition of "AI implements, human decides" (L85, L89, L101, L203, L347-351, L358, L362, L366) drags by the end. Consider tightening Build-vs-Buy or folding The Hardest Lesson prose into the .sovereignty callout.

## Grade
Voice adherence: A-. Mechanically clean, strong opening and closing, signature transparency (callout dead ends/field reports), no forbidden words or emdashes. Held back from A by repeated self-referential framing ("the retrospective earns/gets X" x4), a trailing-summary recap (L138), and thesis over-repetition across the final five sections.
