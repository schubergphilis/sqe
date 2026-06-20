# Findings: 18-epilogue.md

## Thesis
The book set out to answer one security question ("who accessed the customer table last Tuesday?") and SQE answers it; the deeper payoff is a set of architectural opinions that compile, and the realization that an AI agent writes the Rust while the human supplies the opinions.

## Opening
> I started this book with a question from the security team: "Who accessed the customer table last Tuesday?"
> Seventeen chapters later, we can answer it. The query ran as Alice. S3 saw Alice. CloudTrail shows Alice.
Verdict: strong hook. Bookends the whole project with the same concrete question and answers it in staccato, evidence-style fragments.

## Closing
> Build what matters. Leave the rest.
> *Amsterdam, 2026*
Verdict: lands it. Two short imperatives plus a dateline; no trailing summary, restates nothing.

## Voice & editorial issues
1. L17 "Recognising that the timestamp precision bug was in the display formatter, not the data." -- Continuity check, not a voice fault: this is the only place in the epilogue that attributes the timestamp bug to a "display formatter." Confirm an earlier chapter established that exact bug so the callback isn't a dangling reference. If no chapter covers it, the reader hits an unfamiliar example presented as familiar.
2. L19 "an AI agent that can hold twelve crates in its head simultaneously" -- "twelve crates" vs the documented ten-crate structure (CLAUDE.md lists 10 crates). Either the count drifted or it is deliberate hyperbole; if literal, reconcile to the real crate count. Numeric-claim check, not a voice issue.
3. L15 "The agent wrote the Rust. I wrote the opinions. The agent is very good at Rust. It has no opinions of its own." -- Strong rhythm, squarely in voice. Noted as a positive anchor, no change.

Otherwise none of note. No hedging, no throat-clearing, no rhetorical-question transitions, no filler transitions, no forced humour. Paragraphs sit within the 3-5 sentence target, with earned single-sentence paragraphs (L7, L23, L29).

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none

## Continuity data
### Concepts INTRODUCED / defined here
- semantic layer -> property graphs, vectors, agents (named as next book's territory, L21)
- "opinions that compile" -> framing device for design choices (L9-L13)

### Concepts ASSUMED (used as if already known)
- bearer passthrough vs service accounts (L17)
- forking Ballista's model instead of using it as-is (L17)
- knowing when to stop distributing / single-node fallback (L17)
- timestamp precision bug in the display formatter (L17)
- borrow checker / shared reference across thread boundary / credential outliving a session (L13)
- Alice as running example user; S3, CloudTrail, audit log (L5)
- Trino cluster that worked "mostly" (L25)

### Key factual / numeric claims
- "Seventeen chapters later" (L5) -- consistent with this being ch18 epilogue over a 17-chapter body.
- "build most of this in fifteen days" (L15)
- "twelve crates in its head" (L19) -- conflicts with the 10-crate structure in CLAUDE.md; verify.
- "timestamp precision bug was in the display formatter, not the data" (L17)
- Ballista forked rather than used as-is (L17) -- consistent with the Ballista wind-down/fork-the-model narrative.
- Dateline: "Amsterdam, 2026" (L33)

### Cross-references
- L5 "Seventeen chapters later" -- back-ref to the whole book and the ch1 opening question.
- L25 "started with a Trino cluster that worked. Mostly." -- back-ref to the opening chapter premise.
- L17 four specific decisions (bearer passthrough, Ballista fork, stop-distributing, timestamp bug) should each map to an earlier chapter; the timestamp/display-formatter one is the weakest-anchored.
- L21-L23 "that's a different book" -- forward-ref to a sequel (semantic layer); no in-book promise to fulfill.

## Pacing
Flows. Deliberately short and rhythmic, built on the short/long alternation the voice guide prescribes. No wall of text; longest paragraph (L17) is four sentences and earns its length as the thesis paragraph.

## Grade
Voice adherence: A. Textbook execution of the voice guide (staccato hook, understated AI-vs-human framing, clean imperative close, zero forbidden words or mechanical violations); only open items are two continuity numbers to verify, not voice defects.
