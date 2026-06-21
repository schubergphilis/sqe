# Per-Chapter Review Instructions

You are reviewing ONE chapter of the technical book **"Sovereign by Design"** by Jacob
Verhoeks. The book documents building **SQE (Sovereign Query Engine)**, a Rust-based
distributed SQL engine for Apache Iceberg (DataFusion + iceberg-rust, Polaris REST
catalog, OIDC bearer passthrough, OPA/Cedar policy enforcement via plan rewriting).

The book is written **incrementally over months**, so the goal is to hunt **drift and
inconsistency** a single read-through would miss, plus per-chapter voice quality.

## Read these first
1. Your assigned chapter (path given in your task).
2. The voice guide: `docs/ebook/voice.md` (absolute:
   `/Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine/docs/ebook/voice.md`)

## Then write a findings file
Write to the exact path given in your task (e.g. `docs/ebook/review/findings-03-the-engine-you-already-have.md`).
Use EXACTLY this structure:

```
# Findings: <chapter filename>

## Thesis
<1-2 sentences: what this chapter argues / teaches>

## Opening
> <quote the first 1-2 sentences>
Verdict: <strong hook | weak preamble | throat-clearing>. <why, 1 line>

## Closing
> <quote the last 1-2 sentences>
Verdict: <lands it | trailing summary | fizzles>. <why, 1 line>

## Voice & editorial issues
<numbered list. For each: `Lnnn` + quoted offending text + the rule it breaks + a concrete rewrite. If none, "none of note">

## Mechanical violations (PROSE only)
<emdash (—), endash (–), unicode arrows (→ ← ▶), or emoji that appear in PROSE.
EXCLUDE anything inside ``` code fences or ASCII/tree diagrams. Give Lnnn. If none, "none">

## Exclamation marks in prose
<list Lnnn for ! used in actual prose sentences, NOT shell/code output. If none, "none">

## Continuity data
### Concepts INTRODUCED / defined here
<exact term -> 3-word gloss. These are things this chapter is the source of truth for>
### Concepts ASSUMED (used as if already known)
<terms used without definition that must have been defined in an earlier chapter>
### Key factual / numeric claims
<verbatim: benchmark numbers, version numbers (DataFusion 5x, iceberg-rust 0.x), counts,
dates, named components, file/crate names. This list is cross-checked against other chapters>
### Cross-references
<explicit "as we saw in ch X" / "we'll cover in ch Y" promises, and any forward/back refs>

## Pacing
<1 line: does it drag, rush, or flow? any section that is a wall of text?>

## Grade
Voice adherence: <A-F>. One-line justification.
```

## Review rules
- **Voice/editorial — flag these:** hedging ("it could be argued", "arguably",
  "essentially"), throat-clearing intros ("In this chapter we will explore..."),
  rhetorical questions used as transitions ("So what does this mean?"), trailing summaries
  ("In summary, we covered..."), filler transitions ("With that in mind...", "Having
  established that..."), forced humour, walls of text (paragraphs > 5 sentences), weak
  chapter opening (preamble instead of a hook), weak closing.
- **Forbidden words** (flag every occurrence with Lnnn): delve, leverage, utilize,
  facilitate, holistic, paradigm, synergy, arguably, essentially, "it's worth noting",
  "at the end of the day", game-changer, groundbreaking, revolutionary, robust,
  comprehensive, "this approach ensures", "this enables", "this allows for".
- Be specific and cite line numbers (`Lnnn`). Quote the actual text. Give a concrete
  rewrite, not "consider revising".
- No praise padding. This is a working document, not a book report.

## Return to the dispatcher (short — do NOT dump the chapter)
- Chapter filename
- Voice grade (A-F)
- Top 3 issues (one line each)
- Confirm the findings file was written
