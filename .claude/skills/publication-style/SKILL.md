---
name: publication-style
description: Jacob's voice rules for anything published from this repo (ebook chapters, blog posts, docs site). Use when writing or editing prose under docs/site/. Includes the forbidden-character grep to run before committing.
---

# Writing Style (Ebook, Blog, Docs)

All publications (ebook chapters in `docs/site/ebook/chapters/`, blog posts in `docs/site/blog/`, and documentation) MUST follow Jacob's voice from `docs/site/ebook/voice.md`. Key rules:

### Forbidden Characters
- **NEVER use emdash** (`—` U+2014). Replace with periods, commas, colons, or restructured sentences.
- **NEVER use endash** (`–` U+2013). Use a hyphen (`-`) or rewrite.
- **NEVER use Unicode arrows** (`→` `←` `▶`). Use `->` in code blocks, "becomes" or "leads to" in prose.

### Forbidden Words and Phrases (AI tells)
- Never: "delve", "leverage", "utilize", "facilitate", "comprehensive", "robust", "cutting-edge", "game-changer", "paradigm shift", "synergy"
- Never: "it's worth noting", "importantly", "notably", "interestingly", "in conclusion"
- Never: "This approach ensures", "This enables", "This allows for"
- Never start a sentence with "This" referring to the previous sentence. Name the subject.

### Forbidden Patterns
- No rhetorical questions as transitions ("But what about X?" "So how does Y work?")
- No trailing summaries that repeat what was just said
- No exclamation marks
- No emoji in prose (OK in terminal output examples)

### Required Style
- **Short sentences carry the weight.** They land the point.
- Longer sentences do the explaining. Three clauses maximum.
- Alternate between short and long. Rhythm matters.
- Paragraphs: 3-5 sentences. Earned single-sentence paragraphs for emphasis.
- Use "we" for team decisions, "I" sparingly (only in ch17 retrospective).
- Present tense for how things work. Past tense for narrative.
- Direct and opinionated. No hedging. State the tradeoff, pick a side.
- Code examples must compile or be clearly marked as pseudocode.

### Before Committing Any Publication
Run this check: `grep -rn '—' docs/site/ebook/chapters/ docs/site/blog/` — must return zero hits in prose (OK in frontmatter `description` fields and inside code/tree-diagram blocks).
