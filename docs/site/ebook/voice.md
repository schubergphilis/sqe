# Voice Guide — Sovereign by Design

This document captures Jacob Verhoeks' writing voice, distilled from 26 dev.to articles (2022–2026) across technical tutorials, opinion pieces, security analysis, and personal reflections. Use this as the reference for every chapter.

---

## Who is writing

A principal engineer who's been solving problems since childhood. Not a manager. Not a performer. The drive is the puzzle itself — taking something complex apart, understanding why it works (or doesn't), and making it better. The authority comes from doing the work, not from titles.

---

## The core voice

**A senior engineer explaining to a peer who's smart but hasn't seen this particular problem yet.**

Not lecturing down. Not simplifying for beginners. Not showing off for experts. Talking to someone who will understand the nuance if you give it to them straight.

---

## Sentence patterns

**Short sentences carry the weight.** They land the point. They create rhythm.

Longer sentences do the explaining — they walk through the mechanism, connect the pieces, show how one thing leads to another. But they don't run on. Three clauses is plenty. If a sentence needs a semicolon, it's probably two sentences.

Alternate between the two. Short sentence. Then a longer one that unpacks it. Then another short one to land.

**Examples from actual writing:**

> "Context is currency. Spend it wisely."

> "It's a timely critique, especially as tools like advanced Claude models have become incredibly capable. Yet my own experience tells a somewhat different story."

> "The fix was one line. The debugging was four hours."

---

## Paragraph length

Short. Three to five sentences. Rarely more.

A single-sentence paragraph is fine when the point earns it.

White space is a tool. Use it to let ideas breathe. Walls of text are for academic papers. This is a book people will read at midnight after a long day of engineering.

---

## Directness

Say what you think. Don't hedge.

| Instead of | Write |
|---|---|
| "It could be argued that..." | "The problem is..." |
| "One might consider..." | "We tried..." |
| "It's worth noting that..." | Just state the thing |
| "In my humble opinion..." | State the opinion. The reader knows it's yours. |
| "This is a complex topic..." | Explain it. The complexity will be apparent. |

Directness doesn't mean rudeness. It means respecting the reader's time.

---

## Opinionatedness

Jacob is opinionated and comfortable with it. He prescribes. "Do this." "Don't do this." "This is wrong." He frames trade-offs as choices, not as "it depends on your use case."

But he always shows the *why*. An opinion without reasoning is dogma. An opinion with reasoning is engineering judgement.

> "We use the password grant for interactive clients and accept bearer tokens for programmatic ones."

Not: "There are several approaches, each with trade-offs, and the choice depends on your requirements." The reader knows there are trade-offs. Tell them which trade-off you chose and why.

---

## How technical concepts are introduced

**Plain language first, then the term.** Never the other way around.

> "Your issues form a dependency graph (a DAG — directed acyclic graph)"

> "Policy enforcement happens by rewriting the query plan — injecting filter nodes above the table scan before the optimizer runs."

The reader gets the idea before the jargon. The jargon then has something to attach to.

For concepts the audience already knows (SQL, Rust, Arrow, gRPC), don't explain. Assume competence. For concepts specific to this project (bearer passthrough, plan rewriting, fragment scheduling), explain once, clearly, and then use the term freely.

---

## Transparency about what doesn't work

This is a signature trait. Jacob openly states when something fails, is incomplete, or was a dead end.

> "Unfortunately I still get: `Not implemented Error: Alter Schema Entry`"

> "We explored this for about two days. The AI generated working STS assume-role code in twenty minutes. The remaining time was spent on the IAM policy matrix and realising it wouldn't port."

This builds trust faster than any amount of polished success narrative. The reader knows the author is telling the whole story, not the marketing version.

In the book, dead ends get their own callout (`.deadend`). They're not failures — they're paths eliminated. Every dead end taught something.

---

## Humour

Dry. Infrequent. Earned.

Never a joke for its own sake. Never a pun. Never "let me lighten the mood."

Humour appears as understated observation when the situation is genuinely absurd:

> "Writing two books and building an engine simultaneously is either very efficient or very foolish. Ask me again when they're all finished."

> "The security team rejected it in a meeting that lasted twelve minutes."

> "One line of code. Four hours of debugging. That's distributed systems."

If a line doesn't land naturally, cut it. The book will never suffer from too little humour. It will suffer from forced humour instantly.

---

## Emotional register

**Measured enthusiasm.** Jacob is clearly engaged and energised by the problems he's solving, but he doesn't perform excitement. The enthusiasm comes through in the *detail* — he writes more about the things he finds interesting, not louder.

When something is genuinely impressive, he understates: "Definitely. Can I work without it? Yes, but it's hard."

When something is frustrating, he states it plainly: "Each workaround made the system more complex without making it more secure."

No exclamation marks. No "amazing!" or "incredible!" The reader can feel the author's engagement from the level of care in the explanation, not from emotional adjectives.

---

## Structural habits

- **Headers as scannable outline** — a reader skimming headers should get the chapter's story
- **Code blocks immediately after the concept** — show, don't just tell
- **Tables for comparisons** — when three or more things are compared, use a table
- **Bullet lists for enumeration** — but not for everything. Prose is for reasoning; lists are for inventory
- **Progressive disclosure** — broad principle first, then specific implementation, then edge cases

---

## Person and tense

First person plural ("we") for the team's decisions and work. First person singular ("I") for personal reflections and opinions. Present tense for how things work now. Past tense for the narrative of building.

> "We tried three approaches." (narrative)
> "The coordinator sends the JWT to Polaris." (how it works)
> "I'd make this decision differently today." (personal reflection)

---

## Forbidden

These words and patterns are not Jacob's voice. They will make the writing feel generic.

**Words:**
- delve, leverage, utilize, facilitate, holistic, paradigm, synergy, arguably, essentially
- "it's worth noting" / "it should be noted"
- "at the end of the day"
- "game-changer" / "groundbreaking" / "revolutionary"

**Patterns:**
- Rhetorical questions used as transitions ("So what does this mean?")
- Throat-clearing introductions ("In this chapter, we will explore...")
- Trailing summaries ("In summary, we covered X, Y, and Z")
- Apologising for difficulty ("This might seem overwhelming, but...")
- Filler transitions ("With that in mind..." / "Having established that..." / "Moving on...")

**Emoji in prose:** No. The dev.to articles use emoji as visual anchors in lists and headers (💡, 📋). The book does not. Callout boxes serve this function instead.

---

## The test

Read every paragraph aloud. If it sounds like it could have been written by any senior engineer, it's not Jacob's voice yet. If it sounds like it was written by someone who's been staring at this specific problem for three days and finally figured it out — that's right.
