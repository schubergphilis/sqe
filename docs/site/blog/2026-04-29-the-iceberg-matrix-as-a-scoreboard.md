---
title: "Why a Public Iceberg Matrix Beats Vendor Spec Sheets"
description: "Sixty-three capabilities, three levels, no marketing. The Iceberg Matrix is what compatibility looks like when the rubric is public and the evidence has to land in code. Here is why it works, what we learned from sitting on it, and why every open standard needs one."
pubDate: "2026-04-29"
author: "Jacob Verhoeks"
tags: ["iceberg", "matrix", "open-source", "compatibility", "ecosystem"]
---

There is a public scoreboard for Iceberg engines. It lives at [icebergmatrix.org](https://icebergmatrix.org). It tracks sixty-three capabilities across the Iceberg V2 and V3 specs and grades every engine on a three-level scale: full, partial, none. Each cell links to evidence. The rubric is on GitHub. Pull requests are how scores change.

We have been sitting on the matrix for the last six weeks. SQE went from 99 out of 189 points (52%) at the start of April to 158 out of 189 (83.6%) at the end. Every flip was a code change with a test you could run. None of them came from a marketing claim.

This post is about why scoreboards like the matrix work, what they reveal about the rest of the ecosystem, and why every open standard needs one.

## The problem the matrix solves

Compatibility claims are unfalsifiable by default. If a vendor says "we support Iceberg V3," what does that mean? Read support? Write support? All column types? Column defaults? Variant? Geometry? At what scale? Against which catalog? Through which storage backend?

The honest answer is almost always "some subset, and we are not telling you which subset until you trip over it." This is not malice. It is the asymmetry of information. The vendor knows the gaps. The user finds them.

Compatibility matrices flip the asymmetry. They define the unit of measurement (a capability, scoped to a spec version) and force the claim to land in a public table. "Full" means it works end-to-end with evidence. "Partial" means parts of it work, with caveats listed. "None" means it does not. The vendor still controls their own grading, but every other vendor and every user can look at the same table and ask why a cell that says "full" does not link to a passing test.

The matrix is not a benchmark. It does not measure speed. It measures whether the feature exists at all. That is a different question and a more important one.

## Why three levels beats binary

The first thing the matrix gets right is having three levels instead of two.

Binary compatibility ratings (yes / no, ✅ / ❌, supported / not supported) collapse the most interesting state. Most features are partially supported by most engines. The interesting question is which parts. Binary ratings round all of that to "no, not yet" or, worse, to "yes" with a footnote that nobody reads.

Three levels create room for honesty. "Partial" is the cell that says "we have done the work, but the work is not finished." It is the cell that pointed us at the V3 quiet bug we wrote about [a few weeks ago](2026-04-26-the-matrix-and-the-quiet-bug.md). Sixteen of our cells were "partial" with the same hand-waving caveat: "same writer path; not yet exercised against a real catalog." When we finally exercised them, eleven tests failed in the same way. The matrix had been telling us where the gap was. We had stopped listening.

The three-level scale also disciplines the writeups. "Full" cells need evidence pointers to passing tests. "Partial" cells need explicit caveats listing what does not work. "None" cells need a one-sentence "why deferred." Every cell has to say something concrete. Nothing in the matrix is allowed to be vibes.

## What gets measured changes what gets built

There is a corollary to "what gets measured gets managed" that vendors do not love: when the measurement is public, the management has to be public too.

Working through the matrix in order taught us something about our own roadmap. We had been writing features in the order we found interesting. The matrix wanted them in the order the spec defined them. The two orders were different.

Case in point. SQE had partition evolution working at the planning layer for months but the writer was not flagging unpartitioned-but-evolved tables correctly. The matrix called the cell `partition-evolution:v2` "partial" with a caveat about the writer. We knew about the caveat. We did not prioritise it. The cell stared at us every week until someone fixed the writer. That took a day. The cell flipped from partial to full. The actual feature had been ninety percent done for a quarter.

The phenomenon repeats. Cells that read "partial" for a long time without progress are tells. Either the priority is wrong or the caveat is hiding work that nobody wants to own. Both of those are useful signals. Internal roadmaps do not produce that signal because internal roadmaps reflect what was prioritised, not what is missing.

## The rubric matters more than the scores

The hardest part of building a matrix is not collecting the data. It is choosing what to measure.

The Iceberg Matrix lists sixty-three capabilities. Some of them are large (`merge-on-read:v3`, `equality-deletes:v3`). Some of them are narrow (`column-default-values:v3`, `nanosecond-timestamps:v3`). Some are about catalogs, some about file formats, some about compute features. The rubric reflects choices: which axes matter, where to draw cell boundaries, what counts as a complete capability.

A bad rubric grades on irrelevant things. A bad rubric leaves out the things that fail in production. A bad rubric makes everyone look good or everyone look bad, which collapses to no information either way.

Iceberg Matrix's rubric works because it tracks features the spec defines. Each capability maps to a section of the v2 or v3 specification. The rubric is not vendor-defined. It is spec-defined. Vendors can argue about whether their cell deserves "partial" or "full," but they cannot argue that a cell should not exist. The Iceberg specification said it should.

This is the part other ecosystems should copy. If your standard has a written specification, the spec is your rubric source. Walk the table of contents. Each subsection is a candidate cell. The rubric becomes "does the implementation satisfy this subsection." The scores become reproducible by anyone with the spec and a test runner.

## What a "partial" cell teaches you

Sixty-three cells per engine is a lot of partial cells across the industry.

Look at the matrix today. Every engine has them. Trino has partial cells for Iceberg V3 features that are still being implemented upstream. Spark has partial cells where the Iceberg-Spark integration lags the spec. PyIceberg has partial cells for write-path features that are still in progress. Snowflake has partial cells for capabilities that exist but only in private preview.

The partial cell is honest. It says "we are working on it, here is how far we have gotten, here is what is missing." That honesty is rare in vendor communication. It is normalised in the matrix because the format demands it.

Comparing partial cells across engines tells you what the ecosystem has not yet figured out. If three engines all have the same partial cell with the same caveat, the gap is not one engine's problem. It is the standard's problem. Or the catalog's problem. Or a missing reference implementation's problem. Standards bodies should read those.

## What a "full" cell with no evidence teaches you

The other tell is "full" without evidence. If a cell is graded full but the evidence link points at "see vendor docs" or "covered by integration suite (private)," that cell is doing aspirational work.

The matrix format mitigates this by requiring evidence pointers. Apache projects can link to test files. Closed-source vendors can link to documentation. The evidence quality varies, but the requirement to link something is a forcing function. Engines that say "trust us, it works" without a pointer have to either find one or downgrade the cell.

We hit this when reviewing our own scores. The `format-version-3:write-support` cell was sitting at "full" because the unit tests passed. The unit tests really did pass. They asserted that the SQE writer set `format_version: V3` on the metadata object before sending it to the catalog. They did not assert that the catalog accepted it.

When we wrote the integration test that hit Polaris with a real V3 schema, eleven tests failed. The catalog had been silently downgrading the table to V2 because the iceberg-rust REST writer was not forwarding the format-version property. The "full" cell deserved "partial." We downgraded it, fixed the bug, then upgraded it back to "full" with evidence pointing at a passing integration test against a real catalog.

The matrix made that ratchet possible. Full means evidence. Evidence means tests that run. Tests that run means the bug surfaces.

## The scoreboard for the catalog war

The matrix tracks not just file format support but catalog support. There are cells for `polaris:v2`, `nessie:v2`, `aws-glue-catalog:v2`, `hive-metastore:v2`, `unity-catalog:v2`, and so on. Each catalog gets v2 and v3 cells. Each cell needs evidence.

This is the scoreboard for the catalog interoperability question. It is also a scoreboard for the strategic position of each vendor.

Catalogs that speak the open Iceberg REST protocol can move quickly through the matrix. Adding a new catalog backend means a small docker-compose overlay and one integration test. We just did this for Hive Metastore (Thrift), Project Nessie (Iceberg REST adapter), and AWS S3 Tables (Iceberg REST + SigV4). Each one took about a day. None of them required engine changes. The matrix flipped five cells in one branch.

Catalogs that lock you into a proprietary protocol move slowly through the matrix or not at all. Cross-engine validation is harder when the catalog only speaks one engine's wire format. The cells stay partial. The vendors can argue all they want about feature parity, but the matrix shows the gap.

Open protocols win in interoperability matrices because they cost less to adopt across implementations. This is not a controversial claim. It is just what the matrix data says.

## What we learned about our own engine

Sitting on the matrix for six weeks taught us things our internal tests did not.

We learned that "the unit tests pass" is not a synonym for "the feature works." The V3 quiet bug is the canonical example. The unit tests passed for months while Polaris silently rejected every V3 column type at create-table time.

We learned that catalog support is one of the most underrated dimensions of the spec. Most users will never directly care about which delete encoding their engine uses. They will care a lot about whether their engine can talk to the catalog they are mandated to use.

We learned that the deferred-cells list is the part of the matrix you should look at first. The cells that say "none" with "deferred to next cycle" are the parts of the spec that the entire ecosystem has not figured out yet. Variant, shredded variant, geometry, lineage. None of the engines have those. The matrix shows where the standard is still moving.

We learned that the score is the wrong way to read the matrix. Going from 153 to 158 sounds like a 3% improvement. The capability gain (talking to four new catalogs and the AWS S3 Tables service) is much bigger. Scoreboards collapse quality into a number. The cells, the caveats, and the evidence links are where the actual story lives.

## Other standards need their own matrix

Iceberg has a matrix. Most other open standards do not, and they should.

Apache Parquet does not have a public capability matrix tracking which writers and readers support which encodings, page index features, or bloom filter variants. The result is that compatibility surprises ship as production bugs. We hit one ourselves: certain Parquet bloom filter encodings work with Spark and break with the Rust reader. There was no scoreboard to consult.

Apache Avro has the same gap. So does the OpenLineage spec. So does the Substrait IR. Standards without scoreboards drift into a state where every implementation supports a subtly different subset and the user finds out at the worst possible time.

A matrix would help all of these. The structure is portable. Pick the spec. Walk the sections. Define cells. Pick three levels. Require evidence links. Make the rubric public. Accept pull requests.

The work is not technical. The work is editorial discipline. Someone has to maintain the matrix. Someone has to grade pull requests against the rubric. Someone has to push back when a vendor wants "full" without evidence. The Iceberg Matrix has done that work for the Iceberg ecosystem. It is one of the most useful artefacts in the lakehouse space.

## What this is for

The matrix is not a marketing tool. It is a coordination tool.

For users, it tells you which engines actually do the thing the spec says they do, with links to verify. For vendors, it tells you where you stand against the rest of the field, with caveats you cannot hide. For the standards body, it tells you where the spec is moving fast (lots of partials filling in) and where it is stuck (cells that have stayed "none" across every engine for a year).

Public scoreboards for open standards are how ecosystems mature. Closed compatibility claims are how lock-in happens. Pick the one that matches your strategy.

We picked the matrix.

---

## Update 2026-05-27

The matrix pattern transferred. When we ported DuckDB's Quack protocol four weeks after this post, the first artefact we wrote was a type matrix at `docs/quack-datatype-matrix.md`: one row per DuckDB type, status column with the same three-level rubric, evidence column pointing to live `duckdb 1.5.3` CLI sessions. Every parameterised type subsequently got its own MR with the matrix row updated in the same diff. The matrix doc was the design doc and the work tracker. The Quack arc finished in two days; the type matrix told us exactly what was left and when to stop.

Same pattern, different protocol. Same lesson: the rubric beats the score, the evidence beats the claim, and the cell-by-cell discipline is what actually moves the work forward.
