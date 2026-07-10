# Ebook Analytics

Prose metrics exclude code fences, headings, tables, lists, blockquotes.

| Chapter | Prose words | Sentences | Avg sent len | % short (<=8w) | Long sents (>=30w) | Flesch | Code % of lines | H2/H3 | Callouts | Dead-ends | ! in prose |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 00-preface.md | 1823 | 145 | 12.6 | 41% | 10 | 55 | 0% | 9 | 1 | 0 | 0 |
| 01-the-catalog-wars.md | 5530 | 483 | 11.4 | 42% | 12 | 51 | 27% | 16 | 9 | 1 | 0 |
| 02-tables-made-of-files.md | 4692 | 348 | 13.5 | 33% | 15 | 46 | 44% | 19 | 10 | 2 | 2 |
| 03-the-engine-you-already-have.md | 5447 | 461 | 11.8 | 44% | 9 | 49 | 51% | 16 | 11 | 0 | 0 |
| 04-you-are-the-query.md | 5093 | 370 | 13.8 | 34% | 18 | 47 | 39% | 19 | 8 | 1 | 0 |
| 05-speaking-arrow.md | 3616 | 252 | 14.3 | 32% | 21 | 50 | 59% | 21 | 6 | 1 | 0 |
| 06-the-catalog-is-the-api.md | 2322 | 158 | 14.7 | 25% | 12 | 48 | 65% | 11 | 3 | 0 | 1 |
| 06b-speaking-to-many-catalogs.md | 1411 | 146 | 9.7 | 52% | 1 | 65 | 36% | 9 | 0 | 0 | 0 |
| 06c-attaching-at-runtime.md | 1663 | 155 | 10.7 | 50% | 2 | 58 | 60% | 9 | 0 | 0 | 0 |
| 07-making-dbt-work.md | 3048 | 225 | 13.5 | 36% | 13 | 50 | 31% | 9 | 3 | 0 | 0 |
| 08-writing-is-a-contract.md | 3958 | 287 | 13.8 | 32% | 12 | 50 | 13% | 14 | 3 | 1 | 0 |
| 09-what-you-cant-see.md | 4936 | 353 | 14.0 | 31% | 12 | 46 | 50% | 21 | 6 | 0 | 1 |
| 09b-grant-that-polaris-enforces.md | 2071 | 156 | 13.3 | 34% | 6 | 54 | 20% | 7 | 4 | 1 | 0 |
| 09c-one-policy-two-engines.md | 2095 | 133 | 15.8 | 23% | 8 | 51 | 21% | 8 | 3 | 0 | 1 |
| 10-making-it-operable.md | 7250 | 581 | 12.5 | 38% | 24 | 51 | 34% | 27 | 5 | 0 | 1 |
| 11-why-distribute-at-all.md | 4393 | 346 | 12.7 | 34% | 10 | 52 | 22% | 16 | 4 | 0 | 1 |
| 12-standing-on-ballistas-shoulders.md | 4399 | 360 | 12.2 | 36% | 11 | 46 | 74% | 15 | 5 | 1 | 0 |
| 13-neither-trusts-the-other.md | 4617 | 359 | 12.9 | 32% | 13 | 48 | 69% | 22 | 7 | 2 | 0 |
| 14-failure-is-a-feature.md | 5182 | 395 | 13.1 | 33% | 15 | 52 | 68% | 24 | 4 | 1 | 5 |
| 15-deploying-sovereignty.md | 3611 | 287 | 12.6 | 36% | 9 | 48 | 72% | 11 | 6 | 0 | 0 |
| 16-benchmarks-dont-lie.md | 7343 | 594 | 12.4 | 41% | 25 | 54 | 41% | 28 | 10 | 1 | 2 |
| 16b-the-matrix-and-the-quiet-bug.md | 1723 | 165 | 10.4 | 50% | 6 | 65 | 40% | 10 | 0 | 0 | 0 |
| 16c-following-through.md | 3259 | 256 | 12.7 | 41% | 14 | 65 | 59% | 12 | 0 | 0 | 1 |
| 16d-the-duckdb-drift.md | 1870 | 149 | 12.6 | 39% | 6 | 62 | 52% | 10 | 0 | 0 | 0 |
| 16e-the-lineage-trail.md | 2119 | 216 | 9.8 | 50% | 5 | 64 | 54% | 7 | 0 | 0 | 0 |
| 17-what-wed-do-differently.md | 6789 | 541 | 12.5 | 38% | 25 | 51 | 8% | 19 | 8 | 2 | 0 |
| 18-epilogue.md | 516 | 55 | 9.4 | 53% | 1 | 68 | 0% | 0 | 0 | 0 | 0 |

**Totals:** 100,776 prose words, 7,976 sentences, ~403 pages @250 prose-words/pg, ~503 min read @200wpm.

## Reading the numbers

- **Avg sent len**: voice target is rhythm. 12-18 is healthy. >22 means long-sentence drift.
- **% short**: Jacob's voice leans on short sentences to land points. <20% = not enough punch.
- **Flesch**: higher = easier. 50-65 is good for technical prose. <40 = dense.
- **Long sents (>=30w)**: each is a candidate to split (voice: 'three clauses max').
- **Code %**: very high = a chapter that's more listing than argument; very low in a how-it-works chapter = telling not showing.
- **! in prose**: voice forbids exclamation marks in prose. Any non-zero needs a look (may be false positive from code-ish prose).
