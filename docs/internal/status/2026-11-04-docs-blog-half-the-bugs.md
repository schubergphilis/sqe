**STATUS 2026-11-04: blog post "Half the bugs were in the benchmark" (`docs/blog-half-the-bugs`).**

`docs/site/blog/2026-11-04-half-the-bugs-were-in-the-benchmark.md`, a follow-up to the 150M post. Part 1 covers the SQE fixes from 11-03 (#787 TopK dynamic filter, #788 embedded pool / sort share / `EXPLAIN ANALYZE` quota / spill compression, the embedded ATTACH storage fallback, `-f json` NULL). Part 2 covers the #785 generator and comparison fixes (item domain, spec DECIMALs, comparator trim, dimension scaling, q64, value comparison). Every number is taken from the status notes of those branches and the 11-04 SF1 compare JSONs.

Style: checked for forbidden characters and words (publication-style); `make leak-scan` clean. A dev.to copy (dev.to front matter, `canonical_url`, absolute link to the 150M post) is generated from this file; the getsqe.com blog URL pattern in it is a guess and must be checked before posting.

Depends on: !1639 and !1640 (dimension scaling, q64) and the value-comparison MR. The post describes the value comparison as the next change; publish after those merge, and update the "Where it stands" table if the SF10 run changes the picture.

**NEXT:** publish after !1639, !1640 and the value-comparison MR merge; then the decimal rounding follow-up named in the value-comparison status note.
