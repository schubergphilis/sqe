# fix/deny-chrono-unmaintained

**Date:** 2026-11-17

On 2026-11-17, RustSec declared chrono (RUSTSEC-2026-0543), chrono-tz (RUSTSEC-2026-0555) and humantime (RUSTSEC-2026-0542) unmaintained, citing chronotope/chrono#1768. `deps-check` has failed on main and on every MR pipeline since then.

- Every version is affected, and there is no fixed release or known vulnerability.
- arrow 59 and DataFusion 55 depend on chrono and chrono-tz, and the vendored object_store 0.13.2 depends on humantime. SQE uses chrono directly in most of its crates.
- The three IDs are now ignored in `deny.toml` and `.cargo/audit.toml`, with a review-by date of 2027-01-17 and owner sqe-maintainers, like the existing `paste` entry.

Gate, run against an advisory database that contains all three IDs:

- `cargo deny --all-features check advisories`: exit 0, advisories ok.
- `cargo audit`: exit 0.

Left open: moving off chrono depends on arrow and DataFusion moving first.

**NEXT:** at the review date, check arrow's chrono plan and whether a maintained fork took over.
