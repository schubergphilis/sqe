**STATUS 2026-11-09: web UI design refresh (`feat/web-ui-design-refresh`).**

`crates/sqe-coordinator/src/web_ui/dashboard.html` restyled after a data-catalog
mock-up: every tab opens with a one-sentence summary; condition banners with an
action (memory > 80 %, slots >= 90 %, failures in the last hour -> History
filtered to failed, unhealthy workers); one metric strip in place of six stat
cards; tabs grouped under Engine / Workload eyebrows; sortable headers whose
order survives the 2 s refresh; inline bars on elapsed, scanned and in-flight;
state pills; a queued/planning/execution bar on the query detail; dark theme
from `prefers-color-scheme`. Still one self-contained file: no webfonts, CDNs or
build step. Endpoints, escaping, History paging and the 404 fallback unchanged.

Verified against a mock JSON server in Playwright (dark, light, 390 px wide, no
horizontal scroll); `cargo test -p sqe-coordinator --lib web_ui` 15 passed. Not
run against a live coordinator. The book screenshot
`images/sqe-web-ui-overview.png` still shows the old design.

**NEXT:** retake `sqe-web-ui-overview.png` from a live coordinator.
