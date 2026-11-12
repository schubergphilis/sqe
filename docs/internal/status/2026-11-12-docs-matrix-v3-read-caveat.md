**STATUS 2026-11-12: the stale `read-support:v3` caveat is gone from `docs/evidence/iceberg-matrix-state.json` (`docs/matrix-v3-read-caveat`, closes #870).**

The caveat said geometry is unsupported and shredded variants are read unshredded. #863 made geometry full and #870 reads only the named `typed_value` fields. The shredded-variant cell keeps its caveat: it states the remaining scope, which matches Iceberg Java. Scores are unchanged.

**NEXT:** none for #870. Open matrix gaps: #862 (Google Lakehouse), #864 and #871 (blocked upstream).
