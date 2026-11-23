**STATUS 2026-11-23: design for engine-keyed reads of discovered catalogs (`docs/discovered-catalog-engine-read`).**

OpenSpec change `discovered-catalog-engine-read`: proposal, design (D0-D9), tasks and spec. No code yet.

Discovered (polaris-auto) warehouses get the template catalog's engine identity behind an opt-in (`[query] discovery_engine_read`, default off). An allowlist and a membership proof gate it: the person and the engine both resolve the warehouse at the template URL with the same prefix. Ranger default-denies `select` by rule (every name except the configured catalogs without `credentials`). Writes stay delegated. With the opt-in off, behaviour is unchanged: a discovered warehouse reads with the person's delegated keys.

Gates: `openspec validate discovered-catalog-engine-read`; `make leak-scan` on the new files.

**NEXT:** owner review of the design. Then build tasks 1-5 on one branch, with no size-budget raise (D9).
