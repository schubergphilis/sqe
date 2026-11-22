**STATUS 2026-11-22: black-box Iceberg matrix feature suite (`test/iceberg-matrix-independent-suite`, openspec iceberg-matrix-feature-suite 1.x, 2.1-2.7, 3.1-3.3).**

`tests/matrix/` measures SQE against the iceberg-matrix rubric the way the upstream matrix verifies its engines. It works over SQE's client protocols (Trino HTTP, Flight SQL) against a Polaris + RustFS stack. Writes are confirmed by PyIceberg and by raw metadata, manifest, Parquet and Puffin reads, never by SQE. Every rubric cell has a test. Cells agree only when the measured level equals the claimed one. `make matrix-suite SQE_IMAGE=<image>` runs it; `make matrix-export` writes the upstream engine file.

Maintainer decisions (design 9 and 10):
- A cell's level is the engine's capability over any SQE protocol. A feature missing on one protocol is listed under "Protocol gaps" and filed as a bug.
- The bar is the rubric's feature description, read literally.

Author's rules, open to maintainer review because each moves a result toward the claim:
- Design 6: a limit of what the suite can observe is a note, not a partial. Read-support instead proves file pruning black box, by removing a data file the query must skip.
- Design 11: multi-arg v3 is skipped, not failed.
- Bloom filters are rated on the Parquet-footer filters although the rubric text says "Puffin".

Run on v0.57.0, using the release `sqe-server` arm64 binary from the release pipeline artifacts layered onto a distroless base, because the published image needs registry credentials:
- 45 of 54 measured: 39 agree, 6 discrepancies.
- 9 unmeasured: the 8 cloud catalog cells, plus multi-arg v3, which needs a fixture writer.
- Score over measured cells: 129 of 135 claimed.

Discrepancies:
- merge-on-read v2/v3, claimed full, measured partial: without identifier fields, UPDATE and MERGE in merge-on-read mode fall back to copy-on-write.
- schema-evolution v2/v3, claimed full, measured partial: columns cannot be reordered. `ADD COLUMN ... FIRST` is accepted but ignored, and `ALTER COLUMN ... FIRST/AFTER` does not parse.
- statistics v2/v3, claimed full, measured partial: distinct-count Puffin statistics are opt-in (`write.puffin.stats`). The `apache-datasketches-theta-v1` blob body is raw retained hashes, not the DataSketches compact layout, so other engines cannot read the sketch (known in `puffin_stats.rs`).

Protocol gaps: Trino HTTP rejects `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` and `SET WRITE_BRANCH`. All of these work over Flight SQL.

**NEXT:** maintainer review of the six discrepancies (4.2): fix SQE or correct the state file. Also decide the public image route (4.3), then do the official-image run and commit the evidence (4.1). The Spark cross-check (2.6b) is a follow-up.
