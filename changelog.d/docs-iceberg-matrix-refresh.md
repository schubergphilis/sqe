### Changed

- The Iceberg matrix pages score every engine on the same rubric cells (#855)

    `scripts/render-iceberg-matrix-compare.py` reads upstream's per-platform layout, scores all 36 upstream entries on the 54 cells SQE is scored on (max 162), and pins the rubric and peer scores in `docs/evidence/iceberg-matrix-rubric.json`. `scripts/render-iceberg-matrix.py` renders from that snapshot offline: the peer table no longer mixes /162 with /189, rows follow the rubric (Deletion Vectors, Unknown Type and Google Lakehouse were missing; dropped features are gone), and every open cell is listed with its caveats. The book's score reads 150/162 (92.6%).
