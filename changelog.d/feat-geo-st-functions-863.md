### Added

- `ST_*` functions over GEOMETRY values (#863)

    Thirty planar functions from the new `sqe-geo` crate: constructors
    (`ST_GeomFromText`, `ST_GeomFromBinary`, `ST_Point`, `ST_MakeEnvelope`),
    output (`ST_AsText`, `ST_AsBinary`), accessors (`ST_X`, `ST_Y`, the
    bounding box, `ST_GeometryType`, `ST_NumPoints`, `ST_IsEmpty`,
    `ST_IsValid`), measures (`ST_Area`, `ST_Length`, `ST_Distance`,
    `ST_Centroid`, `ST_Envelope`) and the DE-9IM predicates plus
    `ST_DWithin`. They take and return WKB, so a result writes into a
    GEOMETRY column. All math is planar: on a GEOGRAPHY column the measures
    and predicates fail at planning time unless the value is wrapped in
    `ST_GeomFromBinary(ST_AsBinary(...))`. Registered in the coordinator,
    the embedded CLI and the workers. No bounding-box pruning yet.

### Fixed

- A predicate pushed to a worker is no longer dropped whole when one conjunct calls a function (#863)

    The worker decoded the pushed predicate against an empty function
    registry, so any function call failed the decode and the worker shipped
    every row, including for the plain conjuncts next to it. The worker now
    decodes against its own session, and the coordinator leaves out only the
    conjuncts the worker cannot decode. Results were never wrong: the
    coordinator filters again.
