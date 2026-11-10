**STATUS 2026-11-09: GEOGRAPHY measures in meters (`feat/geography-measures-863`, #863).**

`ST_Area`, `ST_Length`, `ST_Distance` and `ST_DWithin` no longer refuse a
`GEOGRAPHY` argument; they measure on the earth (`crates/sqe-geo/src/lib.rs`,
`Earth`, `earth_model`, `sphere_area`, `ellipsoid_area`).

- The edge algorithm in the column's GeoArrow metadata picks the model:
  `spherical` is a sphere of the GRS80 mean radius (haversine distance and
  length, area as the exact spherical excess, summed per edge against the
  north pole); `vincenty` / `thomas` / `andoyer` / `karney` use Karney's
  geodesics on WGS84 (`geo::Geodesic`, `GeodesicArea` after orienting rings,
  since Karney's area reads the winding).
- `geo`'s Chamberlain-Duquette area was the first candidate and is 0.2% off
  on a one-degree square, so it is not used. The exact sum is checked
  against a great-circle octant (pi R^2 / 2, also across the antimeridian
  and with a pole vertex) and against the parallel-bounded square plus the
  great-circle bow.
- Distance / DWithin take two points, as Trino's `SphericalGeography`.
  Rings enclose the smaller side (no polygon over a hemisphere).
- Mixing: an untagged computed geometry (`ST_Point(..)`) is lon/lat; a
  tagged GEOMETRY column next to a GEOGRAPHY one, or two edge models, is a
  planning error. Predicates, centroid, envelope, bounds still refuse it.

Gates: `cargo test -p sqe-geo` 10 passed; clippy clean;
`make test-integration FILTER=st_functions_over_an_sqe_geometry_table`
1 passed (POINT(1 2) to POINT(0 0) = 248,630 m; ST_Centroid on geography
and ST_Distance(geometry, geography) refused).

Left open on #863: spherical predicates (`ST_Intersects` etc. on
GEOGRAPHY), point-to-line distance on the sphere, geospatial types nested
in STRUCT / ARRAY / MAP. The issue's "done when" is met; it can close.

**NEXT:** unchanged: #870 shredded variant, writer-side variant manifest
bounds for nested and double paths, then typed field access on read.
