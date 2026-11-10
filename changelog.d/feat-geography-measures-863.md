### Added

- `ST_Area`, `ST_Length`, `ST_Distance` and `ST_DWithin` measure `GEOGRAPHY` values in meters (#863)

    The column's edge algorithm picks the earth model: `spherical` (the
    default) uses a sphere of the GRS80 mean radius, with haversine distance
    and length and the exact spherical excess for area; `vincenty`,
    `thomas`, `andoyer` and `karney` use Karney's geodesics on WGS84.
    Distance takes two points, as Trino's `SphericalGeography`. A computed
    geometry next to a `GEOGRAPHY` value is read as longitude / latitude; a
    `GEOMETRY` column next to one is refused. The predicates, `ST_Centroid`
    and `ST_Envelope` still refuse `GEOGRAPHY`.
