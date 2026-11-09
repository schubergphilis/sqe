# Geospatial

`ST_*` functions over WKB geometry values, from the `sqe-geo` crate. They are registered in every session, in the embedded CLI, and on the workers. The input is the WKB a `GEOMETRY` column holds (see [DDL, GEOMETRY and GEOGRAPHY](./ddl.md#geometry-and-geography)) or the result of another `ST_*` function. A geometry result is little-endian WKB, so it writes straight into a `GEOMETRY` column.

```sql
SELECT id, ST_AsText(shape), ST_Area(shape)
FROM places
WHERE ST_Intersects(shape, ST_MakeEnvelope(4.8, 52.3, 5.0, 52.4));

INSERT INTO places SELECT id + 100, ST_Centroid(shape), NULL FROM places;
```

## Functions

| Function | Returns | Notes |
|---|---|---|
| `ST_GeometryFromText(wkt)`, `ST_GeomFromText(wkt)` | geometry | Parse well-known text. Invalid WKT is an error. |
| `ST_GeomFromBinary(wkb)`, `ST_GeomFromWKB(wkb)` | geometry | Check and return WKB. Also the opt-in for planar math on a `GEOGRAPHY` value, see below. |
| `ST_Point(x, y)` | geometry | A point. |
| `ST_MakeEnvelope(xmin, ymin, xmax, ymax)` | geometry | The rectangle as a polygon. |
| `ST_AsText(g)`, `ST_AsWKT(g)` | varchar | Well-known text, such as `POINT(1 2)`. |
| `ST_AsBinary(g)`, `ST_AsWKB(g)` | varbinary | WKB. |
| `ST_X(g)`, `ST_Y(g)` | double | A point's coordinate. Any other geometry is an error, as in Trino. |
| `ST_XMin`, `ST_XMax`, `ST_YMin`, `ST_YMax` | double | The bounding box. NULL for an empty geometry. |
| `ST_GeometryType(g)` | varchar | `ST_Point`, `ST_LineString`, `ST_Polygon`, `ST_MultiPoint`, ... |
| `ST_NumPoints(g)` | bigint | The number of coordinates. |
| `ST_IsEmpty(g)`, `ST_IsValid(g)` | boolean | |
| `ST_Envelope(g)` | geometry | The bounding box: a polygon, or a point or line when the box has no area. |
| `ST_Area(g)` | double | Planar area. 0 for points and lines. On `GEOGRAPHY`, square meters. |
| `ST_Length(g)` | double | Planar length of a line or multi-line. Any other geometry is an error. On `GEOGRAPHY`, meters. |
| `ST_Distance(a, b)` | double | Planar distance. NULL when either side is empty. On `GEOGRAPHY`, meters between two points. |
| `ST_Centroid(g)` | geometry | |
| `ST_Intersects`, `ST_Contains`, `ST_Within`, `ST_Disjoint`, `ST_Equals`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps` `(a, b)` | boolean | DE-9IM predicates. |
| `ST_DWithin(a, b, d)` | boolean | `ST_Distance(a, b) <= d`. On `GEOGRAPHY`, `d` is in meters. |
| `ST_SRID(g)` | integer | The SRID of the column's CRS: 4326 for `OGC:CRS84`, n for `EPSG:n`. 0 for any other CRS and for a computed geometry, which carries none. |

Every function returns NULL for a NULL argument. Bytes that are not WKB are an error, and so is a non-binary argument where a geometry is expected.

## Semantics

- **`GEOMETRY` math is planar.** Coordinates are treated as Cartesian, whatever the column's CRS. Areas and distances are in the units of the coordinates, so degrees for `OGC:CRS84`.
- **`GEOGRAPHY` measures are in meters.** `ST_Area`, `ST_Length`, `ST_Distance` and `ST_DWithin` measure a `GEOGRAPHY` value on the earth, with x as longitude and y as latitude in degrees. The column's edge algorithm picks the model. `spherical` (the default) uses a sphere of radius 6,371,008.8 m: haversine for distance and length, the exact spherical excess for area. `vincenty`, `thomas`, `andoyer` and `karney` use the WGS84 ellipsoid with Karney's geodesics. `ST_Distance` and `ST_DWithin` take two points, as Trino's `SphericalGeography` does; any other shape is an error. A polygon's area is the smaller side of its ring, whichever way the ring is wound, so no polygon covers more than a hemisphere. The other side may be a computed geometry such as `ST_Point(4.9, 52.4)`, which is read as longitude and latitude. A `GEOMETRY` column next to a `GEOGRAPHY` one, or two `GEOGRAPHY` columns with different edge algorithms, fail at planning time.

    ```sql
    SELECT name FROM cities
    WHERE ST_DWithin(location, ST_Point(4.9041, 52.3676), 50000);  -- within 50 km of Amsterdam
    ```

- **Other planar functions refuse `GEOGRAPHY`.** Planar math on spherical edges gives wrong answers near the poles and the antimeridian, so the predicates, `ST_Centroid`, `ST_Envelope` and the bounding-box functions fail at planning time on a `GEOGRAPHY` value. The formatting and accessor functions (`ST_AsText`, `ST_AsBinary`, `ST_X`, `ST_Y`, `ST_GeometryType`, `ST_NumPoints`, `ST_IsEmpty`) work. To accept planar math anyway, wrap the value: `ST_Centroid(ST_GeomFromBinary(ST_AsBinary(geo)))`.
- **Results carry no CRS.** A geometry result is plain WKB. Written into a `GEOMETRY` column it takes that column's CRS.
- **The CRS belongs to the column.** `DESCRIBE` and `SHOW CREATE TABLE` print it in the form the DDL takes back, as Spark prints it: `geometry(4326)`, `geometry(3857)`, `geography(4326)`, `geography(4326, karney)`. A CRS without an SRID is quoted: `geometry('ESRI:102100')`.
- **No empty points.** `ST_GeomFromText('POINT EMPTY')` is refused, and so is an empty point in WKB. Empty collections work.
- **Bounding-box pruning on `GEOMETRY`.** `ST_Intersects`, `ST_Contains`, `ST_Within`, `ST_Equals`, `ST_Touches`, `ST_Crosses` and `ST_Overlaps` between a geometry column and a constant geometry, and `ST_DWithin(column, constant, d)`, skip the data files whose manifest bounding box misses the constant's, and the row groups whose Parquet geospatial statistics miss it. Workers prune row groups the same way. The predicate still runs on every row read. `ST_Disjoint`, a negated predicate and `GEOGRAPHY` columns are never pruned on.
