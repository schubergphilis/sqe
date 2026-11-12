## 1. Groundwork

- [ ] 1.1 Find the file TVF glob and directory listing code and record whether it sees each object key before reading (design open question)
- [ ] 1.2 List every Iceberg creation and registration path (DDL LOCATION, CTAS, register) and every Iceberg load path (REST `load_table`, SQLite catalog)
- [x] 1.3 File the Hive follow-up issue (#886)

## 2. Overlap test in sqe-core

- [ ] 2.1 `TvfPolicy::iceberg_location_overlap(location) -> Option<&str>` over global and per-catalog prefixes, on `canonicalize_object_url`, placeholder segments as wildcards
- [ ] 2.2 Unit tests: the four overlap scenarios of the spec, plus dot segments and percent-encoded slashes refused

## 3. Creation guard

- [ ] 3.1 Check an explicit location before the catalog call on each creation path from 1.2
- [ ] 3.2 CTAS and catalog-chosen locations: check between create and the first data write; drop and fail on overlap; on a failed drop, fail naming the table and record the location
- [ ] 3.3 Tests per path, the failed-drop case included

## 4. Load guard, probe and TVF refusal

- [ ] 4.1 `OverlapRegistry` (process-wide, canonical locations), one error log per location
- [ ] 4.2 Call the overlap test on every Iceberg load path from 1.2, cache hits included
- [ ] 4.3 In `check_object_store`, after the trusted-local and inline exits and before allow-all, admin-role and the allow-list: refuse paths under a recorded location
- [ ] 4.4 Ancestor probe for `metadata/*.metadata.json` between the prefix root and the path, cached per statement; per listed key if 1.1 allows
- [ ] 4.5 Unit tests for 4.3 and 4.4: admin role refused, inline credentials not refused, plain staging files allowed

## 5. Startup warning

- [ ] 5.1 Warn once per allowed prefix without `{user}`

## 6. Integration

- [ ] 6.1 Integration test: Iceberg table under an allowed prefix, never loaded, `read_parquet` of its data path refused by the probe; after load, refused by the registry; `SELECT` still applies a mask
- [ ] 6.2 Integration test: staging recipe end to end (`s3://<bucket>/{catalog}/{user}/`, a discovered catalog, no restart)

## 7. Docs and close-out

- [x] 7.1 Production guide: replace the warehouse prefix examples with a landing bucket and state that a TVF prefix never covers a warehouse
- [ ] 7.2 `read_parquet` page and `deployment/configuration.md`: staging recipe, staging root outside every catalog's `allowedLocations`, the refusals
- [ ] 7.3 README roadmap, status fragment, changelog fragment under Security
