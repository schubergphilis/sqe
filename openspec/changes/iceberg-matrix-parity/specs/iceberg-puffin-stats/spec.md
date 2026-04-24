## ADDED Requirements

### Requirement: Parquet bloom filter write

The system SHALL write Parquet bloom filters for columns listed in the table property `write.parquet.bloom-filter-columns`. Default FPP is 0.01 (1% false-positive rate). Absence of the property means no bloom filters.

#### Scenario: Bloom filters written for configured columns

- **GIVEN** a table with `write.parquet.bloom-filter-columns = 'id,session_id'`
- **WHEN** the user runs CTAS or INSERT writing new data files
- **THEN** each produced Parquet file contains bloom filters for `id` and `session_id`
- **AND** `parquet-tools meta` on the file shows `bloom_filter_enabled = true` for those columns

#### Scenario: Bloom filters absent by default

- **GIVEN** a table with no `write.parquet.bloom-filter-columns` property
- **WHEN** the user runs INSERT
- **THEN** produced files have NO bloom filters
- **AND** file size is not inflated

### Requirement: Bloom filter read pruning

The system SHALL probe Parquet bloom filters during scan planning for equality predicates on bloom-enabled columns. Files whose bloom filter does not contain the predicate value SHALL be skipped entirely (no row-group open, no footer parse beyond manifest).

#### Scenario: Point query skips files via bloom

- **GIVEN** a table with 100 data files each containing bloom filters on `id`
- **AND** the target id `42` is present in only 1 file
- **WHEN** the user runs `SELECT * FROM ns.t WHERE id = 42`
- **THEN** EXPLAIN ANALYZE shows `files_pruned_bloom >= 95`
- **AND** the result is correct (the 1 matching row is returned)

#### Scenario: Bloom miss does not cause false negatives

- **GIVEN** a bloom filter with 1% FPP
- **WHEN** 1000 point queries are issued for random existing ids
- **THEN** all 1000 return the correct row
- **AND** no false negatives occur (every existing id is found)

### Requirement: Puffin NDV sketch emission

The system SHALL emit a Puffin sidecar file after every successful data-file-producing commit (CTAS, INSERT, MERGE) containing a `apache-datasketches-theta-v1` blob per data column. The sketch is used for downstream NDV estimation.

#### Scenario: Puffin sidecar emitted on CTAS

- **WHEN** the user runs `CREATE TABLE ns.t AS SELECT id, category FROM source`
- **THEN** a `.stats.puffin` file is present under the table's metadata directory
- **AND** the Puffin file contains one `apache-datasketches-theta-v1` blob per column
- **AND** the corresponding manifest entry references the Puffin statistics file

#### Scenario: Sketch accuracy within 5% of exact

- **GIVEN** a column with 1M distinct values
- **WHEN** the Puffin theta sketch is read
- **THEN** the estimated NDV is within 5% of the true count (1M +/- 50K)

### Requirement: DataFusion StatisticsSource consumer

The system SHALL implement the DataFusion `StatisticsSource` trait (landing in DF 54 per apache/datafusion#21157) using the Puffin sidecar as the backing store. The consumer SHALL serve NDV, min/max, and null count statistics per column during optimization.

#### Scenario: Join reorder uses Puffin NDV

- **GIVEN** two tables with Puffin NDV sketches and no Parquet-footer stats
- **WHEN** the user runs a join query where the CBO must choose join order
- **THEN** the chosen order matches the CBO output when exact statistics are available
- **AND** EXPLAIN shows the NDV estimate taken from Puffin

#### Scenario: Fallback to Parquet stats when Puffin absent

- **GIVEN** a table with no Puffin sidecar (legacy data)
- **WHEN** the CBO requests statistics
- **THEN** the StatisticsSource falls back to Parquet-footer min/max/null_count
- **AND** NDV is estimated from row count (conservative)

### Requirement: Bloom filter column inference

The system SHALL provide a helper `CALL system.suggest_bloom_filter_columns(table => 'schema.table')` that inspects recent query logs and suggests columns for bloom filter enablement based on equality-predicate frequency.

#### Scenario: Suggestion based on query patterns

- **GIVEN** the query log contains 100 queries, 80 of which filter on `customer_id`
- **WHEN** the user runs `CALL system.suggest_bloom_filter_columns(table => 'ns.orders')`
- **THEN** the output includes a row `(column='customer_id', equality_predicate_count=80, recommended=true)`
