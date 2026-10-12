## ADDED Requirements

### Requirement: SPARQL queries execute via DataFusion
The system SHALL compile and execute SPARQL 1.1 SELECT queries against the `rdf.triples` Iceberg table using DataFusion.

#### Scenario: Basic graph pattern
- **GIVEN** `rdf.triples` contains `(:alice, rdf:type, :Customer)`
- **WHEN** `SELECT ?s WHERE { ?s rdf:type :Customer }` is executed
- **THEN** result contains `?s = :alice`

#### Scenario: Multi-pattern BGP
- **GIVEN** triples for subject with two predicates
- **WHEN** SPARQL with two triple patterns on same subject is executed
- **THEN** only subjects matching ALL patterns are returned (inner join semantics)

#### Scenario: Ontology time travel
- **GIVEN** rdf.triples at snapshot S1 has `(:Product, :hasCategory, :Electronics)`
- **AND** snapshot S2 has that triple removed
- **WHEN** SPARQL is run with `FOR SYSTEM_TIME AS OF S1` (via SQL wrapping)
- **THEN** the triple is returned from the historical snapshot

### Requirement: SPARQL dialect auto-detected
- **GIVEN** input string starting with `SELECT ?`, `CONSTRUCT {`, `ASK {`, or `DESCRIBE`
- **WHEN** submitted to `sqe query` or `POST /api/v1/query` with `dialect=auto`
- **THEN** SPARQL compiler is used, not SQL parser
