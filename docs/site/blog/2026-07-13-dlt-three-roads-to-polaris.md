---
title: "One dlt pipeline, three roads to Apache Polaris"
description: "A Python ingestion job should not have to choose between an open Iceberg catalog and a governed SQL engine forever. We connected dlt directly to Apache Polaris through Iceberg REST, then sent the same full, incremental, and SCD2 loads through SQE over Trino HTTP and Arrow Flight SQL. The result is one service-principal identity, one catalog, three front doors, and a test suite large enough to expose the bugs hiding at batch boundaries."
pubDate: "2026-07-13"
author: "Jacob Verhoeks"
tags:
  - "dlt"
  - "iceberg"
  - "polaris"
  - "trino"
  - "arrow-flight"
---

*July 13, 2026*

A Python data pipeline and a SQL engine often arrive at the same Iceberg table
through completely different doors.

The Python job wants the open route. It has records in memory, an Apache
Iceberg REST catalog URL, and a service-principal token. It can write Parquet
files and commit an Iceberg snapshot directly through Apache Polaris. No query
engine has to sit in the middle.

The platform team wants the SQL route. It already exposes a Trino-compatible
endpoint to applications, Arrow Flight SQL to columnar clients, and an audit
trail for every statement. It wants ingestion to use the same identity flow,
the same `MERGE INTO`, and the same catalog boundary as every other workload.

Both are reasonable. More importantly, they do not have to be competing
architectures.

We built both with dltHub's `dlt` Python library and pointed them at the same
Polaris catalog:

```text
                         Keycloak service principal
                                    |
                    +---------------+---------------+
                    |                               |
                    v                               v
       dlt native Iceberg destination       dlt custom SQL destination
                    |                               |
                    v                    +----------+----------+
          Polaris Iceberg REST           |                     |
                                     Trino HTTP          Arrow Flight SQL
                                          |                     |
                                          +----------+----------+
                                                     |
                                                    SQE
                                                     |
                    +---------------+----------------+
                                    |
                              Apache Polaris
                                    |
                              Apache Ranger
                                    |
                              Iceberg on S3
```

One identity, one catalog, three front doors.

## The direct road: dlt to Polaris REST

The direct route uses dlt's Iceberg destination. PyIceberg writes the data
files and commits the metadata transaction through Polaris's Iceberg REST API.
There is no generated SQL because there is no SQL engine in this path.

The core of the setup looks like this:

```python
import dlt
from dlthub.destinations.impl.iceberg.factory import iceberg

destination = iceberg(
    catalog_type="rest",
    credentials={
        "uri": "http://polaris:8181/api/catalog",
        "warehouse": "sales_wh",
        "properties": {
            "token": access_token,
            "s3.endpoint": "http://rustfs:9000",
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
        "headers": {
            "Authorization": f"Bearer {access_token}",
            "X-Iceberg-Access-Delegation": "",
        },
    },
)

pipeline = dlt.pipeline(
    pipeline_name="customers_direct",
    destination=destination,
    dataset_name="sales",
)

customers = dlt.resource(
    rows,
    name="customers",
    primary_key="id",
    write_disposition={"disposition": "merge", "strategy": "upsert"},
    table_format="iceberg",
)

pipeline.run(customers)
```

The bearer token belongs to `sp-admin`, a Keycloak confidential client using
the OAuth2 client-credentials grant. Polaris validates that identity and Apache
Ranger authorizes it at the catalog boundary. PyIceberg receives the storage
settings needed to write the files, then Polaris atomically publishes the new
Iceberg metadata.

This route is attractive because it is an open protocol all the way down. The
pipeline depends on Iceberg REST semantics, not on the SQL dialect or runtime
of one engine. For full refreshes, dlt uses `replace`. For deltas, it uses the
Iceberg destination's `merge` with the `upsert` strategy.

## The SQL road: dlt to SQE

The second route is a small custom dlt destination. dlt still owns extraction,
normalization, batching, local pipeline state, and load-job handling. The
destination turns each normalized batch into explicit SQL.

Why custom? dlt's generic SQLAlchemy Trino destination cannot assume primary
keys or advertise SCD2 merge semantics for every Trino table. That is the
correct generic position. Our target is narrower: SQE writing Iceberg tables,
where `MERGE INTO`, `DELETE`, `INSERT`, and `UPDATE` are supported. The adapter
makes that capability explicit.

The shape is intentionally small:

```python
loader = SqeTableLoader(table="customers", mode="merge")

@dlt.destination(
    name="sqe_merge",
    batch_size=1000,
    skip_dlt_columns_and_tables=True,
    loader_parallelism_strategy="sequential",
)
def sqe_destination(items, _table):
    loader.load(list(items))

pipeline = dlt.pipeline(
    pipeline_name="customers_sqe",
    destination=sqe_destination,
    dataset_name="sales",
)

pipeline.run(dlt.resource(rows, name="customers", primary_key="id"))
```

For an incremental load, the adapter emits the SQL contract we want:

```sql
MERGE INTO sales_wh.sales.customers AS target
USING (VALUES
  (1, 'Ada', 'gold'),
  (3, 'Linus', 'bronze')
) AS source(id, name, tier)
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET name = source.name, tier = source.tier
WHEN NOT MATCHED THEN
  INSERT (id, name, tier)
  VALUES (source.id, source.name, source.tier)
```

ID 1 changes, ID 3 is inserted, and an existing ID 2 remains untouched. The
business key comes from dlt. The Iceberg row-level operation is executed by
SQE. Polaris still owns the table metadata and Ranger still authorizes the
same service principal.

## Two protocols, the same SQL

The SQL destination does not care whether SQE receives the statement through
Trino-compatible HTTP or Arrow Flight SQL. That decision lives at the
connection boundary.

The Trino Python client uses the familiar DB-API surface:

```python
import trino
from trino.auth import BasicAuthentication

connection = trino.dbapi.connect(
    host="sqe",
    port=8080,
    http_scheme="http",
    user="sp-admin",
    auth=BasicAuthentication("sp-admin", client_secret),
    catalog="sales_wh",
    schema="sales",
)
```

The Flight route uses the Arrow ADBC Flight SQL driver:

```python
import adbc_driver_flightsql.dbapi
import adbc_driver_manager

connection = adbc_driver_flightsql.dbapi.connect(
    "grpc://sqe:50051",
    db_kwargs={
        adbc_driver_manager.DatabaseOptions.USERNAME.value: "sp-admin",
        adbc_driver_manager.DatabaseOptions.PASSWORD.value: client_secret,
    },
)
```

Both connections execute the same DDL and DML. Trino HTTP is valuable when the
organization already has Trino clients, BI tools, or Python code. Flight SQL is
valuable when the result path should remain Arrow-columnar and feed an ADBC
client without translating through a row-oriented wire format. The ingestion
semantics do not fork just because the transport does.

## Full, incremental, and SCD2 are different contracts

It is tempting to reduce data loading to one word: merge. The tests became much
more useful when we stopped doing that.

A full load says the incoming dataset is the entire desired state. Rows absent
from the new input must disappear. Direct Iceberg expresses this as dlt
`replace`. The SQE adapter performs one `DELETE`, then inserts all normalized
batches.

An incremental load says the incoming dataset contains only new or changed
keys. Missing keys remain. Direct Iceberg uses dlt's upsert strategy; SQE uses
`MERGE INTO`.

Snapshot SCD2 says each run is the complete current source state. Changed rows
close their active version and open a successor. Missing rows close without a
successor. New rows open their first version.

Event-based SCD2 says every input row is a change with an effective timestamp.
Missing keys mean nothing happened. That distinction matters when one dlt run
contains two changes for the same key:

```text
2026-01-01  bronze -> valid until 2026-01-02
2026-01-02  silver -> valid until 2026-01-03
2026-01-03  gold   -> active
```

A single set-based merge can match both source events to the same active target
row, create an ambiguous multi-match, or skip the middle version. The adapter
sorts events by effective time and closes and opens each version before moving
to the next. The direct route builds the same ordered history in memory and
commits one replacement snapshot. Both assert exactly one active row.

## The small test passed. The real bugs waited after row 1,000

Our first examples used Ada, Grace, and Linus. They proved the semantics and
missed the implementation bugs.

We expanded the suite with a 2,005-row initial load, a 1,501-row replacement,
and a 2,505-row delta that produces a 3,505-row final table. The records include
apostrophes, backslashes, Japanese characters, emoji, and null values. Those
cases found three issues.

First, dlt calls a custom destination once per normalized batch. Our initial
replace implementation ran `DELETE` inside every callback. A 1,501-row
replacement with a batch size of 1,000 inserted the first thousand, deleted
them, and retained only the final 501. The fix is simple and important: keep
one loader instance for the run and delete only before its first batch.

Second, dlt normalization can omit a dictionary key whose input value is
`None`. Code that assumes `row["tier"]` exists enters a retry loop with a
`KeyError`. Optional columns now use `row.get("tier")`, and the SQL literal
builder emits `NULL`.

Third, a large Flight query returned two boundary rows in reverse order despite
`ORDER BY id`. The transport was innocent. SQE's default adaptive sort mode can
remove a non-partition sort under memory pressure. The small dataset happened
to arrive in order without the sort; the multi-file dataset did not. That is an
availability tradeoff, but it is not Trino-compatible behavior. This
quickstart sets `query.sort_mode = "strict"`, and the regression test keeps it
honest.

This is why an integration example should become an executable test. The
interesting failures live at batch, file, protocol, and memory boundaries.

## The audit trail is part of the result

The final clean run executed six loading models across the direct and SQE
paths, once with Trino HTTP and once with Flight SQL:

```text
Trino-compatible HTTP: 12 passed
Arrow Flight SQL:       12 passed
Total:                  24 passed
```

SQE produced 624 canonical audit records and 624 OCSF projections. Every record
was valid JSON and successful. The sequence and previous-hash links formed one
intact chain. Every actor was `sp-admin`, no policy decision was denied, and
all DML resources resolved to `sales_wh`. The DML subset contained 32 merges,
14 inserts, and 8 deletes. There were no SQE errors, panics, failed queries, or
missing-table probes.

We keep full result-set auditing disabled. Identity, statement type, resource,
decision, timing, and row counts are enough to prove the write path without
copying sensitive table contents into an audit log.

## Which road should you take?

Use direct Polaris REST when the ingestion job should depend only on the open
Iceberg catalog protocol, when Python owns the write lifecycle, and when the
shortest route from records to an Iceberg snapshot is the priority.

Use SQE when ingestion should share a governed SQL surface with applications,
when explicit `MERGE INTO` behavior is valuable, when SQL-level audit events
matter, or when existing Trino and Flight clients should use the same route.

Use both when different workloads have different needs. They converge on the
same open table. That is the point of Iceberg and Polaris: choosing a better
front door for one workload does not create a second copy of the data.

The complete runnable guide, Docker overlay, test implementation, generated
SQL traces, before-and-after snapshots, and troubleshooting notes live in the
[Polaris, Ranger, service-principal dlt quickstart](../../../quickstart/polaris-ranger-service-principal/dlt/README.md).

Run the complete matrix with:

```bash
cd quickstart/polaris-ranger-service-principal
./dlt/run.sh --both
```

The runner reuses its images. Add `--build` only after changing the Dockerfile,
requirements, or test source. The default should be a fast rerun, because a
resilience suite only protects a system when people are willing to run it.
