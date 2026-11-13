"""DuckDB and Polars reading the workload as Iceberg tables from the Polaris REST catalog.

    python run_engine_polaris.py <engine> <size e.g. 150m> <query> <mem e.g. 3g>

    engine: duckdb-polaris   DuckDB's iceberg extension, ATTACHed to Polaris
            polars-polaris   pyiceberg's REST catalog + pl.scan_iceberg (polars[iceberg]:
                             pyiceberg plus the polars-iceberg scan planner)

Both read polaris.bench_<size>.sales / .products, the tables `bench.py load`
writes through SQE, so they read the same Iceberg snapshot sqe-polaris and
sqe-embedded-polaris read. The query text and the Polars plans are upstream's
(queries.py, engine-bench@920658085cd9); only the source of the two tables
changes. The answer goes to local disk where run_engine.py writes it, so
`bench.py validate` compares every engine the same way.

Timing, the same rule for every Polaris engine: opening the catalog (token,
GET /v1/config, the extension load) is setup and untimed; loading the table
metadata, planning the scan, reading, computing and writing the answer is
query_s.

Polaris here has no STS (stsUnavailable in bootstrap.sh), so neither client
asks for vended credentials: both read RustFS with the keys in
$S3_ACCESS_KEY / $S3_SECRET_KEY, like every other engine in this bench.
"""
import json
import os
import shutil
import sys
import time

T0 = time.perf_counter()
HERE = os.path.dirname(os.path.abspath(__file__))
UPSTREAM = os.path.join(os.environ.get("EB_WORK", os.path.join(HERE, ".work")), "engine-bench")
sys.path.insert(0, UPSTREAM)

S3_ENDPOINT = os.environ["EB_S3_ENDPOINT"]                         # http://127.0.0.1:39000
CATALOG_URI = f"http://localhost:{os.environ['POLARIS_PORT']}/api/catalog"
TOKEN_URL = f"{CATALOG_URI}/v1/oauth/tokens"
WAREHOUSE = "bench"                                                # the Polaris catalog bootstrap.sh creates
CLIENT_SECRET = os.environ["POLARIS_ROOT_SECRET"]
KEY = os.environ["S3_ACCESS_KEY"]
SECRET = os.environ["S3_SECRET_KEY"]
REGION = "us-east-1"
THREADS = 8

engine, size, query, mem = sys.argv[1:5]
NS = f"bench_{size}"
OUT = os.path.join(UPSTREAM, "out", engine, f"{size}_{query}")
TMP = os.path.join(UPSTREAM, "out", "tmp", engine)
shutil.rmtree(OUT, ignore_errors=True)
os.makedirs(os.path.dirname(OUT), exist_ok=True)
os.makedirs(TMP, exist_ok=True)

from queries import SQL, polars_query  # noqa: E402

res: dict = {"engine": engine, "size": size, "query": query, "mem": mem, "storage": "s3"}

if engine == "duckdb-polaris":
    import duckdb
    con = duckdb.connect()

    def duck(sql):
        """Every statement is built from this script's own arguments and
        environment (memory, threads, catalog and S3 settings, local paths)
        and the constant queries in queries.py; DuckDB SET / CREATE SECRET /
        ATTACH / COPY take no bind parameters."""
        # nosemgrep: opt.aikido.semgrep-rules.python.sqlalchemy.security.sqlalchemy-execute-raw-query, opt.aikido.semgrep-rules.python.lang.security.audit.formatted-sql-query -- in-process DuckDB, operator-supplied config only
        return con.execute(sql)

    duck(f"SET memory_limit = '{mem.upper()}B'")
    duck(f"SET threads = {THREADS}")
    duck(f"SET temp_directory = '{TMP}'")
    duck("INSTALL iceberg")  # a no-op once cached; before t1, so untimed
    duck("INSTALL httpfs")
    duck("LOAD iceberg")
    duck("LOAD httpfs")
    host = S3_ENDPOINT.split("://", 1)[1]
    duck(f"CREATE SECRET bench_s3 (TYPE s3, KEY_ID '{KEY}', SECRET '{SECRET}', "
         f"REGION '{REGION}', ENDPOINT '{host}', URL_STYLE 'path', USE_SSL false)")
    duck(f"CREATE SECRET bench_polaris (TYPE iceberg, CLIENT_ID 'root', CLIENT_SECRET '{CLIENT_SECRET}', "
         f"OAUTH2_SERVER_URI '{TOKEN_URL}', OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL')")
    duck(f"ATTACH '{WAREHOUSE}' AS polaris (TYPE iceberg, SECRET bench_polaris, "
         f"ENDPOINT '{CATALOG_URI}', ACCESS_DELEGATION_MODE 'none')")
    # Views, so upstream's SQL runs unchanged; DuckDB binds (and loads the
    # Iceberg metadata of) the tables when the COPY plans, inside t1..t2.
    duck(f"CREATE VIEW sales AS SELECT * FROM polaris.{NS}.sales")
    duck(f"CREATE VIEW products AS SELECT * FROM polaris.{NS}.products")
    res["version"] = duckdb.__version__
    t1 = time.perf_counter()
    duck(f"COPY ({SQL[query]}) TO '{OUT}.parquet' (FORMAT parquet)")
    t2 = time.perf_counter()
    res["rows_out"] = duck(f"SELECT COUNT(*) FROM read_parquet('{OUT}.parquet')").fetchone()[0]

elif engine == "polars-polaris":
    os.environ.setdefault("POLARS_MAX_THREADS", str(THREADS))
    import polars as pl
    import polars_iceberg  # noqa: F401  (the native scan planner; without it Polars plans through pyiceberg)
    import pyiceberg
    from pyiceberg.catalog import load_catalog
    catalog = load_catalog(
        "polaris", type="rest", uri=CATALOG_URI, warehouse=WAREHOUSE,
        credential=f"root:{CLIENT_SECRET}", scope="PRINCIPAL_ROLE:ALL",
        **{"s3.endpoint": S3_ENDPOINT, "s3.region": REGION, "s3.path-style-access": "true",
           "s3.access-key-id": KEY, "s3.secret-access-key": SECRET,
           # pyiceberg asks for vended credentials by default and this
           # Polaris has none to vend; ask for no delegation instead.
           "header.X-Iceberg-Access-Delegation": ""})
    res["version"] = f"{pl.__version__} (pyiceberg {pyiceberg.__version__})"
    t1 = time.perf_counter()
    # load_table is inside the timed window: the other Polaris engines fetch
    # the table metadata while the query plans.
    # Polars' native reader does not take the S3 settings from the pyiceberg
    # FileIO; without these it goes to AWS, not RustFS.
    store = {"aws_endpoint_url": S3_ENDPOINT, "aws_region": REGION, "aws_allow_http": "true",
             "aws_virtual_hosted_style_request": "false",
             "aws_access_key_id": KEY, "aws_secret_access_key": SECRET}
    sales = pl.scan_iceberg(catalog.load_table(f"{NS}.sales"), storage_options=store)
    products = pl.scan_iceberg(catalog.load_table(f"{NS}.products"), storage_options=store)
    polars_query(query, sales, products).sink_parquet(f"{OUT}.parquet")
    t2 = time.perf_counter()
    res["rows_out"] = pl.scan_parquet(f"{OUT}.parquet").select(pl.len()).collect().item()

else:
    raise SystemExit(f"unknown engine {engine}")

res["setup_s"] = round(t1 - T0, 3)      # interpreter + import + catalog open
res["query_s"] = round(t2 - t1, 3)      # table metadata -> scan from S3 -> compute -> write Parquet locally
print("RESULT " + json.dumps(res), flush=True)
