"""Upstream's run_engine.py, reading the workload from S3 instead of local disk.

    python run_engine_s3.py <engine> <size e.g. 10m> <query> <mem e.g. 3g>
    python run_engine_s3.py spark-jars          # resolve the S3A jars once, untimed

The DuckDB, Polars and Spark blocks are upstream's (engine-bench@920658085cd9)
with one change each: the two tables are read from
$EB_S3_PREFIX/sales_<size>/*.parquet and $EB_S3_PREFIX/products.parquet on the
S3 endpoint $EB_S3_ENDPOINT (RustFS from docker-compose.yml, path-style, plain
HTTP), with the keys in $S3_ACCESS_KEY / $S3_SECRET_KEY. The answer is still
written to local disk, exactly where upstream writes it, so `bench.py validate`
compares every engine the same way. Queries come from upstream's queries.py.
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

ENDPOINT = os.environ["EB_S3_ENDPOINT"]          # http://127.0.0.1:39000
PREFIX = os.environ["EB_S3_PREFIX"].rstrip("/")  # s3://warehouse/engine-bench/data
KEY = os.environ["S3_ACCESS_KEY"]
SECRET = os.environ["S3_SECRET_KEY"]
REGION = "us-east-1"
THREADS = 8
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.5.0"  # matches pyspark 4.2's hadoop-client 3.5.0


def spark_session(mem, tmp):
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master(f"local[{THREADS}]")
            .config("spark.driver.memory", mem)
            .config("spark.local.dir", tmp)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", HADOOP_AWS)
            .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
            .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.access.key", KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate())


if sys.argv[1:] == ["spark-jars"]:
    spark_session("1g", os.path.join(UPSTREAM, "out", "tmp", "spark")).stop()
    raise SystemExit(0)

engine, size, query, mem = sys.argv[1:5]
SALES = f"{PREFIX}/sales_{size}"
PRODUCTS = f"{PREFIX}/products.parquet"
OUT = os.path.join(UPSTREAM, "out", engine, f"{size}_{query}")
TMP = os.path.join(UPSTREAM, "out", "tmp", engine)
shutil.rmtree(OUT, ignore_errors=True)
os.makedirs(os.path.dirname(OUT), exist_ok=True)
os.makedirs(TMP, exist_ok=True)

from queries import SQL, polars_query  # noqa: E402

res: dict = {"engine": engine, "size": size, "query": query, "mem": mem, "storage": "s3"}

if engine == "duckdb":
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{mem.upper()}B'")
    con.execute(f"SET threads = {THREADS}")
    con.execute(f"SET temp_directory = '{TMP}'")
    con.execute("INSTALL httpfs")  # a no-op once cached; before t1, so untimed
    con.execute("LOAD httpfs")
    host = ENDPOINT.split("://", 1)[1]
    con.execute(f"CREATE SECRET bench_s3 (TYPE s3, KEY_ID '{KEY}', SECRET '{SECRET}', "
                f"REGION '{REGION}', ENDPOINT '{host}', URL_STYLE 'path', USE_SSL false)")
    con.execute(f"CREATE VIEW sales AS SELECT * FROM read_parquet('{SALES}/*.parquet')")
    con.execute(f"CREATE VIEW products AS SELECT * FROM read_parquet('{PRODUCTS}')")
    res["version"] = duckdb.__version__
    t1 = time.perf_counter()
    con.execute(f"COPY ({SQL[query]}) TO '{OUT}.parquet' (FORMAT parquet)")
    t2 = time.perf_counter()
    res["rows_out"] = con.execute(f"SELECT COUNT(*) FROM read_parquet('{OUT}.parquet')").fetchone()[0]

elif engine == "polars":
    os.environ.setdefault("POLARS_MAX_THREADS", str(THREADS))
    import polars as pl
    opts = {"aws_access_key_id": KEY, "aws_secret_access_key": SECRET, "aws_region": REGION,
            "aws_endpoint_url": ENDPOINT, "aws_allow_http": "true"}
    sales = pl.scan_parquet(f"{SALES}/*.parquet", storage_options=opts)
    products = pl.scan_parquet(PRODUCTS, storage_options=opts)
    res["version"] = pl.__version__
    t1 = time.perf_counter()
    polars_query(query, sales, products).sink_parquet(f"{OUT}.parquet")
    t2 = time.perf_counter()
    res["rows_out"] = pl.scan_parquet(f"{OUT}.parquet").select(pl.len()).collect().item()

elif engine == "spark":
    import pyspark
    spark = spark_session(mem, TMP)
    spark.sparkContext.setLogLevel("ERROR")
    spark.read.parquet(SALES.replace("s3://", "s3a://", 1)).createOrReplaceTempView("sales")
    spark.read.parquet(PRODUCTS.replace("s3://", "s3a://", 1)).createOrReplaceTempView("products")
    res["version"] = pyspark.__version__
    t1 = time.perf_counter()
    spark.sql(SQL[query]).write.mode("overwrite").parquet(OUT)
    t2 = time.perf_counter()
    res["rows_out"] = spark.read.parquet(OUT).count()
    spark.stop()
else:
    raise SystemExit(f"unknown engine {engine}")

res["setup_s"] = round(t1 - T0, 3)      # interpreter + import + session start
res["query_s"] = round(t2 - t1, 3)      # read Parquet from S3 -> compute -> write Parquet locally
print("RESULT " + json.dumps(res), flush=True)
