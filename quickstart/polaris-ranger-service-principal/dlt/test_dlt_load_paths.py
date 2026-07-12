"""dltHub E2E coverage for the two supported SQE ingestion paths.

Path 1 uses dlt's native Iceberg destination against Polaris REST with the
calling service principal's bearer token. Path 2 is a dlt custom destination
that emits DML through SQE's Trino-compatible endpoint. The custom destination
is intentional: dlt's generic SQLAlchemy/Trino destination does not advertise
merge or SCD2 because Trino has no primary-key constraints, while SQE provides
Iceberg MERGE/UPDATE semantics that we can exercise directly.
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import dlt
import pytest
import requests
import trino
from trino.auth import BasicAuthentication


TOKEN_URL = os.getenv(
    "KEYCLOAK_TOKEN_URL",
    "http://keycloak:8080/realms/iceberg-ranger/protocol/openid-connect/token",
)
POLARIS_URI = os.getenv("POLARIS_REST_URI", "http://polaris:8181/api/catalog")
WAREHOUSE = os.getenv("POLARIS_WAREHOUSE", "sales_wh")
SQE_URL = os.getenv("SQE_TRINO_URL", "http://sqe:8080")
SQE_FLIGHT_URI = os.getenv("SQE_FLIGHT_URI", "grpc://sqe:50051")
SQE_TRANSPORT = os.getenv("SQE_TRANSPORT", "trino").lower()
CLIENT_ID = os.getenv("SP_ADMIN_CLIENT_ID", "sp-admin")
CLIENT_SECRET = os.getenv("SP_ADMIN_SECRET", "sp-admin-secret")
NAMESPACE = "sales"


INITIAL = [
    {"id": 1, "name": "Ada", "tier": "bronze"},
    {"id": 2, "name": "Grace", "tier": "silver"},
]
CHANGED = [
    {"id": 1, "name": "Ada", "tier": "gold"},
    {"id": 3, "name": "Linus", "tier": "bronze"},
]


def bearer_token() -> str:
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=20,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def sqe_connection():
    if SQE_TRANSPORT == "flight":
        import adbc_driver_flightsql.dbapi
        import adbc_driver_manager

        return adbc_driver_flightsql.dbapi.connect(
            SQE_FLIGHT_URI,
            db_kwargs={
                adbc_driver_manager.DatabaseOptions.USERNAME.value: CLIENT_ID,
                adbc_driver_manager.DatabaseOptions.PASSWORD.value: CLIENT_SECRET,
            },
        )
    if SQE_TRANSPORT != "trino":
        raise ValueError(
            f"SQE_TRANSPORT must be 'trino' or 'flight', got {SQE_TRANSPORT!r}"
        )
    parsed = urlparse(SQE_URL)
    return trino.dbapi.connect(
        host=parsed.hostname or "sqe",
        port=parsed.port or 8080,
        http_scheme=parsed.scheme or "http",
        user=CLIENT_ID,
        auth=BasicAuthentication(CLIENT_ID, CLIENT_SECRET),
        catalog=WAREHOUSE,
        schema=NAMESPACE,
    )


def sql(statement: str) -> list[tuple[Any, ...]]:
    connection = sqe_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(statement)
        return cursor.fetchall() if cursor.description else []
    finally:
        connection.close()


def quote(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def fq(table: str) -> str:
    return f'{WAREHOUSE}.{NAMESPACE}."{table}"'


def clean(table: str) -> None:
    sql(f"DROP TABLE IF EXISTS {fq(table)}")


@pytest.fixture(scope="session", autouse=True)
def services_are_ready() -> None:
    bearer_token()
    assert sql("SELECT 1") == [(1,)]


def reset_pipeline(name: str) -> None:
    data_dir = Path(os.getenv("DLT_DATA_DIR", str(Path.home() / ".dlt")))
    shutil.rmtree(data_dir / "pipelines" / name, ignore_errors=True)


def configure_native_iceberg(token: str) -> None:
    """Configure the dedicated dltHub Iceberg REST destination.

    Polaris vends the RustFS credentials and endpoint to PyIceberg. Running the
    test in the compose network makes the internal ``rustfs:9000`` endpoint
    resolvable without exposing storage credentials to the test code.
    """

    values = {
        "DESTINATION__ICEBERG__CATALOG_TYPE": "rest",
        "DESTINATION__ICEBERG__CREDENTIALS__URI": POLARIS_URI,
        "DESTINATION__ICEBERG__CREDENTIALS__WAREHOUSE": WAREHOUSE,
        "DESTINATION__ICEBERG__CREDENTIALS__PROPERTIES__TOKEN": token,
        "DESTINATION__ICEBERG__CREDENTIALS__PROPERTIES__HEADER.X-ICEBERG-ACCESS-DELEGATION": "vended-credentials",
    }
    os.environ.update(values)


def native_resource(table: str, rows: Iterable[dict[str, Any]], disposition: Any):
    return dlt.resource(
        list(rows),
        name=table,
        primary_key="id",
        write_disposition=disposition,
        table_format="iceberg",
    )


def native_run(table: str, rows: Iterable[dict[str, Any]], disposition: Any) -> None:
    pipeline_name = f"e2e_native_{table}"
    configure_native_iceberg(bearer_token())
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="iceberg",
        dataset_name=NAMESPACE,
    )
    info = pipeline.run(native_resource(table, rows, disposition))
    assert not info.has_failed_jobs, str(info)


class SqeTableLoader:
    """Small deterministic dlt destination for SQE-specific Iceberg DML."""

    def __init__(self, table: str, mode: str) -> None:
        self.table = table
        self.mode = mode

    def load(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        sql(
            f"CREATE TABLE IF NOT EXISTS {fq(self.table)} "
            "(id BIGINT, name VARCHAR, tier VARCHAR)"
            if self.mode != "scd2"
            else f"CREATE TABLE IF NOT EXISTS {fq(self.table)} "
            "(id BIGINT, name VARCHAR, tier VARCHAR, "
            "_dlt_valid_from TIMESTAMP, _dlt_valid_to TIMESTAMP)"
        )
        if self.mode == "replace":
            sql(f"DELETE FROM {fq(self.table)}")
            self._insert(rows)
        elif self.mode == "merge":
            self._merge(rows)
        elif self.mode == "scd2":
            self._scd2(rows)
        else:
            raise ValueError(f"unknown load mode: {self.mode}")

    def _insert(self, rows: list[dict[str, Any]]) -> None:
        values = ",".join(
            f"({quote(row['id'])},{quote(row['name'])},{quote(row['tier'])})"
            for row in rows
        )
        sql(f"INSERT INTO {fq(self.table)} (id,name,tier) VALUES {values}")

    def _merge(self, rows: list[dict[str, Any]]) -> None:
        values = ",".join(
            f"({quote(row['id'])},{quote(row['name'])},{quote(row['tier'])})"
            for row in rows
        )
        sql(
            f"MERGE INTO {fq(self.table)} AS target "
            f"USING (VALUES {values}) AS source(id,name,tier) "
            "ON target.id = source.id "
            "WHEN MATCHED THEN UPDATE SET name = source.name, tier = source.tier "
            "WHEN NOT MATCHED THEN INSERT (id,name,tier) "
            "VALUES (source.id,source.name,source.tier)"
        )

    def _scd2(self, rows: list[dict[str, Any]]) -> None:
        boundary = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        for row in rows:
            active = sql(
                f"SELECT name,tier FROM {fq(self.table)} "
                f"WHERE id={quote(row['id'])} AND _dlt_valid_to IS NULL"
            )
            current = (row["name"], row["tier"])
            if active and active[0] == current:
                continue
            if active:
                sql(
                    f"UPDATE {fq(self.table)} SET _dlt_valid_to=TIMESTAMP {quote(boundary)} "
                    f"WHERE id={quote(row['id'])} AND _dlt_valid_to IS NULL"
                )
            sql(
                f"INSERT INTO {fq(self.table)} "
                "(id,name,tier,_dlt_valid_from,_dlt_valid_to) VALUES "
                f"({quote(row['id'])},{quote(row['name'])},{quote(row['tier'])},"
                f"TIMESTAMP {quote(boundary)},NULL)"
            )


def sqe_run(table: str, rows: Iterable[dict[str, Any]], mode: str) -> None:
    loader = SqeTableLoader(table, mode)

    @dlt.destination(
        name=f"sqe_{mode}",
        batch_size=1000,
        skip_dlt_columns_and_tables=True,
        loader_parallelism_strategy="sequential",
    )
    def sqe_destination(items, _table) -> None:
        loader.load(list(items))

    pipeline_name = f"e2e_sqe_{table}"
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=sqe_destination,
        dataset_name=NAMESPACE,
    )
    resource = dlt.resource(list(rows), name=table, primary_key="id")
    info = pipeline.run(resource)
    assert not info.has_failed_jobs, str(info)


@pytest.mark.parametrize("path", ["native", "sqe"])
def test_replace_overwrites_the_table(path: str) -> None:
    table = f"dlt_{path}_replace"
    clean(table)
    try:
        if path == "native":
            native_run(table, INITIAL, "replace")
            native_run(table, CHANGED[:1], "replace")
        else:
            sqe_run(table, INITIAL, "replace")
            sqe_run(table, CHANGED[:1], "replace")
        assert sql(f"SELECT id,name,tier FROM {fq(table)} ORDER BY id") == [
            (1, "Ada", "gold")
        ]
    finally:
        clean(table)


@pytest.mark.parametrize("path", ["native", "sqe"])
def test_delta_merge_updates_and_inserts(path: str) -> None:
    table = f"dlt_{path}_merge"
    clean(table)
    try:
        if path == "native":
            mode = {"disposition": "merge", "strategy": "upsert"}
            native_run(table, INITIAL, mode)
            native_run(table, CHANGED, mode)
        else:
            sqe_run(table, INITIAL, "merge")
            sqe_run(table, CHANGED, "merge")
        assert sql(f"SELECT id,tier FROM {fq(table)} ORDER BY id") == [
            (1, "gold"),
            (2, "silver"),
            (3, "bronze"),
        ]
    finally:
        clean(table)


@pytest.mark.parametrize("path", ["native", "sqe"])
def test_scd2_keeps_history_and_one_active_version(path: str) -> None:
    table = f"dlt_{path}_scd2"
    clean(table)
    try:
        if path == "native":
            mode = {"disposition": "merge", "strategy": "scd2"}
            native_run(table, INITIAL, mode)
            native_run(table, CHANGED, mode)
        else:
            sqe_run(table, INITIAL, "scd2")
            sqe_run(table, CHANGED, "scd2")
        assert sql(
            f"SELECT COUNT(*) FROM {fq(table)} WHERE id=1"
        ) == [(2,)]
        assert sql(
            f"SELECT tier FROM {fq(table)} "
            "WHERE id=1 AND _dlt_valid_to IS NULL"
        ) == [("gold",)]
        assert sql(
            f"SELECT COUNT(*) FROM {fq(table)} "
            "WHERE id=1 AND _dlt_valid_to IS NOT NULL"
        ) == [(1,)]
    finally:
        clean(table)
