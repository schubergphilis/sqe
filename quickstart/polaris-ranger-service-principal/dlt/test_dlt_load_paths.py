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
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://rustfs:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "s3admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "s3adminpw")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
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
SCD2_INITIAL_EVENT = [
    {
        "id": 1,
        "name": "Ada",
        "tier": "bronze",
        "effective_at": datetime(2026, 1, 1, 0, 0, 0),
    }
]
SCD2_MULTI_CHANGE = [
    {
        "id": 1,
        "name": "Ada",
        "tier": "silver",
        "effective_at": datetime(2026, 1, 2, 0, 0, 0),
    },
    {
        "id": 1,
        "name": "Ada",
        "tier": "gold",
        "effective_at": datetime(2026, 1, 3, 0, 0, 0),
    },
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
        # Flight SQL may expose an empty result schema for DDL/DML and defer
        # execution until the result stream is consumed.  Drain that stream
        # even when ``cursor.description`` is empty; closing the connection
        # before doing so can cancel CREATE/INSERT before Polaris commits it.
        if SQE_TRANSPORT == "flight":
            try:
                return [tuple(row) for row in cursor.fetchall()]
            except Exception as error:
                # SQE currently terminates an empty Flight result with EOF.
                # ADBC surfaces that terminator as OperationalError although
                # the update has completed successfully.
                if cursor.description or "EOF" not in str(error):
                    raise
                return []
        # The Trino client returns rows as lists, while ADBC Flight SQL follows
        # DB-API and returns tuples. Normalize at the transport boundary so all
        # load-path assertions are protocol-independent.
        return [tuple(row) for row in cursor.fetchall()] if cursor.description else []
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

    os.environ["DESTINATION__ICEBERG__CATALOG_TYPE"] = "rest"


def native_resource(
    table: str,
    rows: Iterable[dict[str, Any]],
    disposition: Any,
    *,
    primary_key: Any = "id",
    columns: Any = None,
):
    return dlt.resource(
        list(rows),
        name=table,
        primary_key=primary_key,
        columns=columns,
        write_disposition=disposition,
        table_format="iceberg",
    )


def clean_native_state() -> None:
    """Reset dlthub's dataset-wide control tables between native scenarios."""
    for table in ("_dlt_version", "_dlt_loads", "_dlt_pipeline_state"):
        clean(table)


def native_pipeline(table: str):
    from dlthub.destinations.impl.iceberg.factory import iceberg

    pipeline_name = f"e2e_native_{table}"
    reset_pipeline(pipeline_name)
    token = bearer_token()
    configure_native_iceberg(token)
    destination = iceberg(
        catalog_type="rest",
        credentials={
            "uri": POLARIS_URI,
            "warehouse": WAREHOUSE,
            "properties": {
                "token": token,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": S3_ACCESS_KEY,
                "s3.secret-access-key": S3_SECRET_KEY,
                "s3.region": S3_REGION,
                "s3.path-style-access": "true",
            },
            "headers": {
                "Authorization": f"Bearer {token}",
                # PyIceberg defaults this header to vended-credentials. The
                # demo catalog has STS disabled and uses the static S3 options
                # above, so override the default with an empty delegation set.
                "X-Iceberg-Access-Delegation": "",
            },
        },
    )
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=NAMESPACE,
    )


def native_run(
    pipeline,
    table: str,
    rows: Iterable[dict[str, Any]],
    disposition: Any,
    *,
    primary_key: Any = "id",
    columns: Any = None,
) -> None:
    info = pipeline.run(
        native_resource(
            table,
            rows,
            disposition,
            primary_key=primary_key,
            columns=columns,
        )
    )
    assert not info.has_failed_jobs, str(info)


def native_scd2_run(pipeline, table: str, rows: Iterable[dict[str, Any]]) -> None:
    """Materialize SCD2 history, then load it through native Iceberg REST.

    dlthub's Iceberg destination currently advertises delete-insert and upsert,
    but not its built-in SCD2 merge strategy. The test therefore computes the
    version rows explicitly and uses the destination's supported replace load.
    """
    # The history columns are SQL TIMESTAMP (without time zone). Keep the
    # Python value naive as well so dlt/PyIceberg does not infer timestamptz.
    boundary = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        existing = sql(
            f"SELECT id,name,tier,_dlt_valid_from,_dlt_valid_to FROM {fq(table)}"
        )
    except Exception:
        existing = []

    incoming = {row["id"]: row for row in rows}
    active_ids: set[Any] = set()
    history: list[dict[str, Any]] = []
    for row_id, name, tier, valid_from, valid_to in existing:
        current = incoming.get(row_id)
        if valid_to is None:
            active_ids.add(row_id)
            if current is None or (name, tier) != (current["name"], current["tier"]):
                valid_to = boundary
        history.append(
            {
                "id": row_id,
                "name": name,
                "tier": tier,
                "_dlt_valid_from": valid_from,
                "_dlt_valid_to": valid_to,
            }
        )

    for row_id, row in incoming.items():
        current = next(
            (old for old in existing if old[0] == row_id and old[4] is None),
            None,
        )
        if current is None or current[1:3] != (row["name"], row["tier"]):
            history.append(
                {
                    **row,
                    "_dlt_valid_from": boundary,
                    "_dlt_valid_to": None,
                }
            )

    native_run(
        pipeline,
        table,
        history,
        "replace",
        primary_key=("id", "_dlt_valid_from"),
        columns={
            "_dlt_valid_from": {
                "data_type": "timestamp",
                "nullable": False,
                "timezone": False,
            },
            "_dlt_valid_to": {
                "data_type": "timestamp",
                "nullable": True,
                "timezone": False,
            },
        },
    )


def native_scd2_events_run(
    pipeline, table: str, events: Iterable[dict[str, Any]]
) -> None:
    """Apply ordered changes in memory and commit one native dlt snapshot."""
    try:
        existing = sql(
            f"SELECT id,name,tier,_dlt_valid_from,_dlt_valid_to FROM {fq(table)}"
        )
    except Exception:
        existing = []
    history = [
        {
            "id": row_id,
            "name": name,
            "tier": tier,
            "_dlt_valid_from": valid_from,
            "_dlt_valid_to": valid_to,
        }
        for row_id, name, tier, valid_from, valid_to in existing
    ]
    for event in sorted(events, key=lambda row: row["effective_at"]):
        active = next(
            (
                row
                for row in history
                if row["id"] == event["id"] and row["_dlt_valid_to"] is None
            ),
            None,
        )
        if active and (active["name"], active["tier"]) == (
            event["name"],
            event["tier"],
        ):
            continue
        if active:
            active["_dlt_valid_to"] = event["effective_at"]
        history.append(
            {
                "id": event["id"],
                "name": event["name"],
                "tier": event["tier"],
                "_dlt_valid_from": event["effective_at"],
                "_dlt_valid_to": None,
            }
        )

    native_run(
        pipeline,
        table,
        history,
        "replace",
        primary_key=("id", "_dlt_valid_from"),
        columns={
            "_dlt_valid_from": {
                "data_type": "timestamp",
                "nullable": False,
                "timezone": False,
            },
            "_dlt_valid_to": {
                "data_type": "timestamp",
                "nullable": True,
                "timezone": False,
            },
        },
    )


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
            if self.mode not in ("scd2", "scd2_events")
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
        elif self.mode == "scd2_events":
            self._scd2_events(rows)
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
        existing = sql(
            f"SELECT id,name,tier FROM {fq(self.table)} WHERE _dlt_valid_to IS NULL"
        )
        incoming = {row["id"]: row for row in rows}
        active_ids = {row_id for row_id, _, _ in existing}
        source: list[tuple[Any, Any, Any, str]] = []

        for row_id, name, tier in existing:
            changed = incoming.get(row_id)
            if changed is None:
                source.append((row_id, name, tier, "expire"))
            elif (name, tier) != (changed["name"], changed["tier"]):
                source.append((row_id, changed["name"], changed["tier"], "expire"))
                source.append((row_id, changed["name"], changed["tier"], "insert"))

        for row_id, row in incoming.items():
            if row_id not in active_ids:
                source.append((row_id, row["name"], row["tier"], "insert"))

        expiring = [row for row in source if row[3] == "expire"]
        if expiring:
            values = ",".join(
                f"({quote(row_id)},TIMESTAMP {quote(boundary)})"
                for row_id, *_ in expiring
            )
            sql(
                f"MERGE INTO {fq(self.table)} AS target "
                f"USING (VALUES {values}) AS source(id,boundary) "
                "ON target.id=source.id AND target._dlt_valid_to IS NULL "
                "WHEN MATCHED THEN UPDATE SET _dlt_valid_to=source.boundary"
            )

        successors = [row for row in source if row[3] == "insert"]
        if successors:
            values = ",".join(
                f"({quote(row_id)},{quote(name)},{quote(tier)},"
                f"TIMESTAMP {quote(boundary)})"
                for row_id, name, tier, _ in successors
            )
            sql(
                f"MERGE INTO {fq(self.table)} AS target "
                f"USING (VALUES {values}) AS source(id,name,tier,boundary) "
                "ON target.id=source.id AND target._dlt_valid_to IS NULL "
                "WHEN NOT MATCHED THEN INSERT "
                "(id,name,tier,_dlt_valid_from,_dlt_valid_to) VALUES "
                "(source.id,source.name,source.tier,source.boundary,NULL)"
            )

    def _scd2_events(self, rows: list[dict[str, Any]]) -> None:
        for event in sorted(rows, key=lambda row: row["effective_at"]):
            boundary = event["effective_at"]
            active = sql(
                f"SELECT name,tier FROM {fq(self.table)} "
                f"WHERE id={quote(event['id'])} AND _dlt_valid_to IS NULL"
            )
            if active and active[0] == (event["name"], event["tier"]):
                continue
            if active:
                sql(
                    f"MERGE INTO {fq(self.table)} AS target "
                    f"USING (VALUES ({quote(event['id'])},TIMESTAMP {quote(boundary)})) "
                    "AS source(id,boundary) "
                    "ON target.id=source.id AND target._dlt_valid_to IS NULL "
                    "WHEN MATCHED THEN UPDATE SET _dlt_valid_to=source.boundary"
                )
            sql(
                f"MERGE INTO {fq(self.table)} AS target "
                f"USING (VALUES ({quote(event['id'])},{quote(event['name'])},"
                f"{quote(event['tier'])},TIMESTAMP {quote(boundary)})) "
                "AS source(id,name,tier,boundary) "
                "ON target.id=source.id AND target._dlt_valid_to IS NULL "
                "WHEN NOT MATCHED THEN INSERT "
                "(id,name,tier,_dlt_valid_from,_dlt_valid_to) VALUES "
                "(source.id,source.name,source.tier,source.boundary,NULL)"
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
    primary_key = ("id", "effective_at") if mode == "scd2_events" else "id"
    resource = dlt.resource(list(rows), name=table, primary_key=primary_key)
    info = pipeline.run(resource)
    assert not info.has_failed_jobs, str(info)


@pytest.mark.parametrize("path", ["native", "sqe"])
def test_replace_overwrites_the_table(path: str) -> None:
    table = f"dlt_{path}_replace"
    clean(table)
    try:
        if path == "native":
            clean_native_state()
            pipeline = native_pipeline(table)
            native_run(pipeline, table, INITIAL, "replace")
            native_run(pipeline, table, CHANGED[:1], "replace")
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
            clean_native_state()
            pipeline = native_pipeline(table)
            mode = {"disposition": "merge", "strategy": "upsert"}
            native_run(pipeline, table, INITIAL, mode)
            native_run(pipeline, table, CHANGED, mode)
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
            clean_native_state()
            pipeline = native_pipeline(table)
            native_scd2_run(pipeline, table, INITIAL)
            native_scd2_run(pipeline, table, CHANGED)
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


@pytest.mark.parametrize("path", ["native", "sqe"])
def test_scd2_orders_multiple_changes_for_one_key_in_one_run(path: str) -> None:
    table = f"dlt_{path}_scd2_multi"
    clean(table)
    try:
        if path == "native":
            clean_native_state()
            pipeline = native_pipeline(table)
            native_scd2_events_run(pipeline, table, SCD2_INITIAL_EVENT)
            native_scd2_events_run(pipeline, table, SCD2_MULTI_CHANGE)
        else:
            sqe_run(table, SCD2_INITIAL_EVENT, "scd2_events")
            # Both changes are deliberately delivered in one dlt run.
            sqe_run(table, SCD2_MULTI_CHANGE, "scd2_events")

        assert sql(
            f"SELECT tier,_dlt_valid_from,_dlt_valid_to FROM {fq(table)} "
            "WHERE id=1 ORDER BY _dlt_valid_from"
        ) == [
            (
                "bronze",
                datetime(2026, 1, 1),
                datetime(2026, 1, 2),
            ),
            (
                "silver",
                datetime(2026, 1, 2),
                datetime(2026, 1, 3),
            ),
            ("gold", datetime(2026, 1, 3), None),
        ]
        assert sql(
            f"SELECT COUNT(*) FROM {fq(table)} "
            "WHERE id=1 AND _dlt_valid_to IS NULL"
        ) == [(1,)]
    finally:
        clean(table)
