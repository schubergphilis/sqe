"""SQE connection manager using ADBC Flight SQL."""

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Tuple

import agate
import pyarrow  # noqa: F401
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    Credentials,
)
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError


@dataclass
class SQECredentials(Credentials):
    """Connection credentials for SQE."""

    host: str = "localhost"
    port: int = 50051
    user: Optional[str] = None
    password: Optional[str] = None
    database: str = "warehouse"
    schema: str = "default"

    _ALIASES = {"catalog": "database"}

    @property
    def type(self) -> str:
        return "sqe"

    @property
    def unique_field(self) -> str:
        return self.host

    def _connection_keys(self) -> Tuple[str, ...]:
        return ("host", "port", "database", "schema", "user")


class SQEConnectionManager(SQLConnectionManager):
    """Manages ADBC Flight SQL connections to SQE."""

    TYPE = "sqe"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            return connection

        credentials = connection.credentials

        try:
            from adbc_driver_flightsql.dbapi import connect

            uri = f"grpc://{credentials.host}:{credentials.port}"

            kwargs = {"uri": uri}
            if credentials.user:
                kwargs["db_kwargs"] = {
                    "username": credentials.user,
                    "password": credentials.password or "",
                }

            handle = connect(**kwargs)
            connection.handle = handle
            connection.state = "open"
        except Exception as e:
            connection.handle = None
            connection.state = "fail"
            raise DbtRuntimeError(f"Failed to connect to SQE at {uri}: {e}") from e

        return connection

    def cancel(self, connection: Connection):
        if connection.handle:
            try:
                connection.handle.close()
            except Exception:
                pass

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as e:
            msg = str(e)
            raise DbtDatabaseError(msg) from e

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        rowcount = cursor.rowcount if cursor.rowcount >= 0 else -1
        return AdapterResponse(
            _message="OK",
            rows_affected=rowcount,
        )

    @classmethod
    def get_result_from_cursor(cls, cursor, limit=None) -> agate.Table:
        """Convert ADBC Arrow result to agate table for dbt."""
        try:
            table = cursor.fetch_arrow_table()
        except Exception:
            return agate.Table(rows=[], column_names=[], column_types=[])

        if table.num_rows == 0:
            names = [field.name for field in table.schema]
            return agate.Table(rows=[], column_names=names, column_types=[])

        columns = {}
        for i, field in enumerate(table.schema):
            col = table.column(i)
            columns[field.name] = col.to_pylist()

        num_rows = table.num_rows
        if limit:
            num_rows = min(num_rows, limit)

        rows = []
        col_names = list(columns.keys())
        for i in range(num_rows):
            rows.append([columns[name][i] for name in col_names])

        return agate.Table(rows=rows, column_names=col_names)
