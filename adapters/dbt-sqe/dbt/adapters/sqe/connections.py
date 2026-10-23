"""SQE connection manager using ADBC Flight SQL."""

import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Tuple

import agate
import pyarrow
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    Credentials,
)
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.clients.agate_helper import Integer, Number
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError


def _agate_type(arrow_type: pyarrow.DataType) -> agate.DataType:
    """The agate type for an Arrow column, taken from the schema.

    agate's own inference reads a 0/1 integer column as Boolean and a string
    column of digits ("001") as Number; the Arrow type already says which it is.
    """
    t = pyarrow.types
    if t.is_boolean(arrow_type):
        return agate.Boolean()
    if t.is_integer(arrow_type):
        return Integer()
    if t.is_floating(arrow_type) or t.is_decimal(arrow_type):
        return Number()
    if t.is_date(arrow_type):
        return agate.Date()
    if t.is_timestamp(arrow_type):
        return agate.DateTime()
    return agate.Text()


@dataclass
class SQECredentials(Credentials):
    """Connection credentials for SQE.

    Three auth styles are supported (see ``auth.flight_db_kwargs``):
    - Basic: ``user`` + ``password`` (human user).
    - OAuth client_credentials (service principal): ``method: oauth`` with
      ``client_id`` + ``client_secret``. SQE runs the grant and forwards the token.
    - Pre-obtained bearer: ``token`` (the client fetched its own OAuth token).
    """

    host: str = "localhost"
    port: int = 50051
    user: Optional[str] = None
    password: Optional[str] = None
    database: str = "warehouse"
    schema: str = "default"
    # OAuth / service-principal auth.
    method: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    token: Optional[str] = None
    # Transport: TLS unless explicitly disabled (local plaintext stacks).
    tls: bool = True
    tls_root_certs: Optional[str] = None

    _ALIASES = {"catalog": "database"}

    @property
    def type(self) -> str:
        return "sqe"

    @property
    def unique_field(self) -> str:
        return self.host

    def _connection_keys(self) -> Tuple[str, ...]:
        # Never expose password / client_secret / token in dbt debug output.
        return ("host", "port", "tls", "database", "schema", "user", "method", "client_id")


# Leading `/* dbt query comment */` and `-- line` comments, then the first
# keyword. Statements with no result set go through Flight SQL ExecuteUpdate
# (`executescript` -> execute_update): ExecuteQuery+DoGet on them hits #771
# on SQE < 0.49. INSERT/MERGE/DELETE/UPDATE return a rows_affected row via
# ExecuteQuery, so they stay on `execute` and keep their rowcount.
_NO_RESULT_SQL = re.compile(
    r"^(?:\s*(?:/\*.*?\*/|--[^\n]*))*\s*(?:create|drop|alter|set|grant|revoke|comment|use)\b",
    re.IGNORECASE | re.DOTALL,
)


# ExecuteQuery reports a failing statement only when its stream is read, and
# closing an unread DML cursor cancels the write. dbt never fetches DML
# (statement('main') has fetch_result=False), so these are read on execute.
_DML_SQL = re.compile(
    r"^(?:\s*(?:/\*.*?\*/|--[^\n]*))*\s*\(?\s*(?:insert|delete|update|merge)\b",
    re.IGNORECASE | re.DOTALL,
)


class _Cursor:
    """ADBC cursor proxy: routes no-result statements to ExecuteUpdate and
    reads DML results on execute, so failures raise and writes complete."""

    def __init__(self, inner):
        self._inner = inner
        self._result = None

    def execute(self, sql, parameters=None):
        self._result = None
        if parameters is None and _NO_RESULT_SQL.match(sql):
            self._inner.executescript(sql)
            self._result = pyarrow.table({})
        else:
            self._inner.execute(sql, parameters)
            if _DML_SQL.match(sql):
                self._result = self._inner.fetch_arrow_table()
        return self

    def fetch_arrow_table(self):
        if self._result is not None:
            return self._result
        return self._inner.fetch_arrow_table()

    @property
    def rowcount(self):
        result = self._result
        if result is not None and "rows_affected" in result.column_names and result.num_rows == 1:
            return result.column("rows_affected")[0].as_py()
        return self._inner.rowcount

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _Handle:
    """ADBC connection proxy whose cursors are `_Cursor`s.

    It also holds the one scoped transaction a materialization may open
    around its DML (`begin_transaction` / `commit_transaction`). The DB-API
    commit() and rollback() do nothing on an autocommit connection, so this
    drives the ADBC connection underneath. dbt calls rollback() and close()
    on every failure path, and both end an open transaction there. A model
    that fails between its DELETE and INSERT therefore leaves the target
    as it was.
    """

    def __init__(self, inner):
        self._inner = inner
        self.in_transaction = False

    def cursor(self, *args, **kwargs):
        return _Cursor(self._inner.cursor(*args, **kwargs))

    def begin_transaction(self):
        if self.in_transaction:
            raise DbtRuntimeError("an SQE transaction is already open on this connection")
        # Leaving autocommit starts a Flight SQL transaction (BeginTransaction).
        self._inner.adbc_connection.set_autocommit(False)
        self.in_transaction = True

    def commit_transaction(self):
        # Returning to autocommit commits the open transaction. If the commit
        # fails, the transaction stays marked open so close() rolls it back.
        self._inner.adbc_connection.set_autocommit(True)
        self.in_transaction = False

    def rollback(self):
        if not self.in_transaction:
            return self._inner.rollback()
        self.in_transaction = False
        adbc = self._inner.adbc_connection
        try:
            adbc.rollback()
        finally:
            adbc.set_autocommit(True)

    def close(self):
        try:
            if self.in_transaction:
                self.rollback()
        finally:
            self._inner.close()

    def __getattr__(self, name):
        return getattr(self._inner, name)


class SQEConnectionManager(SQLConnectionManager):
    """Manages ADBC Flight SQL connections to SQE."""

    TYPE = "sqe"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            return connection

        credentials = connection.credentials

        uri = f"{credentials.host}:{credentials.port}"
        try:
            from adbc_driver_flightsql.dbapi import connect

            from dbt.adapters.sqe.auth import flight_db_kwargs, flight_location

            uri, tls_kwargs = flight_location(
                credentials.host,
                credentials.port,
                tls=credentials.tls,
                tls_root_certs=credentials.tls_root_certs,
            )
            db_kwargs = tls_kwargs | flight_db_kwargs(
                user=credentials.user,
                password=credentials.password,
                method=credentials.method,
                client_id=credentials.client_id,
                client_secret=credentials.client_secret,
                token=credentials.token,
            )
            kwargs: dict = {"uri": uri}
            if db_kwargs:
                kwargs["db_kwargs"] = db_kwargs

            # Each dbt model commits on its own. autocommit=True keeps it so:
            # without it ADBC opens a Flight SQL transaction, and a model's
            # DML stays on the transaction branch until an explicit commit().
            kwargs["autocommit"] = True
            connection.handle = _Handle(connect(**kwargs))
            connection.state = "open"
        except Exception as e:
            connection.handle = None
            connection.state = "fail"
            raise DbtRuntimeError(f"Failed to connect to SQE at {uri}: {e}") from e

        return connection

    # Models autocommit: a transaction refuses the DDL and CTAS a model
    # runs. dbt-adapters sends BEGIN / COMMIT as SQL around every model, so
    # keep its transaction_open bookkeeping and send nothing, as dbt-trino
    # does.
    def add_begin_query(self):
        pass

    def add_commit_query(self):
        pass

    def begin(self):
        connection = self.get_thread_connection()
        connection.transaction_open = True
        return connection

    def commit(self):
        connection = self.get_thread_connection()
        connection.transaction_open = False
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
        """Convert ADBC Arrow result to agate table for dbt.

        A failed read raises: an empty table here would make dbt treat a
        failed metadata query as "no columns" and render broken SQL from it.
        """
        table = cursor.fetch_arrow_table()

        names = [field.name for field in table.schema]
        types = [_agate_type(field.type) for field in table.schema]
        if limit:
            table = table.slice(0, limit)
        # By position: a dict keyed on name dropped a repeated column name.
        rows = [list(row) for row in zip(*(col.to_pylist() for col in table.columns))]
        return agate.Table(rows=rows, column_names=names, column_types=types)
