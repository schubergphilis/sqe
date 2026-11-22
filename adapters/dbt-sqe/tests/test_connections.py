"""#771/#772: DDL goes through ExecuteUpdate, queries and DML through ExecuteQuery."""

import decimal
import sys
import types

import pyarrow as pa
import pytest

from dbt.adapters.sqe.connections import SQEConnectionManager, SQECredentials, _Cursor
from dbt_common.exceptions import DbtRuntimeError


class FakeCursor:
    rowcount = -1

    def __init__(self, result=None, error=None):
        self.calls = []
        self.result = result if result is not None else pa.table({"rows_affected": [3]})
        self.error = error

    def execute(self, sql, parameters=None):
        self.calls.append(("execute", sql))

    def executescript(self, sql):
        self.calls.append(("executescript", sql))

    def fetch_arrow_table(self):
        self.calls.append(("fetch", None))
        if self.error:
            raise self.error
        return self.result


def _route(sql):
    inner = FakeCursor()
    _Cursor(inner).execute(sql)
    return inner.calls[0][0]


def test_ddl_routes_to_execute_update_even_behind_dbt_comment():
    assert _route('/* {"app": "dbt"} */\n  create schema if not exists c.s') == "executescript"
    assert _route("-- note\nDROP TABLE t") == "executescript"
    assert _route("alter table t rename to u") == "executescript"


def test_queries_and_dml_stay_on_execute():
    assert _route("select 1") == "execute"
    assert _route("insert into t select 1") == "execute"
    assert _route("with x as (select 1) select * from x") == "execute"


# ── DML results are read on execute ───────────────────────────────────────
# ExecuteQuery defers a failure to the stream read, and closing an unread
# DML cursor cancels the write; dbt never fetches DML.


def test_dml_is_read_on_execute_so_a_failure_raises():
    for sql in (
        '/* {"app": "dbt"} */\n  INSERT INTO t (select 1)',
        "delete from t where id in (select id from u)",
        "UPDATE t SET a = 1",
        "merge into t using u on t.id = u.id when matched then delete",
        # dbt's truncate_relation, which a seed re-run calls (#891).
        '/* {"app": "dbt"} */\n  truncate table c.s.seed',
    ):
        inner = FakeCursor(error=RuntimeError("cast failed"))
        with pytest.raises(RuntimeError, match="cast failed"):
            _Cursor(inner).execute(sql)
        assert inner.calls == [("execute", sql), ("fetch", None)]


def test_dml_result_is_kept_for_fetch_and_rowcount():
    inner = FakeCursor()
    cursor = _Cursor(inner).execute("insert into t select 1")
    assert cursor.rowcount == 3
    assert cursor.fetch_arrow_table().to_pylist() == [{"rows_affected": 3}]
    assert [c for c in inner.calls if c[0] == "fetch"] == [("fetch", None)]


def test_ddl_fetch_is_an_empty_result():
    cursor = _Cursor(FakeCursor()).execute("drop table t")
    table = SQEConnectionManager.get_result_from_cursor(cursor)
    assert len(table.rows) == 0 and table.column_names == ()


def test_empty_result_keeps_its_column_names():
    inner = FakeCursor(result=pa.table({"column_name": pa.array([], pa.string()), "n": pa.array([], pa.int64())}))
    table = SQEConnectionManager.get_result_from_cursor(_Cursor(inner).execute("select 1"))
    assert table.column_names == ("column_name", "n")


def _agate(result):
    return SQEConnectionManager.get_result_from_cursor(_Cursor(FakeCursor(result=result)).execute("select 1"))


def test_column_types_follow_the_arrow_schema_not_the_values():
    # agate's inference read a 0/1 integer column as Boolean and "001" as Number.
    table = _agate(
        pa.table(
            {
                "flag": pa.array([0, 1], pa.int64()),
                "code": pa.array(["001", "1"], pa.string()),
                "ok": pa.array([True, None], pa.bool_()),
                "x": pa.array([1.5, None], pa.float64()),
            }
        )
    )
    assert [type(t).__name__ for t in table.column_types] == ["Integer", "Text", "Boolean", "Number"]
    assert list(table.rows[0]) == [0, "001", True, decimal.Decimal("1.5")]
    assert list(table.rows[1]) == [1, "1", None, None]


def test_repeated_column_names_keep_every_column_and_limit_applies():
    table = _agate(pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["n", "n"]))
    assert len(table.columns) == 2
    limited = SQEConnectionManager.get_result_from_cursor(
        _Cursor(FakeCursor(result=pa.table({"n": [1, 2, 3]}))).execute("select 1"), limit=2
    )
    assert [r[0] for r in limited.rows] == [1, 2]


def test_failed_metadata_read_raises_instead_of_returning_no_rows():
    # An empty table would make a MERGE render `UPDATE SET` with no columns.
    inner = FakeCursor(error=RuntimeError("Failed to plan view"))
    cursor = _Cursor(inner).execute("select column_name from information_schema.columns")
    with pytest.raises(RuntimeError, match="Failed to plan view"):
        SQEConnectionManager.get_result_from_cursor(cursor)


def test_queries_are_not_read_on_execute():
    inner = FakeCursor()
    cursor = _Cursor(inner).execute("select 1")
    assert inner.calls == [("execute", "select 1")]
    assert cursor.rowcount == -1


# ── #820: SQE autocommits; dbt must not send BEGIN / COMMIT ───────────────

import sys  # noqa: E402
import types  # noqa: E402

from dbt.adapters.sqe.connections import SQECredentials, SQEConnectionManager  # noqa: E402


class _Conn:
    def __init__(self):
        self.transaction_open = False


def _manager_with(conn, sent):
    mgr = SQEConnectionManager.__new__(SQEConnectionManager)
    mgr.get_thread_connection = lambda: conn
    mgr.add_query = lambda sql, *a, **kw: sent.append(sql)
    return mgr


def test_begin_and_commit_send_no_sql_but_track_the_transaction():
    conn, sent = _Conn(), []
    mgr = _manager_with(conn, sent)

    mgr.begin()
    assert conn.transaction_open is True
    mgr.commit()
    assert conn.transaction_open is False
    mgr.add_begin_query()
    mgr.add_commit_query()

    assert sent == []


def test_open_connects_with_autocommit(monkeypatch):
    seen = {}

    def fake_connect(**kwargs):
        seen.update(kwargs)
        return object()

    fake = types.ModuleType("adbc_driver_flightsql.dbapi")
    fake.connect = fake_connect
    monkeypatch.setitem(sys.modules, "adbc_driver_flightsql.dbapi", fake)

    connection = types.SimpleNamespace(
        state="init",
        handle=None,
        credentials=SQECredentials(
            host="sqe", port=50051, database="c", schema="s", tls=False
        ),
    )
    SQEConnectionManager.open(connection)

    assert connection.state == "open"
    assert seen["autocommit"] is True


def _open_with(monkeypatch, credentials):
    seen = {}

    def fake_connect(**kwargs):
        seen.update(kwargs)
        return object()

    fake = types.ModuleType("adbc_driver_flightsql.dbapi")
    fake.connect = fake_connect
    monkeypatch.setitem(sys.modules, "adbc_driver_flightsql.dbapi", fake)
    connection = types.SimpleNamespace(state="init", handle=None, credentials=credentials)
    SQEConnectionManager.open(connection)
    return connection, seen


SCOPE = "zz-scope-marker-772"  # gitleaks:allow


def test_open_sends_call_headers_beside_the_token(monkeypatch):
    creds = SQECredentials(host="sqe", port=50051, database="c", schema="s", tls=False, token="run-token",
                           call_headers={"x-portal-catalog-scope": SCOPE})
    connection, seen = _open_with(monkeypatch, creds)
    assert connection.state == "open"
    assert seen["db_kwargs"] == {
        "adbc.flight.sql.authorization_header": "Bearer run-token",
        "adbc.flight.sql.rpc.call_header.x-portal-catalog-scope": SCOPE,
    }


def test_a_refused_call_header_fails_the_connection_without_its_value(monkeypatch):
    creds = SQECredentials(host="sqe", port=50051, database="c", schema="s", tls=False,
                           call_headers={"authorization": "Bearer " + SCOPE})
    try:
        _open_with(monkeypatch, creds)
    except DbtRuntimeError as e:
        assert "authorization" in str(e) and SCOPE not in str(e)
    else:
        raise AssertionError("expected the connection to fail")


def test_call_header_values_stay_out_of_debug_output_and_repr():
    creds = SQECredentials(host="sqe", port=50051, database="c", schema="s",
                           call_headers={"x-portal-catalog-scope": SCOPE})
    assert "call_headers" not in creds._connection_keys()
    assert SCOPE not in repr(creds)
    assert SCOPE not in str(dict(creds.connection_info()))


def test_call_headers_load_from_a_profile_dict():
    creds = SQECredentials.from_dict({"host": "sqe", "port": 50051, "database": "c", "schema": "s",
                                      "call_headers": {"x-portal-catalog-scope": SCOPE}})
    assert creds.call_headers == {"x-portal-catalog-scope": SCOPE}


# ── Scoped transaction around a multi-statement write (delete+insert) ─────

from dbt.adapters.sqe.connections import _Handle  # noqa: E402


class _FakeAdbc:
    def __init__(self, calls, fail_on=None):
        self.calls = calls
        self.fail_on = fail_on

    def _record(self, call):
        self.calls.append(call)
        if call == self.fail_on:
            raise RuntimeError(f"{call} failed")

    def set_autocommit(self, on):
        self._record(f"autocommit={on}")

    def rollback(self):
        self._record("adbc.rollback")


class _FakeDbapi:
    def __init__(self, fail_on=None):
        self.calls = []
        self.adbc_connection = _FakeAdbc(self.calls, fail_on)

    def rollback(self):
        self.calls.append("dbapi.rollback")

    def close(self):
        self.calls.append("close")


def test_transaction_begins_and_commits_through_autocommit():
    inner = _FakeDbapi()
    handle = _Handle(inner)
    handle.begin_transaction()
    assert handle.in_transaction
    handle.commit_transaction()
    assert not handle.in_transaction
    handle.close()
    assert inner.calls == ["autocommit=False", "autocommit=True", "close"]


def test_dbt_rollback_aborts_an_open_transaction_and_restores_autocommit():
    inner = _FakeDbapi()
    handle = _Handle(inner)
    handle.begin_transaction()
    handle.rollback()
    assert not handle.in_transaction
    assert inner.calls == ["autocommit=False", "adbc.rollback", "autocommit=True"]


def test_close_rolls_back_a_transaction_the_model_never_committed():
    # A Jinja error between DELETE and INSERT reaches neither commit nor
    # exception_handler; dbt still closes the connection.
    inner = _FakeDbapi()
    handle = _Handle(inner)
    handle.begin_transaction()
    handle.close()
    assert inner.calls == ["autocommit=False", "adbc.rollback", "autocommit=True", "close"]


def test_failed_commit_leaves_the_transaction_for_close_to_roll_back():
    inner = _FakeDbapi(fail_on="autocommit=True")
    handle = _Handle(inner)
    handle.begin_transaction()
    with pytest.raises(RuntimeError):
        handle.commit_transaction()
    assert handle.in_transaction
    with pytest.raises(RuntimeError):
        handle.close()
    assert inner.calls[-2:] == ["autocommit=True", "close"]
    assert "adbc.rollback" in inner.calls


def test_rollback_without_a_transaction_is_the_dbapi_rollback():
    inner = _FakeDbapi()
    _Handle(inner).rollback()
    assert inner.calls == ["dbapi.rollback"]


def test_nested_begin_is_refused():
    handle = _Handle(_FakeDbapi())
    handle.begin_transaction()
    with pytest.raises(Exception, match="already open"):
        handle.begin_transaction()


# ── #891: column types for dbt's snapshot / contract macros ───────────────


def test_data_type_code_to_name_renders_sqe_ddl_types():
    # ADBC's cursor.description carries the Arrow type as the type code.
    cases = [
        (pa.bool_(), "BOOLEAN"),
        (pa.int32(), "INTEGER"),
        (pa.int64(), "BIGINT"),
        (pa.uint64(), "BIGINT"),
        (pa.float64(), "DOUBLE"),
        (pa.decimal128(12, 2), "DECIMAL(12,2)"),
        (pa.string(), "VARCHAR"),
        (pa.large_string(), "VARCHAR"),
        (pa.binary(), "VARBINARY"),
        (pa.date32(), "DATE"),
        (pa.timestamp("us"), "TIMESTAMP"),
        (pa.timestamp("us", tz="UTC"), "TIMESTAMP WITH TIME ZONE"),
        (pa.list_(pa.int64()), "ARRAY<BIGINT>"),
        (pa.map_(pa.string(), pa.int32()), "MAP<VARCHAR, INTEGER>"),
        (pa.struct([("a", pa.string()), ("b", pa.int64())]), "STRUCT<a: VARCHAR, b: BIGINT>"),
    ]
    for arrow_type, name in cases:
        assert SQEConnectionManager.data_type_code_to_name(arrow_type) == name, arrow_type
