"""#771/#772: DDL goes through ExecuteUpdate, queries and DML through ExecuteQuery."""

import pyarrow as pa
import pytest

from dbt.adapters.sqe.connections import SQEConnectionManager, _Cursor


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
