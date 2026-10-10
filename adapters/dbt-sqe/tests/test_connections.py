"""#771/#772: DDL goes through ExecuteUpdate, queries and DML through ExecuteQuery."""

from dbt.adapters.sqe.connections import _Cursor


class FakeCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, parameters=None):
        self.calls.append(("execute", sql))

    def executescript(self, sql):
        self.calls.append(("executescript", sql))


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
