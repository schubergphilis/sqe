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
