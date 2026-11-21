"""dbt finds the relations it wrote in the profile's catalog.

SQE qualifies a bare `information_schema.<view>` with the session catalog,
which dbt-sqe never sets, and loads a namespace's tables for an
information_schema read only in the catalogs the statement names. A profile
whose catalog is not the server default therefore found none of its own
relations: a second `dbt seed` appended the rows again (3 became 6), and
incremental and table models rebuilt as if on a first run.

These tests run real dbt commands against `FakeSQE`, an in-process stand-in
for the ADBC Flight SQL connection that answers metadata reads the way SQE
does: a bare `information_schema` lists the session catalog only.
"""

import re
import textwrap
import threading

import pyarrow as pa
import pytest
from dbt.cli.main import dbtRunner

PROFILE_CATALOG = "ws_energy"
SESSION_CATALOG = "warehouse"
SCHEMA = "analytics"

_NAME = r'(?:"[^"]+"|[A-Za-z_][\w$]*)'
_REL = rf"({_NAME}(?:\s*\.\s*{_NAME}){{0,2}})"
_LEADING_COMMENTS = re.compile(r"^(?:\s*(?:/\*.*?\*/|--[^\n]*))*\s*", re.DOTALL)


def _parts(rel):
    return [p.strip().strip('"') for p in re.findall(_NAME, rel)]


class FakeSQE:
    """Tables, schemas and the statement log of one fake SQE server."""

    def __init__(self, session_catalog=SESSION_CATALOG, lookups_see_nothing=False):
        self.session_catalog = session_catalog
        # Simulates a lookup that misses an existing table, whatever the cause.
        self.lookups_see_nothing = lookups_see_nothing
        self.schemas = set()
        self.tables = {}  # (catalog, schema, name) -> {"type": ..., "rows": int, "columns": [...]}
        self.log = []
        self.lock = threading.Lock()

    def key(self, rel):
        parts = _parts(rel)
        if len(parts) == 3:
            return tuple(parts)
        if len(parts) == 2:
            return (self.session_catalog, *parts)
        return (self.session_catalog, SCHEMA, parts[0])

    def rows(self, name):
        return self.tables[(PROFILE_CATALOG, SCHEMA, name)]["rows"]

    def statements(self, pattern):
        return [s for s in self.log if re.search(pattern, s, re.IGNORECASE)]

    # ── statements ────────────────────────────────────────────────────────

    def run(self, sql):
        with self.lock:
            body = _LEADING_COMMENTS.sub("", sql, count=1).strip().rstrip(";").strip()
            self.log.append(body)
            return self._run(body)

    def _run(self, sql):
        flags = re.IGNORECASE | re.DOTALL
        info = re.search(
            rf"\bfrom\s+(?:({_NAME})\s*\.\s*)?information_schema\s*\.\s*(\w+)",
            sql,
            flags,
        )
        if info:
            catalog = (
                info.group(1).strip('"') if info.group(1) else self.session_catalog
            )
            return self._metadata(sql, catalog, info.group(2).lower())

        if m := re.match(
            rf"create\s+schema\s+(?:if\s+not\s+exists\s+)?{_REL}", sql, flags
        ):
            parts = _parts(m.group(1))
            self.schemas.add(
                (parts[0], parts[1])
                if len(parts) == 2
                else (self.session_catalog, parts[0])
            )
            return None
        if m := re.match(
            rf"drop\s+(?:table|view)\s+(?:if\s+exists\s+)?{_REL}", sql, flags
        ):
            self.tables.pop(self.key(m.group(1)), None)
            return None
        if m := re.match(rf"create\s+or\s+replace\s+view\s+{_REL}", sql, flags):
            self.tables[self.key(m.group(1))] = {
                "type": "VIEW",
                "rows": 0,
                "columns": ["id"],
            }
            return None
        if m := re.match(
            rf"create\s+or\s+replace\s+table\s+{_REL}.*?\bas\b", sql, flags
        ):
            self.tables[self.key(m.group(1))] = {
                "type": "BASE TABLE",
                "rows": 1,
                "columns": ["id"],
            }
            return None
        if m := re.match(
            rf"create\s+table\s+(if\s+not\s+exists\s+)?{_REL}\s*\((.*)\)", sql, flags
        ):
            key = self.key(m.group(2))
            if key in self.tables:
                if m.group(1):
                    return None
                raise RuntimeError(f"table {'.'.join(key)} already exists")
            columns = [c.split()[0] for c in m.group(3).split(",") if c.strip()]
            self.tables[key] = {"type": "BASE TABLE", "rows": 0, "columns": columns}
            return None
        if m := re.match(rf"truncate\s+table\s+{_REL}", sql, flags):
            affected = self._table(m.group(1))["rows"]
            self._table(m.group(1))["rows"] = 0
            return pa.table({"rows_affected": [affected]})
        if m := re.match(
            rf"insert\s+into\s+{_REL}\s*(\([^)]*\))?\s*values\s*(.*)", sql, flags
        ):
            added = _count_tuples(m.group(3))
            self._table(m.group(1))["rows"] += added
            return pa.table({"rows_affected": [added]})
        if m := re.match(rf"insert\s+into\s+{_REL}", sql, flags):
            self._table(m.group(1))["rows"] += 1
            return pa.table({"rows_affected": [1]})
        return pa.table({})

    def _table(self, rel):
        key = self.key(rel)
        if key not in self.tables:
            raise RuntimeError(f"table {'.'.join(key)} not found")
        return self.tables[key]

    def _metadata(self, sql, catalog, view):
        def literal(column):
            m = re.search(rf"\b{column}\s*=\s*'([^']*)'", sql, re.IGNORECASE)
            return m.group(1) if m else None

        visible = (
            []
            if self.lookups_see_nothing
            else [(key, t) for key, t in self.tables.items() if key[0] == catalog]
        )
        if view == "schemata":
            names = sorted({s for c, s in self.schemas if c == catalog})
            if literal("schema_name") is not None:
                return pa.table({"num_schemas": [int(literal("schema_name") in names)]})
            return pa.table({"schema_name": pa.array(names, pa.string())})

        wanted = {
            0: literal("table_catalog"),
            1: literal("table_schema"),
            2: literal("table_name"),
        }
        hits = [
            (k, t)
            for k, t in visible
            if all(v is None or k[i] == v for i, v in wanted.items())
        ]
        if re.search(r"count\s*\(\s*\*\s*\)", sql, re.IGNORECASE):
            return pa.table({"n": [len(hits)]})
        if view == "columns":
            cols = [c for _, t in hits for c in t["columns"]]
            return pa.table(
                {
                    "column_name": pa.array(cols, pa.string()),
                    "data_type": pa.array(["BIGINT"] * len(cols), pa.string()),
                    "character_maximum_length": pa.array(
                        [None] * len(cols), pa.int64()
                    ),
                    "numeric_precision": pa.array([None] * len(cols), pa.int64()),
                    "numeric_scale": pa.array([None] * len(cols), pa.int64()),
                }
            )
        return pa.table(
            {
                "database": pa.array([k[0] for k, _ in hits], pa.string()),
                "name": pa.array([k[2] for k, _ in hits], pa.string()),
                "schema": pa.array([k[1] for k, _ in hits], pa.string()),
                "type": pa.array(
                    ["view" if t["type"] == "VIEW" else "table" for _, t in hits],
                    pa.string(),
                ),
            }
        )


def _count_tuples(values):
    """Row tuples in a VALUES list: parentheses opened at depth 0."""
    depth, count, quoted = 0, 0, False
    for ch in values:
        if ch == "'":
            quoted = not quoted
        elif not quoted and ch == "(":
            count += depth == 0
            depth += 1
        elif not quoted and ch == ")":
            depth -= 1
    return count


class _FakeCursor:
    def __init__(self, engine):
        self.engine = engine
        self.result = None
        self.rowcount = -1
        self.description = None

    def execute(self, sql, parameters=None):
        self.result = self.engine.run(sql)

    def executescript(self, sql):
        self.engine.run(sql)

    def fetch_arrow_table(self):
        return self.result if self.result is not None else pa.table({})

    def close(self):
        pass


class _FakeAdbc:
    def set_autocommit(self, value):
        pass

    def rollback(self):
        pass


class _FakeConnection:
    def __init__(self, engine):
        self.engine = engine
        self.adbc_connection = _FakeAdbc()

    def cursor(self):
        return _FakeCursor(self.engine)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


@pytest.fixture
def project(tmp_path):
    (tmp_path / "seeds").mkdir()
    (tmp_path / "models").mkdir()
    (tmp_path / "dbt_project.yml").write_text(
        textwrap.dedent("""\
        name: lookup_test
        version: "1.0"
        config-version: 2
        profile: lookup_test
        seed-paths: ["seeds"]
        model-paths: ["models"]
    """)
    )
    (tmp_path / "profiles.yml").write_text(
        textwrap.dedent(f"""\
        lookup_test:
          target: dev
          outputs:
            dev:
              type: sqe
              host: localhost
              port: 50051
              tls: false
              catalog: {PROFILE_CATALOG}
              schema: {SCHEMA}
              threads: 1
    """)
    )
    (tmp_path / "seeds" / "people.csv").write_text("id,name\n1,ann\n2,bob\n3,cy\n")
    (tmp_path / "models" / "events.sql").write_text(
        "{{ config(materialized='incremental', incremental_strategy='append') }}\nselect 1 as id\n"
    )
    (tmp_path / "models" / "summary.sql").write_text(
        "{{ config(materialized='table') }}\nselect 1 as id\n"
    )
    return tmp_path


def _dbt(project, monkeypatch, engine, *args):
    import adbc_driver_flightsql.dbapi as flight

    monkeypatch.setattr(flight, "connect", lambda **kwargs: _FakeConnection(engine))
    result = dbtRunner().invoke(
        [
            *args,
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--no-send-anonymous-usage-stats",
            "--no-partial-parse",
        ]
    )
    assert result.success, result.exception or result.result
    return result


def test_seeding_twice_keeps_the_csv_rows(project, monkeypatch):
    engine = FakeSQE()
    _dbt(project, monkeypatch, engine, "seed")
    _dbt(project, monkeypatch, engine, "seed")
    assert engine.rows("people") == 3
    # The second run found the table and truncated it rather than recreating it.
    assert engine.statements(r'^truncate table "ws_energy"\."analytics"\."people"')


def test_a_seed_whose_table_lookup_misses_still_replaces_the_rows(project, monkeypatch):
    engine = FakeSQE()
    _dbt(project, monkeypatch, engine, "seed")
    engine.lookups_see_nothing = True
    _dbt(project, monkeypatch, engine, "seed")
    assert engine.rows("people") == 3


def test_incremental_model_finds_its_table_and_appends(project, monkeypatch):
    engine = FakeSQE()
    _dbt(project, monkeypatch, engine, "run", "--select", "events")
    _dbt(project, monkeypatch, engine, "run", "--select", "events")
    assert engine.rows("events") == 2
    assert (
        len(
            engine.statements(
                r'^create or replace table "ws_energy"\."analytics"\."events"'
            )
        )
        == 1
    )
    assert (
        len(engine.statements(r'^insert into "ws_energy"\."analytics"\."events"')) == 1
    )


def test_table_model_finds_a_view_in_the_way_and_drops_it(project, monkeypatch):
    engine = FakeSQE()
    engine.schemas.add((PROFILE_CATALOG, SCHEMA))
    engine.tables[(PROFILE_CATALOG, SCHEMA, "summary")] = {
        "type": "VIEW",
        "rows": 0,
        "columns": ["id"],
    }
    _dbt(project, monkeypatch, engine, "run", "--select", "summary")
    assert engine.statements(
        r'^drop view if exists "ws_energy"\."analytics"\."summary"'
    )
    assert engine.tables[(PROFILE_CATALOG, SCHEMA, "summary")]["type"] == "BASE TABLE"


def test_every_metadata_read_names_the_profile_catalog(project, monkeypatch):
    engine = FakeSQE()
    _dbt(project, monkeypatch, engine, "seed")
    _dbt(project, monkeypatch, engine, "run")
    reads = engine.statements(r"information_schema")
    assert reads
    for sql in reads:
        assert re.search(
            r'from\s+"ws_energy"\.information_schema\.', sql, re.IGNORECASE
        ), sql
        assert not re.search(r"(?<![.\w])information_schema\.", sql), sql
