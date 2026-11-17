#!/usr/bin/env python3
"""ODBC compatibility probe for a Flight SQL server, via the Apache Arrow
Flight SQL ODBC driver. Prints one line per check: OK <label> <detail> or
FAIL <label> <exception>. One failure never stops the rest."""
import argparse
import os
import sys
import traceback

import pyodbc

DRIVER = "Apache Arrow Flight SQL ODBC Driver"
DEFAULT_TYPED_SQL = (
    "SELECT CAST(1 AS INTEGER) AS c_int, "
    "CAST(1234567890123 AS BIGINT) AS c_bigint, "
    "CAST(1.5 AS DOUBLE) AS c_double, "
    "CAST(123.45 AS DECIMAL(10,2)) AS c_decimal, "
    "CAST('abc' AS VARCHAR) AS c_varchar, "
    "TRUE AS c_boolean, "
    "DATE '2024-01-02' AS c_date, "
    "TIMESTAMP '2024-01-02 03:04:05' AS c_timestamp, "
    "CAST(NULL AS INTEGER) AS c_null"
)

# (name, id, kind) kind: s = string, i = integer/bitmask
INFO = [
    ("SQL_DBMS_NAME", 17, "s"), ("SQL_DBMS_VER", 18, "s"),
    ("SQL_DRIVER_NAME", 6, "s"), ("SQL_DRIVER_VER", 7, "s"),
    ("SQL_DRIVER_ODBC_VER", 77, "s"), ("SQL_ODBC_VER", 10, "s"),
    ("SQL_SERVER_NAME", 13, "s"), ("SQL_DATABASE_NAME", 16, "s"),
    ("SQL_USER_NAME", 47, "s"), ("SQL_DATA_SOURCE_NAME", 2, "s"),
    ("SQL_DATA_SOURCE_READ_ONLY", 25, "s"),
    ("SQL_IDENTIFIER_QUOTE_CHAR", 29, "s"), ("SQL_IDENTIFIER_CASE", 28, "i"),
    ("SQL_QUOTED_IDENTIFIER_CASE", 93, "i"),
    ("SQL_CATALOG_TERM", 42, "s"), ("SQL_SCHEMA_TERM", 39, "s"),
    ("SQL_TABLE_TERM", 45, "s"), ("SQL_PROCEDURE_TERM", 40, "s"),
    ("SQL_CATALOG_NAME_SEPARATOR", 41, "s"), ("SQL_CATALOG_LOCATION", 114, "i"),
    ("SQL_CATALOG_NAME", 10003, "s"), ("SQL_CATALOG_USAGE", 92, "i"),
    ("SQL_SCHEMA_USAGE", 91, "i"),
    ("SQL_MAX_CATALOG_NAME_LEN", 34, "i"), ("SQL_MAX_SCHEMA_NAME_LEN", 32, "i"),
    ("SQL_MAX_TABLE_NAME_LEN", 35, "i"), ("SQL_MAX_COLUMN_NAME_LEN", 30, "i"),
    ("SQL_MAX_PROCEDURE_NAME_LEN", 33, "i"), ("SQL_MAX_IDENTIFIER_LEN", 10005, "i"),
    ("SQL_MAX_COLUMNS_IN_SELECT", 100, "i"), ("SQL_MAX_COLUMNS_IN_TABLE", 101, "i"),
    ("SQL_MAX_TABLES_IN_SELECT", 106, "i"), ("SQL_MAX_STATEMENT_LEN", 105, "i"),
    ("SQL_MAX_ROW_SIZE", 104, "i"), ("SQL_MAX_CONCURRENT_ACTIVITIES", 1, "i"),
    ("SQL_ACTIVE_CONNECTIONS(MAX_DRIVER_CONNECTIONS)", 0, "i"),
    ("SQL_KEYWORDS", 89, "s"), ("SQL_SPECIAL_CHARACTERS", 94, "s"),
    ("SQL_SEARCH_PATTERN_ESCAPE", 14, "s"),
    ("SQL_NUMERIC_FUNCTIONS", 49, "i"), ("SQL_STRING_FUNCTIONS", 50, "i"),
    ("SQL_SYSTEM_FUNCTIONS", 51, "i"), ("SQL_TIMEDATE_FUNCTIONS", 52, "i"),
    ("SQL_CONVERT_FUNCTIONS", 48, "i"),
    ("SQL_TIMEDATE_ADD_INTERVALS", 109, "i"), ("SQL_TIMEDATE_DIFF_INTERVALS", 110, "i"),
    ("SQL_GETDATA_EXTENSIONS", 81, "i"), ("SQL_TXN_CAPABLE", 46, "i"),
    ("SQL_DEFAULT_TXN_ISOLATION", 26, "i"), ("SQL_TXN_ISOLATION_OPTION", 72, "i"),
    ("SQL_MULT_RESULT_SETS", 36, "s"), ("SQL_MULTIPLE_ACTIVE_TXN", 37, "s"),
    ("SQL_NEED_LONG_DATA_LEN", 111, "s"), ("SQL_COLUMN_ALIAS", 87, "s"),
    ("SQL_ORDER_BY_COLUMNS_IN_SELECT", 90, "s"), ("SQL_EXPRESSIONS_IN_ORDERBY", 27, "s"),
    ("SQL_GROUP_BY", 88, "i"), ("SQL_NULL_COLLATION", 85, "i"),
    ("SQL_CONCAT_NULL_BEHAVIOR", 22, "i"), ("SQL_NON_NULLABLE_COLUMNS", 75, "i"),
    ("SQL_CORRELATION_NAME", 74, "i"), ("SQL_LIKE_ESCAPE_CLAUSE", 113, "s"),
    ("SQL_OUTER_JOINS(OJ_CAPABILITIES)", 115, "i"), ("SQL_SQL_CONFORMANCE", 118, "i"),
    ("SQL_ODBC_INTERFACE_CONFORMANCE", 152, "i"),
    ("SQL_SQL92_PREDICATES", 160, "i"), ("SQL_SQL92_RELATIONAL_JOIN_OPERATORS", 161, "i"),
    ("SQL_SQL92_VALUE_EXPRESSIONS", 165, "i"), ("SQL_AGGREGATE_FUNCTIONS", 169, "i"),
    ("SQL_SUBQUERIES", 95, "i"), ("SQL_UNION", 96, "i"),
    ("SQL_INSERT_STATEMENT", 172, "i"), ("SQL_CREATE_TABLE", 132, "i"),
    ("SQL_ALTER_TABLE", 86, "i"), ("SQL_STATIC_CURSOR_ATTRIBUTES1", 167, "i"),
    ("SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1", 146, "i"), ("SQL_CURSOR_COMMIT_BEHAVIOR", 23, "i"),
    ("SQL_CURSOR_ROLLBACK_BEHAVIOR", 24, "i"), ("SQL_SCROLL_OPTIONS", 44, "i"),
    ("SQL_BATCH_SUPPORT", 121, "i"), ("SQL_PARAM_ARRAY_ROW_COUNTS", 153, "i"),
    ("SQL_BOOKMARK_PERSISTENCE", 82, "i"), ("SQL_ASYNC_MODE", 10021, "i"),
    ("SQL_ACCESSIBLE_TABLES", 19, "s"), ("SQL_ACCESSIBLE_PROCEDURES", 20, "s"),
    ("SQL_PROCEDURES", 21, "s"), ("SQL_INTEGRITY", 73, "s"),
    ("SQL_MAX_ROW_SIZE_INCLUDES_LONG", 103, "s"),
    ("SQL_CONVERT_VARCHAR", 70, "i"),
    ("SQL_CONVERT_BIGINT", 53, "i"), ("SQL_CONVERT_DECIMAL", 58, "i"),
    ("SQL_CONVERT_TIMESTAMP", 67, "i"), ("SQL_CONVERT_DATE", 57, "i"),
    ("SQL_CONVERT_INTEGER", 61, "i"), ("SQL_CONVERT_DOUBLE", 59, "i"),
]


def mask(s, secrets):
    for v in secrets:
        if v:
            s = s.replace(v, "***")
    return s


def say(ok, label, detail=""):
    print(("OK   " if ok else "FAIL ") + label + (" " + str(detail) if detail != "" else ""))
    sys.stdout.flush()


def step(label):
    """Decorator: run fn, print one OK/FAIL line; the fn returns the detail."""
    def deco(fn):
        try:
            say(True, label, fn())
        except BaseException as e:  # noqa: BLE001 - one failure must not stop the rest
            if isinstance(e, KeyboardInterrupt):
                raise
            say(False, label, "%s: %s" % (type(e).__name__, e))
        return fn
    return deco


def rows_of(cur, limit=None):
    cols = [d[0] for d in (cur.description or [])]
    rows = cur.fetchall() if limit is None else cur.fetchmany(limit)
    return cols, rows


def show_rows(label, cur, first=10):
    cols, rows = rows_of(cur)
    print("     columns: %s" % cols)
    for r in rows[:first]:
        print("     %s" % (tuple(r),))
    return "%d rows" % len(rows), rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("host")
    ap.add_argument("port")
    ap.add_argument("--user")
    ap.add_argument("--password")
    ap.add_argument("--token")
    ap.add_argument("--catalog")
    ap.add_argument("--schema")
    ap.add_argument("--timeout", type=int, default=30)
    a = ap.parse_args()

    parts = ["driver={%s}" % DRIVER, "host=%s" % a.host, "port=%s" % a.port]
    if a.token:
        parts.append("token=%s" % a.token)
    if a.user is not None:
        parts.append("uid=%s" % a.user)
    if a.password is not None:
        parts.append("pwd=%s" % a.password)
    parts.append("useEncryption=%s" % os.environ.get("PROBE_USE_ENCRYPTION", "false"))
    extra = os.environ.get("PROBE_EXTRA_CONN", "")  # e.g. "useWideChar=true;stringColumnLength=1024"
    if extra:
        parts.append(extra)
    cs = ";".join(parts) + ";"
    print("pyodbc %s, driver manager drivers: %s" % (pyodbc.version, pyodbc.drivers()))
    print("connection string: %s" % mask(cs, [a.password, a.token]))

    state = {}
    try:
        conn = pyodbc.connect(cs, autocommit=True, timeout=a.timeout)
        say(True, "connect", "connected")
    except BaseException as e:  # noqa: BLE001
        say(False, "connect", "%s: %s" % (type(e).__name__, mask(str(e), [a.password, a.token])))
        print("cannot continue without a connection")
        return 2

    # ---- getinfo ----
    for name, ident, kind in INFO:
        label = "conn.getinfo %s(%d)" % (name, ident)

        @step(label)
        def _():
            v = conn.getinfo(ident)
            if kind == "i" and isinstance(v, int):
                return "%r (0x%X)" % (v, v & 0xFFFFFFFF)
            return repr(v)

    # ---- catalog enumeration ----
    @step("tables(catalog='%', schema='', table='')  [list catalogs]")
    def _():
        d, rows = show_rows("catalogs", conn.cursor().tables(catalog="%", schema="", table=""))
        state["catalogs"] = [r[0] for r in rows]
        return d

    @step("tables(schema='%', catalog='', table='')  [list schemas]")
    def _():
        d, rows = show_rows("schemas", conn.cursor().tables(schema="%", catalog="", table=""))
        return d

    @step("tables(tableType='%', catalog='', schema='', table='')  [list table types]")
    def _():
        d, rows = show_rows("types", conn.cursor().tables(tableType="%", catalog="", schema="", table=""))
        return d

    @step("tables() no filter")
    def _():
        d, rows = show_rows("tables", conn.cursor().tables(), first=10)
        state["tables"] = [(r[0], r[1], r[2], r[3]) for r in rows]
        return d

    tbls = []
    if a.catalog or a.schema:
        @step("tables(catalog=%r, schema=%r)" % (a.catalog, a.schema))
        def _():
            kw = {}
            if a.catalog:
                kw["catalog"] = a.catalog
            if a.schema:
                kw["schema"] = a.schema
            d, rows = show_rows("scoped", conn.cursor().tables(**kw), first=20)
            tbls.extend((r[0], r[1], r[2], r[3]) for r in rows)
            return d

    first = None
    pool = state.get("tables", [])
    user_tbls = [t for t in pool if (t[3] or "").upper() in ("TABLE", "VIEW", "BASE TABLE")]
    if a.schema and tbls:
        first = tbls[0]
    elif user_tbls:
        first = user_tbls[0]
    elif pool:
        first = pool[0]
    print("first table used for per-table checks: %s" % (first,))

    def col_args(t, **extra):
        kw = dict(extra)
        if t[0]:
            kw["catalog"] = t[0]
        if t[1]:
            kw["schema"] = t[1]
        return kw

    def probe_columns(t):
        @step("columns(table=%r, schema=%r, catalog=%r)" % (t[2], t[1], t[0]))
        def _():
            cur = conn.cursor().columns(table=t[2], **col_args(t))
            rows = cur.fetchall()
            for r in rows:
                print("     %-30s type_name=%-12s data_type=%-6s column_size=%-8s decimal_digits=%-4s nullable=%s"
                      % (r.column_name, r.type_name, r.data_type, r.column_size, r.decimal_digits, r.nullable))
            return "%d columns" % len(rows)

    if first:
        probe_columns(first)
    if a.schema:
        for t in tbls:
            if t != first:
                probe_columns(t)

    # ---- other catalog calls ----
    @step("getTypeInfo() all types")
    def _():
        cur = conn.cursor().getTypeInfo()
        rows = cur.fetchall()
        for r in rows:
            print("     type_name=%-20s data_type=%s" % (r.type_name, r.data_type))
        return "%d types" % len(rows)

    t = first or (None, None, None, None)
    tn = t[2]
    ca = col_args(t) if first else {}

    def other(label, fn, needs_table=False):
        @step(label)
        def _():
            if needs_table and not first:
                raise RuntimeError("no table found to probe")
            cur = fn()
            return show_rows(label, cur)[0]

    other("primaryKeys(table=%r)" % tn, lambda: conn.cursor().primaryKeys(tn, **ca), needs_table=True)
    other("foreignKeys(table=%r)" % tn, lambda: conn.cursor().foreignKeys(table=tn, **ca), needs_table=True)
    other("foreignKeys(foreignTable=%r)" % tn, lambda: conn.cursor().foreignKeys(foreignTable=tn, **{("foreign" + k.capitalize()): v for k, v in ca.items()}), needs_table=True)
    other("statistics(table=%r)" % tn, lambda: conn.cursor().statistics(tn, **ca), needs_table=True)
    other("rowIdColumns(table=%r)" % tn, lambda: conn.cursor().rowIdColumns(tn, **ca), needs_table=True)
    other("rowVerColumns(table=%r)" % tn, lambda: conn.cursor().rowVerColumns(tn, **ca), needs_table=True)
    other("procedures()", lambda: conn.cursor().procedures())
    other("procedureColumns()", lambda: conn.cursor().procedureColumns())

    # ---- queries ----
    def run(label, sql, *params):
        @step(label)
        def _():
            cur = conn.cursor()
            cur.execute(sql, *params)
            desc = cur.description
            print("     description: %s" % (desc,))
            rows = cur.fetchall()
            for r in rows[:10]:
                print("     values: %s" % [(v, type(v).__name__) for v in r])
            return "%d rows" % len(rows)

    run("SELECT 1", "SELECT 1")
    run("typed query", os.environ.get("PROBE_TYPED_SQL") or DEFAULT_TYPED_SQL)
    run("parameterised SELECT ? + 1", "SELECT ? + 1", 41)
    if first:
        q = ".".join('"%s"' % p for p in (first[0], first[1], first[2]) if p)
        run("SELECT * FROM %s LIMIT 5" % q, "SELECT * FROM %s LIMIT 5" % q)
    else:
        say(False, "SELECT * FROM <first table> LIMIT 5", "no table found")

    try:
        conn.close()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
