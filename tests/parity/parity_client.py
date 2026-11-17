"""SQL clients and result comparison shared by the parity harnesses.

Two front doors into SQE, one result shape:

  TrinoHttp   -- the Trino HTTP protocol (``/v1/statement``), stdlib only. Also
                 the client for the real Trino baseline.
  FlightAdbc  -- Arrow Flight SQL through the ADBC driver
                 (``adbc-driver-flightsql``, the path dbt-sqe uses). Imported
                 lazily, so the HTTP-only harness needs no third-party package.

Every ``run(sql)`` returns a ``Result``: status ``OK`` or ``ERR``, the column
names, the column types as Trino type names, and the rows as JSON-like values
(Trino's own wire encoding: decimals and dates as strings, rows as lists).
``compare`` then canonicalises both sides by declared type and returns one
verdict, so a Flight result and a Trino HTTP result are compared on the same
terms.
"""
import base64
import datetime as _dt
import decimal
import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field


@dataclass
class Result:
    status: str  # "OK" | "ERR"
    columns: list = field(default_factory=list)  # [(name, trino_type)]
    rows: list = field(default_factory=list)
    error: str = ""

    @property
    def types(self):
        return [norm_type(t) for _, t in self.columns]


def _first_line(msg, limit=200):
    msg = (msg or "").strip()
    return (msg.splitlines()[0] if msg else "")[:limit]


# ── Trino HTTP ──────────────────────────────────────────────────────────


class TrinoHttp:
    """Trino HTTP client. ``password=None`` sends no Authorization header
    (the unauthenticated Trino baseline)."""

    name = "http"

    def __init__(self, url, user, password, catalog, schema, timeout=300):
        self.url = url
        self.timeout = timeout
        self.headers = {
            "X-Trino-User": user,
            "X-Trino-Catalog": catalog,
            "X-Trino-Schema": schema,
            "Content-Type": "text/plain",
        }
        if password is not None:
            tok = base64.b64encode(f"{user}:{password}".encode()).decode()
            self.headers["Authorization"] = "Basic " + tok

    def _req(self, url, data=None):
        req = urllib.request.Request(url, data=data.encode() if data else None)
        for k, v in self.headers.items():
            req.add_header(k, v)
        # nosemgrep: opt.aikido.semgrep-rules.python.lang.security.audit.dynamic-urllib-use-detected -- URL is the local test stack endpoint passed on the command line
        return json.load(urllib.request.urlopen(req, timeout=self.timeout))

    def run(self, sql):
        try:
            d = self._req(self.url, sql)
            rows, columns = [], []
            while True:
                if d.get("error"):
                    return Result("ERR", error=_first_line(d["error"].get("message")))
                if d.get("columns") and not columns:
                    columns = [(c["name"], c["type"]) for c in d["columns"]]
                rows.extend(d.get("data") or [])
                nxt = d.get("nextUri")
                if not nxt:
                    return Result("OK", columns, rows)
                d = self._req(nxt)
        except (urllib.error.URLError, OSError, ValueError) as e:
            return Result("ERR", error=_first_line(f"transport: {e}"))

    def close(self):
        pass


# ── Flight SQL (ADBC) ───────────────────────────────────────────────────


class FlightAdbc:
    """Flight SQL client over ADBC. Signs in with a basic-auth handshake (the
    server answers with a bearer token the driver then sends), then scopes the
    session with ``USE catalog.schema``: SQE keys the Flight session on the
    bearer token, the same mechanism ``sqe-bench``'s Flight client uses."""

    name = "flight"

    def __init__(self, uri, user, password, catalog, schema):
        try:
            import adbc_driver_flightsql.dbapi as flight_sql
        except ImportError as e:  # Flight mode was asked for: never skip.
            raise SystemExit(
                "Flight mode needs adbc-driver-flightsql and pyarrow "
                "(pip install -r tests/parity/requirements.txt)"
            ) from e
        self._conn = flight_sql.connect(
            uri, db_kwargs={"username": user, "password": password}, autocommit=True
        )
        if catalog and schema:
            use = self.run(f"USE {catalog}.{schema}")
            if use.status != "OK":
                raise SystemExit(f"USE {catalog}.{schema} over Flight failed: {use.error}")

    def run(self, sql):
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            table = cur.fetch_arrow_table()
        except Exception as e:  # driver raises its own DB-API error classes
            return Result("ERR", error=_first_line(str(e)))
        finally:
            cur.close()
        columns = [(f.name, arrow_to_trino_type(f.type)) for f in table.schema]
        cols = [table.column(i).to_pylist() for i in range(table.num_columns)]
        rows = [
            [to_wire(cols[c][r], table.schema.field(c).type) for c in range(len(cols))]
            for r in range(table.num_rows)
        ]
        return Result("OK", columns, rows)

    def close(self):
        self._conn.close()


_UNIT_DIGITS = {"s": 0, "ms": 3, "us": 6, "ns": 9}


def arrow_to_trino_type(t):
    """The Trino type name a driver reading this Arrow type would report."""
    import pyarrow as pa
    import pyarrow.types as pt

    if pt.is_boolean(t):
        return "boolean"
    for check, name in (
        (pt.is_int8, "tinyint"), (pt.is_int16, "smallint"),
        (pt.is_int32, "integer"), (pt.is_int64, "bigint"),
        (pt.is_float32, "real"), (pt.is_float64, "double"),
        (pt.is_date32, "date"), (pt.is_date64, "date"),
    ):
        if check(t):
            return name
    if pt.is_decimal(t):
        return f"decimal({t.precision},{t.scale})"
    if pt.is_string(t) or pt.is_large_string(t) or str(t) == "string_view":
        return "varchar"
    if pt.is_binary(t) or pt.is_large_binary(t) or pt.is_fixed_size_binary(t) or str(t) == "binary_view":
        return "varbinary"
    if pt.is_timestamp(t):
        p = _UNIT_DIGITS[t.unit]
        return f"timestamp({p}) with time zone" if t.tz else f"timestamp({p})"
    if pt.is_time(t):
        return f"time({_UNIT_DIGITS[t.unit]})"
    if pt.is_list(t) or pt.is_large_list(t) or pt.is_fixed_size_list(t):
        return f"array({arrow_to_trino_type(t.value_type)})"
    if pt.is_map(t):
        return f"map({arrow_to_trino_type(t.key_type)},{arrow_to_trino_type(t.item_type)})"
    if pt.is_struct(t):
        inner = ",".join(f"{f.name} {arrow_to_trino_type(f.type)}" for f in t)
        return f"row({inner})"
    if pt.is_null(t):
        return "unknown"
    if isinstance(t, pa.ExtensionType):
        return arrow_to_trino_type(t.storage_type)
    return f"arrow:{t}"


def to_wire(v, t):
    """Encode a pyarrow Python value the way Trino's HTTP protocol would."""
    import pyarrow.types as pt

    if v is None:
        return None
    if pt.is_decimal(t):
        return format(v, "f")
    if pt.is_timestamp(t):
        return _fmt_ts(v)
    if isinstance(v, (_dt.date, _dt.time)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return base64.b64encode(bytes(v)).decode()
    if pt.is_map(t):
        return {to_wire(k, t.key_type): to_wire(x, t.item_type) for k, x in v}
    if pt.is_list(t) or pt.is_large_list(t) or pt.is_fixed_size_list(t):
        return [to_wire(x, t.value_type) for x in v]
    if pt.is_struct(t):
        return [to_wire(v[f.name], f.type) for f in t]
    return v


def _fmt_ts(v):
    s = v.strftime("%Y-%m-%d %H:%M:%S")
    if v.microsecond:
        s += f".{v.microsecond:06d}"
    if v.tzinfo is not None:
        s += " UTC" if v.utcoffset() == _dt.timedelta(0) else v.strftime(" %z")
    return s


# ── Comparison ──────────────────────────────────────────────────────────

_VARCHAR_LEN = re.compile(r"\bvarchar\(\d+\)")
_TS = re.compile(r"^(\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d)(\.\d+)?(.*)$")


def norm_type(t):
    """Trino type name, lowercased and without spaces after commas. A bounded
    varchar(n) counts as varchar: Iceberg has no length, and Trino types a
    literal 'abc' as varchar(3)."""
    t = (t or "").lower().replace(", ", ",")
    return _VARCHAR_LEN.sub("varchar", t)


def canon(v, t, sig_digits=9):
    """Canonical form of a wire value of declared Trino type ``t``."""
    if v is None:
        return None
    t = norm_type(t)
    if t.startswith("decimal"):
        try:
            return ("dec", decimal.Decimal(str(v)).normalize())
        except decimal.InvalidOperation:
            return ("str", str(v))
    if t in ("double", "real"):
        if isinstance(v, str):  # Trino encodes NaN/Infinity as strings
            return ("f", v.lower())
        f = float(v)
        return ("f", "nan") if f != f else ("f", float(f"{f:.{sig_digits}g}"))
    if t.startswith(("timestamp", "time")) and isinstance(v, str):
        m = _TS.match(v)
        if m:
            frac = (m.group(2) or ".").rstrip("0").rstrip(".")
            return ("ts", m.group(1).replace("T", " ") + frac + m.group(3).strip().replace("Z", "UTC"))
        return ("str", v)
    if t.startswith("array(") and isinstance(v, list):
        inner = t[len("array("):-1]
        return ("arr", tuple(canon(x, inner, sig_digits) for x in v))
    if t.startswith("map(") and isinstance(v, dict):
        return ("map", tuple(sorted((json.dumps(k, default=str), json.dumps(canon(x, "", sig_digits), default=str)) for k, x in v.items())))
    if isinstance(v, bool):
        return ("b", v)
    if isinstance(v, int):
        return ("i", v)
    if isinstance(v, float):
        return ("f", float(f"{v:.{sig_digits}g}"))
    if isinstance(v, list):
        return ("list", tuple(canon(x, "", sig_digits) for x in v))
    if isinstance(v, dict):
        return ("dict", tuple(sorted((str(k), json.dumps(canon(x, "", sig_digits), default=str)) for k, x in v.items())))
    return ("str", str(v))


def canon_rows(res, types=None):
    types = types or res.types
    return [tuple(canon(v, types[i] if i < len(types) else "") for i, v in enumerate(r)) for r in res.rows]


_ORDER_BY = re.compile(r"\border\s+by\b", re.I)


def has_top_level_order_by(sql):
    """True when the statement's last ORDER BY sits outside any parentheses
    (a window's or a subquery's ORDER BY does not order the result)."""
    depth, last = 0, None
    for i, ch in enumerate(sql):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and sql[i:i + 5].lower() == "order" and _ORDER_BY.match(sql, i):
            last = i
    return last is not None


def _sort_key(row):
    return json.dumps(row, default=str)


def compare(sql, sqe, trino):
    """Verdict for one statement. Verdicts, strictest first:

      MATCH       same values, same column types (and order when ORDER BY)
      ORDER       same rows, different order under a top-level ORDER BY
      TYPE        same values, different column types
      DIFF        different values
      SQE-ERR     SQE rejects, Trino answers
      TRINO-ERR   Trino rejects, SQE answers (the test or SQE is too lenient)
      BOTH-ERR    both reject (kept apart: confirm each is an intended negative)
    """
    if sqe.status == "ERR" and trino.status == "ERR":
        return "BOTH-ERR", f"SQE: {sqe.error} | Trino: {trino.error}"
    if sqe.status == "ERR":
        return "SQE-ERR", sqe.error
    if trino.status == "ERR":
        return "TRINO-ERR", trino.error
    # Canonicalise both sides by Trino's declared types, so a decimal that
    # SQE types differently still compares by value (TYPE is reported apart).
    tt = trino.types
    s_rows, t_rows = canon_rows(sqe, tt), canon_rows(trino, tt)
    ordered = has_top_level_order_by(sql)
    same_sorted = sorted(s_rows, key=_sort_key) == sorted(t_rows, key=_sort_key)
    if not same_sorted:
        return "DIFF", f"SQE={len(s_rows)}r Trino={len(t_rows)}r | SQE {sqe.rows[:2]} vs Trino {trino.rows[:2]}"
    if ordered and s_rows != t_rows:
        return "ORDER", f"{len(s_rows)} rows, order differs"
    if sqe.types != tt:
        diffs = [f"{n}: {s} vs {t}" for (n, _), s, t in zip(trino.columns, sqe.types, tt) if s != t]
        if len(sqe.types) != len(tt):
            diffs.append(f"{len(sqe.types)} vs {len(tt)} columns")
        return "TYPE", "; ".join(diffs)
    return "MATCH", f"{len(s_rows)} rows"


OK_VERDICTS = ("MATCH",)
