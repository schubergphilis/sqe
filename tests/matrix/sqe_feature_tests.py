#!/usr/bin/env python3
"""Black-box Iceberg feature-matrix suite for SQE.

Measures SQE only through its client protocol (Trino HTTP), the Iceberg REST
catalog and object storage. Nothing here imports SQE code or reads SQE logs.

How it works
  * The cell list comes from the pinned rubric (docs/evidence/iceberg-matrix-rubric.json):
    one cell per feature and applicable format version. A rubric cell with no
    registered test is reported `untested` and fails the run; a registered test
    for a cell that is not in the rubric is a startup error.
  * Each test drives SQE with SQL, then looks at what is really on storage
    through PyIceberg against the same catalog (the second witness). SQE reading
    back its own write never counts on its own.
  * Verdict mapping is exact: pass without partial_reason = full, pass with
    partial_reason = partial, fail = none, skip = unmeasured. A cell agrees with
    the state file only when measured == claimed.
  * The state file is read, never written. Discrepancies are listed as proposed
    edits for a maintainer.

Usage
  source tests/matrix/docker/env.sh
  python tests/matrix/sqe_feature_tests.py [--out DIR] [--only FEATURE[:vN]]...
"""
import argparse
import base64
import datetime as dt
import json
import os
import sys
import time
import traceback
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
RUBRIC_PATH = REPO / "docs" / "evidence" / "iceberg-matrix-rubric.json"
STATE_PATH = REPO / "docs" / "evidence" / "iceberg-matrix-state.json"
WEIGHT = {"full": 3, "partial": 2, "none": 0}
VERSIONS = ("v2", "v3")


# ── rubric and claims ──────────────────────────────────────────────────────


def vnum(v):
    return int(v.lstrip("v"))


def load_rubric(path=RUBRIC_PATH):
    """Return (cells, features): cells is an ordered list of (feature_id, 'v2'|'v3')."""
    rubric = json.loads(Path(path).read_text())
    cells = []
    for f in rubric["features"]:
        for ver in VERSIONS:
            if vnum(ver) < vnum(f["introducedIn"]):
                continue
            if f.get("removedIn") and vnum(ver) >= vnum(f["removedIn"]):
                continue
            cells.append((f["id"], ver))
    expected = rubric.get("cells")
    if expected is not None and expected != len(cells):
        raise SystemExit(f"rubric says {expected} cells, computed {len(cells)}")
    return cells, {f["id"]: f for f in rubric["features"]}


def load_claims(path=STATE_PATH):
    """Claimed level per cell from the state file (read only). Missing = none."""
    state = json.loads(Path(path).read_text())
    return {
        k.split(":", 1)[1]: v for k, v in state["support"].items() if k.startswith("sqe:")
    }, state


# ── results and registry ───────────────────────────────────────────────────


class CellFail(Exception):
    """The engine did not do what the cell requires (verdict: fail / none)."""


class CellSkip(Exception):
    """The cell cannot be measured in this environment (verdict: unmeasured)."""


@dataclass
class TestResult:
    status: str  # pass | fail | skip | error
    partial_reason: str = ""
    details: list = field(default_factory=list)
    message: str = ""
    seconds: float = 0.0
    # Features that work over one SQE protocol but not another. Reported on
    # their own; they do not lower the level (the level is the engine's
    # capability over any of its client protocols).
    protocol_gaps: list = field(default_factory=list)

    @property
    def measured_level(self):
        """Exact mapping. None when the cell is unmeasured (skip or error)."""
        if self.status == "pass":
            return "partial" if self.partial_reason else "full"
        if self.status == "fail":
            return "none"
        return None


REGISTRY = {}  # (feature, version) -> function


def cell(feature, version):
    def deco(fn):
        key = (feature, version)
        if key in REGISTRY:
            raise SystemExit(f"two tests registered for {feature}:{version}")
        REGISTRY[key] = fn
        return fn

    return deco


# ── SQE client (Trino HTTP) ────────────────────────────────────────────────


class SqlError(CellFail):
    pass


class Sqe:
    def __init__(self, host, port, user, password, catalog, timeout=300):
        self.url = f"http://{host}:{port}/v1/statement"
        self.catalog = catalog
        self.timeout = timeout
        tok = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {
            "X-Trino-User": user,
            "X-Trino-Catalog": catalog,
            "Authorization": "Basic " + tok,
            "Content-Type": "text/plain",
        }

    def sql(self, statement):
        """Run one statement, return the rows. A rejected statement is a CellFail."""
        try:
            d = requests.post(
                self.url, data=statement.encode(), headers=self.headers, timeout=self.timeout
            ).json()
            rows = []
            while True:
                if d.get("error"):
                    raise SqlError(f"{statement!r} -> {d['error'].get('message', d['error'])}")
                rows.extend(d.get("data") or [])
                nxt = d.get("nextUri")
                if not nxt:
                    return rows
                d = requests.get(nxt, headers=self.headers, timeout=self.timeout).json()
        except requests.RequestException as e:
            raise RuntimeError(f"SQE transport error: {e}") from e


class Flight:
    """Flight SQL client (ADBC). Needed where the Trino HTTP front door has no
    session or does not parse the clause (SET WRITE_BRANCH, FOR VERSION AS OF)."""

    def __init__(self, host, port, user, password):
        import adbc_driver_flightsql.dbapi as flight_sql

        self.conn = flight_sql.connect(
            f"grpc://{host}:{port}", db_kwargs={"username": user, "password": password},
            autocommit=True)

    def sql(self, statement):
        cur = self.conn.cursor()
        try:
            cur.execute(statement)
            t = cur.fetch_arrow_table()
        except Exception as e:  # noqa: BLE001 - driver raises its own error classes
            raise SqlError(f"[flight] {statement!r} -> {str(e).splitlines()[0][:300]}") from e
        finally:
            cur.close()
        cols = [t.column(i).to_pylist() for i in range(t.num_columns)]
        return [[c[r] for c in cols] for r in range(t.num_rows)]

    def close(self):
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001
            pass


# ── PyIceberg witness ──────────────────────────────────────────────────────


class Witness:
    """Reads tables through PyIceberg against the same Polaris, bypassing SQE."""

    def __init__(self):
        from pyiceberg.catalog import load_catalog

        props = {
            "type": "rest",
            "uri": env("ICEBERG_REST_URI", "http://127.0.0.1:48181/api/catalog"),
            "warehouse": env("ICEBERG_REST_WAREHOUSE", "demo"),
            "credential": env("ICEBERG_REST_CREDENTIAL", "root:s3cr3t"),
            "scope": env("ICEBERG_REST_SCOPE", "PRINCIPAL_ROLE:ALL"),
        }
        realm = os.environ.get("ICEBERG_REST_REALM")
        if realm:
            props["header.Polaris-Realm"] = realm
        self.catalog = load_catalog("sqe-matrix", **props)
        # The catalog advertises one storage endpoint to every client. SQE (in
        # the compose network) needs http://rustfs:9000; this host-side reader
        # swaps in its own address after the table metadata arrives.
        self.io_props = {
            "s3.access-key-id": env("ICEBERG_S3_KEY_ID", "rustfs"),
            "s3.secret-access-key": env("ICEBERG_S3_SECRET", "rustfs12345"),
            "s3.region": env("ICEBERG_S3_REGION", "us-east-1"),
            "s3.path-style-access": "true",
        }
        # Same default as docker/env.sh; set ICEBERG_S3_ENDPOINT= (empty) to keep
        # the catalog's own endpoint, e.g. against upstream's start-polaris.sh.
        endpoint = os.environ.get(
            "ICEBERG_S3_ENDPOINT", f"http://127.0.0.1:{os.environ.get('RUSTFS_PORT', '49000')}")
        if endpoint:
            self.io_props["s3.endpoint"] = endpoint

    def _token(self):
        uri = env("ICEBERG_REST_URI", "http://127.0.0.1:48181/api/catalog")
        cid, _, secret = env("ICEBERG_REST_CREDENTIAL", "root:s3cr3t").partition(":")
        headers = {}
        if os.environ.get("ICEBERG_REST_REALM"):
            headers["Polaris-Realm"] = os.environ["ICEBERG_REST_REALM"]
        r = requests.post(f"{uri}/v1/oauth/tokens", headers=headers, timeout=30, data={
            "grant_type": "client_credentials", "client_id": cid, "client_secret": secret,
            "scope": env("ICEBERG_REST_SCOPE", "PRINCIPAL_ROLE:ALL")})
        r.raise_for_status()
        return r.json()["access_token"], headers

    def rest_get(self, path):
        """GET a catalog path (after /v1/{prefix}) and return the raw JSON."""
        uri = env("ICEBERG_REST_URI", "http://127.0.0.1:48181/api/catalog")
        token, headers = self._token()
        headers = dict(headers, Authorization=f"Bearer {token}")
        prefix = self.catalog.properties.get("prefix") or env("ICEBERG_REST_WAREHOUSE", "demo")
        r = requests.get(f"{uri}/v1/{prefix}{path}", headers=headers, timeout=60)
        if r.status_code >= 400:
            raise CellFail(f"catalog GET {path} -> {r.status_code} {r.text[:300]}")
        return r.json()

    def rest_call(self, method, path, body=None):
        """Raw catalog call (after /v1/{prefix}); returns (status, JSON or text)."""
        uri = env("ICEBERG_REST_URI", "http://127.0.0.1:48181/api/catalog")
        token, headers = self._token()
        headers = dict(headers, Authorization=f"Bearer {token}")
        prefix = self.catalog.properties.get("prefix") or env("ICEBERG_REST_WAREHOUSE", "demo")
        r = requests.request(method, f"{uri}/v1/{prefix}{path}", headers=headers, json=body, timeout=60)
        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, r.text

    def upgrade_format_version(self, namespace, name, version):
        """Metadata-only upgrade through the catalog's commit endpoint. PyIceberg 0.12
        refuses format-version 3, so the update is sent as raw REST."""
        code, body = self.rest_call("POST", f"/namespaces/{namespace}/tables/{name}", {
            "requirements": [],
            "updates": [{"action": "upgrade-format-version", "format-version": version}]})
        if code >= 400:
            raise CellFail(f"catalog refused upgrade-format-version {version}: {code} {str(body)[:300]}")

    def load_raw(self, namespace, name):
        return RawTable(self, namespace, name)

    def load(self, namespace, name):
        from pyiceberg.io import load_file_io

        t = self.catalog.load_table((namespace, name))
        if self.io_props.get("s3.endpoint"):
            t.io = load_file_io(dict(self.io_props), t.metadata_location)
        return t

    def snapshot_ops(self, table):
        return [
            (s.snapshot_id, s.summary.operation.value if s.summary else None,
             dict(s.summary.additional_properties) if s.summary else {})
            for s in table.snapshots()
        ]

    def delete_entries(self, table):
        """Live data and delete files of the current snapshot, read from the manifests.

        The manifests are read as raw Avro (fastavro) with the schema stored in the
        file, not through PyIceberg's typed reader: PyIceberg 0.12 drops the v3
        fields referenced_data_file, content_offset and content_size_in_bytes, which
        are exactly what identifies a deletion vector.
        """
        from fastavro import reader
        from pyiceberg.manifest import ManifestContent

        snap = table.current_snapshot()
        found = {"delete_manifests": 0, "data_manifests": 0, "files": [], "live_data_files": 0}
        if snap is None:
            return found
        for m in snap.manifests(table.io):
            if m.content == ManifestContent.DELETES:
                found["delete_manifests"] += 1
            else:
                found["data_manifests"] += 1
            with table.io.new_input(m.manifest_path).open() as f:
                for e in reader(f):
                    if e["status"] == 2:  # DELETED entry, no longer live
                        continue
                    df = e["data_file"]
                    if df["content"] == 0:
                        found["live_data_files"] += 1
                        continue
                    found["files"].append({
                        "content": {1: "POSITION_DELETES", 2: "EQUALITY_DELETES"}[df["content"]],
                        "file_format": df["file_format"],
                        "records": df["record_count"],
                        "referenced_data_file": df.get("referenced_data_file"),
                        "content_offset": df.get("content_offset"),
                        "content_size_in_bytes": df.get("content_size_in_bytes"),
                        "equality_ids": df.get("equality_ids"),
                        "path": df["file_path"],
                    })
        return found

    def live_files(self, table, snapshot=None):
        """Every live manifest entry of a snapshot (default: current), as raw Avro.

        Each item: content (0 data, 1 position deletes, 2 equality deletes), path,
        partition (the raw partition tuple), spec_id (from the manifest list), seq
        (data sequence number, inherited from the manifest when the entry has none),
        records, equality_ids.
        """
        from fastavro import reader

        snap = snapshot or table.current_snapshot()
        out = []
        if snap is None:
            return out
        for m in snap.manifests(table.io):
            with table.io.new_input(m.manifest_path).open() as f:
                for e in reader(f):
                    if e["status"] == 2:
                        continue
                    df = e["data_file"]
                    seq = e.get("sequence_number")
                    out.append({
                        "content": df["content"], "path": df["file_path"],
                        "partition": dict(df.get("partition") or {}),
                        "spec_id": m.partition_spec_id,
                        "seq": m.sequence_number if seq is None else seq,
                        "records": df["record_count"],
                        "equality_ids": df.get("equality_ids"),
                    })
        return out

    def raw_metadata(self, table):
        """The table metadata JSON exactly as stored (PyIceberg cannot re-serialise v3)."""
        import gzip

        with table.io.new_input(table.metadata_location).open() as f:
            data = f.read()
        if data[:2] == b"\x1f\x8b":
            data = gzip.decompress(data)
        return json.loads(data)

    def read_parquet(self, table, path):
        import pyarrow.parquet as pq

        with table.io.new_input(path).open() as f:
            return pq.read_table(f)

    def data_rows(self, table, columns):
        """Rows of the live data files read straight from Parquet (no engine, no
        delete application). Valid for tables that carry no delete files."""
        rows = []
        for e in self.live_files(table):
            if e["content"] == 0:
                t = self.read_parquet(table, e["path"])
                have = [c for c in columns if c in t.column_names]
                cols = {c: t.column(c).to_pylist() for c in have}
                for i in range(t.num_rows):
                    rows.append({c: cols[c][i] if c in cols else None for c in columns})
        return rows

    def logical_rows(self, table, columns):
        """Rows after applying equality deletes by key, sequence-number aware
        (a delete applies to data files with a lower data sequence number).
        Written for the equality-delete cells because PyIceberg cannot apply them."""
        fields = {f.field_id: f.name for f in table.schema().fields}
        files = self.live_files(table)
        deletes = []
        for e in files:
            if e["content"] == 2:
                names = [fields[i] for i in e["equality_ids"]]
                t = self.read_parquet(table, e["path"])
                keys = set(zip(*[t.column(n).to_pylist() for n in names]))
                deletes.append((e["seq"], names, keys))
            elif e["content"] == 1:
                raise CellFail("logical_rows does not model position deletes")
        rows = []
        for e in files:
            if e["content"] != 0:
                continue
            t = self.read_parquet(table, e["path"])
            cols = {c: t.column(c).to_pylist() for c in t.column_names}
            for i in range(t.num_rows):
                row = {c: cols[c][i] if c in cols else None for c in columns}
                dead = any(e["seq"] < seq and tuple(cols[n][i] for n in names) in keys
                           for seq, names, keys in deletes)
                if not dead:
                    rows.append(row)
        return rows

    def puffin_blob_types(self, table, path):
        """Blob types named in a Puffin file's footer (Puffin: magic, blobs, magic,
        footer JSON, payload size, flags, magic)."""
        with table.io.new_input(path).open() as f:
            data = f.read()
        if data[:4] != b"PFA1" or data[-4:] != b"PFA1":
            raise CellFail(f"{path} is not a Puffin file (magic bytes)")
        flags = int.from_bytes(data[-8:-4], "little")
        size = int.from_bytes(data[-12:-8], "little")
        if flags & 1:
            return ["(footer compressed, not decoded)"]
        footer = json.loads(data[-12 - size:-12])
        return [b.get("type") for b in footer.get("blobs", [])]


class RawTable:
    """A table read without PyIceberg's schema model.

    PyIceberg 0.12 refuses to load a table whose schema holds a v3 type it does
    not know (variant, geometry, unknown). This reads the catalog's loadTable JSON
    as the REST wire carries it, then the manifest list and manifests as raw Avro
    and data files as Parquet. Nothing here is SQE.
    """

    def __init__(self, witness, namespace, name):
        self.w, self.namespace, self.name = witness, namespace, name
        d = witness.rest_get(f"/namespaces/{namespace}/tables/{name}")
        self.load_json = d
        self.metadata = d["metadata"]
        self.metadata_location = d["metadata-location"]
        from pyiceberg.io import load_file_io

        self.io = load_file_io(dict(witness.io_props), self.metadata_location)

    @property
    def format_version(self):
        return self.metadata["format-version"]

    def current_schema(self):
        return next(s for s in self.metadata["schemas"]
                    if s["schema-id"] == self.metadata["current-schema-id"])

    def snapshots(self):
        return self.metadata.get("snapshots") or []

    def current_snapshot(self):
        sid = self.metadata.get("current-snapshot-id")
        return next((s for s in self.snapshots() if s["snapshot-id"] == sid), None)

    def manifest_list(self, snapshot=None):
        from fastavro import reader

        snap = snapshot or self.current_snapshot()
        if snap is None:
            return []
        with self.io.new_input(snap["manifest-list"]).open() as f:
            return list(reader(f))

    def live_files(self, snapshot=None):
        """Live manifest entries of a snapshot as raw Avro records, with the manifest
        list entry attached as `_manifest`."""
        from fastavro import reader

        out = []
        for m in self.manifest_list(snapshot):
            with self.io.new_input(m["manifest_path"]).open() as f:
                for e in reader(f):
                    if e["status"] == 2:
                        continue
                    e = dict(e)
                    e["_manifest"] = m
                    out.append(e)
        return out

    def data_files(self, snapshot=None):
        return [e for e in self.live_files(snapshot) if e["data_file"]["content"] == 0]

    def read_parquet(self, path):
        import pyarrow.parquet as pq

        with self.io.new_input(path).open() as f:
            return pq.read_table(f)

    def parquet_schema(self, path):
        import pyarrow.parquet as pq

        with self.io.new_input(path).open() as f:
            return pq.ParquetFile(f).schema_arrow

    def parquet_meta(self, path):
        import pyarrow.parquet as pq

        with self.io.new_input(path).open() as f:
            return pq.ParquetFile(f).metadata


def assert_raw_format_version(raw, n):
    if raw.format_version != n:
        raise CellFail(f"catalog reports format-version {raw.format_version}, the cell needs {n}")
    return f"format-version={raw.format_version} (catalog loadTable JSON)"


def assert_format_version(table, n):
    """format-version comes from the catalog's loadTable metadata, so a v3 cell can
    never pass on a v2 table."""
    got = table.metadata.format_version
    if got != n:
        raise CellFail(f"catalog reports format-version {got}, the cell needs {n}")
    return f"format-version={got} (catalog loadTable)"


# ── test context ───────────────────────────────────────────────────────────


def env(name, default):
    return os.environ.get(name) or default


class Ctx:
    def __init__(self, sqe, witness, feature, version):
        self.sqe, self.witness = sqe, witness
        self.catalog = sqe.catalog
        self.ns = f"matrix_{feature}_{version}_{uuid.uuid4().hex[:8]}".replace("-", "_")
        self.tables = []
        self.details = []
        self.partial_reason = ""
        self.protocol_gaps = []
        self._flight = None

    def note(self, msg):
        self.details.append(msg)

    def flight(self):
        """One Flight SQL session per cell (session state such as WRITE_BRANCH)."""
        if self._flight is None:
            self._flight = Flight(env("SQE_HOST", "127.0.0.1"), env("SQE_FLIGHT_PORT", "48051"),
                                  env("SQE_USER", "root"), env("SQE_PASSWORD", "s3cr3t"))
        return self._flight

    def trino_error(self, statement):
        """Run a statement over Trino HTTP; return the error text, or None if it ran."""
        try:
            self.sqe.sql(statement)
            return None
        except SqlError as e:
            return str(e)

    def sql(self, statement):
        return self.sqe.sql(statement)

    def new_table(self, name):
        self.tables.append(name)
        return f"{self.catalog}.{self.ns}.{name}"

    def add_partial(self, reason):
        """Accumulate partial reasons (a cell can fall short in more than one way)."""
        self.partial_reason = f"{self.partial_reason}; {reason}" if self.partial_reason else reason

    def add_protocol_gap(self, gap):
        """The feature works over another SQE protocol but not this one. Not a
        partial: reported separately as an SQE bug to file."""
        self.protocol_gaps.append(gap)

    def setup(self):
        self.sql(f"CREATE SCHEMA {self.catalog}.{self.ns}")

    def cleanup(self):
        if self._flight is not None:
            self._flight.close()
        for t in self.tables:
            try:
                self.sql(f"DROP TABLE IF EXISTS {self.catalog}.{self.ns}.{t}")
            except Exception:  # noqa: BLE001 - best effort, the namespace drop reports leftovers
                pass
        try:
            self.sql(f"DROP SCHEMA IF EXISTS {self.catalog}.{self.ns}")
        except Exception:  # noqa: BLE001
            pass

    def load(self, name):
        return self.witness.load(self.ns, name)

    def load_raw(self, name):
        return self.witness.load_raw(self.ns, name)


def run_cell(sqe, witness, feature, version, fn):
    ctx = Ctx(sqe, witness, feature, version)
    t0 = time.time()
    try:
        ctx.setup()
        fn(ctx)
        res = TestResult("pass", ctx.partial_reason, ctx.details)
    except CellFail as e:
        res = TestResult("fail", "", ctx.details, str(e))
    except CellSkip as e:
        res = TestResult("skip", "", ctx.details, str(e))
    except Exception as e:  # noqa: BLE001 - harness/infrastructure problem, not a verdict
        res = TestResult("error", "", ctx.details, f"{type(e).__name__}: {e}")
        res.details.append(traceback.format_exc(limit=6))
    finally:
        ctx.cleanup()
    res.protocol_gaps = list(ctx.protocol_gaps)
    res.seconds = round(time.time() - t0, 1)
    return res


# ── cells ──────────────────────────────────────────────────────────────────
# SQL is ported from the Rust tests named in the state file's evidence
# (ctas_write_modes_e2e.rs, v3_e2e.rs, integration_test.rs::test_ctas_roundtrip).


def rows_of(table):
    return table.scan().to_arrow().to_pylist()


def table_creation_cell(ctx, n):
    t = ctx.new_table("created")
    ctx.sql(
        f"CREATE TABLE {t} (id BIGINT, name VARCHAR, price DOUBLE, ts TIMESTAMP) "
        f"TBLPROPERTIES ('format-version' = '{n}')"
    )
    ctx.sql(f"INSERT INTO {t} VALUES (1, 'a', 1.5, TIMESTAMP '2026-01-01 00:00:00'), "
            f"(2, 'b', 2.5, TIMESTAMP '2026-01-02 00:00:00')")
    tbl = ctx.load("created")
    ctx.note(assert_format_version(tbl, n))
    fields = {f.name: str(f.field_type) for f in tbl.schema().fields}
    ctx.note(f"schema seen by PyIceberg: {fields}")
    for want, kinds in {"id": ("long",), "name": ("string",), "price": ("double",),
                        "ts": ("timestamp", "timestamptz")}.items():
        if fields.get(want) not in kinds:
            raise CellFail(f"column {want}: PyIceberg sees {fields.get(want)}, expected one of {kinds}")
    got = sorted(r["id"] for r in rows_of(tbl))
    ctx.note(f"PyIceberg scan after SQE INSERT: ids {got}")
    if got != [1, 2]:
        raise CellFail(f"PyIceberg reads ids {got}, expected [1, 2]")

    # CTAS is the second creation path.
    c = ctx.new_table("ctas")
    ctx.sql(f"CREATE TABLE {c} TBLPROPERTIES ('format-version' = '{n}') "
            f"AS SELECT 1 AS id, 10 AS v")
    ctbl = ctx.load("ctas")
    ctx.note("CTAS: " + assert_format_version(ctbl, n))
    got = [(r["id"], r["v"]) for r in rows_of(ctbl)]
    if got != [(1, 10)]:
        raise CellFail(f"CTAS table read by PyIceberg: {got}")
    ctx.note("CTAS table read by PyIceberg: [(1, 10)]")

    # The description also names partitioning and table properties.
    p = ctx.new_table("created_props")
    ctx.sql(f"CREATE TABLE {p} (id BIGINT, region VARCHAR, ts TIMESTAMP) "
            f"PARTITIONED BY (region, day(ts)) "
            f"TBLPROPERTIES ('format-version' = '{n}', 'matrix.suite.marker' = 'v1', "
            f"'write.parquet.compression-codec' = 'zstd')")
    ptbl = ctx.load("created_props")
    ctx.note("partitioned: " + assert_format_version(ptbl, n))
    spec = [(f.name, str(f.transform)) for f in ptbl.spec().fields]
    ctx.note(f"partition spec in the catalog: {spec}")
    transforms = sorted(tr for _, tr in spec)
    if len(spec) != 2 or "identity" not in transforms or "day" not in transforms:
        raise CellFail(f"partition spec {spec}, expected identity(region) and day(ts)")
    props = dict(ptbl.properties)
    want = {"matrix.suite.marker": "v1", "write.parquet.compression-codec": "zstd"}
    seen = {k: props.get(k) for k in want}
    ctx.note(f"table properties in the catalog: {seen}")
    if seen != want:
        raise CellFail(f"table properties {seen}, expected {want}")


@cell("table-creation", "v2")
def table_creation_v2(ctx):
    table_creation_cell(ctx, 2)


@cell("table-creation", "v3")
def table_creation_v3(ctx):
    table_creation_cell(ctx, 3)


def seed_three_files(ctx, name, props):
    t = ctx.new_table(name)
    ctx.sql(f"CREATE TABLE {t} (id BIGINT, v BIGINT) TBLPROPERTIES ({props})")
    for i in (1, 2, 3):
        ctx.sql(f"INSERT INTO {t} VALUES ({i}, {i * 10})")
    return t


def delete_witness(ctx, name, version):
    """Shared MoR DELETE check; returns (table, delete-entry report)."""
    t = f"{ctx.catalog}.{ctx.ns}.{name}"
    tbl = ctx.load(name)
    ctx.note(assert_format_version(tbl, version))
    before = ctx.witness.delete_entries(tbl)
    ctx.note(f"before DELETE: {before['live_data_files']} live data files, "
             f"{before['delete_manifests']} delete manifests")
    if before["live_data_files"] != 3:
        raise CellFail(f"expected 3 live data files before the DELETE, PyIceberg sees "
                       f"{before['live_data_files']}")
    ctx.sql(f"DELETE FROM {t} WHERE id = 1")
    tbl = ctx.load(name)
    ctx.note(assert_format_version(tbl, version))
    ops = ctx.witness.snapshot_ops(tbl)
    last_id, last_op, last_summary = ops[-1]
    ctx.note(f"snapshots (id, operation): {[(i, o) for i, o, _ in ops]}")
    ctx.note(f"last snapshot summary: {last_summary}")
    after = ctx.witness.delete_entries(tbl)
    ctx.note(f"after DELETE: {after['live_data_files']} live data files, "
             f"{after['delete_manifests']} delete manifests, delete files: {after['files']}")
    if after["live_data_files"] != 3:
        raise CellFail("merge-on-read DELETE rewrote data files: "
                       f"{after['live_data_files']} live data files, expected 3")
    if after["delete_manifests"] < 1 or not after["files"]:
        raise CellFail("no delete manifest / delete file after DELETE; "
                       f"operation={last_op} summary={last_summary}")
    return tbl, after, last_summary


def check_remaining(ctx, tbl):
    got = sorted(r["id"] for r in rows_of(tbl))
    ctx.note(f"PyIceberg scan after DELETE: ids {got}")
    if got != [2, 3]:
        raise CellFail(f"PyIceberg reads ids {got} after DELETE id=1, expected [2, 3]")


@cell("position-deletes", "v2")
def position_deletes_v2(ctx):
    seed_three_files(ctx, "posdel", "'write.delete.mode' = 'merge-on-read', 'format-version' = '2'")
    tbl, after, _ = delete_witness(ctx, "posdel", 2)
    for f in after["files"]:
        if f["content"] != "POSITION_DELETES":
            raise CellFail(f"delete file content is {f['content']}, expected POSITION_DELETES")
        if f["file_format"] == "PUFFIN":
            raise CellFail("v2 delete file is a Puffin file; v2 position deletes are Parquet")
    check_remaining(ctx, tbl)


@cell("deletion-vectors", "v3")
def deletion_vectors_v3(ctx):
    seed_three_files(ctx, "dv", "'write.delete.mode' = 'merge-on-read', 'format-version' = '3'")
    tbl, after, summary = delete_witness(ctx, "dv", 3)
    for f in after["files"]:
        if f["file_format"] != "PUFFIN":
            raise CellFail(f"v3 delete file format is {f['file_format']}, a deletion vector is PUFFIN")
        if f["content_offset"] is None or f["content_size_in_bytes"] is None \
                or not f["referenced_data_file"]:
            raise CellFail(f"Puffin delete file is not a deletion vector (no blob offset/size or "
                           f"referenced data file): {f}")
    for f in after["files"]:
        types = ctx.witness.puffin_blob_types(tbl, f["path"])
        ctx.note(f"Puffin footer blob types: {types}")
        if types != ["deletion-vector-v1"]:
            raise CellFail(f"Puffin delete file blob types {types}, expected ['deletion-vector-v1']")
    if "added-position-delete-files" in summary:
        raise CellFail(f"v3 DELETE added position-delete files: {summary}")
    ctx.note(f"summary added-dvs={summary.get('added-dvs')} "
             f"(witness read the manifests, not SQE)")
    try:
        got = sorted(r["id"] for r in rows_of(tbl))
    except Exception as e:  # noqa: BLE001 - a reader limitation, recorded not judged
        ctx.note(f"PyIceberg {pyiceberg_version()} could not scan the v3 table to apply the vector "
                 f"({type(e).__name__}: {e}); verdict rests on the manifest entries above")
        return
    ctx.note(f"PyIceberg scan after DELETE: ids {got}")
    if got != [2, 3]:
        raise CellFail(f"PyIceberg reads ids {got} after DELETE id=1, expected [2, 3]")


def pyiceberg_version():
    import pyiceberg

    return pyiceberg.__version__


# ── part A cells: row-level, schema, partitioning, snapshots ───────────────
# SQL is ported from the Rust tests named in the state file's evidence
# (v3_e2e.rs, mor_spark_interop.rs, ctas_write_modes_e2e.rs, partition_e2e.rs,
# partition_evolution_e2e.rs, maintenance_procedures_test.rs, integration_test.rs).

MOR_PROPS = ("'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', "
             "'write.merge.mode' = 'merge-on-read'")
COW_PROPS = ("'write.delete.mode' = 'copy-on-write', 'write.update.mode' = 'copy-on-write', "
             "'write.merge.mode' = 'copy-on-write'")
MERGE_SRC = "(SELECT 3 AS id, 333 AS v UNION ALL SELECT 4 AS id, 40 AS v)"


def merge_sql(t):
    return (f"MERGE INTO {t} t USING {MERGE_SRC} s ON t.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET v = s.v "
            f"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)")


def kv_table(ctx, name, version, props, keyed):
    """id/v table, three single-row INSERTs = three data files. keyed declares id as
    the identifier field (SQE's only route to equality deletes)."""
    t = ctx.new_table(name)
    ctx.sql(f"CREATE TABLE {t} (id BIGINT NOT NULL, v BIGINT) "
            f"TBLPROPERTIES ('format-version' = '{version}', {props})")
    if keyed:
        ctx.sql(f"ALTER TABLE {t} SET IDENTIFIER FIELDS id")
    for i in (1, 2, 3):
        ctx.sql(f"INSERT INTO {t} VALUES ({i}, {i * 10})")
    return t


def data_paths(ctx, name):
    tbl = ctx.load(name)
    return tbl, {e["path"] for e in ctx.witness.live_files(tbl) if e["content"] == 0}


def last_snapshot(ctx, tbl):
    sid, op, summ = ctx.witness.snapshot_ops(tbl)[-1]
    return op, summ


def pairs(rows):
    return sorted((r["id"], r["v"]) for r in rows)


def sqe_pairs(ctx, t):
    return sorted((int(r[0]), int(r[1])) for r in ctx.sql(f"SELECT id, v FROM {t}"))


def table_rows_independent(ctx, tbl):
    """(id, v) pairs for a delete-free table: PyIceberg scan when it can, else the
    Parquet files read directly. Never SQE."""
    try:
        got = pairs(rows_of(tbl))
        how = "PyIceberg scan"
    except Exception as e:  # noqa: BLE001 - reader limitation (v3), recorded
        ctx.note(f"PyIceberg {pyiceberg_version()} could not scan: {type(e).__name__}; "
                 f"reading the live Parquet files directly")
        got = pairs(ctx.witness.data_rows(tbl, ["id", "v"]))
        how = "live Parquet files"
    return got, how


def expect(ctx, label, got, want):
    ctx.note(f"{label}: {got}")
    if got != want:
        raise CellFail(f"{label}: {got}, expected {want}")


def equality_deletes_cell(ctx, n):
    t = kv_table(ctx, "eq", n, MOR_PROPS, True)
    tbl = ctx.load("eq")
    ctx.note(assert_format_version(tbl, n))
    ids = tbl.schema().identifier_field_ids
    ctx.note(f"identifier-field-ids in the catalog schema: {ids}")
    if ids != [1]:
        raise CellFail(f"identifier fields not recorded in the schema: {ids}")
    _, seed_paths = data_paths(ctx, "eq")
    if len(seed_paths) != 3:
        raise CellFail(f"expected 3 data files after 3 INSERTs, found {len(seed_paths)}")

    # DELETE: one equality-delete file, no data file rewritten.
    ctx.sql(f"DELETE FROM {t} WHERE id = 1")
    tbl, paths = data_paths(ctx, "eq")
    op, summ = last_snapshot(ctx, tbl)
    eqs = [e for e in ctx.witness.live_files(tbl) if e["content"] == 2]
    ctx.note(f"after DELETE: snapshot op={op}, equality-delete files={len(eqs)}, "
             f"data files unchanged={paths == seed_paths}")
    if len(eqs) != 1 or any(e["equality_ids"] != [1] for e in eqs):
        raise CellFail(f"DELETE wrote {[(e['content'], e['equality_ids']) for e in eqs]} "
                       "delete entries, expected one equality delete on field id 1")
    if paths != seed_paths:
        raise CellFail("equality-delete DELETE rewrote data files")
    keys = ctx.witness.read_parquet(tbl, eqs[0]["path"]).column("id").to_pylist()
    ctx.note(f"equality-delete file holds keys {keys} (read from the Parquet file)")
    if keys != [1]:
        raise CellFail(f"equality-delete file holds keys {keys}, expected [1]")
    expect(ctx, "rows after DELETE (witness applies the equality delete)",
           pairs(ctx.witness.logical_rows(tbl, ["id", "v"])), [(2, 20), (3, 30)])

    # UPDATE: equality delete for the old key + a new data file in one commit.
    ctx.sql(f"UPDATE {t} SET v = 99 WHERE id = 2")
    tbl, paths2 = data_paths(ctx, "eq")
    eqs = [e for e in ctx.witness.live_files(tbl) if e["content"] == 2]
    op, summ = last_snapshot(ctx, tbl)
    ctx.note(f"after UPDATE: snapshot op={op}, added-equality-delete-files="
             f"{summ.get('added-equality-delete-files')}, added-data-files="
             f"{summ.get('added-data-files')}, equality-delete files live={len(eqs)}")
    if len(eqs) != 2 or not seed_paths <= paths2 or len(paths2) != 4:
        raise CellFail("UPDATE did not write one equality delete plus one new data file "
                       f"(eq files {len(eqs)}, data files {len(paths2)})")
    expect(ctx, "rows after UPDATE", pairs(ctx.witness.logical_rows(tbl, ["id", "v"])),
           [(2, 99), (3, 30)])

    # MERGE: matched update + not matched insert.
    ctx.sql(merge_sql(t))
    tbl, paths3 = data_paths(ctx, "eq")
    eqs = [e for e in ctx.witness.live_files(tbl) if e["content"] == 2]
    ctx.note(f"after MERGE: equality-delete files live={len(eqs)}, data files={len(paths3)}")
    if len(eqs) < 3 or not paths2 <= paths3:
        raise CellFail("MERGE did not add an equality delete without rewriting data files")
    want = [(2, 99), (3, 333), (4, 40)]
    expect(ctx, "rows after MERGE", pairs(ctx.witness.logical_rows(tbl, ["id", "v"])), want)
    expect(ctx, "SQE's own read (cross-check, not the witness)", sqe_pairs(ctx, t), want)


@cell("equality-deletes", "v2")
def equality_deletes_v2(ctx):
    equality_deletes_cell(ctx, 2)


@cell("equality-deletes", "v3")
def equality_deletes_v3(ctx):
    equality_deletes_cell(ctx, 3)


def delete_kinds(ctx, name):
    tbl = ctx.load(name)
    es = [e for e in ctx.witness.live_files(tbl) if e["content"] != 0]
    return tbl, es


def merge_on_read_cell(ctx, n):
    # Plain table (no identifier fields): the shape a user gets from the property alone.
    t = seed_three_files(ctx, "mor_plain",
                         f"{MOR_PROPS}, 'format-version' = '{n}'")
    tbl, after, _ = delete_witness(ctx, "mor_plain", n)
    want_fmt = "PUFFIN" if n == 3 else "PARQUET"
    want_kind = "POSITION_DELETES"
    for f in after["files"]:
        if f["content"] != want_kind or f["file_format"] != want_fmt:
            raise CellFail(f"MoR DELETE wrote {f['content']}/{f['file_format']}, "
                           f"expected {want_kind}/{want_fmt}")
    ctx.note(f"MoR DELETE on a plain v{n} table: {len(after['files'])} {want_kind} "
             f"({want_fmt}) file(s), no data file rewritten")
    _, base_paths = data_paths(ctx, "mor_plain")
    _, base_dels = delete_kinds(ctx, "mor_plain")

    fell_back = []
    for label, stmt in (("UPDATE", f"UPDATE {t} SET v = 99 WHERE id = 2"),
                        ("MERGE", merge_sql(t))):
        ctx.sql(stmt)
        tbl, paths = data_paths(ctx, "mor_plain")
        _, dels = delete_kinds(ctx, "mor_plain")
        op, summ = last_snapshot(ctx, tbl)
        added = int(summ.get("added-delete-files", 0) or 0)
        ctx.note(f"plain table, MoR {label}: snapshot op={op}, added-delete-files={added}, "
                 f"previous data files still live={base_paths <= paths}")
        if added == 0 and not base_paths <= paths:
            fell_back.append(label)
        elif added == 0:
            raise CellFail(f"{label} in merge-on-read mode added no delete files and kept "
                           "all data files: the change is not visible on storage")
        base_paths = paths
    if fell_back:
        ctx.add_partial(f"without identifier fields {' and '.join(fell_back)} in "
                        "merge-on-read mode rewrite data files (copy-on-write fallback); only "
                        "DELETE writes delete files on a plain table")
    # Rows (PyIceberg applies position deletes; v3 may not scan).
    tbl = ctx.load("mor_plain")
    try:
        expect(ctx, "plain table rows (PyIceberg scan)", pairs(rows_of(tbl)),
               [(2, 99), (3, 333), (4, 40)])
    except CellFail:
        raise
    except Exception as e:  # noqa: BLE001
        ctx.note(f"PyIceberg {pyiceberg_version()} could not scan the v{n} table "
                 f"({type(e).__name__}: {e}); verdict rests on the manifest entries")

    # Keyed table: DELETE, UPDATE and MERGE are delete-file based end to end.
    k = kv_table(ctx, "mor_keyed", n, MOR_PROPS, True)
    _, seed = data_paths(ctx, "mor_keyed")
    ctx.sql(f"DELETE FROM {k} WHERE id = 1")
    ctx.sql(f"UPDATE {k} SET v = 99 WHERE id = 2")
    ctx.sql(merge_sql(k))
    tbl, paths = data_paths(ctx, "mor_keyed")
    es = [e for e in ctx.witness.live_files(tbl) if e["content"] != 0]
    ctx.note(f"keyed table: delete entries {[(e['content'], e['equality_ids']) for e in es]}, "
             f"original data files still live={seed <= paths}")
    if not seed <= paths or len([e for e in es if e["content"] == 2]) < 3:
        raise CellFail("keyed merge-on-read DELETE/UPDATE/MERGE did not leave original data "
                       f"files in place with equality deletes: {len(es)} delete entries")
    expect(ctx, "keyed table rows (witness applies equality deletes)",
           pairs(ctx.witness.logical_rows(tbl, ["id", "v"])), [(2, 99), (3, 333), (4, 40)])


@cell("merge-on-read", "v2")
def merge_on_read_v2(ctx):
    merge_on_read_cell(ctx, 2)


@cell("merge-on-read", "v3")
def merge_on_read_v3(ctx):
    merge_on_read_cell(ctx, 3)


def copy_on_write_cell(ctx, n):
    t = kv_table(ctx, "cow", n, COW_PROPS, False)
    tbl, seed = data_paths(ctx, "cow")
    ctx.note(assert_format_version(tbl, n))
    for label, stmt, want in (
        ("DELETE", f"DELETE FROM {t} WHERE id = 1", [(2, 20), (3, 30)]),
        ("UPDATE", f"UPDATE {t} SET v = 99 WHERE id = 2", [(2, 99), (3, 30)]),
        ("MERGE", merge_sql(t), [(2, 99), (3, 333), (4, 40)]),
    ):
        before = {e["path"] for e in ctx.witness.live_files(ctx.load("cow")) if e["content"] == 0}
        ctx.sql(stmt)
        tbl = ctx.load("cow")
        ctx.note(assert_format_version(tbl, n))
        es = ctx.witness.live_files(tbl)
        dels = [e for e in es if e["content"] != 0]
        after = {e["path"] for e in es if e["content"] == 0}
        op, summ = last_snapshot(ctx, tbl)
        ctx.note(f"{label}: snapshot op={op}, files removed={len(before - after)}, "
                 f"added={len(after - before)}, delete files live={len(dels)}, "
                 f"added-data-files={summ.get('added-data-files')}, "
                 f"deleted-data-files={summ.get('deleted-data-files')}")
        if dels:
            raise CellFail(f"copy-on-write {label} left delete files: "
                           f"{[(e['content'], e['path']) for e in dels]}")
        if not before - after:
            raise CellFail(f"copy-on-write {label} replaced no data file")
        got, how = table_rows_independent(ctx, tbl)
        expect(ctx, f"rows after {label} ({how})", got, want)


@cell("copy-on-write", "v2")
def copy_on_write_v2(ctx):
    copy_on_write_cell(ctx, 2)


@cell("copy-on-write", "v3")
def copy_on_write_v3(ctx):
    copy_on_write_cell(ctx, 3)


# ── schema: evolution, promotion, defaults ─────────────────────────────────


def parquet_field_ids(ctx, tbl, path):
    t = ctx.witness.read_parquet(tbl, path)
    return {f.name: int(f.metadata[b"PARQUET:field_id"]) for f in t.schema
            if f.metadata and b"PARQUET:field_id" in f.metadata}, t


def schema_evolution_cell(ctx, n):
    t = ctx.new_table("se")
    ctx.sql(f"CREATE TABLE {t} (id BIGINT, a INT, b VARCHAR) "
            f"TBLPROPERTIES ('format-version' = '{n}')")
    ctx.sql(f"INSERT INTO {t} VALUES (1, 10, 'x')")
    tbl = ctx.load("se")
    ctx.note(assert_format_version(tbl, n))
    ids0 = {f.name: f.field_id for f in tbl.schema().fields}
    schema_id0 = tbl.metadata.current_schema_id
    old_files = [e["path"] for e in ctx.witness.live_files(tbl)]
    ctx.note(f"before: field ids {ids0}, schema-id {schema_id0}")

    ctx.sql(f"ALTER TABLE {t} ADD COLUMN c DOUBLE")
    ctx.sql(f"ALTER TABLE {t} RENAME COLUMN b TO b2")
    ctx.sql(f"ALTER TABLE {t} DROP COLUMN a")
    ctx.sql(f"INSERT INTO {t} (id, b2, c) VALUES (2, 'y', 1.5)")
    tbl = ctx.load("se")
    ctx.note(assert_format_version(tbl, n))
    ids1 = {f.name: f.field_id for f in tbl.schema().fields}
    ctx.note(f"after ADD c, RENAME b->b2, DROP a: field ids {ids1}, "
             f"schema-id {tbl.metadata.current_schema_id}, "
             f"schema ids kept in metadata {[s.schema_id for s in tbl.metadata.schemas]}")
    if set(ids1) != {"id", "b2", "c"}:
        raise CellFail(f"columns after evolution: {sorted(ids1)}, expected id, b2, c")
    if ids1["id"] != ids0["id"] or ids1["b2"] != ids0["b"]:
        raise CellFail(f"rename changed a field id: before {ids0}, after {ids1}")
    if ids1["c"] in ids0.values():
        raise CellFail(f"new column c reuses an old field id: {ids1['c']}")
    if tbl.metadata.current_schema_id <= schema_id0:
        raise CellFail("schema id did not advance")
    # Existing data was not rewritten, and its physical columns resolve by field id.
    live = [e["path"] for e in ctx.witness.live_files(tbl)]
    if not set(old_files) <= set(live):
        raise CellFail("the data file written before the evolution is no longer live")
    pids, ptab = parquet_field_ids(ctx, tbl, old_files[0])
    ctx.note(f"pre-evolution Parquet file still live; its physical columns/field ids: {pids}")
    if pids.get("b") != ids0["b"]:
        raise CellFail(f"old file column b carries field id {pids.get('b')}, expected {ids0['b']}")
    try:
        got = sorted((r["id"], r["b2"], r["c"]) for r in rows_of(tbl))
        ctx.note(f"PyIceberg scan across old and new files: {got}")
        if got != [(1, "x", None), (2, "y", 1.5)]:
            raise CellFail(f"PyIceberg reads {got} across the evolution")
    except CellFail:
        raise
    except Exception as e:  # noqa: BLE001 - reader limitation, recorded
        ctx.note(f"PyIceberg {pyiceberg_version()} could not scan the v{n} table "
                 f"({type(e).__name__}: {e}); field-id evidence above stands")

    # Reorder: Iceberg's schema update API can move columns; SQL has FIRST / AFTER.
    ctx.sql(f"ALTER TABLE {t} ADD COLUMN d INT FIRST")  # Trino/Spark syntax
    tbl = ctx.load("se")
    names = [f.name for f in tbl.schema().fields]
    ctx.note(f"ADD COLUMN d INT FIRST accepted; column order in the catalog schema: {names}")
    reorder_possible = names[0] == "d"
    if not reorder_possible:
        ctx.note("FIRST was accepted and then ignored: d was appended last")
    for stmt in (f"ALTER TABLE {t} ALTER COLUMN c FIRST", f"ALTER TABLE {t} ALTER COLUMN c AFTER id"):
        try:
            ctx.sql(stmt)
            tbl = ctx.load("se")
            names2 = [f.name for f in tbl.schema().fields]
            ctx.note(f"{stmt!r} accepted; order {names2}")
            reorder_possible = reorder_possible or names2[0] == "c" or names2[1] == "c"
        except SqlError as e:
            ctx.note(f"reorder statement rejected: {str(e)[:200]}")
    if not reorder_possible:
        ctx.add_partial("columns cannot be reordered: ADD COLUMN ... FIRST is accepted but "
                        "ignored (column appended last) and ALTER COLUMN ... FIRST/AFTER is a "
                        "parse error")


@cell("schema-evolution", "v2")
def schema_evolution_v2(ctx):
    schema_evolution_cell(ctx, 2)


@cell("schema-evolution", "v3")
def schema_evolution_v3(ctx):
    schema_evolution_cell(ctx, 3)


def type_promotion_cell(ctx, n):
    from decimal import Decimal

    t = ctx.new_table("tp")
    ctx.sql(f"CREATE TABLE {t} (i INT, f REAL, d DECIMAL(5,2)) "
            f"TBLPROPERTIES ('format-version' = '{n}')")
    ctx.sql(f"INSERT INTO {t} VALUES (1, 1.5, 12.34)")
    tbl = ctx.load("tp")
    ctx.note(assert_format_version(tbl, n))
    before = {f.name: str(f.field_type) for f in tbl.schema().fields}
    old_files = [e["path"] for e in ctx.witness.live_files(tbl)]
    ctx.note(f"schema before: {before}")
    if before != {"i": "int", "f": "float", "d": "decimal(5, 2)"}:
        raise CellFail(f"unexpected starting schema {before}")
    for col, ty in (("i", "BIGINT"), ("f", "DOUBLE"), ("d", "DECIMAL(10,2)")):
        ctx.sql(f"ALTER TABLE {t} ALTER COLUMN {col} SET DATA TYPE {ty}")
    ctx.sql(f"INSERT INTO {t} VALUES (3000000000, 2.5, 12345678.90)")
    tbl = ctx.load("tp")
    ctx.note(assert_format_version(tbl, n))
    after = {f.name: str(f.field_type) for f in tbl.schema().fields}
    ctx.note(f"schema after (catalog): {after}")
    if after != {"i": "long", "f": "double", "d": "decimal(10, 2)"}:
        raise CellFail(f"catalog schema after promotion: {after}")
    # The old data file keeps its narrow physical types: nothing was rewritten.
    if not set(old_files) <= {e["path"] for e in ctx.witness.live_files(tbl)}:
        raise CellFail("the data file written before the promotion is no longer live")
    ptab = ctx.witness.read_parquet(tbl, old_files[0])
    phys = {f.name: str(f.type) for f in ptab.schema}
    ctx.note(f"old Parquet file physical types (not rewritten): {phys}")
    if phys.get("i") != "int32":
        raise CellFail(f"old file column i is {phys.get('i')}, so it was rewritten")
    try:
        got = sorted((r["i"], r["f"], str(r["d"])) for r in rows_of(tbl))
        ctx.note(f"PyIceberg scan of old and new files as promoted types: {got}")
        if got != [(1, 1.5, "12.34"), (3000000000, 2.5, "12345678.90")]:
            raise CellFail(f"PyIceberg reads {got} after promotion")
    except CellFail:
        raise
    except Exception as e:  # noqa: BLE001
        ctx.note(f"PyIceberg {pyiceberg_version()} could not scan the v{n} table "
                 f"({type(e).__name__}: {e}); verdict rests on the schema and file evidence")
    # Narrowing must be refused and leave the schema alone.
    try:
        ctx.sql(f"ALTER TABLE {t} ALTER COLUMN i SET DATA TYPE INT")
        raise CellFail("narrowing BIGINT -> INT was accepted")
    except SqlError as e:
        ctx.note(f"narrowing long -> int refused: {str(e)[:160]}")
    if {f.name: str(f.field_type) for f in ctx.load("tp").schema().fields} != after:
        raise CellFail("a refused narrowing changed the schema")


@cell("type-promotion", "v2")
def type_promotion_v2(ctx):
    type_promotion_cell(ctx, 2)


@cell("type-promotion", "v3")
def type_promotion_v3(ctx):
    type_promotion_cell(ctx, 3)


@cell("column-default-values", "v3")
def column_default_values_v3(ctx):
    t = ctx.new_table("dflt")
    ctx.sql(f"CREATE TABLE {t} (id BIGINT) TBLPROPERTIES ('format-version' = '3')")
    ctx.sql(f"INSERT INTO {t} VALUES (1)")
    ctx.sql(f"ALTER TABLE {t} ADD COLUMN region VARCHAR DEFAULT 'eu'")
    ctx.sql(f"INSERT INTO {t} (id) VALUES (2)")
    tbl = ctx.load("dflt")
    ctx.note(assert_format_version(tbl, 3))
    meta = ctx.witness.raw_metadata(tbl)
    schema = next(s for s in meta["schemas"] if s["schema-id"] == meta["current-schema-id"])
    fld = next((f for f in schema["fields"] if f["name"] == "region"), None)
    ctx.note(f"raw metadata JSON, current schema field: {fld}")
    if fld is None:
        raise CellFail("column region missing from the current schema")
    if fld.get("initial-default") != "eu" or fld.get("write-default") != "eu":
        raise CellFail(f"field lacks initial-default/write-default 'eu': {fld}")
    files = [e for e in ctx.witness.live_files(tbl) if e["content"] == 0]
    shapes = {}
    for e in files:
        tab = ctx.witness.read_parquet(tbl, e["path"])
        shapes[e["path"]] = tab
    old = [tb for tb in shapes.values() if "region" not in tb.column_names]
    new = [tb for tb in shapes.values() if "region" in tb.column_names]
    ctx.note(f"data files: {len(old)} written before the column (no region column), "
             f"{len(new)} after")
    if len(old) != 1 or len(new) != 1:
        raise CellFail("expected one file before and one after the ALTER")
    wrote = new[0].column("region").to_pylist()
    ctx.note(f"write-default: the file written by INSERT (id) holds region = {wrote} "
             "(read from Parquet)")
    if wrote != ["eu"]:
        raise CellFail(f"INSERT omitting the column wrote region {wrote}, expected ['eu']")
    rows = sorted((int(r[0]), r[1]) for r in ctx.sql(f"SELECT id, region FROM {t}"))
    ctx.note(f"SQE read, old file has no region column so 'eu' can only be the initial-default: {rows}")
    if rows != [(1, "eu"), (2, "eu")]:
        raise CellFail(f"rows {rows}, expected [(1,'eu'),(2,'eu')]")
    try:
        got = sorted((r["id"], r["region"]) for r in rows_of(tbl))
        ctx.note(f"PyIceberg scan (second reader): {got}")
    except Exception as e:  # noqa: BLE001
        ctx.note(f"PyIceberg {pyiceberg_version()} could not scan the v3 table "
                 f"({type(e).__name__}: {str(e)[:120]})")


# ── partitioning ───────────────────────────────────────────────────────────


def norm_partition_value(v):
    """Avro-decoded partition values to the integers Iceberg transforms produce."""
    if isinstance(v, dt.datetime):
        return (v.replace(tzinfo=None) - dt.datetime(1970, 1, 1)) // dt.timedelta(microseconds=1)
    if isinstance(v, dt.date):
        return (v - dt.date(1970, 1, 1)).days
    return v


def to_transform_input(v):
    return norm_partition_value(v) if isinstance(v, (dt.date, dt.datetime)) else v


def hidden_partitioning_cell(ctx, n):
    t = ctx.new_table("hp")
    ctx.sql(
        f"CREATE TABLE {t} (id BIGINT, cat VARCHAR, nm VARCHAR, d DATE, ts TIMESTAMP, "
        f"ts2 TIMESTAMP, ts3 TIMESTAMP) "
        f"PARTITIONED BY (cat, year(d), month(ts), day(ts2), hour(ts3), bucket(4, id), "
        f"truncate(2, nm)) TBLPROPERTIES ('format-version' = '{n}')")
    for i in range(1, 7):
        ctx.sql(f"INSERT INTO {t} VALUES ({i}, 'c{i % 2}', 'name{i}', DATE '202{i % 3 + 4}-0{i}-15', "
                f"TIMESTAMP '2026-0{i}-1{i} 0{i}:30:00', TIMESTAMP '2026-01-0{i % 3 + 1} 10:00:00', "
                f"TIMESTAMP '2026-01-01 0{i}:15:00')")
    tbl = ctx.load("hp")
    ctx.note(assert_format_version(tbl, n))
    spec = tbl.spec()
    kinds = [(f.name, str(f.transform)) for f in spec.fields]
    ctx.note(f"partition spec in the catalog: {kinds}")
    got_kinds = {k for _, k in kinds}
    want_kinds = {"identity", "year", "month", "day", "hour", "bucket[4]", "truncate[2]"}
    if got_kinds != want_kinds:
        raise CellFail(f"spec transforms {sorted(got_kinds)}, expected {sorted(want_kinds)}")
    src_cols = {f.field_id: f.name for f in tbl.schema().fields}
    files = [e for e in ctx.witness.live_files(tbl) if e["content"] == 0]
    if len(files) != 6:
        raise CellFail(f"expected 6 data files (one per INSERT), found {len(files)}")
    checked = 0
    for e in files:
        row = ctx.witness.read_parquet(tbl, e["path"]).to_pylist()[0]
        for pf in spec.fields:
            col = src_cols[pf.source_id]
            ftype = tbl.schema().find_field(pf.source_id).field_type
            want = pf.transform.transform(ftype)(to_transform_input(row[col]))
            have = norm_partition_value(e["partition"].get(pf.name))
            if want != have:
                raise CellFail(f"id {row['id']}: partition field {pf.name} is {have} in the "
                               f"manifest, the Iceberg transform of {row[col]!r} is {want}")
            checked += 1
    ctx.note(f"manifest partition tuples match the Iceberg transforms: {checked} values over "
             f"{len(files)} files (expected values from PyIceberg's transform implementations)")
    ctx.note("one sample manifest entry partition: "
             f"{ {k: str(v) for k, v in files[0]['partition'].items()} }")
    # Filtering on the source column (no partition column in the query).
    target = ctx.sql(f"SELECT count(*) FROM {t} WHERE ts2 >= TIMESTAMP '2026-01-02 00:00:00' "
                     f"AND ts2 < TIMESTAMP '2026-01-03 00:00:00'")[0][0]
    day_field = next(f for f in spec.fields if f.name.startswith("ts2"))
    day = (dt.date(2026, 1, 2) - dt.date(1970, 1, 1)).days
    in_manifest = sum(1 for e in files if norm_partition_value(e["partition"].get(day_field.name)) == day)
    ctx.note(f"filter on ts2 day 2026-01-02: SQE counts {target} rows; the manifests hold "
             f"{in_manifest} files with {day_field.name} = {day} (the files a pruning scan keeps)")
    if int(target) != in_manifest:
        raise CellFail(f"filtered count {target} differs from the {in_manifest} rows in matching files")
    ctx.note("SQE exposes no per-query scanned-file count, so file-level pruning itself is not "
             "observed; the partition tuples above are what a pruning scan uses")


@cell("hidden-partitioning", "v2")
def hidden_partitioning_v2(ctx):
    hidden_partitioning_cell(ctx, 2)


@cell("hidden-partitioning", "v3")
def hidden_partitioning_v3(ctx):
    hidden_partitioning_cell(ctx, 3)


def partition_evolution_cell(ctx, n):
    t = ctx.new_table("pe")
    ctx.sql(f"CREATE TABLE {t} (id BIGINT, ts TIMESTAMP, region VARCHAR) PARTITIONED BY (region) "
            f"TBLPROPERTIES ('format-version' = '{n}')")
    ctx.sql(f"INSERT INTO {t} VALUES (1, TIMESTAMP '2026-01-01 00:00:00', 'a')")
    ctx.sql(f"ALTER TABLE {t} ADD PARTITION FIELD day(ts)")
    ctx.sql(f"INSERT INTO {t} VALUES (2, TIMESTAMP '2026-01-02 00:00:00', 'b')")
    ctx.sql(f"ALTER TABLE {t} DROP PARTITION FIELD region")
    ctx.sql(f"INSERT INTO {t} VALUES (3, TIMESTAMP '2026-01-03 00:00:00', 'c')")
    tbl = ctx.load("pe")
    ctx.note(assert_format_version(tbl, n))
    spec_ids = [s.spec_id for s in tbl.metadata.partition_specs]
    ctx.note(f"partition specs in the catalog: ids {spec_ids}, default {tbl.metadata.default_spec_id}; "
             f"fields per spec {[[f.name for f in s.fields] for s in tbl.metadata.partition_specs]}")
    if len(spec_ids) < 3 or tbl.metadata.default_spec_id != max(spec_ids):
        raise CellFail(f"expected at least 3 specs with the newest as default, got {spec_ids}")
    files = [e for e in ctx.witness.live_files(tbl) if e["content"] == 0]
    by_row = {}
    for e in files:
        rid = ctx.witness.read_parquet(tbl, e["path"]).column("id").to_pylist()[0]
        by_row[rid] = (e["spec_id"], sorted(e["partition"]))
    ctx.note(f"per data file (row id -> manifest spec id, partition fields): {by_row}")
    if len({v[0] for v in by_row.values()}) != 3:
        raise CellFail(f"files carry spec ids {sorted({v[0] for v in by_row.values()})}, "
                       "expected three different ones")
    if by_row[1][1] == by_row[3][1]:
        raise CellFail("old and new files carry the same partition fields")
    got, how = table_rows_independent_ids(ctx, tbl)
    expect(ctx, f"all rows across three specs ({how})", got, [1, 2, 3])
    for stmt, want in ((f"SELECT id FROM {t} WHERE region = 'a'", [1]),
                       (f"SELECT id FROM {t} WHERE ts >= TIMESTAMP '2026-01-03 00:00:00'", [3])):
        got = sorted(int(r[0]) for r in ctx.sql(stmt))
        ctx.note(f"SQE {stmt[len('SELECT id FROM '):]} -> {got}")
        if got != want:
            raise CellFail(f"{stmt} -> {got}, expected {want}")


def table_rows_independent_ids(ctx, tbl):
    try:
        return sorted(r["id"] for r in rows_of(tbl)), "PyIceberg scan"
    except Exception as e:  # noqa: BLE001
        ctx.note(f"PyIceberg {pyiceberg_version()} could not scan: {type(e).__name__}; "
                 f"reading the live Parquet files directly")
        return sorted(r["id"] for r in ctx.witness.data_rows(tbl, ["id"])), "live Parquet files"


@cell("partition-evolution", "v2")
def partition_evolution_v2(ctx):
    partition_evolution_cell(ctx, 2)


@cell("partition-evolution", "v3")
def partition_evolution_v3(ctx):
    partition_evolution_cell(ctx, 3)


@cell("multi-arg-transforms", "v3")
def multi_arg_transforms_v3(ctx):
    # The v3 feature is a transform over several source columns. The upstream
    # Trino test creates bucket(a, b, 4) on a v3 table and counts acceptance.
    t = ctx.new_table("multi")
    attempts = [f"CREATE TABLE {t} (a BIGINT, b BIGINT, v VARCHAR) PARTITIONED BY (bucket(4, a, b)) "
                f"TBLPROPERTIES ('format-version' = '3')",
                f"CREATE TABLE {t} (a BIGINT, b BIGINT, v VARCHAR) PARTITIONED BY (bucket(a, b, 4)) "
                f"TBLPROPERTIES ('format-version' = '3')"]
    errors = []
    for sql in attempts:
        try:
            ctx.sql(sql)
        except SqlError as e:
            errors.append(str(e).split("->", 1)[-1].strip()[:200])
            ctx.note(f"rejected: {sql[sql.index('PARTITIONED'):sql.index(' TBLPROPERTIES')]} -> {errors[-1]}")
            continue
        tbl = ctx.load("multi")
        ctx.note(assert_format_version(tbl, 3))
        multi = [f for f in tbl.spec().fields if getattr(f, "source_ids", None) and len(f.source_ids) > 1]
        ctx.note(f"table created; partition fields with several source ids: {multi}")
        if not multi:
            raise CellFail("the table was created but its spec has no multi-source transform")
        return
    ctx.note("SQE refuses to create a table partitioned by a transform over several source "
             "columns. The state file's partial rests on read-only handling of such tables "
             "written by another engine; no engine writes them yet and PyIceberg cannot write "
             "v3, so that read behaviour is not measured here.")
    # A write refusal is consistent with the claimed partial (read-only handling),
    # so it is not a verdict on the claim: the cell stays unmeasured until a
    # fixture writer (Spark) produces such a table for SQE to read.
    raise CellSkip("needs a v3 multi-arg-transform table written by another engine (no fixture "
               "writer yet); SQE refuses to create one: " + errors[0])


# ── snapshots: time travel, refs, maintenance ──────────────────────────────


def ids_at(ctx, tbl, snap):
    out = []
    for e in ctx.witness.live_files(tbl, snap):
        if e["content"] == 0:
            out += ctx.witness.read_parquet(tbl, e["path"]).column("id").to_pylist()
    return sorted(out)


def fids(ctx, stmt, over=None):
    return sorted(int(r[0]) for r in (over or ctx.flight()).sql(stmt))


def time_travel_cell(ctx, n):
    t = ctx.new_table("tt")
    ctx.sql(f"CREATE TABLE {t} (id BIGINT) TBLPROPERTIES ('format-version' = '{n}')")
    for i in (1, 2, 3):
        ctx.sql(f"INSERT INTO {t} VALUES ({i})")
        time.sleep(2.2)
    tbl = ctx.load("tt")
    ctx.note(assert_format_version(tbl, n))
    snaps = sorted(tbl.snapshots(), key=lambda s: s.timestamp_ms)
    if len(snaps) != 3:
        raise CellFail(f"expected 3 snapshots after 3 INSERTs, catalog has {len(snaps)}")
    ctx.note("snapshots (id, commit ms): " + str([(s.snapshot_id, s.timestamp_ms) for s in snaps]))
    want = [ids_at(ctx, tbl, s) for s in snaps]
    ctx.note(f"rows per snapshot, read from the manifests by the witness: {want}")
    for s, w in zip(snaps, want):
        got = fids(ctx, f"SELECT id FROM {t} FOR VERSION AS OF {s.snapshot_id}")
        ctx.note(f"FOR VERSION AS OF {s.snapshot_id} -> {got}")
        if got != w:
            raise CellFail(f"FOR VERSION AS OF {s.snapshot_id} returned {got}, snapshot holds {w}")
    # By timestamp: a boundary between snapshot 1 and snapshot 2.
    ms = (snaps[0].timestamp_ms // 1000 + 1) * 1000
    utc = dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if not snaps[0].timestamp_ms < ms <= snaps[1].timestamp_ms:
        raise CellFail("timestamp boundary fell outside the snapshot gap (harness timing)")
    forms = {
        "FOR TIMESTAMP AS OF <epoch ms>": f"FOR TIMESTAMP AS OF {ms}",
        "FOR TIMESTAMP AS OF TIMESTAMP '<utc>'": f"FOR TIMESTAMP AS OF TIMESTAMP '{utc}'",
        "FOR SYSTEM_TIME AS OF TIMESTAMP '<utc>'": f"FOR SYSTEM_TIME AS OF TIMESTAMP '{utc}'",
    }
    for label, clause in forms.items():
        got = fids(ctx, f"SELECT id FROM {t} {clause}")
        ctx.note(f"{label} (boundary {utc} UTC, between snapshot 1 and 2) -> {got}")
        if got != want[0]:
            raise CellFail(f"{label} returned {got}, the snapshot current at that time holds {want[0]}")
    # The same clause over the Trino HTTP front door.
    err = ctx.trino_error(f"SELECT id FROM {t} FOR VERSION AS OF {snaps[0].snapshot_id}")
    if err:
        ctx.note(f"Trino HTTP: FOR VERSION AS OF rejected: {err.split('->', 1)[-1].strip()[:200]}")
        ctx.add_protocol_gap("Trino HTTP rejects FOR VERSION AS OF / FOR TIMESTAMP AS OF with a "
                             "parse error (works over Flight SQL)")
    else:
        ctx.note("Trino HTTP accepts FOR VERSION AS OF")


@cell("time-travel", "v2")
def time_travel_v2(ctx):
    time_travel_cell(ctx, 2)


@cell("time-travel", "v3")
def time_travel_v3(ctx):
    time_travel_cell(ctx, 3)


def branching_cell(ctx, n):
    t = ctx.new_table("br")
    ctx.sql(f"CREATE TABLE {t} (id BIGINT) TBLPROPERTIES ('format-version' = '{n}')")
    ctx.sql(f"INSERT INTO {t} VALUES (1)")
    ctx.sql(f"INSERT INTO {t} VALUES (2)")
    fl = ctx.flight()
    fl.sql(f"ALTER TABLE {t} CREATE BRANCH dev")
    fl.sql(f"ALTER TABLE {t} CREATE TAG rel1")
    tbl = ctx.load("br")
    ctx.note(assert_format_version(tbl, n))
    refs = tbl.metadata.refs
    main_id = tbl.current_snapshot().snapshot_id
    ctx.note("refs in the catalog metadata: " + str({k: (v.snapshot_ref_type.value, v.snapshot_id)
                                                     for k, v in refs.items()}))
    if refs.get("dev") is None or refs["dev"].snapshot_ref_type.value != "branch" \
            or refs.get("rel1") is None or refs["rel1"].snapshot_ref_type.value != "tag":
        raise CellFail(f"metadata refs are {list(refs)}, expected branch dev and tag rel1")
    if refs["dev"].snapshot_id != main_id or refs["rel1"].snapshot_id != main_id:
        raise CellFail("new refs do not point at main's current snapshot")
    # Write to the branch (session state, Flight SQL).
    fl.sql("SET WRITE_BRANCH = 'dev'")
    fl.sql(f"INSERT INTO {t} VALUES (100)")
    fl.sql(f"DELETE FROM {t} WHERE id = 1")
    fl.sql("RESET WRITE_BRANCH")
    tbl = ctx.load("br")
    refs = tbl.metadata.refs
    by_id = {s.snapshot_id: s for s in tbl.snapshots()}
    ctx.note("refs after writes to dev: " + str({k: v.snapshot_id for k, v in refs.items()}))
    if tbl.current_snapshot().snapshot_id != main_id or refs["main"].snapshot_id != main_id:
        raise CellFail("main moved although the writes went to the branch")
    if refs["dev"].snapshot_id == main_id:
        raise CellFail("the branch did not advance")
    if refs["rel1"].snapshot_id != main_id:
        raise CellFail("the tag moved")
    dev_ids = ids_at(ctx, tbl, by_id[refs["dev"].snapshot_id])
    main_ids = ids_at(ctx, tbl, by_id[main_id])
    ctx.note(f"rows at branch dev (witness): {dev_ids}; at main: {main_ids}")
    if dev_ids != [2, 100] or main_ids != [1, 2]:
        raise CellFail(f"branch holds {dev_ids} and main {main_ids}, expected [2, 100] and [1, 2]")
    for ref, want in (("dev", [2, 100]), ("rel1", [1, 2])):
        got = fids(ctx, f"SELECT id FROM {t} FOR VERSION AS OF '{ref}'")
        ctx.note(f"FOR VERSION AS OF '{ref}' -> {got}")
        if got != want:
            raise CellFail(f"FOR VERSION AS OF '{ref}' returned {got}, expected {want}")
    cur = fids(ctx, f"SELECT id FROM {t}")
    ctx.note(f"current (main) read -> {cur}")
    if cur != [1, 2]:
        raise CellFail(f"main reads {cur} after branch writes, expected [1, 2]")
    # Drop the refs again.
    fl.sql(f"ALTER TABLE {t} DROP TAG rel1")
    fl.sql(f"ALTER TABLE {t} DROP BRANCH dev")
    left = set(ctx.load("br").metadata.refs)
    ctx.note(f"refs after DROP TAG / DROP BRANCH: {sorted(left)}")
    if left != {"main"}:
        raise CellFail(f"refs left after drops: {sorted(left)}")
    # The same through Trino HTTP.
    ctx.sql(f"ALTER TABLE {t} CREATE BRANCH dev2")
    ctx.note("Trino HTTP: ALTER TABLE ... CREATE BRANCH accepted")
    gaps = []
    e = ctx.trino_error("SET WRITE_BRANCH = 'dev2'")
    if e:
        ctx.note(f"Trino HTTP: SET WRITE_BRANCH rejected: {e.split('->', 1)[-1].strip()[:160]}")
        gaps.append("SET WRITE_BRANCH")
    e = ctx.trino_error(f"SELECT id FROM {t} FOR VERSION AS OF 'dev2'")
    if e:
        ctx.note(f"Trino HTTP: FOR VERSION AS OF '<ref>' rejected: {e.split('->', 1)[-1].strip()[:160]}")
        gaps.append("FOR VERSION AS OF '<ref>'")
    if gaps:
        ctx.add_protocol_gap("Trino HTTP rejects " + " and ".join(gaps)
                             + " (branch writes and reads work over Flight SQL)")


@cell("branching-tagging", "v2")
def branching_tagging_v2(ctx):
    branching_cell(ctx, 2)


@cell("branching-tagging", "v3")
def branching_tagging_v3(ctx):
    branching_cell(ctx, 3)


def live_ids_direct(ctx, tbl):
    return sorted(r["id"] for r in ctx.witness.data_rows(tbl, ["id"]))


def maintenance_cell(ctx, n):
    t = ctx.new_table("maint")
    ctx.sql(f"CREATE TABLE {t} (id BIGINT, v VARCHAR) TBLPROPERTIES ('format-version' = '{n}')")
    for i in range(1, 6):
        ctx.sql(f"INSERT INTO {t} VALUES ({i}, 'row{i}')")
    q = f"{ctx.ns}.maint"
    tbl = ctx.load("maint")
    ctx.note(assert_format_version(tbl, n))

    def shape(tbl):
        fs = ctx.witness.live_files(tbl)
        ma = ctx.witness.delete_entries(tbl)
        return {"snapshots": len(tbl.snapshots()), "data_files": sum(1 for e in fs if e["content"] == 0),
                "data_manifests": ma["data_manifests"]}

    s0 = shape(tbl)
    ctx.note(f"before maintenance: {s0}")
    if s0["data_files"] != 5 or s0["snapshots"] != 5:
        raise CellFail(f"expected 5 snapshots and 5 data files, found {s0}")
    want_ids = [1, 2, 3, 4, 5]

    # rewrite_data_files (compaction)
    ctx.sql(f"CALL system.rewrite_data_files(table => '{q}', min_input_files => 2, "
            f"target_file_size_bytes => 1073741824)")
    tbl = ctx.load("maint")
    s1 = shape(tbl)
    op, summ = last_snapshot(ctx, tbl)
    ctx.note(f"after rewrite_data_files: {s1}, snapshot op={op}, "
             f"added-data-files={summ.get('added-data-files')}, "
             f"deleted-data-files={summ.get('deleted-data-files')}")
    if not s1["data_files"] < s0["data_files"] or s1["snapshots"] != s0["snapshots"] + 1:
        raise CellFail(f"rewrite_data_files: {s0['data_files']} -> {s1['data_files']} data files, "
                       f"snapshots {s0['snapshots']} -> {s1['snapshots']}")
    ids = live_ids_direct(ctx, tbl)
    ctx.note(f"rows in the live files after compaction (witness, Parquet): {ids}")
    if ids != want_ids:
        raise CellFail(f"compaction changed the rows: {ids}")

    # rewrite_manifests
    ctx.sql(f"CALL system.rewrite_manifests(table => '{q}')")
    tbl = ctx.load("maint")
    s2 = shape(tbl)
    ctx.note(f"after rewrite_manifests: {s2} (data manifests {s1['data_manifests']} -> "
             f"{s2['data_manifests']})")
    if not s2["data_manifests"] < s1["data_manifests"]:
        raise CellFail(f"rewrite_manifests left {s2['data_manifests']} data manifests "
                       f"(were {s1['data_manifests']})")
    if live_ids_direct(ctx, tbl) != want_ids:
        raise CellFail("rewrite_manifests changed the rows")

    # expire_snapshots
    cur_id = tbl.current_snapshot().snapshot_id
    cutoff = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ctx.sql(f"CALL system.expire_snapshots(table => '{q}', older_than => '{cutoff}', retain_last => 1)")
    tbl = ctx.load("maint")
    left = [s.snapshot_id for s in tbl.snapshots()]
    ctx.note(f"after expire_snapshots: snapshots {s2['snapshots']} -> {len(left)}; "
             f"current snapshot kept={tbl.current_snapshot().snapshot_id == cur_id}")
    if left != [cur_id]:
        raise CellFail(f"expire_snapshots left snapshots {left}, expected only the current {cur_id}")
    if live_ids_direct(ctx, tbl) != want_ids:
        raise CellFail("expire_snapshots changed the rows")

    # remove_orphan_files: plant a file under the table, then delete it.
    live = [e["path"] for e in ctx.witness.live_files(tbl) if e["content"] == 0]
    orphan = live[0].rsplit("/", 1)[0] + "/orphan-matrix-suite.parquet"
    out = tbl.io.new_output(orphan)
    with out.create() as f:
        f.write(b"not referenced by any manifest")
    ctx.note(f"planted orphan {orphan}")
    time.sleep(3)
    now = dt.datetime.now(dt.timezone.utc)
    cutoff = (now - dt.timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    dry = ctx.sql(f"CALL system.remove_orphan_files(table => '{q}', older_than => '{cutoff}', "
                  f"dry_run => true)")
    listed = [str(r) for r in dry if "orphan-matrix-suite" in str(r)]
    ctx.note(f"dry run lists the orphan: {bool(listed)}; file still present: "
             f"{tbl.io.new_input(orphan).exists()}")
    if not listed or not tbl.io.new_input(orphan).exists():
        raise CellFail("remove_orphan_files dry run did not list the planted orphan (or deleted it)")
    ctx.sql(f"CALL system.remove_orphan_files(table => '{q}', older_than => '{cutoff}')")
    gone = not tbl.io.new_input(orphan).exists()
    kept = all(tbl.io.new_input(p).exists() for p in live)
    ctx.note(f"after remove_orphan_files: orphan removed={gone}, live data files intact={kept}")
    if not gone:
        raise CellFail("remove_orphan_files did not delete the planted orphan")
    if not kept:
        raise CellFail("remove_orphan_files deleted a live data file")
    if live_ids_direct(ctx, ctx.load("maint")) != want_ids:
        raise CellFail("remove_orphan_files changed the rows")
    ctx.note("matrix config sets [maintenance] orphan_min_age_secs = 1 (default one day) so a "
             "planted file can be deleted within the run")


@cell("table-maintenance", "v2")
def table_maintenance_v2(ctx):
    maintenance_cell(ctx, 2)


@cell("table-maintenance", "v3")
def table_maintenance_v3(ctx):
    maintenance_cell(ctx, 3)



# ── part B cells: reading, writing, statistics, catalogs, v3 types ─────────
# Every write cell proves its result with a reader that is not SQE: PyIceberg, the
# catalog's raw loadTable JSON, raw Avro manifests, Parquet and Puffin files read
# directly. SQE reading its own data back is noted as a cross-check only.
# PyIceberg 0.12 cannot write or upgrade to format-version 3, so a v3 fixture is a
# PyIceberg-written v2 table upgraded by a raw catalog commit (metadata only).


def explain_scan_line(ctx, statement):
    """The IcebergScanExec line of an EXPLAIN result (a client-protocol output)."""
    rows = ctx.sql(f"EXPLAIN {statement}")
    text = "\n".join(str(r[0]) for r in rows)
    for line in text.splitlines():
        if "IcebergScanExec" in line:
            return line.strip(), text
    return None, text


def scan_output_rows(ctx, statement):
    """Rows the IcebergScanExec node emitted, from EXPLAIN ANALYZE (a client-protocol
    result), or None when the output carries no such line."""
    import re

    for r in ctx.sql(f"EXPLAIN ANALYZE {statement}"):
        m = re.search(r"operation=IcebergScanExec output_rows=(\d+)", str(r[0]))
        if m:
            return int(m.group(1))
    return None


def pyiceberg_fixture(ctx, name, n):
    """A table SQE did not write: PyIceberg creates it (two identity partitions,
    four data files over two appends) and, for v3, the catalog upgrades it."""
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    schema = Schema(NestedField(1, "id", LongType(), required=False),
                    NestedField(2, "region", StringType(), required=False),
                    NestedField(3, "val", LongType(), required=False))
    spec = PartitionSpec(PartitionField(source_id=2, field_id=1000,
                                        transform=IdentityTransform(), name="region"))
    ctx.tables.append(name)
    ctx.witness.catalog.create_table((ctx.ns, name), schema=schema, partition_spec=spec,
                                     properties={"format-version": "2"})
    tbl = ctx.load(name)

    def batch(ids, regions, vals):
        return pa.table({"id": pa.array(ids, pa.int64()), "region": regions,
                         "val": pa.array(vals, pa.int64())})

    tbl.append(batch([1, 2, 3], ["a", "a", "b"], [10, 20, 30]))
    tbl.append(batch([4, 5], ["b", "a"], [40, 50]))
    if n == 3:
        ctx.witness.upgrade_format_version(ctx.ns, name, 3)
    return f"{ctx.catalog}.{ctx.ns}.{name}"


PYI_ROWS = [(1, "a", 10), (2, "a", 20), (3, "b", 30), (4, "b", 40), (5, "a", 50)]


def read_support_cell(ctx, n):
    t = pyiceberg_fixture(ctx, "fixture", n)
    raw = ctx.load_raw("fixture")
    ctx.note(assert_raw_format_version(raw, n))
    files = raw.data_files()
    parts = sorted({e["data_file"]["partition"]["region"] for e in files})
    ctx.note(f"fixture written by PyIceberg {pyiceberg_version()}: {len(files)} data files, "
             f"partitions {parts}")
    if len(files) < 3 or len(parts) != 2:
        raise CellFail(f"fixture has {len(files)} files over partitions {parts}")
    got = sorted((int(r[0]), r[1], int(r[2])) for r in ctx.sql(f"SELECT id, region, val FROM {t}"))
    expect(ctx, "SQE reads the PyIceberg-written table", got, PYI_ROWS)
    # Predicate pushdown and projection, observed in EXPLAIN output.
    got = sorted((int(r[0]), int(r[1])) for r in ctx.sql(
        f"SELECT id, val FROM {t} WHERE region = 'a' AND val > 10"))
    expect(ctx, "filtered rows", got, [(2, 20), (5, 50)])
    line, text = explain_scan_line(ctx, f"SELECT id, val FROM {t} WHERE region = 'a' AND val > 10")
    ctx.note(f"EXPLAIN scan line (filter): {line}")
    if line is None:
        ctx.add_partial("EXPLAIN shows no Iceberg scan node: predicate pushdown not observable black box")
    else:
        if "predicate=[" not in line or "region" not in line or "val" not in line:
            raise CellFail(f"the filter does not reach the Iceberg scan: {line}")
        ctx.note("predicate pushdown: the filter is in the Iceberg scan's predicate")
    line, _ = explain_scan_line(ctx, f"SELECT id, val FROM {t}")
    ctx.note(f"EXPLAIN scan line (projection): {line}")
    if line is None:
        ctx.add_partial("EXPLAIN shows no Iceberg scan node: column projection not observable black box")
    else:
        if '"region"' in line or '"id"' not in line or '"val"' not in line:
            raise CellFail(f"the scan does not project only id and val: {line}")
        ctx.note('column projection: the scan reads only ["id", "val"]')
    # Scan planning: EXPLAIN ANALYZE reports the rows the scan node emitted.
    kept = scan_output_rows(ctx, f"SELECT id FROM {t} WHERE region = 'a'")
    none = scan_output_rows(ctx, f"SELECT id FROM {t} WHERE id > 100")
    ctx.note(f"EXPLAIN ANALYZE scan output rows: region = 'a' -> {kept} of 5; id > 100 -> {none}")
    if kept is None or none is None:
        ctx.add_partial("EXPLAIN ANALYZE shows no scan row count: scan planning not observable black box")
    elif kept != 3 or none != 0:
        raise CellFail(f"the scan emits {kept} rows for region = 'a' (3 expected) and {none} for "
                       f"id > 100 (0 expected): the filter is not applied at the scan")
    # Scan planning, file level: a second fixture SQE has never read. Remove one
    # region='b' data file from storage, so a scan that opens it fails. A
    # region='a' query that still succeeds proves the planner pruned the file
    # from manifest partition data; the full scan failing is the control.
    pt = pyiceberg_fixture(ctx, "fixture_prune", n)
    ptbl = ctx.load("fixture_prune")
    b_files = [e["data_file"]["file_path"] for e in ctx.load_raw("fixture_prune").data_files()
               if e["data_file"]["partition"]["region"] == "b"]
    if not b_files:
        raise CellFail("prune fixture has no region='b' data file")
    ptbl.io.delete(b_files[0])
    ctx.note(f"pruning probe: removed region='b' data file {b_files[0].rsplit('/', 1)[-1]} from storage")
    try:
        got = sorted(int(r[0]) for r in ctx.sql(f"SELECT id FROM {pt} WHERE region = 'a'"))
    except SqlError as e:
        raise CellFail(f"region = 'a' scan opened a region='b' file (no partition pruning): {e}")
    expect(ctx, "pruning probe: region = 'a' rows with a region='b' file missing", got, [1, 2, 5])
    try:
        # A column aggregate, not count(*): a count may one day be answered from metadata.
        ctx.sql(f"SELECT sum(val) FROM {pt}")
        ctx.add_partial("pruning probe control: a full scan did not fail on the missing file, "
                        "so the region = 'a' result does not prove file pruning")
    except SqlError as e:
        ctx.note(f"pruning probe control: full scan fails on the missing file ({str(e)[:120]}), "
                 "so the region = 'a' scan skipped it in planning")
    try:
        fl = ctx.flight().sql(f"SELECT id, region, val FROM {t}")
        fl = sorted((int(r[0]), r[1], int(r[2])) for r in fl)
        if fl != PYI_ROWS:
            ctx.add_protocol_gap(f"Flight SQL returns {fl} for the PyIceberg table, Trino HTTP returns {PYI_ROWS}")
    except SqlError as e:
        ctx.add_protocol_gap(f"Flight SQL cannot read the PyIceberg table: {e}")


@cell("read-support", "v2")
def read_support_v2(ctx):
    read_support_cell(ctx, 2)


@cell("read-support", "v3")
def read_support_v3(ctx):
    read_support_cell(ctx, 3)


def kv_pairs_independent(ctx, tbl, label):
    got = pairs(rows_of(tbl))
    ctx.note(f"{label} (PyIceberg scan): {got}")
    return got


def write_insert_cell(ctx, n):
    t = ctx.new_table("ins")
    ctx.sql(f"CREATE TABLE {t} (id BIGINT, v BIGINT) TBLPROPERTIES ('format-version' = '{n}')")
    ctx.sql(f"INSERT INTO {t} VALUES (1, 10), (2, 20)")
    src = ctx.new_table("ins_src")
    ctx.sql(f"CREATE TABLE {src} (id BIGINT, v BIGINT) TBLPROPERTIES ('format-version' = '{n}')")
    ctx.sql(f"INSERT INTO {src} VALUES (3, 30), (4, 40), (5, 50)")
    ctx.sql(f"INSERT INTO {t} SELECT id, v FROM {src}")
    tbl = ctx.load("ins")
    ctx.note(assert_format_version(tbl, n))
    ops = ctx.witness.snapshot_ops(tbl)
    ctx.note(f"snapshots (operation, added-records): "
             f"{[(o, s.get('added-records')) for _, o, s in ops]}")
    if [o for _, o, _ in ops] != ["append", "append"]:
        raise CellFail(f"snapshot operations {[o for _, o, _ in ops]}, expected two appends")
    if [s.get("added-records") for _, _, s in ops] != ["2", "3"]:
        raise CellFail(f"added-records {[s.get('added-records') for _, _, s in ops]}, expected 2 and 3")
    got = kv_pairs_independent(ctx, tbl, "rows after VALUES and SELECT inserts")
    if got != [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]:
        raise CellFail(f"PyIceberg reads {got} after the inserts")
    entries = ctx.witness.delete_entries(tbl)
    ctx.note(f"{entries['live_data_files']} data files, {entries['delete_manifests']} delete manifests (raw manifests)")
    cross = sqe_pairs(ctx, t)
    ctx.note(f"SQE cross-check: {cross}")


@cell("write-insert", "v2")
def write_insert_v2(ctx):
    write_insert_cell(ctx, 2)


@cell("write-insert", "v3")
def write_insert_v3(ctx):
    write_insert_cell(ctx, 3)


def write_merge_cell(ctx, n):
    t = ctx.new_table("mut")
    ctx.sql(f"CREATE TABLE {t} (id BIGINT, v BIGINT) TBLPROPERTIES ('format-version' = '{n}')")
    ctx.sql(f"INSERT INTO {t} VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)")
    ctx.sql(f"DELETE FROM {t} WHERE id = 1")
    tbl = ctx.load("mut")
    ctx.note(assert_format_version(tbl, n))
    expect(ctx, "after DELETE id=1 (PyIceberg scan)", pairs(rows_of(tbl)),
           [(2, 20), (3, 30), (4, 40), (5, 50), (6, 60)])
    ctx.sql(f"UPDATE {t} SET v = 222 WHERE id = 2")
    tbl = ctx.load("mut")
    expect(ctx, "after UPDATE id=2 (PyIceberg scan)", pairs(rows_of(tbl)),
           [(2, 222), (3, 30), (4, 40), (5, 50), (6, 60)])
    # One MERGE with all three clauses: matched update (3), matched delete (4), not matched insert (7).
    ctx.sql(f"MERGE INTO {t} t USING (SELECT 3 AS id, 333 AS v, false AS del "
            f"UNION ALL SELECT 4, 0, true UNION ALL SELECT 7, 70, false) s ON t.id = s.id "
            f"WHEN MATCHED AND s.del THEN DELETE "
            f"WHEN MATCHED THEN UPDATE SET v = s.v "
            f"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)")
    tbl = ctx.load("mut")
    ctx.note(assert_format_version(tbl, n))
    want = [(2, 222), (3, 333), (5, 50), (6, 60), (7, 70)]
    expect(ctx, "after MERGE (PyIceberg scan)", pairs(rows_of(tbl)), want)
    ops = [o for _, o, _ in ctx.witness.snapshot_ops(tbl)]
    ctx.note(f"snapshot operations (catalog metadata): {ops}")
    if len(ops) != 4:
        raise CellFail(f"expected four snapshots (insert, delete, update, merge), the catalog has {ops}")
    entries = ctx.witness.delete_entries(tbl)
    ctx.note(f"raw manifests: {entries['live_data_files']} live data files, "
             f"{entries['delete_manifests']} delete manifests (default write mode)")
    ctx.note(f"SQE cross-check: {sqe_pairs(ctx, t)}")


@cell("write-merge-update-delete", "v2")
def write_merge_v2(ctx):
    write_merge_cell(ctx, 2)


@cell("write-merge-update-delete", "v3")
def write_merge_v3(ctx):
    write_merge_cell(ctx, 3)


# ── statistics and bloom filters ───────────────────────────────────────────


def avro_kv(lst):
    """An Avro map-as-array of {key, value} -> {field id: bytes}."""
    return {x["key"]: x["value"] for x in (lst or [])}


def puffin_parts(raw, path):
    """(footer JSON, file bytes) of a Puffin file read by hand."""
    with raw.io.new_input(path).open() as f:
        data = f.read()
    if data[:4] != b"PFA1" or data[-4:] != b"PFA1":
        raise CellFail(f"{path} is not a Puffin file")
    flags = int.from_bytes(data[-8:-4], "little")
    size = int.from_bytes(data[-12:-8], "little")
    if flags & 1:
        raise CellFail("Puffin footer is compressed; not decoded by this witness")
    return json.loads(data[-12 - size:-12]), data


def statistics_cell(ctx, n):
    import struct

    t = ctx.new_table("stats")
    ctx.sql(f"CREATE TABLE {t} (id BIGINT, name VARCHAR, score DOUBLE) TBLPROPERTIES "
            f"('format-version' = '{n}', 'write.puffin.stats' = 'true')")
    ctx.sql(f"INSERT INTO {t} VALUES (1, 'a', 1.5), (2, NULL, 2.5), (3, 'c', NULL), (4, NULL, NULL)")
    raw = ctx.load_raw("stats")
    ctx.note(assert_raw_format_version(raw, n))
    ids = {f["name"]: f["id"] for f in raw.current_schema()["fields"]}
    files = raw.data_files()
    if len(files) != 1:
        raise CellFail(f"expected one data file, the manifests list {len(files)}")
    df = files[0]["data_file"]
    lo, hi = avro_kv(df.get("lower_bounds")), avro_kv(df.get("upper_bounds"))
    nulls, counts = avro_kv(df.get("null_value_counts")), avro_kv(df.get("value_counts"))
    got = {
        "id": (struct.unpack("<q", lo[ids["id"]])[0], struct.unpack("<q", hi[ids["id"]])[0]),
        "name": (lo[ids["name"]].decode(), hi[ids["name"]].decode()),
        "score": (struct.unpack("<d", lo[ids["score"]])[0], struct.unpack("<d", hi[ids["score"]])[0]),
    }
    ctx.note(f"manifest lower/upper bounds (raw Avro): {got}")
    if got != {"id": (1, 4), "name": ("a", "c"), "score": (1.5, 2.5)}:
        raise CellFail(f"manifest bounds {got}")
    nc = {k: nulls.get(v) for k, v in ids.items()}
    ctx.note(f"manifest null_value_counts: {nc}; value_counts: { {k: counts.get(v) for k, v in ids.items()} }")
    if nc != {"id": 0, "name": 2, "score": 2}:
        raise CellFail(f"manifest null counts {nc}, expected id 0, name 2, score 2")

    # Distinct counts: a Puffin statistics file with apache-datasketches-theta-v1 blobs.
    stats = raw.metadata.get("statistics") or []
    ctx.note(f"metadata `statistics` entries: {len(stats)}")
    if not stats:
        raise CellFail("table metadata lists no statistics file even with write.puffin.stats=true")
    snap = raw.current_snapshot()["snapshot-id"]
    entry = next((s for s in stats if s["snapshot-id"] == snap), None)
    if entry is None:
        raise CellFail(f"no statistics file for the current snapshot {snap}")
    footer, data = puffin_parts(raw, entry["statistics-path"])
    blobs = footer["blobs"]
    types = sorted({b["type"] for b in blobs})
    ctx.note(f"Puffin footer blob types (read by hand): {types}; {len(blobs)} blobs for fields "
             f"{[b['fields'] for b in blobs]}")
    if types != ["apache-datasketches-theta-v1"]:
        raise CellFail(f"Puffin blob types {types}, expected apache-datasketches-theta-v1")
    if sorted(f for b in blobs for f in b["fields"]) != sorted(ids.values()):
        raise CellFail("Puffin blobs do not cover every column")
    ndv = {}
    compact = True
    for b in blobs:
        body = data[b["offset"]:b["offset"] + b["length"]]
        name = next(k for k, v in ids.items() if v == b["fields"][0])
        ndv[name] = int(b.get("properties", {}).get("ndv", -1))
        ctx.note(f"blob for {name}: {len(body)} bytes for ndv {ndv[name]}, "
                 f"properties={sorted(b.get('properties', {}))}")
        # A DataSketches compact theta image starts with a preamble (preLongs, serVer=3,
        # family=3) and holds 16 bytes of preamble plus 8 bytes per retained hash.
        if not (len(body) >= 16 and body[1] == 3 and body[2] == 3):
            compact = False
        if len(body) == 8 * ndv[name]:
            ctx.note(f"  {name}: exactly 8 bytes per distinct value, a raw list of 64-bit hashes")
    ctx.note(f"ndv blob properties: {ndv}")
    if ndv != {"id": 4, "name": 2, "score": 2}:
        raise CellFail(f"distinct counts {ndv}, expected id 4, name 2, score 2 (nulls excluded)")
    if not compact:
        ctx.add_partial("the apache-datasketches-theta-v1 blob body is not the DataSketches compact "
                        "theta layout (serVer 3, family 3), so other engines cannot read the sketch")
    # The default: statistics are written only when asked for.
    d = ctx.new_table("stats_default")
    ctx.sql(f"CREATE TABLE {d} (id BIGINT) TBLPROPERTIES ('format-version' = '{n}')")
    ctx.sql(f"INSERT INTO {d} VALUES (1), (2)")
    dstats = ctx.load_raw("stats_default").metadata.get("statistics") or []
    ctx.note(f"default table (no write.puffin.stats): {len(dstats)} statistics files")
    if not dstats:
        ctx.add_partial("distinct-count Puffin statistics are opt-in (write.puffin.stats=true); "
                        "the default write has none")
    # Used for scan planning and predicate pushdown.
    line, _ = explain_scan_line(ctx, f"SELECT id FROM {t} WHERE id = 3")
    ctx.note(f"EXPLAIN scan line: {line}")
    if line is None or "predicate=[" not in line:
        ctx.add_partial("EXPLAIN does not show the predicate reaching the scan: not observable black box")
    # Whether the planner uses the distinct counts is not observable from outside;
    # that limits the suite, not SQE, so it is noted and not counted against the cell.
    ctx.note("use of the distinct counts in planning: not observable black box (not scored)")


@cell("statistics", "v2")
def statistics_v2(ctx):
    statistics_cell(ctx, 2)


@cell("statistics", "v3")
def statistics_v3(ctx):
    statistics_cell(ctx, 3)


def bloom_filters_cell(ctx, n):
    t = ctx.new_table("bloom")
    ctx.sql(f"CREATE TABLE {t} (id BIGINT, name VARCHAR) TBLPROPERTIES "
            f"('format-version' = '{n}', 'write.parquet.bloom-filter-enabled.column.id' = 'true')")
    ctx.sql(f"INSERT INTO {t} SELECT value AS id, 'n' || CAST(value AS VARCHAR) AS name FROM range(0, 500)")
    raw = ctx.load_raw("bloom")
    ctx.note(assert_raw_format_version(raw, n))
    ctx.note(f"table property in the catalog: "
             f"{ {k: v for k, v in raw.metadata['properties'].items() if 'bloom' in k} }")
    files = raw.data_files()
    if not files:
        raise CellFail("no data file written")
    present = {}
    for e in files:
        meta = raw.parquet_meta(e["data_file"]["file_path"])
        for rg in range(meta.num_row_groups):
            for ci in range(meta.num_columns):
                col = meta.row_group(rg).column(ci)
                present.setdefault(col.path_in_schema, []).append(col.bloom_filter_offset)
    ctx.note(f"Parquet footer bloom_filter_offset per column (read by pyarrow): {present}")
    if not present.get("id") or any(o is None for o in present["id"]):
        raise CellFail(f"no Parquet bloom filter on column id: {present}")
    if any(o is not None for o in present.get("name", [None])):
        raise CellFail(f"a bloom filter was written for a column that did not ask for one: {present}")
    # The rubric wording says Puffin: look there too.
    puffin = []
    for s in raw.metadata.get("statistics") or []:
        puffin += [b["type"] for b in s.get("blob-metadata", [])]
    ctx.note(f"blob types in the metadata statistics files: {puffin}")
    ctx.note('rubric wording: "Probabilistic data structure written in Puffin files". Measured: '
             "the bloom filter is in the Parquet footer (Iceberg's bloom-filter format), "
             "and no Puffin file holds one. The level follows the Parquet evidence.")
    ctx.note(f"SQE cross-check, point lookup: {ctx.sql(f'SELECT name FROM {t} WHERE id = 250')}")


@cell("bloom-filters", "v2")
def bloom_filters_v2(ctx):
    bloom_filters_cell(ctx, 2)


@cell("bloom-filters", "v3")
def bloom_filters_v3(ctx):
    bloom_filters_cell(ctx, 3)


# ── catalogs ───────────────────────────────────────────────────────────────


def catalog_cell(ctx, n, feature):
    cat = ctx.witness.catalog
    a, b = (f"matrix_{feature.replace('-', '_')}_{k}_{uuid.uuid4().hex[:6]}" for k in ("sqe", "pyi"))
    code, cfg = ctx.witness.rest_call("GET", "/namespaces")
    if code >= 400:
        raise CellFail(f"catalog namespace listing refused: {code} {cfg}")
    # SQE creates a namespace, PyIceberg sees it.
    ctx.sql(f"CREATE SCHEMA {ctx.catalog}.{a}")
    seen = {ns[0] for ns in cat.list_namespaces()}
    ctx.note(f"SQE CREATE SCHEMA {a}; PyIceberg list_namespaces sees it: {a in seen}")
    if a not in seen:
        raise CellFail("a namespace SQE created is not listed by PyIceberg's REST client")
    # PyIceberg creates one, SQE sees it.
    cat.create_namespace(b)
    names = {r[0] for r in ctx.sql(f"SHOW SCHEMAS FROM {ctx.catalog}")}
    ctx.note(f"PyIceberg create_namespace {b}; SQE SHOW SCHEMAS sees it: {b in names}")
    if b not in names:
        raise CellFail("a namespace PyIceberg created is not listed by SQE")
    # Drops, both directions.
    ctx.sql(f"DROP SCHEMA {ctx.catalog}.{a}")
    if a in {ns[0] for ns in cat.list_namespaces()}:
        raise CellFail("a namespace SQE dropped is still listed by PyIceberg")
    cat.drop_namespace(b)
    if b in {r[0] for r in ctx.sql(f"SHOW SCHEMAS FROM {ctx.catalog}")}:
        raise CellFail("a namespace PyIceberg dropped is still listed by SQE")
    ctx.note("both drops are visible to the other client")
    # A table PyIceberg created is readable by SQE (and is v2 or upgraded to v3).
    t = pyiceberg_fixture(ctx, "pyi_table", n)
    ctx.note(assert_raw_format_version(ctx.load_raw("pyi_table"), n))
    got = sorted((int(r[0]), r[1], int(r[2])) for r in ctx.sql(f"SELECT id, region, val FROM {t}"))
    expect(ctx, "SQE reads the PyIceberg-created table", got, PYI_ROWS)
    # SQE commits; the catalog shows a new metadata location and snapshot.
    before = ctx.load_raw("pyi_table")
    ctx.sql(f"INSERT INTO {t} VALUES (6, 'c', 60)")
    after = ctx.load_raw("pyi_table")
    ctx.note(f"metadata location before: ...{before.metadata_location[-48:]}")
    ctx.note(f"metadata location after the SQE INSERT: ...{after.metadata_location[-48:]}")
    if after.metadata_location == before.metadata_location:
        raise CellFail("the SQE commit did not change the catalog's metadata location")
    if len(after.snapshots()) != len(before.snapshots()) + 1:
        raise CellFail(f"snapshots {len(before.snapshots())} -> {len(after.snapshots())}, expected +1")
    seen = rows_of(ctx.load("pyi_table"))
    ctx.note(f"PyIceberg reads the SQE commit: {len(seen)} rows")
    if len(seen) != 6:
        raise CellFail(f"PyIceberg reads {len(seen)} rows after the SQE commit, expected 6")
    # Table-level metadata management: SQE DROP TABLE is visible, PyIceberg lists the table first.
    if "pyi_table" not in {tn[1] for tn in cat.list_tables(ctx.ns)}:
        raise CellFail("PyIceberg does not list the table")
    if feature == "rest-catalog":
        code, body = ctx.witness.rest_call("GET", f"/namespaces/{ctx.ns}/tables/pyi_table")
        keys = sorted(body) if isinstance(body, dict) else body
        ctx.note(f"raw REST loadTable answers {code} with keys {keys}")
        if code != 200 or "metadata-location" not in body or "metadata" not in body:
            raise CellFail("the REST loadTable response lacks metadata-location or metadata")
    ctx.sql(f"DROP TABLE {t}")
    if "pyi_table" in {tn[1] for tn in cat.list_tables(ctx.ns)}:
        raise CellFail("a table SQE dropped is still listed by the REST catalog")
    ctx.note("SQE DROP TABLE is visible to PyIceberg's REST client")


for _f in ("catalog-integration", "rest-catalog"):
    for _n in (2, 3):
        cell(_f, f"v{_n}")((lambda f, n: lambda ctx: catalog_cell(ctx, n, f))(_f, _n))


def _cloud(feature, provider):
    for _v in ("v2", "v3"):
        def run(ctx, p=provider):
            raise CellSkip(f"cloud catalog: needs {p} credentials; not run in the local suite")
        cell(feature, _v)(run)


_cloud("aws-glue-catalog", "AWS")
_cloud("unity-catalog", "Databricks")
_cloud("snowflake-horizon-catalog", "Snowflake")
_cloud("google-lakehouse", "Google Cloud")


# ── v3 types: variant, shredded variant, geometry, nanoseconds, unknown ────
# PyIceberg 0.12 cannot load a table whose schema holds one of these types, so the
# witnesses are the catalog's raw loadTable JSON, raw Avro manifests and Parquet.


def schema_type(raw, column):
    return next(f["type"] for f in raw.current_schema()["fields"] if f["name"] == column)


def field_id(raw, column):
    return next(f["id"] for f in raw.current_schema()["fields"] if f["name"] == column)


def variant_metadata_names(buf):
    """Dictionary strings of a Variant metadata value (Parquet-variant spec): header
    byte (version in the low 4 bits, offset size in the top 2 bits), dictionary size,
    offsets, then the string bytes."""
    version, osz = buf[0] & 0x0F, ((buf[0] >> 6) & 3) + 1
    if version != 1:
        raise CellFail(f"variant metadata version {version}, expected 1")
    size = int.from_bytes(buf[1:1 + osz], "little")
    offs = [int.from_bytes(buf[1 + osz * (i + 1):1 + osz * (i + 2)], "little") for i in range(size + 1)]
    base = 1 + osz * (size + 2)
    return [buf[base + offs[i]:base + offs[i + 1]].decode() for i in range(size)]


def variant_object_field_ids(buf):
    """Field ids (dictionary indices) of a Variant object value, or None if it is not
    an object. basic_type 2 = object."""
    if buf is None or (buf[0] & 3) != 2:
        return None
    vh = buf[0] >> 2
    off_sz, id_sz, large = (vh & 3) + 1, ((vh >> 2) & 3) + 1, (vh >> 4) & 1
    nsz = 4 if large else 1
    n = int.from_bytes(buf[1:1 + nsz], "little")
    p = 1 + nsz
    return [int.from_bytes(buf[p + i * id_sz:p + (i + 1) * id_sz], "little") for i in range(n)]


def variant_cell_rows(raw, column):
    """Rows of a table with one variant column read from Parquet: [(id, {metadata, value,
    typed_value?})] in file order."""
    out = []
    for e in raw.data_files():
        t = raw.read_parquet(e["data_file"]["file_path"])
        ids, vs = t.column("id").to_pylist(), t.column(column).to_pylist()
        out += list(zip(ids, vs))
    return sorted(out, key=lambda r: r[0])


@cell("variant-type", "v3")
def variant_type_v3(ctx):
    t = ctx.new_table("var")
    ctx.sql(f"CREATE TABLE {t} (id INT, v VARIANT) TBLPROPERTIES ('format-version' = '3')")
    ctx.sql(f"INSERT INTO {t} VALUES (1, '{{\"a\": 1, \"b\": \"x\"}}'), (2, '{{\"a\": 2, \"c\": [1, 2]}}')")
    raw = ctx.load_raw("var")
    ctx.note(assert_raw_format_version(raw, 3))
    ty = schema_type(raw, "v")
    ctx.note(f"raw metadata JSON type of column v: {ty!r}")
    if ty != "variant":
        raise CellFail(f"metadata type {ty!r}, expected 'variant'")
    path = raw.data_files()[0]["data_file"]["file_path"]
    pq_schema = raw.parquet_meta(path).schema
    ctx.note(f"Parquet schema of v: {[(pq_schema.column(i).path, str(pq_schema.column(i).physical_type)) for i in range(len(pq_schema)) if pq_schema.column(i).path.startswith('v')]}")
    paths = {pq_schema.column(i).path: str(pq_schema.column(i).physical_type) for i in range(len(pq_schema))}
    if paths.get("v.metadata") != "BYTE_ARRAY" or paths.get("v.value") != "BYTE_ARRAY":
        raise CellFail(f"Parquet does not hold the variant encoding (metadata and value binary): {paths}")
    rows = variant_cell_rows(raw, "v")
    names = []
    for rid, v in rows:
        n = variant_metadata_names(v["metadata"])
        obj = variant_object_field_ids(v["value"])
        if obj is None:
            raise CellFail(f"row {rid}: the value bytes are not a Variant object")
        names.append(sorted(n[i] for i in obj))
    ctx.note(f"Parquet metadata dictionaries decoded by hand; object keys per row: {names}")
    if names != [["a", "b"], ["a", "c"]]:
        raise CellFail(f"decoded variant keys {names}, expected [['a','b'],['a','c']]")
    # Field access through SQE (a cross-check: the witnesses above prove storage).
    got = [(int(r[0]), r[1]) for r in ctx.sql(
        f"SELECT id, variant_get(v, '$.a', 'int') FROM {t} ORDER BY id")]
    got = [(i, int(x)) for i, x in got]
    expect(ctx, "variant_get(v, '$.a') through SQE", got, [(1, 1), (2, 2)])
    ctx.note("'efficient field access without parsing' is not observable black box; the stored "
             "encoding above is the binary layout that allows it")


@cell("shredded-variant", "v3")
def shredded_variant_v3(ctx):
    t = ctx.new_table("shred")
    ctx.sql(f"CREATE TABLE {t} (id INT, v VARIANT) TBLPROPERTIES ('format-version' = '3', "
            f"'write.parquet.shred-variants' = 'true')")
    ctx.sql(f"INSERT INTO {t} VALUES (1, '{{\"a\": 1, \"b\": \"x\", \"c\": true}}'), "
            f"(2, '{{\"a\": 2, \"b\": \"y\", \"c\": false}}'), "
            f"(3, '{{\"a\": 3, \"b\": \"z\", \"c\": true, \"d\": 9}}')")
    raw = ctx.load_raw("shred")
    ctx.note(assert_raw_format_version(raw, 3))
    ctx.note(f"table property: {raw.metadata['properties'].get('write.parquet.shred-variants')}")
    path = raw.data_files()[0]["data_file"]["file_path"]
    sch = raw.parquet_meta(path).schema
    paths = {sch.column(i).path: str(sch.column(i).physical_type) for i in range(len(sch))}
    ctx.note(f"Parquet leaf columns under v: { {k: v for k, v in paths.items() if k.startswith('v')} }")
    typed = sorted(k for k in paths if k.startswith("v.typed_value."))
    if not typed:
        raise CellFail(f"no typed_value subtree in the Parquet schema: {sorted(paths)}")
    if "v.metadata" not in paths or "v.value" not in paths:
        raise CellFail(f"the variant lost its metadata or value column: {sorted(paths)}")
    shredded = sorted({k.split(".")[2] for k in typed})
    ctx.note(f"shredded fields (typed_value children): {shredded}")
    if not {"a", "b"} <= set(shredded):
        raise CellFail(f"fields a and b are not shredded: {shredded}")
    # The full variant is still kept: per row, the shredded typed columns plus the
    # residual `value` together carry every key of the original object.
    tbl = raw.read_parquet(path)
    rows = sorted(tbl.to_pylist(), key=lambda r: r["id"])
    originals = {1: {"a": 1, "b": "x", "c": True}, 2: {"a": 2, "b": "y", "c": False},
                 3: {"a": 3, "b": "z", "c": True, "d": 9}}
    for r in rows:
        v = r["v"]
        names = variant_metadata_names(v["metadata"])
        resid = variant_object_field_ids(v["value"])
        resid_keys = sorted(names[i] for i in resid) if resid else []
        in_typed = {}
        for f, leaf in (v["typed_value"] or {}).items():
            val = leaf["typed_value"]
            if val is not None:
                in_typed[f] = val.decode() if isinstance(val, bytes) else val
        ctx.note(f"row {r['id']}: typed_value {in_typed}; residual value keys {resid_keys}")
        want = originals[r["id"]]
        if sorted(set(in_typed) | set(resid_keys)) != sorted(want):
            raise CellFail(f"row {r['id']}: typed columns {sorted(in_typed)} and residual {resid_keys} "
                           f"do not cover the object's keys {sorted(want)}")
        for f, val in in_typed.items():
            if val != want[f]:
                raise CellFail(f"row {r['id']}: shredded {f} holds {val!r}, expected {want[f]!r}")
        if not set(want) <= set(names):
            raise CellFail(f"row {r['id']}: metadata dictionary {names} lacks keys of {sorted(want)}")
    got = [(int(r[0]), r[1]) for r in ctx.sql(f"SELECT id, CAST(variant_get(v, '$.c', 'boolean') AS VARCHAR) FROM {t} ORDER BY id")]
    ctx.note(f"SQE cross-check, full variant still readable through the shredded columns (field c): {got}")
    ctx.note("'faster queries' is not observable black box; the layout above is what allows it")


def wkb_point(x, y):
    import struct

    return struct.pack("<BIdd", 1, 1, x, y)


def wkb_decode(b):
    """Decode a little-endian WKB geometry into (type name, coordinates)."""
    import struct

    byte_order, gtype = b[0], struct.unpack("<I", b[1:5])[0]
    if byte_order != 1:
        raise CellFail("WKB is not little-endian")
    if gtype == 1:
        return "Point", [struct.unpack("<dd", b[5:21])]
    if gtype == 2:
        n = struct.unpack("<I", b[5:9])[0]
        return "LineString", [struct.unpack("<dd", b[9 + 16 * i:25 + 16 * i]) for i in range(n)]
    if gtype == 3:
        rings = struct.unpack("<I", b[5:9])[0]
        n = struct.unpack("<I", b[9:13])[0]
        return "Polygon", [struct.unpack("<dd", b[13 + 16 * i:29 + 16 * i]) for i in range(n)][:n] if rings == 1 else None
    return {4: "MultiPoint", 5: "MultiLineString", 6: "MultiPolygon", 7: "GeometryCollection"}.get(gtype, str(gtype)), None


@cell("geometry-type", "v3")
def geometry_type_v3(ctx):
    import struct

    t = ctx.new_table("geo")
    ctx.sql(f"CREATE TABLE {t} (id INT, g GEOMETRY(4326)) TBLPROPERTIES ('format-version' = '3')")
    # WKB (hex literal) and WKT, one INSERT per row (a mixed VALUES list is refused by SQE).
    ctx.sql(f"INSERT INTO {t} VALUES (1, X'{wkb_point(1.0, 2.0).hex()}')")
    ctx.sql(f"INSERT INTO {t} VALUES (2, ST_GeomFromText('POINT(5 6)'))")
    ctx.sql(f"INSERT INTO {t} VALUES (3, ST_GeomFromText('LINESTRING(-1 -2, 3 3)'))")
    ctx.sql(f"INSERT INTO {t} VALUES (4, ST_GeomFromText('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'))")
    ctx.sql(f"INSERT INTO {t} VALUES (5, ST_GeomFromText('MULTIPOINT((7 8), (9 10))'))")
    raw = ctx.load_raw("geo")
    ctx.note(assert_raw_format_version(raw, 3))
    ty = schema_type(raw, "g")
    ctx.note(f"raw metadata JSON type of column g: {ty!r} (declared GEOMETRY(4326))")
    if not str(ty).startswith("geometry"):
        raise CellFail(f"metadata type {ty!r}, expected geometry(...)")
    wkb, bounds = {}, []
    fid = field_id(raw, "g")
    for e in raw.data_files():
        df = e["data_file"]
        tbl = raw.read_parquet(df["file_path"])
        for i, g in zip(tbl.column("id").to_pylist(), tbl.column("g").to_pylist()):
            wkb[i] = g
        lo, hi = avro_kv(df.get("lower_bounds")).get(fid), avro_kv(df.get("upper_bounds")).get(fid)
        if lo and hi:
            bounds.append((struct.unpack("<dd", lo[:16]), struct.unpack("<dd", hi[:16])))
    decoded = {i: wkb_decode(b) for i, b in wkb.items()}
    ctx.note(f"Parquet WKB decoded by hand: { {i: d[0] for i, d in sorted(decoded.items())} }")
    want = {1: "Point", 2: "Point", 3: "LineString", 4: "Polygon", 5: "MultiPoint"}
    if {i: d[0] for i, d in decoded.items()} != want:
        raise CellFail(f"stored geometry types {({i: d[0] for i, d in sorted(decoded.items())})}, expected {want}")
    if decoded[1][1] != [(1.0, 2.0)] or decoded[2][1] != [(5.0, 6.0)]:
        raise CellFail(f"point coordinates {decoded[1]} {decoded[2]}")
    if decoded[3][1] != [(-1.0, -2.0), (3.0, 3.0)]:
        raise CellFail(f"linestring coordinates {decoded[3]}")
    if decoded[4][1] != [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]:
        raise CellFail(f"polygon ring {decoded[4]}")
    ctx.note(f"manifest geometry bounds per file (lower point, upper point): {bounds}")
    if not bounds:
        raise CellFail("the manifests carry no geometry bounds")
    lo = (min(b[0][0] for b in bounds), min(b[0][1] for b in bounds))
    hi = (max(b[1][0] for b in bounds), max(b[1][1] for b in bounds))
    ctx.note(f"bounds across files: lower {lo}, upper {hi}")
    if lo != (-1.0, -2.0) or hi != (9.0, 10.0):
        raise CellFail(f"geometry bounds {lo} {hi}, expected (-1, -2) and (9, 10)")
    got = [(int(r[0]), r[1]) for r in ctx.sql(f"SELECT id, ST_AsText(g) FROM {t} ORDER BY id")]
    ctx.note(f"SQE cross-check, ST_AsText: {got}")
    ctx.note("the declared SRID 4326 is stored in the table as the CRS name in the metadata type above")


@cell("nanosecond-timestamps", "v3")
def nanosecond_timestamps_v3(ctx):
    t = ctx.new_table("ns")
    ctx.sql(f"CREATE TABLE {t} (id INT, t TIMESTAMP_NS(9), tz TIMESTAMPTZ_NS(9)) "
            f"TBLPROPERTIES ('format-version' = '3')")
    ctx.sql(f"INSERT INTO {t} VALUES (1, TIMESTAMP '2026-01-01 00:00:00.123456789', "
            f"TIMESTAMP '2026-01-01 00:00:00.987654321 UTC')")
    ctx.sql(f"INSERT INTO {t} VALUES (2, CAST('2026-03-04 05:06:07.000000001' AS TIMESTAMP(9)), "
            f"CAST('2026-03-04 05:06:07.000000001+00:00' AS TIMESTAMP(9) WITH TIME ZONE))")
    raw = ctx.load_raw("ns")
    ctx.note(assert_raw_format_version(raw, 3))
    types = (schema_type(raw, "t"), schema_type(raw, "tz"))
    ctx.note(f"raw metadata JSON types: {types}")
    if types != ("timestamp_ns", "timestamptz_ns"):
        raise CellFail(f"metadata types {types}, expected timestamp_ns and timestamptz_ns")
    import datetime as dt

    def epoch_ns(day, frac):
        secs = int(dt.datetime.fromisoformat(day).replace(tzinfo=dt.timezone.utc).timestamp())
        return secs * 10**9 + frac

    want = {1: (epoch_ns("2026-01-01T00:00:00", 123456789), epoch_ns("2026-01-01T00:00:00", 987654321)),
            2: (epoch_ns("2026-03-04T05:06:07", 1), epoch_ns("2026-03-04T05:06:07", 1))}
    got, units = {}, set()
    for e in raw.data_files():
        pth = e["data_file"]["file_path"]
        sch = raw.parquet_meta(pth).schema
        for i in range(len(sch)):
            if sch.column(i).path in ("t", "tz"):
                units.add((sch.column(i).path, str(sch.column(i).physical_type), str(sch.column(i).logical_type)))
        tbl = raw.read_parquet(pth)
        for i, a, b in zip(tbl.column("id").to_pylist(), tbl.column("t").cast("int64").to_pylist(),
                           tbl.column("tz").cast("int64").to_pylist()):
            got[i] = (a, b)
    ctx.note(f"Parquet columns (name, physical, logical): {sorted(units)}")
    for name, phys, logical in units:
        utc = "true" if name == "tz" else "false"
        if phys != "INT64" or "timeUnit=nanoseconds" not in logical or f"isAdjustedToUTC={utc}" not in logical:
            raise CellFail(f"Parquet column {name} is {phys} {logical}, expected INT64 with "
                           f"nanosecond units and isAdjustedToUTC={utc}")
    ctx.note(f"Parquet values as int64 nanoseconds since the epoch: {got}")
    if got != want:
        raise CellFail(f"stored nanoseconds {got}, expected {want}")
    back = [(int(r[0]), r[1], r[2]) for r in ctx.sql(
        f"SELECT id, CAST(t AS VARCHAR), CAST(tz AS VARCHAR) FROM {t} ORDER BY id")]
    ctx.note(f"SQE cross-check: {back}")
    if "123456789" not in back[0][1] or "987654321" not in back[0][2] or "000000001" not in back[1][1]:
        raise CellFail(f"SQE reads the nanoseconds back truncated: {back}")


@cell("unknown-type", "v3")
def unknown_type_v3(ctx):
    t = ctx.new_table("unk")
    ctx.sql(f"CREATE TABLE {t} (id INT, u UNKNOWN) TBLPROPERTIES ('format-version' = '3')")
    ctx.sql(f"INSERT INTO {t} VALUES (1, NULL), (2, NULL)")
    raw = ctx.load_raw("unk")
    ctx.note(assert_raw_format_version(raw, 3))
    ty = schema_type(raw, "u")
    ctx.note(f"raw metadata JSON type of column u: {ty!r}")
    if ty != "unknown":
        raise CellFail(f"metadata type {ty!r}, expected 'unknown'")
    files = raw.data_files()
    cols = [raw.read_parquet(e["data_file"]["file_path"]).column_names for e in files]
    ctx.note(f"columns in the Parquet data files: {cols}")
    if any("u" in c for c in cols):
        raise CellFail(f"the unknown column is written to a data file: {cols}")
    before = sorted(e["data_file"]["file_path"] for e in files)
    # All values read NULL.
    nulls = ctx.sql(f"SELECT id, u IS NULL FROM {t} ORDER BY id")
    ctx.note(f"Trino HTTP: (id, u IS NULL) = {nulls}")
    if [(int(r[0]), r[1]) for r in nulls] != [(1, True), (2, True)]:
        raise CellFail(f"u is not NULL for every row: {nulls}")
    try:
        fl = ctx.flight().sql(f"SELECT id, u IS NULL FROM {t} ORDER BY id")
        ctx.note(f"Flight SQL: (id, u IS NULL) = {fl}")
        if [(int(r[0]), r[1]) for r in fl] != [(1, True), (2, True)]:
            ctx.add_protocol_gap(f"Flight SQL does not read u as NULL for every row: {fl}")
    except SqlError as e:
        ctx.add_protocol_gap(f"Flight SQL cannot read the unknown column: {e}")
    # Promote without rewriting files.
    ctx.sql(f"ALTER TABLE {t} ALTER COLUMN u SET DATA TYPE INT")
    raw2 = ctx.load_raw("unk")
    ty2 = schema_type(raw2, "u")
    after = sorted(e["data_file"]["file_path"] for e in raw2.data_files())
    ctx.note(f"after promotion: raw metadata type {ty2!r}, current-schema-id {raw2.metadata['current-schema-id']}; "
             f"data file paths unchanged: {before == after}")
    if ty2 != "int":
        raise CellFail(f"promoted type {ty2!r}, expected 'int'")
    if before != after:
        raise CellFail(f"promotion rewrote data files: {before} -> {after}")
    ctx.sql(f"INSERT INTO {t} VALUES (3, 30)")
    back = sorted((int(r[0]), None if r[1] is None else int(r[1])) for r in ctx.sql(f"SELECT id, u FROM {t}"))
    ctx.note(f"SQE cross-check after promotion and a new insert: {back}")
    if back != [(1, None), (2, None), (3, 30)]:
        raise CellFail(f"rows after promotion: {back}")
    raw3 = ctx.load_raw("unk")
    new = [e for e in raw3.data_files() if e["data_file"]["file_path"] not in before]
    cols = [raw3.read_parquet(e["data_file"]["file_path"]).column_names for e in new]
    ctx.note(f"the file written after promotion holds the column: {cols}")
    if not cols or "u" not in cols[0]:
        raise CellFail(f"the post-promotion file does not hold u: {cols}")


# ── lineage ────────────────────────────────────────────────────────────────


def lineage_rows(raw):
    """id -> (_row_id, _last_updated_sequence_number) per the v3 spec, from the raw
    manifests and Parquet. A materialized value wins; a null inherits: the row id is
    the file's first_row_id plus the row position, the sequence number is the file's
    data sequence number. A file's own first_row_id is null for a newly added file
    and then comes from the manifest-list first_row_id plus the rows of earlier added
    files in the same manifest."""
    from fastavro import reader

    out = {}
    for m in raw.manifest_list():
        if m.get("content", 0) != 0:
            continue
        counter = m.get("first_row_id")
        with raw.io.new_input(m["manifest_path"]).open() as f:
            entries = list(reader(f))
        for e in entries:
            if e["status"] == 2 or e["data_file"]["content"] != 0:
                continue
            df = e["data_file"]
            first = df.get("first_row_id")
            if first is None:
                if e["status"] != 1 or counter is None:
                    raise CellFail(f"cannot derive first_row_id for {df['file_path']}: "
                                   f"status {e['status']}, manifest first_row_id {counter}")
                first = counter
            if e["status"] == 1:
                counter = (counter or 0) + df["record_count"]
            seq = e.get("sequence_number")
            if seq is None:
                seq = m["sequence_number"]
            tbl = raw.read_parquet(df["file_path"])
            ids = tbl.column("id").to_pylist()
            rid = tbl.column("_row_id").to_pylist() if "_row_id" in tbl.column_names else [None] * len(ids)
            ls = (tbl.column("_last_updated_sequence_number").to_pylist()
                  if "_last_updated_sequence_number" in tbl.column_names else [None] * len(ids))
            for pos, i in enumerate(ids):
                out[i] = (first + pos if rid[pos] is None else rid[pos], seq if ls[pos] is None else ls[pos])
    return out


@cell("lineage", "v3")
def lineage_v3(ctx):
    t = ctx.new_table("lin")
    ctx.sql(f"CREATE TABLE {t} (id BIGINT, v BIGINT) TBLPROPERTIES ('format-version' = '3', {COW_PROPS})")
    ctx.sql(f"INSERT INTO {t} VALUES (1, 10), (2, 20), (3, 30)")
    raw1 = ctx.load_raw("lin")
    ctx.note(assert_raw_format_version(raw1, 3))
    nri1 = raw1.metadata.get("next-row-id")
    ml1 = [m.get("first_row_id") for m in raw1.manifest_list()]
    own = [e["data_file"].get("first_row_id") for e in raw1.data_files()]
    ctx.note(f"after INSERT of 3 rows: next-row-id={nri1}; manifest-list first_row_id={ml1}; "
             f"data-file first_row_id in the manifest={own} (null = inherited)")
    if nri1 != 3:
        raise CellFail(f"next-row-id {nri1} after inserting 3 rows, expected 3")
    if ml1 != [0]:
        raise CellFail(f"manifest-list first_row_id {ml1}, expected [0]")
    ctx.sql(f"INSERT INTO {t} VALUES (4, 40), (5, 50)")
    raw2 = ctx.load_raw("lin")
    nri2 = raw2.metadata.get("next-row-id")
    ctx.note(f"after a second INSERT of 2 rows: next-row-id={nri2}")
    if nri2 != 5:
        raise CellFail(f"next-row-id {nri2} after 5 rows in total, expected 5")
    before = lineage_rows(raw2)
    ctx.note(f"(_row_id, _last_updated_sequence_number) per id, derived per spec before the UPDATE: {before}")
    if sorted(r for r, _ in before.values()) != [0, 1, 2, 3, 4]:
        raise CellFail(f"row ids before the UPDATE {sorted(r for r, _ in before.values())}, expected 0..4")
    ctx.sql(f"UPDATE {t} SET v = 222 WHERE id = 2")
    raw3 = ctx.load_raw("lin")
    ctx.note(f"after the copy-on-write UPDATE: next-row-id={raw3.metadata.get('next-row-id')}, "
             f"snapshot sequence number {raw3.current_snapshot()['sequence-number']}")
    cur_seq = raw3.current_snapshot()["sequence-number"]
    mat = []
    for e in raw3.data_files():
        cols = raw3.read_parquet(e["data_file"]["file_path"]).column_names
        mat.append("_row_id" in cols and "_last_updated_sequence_number" in cols)
    ctx.note(f"rewritten data files carry the lineage columns in Parquet: {mat}")
    after = lineage_rows(raw3)
    ctx.note(f"(_row_id, _last_updated_sequence_number) per id after the UPDATE: {after}")
    if sorted(after) != [1, 2, 3, 4, 5]:
        raise CellFail(f"rows after the UPDATE: {sorted(after)}")
    for i in (1, 3, 4, 5):
        if after[i] != before[i]:
            raise CellFail(f"unchanged row {i} changed lineage: {before[i]} -> {after[i]}")
    if after[2][0] != before[2][0]:
        raise CellFail(f"the updated row lost its _row_id: {before[2]} -> {after[2]}")
    if after[2][1] != cur_seq or not after[2][1] > before[2][1]:
        raise CellFail(f"the updated row's _last_updated_sequence_number is {after[2][1]}, "
                       f"expected the new snapshot's {cur_seq} (was {before[2][1]})")
    ctx.note("updated row: _row_id kept, sequence number advanced; unchanged rows: both kept")


# ── run and report ─────────────────────────────────────────────────────────


def classify(cell_key, res, claims):
    """One report row. `agree` is None for unmeasured cells."""
    claimed = claims.get(f"{cell_key[0]}:{cell_key[1]}", {}).get("level", "none")
    measured = res.measured_level if res else None
    agree = None if measured is None else (measured == claimed)
    return {
        "feature": cell_key[0], "version": cell_key[1],
        "status": res.status if res else "untested",
        "claimed": claimed, "measured": measured or "unmeasured", "agree": agree,
        "partial_reason": res.partial_reason if res else "",
        "message": res.message if res else "no test registered for this rubric cell",
        "details": res.details if res else [], "seconds": res.seconds if res else 0.0,
        "protocol_gaps": res.protocol_gaps if res else [],
    }


def summarize(rows, total):
    measured = [r for r in rows if r["measured"] != "unmeasured"]
    s = {
        "total": total,
        "measured": len(measured),
        "agreed": sum(1 for r in measured if r["agree"]),
        "discrepancies": sum(1 for r in measured if not r["agree"]),
        "unmeasured": sum(1 for r in rows if r["status"] in ("skip", "error")),
        "errors": sum(1 for r in rows if r["status"] == "error"),
        "untested": sum(1 for r in rows if r["status"] == "untested"),
        "measured_score": sum(WEIGHT[r["measured"]] for r in measured),
        "claimed_score_on_measured": sum(WEIGHT[r["claimed"]] for r in measured),
        "claimed_score_all": sum(WEIGHT[r["claimed"]] for r in rows),
        "max_all": 3 * total,
    }
    s["max_measured"] = 3 * s["measured"]
    return s


def render_markdown(meta, s, rows):
    L = [
        "# SQE Iceberg feature-matrix suite report", "",
        f"**Measured {s['measured']} of {s['total']} cells.**", "",
        f"- SQE image: `{meta['sqe_image']}`" if meta.get("sqe_image") else "- SQE image: (not given)",
        f"- SQE version: {meta['sqe_version']}",
        f"- PyIceberg: {meta['pyiceberg']}",
        f"- Catalog and storage: `{meta.get('polaris_image', '?')}`, `{meta.get('rustfs_image', '?')}` "
        "(from POLARIS_IMAGE / RUSTFS_VERSION in the runner's environment)",
        "- SQE config: `tests/matrix/docker/sqe-config.toml`. Non-default: "
        "`maintenance.orphan_min_age_secs = 1`, so the orphan-file test finishes within the run "
        "(the default is one day).",
        f"- Run at: {meta['run_at']}", "",
        "| Measured | Agreed | Discrepancies | Unmeasured (skip/error) | Untested |",
        "|---|---|---|---|---|",
        f"| {s['measured']} | {s['agreed']} | {s['discrepancies']} | {s['unmeasured']} "
        f"({s['errors']} error) | {s['untested']} |", "",
        f"Score over measured cells: **{s['measured_score']} of {s['max_measured']}** "
        f"(claimed for the same cells: {s['claimed_score_on_measured']}). "
        f"Claimed score over all cells: {s['claimed_score_all']} of {s['max_all']}.", "",
        "## Measured cells", "",
        "| Cell | Claimed | Measured | Agree | Seconds |", "|---|---|---|---|---|",
    ]
    for r in rows:
        if r["measured"] != "unmeasured":
            L.append(f"| {r['feature']}:{r['version']} | {r['claimed']} | {r['measured']} | "
                     f"{'yes' if r['agree'] else 'NO'} | {r['seconds']} |")
    L += ["", "## Witness observations", ""]
    for r in rows:
        if r["status"] in ("pass", "fail", "error"):
            L.append(f"### {r['feature']}:{r['version']} ({r['status']})")
            if r["message"]:
                L.append(f"- message: {r['message']}")
            if r["partial_reason"]:
                L.append(f"- partial_reason: {r['partial_reason']}")
            L += [f"- {d}" for d in r["details"]]
            L.append("")
    disc = [r for r in rows if r["agree"] is False]
    L += ["## Proposed state-file edits (not applied)", ""]
    if disc:
        L += ["| Cell | Claimed | Measured | Reason |", "|---|---|---|---|"]
        for r in disc:
            L.append(f"| sqe:{r['feature']}:{r['version']} | {r['claimed']} | {r['measured']} | "
                     f"{(r['message'] or r['partial_reason']).replace('|', '/')} |")
    else:
        L.append("None.")
    gaps = [(r, g) for r in rows for g in r["protocol_gaps"]]
    L += ["", "## Protocol gaps (not counted in the level)", "",
          "Features that work over one SQE client protocol but not another. A cell's level is "
          "the engine's capability over any of its protocols; each gap here is an SQE bug.", ""]
    L += [f"- {r['feature']}:{r['version']}: {g}" for r, g in gaps] or ["None."]
    L += ["", "## Unmeasured cells", ""]
    skipped = [r for r in rows if r["status"] in ("skip", "error")]
    L += [f"- {r['feature']}:{r['version']} ({r['status']}): {r['message']}" for r in skipped] or ["None."]
    L += ["", f"## Untested cells ({s['untested']})", "",
          "Rubric cells with no registered test. Each one fails the run.", ""]
    L += [f"- {r['feature']}:{r['version']}" for r in rows if r["status"] == "untested"] or ["None."]
    L.append("")
    return "\n".join(L)


def sqe_version(sqe):
    try:
        r = requests.get(sqe.url.replace("/v1/statement", "/v1/info"), timeout=10).json()
        return r.get("nodeVersion", {}).get("version") or json.dumps(r)[:80]
    except Exception:  # noqa: BLE001
        return "unknown"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", default="target/matrix-suite", help="report directory")
    ap.add_argument("--only", action="append", default=[],
                    help="run only FEATURE or FEATURE:vN (repeatable); other cells stay untested")
    ap.add_argument("--rubric", default=str(RUBRIC_PATH))
    ap.add_argument("--state", default=str(STATE_PATH))
    args = ap.parse_args()

    cells, _ = load_rubric(args.rubric)
    claims, _state = load_claims(args.state)
    unknown = sorted(set(REGISTRY) - set(cells))
    if unknown:
        print(f"error: tests registered for cells outside the rubric: {unknown}", file=sys.stderr)
        return 2

    sqe = Sqe(env("SQE_HOST", "127.0.0.1"), env("SQE_PORT", "48080"), env("SQE_USER", "root"),
              env("SQE_PASSWORD", "s3cr3t"), env("SQE_CATALOG", "iceberg"))
    witness = Witness()

    def selected(key):
        return not args.only or any(o in (key[0], f"{key[0]}:{key[1]}") for o in args.only)

    rows = []
    for key in cells:
        fn = REGISTRY.get(key)
        res = None
        if fn and selected(key):
            print(f"[matrix] {key[0]}:{key[1]} ...", flush=True)
            res = run_cell(sqe, witness, key[0], key[1], fn)
            print(f"[matrix] {key[0]}:{key[1]} -> {res.status} {res.message}", flush=True)
        elif fn:
            res = TestResult("skip", message="not selected by --only")
        rows.append(classify(key, res, claims))

    s = summarize(rows, len(cells))
    meta = {
        "sqe_image": os.environ.get("SQE_IMAGE", ""), "sqe_version": sqe_version(sqe),
        "pyiceberg": pyiceberg_version(),
        # As start-sqe.sh resolves them; the compose defaults apply when unset.
        "polaris_image": os.environ.get("POLARIS_IMAGE", "apache/polaris:1.7.0"),
        "rustfs_image": f"rustfs/rustfs:{os.environ.get('RUSTFS_VERSION', '1.0.0-rc.6')}",
        "run_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ"),
    }
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps({"meta": meta, "summary": s, "cells": rows}, indent=2))
    md = render_markdown(meta, s, rows)
    (out / "report.md").write_text(md)
    print(md)
    ok = s["discrepancies"] == 0 and s["errors"] == 0 and s["untested"] == 0
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
