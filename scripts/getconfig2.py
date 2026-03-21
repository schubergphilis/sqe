#!/usr/bin/env python3
"""Dump the full Polaris configuration as JSON.

Mirrors what bootstrap-test.sh sets up:
  catalogs, principal roles, principals, catalog roles + grants,
  principal-role→catalog-role assignments, namespaces, tables, table metadata.

Usage:
    python3 scripts/getconfig.py
    python3 scripts/getconfig.py | jq .catalogs

Environment overrides (same defaults as bootstrap-test.sh):
    POLARIS_URL     http://localhost:8181
    CLIENT_ID       root
    CLIENT_SECRET   s3cr3t
    WAREHOUSE       test_warehouse
"""

import json
import os
import sys
import requests

POLARIS       = os.getenv("POLARIS_URL", "https://localhost")
KEYCLOAK       = os.getenv("POLARIS_URL", "https://auth.local")
CLIENT_ID     = os.getenv("CLIENT_ID",   "root")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "root123")
WAREHOUSE     = os.getenv("WAREHOUSE",   "main_warehouse")

# ── Auth ──────────────────────────────────────────────────────────────────────
resp = requests.post(
    f"{KEYCLOAK}/api/v1/oauth/tokens",
    data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "PRINCIPAL_ROLE:ALL",
    },
)
resp.raise_for_status()
token = resp.json()["access_token"]
h = {"Authorization": f"Bearer {token}"}


def get(path):
    r = requests.get(f"{POLARIS}{path}", headers=h)
    return r.json() if r.ok else {"_error": r.status_code, "_text": r.text}


config = {}

# ── Management API ────────────────────────────────────────────────────────────
config["catalogs"]        = get("/api/management/v1/catalogs")
config["principal_roles"] = get("/api/management/v1/principal-roles")
config["principals"]      = get("/api/management/v1/principals")

# Catalog roles + grants per catalog
config["catalog_roles"]       = {}
config["catalog_role_grants"] = {}
for catalog in config["catalogs"].get("catalogs", []):
    cname = catalog["name"]
    roles = get(f"/api/management/v1/catalogs/{cname}/catalog-roles")
    config["catalog_roles"][cname] = roles
    grants = {}
    for role in roles.get("roles", []):
        rname = role["name"]
        grants[rname] = get(f"/api/management/v1/catalogs/{cname}/catalog-roles/{rname}/grants")
    config["catalog_role_grants"][cname] = grants

# Principal-role → catalog-role assignments
config["principal_role_catalog_assignments"] = {}
for pr in config["principal_roles"].get("roles", []):
    prname = pr["name"]
    config["principal_role_catalog_assignments"][prname] = get(
        f"/api/management/v1/principal-roles/{prname}/catalog-roles/{WAREHOUSE}"
    )

# ── Iceberg REST (catalog) API ─────────────────────────────────────────────────
namespaces = get(f"/api/catalog/v1/{WAREHOUSE}/namespaces").get("namespaces", [])
config["namespaces"] = namespaces

config["tables"]         = {}
config["table_metadata"] = {}
for ns in namespaces:
    ns_name = ".".join(ns)
    tables_resp = get(f"/api/catalog/v1/{WAREHOUSE}/namespaces/{ns_name}/tables")
    identifiers = tables_resp.get("identifiers", [])
    config["tables"][ns_name] = identifiers
    for ident in identifiers:
        tname = ident["name"]
        config["table_metadata"][f"{ns_name}.{tname}"] = get(
            f"/api/catalog/v1/{WAREHOUSE}/namespaces/{ns_name}/tables/{tname}"
        )

print(json.dumps(config, indent=2))
