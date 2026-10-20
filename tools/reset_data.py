#!/usr/bin/env python3
"""
Reset the demo's DATA layers so the build notebooks can be re-run from an empty state.

Drops: the LDP pipeline (and its tables), the bronze/silver/gold/ldp schemas (CASCADE,
incl. checkpoints volume) and the Lakebase operational schema. Keeps the long-lived
resources the notebooks update idempotently: SOP knowledge schema + Vector Search index,
UC model, Genie space, Agent Bricks agents, operational job, and the app.

  python tools/reset_data.py --yes
"""
import sys
import uuid

import psycopg
from databricks.sdk import WorkspaceClient

ROOT_CONF = dict(l.strip().split("=", 1) for l in open(__file__.rsplit("/tools/", 1)[0] + "/config.env")
                 if "=" in l and not l.lstrip().startswith("#"))
C = ROOT_CONF
if "--yes" not in sys.argv:
    sys.exit("refusing to run without --yes (drops demo data schemas)")
w = WorkspaceClient(profile=C["DBX_PROFILE"])

for p in w.pipelines.list_pipelines(filter="name LIKE 'pharmaco_mfg_rt_ldp_medallion'"):
    w.pipelines.delete(p.pipeline_id)
    print(f"deleted LDP pipeline {p.pipeline_id}")

for s in [C["DEMO_BRONZE"], C["DEMO_SILVER"], C["DEMO_GOLD"], C["DEMO_LDP_SCHEMA"]]:
    r = w.statement_execution.execute_statement(
        warehouse_id=C["DBX_WAREHOUSE_ID"], wait_timeout="50s",
        statement=f"DROP SCHEMA IF EXISTS {C['DEMO_CATALOG']}.{s} CASCADE")
    print(f"drop schema {s}: {r.status.state.value}")

inst = w.api_client.do("GET", f"/api/2.0/database/instances/{C['LAKEBASE_INSTANCE']}")
tok = w.api_client.do("POST", "/api/2.0/database/credentials",
                      body={"request_id": str(uuid.uuid4()), "instance_names": [C["LAKEBASE_INSTANCE"]]})["token"]
with psycopg.connect(host=inst["read_write_dns"], dbname=C["LAKEBASE_DB"], user=w.current_user.me().user_name,
                     password=tok, sslmode="require", autocommit=True) as c:
    c.execute(f"DROP SCHEMA IF EXISTS {C['LAKEBASE_APP_DB']} CASCADE")
    print(f"dropped Lakebase schema {C['LAKEBASE_APP_DB']}")
print("data layers reset")
