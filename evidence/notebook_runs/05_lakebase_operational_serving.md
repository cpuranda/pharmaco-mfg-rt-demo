# Executed notebook: `05_lakebase_operational_serving`

> Exported from Databricks job run **24808449534624** / task run **358974856401335** — result **SUCCESS**, executed 2026-10-20T13:34:26+00:00 on serverless compute in workspace `https://fevm-hls-streaming-solution-demo.cloud.databricks.com`. Outputs below are the real cell outputs of that run.

# 05 · Lakebase — operational serving + transactional operator workflow

Delta gold is the analytical system of record; **Lakebase (managed Postgres)** holds the
small, hot, *operational* view the operator app reads in milliseconds and the
**transactional workflow state** it writes (alert acknowledgements, demo controls).

1. Provision the Lakebase instance (idempotent) and apply `lakebase/schema.sql`
2. Direct-write serving sync gold → Lakebase (`rt_pipeline._sync_to_lakebase`, the same
   upsert the app's background sync uses) — operator-owned columns are preserved
3. Operator **acknowledges** a CRITICAL alert in a Postgres transaction, then a second
   sync proves the acknowledgement **survives** pipeline upserts
4. Show that Lakebase access is governed by Databricks identities + OAuth (no static passwords)

```python
%pip install -q "psycopg[binary]==3.2.3"
%restart_python
```

**Output:**

```text
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

```python
%run ./_config
```

```python

```

**Output:**

```text
repo root : /Workspace/Users/chandrashekhar.purandare@databricks.com/pharmaco_mfg_rt_demo
catalog   : hls_streaming_solution_demo_catalog  schemas=['pharmaco_mfg_rt_bronze', 'pharmaco_mfg_rt_silver', 'pharmaco_mfg_rt_gold', 'pharmaco_mfg_rt_ldp', 'pharmaco_mfg_rt_ml', 'pharmaco_mfg_rt_knowledge']
run as    : chandrashekhar.purandare@databricks.com   at 2026-10-20T13:35:18+00:00
```

```python
banner("1. Lakebase instance")
def get_instance():
    try:
        return w.api_client.do("GET", f"/api/2.0/database/instances/{LAKEBASE_INSTANCE}")
    except Exception:
        return None

inst = get_instance()
if not inst:
    print(f"creating Lakebase instance {LAKEBASE_INSTANCE} (CU_1) ...")
    w.api_client.do("POST", "/api/2.0/database/instances",
                    body={"name": LAKEBASE_INSTANCE, "capacity": "CU_1"})
for _ in range(80):
    inst = get_instance()
    if inst and inst.get("state") == "AVAILABLE":
        break
    print(f"  state={inst.get('state') if inst else '?'}; waiting ...")
    time.sleep(15)
PGHOST = inst["read_write_dns"]
print(json.dumps({k: inst.get(k) for k in
                  ["name", "state", "capacity", "pg_version", "read_write_dns", "creator"]}, indent=1))
```

**Output:**

```text

==============================================================================
1. Lakebase instance
==============================================================================
{
 "name": "pharmaco-mfg-rt-demo-lb",
 "state": "AVAILABLE",
 "capacity": "CU_1",
 "pg_version": "PG_VERSION_16",
 "read_write_dns": "ep-steep-wildflower-d23qtgpw.database.us-east-1.cloud.databricks.com",
 "creator": "chandrashekhar.purandare@databricks.com"
}
```

```python
import psycopg

def pg_connect():
    cred = w.api_client.do("POST", "/api/2.0/database/credentials",
                           body={"request_id": str(uuid.uuid4()),
                                 "instance_names": [LAKEBASE_INSTANCE]})
    return psycopg.connect(host=PGHOST, port=5432, dbname=LAKEBASE_DB, user=ME,
                           password=cred["token"], sslmode="require", autocommit=True)

def pg_query(q, params=None):
    with pg_connect() as c, c.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute(q, params)
        rows = cur.fetchall() if cur.description else []
        cols = [d.name for d in cur.description] if cur.description else []
        ms = (time.perf_counter() - t0) * 1000
    return cols, rows, ms

def pg_show(q, params=None, title=None):
    cols, rows, ms = pg_query(q, params)
    if title:
        print(f"-- {title}  ({len(rows)} rows, {ms:.1f} ms)")
    if rows:
        display(spark.createDataFrame([[str(v) if v is not None else None for v in r] for r in rows],
                                      [str(c) for c in cols]))

banner("2. Apply Postgres operational schema (lakebase/schema.sql)")
ddl = open(os.path.join(REPO_ROOT, "lakebase", "schema.sql")).read()
with pg_connect() as c, c.cursor() as cur:
    cur.execute(ddl)
pg_show("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = %s ORDER BY table_name""", (PG_SCHEMA,), "tables in Lakebase")
```

**Output:**

```text

==============================================================================
2. Apply Postgres operational schema (lakebase/schema.sql)
==============================================================================
-- tables in Lakebase  (6 rows, 5.7 ms)
```

| table_name |
|---|
| active_alerts |
| asset_current_state |
| asset_ml_scores |
| demo_control_state |
| line_health_summary |
| operator_acknowledgements |

```python
banner("3. Direct-write serving sync: gold Delta → Lakebase")
import rt_pipeline
conf = {"lakebase_host": PGHOST, "lakebase_instance": LAKEBASE_INSTANCE, "lakebase_db": LAKEBASE_DB}
gold = {
    "current_state": spark.table(f"{CATALOG}.{GOLD}.asset_current_state"),
    "line_health": spark.table(f"{CATALOG}.{GOLD}.line_health_summary"),
    "alerts": spark.table(f"{CATALOG}.{GOLD}.active_alerts").filter("status <> 'RESOLVED'")
              .drop("acknowledged_by", "acknowledged_ts", "resolved_ts"),
}
rt_pipeline._sync_to_lakebase(gold, conf)
pg_show(f"""SELECT line_id, health_status, throughput_pct, open_warnings, open_criticals, slowdown_hint
            FROM {PG_SCHEMA}.line_health_summary ORDER BY line_id""", title="line health served from Lakebase")
pg_show(f"""SELECT alert_id, asset_id, sensor_type, severity, status, observed_value
            FROM {PG_SCHEMA}.active_alerts WHERE status <> 'RESOLVED'
            ORDER BY severity, asset_id""", title="open alerts served from Lakebase")
```

**Output:**

```text

==============================================================================
3. Direct-write serving sync: gold Delta → Lakebase
==============================================================================
[pipeline] lakebase direct-write: assets=15 lines=3 alerts=1 in 6851ms (serving write latency)
-- line health served from Lakebase  (3 rows, 4.0 ms)
```

| line_id | health_status | throughput_pct | open_warnings | open_criticals | slowdown_hint |
|---|---|---|---|---|---|
| LINE-A | CRITICAL | 106.6 | 0 | 1 | nominal |
| LINE-B | HEALTHY | 107.6 | 0 | 0 | nominal |
| LINE-C | HEALTHY | 106.8 | 0 | 0 | nominal |

```text
-- open alerts served from Lakebase  (1 rows, 3.2 ms)
```

| alert_id | asset_id | sensor_type | severity | status | observed_value |
|---|---|---|---|---|---|
| 584e1538ea51984bcdb817f54bc0695e7c42c31f9b8d0a9b5b3d24a80c589b19 | A-COMPRESS | temperature | CRITICAL | NEW | 77.208 |

```python
banner("4. Operator workflow — acknowledge the A-COMPRESS CRITICAL alert (one transaction)")
_, rows, _ = pg_query(f"""SELECT alert_id FROM {PG_SCHEMA}.active_alerts
                          WHERE asset_id='A-COMPRESS' AND sensor_type='temperature'
                            AND status <> 'RESOLVED' ORDER BY severity LIMIT 1""")
assert rows, "no open A-COMPRESS temperature alert in Lakebase"
alert_id = rows[0][0]
with pg_connect() as c:
    c.autocommit = False
    with c.cursor() as cur:
        cur.execute(f"""UPDATE {PG_SCHEMA}.active_alerts
                        SET status='ACKNOWLEDGED', acknowledged_by=%s, acknowledged_ts=now()
                        WHERE alert_id=%s""", ("operator.shift-a (notebook)", alert_id))
        cur.execute(f"""INSERT INTO {PG_SCHEMA}.operator_acknowledgements (alert_id, action, operator, note)
                        VALUES (%s, 'ACKNOWLEDGE', %s, %s)""",
                    (alert_id, "operator.shift-a (notebook)",
                     "Tablet press running hot - cooling check started per SOP-TEMP-014"))
    c.commit()
print(f"acknowledged alert {alert_id[:16]}…  (UPDATE + audit INSERT committed atomically)")

banner("5. Re-run the pipeline serving sync — acknowledgement must be preserved")
rt_pipeline._sync_to_lakebase(gold, conf)
cols, rows, ms = pg_query(f"""SELECT status, acknowledged_by, acknowledged_ts, synced_ts
                              FROM {PG_SCHEMA}.active_alerts WHERE alert_id=%s""", (alert_id,))
print(dict(zip(cols, rows[0])))
assert rows[0][0] == "ACKNOWLEDGED", "operator acknowledgement was clobbered by the sync!"
print("CHECK PASSED: operator state survived the pipeline upsert (synced_ts advanced, status kept).")
pg_show(f"SELECT * FROM {PG_SCHEMA}.operator_acknowledgements ORDER BY created_ts DESC LIMIT 5",
        title="audit log")
```

**Output:**

```text

==============================================================================
4. Operator workflow — acknowledge the A-COMPRESS CRITICAL alert (one transaction)
==============================================================================
acknowledged alert 584e1538ea51984b…  (UPDATE + audit INSERT committed atomically)

==============================================================================
5. Re-run the pipeline serving sync — acknowledgement must be preserved
==============================================================================
[pipeline] lakebase direct-write: assets=15 lines=3 alerts=1 in 2063ms (serving write latency)
{'status': 'ACKNOWLEDGED', 'acknowledged_by': 'operator.shift-a (notebook)', 'acknowledged_ts': datetime.datetime(2026, 10, 20, 13, 35, 33, 337252, tzinfo=zoneinfo.ZoneInfo(key='GMT')), 'synced_ts': datetime.datetime(2026, 10, 20, 13, 35, 35, 409179, tzinfo=zoneinfo.ZoneInfo(key='GMT'))}
CHECK PASSED: operator state survived the pipeline upsert (synced_ts advanced, status kept).
-- audit log  (1 rows, 3.5 ms)
```

| ack_id | alert_id | action | operator | note | created_ts |
|---|---|---|---|---|---|
| 1 | 584e1538ea51984bcdb817f54bc0695e7c42c31f9b8d0a9b5b3d24a80c589b19 | ACKNOWLEDGE | operator.shift-a (notebook) | Tablet press running hot - cooling check started per SOP-TEMP-014 | 2026-10-20 13:35:33.337252+00:00 |

```python
banner("6. Serving read latency (what the app pays per request)")
lat = []
with pg_connect() as c, c.cursor() as cur:
    for _ in range(20):
        t0 = time.perf_counter()
        cur.execute(f"SELECT * FROM {PG_SCHEMA}.asset_current_state WHERE line_id='LINE-A'")
        cur.fetchall()
        lat.append((time.perf_counter() - t0) * 1000)
lat.sort()
print(f"20 point reads of LINE-A asset state: p50={lat[10]:.1f} ms  p95={lat[18]:.1f} ms  max={lat[-1]:.1f} ms")
```

**Output:**

```text

==============================================================================
6. Serving read latency (what the app pays per request)
==============================================================================
20 point reads of LINE-A asset state: p50=3.4 ms  p95=6.6 ms  max=6.9 ms
```

```python
banner("7. Lakebase access is governed by Databricks identity (no static DB passwords)")
# Postgres roles map 1:1 to Databricks users / service principals; logins use short-lived
# OAuth tokens minted by /api/2.0/database/credentials (as above). Instance ACLs:
acl = w.api_client.do("GET", f"/api/2.0/permissions/database-instances/{LAKEBASE_INSTANCE}")
for e in acl.get("access_control_list", []):
    who = e.get("user_name") or e.get("service_principal_name") or e.get("group_name")
    print(f"  instance ACL: {who:<45} {[p['permission_level'] for p in e.get('all_permissions', [])]}")
pg_show("""SELECT rolname, rolcanlogin FROM pg_roles
           WHERE rolname LIKE '%@%' OR rolname ~ '^[0-9a-f]{8}-' ORDER BY rolname""",
        title="Databricks identities provisioned as Postgres roles")
cols, rows, _ = pg_query("SELECT current_user, now() - pg_postmaster_start_time() AS uptime")
print(f"connected as {rows[0][0]} using an OAuth token (expires ~1h); instance uptime {rows[0][1]}")
dbutils.jobs.taskValues.set("lakebase_host", PGHOST)
```

**Output:**

```text

==============================================================================
7. Lakebase access is governed by Databricks identity (no static DB passwords)
==============================================================================
  instance ACL: admins                                        ['CAN_MANAGE']
  instance ACL: chandrashekhar.purandare@databricks.com       ['CAN_MANAGE']
  instance ACL: users                                         ['CAN_CREATE']
-- Databricks identities provisioned as Postgres roles  (2 rows, 3.9 ms)
```

| rolname | rolcanlogin |
|---|---|
| b6a9206b-e130-42c5-8386-40b08a8eb73f | True |
| chandrashekhar.purandare@databricks.com | True |

```text
connected as chandrashekhar.purandare@databricks.com using an OAuth token (expires ~1h); instance uptime 2:09:47.484843
```
