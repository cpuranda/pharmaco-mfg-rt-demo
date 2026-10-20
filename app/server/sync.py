"""
Gold -> Lakebase serving sync (runs inside the app as a periodic async task).

Reads the small current operational view from GOLD Delta (via the SQL warehouse)
and upserts it into Lakebase Postgres for low-latency serving. This is the
DIRECT-WRITE serving path, decoupled from the Spark stream so serving freshness is
independent of the streaming cluster environment. Operator-owned columns on
active_alerts (status / acknowledged_* / resolved_ts) are PRESERVED on upsert.

Managed alternative (documented, not used here): Lakebase *synced tables*
(continuous UC Delta -> Postgres, ~15s). We use direct write for lower latency and
because it lets the pipeline/app cooperate on operator-owned alert state.
"""
import asyncio
import time

from .warehouse import query as wh_query
from .db import db
from .config import CATALOG, GOLD

# (column, postgres cast) per target table — warehouse returns all values as text,
# so we cast in SQL. Order matters (matches the SELECT).
ACS = [("asset_id", "text"), ("site_id", "text"), ("plant_id", "text"), ("line_id", "text"),
       ("last_event_ts", "timestamptz"), ("updated_ts", "timestamptz"),
       ("operating_state", "text"), ("temperature", "float8"), ("vibration", "float8"),
       ("pressure", "float8"), ("motor_current", "float8"), ("speed", "float8"),
       ("throughput", "float8"), ("energy", "float8"), ("anomaly_score", "float8"),
       ("condition", "text"), ("active_alert_count", "int")]
LHS = [("line_id", "text"), ("site_id", "text"), ("plant_id", "text"),
       ("product_family", "text"), ("updated_ts", "timestamptz"), ("health_status", "text"),
       ("running_assets", "int"), ("idle_assets", "int"), ("faulted_assets", "int"),
       ("total_assets", "int"), ("avg_throughput", "float8"), ("baseline_throughput", "float8"),
       ("throughput_pct", "float8"), ("avg_speed", "float8"), ("open_warnings", "int"),
       ("open_criticals", "int"), ("is_slowing_down", "bool"), ("slowdown_hint", "text")]
ALERTS = [("alert_id", "text"), ("created_ts", "timestamptz"), ("updated_ts", "timestamptz"),
          ("site_id", "text"), ("plant_id", "text"), ("line_id", "text"), ("asset_id", "text"),
          ("alert_type", "text"), ("severity", "text"), ("sensor_type", "text"),
          ("observed_value", "float8"), ("threshold_value", "float8"),
          ("anomaly_score", "float8"), ("confidence", "float8"),
          ("recommended_action", "text"), ("root_cause_hint", "text"),
          ("last_event_ts", "timestamptz"), ("status", "text")]

_last_sync_ms = 0.0
_last_error: str | None = None
_last_ok_ts: float = 0.0


def _upsert_sql(table, cols, pk, preserve=()):
    names = [c for c, _ in cols]
    # bind every param as text (warehouse returns strings; asyncpg accepts str for
    # text), then cast text->target type in SQL so Postgres does the conversion.
    vals = ",".join(f"${i+1}::text::{t}" for i, (_, t) in enumerate(cols))
    upd = [f"{c}=EXCLUDED.{c}" for c in names if c not in pk and c not in preserve]
    upd.append("synced_ts=now()")
    return (f"INSERT INTO {table} ({','.join(names)}) VALUES ({vals}) "
            f"ON CONFLICT ({','.join(pk)}) DO UPDATE SET {','.join(upd)}")


async def _sync_table(gold_table, pg_table, cols, pk, preserve=()):
    names = [c for c, _ in cols]
    rows = await wh_query(f"SELECT {','.join(names)} FROM {CATALOG}.{GOLD}.{gold_table}")
    if not rows:
        return 0
    sql = _upsert_sql(pg_table, cols, pk, preserve)
    pool = await db.get_pool()
    if pool is None:
        raise RuntimeError("Lakebase pool unavailable")
    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in rows:
                await conn.execute(sql, *[r.get(c) for c in names])
    return len(rows)


def _alerts_upsert_sql():
    """active_alerts upsert. Alert-derived fields always refreshed. Status reconciles
    the operator-owned serving row with the pipeline's gold status (EXCLUDED):

      * status='NEW'        -> follow the pipeline (incl. auto-RESOLVE when the breach
                               clears in gold).
      * status='RESOLVED'   -> RE-FIRE back to NEW when gold reports NEW again (the
                               same alert is breaching once more). Without this the
                               alert stays stuck 'RESOLVED' in the serving layer and
                               the asset's "Active alerts" panel shows nothing despite
                               an active breach. Mirrors the gold-Delta merge.
      * ACKNOWLEDGED / MAINTENANCE -> operator-owned; always preserved.

    On a RESOLVED->NEW re-fire the now-stale resolution/ack metadata is cleared."""
    names = [c for c, _ in ALERTS]
    vals = ",".join(f"${i+1}::text::{t}" for i, (_, t) in enumerate(ALERTS))
    derived = ["created_ts", "updated_ts", "site_id", "plant_id", "line_id", "asset_id",
               "alert_type", "severity", "sensor_type", "observed_value", "threshold_value",
               "anomaly_score", "confidence", "recommended_action", "root_cause_hint",
               "last_event_ts"]
    sets = [f"{c}=EXCLUDED.{c}" for c in derived]
    sets.append("synced_ts=now()")
    _REFIRE = "active_alerts.status='RESOLVED' AND EXCLUDED.status='NEW'"
    sets.append("status = CASE "
                "WHEN active_alerts.status='NEW' THEN EXCLUDED.status "
                f"WHEN {_REFIRE} THEN 'NEW' "
                "ELSE active_alerts.status END")
    sets.append("resolved_ts = CASE "
                "WHEN active_alerts.status='NEW' AND EXCLUDED.status='RESOLVED' THEN now() "
                f"WHEN {_REFIRE} THEN NULL "
                "ELSE active_alerts.resolved_ts END")
    sets.append(f"acknowledged_by = CASE WHEN {_REFIRE} THEN NULL "
                "ELSE active_alerts.acknowledged_by END")
    sets.append(f"acknowledged_ts = CASE WHEN {_REFIRE} THEN NULL "
                "ELSE active_alerts.acknowledged_ts END")
    return (f"INSERT INTO active_alerts ({','.join(names)}) VALUES ({vals}) "
            f"ON CONFLICT (alert_id) DO UPDATE SET {','.join(sets)}")


async def _sync_alerts():
    names = [c for c, _ in ALERTS]
    rows = await wh_query(f"SELECT {','.join(names)} FROM {CATALOG}.{GOLD}.active_alerts")
    if not rows:
        return 0
    sql = _alerts_upsert_sql()
    pool = await db.get_pool()
    if pool is None:
        raise RuntimeError("Lakebase pool unavailable")
    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in rows:
                await conn.execute(sql, *[r.get(c) for c in names])
    return len(rows)


async def sync_once() -> dict:
    global _last_sync_ms, _last_error, _last_ok_ts
    t0 = time.perf_counter()
    try:
        # run the three gold reads + upserts concurrently (warehouse round-trips dominate)
        a, l, al = await asyncio.gather(
            _sync_table("asset_current_state", "asset_current_state", ACS, ["asset_id"]),
            _sync_table("line_health_summary", "line_health_summary", LHS, ["line_id"]),
            _sync_alerts(),
        )
        _last_sync_ms = (time.perf_counter() - t0) * 1000
        _last_error = None
        _last_ok_ts = time.time()
        return {"assets": a, "lines": l, "alerts": al, "sync_ms": round(_last_sync_ms, 1)}
    except Exception as e:
        _last_error = f"{type(e).__name__}: {e}"
        raise


def status() -> dict:
    return {"last_sync_ms": round(_last_sync_ms, 1), "last_error": _last_error,
            "seconds_since_ok": round(time.time() - _last_ok_ts, 1) if _last_ok_ts else None}


async def sync_loop(interval: float = 5.0):
    """Background task: periodically push gold -> Lakebase."""
    # small initial delay so the app is serving before first sync
    await asyncio.sleep(3)
    while True:
        try:
            await sync_once()
        except Exception as e:  # keep serving even if a sync cycle fails
            print(f'{{"level":"WARN","logger":"serving-sync","msg":"sync failed: {e}"}}')
        await asyncio.sleep(interval)
