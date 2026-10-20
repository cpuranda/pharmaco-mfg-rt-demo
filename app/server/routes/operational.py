"""Operational read endpoints + alert acknowledgement transactions."""
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..db import db, LakebaseUnavailable
from ..warehouse import query as wh_query
from ..config import CATALOG, SILVER

router = APIRouter()

SEVERITIES = {"WARNING", "CRITICAL"}
STATUSES = {"NEW", "ACKNOWLEDGED", "RESOLVED", "MAINTENANCE"}


def _iso(v):
    return v.isoformat() if isinstance(v, datetime) else v


def _clean(rows: list[dict]) -> list[dict]:
    for r in rows:
        for k, v in list(r.items()):
            if isinstance(v, datetime):
                r[k] = v.isoformat()
    return rows


@router.get("/plant/summary")
async def plant_summary():
    lines = _clean(await db.fetch("SELECT * FROM line_health_summary ORDER BY line_id"))
    assets = await db.fetch("SELECT operating_state, condition, active_alert_count, last_event_ts "
                            "FROM asset_current_state")
    alerts = await db.fetch("SELECT severity, status FROM active_alerts "
                            "WHERE status NOT IN ('RESOLVED')")
    total = len(assets)
    running = sum(1 for a in assets if a["operating_state"] == "RUNNING")
    idle = sum(1 for a in assets if a["operating_state"] == "IDLE")
    faulted = sum(1 for a in assets if a["operating_state"] == "FAULT")
    latest = max([a["last_event_ts"] for a in assets if a["last_event_ts"]], default=None)
    open_warn = sum(1 for a in alerts if a["severity"] == "WARNING")
    open_crit = sum(1 for a in alerts if a["severity"] == "CRITICAL")
    status = "HEALTHY"
    if any(l["health_status"] == "CRITICAL" for l in lines) or open_crit:
        status = "CRITICAL"
    elif any(l["health_status"] in ("DEGRADED", "WATCH") for l in lines) or open_warn:
        status = "DEGRADED"
    return {
        "plant_status": status,
        "total_assets": total, "running_assets": running,
        "idle_assets": idle, "faulted_assets": faulted,
        "open_warnings": open_warn, "open_criticals": open_crit,
        "lines": len(lines),
        "latest_event_ts": _iso(latest),
        "server_ts": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/lines")
async def list_lines():
    return _clean(await db.fetch("SELECT * FROM line_health_summary ORDER BY line_id"))


@router.get("/lines/{line_id}")
async def get_line(line_id: str):
    line = await db.fetchrow("SELECT * FROM line_health_summary WHERE line_id = $1", line_id)
    if not line:
        raise HTTPException(404, f"line {line_id} not found")
    assets = await db.fetch("SELECT * FROM asset_current_state WHERE line_id = $1 "
                            "ORDER BY anomaly_score DESC, asset_id", line_id)
    return {"line": _clean([line])[0], "assets": _clean(assets)}


@router.get("/assets/{asset_id}")
async def get_asset(asset_id: str):
    a = await db.fetchrow("SELECT * FROM asset_current_state WHERE asset_id = $1", asset_id)
    if not a:
        raise HTTPException(404, f"asset {asset_id} not found")
    alerts = await db.fetch("SELECT * FROM active_alerts WHERE asset_id = $1 "
                            "AND status <> 'RESOLVED' ORDER BY severity", asset_id)
    return {"asset": _clean([a])[0], "alerts": _clean(alerts), "ml": await _ml_score(asset_id)}


async def _ml_score(asset_id: str) -> dict | None:
    """Latest ML anomaly-detector score for an asset (written by the ML scoring task)."""
    try:
        row = await db.fetchrow("SELECT * FROM asset_ml_scores WHERE asset_id = $1", asset_id)
    except Exception:  # table absent before the first scoring run
        return None
    return _clean([row])[0] if row else None


@router.get("/ml/scores")
async def ml_scores():
    """Latest ML anomaly score per asset, most anomalous first."""
    try:
        return _clean(await db.fetch("SELECT * FROM asset_ml_scores "
                                     "ORDER BY ml_anomaly_score DESC, asset_id"))
    except Exception as e:
        raise HTTPException(503, f"ML scores unavailable: {e}")


@router.get("/telemetry/{asset_id}")
async def telemetry(asset_id: str,
                    minutes: int = Query(15, ge=1, le=180),
                    sensor: str | None = Query(None)):
    """Recent telemetry history for charts (from silver Delta via SQL warehouse)."""
    # asset_id/sensor are parameterized; minutes is bounded int.
    sql = (f"SELECT asset_id, sensor_type, event_ts, sensor_value, unit, "
           f"nominal_min, nominal_max, operating_state "
           f"FROM {CATALOG}.{SILVER}.telemetry_clean "
           f"WHERE asset_id = :asset_id "
           f"AND event_ts >= current_timestamp() - INTERVAL {int(minutes)} MINUTES ")
    params = [{"name": "asset_id", "value": asset_id, "type": "STRING"}]
    if sensor:
        sql += "AND sensor_type = :sensor "
        params.append({"name": "sensor", "value": sensor, "type": "STRING"})
    sql += "ORDER BY event_ts LIMIT 5000"
    try:
        rows = await wh_query(sql, params)
    except Exception as e:
        raise HTTPException(503, f"telemetry source unavailable: {e}")
    # The SQL warehouse returns all values as strings; coerce numeric fields so the
    # frontend charts get real numbers (avoids sensor_value.toFixed crashes).
    def _num(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None
    for r in rows:
        for k in ("sensor_value", "nominal_min", "nominal_max"):
            if k in r:
                r[k] = _num(r[k])
    return {"asset_id": asset_id, "points": rows, "minutes": minutes}


@router.get("/alerts")
async def list_alerts(severity: str | None = Query(None),
                      status: str | None = Query(None),
                      line_id: str | None = Query(None),
                      asset_id: str | None = Query(None),
                      alert_type: str | None = Query(None),
                      limit: int = Query(100, ge=1, le=500),
                      offset: int = Query(0, ge=0)):
    where, args = [], []
    def add(col, val):
        args.append(val); where.append(f"{col} = ${len(args)}")
    if severity:
        if severity not in SEVERITIES: raise HTTPException(422, "bad severity")
        add("severity", severity)
    if status:
        if status not in STATUSES: raise HTTPException(422, "bad status")
        add("status", status)
    if line_id: add("line_id", line_id)
    if asset_id: add("asset_id", asset_id)
    if alert_type: add("alert_type", alert_type)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    args.extend([limit, offset])
    sql = (f"SELECT * FROM active_alerts {clause} "
           f"ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 ELSE 1 END, last_event_ts DESC "
           f"LIMIT ${len(args)-1} OFFSET ${len(args)}")
    return _clean(await db.fetch(sql, *args))


class AckBody(BaseModel):
    operator: str = Field(min_length=1, max_length=120)
    note: str | None = Field(default=None, max_length=2000)


async def _record_ack(alert_id: str, action: str, body: AckBody, new_status: str):
    existing = await db.fetchrow("SELECT alert_id FROM active_alerts WHERE alert_id = $1", alert_id)
    if not existing:
        raise HTTPException(404, f"alert {alert_id} not found")
    now = datetime.now(timezone.utc)
    if action == "ACKNOWLEDGE":
        await db.execute("UPDATE active_alerts SET status=$2, acknowledged_by=$3, "
                         "acknowledged_ts=$4, updated_ts=$4 WHERE alert_id=$1",
                         alert_id, new_status, body.operator, now)
    else:  # RESOLVE / MAINTENANCE
        await db.execute("UPDATE active_alerts SET status=$2, resolved_ts=$3, updated_ts=$3, "
                         "acknowledged_by=COALESCE(acknowledged_by,$4) WHERE alert_id=$1",
                         alert_id, new_status, now, body.operator)
    await db.execute("INSERT INTO operator_acknowledgements (alert_id, action, operator, note) "
                     "VALUES ($1,$2,$3,$4)", alert_id, action, body.operator, body.note)
    row = await db.fetchrow("SELECT * FROM active_alerts WHERE alert_id=$1", alert_id)
    return _clean([row])[0]


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge(alert_id: str, body: AckBody):
    return await _record_ack(alert_id, "ACKNOWLEDGE", body, "ACKNOWLEDGED")


class ResolveBody(AckBody):
    maintenance: bool = False


@router.post("/alerts/{alert_id}/resolve")
async def resolve(alert_id: str, body: ResolveBody):
    status = "MAINTENANCE" if body.maintenance else "RESOLVED"
    action = "MAINTENANCE" if body.maintenance else "RESOLVE"
    return await _record_ack(alert_id, action, body, status)


@router.get("/alerts/{alert_id}/history")
async def alert_history(alert_id: str):
    return _clean(await db.fetch(
        "SELECT * FROM operator_acknowledgements WHERE alert_id=$1 ORDER BY created_ts DESC",
        alert_id))
