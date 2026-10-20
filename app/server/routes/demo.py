"""Demo / scenario control endpoints (SYNTHETIC data generation)."""
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..db import db
from ..ingest import controller, VALID_SCENARIOS

router = APIRouter()

# Scenarios considered "high volume / multi-line" -> require explicit confirm
HIGH_IMPACT = {"alarm_burst", "line_slowdown"}


async def _persist_control(operator: str = "app"):
    """Mirror the in-app controller state to demo_control_state (best-effort)."""
    try:
        await db.execute(
            "INSERT INTO demo_control_state (id, scenario, run_state, event_rate, "
            "anomaly_secs, seed, updated_by, updated_ts) "
            "VALUES (1,$1,$2,$3,$4,$5,$6,$7) "
            "ON CONFLICT (id) DO UPDATE SET scenario=$1, run_state=$2, event_rate=$3, "
            "anomaly_secs=$4, seed=$5, updated_by=$6, updated_ts=$7",
            controller.scenario, "running" if controller.running else "stopped",
            controller.rate, controller.anomaly_secs, controller.seed,
            operator, datetime.now(timezone.utc))
    except Exception:
        pass  # control mirror is best-effort; not fatal


@router.get("/demo/status")
async def demo_status():
    st = controller.status()
    try:
        row = await db.fetchrow("SELECT * FROM demo_control_state WHERE id=1")
        if row and isinstance(row.get("updated_ts"), datetime):
            row["updated_ts"] = row["updated_ts"].isoformat()
        st["control_state"] = row
    except Exception as e:
        st["control_state"] = {"error": str(e)}
    return st


class ScenarioBody(BaseModel):
    scenario: str
    rate: float | None = Field(default=None, ge=0.1, le=20)
    seed: int | None = None
    anomaly_secs: int | None = Field(default=None, ge=5, le=3600)
    confirm_high_impact: bool = False
    operator: str = "operator"


@router.post("/demo/scenario")
async def set_scenario(body: ScenarioBody):
    if body.scenario not in VALID_SCENARIOS:
        raise HTTPException(422, f"invalid scenario; valid: {VALID_SCENARIOS}")
    if body.scenario in HIGH_IMPACT and not body.confirm_high_impact:
        raise HTTPException(409, {
            "error": "confirmation_required",
            "message": f"'{body.scenario}' is a high-volume / multi-line scenario. "
                       f"Resubmit with confirm_high_impact=true.",
        })
    # Do NOT forward `seed` here. Pinning a seed on scenario-apply makes every
    # subsequent restart re-emit identical source_event_ids that silver dedup drops,
    # freezing the Plant Overview / Alert Console. Leaving it unset lets the ingest
    # controller rotate the seed per start (its intended anti-dedup design). A pinned
    # seed for reproducible replay must go through an explicit path, not this one.
    controller.configure(scenario=body.scenario, rate=body.rate,
                         anomaly_secs=body.anomaly_secs)
    await _persist_control(body.operator)
    return controller.status()


class RunBody(BaseModel):
    operator: str = "operator"


@router.post("/demo/start")
async def start(body: RunBody):
    controller.start()
    await _persist_control(body.operator)
    return controller.status()


@router.post("/demo/stop")
async def stop(body: RunBody):
    controller.stop()
    await _persist_control(body.operator)
    return controller.status()
