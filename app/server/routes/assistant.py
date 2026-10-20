"""Fix Assistant endpoints — SOP-grounded remediation + multi-agent supervisor."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .. import assistant
from ..db import db

router = APIRouter()

SUPERVISOR_SUGGESTIONS = [
    "Why is LINE-A degraded right now and what should I do?",
    "A-COMPRESS is running hot — what does the SOP say to do?",
    "Which assets have the highest anomaly score, and how do I fix the worst one?",
    "How do I handle an alarm burst on the sterile line?",
    "What is the return-to-service procedure after maintenance?",
]


@router.get("/assistant/status")
async def assistant_status():
    return {**assistant.status(), "suggestions": SUPERVISOR_SUGGESTIONS}


class AlertContext(BaseModel):
    alert_id: str | None = None
    asset_id: str | None = None
    line_id: str | None = None
    sensor_type: str | None = None
    alert_type: str | None = None
    severity: str | None = None
    observed_value: float | None = None
    threshold_value: float | None = None
    anomaly_score: float | None = None
    condition: str | None = None


@router.post("/assistant/explain-fix")
async def explain_fix(ctx: AlertContext):
    alert = ctx.model_dump()
    # If only an alert_id was supplied, hydrate the context from the serving table.
    if ctx.alert_id and not ctx.sensor_type:
        try:
            row = await db.fetchrow(
                "SELECT a.alert_id, a.asset_id, a.line_id, a.sensor_type, a.alert_type, "
                "a.severity, a.observed_value, a.threshold_value, a.anomaly_score, "
                "s.condition FROM active_alerts a "
                "LEFT JOIN asset_current_state s ON s.asset_id = a.asset_id "
                "WHERE a.alert_id = $1", ctx.alert_id)
            if row:
                alert.update({k: v for k, v in row.items() if v is not None})
        except Exception:
            pass  # fall back to whatever context was provided
    if not (alert.get("sensor_type") or alert.get("asset_id")):
        raise HTTPException(422, "provide an alert_id or at least asset_id/sensor_type")
    return await assistant.explain_fix(alert)


class AskBody(BaseModel):
    question: str = Field(min_length=1, max_length=1000)


@router.post("/assistant/ask")
async def assistant_ask(body: AskBody):
    return await assistant.supervisor_ask(body.question)
