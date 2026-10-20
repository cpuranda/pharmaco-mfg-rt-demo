"""Genie chat endpoints — natural-language Q&A over the demo data."""
from fastapi import APIRouter
from pydantic import BaseModel, Field

from .. import genie

router = APIRouter()

SUGGESTIONS = [
    "Which production lines are unhealthy right now?",
    "Which assets have the highest anomaly score?",
    "How many open critical alerts are there per line?",
    "Which line has the lowest throughput versus baseline?",
    "Show the most recent anomaly events and their severity",
    "Which assets are currently in a FAULT or non-running state?",
]


class AskBody(BaseModel):
    question: str = Field(min_length=1, max_length=1000)
    conversation_id: str | None = None


@router.get("/genie/status")
async def genie_status():
    return {"configured": genie.configured(), "suggestions": SUGGESTIONS}


@router.post("/genie/ask")
async def genie_ask(body: AskBody):
    return await genie.ask(body.question, body.conversation_id)
