"""Genie (AI/BI) conversation client — natural-language Q&A over the demo data.

Calls the Databricks Genie Conversation API with the app's service-principal
credentials. The Genie space is bound to the app as a resource; its id is read
from env (GENIE_SPACE_ID, or the injected resource env var)."""
import asyncio
import os
import time

import aiohttp

from .config import get_workspace_host, get_oauth_token
from .warehouse import _connector  # reuse local-dev SSL relaxation

# Resource binding injects the space id; allow explicit override via GENIE_SPACE_ID.
SPACE_ID = (os.environ.get("GENIE_SPACE_ID")
            or os.environ.get("GENIE_SPACE")
            or os.environ.get("DATABRICKS_GENIE_SPACE_ID", ""))

_TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}


def configured() -> bool:
    return bool(SPACE_ID)


async def _api(session, method, path, body=None):
    host = get_workspace_host()
    headers = {"Authorization": f"Bearer {get_oauth_token()}"}
    url = f"{host}{path}"
    async with session.request(method, url, headers=headers, json=body,
                               timeout=aiohttp.ClientTimeout(total=30)) as r:
        return await r.json()


async def _poll(session, cid, mid, timeout_s=90):
    deadline = time.time() + timeout_s
    last = {}
    while time.time() < deadline:
        m = await _api(session, "GET",
                       f"/api/2.0/genie/spaces/{SPACE_ID}/conversations/{cid}/messages/{mid}")
        last = m
        if m.get("status") in _TERMINAL:
            return m
        await asyncio.sleep(2)
    last["status"] = last.get("status") or "TIMEOUT"
    return last


async def _fetch_query_result(session, cid, mid, aid):
    try:
        r = await _api(session, "GET",
                       f"/api/2.0/genie/spaces/{SPACE_ID}/conversations/{cid}/messages/{mid}"
                       f"/attachments/{aid}/query-result")
        sr = r.get("statement_response", r)
        cols = [c["name"] for c in sr.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = (sr.get("result", {}) or {}).get("data_array", []) or []
        return {"columns": cols, "rows": rows[:100]}
    except Exception:
        return None


def _extract(msg: dict) -> dict:
    text, sql, aid = None, None, None
    for a in msg.get("attachments", []) or []:
        if a.get("text") and a["text"].get("content"):
            text = a["text"]["content"]
        if a.get("query"):
            sql = a["query"].get("query") or a["query"].get("description")
            aid = a.get("attachment_id") or a.get("id")
    return {"text": text, "sql": sql, "attachment_id": aid}


async def ask(question: str, conversation_id: str | None = None) -> dict:
    """Ask a question (new conversation) or follow up on an existing one.
    Returns {conversation_id, message_id, status, answer, sql, result}."""
    if not SPACE_ID:
        return {"status": "UNCONFIGURED",
                "answer": "Genie is not configured for this app (no space bound)."}
    async with aiohttp.ClientSession(connector=_connector()) as s:
        if conversation_id:
            r = await _api(s, "POST",
                           f"/api/2.0/genie/spaces/{SPACE_ID}/conversations/{conversation_id}/messages",
                           {"content": question})
            cid = conversation_id
            mid = r.get("message_id") or (r.get("message") or {}).get("id") or r.get("id")
        else:
            r = await _api(s, "POST",
                           f"/api/2.0/genie/spaces/{SPACE_ID}/start-conversation",
                           {"content": question})
            cid = r.get("conversation_id") or (r.get("conversation") or {}).get("id")
            mid = r.get("message_id") or (r.get("message") or {}).get("id")
        if not cid or not mid:
            return {"status": "ERROR", "answer": f"Genie did not start a conversation: {str(r)[:300]}"}
        msg = await _poll(s, cid, mid)
        parts = _extract(msg)
        result = None
        if parts["attachment_id"] and msg.get("status") == "COMPLETED":
            result = await _fetch_query_result(s, cid, mid, parts["attachment_id"])
        answer = parts["text"] or ("Query executed." if parts["sql"] else
                                   f"Genie returned status {msg.get('status')}.")
        return {"conversation_id": cid, "message_id": mid, "status": msg.get("status"),
                "answer": answer, "sql": parts["sql"], "result": result}
