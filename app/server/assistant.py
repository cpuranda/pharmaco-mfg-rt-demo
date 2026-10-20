"""
Fix Assistant — SOP-grounded remediation + lightweight multi-agent supervisor.
==============================================================================

Two capabilities, both grounded in governed sources:

  * explain_fix(alert)  — retrieves the relevant SOP sections from the Vector
    Search index and asks a Foundation Model to produce a cited, step-by-step
    remediation for a specific alert (asset / sensor / severity aware).

  * supervisor_ask(question) — a small supervisor that routes a free-text
    question to the right agent(s): the Genie space (structured live data) and/or
    the SOP Knowledge retrieval (unstructured), then synthesizes one grounded
    answer. This is the code-path mirror of an Agent Bricks Multi-Agent
    Supervisor; set SUPERVISOR_ENDPOINT to instead call a managed Agent Bricks
    Supervisor serving endpoint.

All calls use the app/workspace OAuth token (app service principal in the
deployed app). Everything degrades gracefully so the UI can show a clear state.
"""
from __future__ import annotations

import json
import os

import aiohttp

from . import genie
from .config import get_workspace_host, get_oauth_token
from .warehouse import _connector

# --- configuration (env-overridable) ---------------------------------------
LLM_ENDPOINT = os.environ.get("ASSISTANT_LLM_ENDPOINT", "databricks-claude-sonnet-4-5")
VS_INDEX = os.environ.get(
    "SOP_VS_INDEX",
    "hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_knowledge.sop_index")
# Optional: a managed Agent Bricks Multi-Agent Supervisor serving endpoint. When
# set, supervisor_ask() calls it instead of the code-path orchestration.
SUPERVISOR_ENDPOINT = os.environ.get("SUPERVISOR_ENDPOINT", "")

_TIMEOUT = aiohttp.ClientTimeout(total=60)


def status() -> dict:
    return {
        "llm_endpoint": LLM_ENDPOINT,
        "vs_index": VS_INDEX,
        "supervisor_endpoint": SUPERVISOR_ENDPOINT or None,
        "managed_supervisor": bool(SUPERVISOR_ENDPOINT),
        "genie_configured": genie.configured(),
    }


async def _post(session, path, body):
    host = get_workspace_host()
    async with session.post(f"{host}{path}",
                            headers={"Authorization": f"Bearer {get_oauth_token()}"},
                            json=body, timeout=_TIMEOUT) as r:
        text = await r.text()
        try:
            return r.status, json.loads(text)
        except Exception:
            return r.status, {"_raw": text}


async def _responses_call(session, endpoint: str, question: str, timeout_s: int = 170):
    """Call a Databricks Agent (agent/v1/responses) endpoint — e.g. a managed Agent
    Bricks Multi-Agent Supervisor. Returns (final_answer, tools_used)."""
    host = get_workspace_host()
    async with session.post(
            f"{host}/serving-endpoints/{endpoint}/invocations",
            headers={"Authorization": f"Bearer {get_oauth_token()}"},
            json={"input": [{"role": "user", "content": question}]},
            timeout=aiohttp.ClientTimeout(total=timeout_s)) as r:
        text = await r.text()
        if r.status != 200:
            raise RuntimeError(f"supervisor {endpoint} returned {r.status}: {text[:300]}")
        data = json.loads(text)
    out = data.get("output", []) or []
    tools = [o.get("name") for o in out
             if o.get("type") == "function_call" and o.get("name")]
    msgs = [o for o in out if o.get("type") == "message" and o.get("role") == "assistant"]
    answer = ""
    if msgs:  # the final assistant message is the synthesized answer
        answer = "".join(c.get("text", "") for c in msgs[-1].get("content", [])
                         if c.get("type") == "output_text")
    if not answer:
        answer = "".join(c.get("text", "") for o in out for c in o.get("content", [])
                         if c.get("type") == "output_text")
    return answer, tools


async def _chat(session, messages, max_tokens=700, temperature=0.1, endpoint=None) -> str:
    """Call a Databricks chat serving endpoint (OpenAI-style)."""
    ep = endpoint or LLM_ENDPOINT
    st, data = await _post(session, f"/serving-endpoints/{ep}/invocations",
                           {"messages": messages, "max_tokens": max_tokens,
                            "temperature": temperature})
    if st != 200:
        raise RuntimeError(f"LLM {ep} returned {st}: {str(data)[:300]}")
    # OpenAI-style chat response
    ch = (data.get("choices") or [{}])[0]
    msg = ch.get("message", {})
    if isinstance(msg.get("content"), list):  # some providers return content parts
        return "".join(part.get("text", "") for part in msg["content"])
    return msg.get("content") or ch.get("text") or ""


async def retrieve_sops(session, query: str, num_results: int = 5) -> list[dict]:
    """Semantic search over the SOP Vector Search index (managed embeddings)."""
    cols = ["chunk_id", "sop_id", "title", "revision", "section",
            "asset_types", "sensor_types", "scenarios", "source_file", "content"]
    st, data = await _post(session, f"/api/2.0/vector-search/indexes/{VS_INDEX}/query",
                           {"columns": cols, "query_text": query,
                            "num_results": num_results, "query_type": "ANN"})
    if st != 200:
        raise RuntimeError(f"vector-search query returned {st}: {str(data)[:300]}")
    manifest_cols = [c["name"] for c in
                     data.get("manifest", {}).get("columns", [])] or (cols + ["score"])
    rows = (data.get("result", {}) or {}).get("data_array", []) or []
    out = []
    for row in rows:
        rec = dict(zip(manifest_cols, row))
        # score is the trailing column when present
        if "score" not in rec and len(row) == len(cols) + 1:
            rec["score"] = row[-1]
        out.append(rec)
    return out


def _alert_query(alert: dict) -> str:
    """Build a retrieval query from an alert's context."""
    parts = [
        alert.get("sensor_type", ""), alert.get("alert_type", ""),
        alert.get("severity", ""), "on", alert.get("asset_id", ""),
        "line", alert.get("line_id", ""),
    ]
    q = " ".join(str(x) for x in parts if x)
    return (f"How to diagnose and fix a {alert.get('severity','')} "
            f"{alert.get('sensor_type','')} {alert.get('alert_type','')} issue "
            f"on {alert.get('asset_id','')} ({alert.get('line_id','')}). {q}")


def _citations(chunks: list[dict]) -> list[dict]:
    seen, cites = set(), []
    for c in chunks:
        key = c.get("sop_id")
        if key and key not in seen:
            seen.add(key)
            cites.append({"sop_id": c.get("sop_id"), "title": c.get("title"),
                          "section": c.get("section"), "source_file": c.get("source_file"),
                          "score": round(float(c.get("score", 0) or 0), 3)})
    return cites


FIX_SYSTEM = (
    "You are a pharmaceutical manufacturing operations assistant for a real-time line "
    "monitoring system. Using ONLY the provided SOP excerpts, give the operator a concise, "
    "actionable remediation for the specific alert. Structure the answer as: a one-line "
    "assessment, then 'Immediate actions' (numbered, from the SOPs), then 'Likely causes', "
    "then 'Escalation' if warranted. Cite the SOP id in square brackets, e.g. [SOP-TEMP-014], "
    "next to the steps that use it. If the SOPs do not cover the issue, say so plainly. "
    "Never invent procedures or thresholds that are not in the excerpts. All data is synthetic.")


async def explain_fix(alert: dict) -> dict:
    """SOP-grounded remediation for a specific alert."""
    async with aiohttp.ClientSession(connector=_connector()) as s:
        query = _alert_query(alert)
        try:
            chunks = await retrieve_sops(s, query, num_results=6)
        except Exception as e:
            return {"status": "RETRIEVAL_ERROR", "answer": f"SOP retrieval unavailable: {e}",
                    "citations": []}
        if not chunks:
            return {"status": "NO_MATCH",
                    "answer": "No SOP matched this alert. Check the knowledge base coverage.",
                    "citations": []}
        context = "\n\n---\n\n".join(
            f"[{c.get('sop_id')} · {c.get('section')}]\n{c.get('content','')}" for c in chunks)
        alert_desc = (
            f"Alert: {alert.get('severity','?')} {alert.get('alert_type','')} on asset "
            f"{alert.get('asset_id','?')} (line {alert.get('line_id','?')}). "
            f"Sensor={alert.get('sensor_type','?')}, observed={alert.get('observed_value','?')}, "
            f"threshold={alert.get('threshold_value','?')}, anomaly_score="
            f"{alert.get('anomaly_score','?')}, condition={alert.get('condition','?')}.")
        try:
            answer = await _chat(s, [
                {"role": "system", "content": FIX_SYSTEM},
                {"role": "user", "content": f"{alert_desc}\n\nSOP excerpts:\n{context}\n\n"
                                            f"Give the operator the remediation now."},
            ])
        except Exception as e:
            return {"status": "LLM_ERROR", "answer": f"Assistant model unavailable: {e}",
                    "citations": _citations(chunks)}
        return {"status": "OK", "answer": answer, "citations": _citations(chunks),
                "query": query}


# --------------------------------------------------------------------------- #
# Lightweight multi-agent supervisor (code path)
# --------------------------------------------------------------------------- #
PLANNER_SYSTEM = (
    "You route an operator's question to data/knowledge agents for a manufacturing "
    "monitoring app. Respond with STRICT JSON only: "
    '{"needs_data": bool, "needs_sops": bool, "genie_question": str, "sop_query": str}. '
    "needs_data=true when the answer depends on current/live plant state, metrics, alerts, "
    "or history (route to the Genie data agent). needs_sops=true when it asks how to fix, "
    "diagnose, what procedure/SOP applies, or best practice (route to SOP knowledge). "
    "genie_question is a self-contained rephrase for the data agent; sop_query is a search "
    "query for the SOP index. Leave a field empty string if not needed.")

SYNTH_SYSTEM = (
    "You are the supervisor for a pharma line-monitoring app. Combine the DATA findings "
    "(live plant data) and the SOP KNOWLEDGE excerpts into one clear answer for an operator. "
    "Lead with the direct answer. When you give remediation, cite SOP ids in [brackets]. "
    "Do not invent numbers not present in the data, or steps not in the SOPs. If a source "
    "was not needed or returned nothing, just use what you have. All data is synthetic.")


async def _plan(session, question: str) -> dict:
    try:
        raw = await _chat(session, [
            {"role": "system", "content": PLANNER_SYSTEM},
            {"role": "user", "content": question},
        ], max_tokens=200, temperature=0.0)
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.strip("`").split("\n", 1)[-1]
        plan = json.loads(raw)
    except Exception:
        # safe default: try both
        plan = {"needs_data": True, "needs_sops": True,
                "genie_question": question, "sop_query": question}
    plan.setdefault("genie_question", question)
    plan.setdefault("sop_query", question)
    return plan


async def supervisor_ask(question: str) -> dict:
    """Route to Genie (data) and/or SOP knowledge, then synthesize a grounded answer.

    If SUPERVISOR_ENDPOINT is set, defer to the managed Agent Bricks supervisor."""
    if SUPERVISOR_ENDPOINT:
        async with aiohttp.ClientSession(connector=_connector()) as s:
            try:
                answer, tools = await _responses_call(s, SUPERVISOR_ENDPOINT, question)
            except Exception as e:
                return {"status": "LLM_ERROR", "route": "managed-supervisor",
                        "answer": f"Managed supervisor unavailable: {e}"}
            return {"status": "OK", "answer": answer, "route": "managed-supervisor",
                    "tools_used": tools,
                    "sources": {"supervisor_endpoint": SUPERVISOR_ENDPOINT}}

    import asyncio
    async with aiohttp.ClientSession(connector=_connector()) as s:
        plan = await _plan(s, question)
        data_answer, sop_chunks = None, []

        async def _data():
            nonlocal data_answer
            if plan.get("needs_data") and genie.configured():
                try:
                    g = await genie.ask(plan.get("genie_question") or question)
                    data_answer = {"answer": g.get("answer"), "sql": g.get("sql"),
                                   "result": g.get("result"), "status": g.get("status")}
                except Exception as e:
                    data_answer = {"answer": f"(data agent error: {e})"}

        async def _sops():
            nonlocal sop_chunks
            if plan.get("needs_sops"):
                try:
                    sop_chunks = await retrieve_sops(s, plan.get("sop_query") or question, 6)
                except Exception:
                    sop_chunks = []

        await asyncio.gather(_data(), _sops())

        data_block = json.dumps(data_answer, default=str)[:2500] if data_answer else "(no data agent used)"
        sop_block = ("\n\n---\n\n".join(
            f"[{c.get('sop_id')} · {c.get('section')}]\n{c.get('content','')}" for c in sop_chunks)
            or "(no SOP knowledge used)")
        try:
            answer = await _chat(s, [
                {"role": "system", "content": SYNTH_SYSTEM},
                {"role": "user", "content": f"Operator question: {question}\n\n"
                                            f"DATA findings:\n{data_block}\n\n"
                                            f"SOP KNOWLEDGE:\n{sop_block}\n\nAnswer now."},
            ], max_tokens=900)
        except Exception as e:
            return {"status": "LLM_ERROR", "answer": f"Supervisor model unavailable: {e}",
                    "route": "code-supervisor", "plan": plan}
        return {"status": "OK", "answer": answer, "route": "code-supervisor", "plan": plan,
                "sources": {"data": data_answer, "sops": _citations(sop_chunks)}}
