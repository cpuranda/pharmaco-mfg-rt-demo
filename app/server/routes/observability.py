"""Observability / architecture panel metrics."""
import os
import time
from datetime import datetime, timezone

from fastapi import APIRouter

from ..db import db
from ..warehouse import query as wh_query
from ..ingest import controller
from .. import assistant
from ..config import CATALOG, BRONZE, SILVER, GOLD, get_workspace_host, get_oauth_token

router = APIRouter()


@router.get("/observability/metrics")
async def metrics():
    out = {"server_ts": datetime.now(timezone.utc).isoformat()}

    # ---- ingestion (Zerobus) ----
    out["ingest"] = controller.status()

    # ---- bronze / pipeline volumes + freshness (Delta via warehouse) ----
    try:
        t0 = time.perf_counter()
        rows = await wh_query(
            f"SELECT "
            f"(SELECT count(*) FROM {CATALOG}.{BRONZE}.telemetry_raw) AS bronze_rows, "
            f"(SELECT count(*) FROM {CATALOG}.{SILVER}.telemetry_clean) AS silver_rows, "
            f"(SELECT count(*) FROM {CATALOG}.{SILVER}.telemetry_quarantine) AS quarantine_rows, "
            f"(SELECT count(*) FROM {CATALOG}.{GOLD}.asset_anomaly_events) AS anomaly_events, "
            f"(SELECT max(ingested_ts) FROM {CATALOG}.{BRONZE}.telemetry_raw) AS last_bronze_ts, "
            f"(SELECT max(processed_ts) FROM {CATALOG}.{SILVER}.telemetry_clean) AS last_silver_ts")
        out["delta"] = rows[0] if rows else {}
        for k in ("bronze_rows", "silver_rows", "quarantine_rows", "anomaly_events"):
            if out["delta"].get(k) is not None:
                try:
                    out["delta"][k] = int(out["delta"][k])
                except (TypeError, ValueError):
                    pass
        out["delta"]["query_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    except Exception as e:
        out["delta"] = {"error": str(e)}

    # ---- silver quality / latency window ----
    try:
        q = await wh_query(
            f"SELECT count(*) AS n, round(avg(ingest_latency_ms),1) AS avg_ingest_latency_ms, "
            f"sum(CASE WHEN is_out_of_band THEN 1 ELSE 0 END) AS oob "
            f"FROM {CATALOG}.{SILVER}.telemetry_clean "
            f"WHERE processed_ts >= current_timestamp() - INTERVAL 5 MINUTES")
        qm = q[0] if q else {}
        for k in ("n", "oob"):
            if qm.get(k) is not None:
                try: qm[k] = int(qm[k])
                except (TypeError, ValueError): pass
        if qm.get("avg_ingest_latency_ms") is not None:
            try: qm["avg_ingest_latency_ms"] = float(qm["avg_ingest_latency_ms"])
            except (TypeError, ValueError): pass
        out["quality_5m"] = qm
    except Exception as e:
        out["quality_5m"] = {"error": str(e)}

    # ---- Lakebase serving freshness ----
    try:
        t0 = time.perf_counter()
        row = await db.fetchrow(
            "SELECT max(synced_ts) AS last_sync, count(*) AS assets FROM asset_current_state")
        lat = round((time.perf_counter() - t0) * 1000, 1)
        last_sync = row["last_sync"] if row else None
        from ..sync import status as sync_status
        out["lakebase"] = {
            "serving_read_latency_ms": lat,
            "assets": row["assets"] if row else 0,
            "last_sync_ts": last_sync.isoformat() if isinstance(last_sync, datetime) else last_sync,
            "available": True,
            "serving_sync": sync_status(),
        }
    except Exception as e:
        out["lakebase"] = {"available": False, "error": str(e)}

    # ---- streaming job status ----
    out["streaming_job"] = await _streaming_job_status()

    # ---- agentic layer (Fix Assistant + multi-agent supervisor) ----
    out["agentic"] = await _agentic_status()

    # ---- architecture flow (static description for the UI panel) ----
    out["architecture"] = [
        {"stage": "Generator", "detail": "Synthetic plant telemetry (1 plant / 3 lines / 15 assets)"},
        {"stage": "Zerobus Ingest", "detail": f"{CATALOG}.{BRONZE}.telemetry_raw (managed Delta)"},
        {"stage": "Structured Streaming", "detail": "validate / dedup / threshold / anomaly / slowdown"},
        {"stage": "Gold Delta", "detail": "current state, alerts, line health, event history"},
        {"stage": "Lakebase", "detail": "operational serving + acknowledgements"},
        {"stage": "Databricks App", "detail": "operator + manager UI"},
    ]
    # ---- agentic flow (knowledge + agents layered on the serving tier) ----
    out["agentic_architecture"] = [
        {"stage": "SOP Knowledge", "detail": "Synthetic SOPs in a UC Volume → Delta chunks (CDF)"},
        {"stage": "Vector Search", "detail": "Delta-Sync index, managed embeddings (gte-large-en)"},
        {"stage": "Foundation Model", "detail": f"{assistant.LLM_ENDPOINT} — grounded, cited answers"},
        {"stage": "Fix Assistant", "detail": "Explain & Fix: SOP-grounded remediation per alert"},
        {"stage": "Supervisor", "detail": "Routes Genie (data) + SOP knowledge, then synthesizes"},
    ]
    return out


async def _agentic_status() -> dict:
    """Config + live health of the Fix Assistant / supervisor components."""
    out = dict(assistant.status())  # llm_endpoint, vs_index, supervisor mode, genie_configured
    # SOP corpus size (Delta chunks + distinct SOPs)
    try:
        table = assistant.VS_INDEX.rsplit(".", 1)[0] + ".sop_chunks"
        rows = await wh_query(f"SELECT count(*) AS chunks, count(DISTINCT sop_id) AS sops "
                              f"FROM {table}")
        r = rows[0] if rows else {}
        out["sop_chunks"] = int(r.get("chunks")) if r.get("chunks") is not None else None
        out["sop_count"] = int(r.get("sops")) if r.get("sops") is not None else None
    except Exception as e:
        out["sop_chunks"] = None
        out["sop_count"] = None
        out["sop_error"] = str(e)
    # Vector Search index readiness
    out["vector_search"] = await _vs_index_status(assistant.VS_INDEX)
    return out


async def _vs_index_status(index_fqn: str) -> dict:
    import aiohttp
    host = get_workspace_host(); token = get_oauth_token()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{host}/api/2.0/vector-search/indexes/{index_fqn}",
                             headers={"Authorization": f"Bearer {token}"},
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                d = await r.json()
        st = d.get("status", {}) or {}
        return {"ready": st.get("ready"),
                "state": st.get("detailed_state"),
                "indexed_rows": st.get("indexed_row_count")}
    except Exception as e:
        return {"error": str(e)}


async def _streaming_job_status() -> dict:
    import aiohttp
    job_id = os.environ.get("STREAMING_JOB_ID", "")
    if not job_id:
        return {"configured": False}
    host = get_workspace_host(); token = get_oauth_token()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{host}/api/2.2/jobs/runs/list",
                             headers={"Authorization": f"Bearer {token}"},
                             params={"job_id": job_id, "limit": 1},
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                d = await r.json()
        runs = d.get("runs", [])
        if not runs:
            return {"configured": True, "job_id": job_id, "state": "IDLE", "active": False}
        run = runs[0]
        st = run.get("state", {})
        return {"configured": True, "job_id": job_id,
                "life_cycle_state": st.get("life_cycle_state"),
                "result_state": st.get("result_state"),
                "active": st.get("life_cycle_state") in ("RUNNING", "PENDING")}
    except Exception as e:
        return {"configured": True, "job_id": job_id, "error": str(e)}
