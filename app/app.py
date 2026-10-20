"""
PharmaCo Manufacturing Real-Time demo — Databricks App (FastAPI).

Serves the operator/manager UI (React) and a JSON API backed by Lakebase
(operational state) and Delta (telemetry history / pipeline metrics). Drives
synthetic ingestion via Zerobus. All data is SYNTHETIC.
"""
import logging
import os
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import asyncio
import os

from server.db import db
from server.routes import operational, demo, observability, genie, assistant
from server import sync as serving_sync

# ---- structured logging ----
logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
log = logging.getLogger("nvs-mfg-app")

app = FastAPI(title="PharmaCo MFG Real-Time Demo", version="1.0.0")


@app.on_event("startup")
async def _start_serving_sync():
    """Background gold->Lakebase serving sync (interval from env, default 5s).
    Set SERVING_SYNC_ENABLED=false to disable (e.g. if the pipeline writes Lakebase)."""
    if os.environ.get("SERVING_SYNC_ENABLED", "true").lower() != "true":
        log.info("serving-sync disabled")
        return
    interval = float(os.environ.get("SERVING_SYNC_INTERVAL", "5"))
    asyncio.create_task(serving_sync.sync_loop(interval))
    log.info(f"serving-sync started (interval={interval}s)")


@app.middleware("http")
async def request_logging(request: Request, call_next):
    rid = request.headers.get("x-request-id", uuid.uuid4().hex[:12])
    t0 = time.perf_counter()
    try:
        resp = await call_next(request)
    except Exception as e:  # pragma: no cover
        dt = (time.perf_counter() - t0) * 1000
        log.error(f"request_id={rid} {request.method} {request.url.path} "
                  f"ERROR {type(e).__name__}: {e} latency_ms={dt:.1f}")
        return JSONResponse(status_code=500,
                            content={"error": "internal_error", "request_id": rid})
    dt = (time.perf_counter() - t0) * 1000
    resp.headers["x-request-id"] = rid
    log.info(f"request_id={rid} {request.method} {request.url.path} "
             f"status={resp.status_code} latency_ms={dt:.1f}")
    return resp


app.include_router(operational.router, prefix="/api")
app.include_router(demo.router, prefix="/api")
app.include_router(observability.router, prefix="/api")
app.include_router(genie.router, prefix="/api")
app.include_router(assistant.router, prefix="/api")


@app.get("/health")
async def health():
    """Liveness — process is up."""
    return {"status": "ok", "service": "pharmaco-mfg-rt-demo"}


@app.get("/ready")
async def ready():
    """Readiness — can we reach Lakebase?"""
    pool = await db.get_pool()
    if pool is None:
        return JSONResponse(status_code=503, content={
            "status": "degraded", "lakebase": False,
            "reason": db.unavailable_reason})
    try:
        await db.fetchrow("SELECT 1 AS ok")
        return {"status": "ready", "lakebase": True}
    except Exception as e:
        return JSONResponse(status_code=503, content={
            "status": "degraded", "lakebase": False, "reason": str(e)})


# ---- serve React frontend (built to frontend/dist) ----
_frontend = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.isdir(_frontend):
    _assets = os.path.join(_frontend, "assets")
    if os.path.isdir(_assets):
        app.mount("/assets", StaticFiles(directory=_assets), name="assets")

    @app.get("/{full_path:path}")
    async def spa(full_path: str):
        # never shadow the API
        if full_path.startswith("api/"):
            return JSONResponse(status_code=404, content={"error": "not_found"})
        idx = os.path.join(_frontend, "index.html")
        if os.path.isfile(idx):
            return FileResponse(idx)
        return JSONResponse(status_code=404, content={"error": "frontend_not_built"})
else:
    @app.get("/")
    async def root():
        return {"service": "pharmaco-mfg-rt-demo",
                "note": "frontend not built; API under /api", "docs": "/docs"}
