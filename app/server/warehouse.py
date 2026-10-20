"""Thin Databricks SQL Statement Execution client for Delta reads
(telemetry history + pipeline metrics). Uses the app/workspace OAuth token."""
import os
import ssl

import aiohttp

from .config import get_workspace_host, get_oauth_token

WAREHOUSE_ID = os.environ.get("DBX_WAREHOUSE_ID", "1b3fb1ab4eaa9511")
# Local-dev only: relax TLS verification when behind a TLS-inspecting corporate
# proxy (self-signed cert in chain). NEVER set this in the deployed app.
_INSECURE = os.environ.get("INSECURE_SSL", "").lower() == "true"


def _connector():
    if _INSECURE:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return aiohttp.TCPConnector(ssl=ctx)
    return None


async def query(sql: str, params: list | None = None) -> list[dict]:
    """Run a parameterized SQL statement; return list of dict rows.
    params use :name style? Here we use positional via named markers is not
    supported by the REST API cleanly, so callers pass fully-validated SQL with
    literal-safe, bounded inputs only."""
    host = get_workspace_host()
    token = get_oauth_token()
    body = {"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "30s"}
    if params:
        body["parameters"] = params
    async with aiohttp.ClientSession(connector=_connector()) as sess:
        async with sess.post(f"{host}/api/2.0/sql/statements",
                             headers={"Authorization": f"Bearer {token}"},
                             json=body, timeout=aiohttp.ClientTimeout(total=40)) as r:
            data = await r.json()
    state = data.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(f"SQL {state}: {data.get('status', {}).get('error', {})}")
    result = data.get("result", {})
    cols = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = result.get("data_array", []) or []
    return [dict(zip(cols, row)) for row in rows]
