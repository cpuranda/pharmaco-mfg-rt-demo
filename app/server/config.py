"""App configuration + dual-mode auth (Databricks App vs local dev)."""
import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# Unity Catalog namespace (matches the deployed demo)
CATALOG = os.environ.get("DEMO_CATALOG", "hls_streaming_solution_demo_catalog")
BRONZE = os.environ.get("DEMO_BRONZE", "pharmaco_mfg_rt_bronze")
SILVER = os.environ.get("DEMO_SILVER", "pharmaco_mfg_rt_silver")
GOLD = os.environ.get("DEMO_GOLD", "pharmaco_mfg_rt_gold")
PG_SCHEMA = os.environ.get("PG_SCHEMA", "pharmaco_mfg_rt")

# Zerobus (used by the in-app ingest thread; optional/graceful)
ZB_ENDPOINT = os.environ.get("ZB_ENDPOINT", "")
ZB_CLIENT_ID = os.environ.get("ZB_CLIENT_ID", "")
ZB_CLIENT_SECRET = os.environ.get("ZB_CLIENT_SECRET", "")
BRONZE_TABLE = os.environ.get("BRONZE_TABLE", "telemetry_raw")

# Lakebase instance name (for token minting when PG* not injected, e.g. local)
LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE", "pharmaco-mfg-rt-demo-lb")


@lru_cache(maxsize=1)
def get_workspace_client() -> WorkspaceClient:
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "fe-vm-hls-streaming-solution-demo")
    return WorkspaceClient(profile=profile)


def get_oauth_token() -> str:
    """Workspace OAuth bearer token (for REST calls)."""
    w = get_workspace_client()
    hdrs = w.config.authenticate()
    if hdrs and "Authorization" in hdrs:
        return hdrs["Authorization"].replace("Bearer ", "")
    return w.config.token or ""


def get_workspace_host() -> str:
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    return get_workspace_client().config.host


def mint_lakebase_token() -> str:
    """Mint a short-lived Lakebase Postgres credential.

    In a Databricks App the injected workspace token is ALSO a valid Postgres
    password for the bound database resource, so we can use it directly. Locally
    (or if that fails) we mint an instance credential via the REST API."""
    if IS_DATABRICKS_APP:
        tok = os.environ.get("DATABRICKS_TOKEN") or get_oauth_token()
        if tok:
            return tok
    import uuid
    w = get_workspace_client()
    resp = w.api_client.do("POST", "/api/2.0/database/credentials",
                           body={"request_id": str(uuid.uuid4()),
                                 "instance_names": [LAKEBASE_INSTANCE]})
    return resp["token"] if isinstance(resp, dict) else resp.token


def pg_conn_params() -> dict:
    """Postgres connection params. PG* env vars are injected by the app's
    database resource binding; fall back to explicit config locally."""
    return {
        "host": os.environ.get("PGHOST", os.environ.get("LAKEBASE_HOST", "")),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", os.environ.get("LAKEBASE_DB", "databricks_postgres")),
        "user": os.environ.get("PGUSER", ""),
    }
