# Redeploying the PharmaCo MFG Real-Time demo to another workspace

This package stands up the entire demo — Unity Catalog namespace, Lakebase, Zerobus
ingest, the medallion streaming job, the Vector Search SOP index, the Agent Bricks
Knowledge Assistant + Multi-Agent Supervisor, and the Databricks App — from a single
config file and a phased orchestrator.

```
deploy/
  deploy.config.env          # EDIT THIS — every workspace-specific value
  deploy.sh                  # phased, idempotent orchestrator
  templates/
    app.yaml.tmpl            # app env, rendered from config
    streaming_job.json.tmpl  # streaming job, rendered from config
  .deploy.state.env          # (auto) discovered values: Lakebase DNS, app SP, endpoints, job id
```

## What it deploys (recap)

Synthetic generator → **Zerobus** → bronze Delta → **Structured Streaming** (medallion)
→ gold Delta → **Lakebase** serving → **Databricks App** (React + FastAPI), plus a
**Fix Assistant** (Vector Search over synthetic SOPs + a Foundation Model) and a managed
**Agent Bricks** Knowledge Assistant + Multi-Agent Supervisor wired to a Genie space.

## Prerequisites

**On your machine**
- Databricks CLI (recent), authenticated to the target workspace: `databricks auth login --profile <name>`
- `jq` and `envsubst` (`brew install jq gettext`)
- Node + npm (to build the app frontend)
- A Python with `databricks-sdk`, `psycopg[binary]`, `protobuf` installed. Point the
  orchestrator at it with `export DEPLOY_PYTHON=/path/to/venv/bin/python` (it defaults to
  the repo's `.venv-introspect/bin/python`, else `python3`).

**In the target workspace (entitlements)**
- Unity Catalog (ability to create a catalog, or an existing catalog you name in config)
- A serverless **SQL warehouse** (put its id in config)
- **Lakebase**, **Vector Search**, **Model Serving / Foundation Model APIs**, **Databricks Apps**,
  and **Agent Bricks** (Beta) enabled
- Permission to create a **secret scope**

**Two things the orchestrator does NOT fully create for you** (set them up, then put the
ids in `deploy.config.env`):
1. **Zerobus service principal + OAuth secret** — create an SP, generate an OAuth secret,
   put the SP's application id in `ZB_CLIENT_ID`/`DEMO_SP_CLIENT_ID`, and either
   `export ZB_CLIENT_SECRET=<secret>` before the `secret` phase or store it manually.
   The SP needs write access to the bronze table.
2. **Genie space** — curate a Genie space over the 4 gold tables
   (`asset_current_state`, `line_health_summary`, `active_alerts`, `asset_anomaly_events`),
   then set `GENIE_SPACE_ID`. If left blank, the supervisor runs SOP-only and the app's
   Ask tab still works; add Genie later by setting the id and re-running `agents` + `app`.

## Steps

1. **Edit `deploy/deploy.config.env`** — set `DBX_PROFILE`, workspace url/id/region,
   `DBX_WAREHOUSE_ID`, `APP_USER_EMAIL`, the catalog/schema names, `LAKEBASE_INSTANCE`,
   `DEMO_SP_CLIENT_ID`/`ZB_CLIENT_ID`, and (if ready) `GENIE_SPACE_ID`. Pick names that do
   not collide with an existing deployment.

2. **Preflight**
   ```bash
   export DEPLOY_PYTHON=/path/to/venv/bin/python   # optional
   export ZB_CLIENT_SECRET='<sp-oauth-secret>'     # for the secret phase
   deploy/deploy.sh preflight
   ```

3. **Deploy everything**
   ```bash
   deploy/deploy.sh all
   ```
   or phase-by-phase (recommended):
   ```
   preflight → secret → notebooks → discover → app → grants → validate → status
   ```
   Phase notes:
   - **notebooks** runs `notebooks/01…10` as one chained serverless Lakeflow Job via
     `tools/run_build.py` (UC setup, Zerobus ingest, streaming medallion, Declarative Pipeline,
     Lakebase, ML model, SOP RAG + Vector Search, Genie space, Agent Bricks, operational job) and
     exports every executed notebook to `evidence/notebook_runs/`. First run ≈ 45–60 min
     (Lakebase, Vector Search endpoint and agent endpoints are provisioned).
   - **discover** reads the ids the notebooks created (Genie space, KA + supervisor endpoints,
     operational job, Lakebase DNS) into `deploy/.deploy.state.env`.
   - **app** renders `app/app.yaml`, binds resources (Lakebase, secret, LLM, Genie, supervisor,
     KA, SQL warehouse, operational job), builds the frontend, syncs and deploys.
   - **grants** applies the app service principal's grants: UC schemas, Vector Search, serving
     endpoints, and Postgres privileges on the Lakebase schema.
   - **validate** runs `tests/acceptance.py` against the live app API and then notebook 11
     (end-to-end PASS/FAIL matrix + lineage). If your laptop sits behind a TLS-inspecting proxy,
     export `SSL_CERT_FILE` to a CA bundle that includes the proxy CA.
   - Legacy single-purpose phases (`catalog lakebase sql pipeline knowledge agents`) still work
     for targeted re-runs without notebooks.

4. **Verify**
   ```bash
   deploy/deploy.sh status
   ```
   Open the printed app URL, go to **Scenario Controls → Start**, watch **Plant Overview**
   populate, then try **Explain & Fix** on an alert and **Ask the Assistant**. Full checklist
   in `docs/DEMO_SCRIPT.md`; agentic manual steps in `docs/DEMO_SCRIPT.md` §9 and
   `docs/AGENT_BRICKS.md`.

## Re-running / partial updates

Every phase is idempotent and independently runnable, e.g.:
```bash
deploy/deploy.sh app        # rebuild + redeploy just the app
deploy/deploy.sh agents     # (re)create KA + supervisor
deploy/deploy.sh grants     # re-apply SP grants
```
Discovered values persist in `deploy/.deploy.state.env` between runs.

## Switch managed ↔ built-in supervisor

- Managed (default once `agents` ran): `SUPERVISOR_ENDPOINT` is set in state and rendered
  into `app/app.yaml`.
- Built-in code supervisor: unset `SUPERVISOR_ENDPOINT` (remove it from
  `deploy/.deploy.state.env`) and re-run `deploy/deploy.sh app`.

## Teardown

```bash
deploy/deploy.sh teardown   # deletes app, job, KA, supervisor, VS index (asks for confirmation)
```
Lakebase instance, secret scope, and the catalog are **not** auto-deleted (data-loss guard);
the command prints the exact commands to remove them if you want a full cleanup.

## Known limitations

- Validated end-to-end against the reference workspace; on a new workspace, watch the
  infra-creation phases (catalog / Lakebase / app) — they depend on your entitlements.
- Zerobus SP + secret and Genie space curation are guided (see Prerequisites), not scripted.
- Agent Bricks KA/MAS APIs are **Beta**; endpoint provisioning is asynchronous.
