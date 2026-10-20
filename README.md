# PharmaCo — Real-Time Line Monitoring & SOP-Guided Remediation on Databricks

> **Industry:** Life sciences / pharmaceutical manufacturing (oral-solid-dose, sterile fill-finish, packaging)
> **All data is 100 % synthetic.** "PharmaCo" is a fictitious manufacturer; no customer data, names or
> records are used anywhere in this repository.

---

## 1. The customer problem

A pharma plant runs three lines — **LINE-A oral-solid-dose tablets**, **LINE-B sterile vials**,
**LINE-C packaging** — 15 assets, 7 sensors each. Today:

* Equipment drift (a tablet press running hot, an aseptic-filler bearing degrading, a lyophilizer
  pressure loop oscillating) is noticed **late**, usually when QA raises a **batch deviation**.
  Each deviation means a CAPA investigation and, for sterile product, often a scrapped batch.
* Historian data, alarms, SOPs and the operator log live in **separate systems**. Operators get an
  alarm, then search a document system for the right SOP, then log actions somewhere else.
* Fixed validated bands only fire *after* a parameter leaves spec. Instabilities that stay "in spec on
  average" (oscillating pressure) are invisible.

**Outcome sought** (Industry Outcome Map: *Manufacturing → reduce unplanned downtime & batch
deviations, improve OEE*): detect drift earlier, tell the operator **what to do according to the
approved SOP**, and keep a governed, auditable record. The buyer KPIs are time-to-detect,
time-to-acknowledge / MTTR, deviations per 1,000 batches, OEE availability.

## 2. The end-to-end data journey (one governed event flow)

```
 synthetic line telemetry generator (deterministic seed, 1 plant / 3 lines / 15 assets / 7 sensors)
        │  protobuf over gRPC, OAuth service principal
        ▼
 ① LAKEFLOW CONNECT — Zerobus Ingest ──► bronze.telemetry_raw (UC managed Delta)
        │
        ├─► ① LAKEFLOW Structured Streaming job (operational)        ├─► ① LAKEFLOW Declarative Pipeline
        │     silver: validate · dedup · quarantine                  │     silver_telemetry (5 expectations)
        │     gold:   asset state · alert state-machine ·            │     silver_quarantine
        │             line health · DQ metrics                       │     gold MVs: ML features · line KPIs ·
        │                                                            │               asset reliability
        ▼                                                            ▼
 ③ LAKEBASE (Postgres) ◄── serving sync ── gold        ④ ML: per-asset anomaly detector (MLflow, UC model
     asset state · alerts · operator acks (txn) ·            @champion) ── scores ─► gold + Lakebase
     ML scores · demo controls
        │                                              ④ GenAI: SOP library → Vector Search → Claude
        │                                                  "Explain & Fix" (cited) · ai_query() handover
        │                                              ④ Agent Bricks: Knowledge Assistant + Supervisor
        │                                              ⑤ GENIE space over gold + LDP KPIs + ML scores
        ▼
 ⑥ DATABRICKS APP (FastAPI + React): Plant overview · Line · Asset (rule + ML) · Alert console
    (acknowledge/resolve → Lakebase) · Explain & Fix · Ask the Assistant (supervisor) · Observability

 ② UNITY CATALOG governs every object above: schemas, tables, volumes, tags, column mask, grants,
    model registry, vector index, lineage.  ① Lakeflow Jobs orchestrates: streaming → LDP → ML scoring.
```

## 3. Requirement → implementation → execution evidence

Every stage was **executed in a Databricks workspace** (serverless, AWS us-east-1). The executed
notebooks — code **and real cell outputs** — are committed as text in
[`evidence/notebook_runs/`](evidence/notebook_runs/) (`.md` for reading, `.ipynb` for Jupyter), with
job-run metadata in [`evidence/runs/`](evidence/runs/) and CLI/test logs in
[`evidence/logs/`](evidence/logs/).

| Stage | What was built | Code | Execution evidence (text) |
|---|---|---|---|
| **Lakeflow — ingest** | Zerobus Ingest (direct gRPC write to UC Delta, OAuth SP) of 30 min backfill + live stream + DQ probes | [`ingest/zerobus_ingest.py`](ingest/zerobus_ingest.py), [`generator/`](generator/telemetry_generator.py), [`notebooks/02`](notebooks/02_ingest_zerobus.py) | [`02_ingest_zerobus.md`](evidence/notebook_runs/02_ingest_zerobus.md) |
| **Lakeflow — streaming** | Structured Streaming medallion, dedup, quarantine, alert state machine | [`pipeline/rt_pipeline.py`](pipeline/rt_pipeline.py), [`notebooks/03`](notebooks/03_streaming_medallion_pipeline.py) | [`03_streaming_medallion_pipeline.md`](evidence/notebook_runs/03_streaming_medallion_pipeline.md) |
| **Lakeflow — declarative** | Serverless Declarative Pipeline: expectations, quarantine, 3 gold MVs | [`pipelines/ldp_medallion.py`](pipelines/ldp_medallion.py), [`notebooks/04`](notebooks/04_lakeflow_declarative_pipeline.py) | [`04_lakeflow_declarative_pipeline.md`](evidence/notebook_runs/04_lakeflow_declarative_pipeline.md) (event-log expectation metrics) |
| **Lakeflow — jobs** | Operational job: streaming → LDP refresh → ML scoring | [`notebooks/10`](notebooks/10_operational_lakeflow_job.py), [`notebooks/ops/`](notebooks/ops/) | [`10_operational_lakeflow_job.md`](evidence/notebook_runs/10_operational_lakeflow_job.md) |
| **Unity Catalog** | 6 schemas, DDL, governance tags, column mask, least-privilege SP grants, UC model, lineage | [`sql/`](sql/), [`notebooks/01`](notebooks/01_uc_setup_and_governance.py) | [`01_uc_setup_and_governance.md`](evidence/notebook_runs/01_uc_setup_and_governance.md), lineage in [`11_end_to_end_validation.md`](evidence/notebook_runs/11_end_to_end_validation.md) |
| **Lakebase** | Postgres operational store, direct-write sync, transactional acknowledgements, ack-preservation proof, latency | [`lakebase/schema.sql`](lakebase/schema.sql), [`notebooks/05`](notebooks/05_lakebase_operational_serving.py) | [`05_lakebase_operational_serving.md`](evidence/notebook_runs/05_lakebase_operational_serving.md) |
| **ML** | Per-asset learned-baseline anomaly detector, evaluated vs rule baseline, MLflow + UC `@champion`, batch scoring to Delta + Lakebase | [`pipeline/anomaly_model.py`](pipeline/anomaly_model.py), [`pipeline/ml_scoring.py`](pipeline/ml_scoring.py), [`notebooks/06`](notebooks/06_ml_anomaly_model.py) | [`06_ml_anomaly_model.md`](evidence/notebook_runs/06_ml_anomaly_model.md) |
| **GenAI** | SOP RAG: UC Volume + Delta chunks → Vector Search → Claude Sonnet 4.5 cited remediation; retrieval eval; `ai_query()` | [`knowledge/`](knowledge/), [`notebooks/07`](notebooks/07_genai_sop_fix_assistant.py), [`app/server/assistant.py`](app/server/assistant.py) | [`07_genai_sop_fix_assistant.md`](evidence/notebook_runs/07_genai_sop_fix_assistant.md) |
| **Agents** | Agent Bricks Knowledge Assistant + Multi-Agent Supervisor (tools: SOP KA + Genie) | [`knowledge/create_agent_bricks.py`](knowledge/create_agent_bricks.py), [`notebooks/09`](notebooks/09_agent_bricks_supervisor.py) | [`09_agent_bricks_supervisor.md`](evidence/notebook_runs/09_agent_bricks_supervisor.md) |
| **Genie** | Curated Genie space created via API; real conversation incl. follow-up; generated SQL + rows | [`notebooks/08`](notebooks/08_genie_space.py), [`app/server/genie.py`](app/server/genie.py) | [`08_genie_space.md`](evidence/notebook_runs/08_genie_space.md) |
| **Databricks App** | FastAPI + React operator/manager app bound to Lakebase, warehouse, Genie, LLM, agents | [`app/`](app/), [`deploy/deploy.sh`](deploy/deploy.sh) | [`app_acceptance_test.log`](evidence/logs/app_acceptance_test.log) (live API calls incl. acknowledge, Explain & Fix, Genie, supervisor), [`deploy_app.log`](evidence/logs/deploy_app.log) |
| **End-to-end** | PASS/FAIL matrix across all stages, lineage, app-originated writes in Lakebase | [`notebooks/11`](notebooks/11_end_to_end_validation.py) | [`11_end_to_end_validation.md`](evidence/notebook_runs/11_end_to_end_validation.md) |

How evidence was produced: [`tools/run_build.py`](tools/run_build.py) syncs this repo to the workspace,
runs the notebooks as a chained **serverless Lakeflow Job**, then exports each task's executed notebook
through the Jobs `runs/export` API and writes the outputs as text. Nothing in `evidence/` is hand-edited.

## 4. Results (from the committed clean run — job run `24808449534624`, all 10 tasks first-attempt SUCCESS)

| What | Result | Evidence |
|---|---|---|
| Zerobus ingest | **113,618** synthetic records landed via Zerobus (94,500 backfill at ~26.7k rec/s + 18,900 live over 180 s + DQ probes); live event→writer **p50 2 ms / p95 3 ms** | notebook 02 |
| Data quality | **105** duplicate re-emissions removed, **8** malformed records quarantined — by the streaming job *and* by LDP expectations (`valid_sensor_type` 5 failed, `valid_sensor_value` 3 failed) | notebooks 03, 04 |
| Operational alerting | **CRITICAL** TREND alert on the A-COMPRESS tablet press (77.2 °C sustained vs 75 °C validated limit) | notebook 03 |
| Lakebase | 15 assets / 3 lines / open alerts served; operator acknowledgement **survives** pipeline upserts; point reads **p50 3.4 ms / p95 6.6 ms** | notebook 05 |
| ML vs rule | Per-asset baseline model **precision 0.978 · recall 0.763 · F1 0.857** vs validated-band rule 1.0 · 0.658 · 0.794. Catches the lyophilizer **pressure instability the band rule never flags**; **20 s earlier** on temperature and vibration; names the driving sensor correctly in every scenario | notebook 06 |
| GenAI | Vector Search retrieval **hit@3 = 7/8**; Explain & Fix for the live alert cites only SOPs from the retrieved context (SOP-TEMP-014, SOP-GEN-001) | notebook 07 |
| Agents | Supervisor answered "why is LINE-A degraded and what do I do" with **both** tools (`genie_data` → live cause, `sop_ka` → SOP-TEMP-014 steps) | notebook 09 |
| Genie | NL questions → generated SQL → rows, incl. a follow-up in the same conversation and questions over ML scores + LDP KPIs | notebook 08 |
| Orchestration | Operational Lakeflow Job: new Zerobus events → CRITICAL B-FILL vibration alert → ML flags B-FILL (`top_sensor=vibration`) | notebook 10 |
| App | Deployed, RUNNING; **33/33** acceptance checks against the live app API (reads, ML scores, acknowledge → Lakebase txn + audit, Explain & Fix, Genie, supervisor) | `logs/app_acceptance_test.log` |
| End-to-end | **23/23** cross-stage checks PASS + UC lineage bronze → silver → gold / LDP / features, SOP chunks → vector index | notebook 11 |

## 5. Reproduce in your own workspace

Prereqs: Databricks CLI profile, `jq`, `envsubst`, Node/npm, Python with `databricks-sdk psycopg[binary] protobuf`.
Workspace needs Unity Catalog, serverless, Zerobus Ingest, Lakebase, Vector Search, Model Serving, Apps
and Agent Bricks (Beta).

```bash
# 0) edit config.env + deploy/deploy.config.env (workspace, catalog, warehouse)
# 1) create the ingest service principal + OAuth secret in a secret scope (see evidence/logs/00_provision_ingest_sp.log)
# 2) build everything as a Lakeflow Job and export evidence
export DEPLOY_PYTHON=/path/to/venv/bin/python
deploy/deploy.sh notebooks        # notebooks 01-10 on serverless (≈45 min first time)
deploy/deploy.sh discover         # Genie space / agents / job / Lakebase ids -> deploy/.deploy.state.env
deploy/deploy.sh app grants       # deploy the app, bind resources, grant its service principal
deploy/deploy.sh validate         # app acceptance test + notebook 11 (end-to-end matrix)
```

Re-run from empty data layers: `python tools/reset_data.py --yes`, then the steps above.
Presenter runbook: [`docs/DEMO_SCRIPT.md`](docs/DEMO_SCRIPT.md). Agent details: [`docs/AGENT_BRICKS.md`](docs/AGENT_BRICKS.md).
API contract: [`docs/openapi.json`](docs/openapi.json).

## 6. Repository layout

```
notebooks/            01-11 build notebooks (Databricks source format) + ops/ job tasks + _config
evidence/             executed notebook outputs (.md/.ipynb), job-run metadata, CLI + test logs
generator/            synthetic telemetry generator (stdlib only, deterministic seed, scenario injection)
ingest/               Zerobus protobuf schema + ingest driver
pipeline/             rt_pipeline (Structured Streaming medallion), anomaly_model, ml_scoring
pipelines/            Lakeflow Declarative Pipeline source
sql/                  bronze / silver / gold DDL
lakebase/             Postgres operational schema
knowledge/            synthetic SOP corpus, chunker, Vector Search loader, Agent Bricks provisioning
app/                  Databricks App — FastAPI backend (server/) + React frontend (frontend/src)
deploy/               config + phased deploy orchestrator + templates
tests/acceptance.py   data-layer + live app API acceptance test
tools/                run_build.py (run + export evidence), reset_data.py
```

## 7. Design notes and honest limitations

* **Real-Time Mode:** Spark `Trigger.RealTime` supports message-bus sources/sinks only, not Delta, so
  the Delta medallion uses standard Structured Streaming (AvailableNow on serverless; ProcessingTime on
  classic compute for ~5–10 s latency). The operational job is scheduled every 5 min and **paused by
  default** to save cost; the app's own serving sync refreshes Lakebase every ~5 s.
* **Two Lakeflow pipelines on one bronze table, on purpose:** the Structured Streaming job owns
  stateful operational logic (alert state machine preserving operator acknowledgements), which
  materialized views can't express. The Declarative Pipeline owns declarative DQ and analytical
  features. Both are governed in UC and orchestrated by one Lakeflow Job.
* **ML evaluation** uses synthetic ground-truth labels derived from the generator's scenario targets;
  the model is trained only on healthy history, and the threshold (6σ) was chosen on this data. Real
  deployment would tune on plant history and validate per GxP.
* **Lakebase in UC:** registering the instance as a UC database catalog needs `CREATE CATALOG` on the
  metastore, which this shared demo workspace doesn't grant. Lakebase access is governed through
  Databricks identities and OAuth tokens instead (shown in notebook 05).
* Not production-qualified: no DR, no CSV/GxP validation, single small warehouse.
