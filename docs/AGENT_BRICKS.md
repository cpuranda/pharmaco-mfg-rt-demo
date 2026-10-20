# Agentic layer — SOP-grounded Fix Assistant + Multi-Agent Supervisor

This document describes the agentic enhancement added to the PharmaCo Manufacturing
Real-Time Line Monitoring demo, and how to run it in **two modes**:

1. **Code path (built + live now)** — a self-contained Retrieval-Augmented pipeline in
   the app: Databricks **Vector Search** over synthetic SOPs + a **Foundation Model**
   endpoint, with a lightweight in-app supervisor that also calls the Genie space.
2. **Managed Agent Bricks (optional)** — a **Knowledge Assistant** + **Multi-Agent
   Supervisor** created in the Agent Bricks UI over the *same* SOP corpus and Genie
   space; the app is flipped to call the managed supervisor endpoint by setting one env var.

Everything is 100% SYNTHETIC.

---

## What it adds to the demo

| Surface | Before | After |
|---|---|---|
| Alert Console row / Asset Detail alert | static `recommended_action` heuristic | **"Explain & Fix"** → SOP-grounded, cited, step-by-step remediation scoped to that alert |
| Ask tab | Genie only (structured data) | **"Ask the Assistant"** → a supervisor that routes across the **Genie data agent** and the **SOP knowledge base**, then answers with citations and shows which agents were used |

## Architecture

```
                           ┌─────────────────────────── Databricks App (FastAPI) ───────────────────────────┐
  Operator ── "Explain &   │  /api/assistant/explain-fix ─► retrieve_sops() ─► Vector Search (sop_index)     │
  Fix" / "Ask" ───────────►│                              └─► Foundation Model (databricks-claude-sonnet-4-5)│
                           │  /api/assistant/ask ─► planner(LLM) ─┬─► Genie space (live gold data)           │
                           │                                       └─► Vector Search (SOPs) ─► synth(LLM)     │
                           └──────────────────────────────────────────────────────────────────────────────┘
   Governed sources: UC Volume sop_docs (raw) ─► Delta sop_chunks (CDF) ─► Vector Search index sop_index
```

Backend: `app/server/assistant.py` (+ routes `app/server/routes/assistant.py`).
Knowledge build: `knowledge/generate_sops.py` (corpus) + `knowledge/load_knowledge.py` (UC + index).

## Unity Catalog assets

- Schema: `hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_knowledge`
- Volume: `…knowledge.sop_docs` — 10 synthetic SOP markdown files (KA source)
- Table: `…knowledge.sop_chunks` — 42 section-level chunks, CDF enabled, PK `chunk_id`
- Vector index: `…knowledge.sop_index` — Delta-Sync, managed embeddings (`databricks-gte-large-en`)
- VS endpoint: `pharmaco-mfg-rt-vs`

## Rebuilding the knowledge base

```bash
# from repo root, with a workspace-authenticated python env (databricks-sdk installed)
python knowledge/generate_sops.py         # regenerate SOP markdown + sop_chunks.json
python knowledge/load_knowledge.py        # (re)create schema/volume/table/index + grants
```
Edit `SOPS` in `generate_sops.py` to add/adjust procedures; re-run both steps. The
Delta-Sync index picks up changes on the next sync (CDF).

## App configuration (`app/app.yaml`)

```yaml
- name: ASSISTANT_LLM_ENDPOINT
  value: databricks-claude-sonnet-4-5      # any llm/v1/chat serving endpoint
- name: SOP_VS_INDEX
  value: hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_knowledge.sop_index
# - name: SUPERVISOR_ENDPOINT                # set to use a managed Agent Bricks supervisor
#   value: <serving-endpoint-name>
```

Permissions granted to the app service principal: `USE CATALOG/SCHEMA`, `SELECT` on
`sop_chunks` and `sop_index`, `READ VOLUME` on `sop_docs`, `CAN_USE` on the Vector Search
endpoint, and `CAN_QUERY` on the LLM endpoint (bound as an app `serving-endpoint` resource).

---

## Managed Agent Bricks version — PROVISIONED

The managed Knowledge Assistant + Multi-Agent Supervisor are **live** and the app's Ask tab
is wired to them. They were created programmatically via the Agent Bricks Beta APIs
(`databricks-sdk` `knowledgeassistants` / `supervisoragents`) — see
`knowledge/create_agent_bricks.py` (idempotent; re-run to recreate).

| Resource | Name / endpoint |
|---|---|
| Knowledge Assistant | `pharmaco-mfg-sop-ka` → endpoint `ka-aaab91b9-endpoint` |
| KA knowledge source | `source_type=index` over `…knowledge.sop_index` (text_col `content`, doc_uri_col `source_file`) |
| Multi-Agent Supervisor | `pharmaco-mfg-supervisor` → endpoint `mas-ed49a452-endpoint` |
| Supervisor tools | `sop_ka` (Knowledge Assistant) + `genie_data` (Genie space `01f1cc7fa00816ba81cd394475f36651`) |

App wiring: `app/app.yaml` sets `SUPERVISOR_ENDPOINT=mas-ed49a452-endpoint`; the MAS + KA
endpoints are bound as app `serving-endpoint` resources and the app SP has `CAN_QUERY` on
both (the supervisor invokes the KA on-behalf-of the caller, so the SP needs KA access too).
The managed MAS speaks the `agent/v1/responses` API; `assistant.py:_responses_call` handles it.

- **To switch back to the built-in code supervisor:** remove/blank `SUPERVISOR_ENDPOINT` in
  `app/app.yaml` and redeploy. The Ask tab shows "built-in supervisor" again.
- **"Explain & Fix"** on alerts still uses the governed Vector Search retrieval path (fast,
  alert-scoped); the managed KA powers the supervisor's SOP tool.
- Manage/inspect via CLI: `databricks knowledge-assistants list-knowledge-assistants`,
  `databricks supervisor-agents list-supervisor-agents` (both Beta).

### Recreate from scratch (or via the UI)

Run `.venv-introspect/bin/python knowledge/create_agent_bricks.py`, **or** build it in the
Agent Bricks UI:

### 1. Knowledge Assistant (KA)
1. Databricks UI → **Agents / Agent Bricks → Knowledge Assistant → Create**.
2. Name it e.g. `pharmaco-mfg-sop-ka`; add a description that tells it to answer plant
   operators using pharma manufacturing SOPs and to cite the SOP id.
3. **Knowledge source**: point it at the UC Volume
   `hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_knowledge.sop_docs`
   (it will parse + index the markdown; it manages its own vector index).
4. Test with: *"How do I respond to a tablet press over-temperature?"* → expect SOP-TEMP-014.
5. Note the KA's **serving endpoint name**.

### 2. Multi-Agent Supervisor (MAS)
1. Agent Bricks → **Multi-Agent Supervisor → Create** (e.g. `pharmaco-mfg-supervisor`).
2. Add agents:
   - the **Genie space** `01f1cc7fa00816ba81cd394475f36651` (live plant data), and
   - the **Knowledge Assistant** created above (SOP remediation).
3. Give a supervisor description: route data/metric/alert questions to Genie, "how to fix /
   which SOP" questions to the KA, and blend them for "why + how" questions.
4. Test: *"Why is LINE-A degraded and what should I do?"*
5. Note the supervisor's **serving endpoint name**.

### 3. Flip the app to the managed supervisor
```bash
# add SUPERVISOR_ENDPOINT to app/app.yaml (value = the MAS serving endpoint name), then:
cd app && bash deploy.sh
```
Also grant the app SP `CAN_QUERY` on the MAS endpoint (add it as a `serving-endpoint` app
resource, same pattern as `assistant-llm`). The **Ask the Assistant** tab will then show
"managed Agent Bricks supervisor" and route through the managed agent; **Explain & Fix**
continues to use the governed Vector Search path (or repoint it to the KA endpoint).

### Governance / GxP talking points
- SOPs, chunks, index, and endpoints are all Unity Catalog securables with lineage & audit.
- Answers are **grounded with citations** to controlled documents — no free-form hallucination.
- Agent Bricks **evaluation + review app** lets SMEs validate answers before promotion.
- AI Gateway guardrails (PII/safety, rate limits) apply to the serving endpoints.

---

## Demo script talk track (additions)

1. On **Scenario Controls**, run *Temperature excursion* (targets A-COMPRESS).
2. Open **Alert Console** → the A-COMPRESS CRITICAL temperature alert → click **Explain & Fix**.
   → SOP-grounded remediation appears with `[SOP-TEMP-014]` citations. *"Grounded in our SOPs,
   not invented — every step cites the controlled document."*
3. Open **Ask the Assistant** → *"Why is LINE-A degraded right now and what should I do?"*
   → watch the **Genie data** + **SOP knowledge** badges; the answer states the live cause
   (A-COMPRESS, anomaly 1.0) and the remediation with citations. *"One supervisor, two
   agents — structured data and unstructured knowledge, orchestrated."*
4. (If managed) show the KA and Multi-Agent Supervisor in the Agent Bricks UI and note it is
   the same governed assets, created without code.
