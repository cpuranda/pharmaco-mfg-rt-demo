# Execution evidence (text only — nothing here is hand-edited)

Everything in this folder was produced by actually running the build in a Databricks workspace
(serverless compute, AWS us-east-1) on **synthetic data**.

| Path | What it is | Produced by |
|---|---|---|
| `notebook_runs/NN_*.md` | Each executed notebook rendered as text: every code cell followed by its **real output** (stdout, tables, errors) | `tools/run_build.py` → Jobs `runs/export` API |
| `notebook_runs/NN_*.ipynb` | Same content as Jupyter notebooks with outputs embedded (renders on GitHub) | same |
| `runs/<job_run_id>.json` | Job run metadata: run id, run page URL, start time, every task's run id / result / duration | Jobs `runs/get` API |
| `logs/00_provision_ingest_sp.log` | Creation of the Zerobus ingest service principal + OAuth secret into a secret scope (secret value never logged) | Databricks CLI |
| `logs/reset_before_clean_run.log` | Data layers dropped so the submitted run starts from an empty namespace | `tools/reset_data.py` |
| `logs/clean_full_build_run.log` | Task-state timeline of the single job run that executed notebooks 01 → 10 | `tools/run_build.py` |
| `logs/deploy_discover.log`, `deploy_app.log`, `deploy_grants.log` | App deployment: discovered resource ids, app resources bound, service-principal grants (UC, Vector Search, endpoints, Lakebase) | `deploy/deploy.sh` |
| `logs/app_acceptance_test.log` | Live calls against the deployed Databricks App API — reads, ML scores, **operator acknowledge (Lakebase transaction)**, Explain & Fix, Genie, Agent Bricks supervisor — with latencies and answers | `tests/acceptance.py` |
| `logs/validate_phase.log` | Full validate phase output (acceptance test + notebook 11 run) | `deploy/deploy.sh validate` |

Reading order for a reviewer: `01` → `11` in `notebook_runs/` (one per stage of the journey), then
`logs/app_acceptance_test.log` for the app, then `notebook_runs/11_end_to_end_validation.md` for the
PASS/FAIL matrix and Unity Catalog lineage.

Notes
* Lines starting with `ERROR: pip's dependency resolver …` are pip warnings from `%pip install`
  cells (pre-installed serverless packages vs pinned versions), not execution failures — the task
  result for every notebook is recorded in `runs/*.json`.
* Serverless may retry a task automatically; the export keeps the latest attempt of each task.
