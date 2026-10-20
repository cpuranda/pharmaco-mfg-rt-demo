#!/usr/bin/env python3
"""
Run the build notebooks as a serverless Lakeflow Job and commit their executed outputs.

  python tools/run_build.py 01 02 03          # run these notebooks (chained tasks)
  python tools/run_build.py --export-run <job_run_id>   # only re-export an existing run

For every task run, the executed notebook (code + outputs as produced in the workspace)
is exported via the Jobs `export-run` API and written as:
  evidence/notebook_runs/<notebook>.ipynb   (Jupyter, outputs embedded as text)
  evidence/notebook_runs/<notebook>.md      (plain-text rendering for quick reading)
plus evidence/runs/<job_run_id>.json with run ids, task states, durations and URLs.

Requires the Databricks CLI profile in config.env (DBX_PROFILE) and databricks-sdk.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONF = dict(l.strip().split("=", 1) for l in open(os.path.join(ROOT, "config.env"))
            if "=" in l and not l.lstrip().startswith("#"))
PROFILE = CONF["DBX_PROFILE"]
WS_DIR = CONF["WS_REPO_DIR"]
OUT_NB = os.path.join(ROOT, "evidence", "notebook_runs")
OUT_RUNS = os.path.join(ROOT, "evidence", "runs")
MAX_TABLE_ROWS = 40

w = WorkspaceClient(profile=PROFILE)


def nb_for(prefix: str) -> str:
    for f in sorted(os.listdir(os.path.join(ROOT, "notebooks"))):
        if f.startswith(prefix) and f.endswith(".py"):
            return f[:-3]
    raise SystemExit(f"no notebook starting with {prefix}")


def sync_repo():
    print(f"syncing repo -> {WS_DIR}")
    excludes = ["app/frontend/node_modules", "app/frontend/src", "evidence", ".git",
                "__pycache__", "*.pyc", ".DS_Store", "deploy/.deploy.state.env"]
    cmd = ["databricks", "sync", ROOT, WS_DIR, "--full", "-p", PROFILE]
    for e in excludes:
        cmd += ["--exclude", e]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)


def run_job(notebooks: list[str]) -> int:
    tasks, prev = [], None
    for nb in notebooks:
        key = nb.split("_")[0] + "_" + nb.split("_")[1]
        t = {"task_key": key, "notebook_task": {"notebook_path": f"{WS_DIR}/notebooks/{nb}"},
             "timeout_seconds": 5400, "max_retries": 0}
        if prev:
            t["depends_on"] = [{"task_key": prev}]
        tasks.append(t)
        prev = key
    body = {"run_name": f"pharmaco_mfg_rt_e2e_build [{' '.join(n[:2] for n in notebooks)}]",
            "tasks": tasks}
    run_id = w.api_client.do("POST", "/api/2.2/jobs/runs/submit", body=body)["run_id"]
    print(f"submitted job run {run_id}")
    last = None
    while True:
        r = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        states = {t["task_key"]: t["state"].get("result_state") or t["state"]["life_cycle_state"]
                  for t in r["tasks"]}
        if states != last:
            print(f"  {datetime.now().strftime('%H:%M:%S')} {states}")
            last = states
        if r["state"]["life_cycle_state"] in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            break
        time.sleep(20)
    print(f"run {run_id}: {r['state'].get('result_state')}  {r.get('run_page_url')}")
    return run_id


# --------------------------------------------------------------------------- #
# export → ipynb / md
# --------------------------------------------------------------------------- #
def _model_from_html(html: str) -> dict:
    m = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", html)
    return json.loads(urllib.parse.unquote(base64.b64decode(m.group(1)).decode()))


def _strip_ansi(s: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", s or "")


def _md_table(schema, data) -> str:
    cols = [c["name"] for c in schema]
    rows = data[:MAX_TABLE_ROWS]
    def cell(v):
        return ("" if v is None else str(v)).replace("|", "\\|").replace("\n", " ")[:1500]
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    out += ["| " + " | ".join(cell(v) for v in r) + " |" for r in rows]
    if len(data) > MAX_TABLE_ROWS:
        out.append(f"\n_({len(data) - MAX_TABLE_ROWS} more rows not shown)_")
    return "\n".join(out)


def _outputs(results) -> list[str]:
    """Return a list of text blocks for a command's results."""
    if not results:
        return []
    items = results.get("data") if results.get("type") == "listResults" else [results]
    blocks = []
    for it in items or []:
        t = it.get("type")
        if t in ("ansi", "text"):
            txt = _strip_ansi(it.get("data") if isinstance(it.get("data"), str) else str(it.get("data")))
            if txt.strip():
                blocks.append(txt.rstrip())
        elif t == "table":
            blocks.append(_md_table(it.get("schema", []), it.get("data", [])))
        elif t == "html":
            txt = re.sub(r"<[^>]+>", " ", it.get("data", ""))
            if txt.strip():
                blocks.append(re.sub(r"\s+", " ", txt).strip()[:4000])
        elif t == "error":
            blocks.append("ERROR: " + _strip_ansi(str(it.get("data") or it.get("summary")))[:4000])
    if results.get("type") == "error" or results.get("cause"):
        blocks.append("ERROR: " + _strip_ansi(str(results.get("summary") or results.get("cause")))[:4000])
    return blocks


def export_task(task: dict, run_meta: dict):
    tr = task["run_id"]
    exp = w.api_client.do("GET", "/api/2.2/jobs/runs/export", query={"run_id": tr, "views_to_export": "CODE"})
    model = _model_from_html(exp["views"][0]["content"])
    nb_name = os.path.basename(task["notebook_task"]["notebook_path"])
    cells, md = [], [f"# Executed notebook: `{nb_name}`\n",
                     f"> Exported from Databricks job run **{run_meta['run_id']}** / task run **{tr}** — "
                     f"result **{task['state'].get('result_state')}**, executed "
                     f"{datetime.fromtimestamp(task['start_time']/1000, timezone.utc).isoformat(timespec='seconds')} "
                     f"on serverless compute in workspace `{CONF['DBX_WORKSPACE_URL']}`. "
                     f"Outputs below are the real cell outputs of that run.\n"]
    for c in sorted(model["commands"], key=lambda c: c.get("position", 0)):
        src = c.get("command", "")
        if src.lstrip().startswith("%md"):
            text = re.sub(r"^\s*%md\s?", "", src)
            cells.append({"cell_type": "markdown", "metadata": {}, "source": text})
            md.append(text + "\n")
            continue
        outs = _outputs(c.get("results"))
        if c.get("error"):
            outs.append("ERROR: " + _strip_ansi(c.get("errorSummary") or "") + "\n" + _strip_ansi(c["error"])[:1500])
        elif c.get("errorSummary") and c.get("errorSummary") != "Command skipped":
            outs.append("ERROR: " + _strip_ansi(c["errorSummary"]))
        cells.append({"cell_type": "code", "metadata": {}, "execution_count": None, "source": src,
                      "outputs": [{"output_type": "stream", "name": "stdout",
                                   "text": "\n\n".join(outs) + "\n"}] if outs else []})
        md.append("```python\n" + src.rstrip() + "\n```\n")
        if outs:
            md.append("**Output:**\n")
            for b in outs:
                md.append(b + "\n" if b.startswith("|") else "```text\n" + b + "\n```\n")
    nbjson = {"nbformat": 4, "nbformat_minor": 5, "cells": cells,
              "metadata": {"language_info": {"name": "python"},
                           "databricks_run": {"job_run_id": run_meta["run_id"], "task_run_id": tr,
                                              "result_state": task["state"].get("result_state")}}}
    os.makedirs(OUT_NB, exist_ok=True)
    json.dump(nbjson, open(os.path.join(OUT_NB, nb_name + ".ipynb"), "w"), indent=1)
    open(os.path.join(OUT_NB, nb_name + ".md"), "w").write("\n".join(md))
    print(f"  exported {nb_name} ({len(cells)} cells)")


def export_run(run_id: int):
    r = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
    meta = {"run_id": run_id, "run_name": r.get("run_name"), "result_state": r["state"].get("result_state"),
            "run_page_url": r.get("run_page_url"),
            "start_time": datetime.fromtimestamp(r["start_time"]/1000, timezone.utc).isoformat(),
            "tasks": []}
    latest = {}
    for t in sorted(r["tasks"], key=lambda t: t.get("start_time", 0)):
        latest[t["task_key"]] = t
    for t in latest.values():
        meta["tasks"].append({"task_key": t["task_key"], "task_run_id": t["run_id"],
                              "notebook": t["notebook_task"]["notebook_path"],
                              "result_state": t["state"].get("result_state"),
                              "duration_s": round((t.get("end_time", 0) - t.get("start_time", 0)) / 1000)})
        if t["state"].get("result_state") in ("SUCCESS", "FAILED"):
            export_task(t, meta)
    os.makedirs(OUT_RUNS, exist_ok=True)
    json.dump(meta, open(os.path.join(OUT_RUNS, f"{run_id}.json"), "w"), indent=1)
    print(json.dumps(meta, indent=1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("notebooks", nargs="*", help="notebook number prefixes, e.g. 01 02")
    ap.add_argument("--export-run", type=int)
    ap.add_argument("--no-sync", action="store_true")
    a = ap.parse_args()
    if a.export_run:
        export_run(a.export_run)
        sys.exit(0)
    if not a.no_sync:
        sync_repo()
    rid = run_job([nb_for(p) for p in a.notebooks])
    export_run(rid)
