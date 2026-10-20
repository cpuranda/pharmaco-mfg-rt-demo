#!/usr/bin/env python3
"""Apply a .sql file against a Databricks SQL warehouse via the Statement
Execution API, substituting ${cat}/${bronze}/${silver}/${gold} placeholders.

Uses the Databricks CLI (`databricks api post`) for auth, so no SDK needed.
Config comes from env vars (see pharmaco-mfg-rt-demo/config.env)."""
import json
import os
import re
import subprocess
import sys

PROFILE = os.environ.get("DBX_PROFILE", "fe-vm-hls-streaming-solution-demo")
WAREHOUSE = os.environ.get("DBX_WAREHOUSE_ID", "1b3fb1ab4eaa9511")
SUBS = {
    "cat": os.environ.get("DEMO_CATALOG", "hls_streaming_solution_demo_catalog"),
    "bronze": os.environ.get("DEMO_BRONZE", "pharmaco_mfg_rt_bronze"),
    "silver": os.environ.get("DEMO_SILVER", "pharmaco_mfg_rt_silver"),
    "gold": os.environ.get("DEMO_GOLD", "pharmaco_mfg_rt_gold"),
}


def substitute(text: str) -> str:
    for k, v in SUBS.items():
        text = text.replace("${" + k + "}", v)
    return text


def split_statements(sql: str) -> list[str]:
    # strip line comments, then split on semicolons that are NOT inside a
    # single-quoted string literal (comments/strings may contain ';').
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    body = "\n".join(lines)
    stmts, cur, in_str = [], [], False
    for ch in body:
        if ch == "'":
            in_str = not in_str
            cur.append(ch)
        elif ch == ";" and not in_str:
            stmts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        stmts.append("".join(cur))
    return [s.strip() for s in stmts if s.strip()]


def run_statement(stmt: str) -> dict:
    payload = {"warehouse_id": WAREHOUSE, "statement": stmt, "wait_timeout": "50s"}
    proc = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "-p", PROFILE, "--json", json.dumps(payload)],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return {"status": {"state": "CLI_ERROR"}, "error": proc.stderr[:500]}
    return json.loads(proc.stdout)


def main(paths: list[str]) -> int:
    rc = 0
    for path in paths:
        with open(path) as f:
            sql = substitute(f.read())
        stmts = split_statements(sql)
        print(f"\n=== {os.path.basename(path)}: {len(stmts)} statement(s) ===")
        for i, stmt in enumerate(stmts, 1):
            res = run_statement(stmt)
            state = res.get("status", {}).get("state")
            head = " ".join(stmt.split())[:70]
            if state == "SUCCEEDED":
                print(f"  [{i}] OK    {head}")
            else:
                rc = 1
                err = res.get("status", {}).get("error", res.get("error", res))
                print(f"  [{i}] FAIL  {head}\n        {str(err)[:300]}")
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
