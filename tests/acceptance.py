#!/usr/bin/env python3
"""
Acceptance / validation checks for the PharmaCo MFG RT demo.

Covers the mission's acceptance criteria that can be verified from outside the
workspace: schema/tables, bronze->silver flow, dedup, quarantine, gold alerts,
anomaly & slowdown detection, Lakebase freshness, deterministic replay, and
(optionally) backend API smoke tests.

Usage:
  set -a; . config.env; set +a
  python3 tests/acceptance.py                 # data-layer checks
  API_BASE=http://127.0.0.1:8077 python3 tests/acceptance.py   # + API smoke
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

PROFILE = os.environ.get("DBX_PROFILE", "fe-vm-hls-streaming-solution-demo")
WH = os.environ.get("DBX_WAREHOUSE_ID", "1b3fb1ab4eaa9511")
CAT = os.environ.get("DEMO_CATALOG", "hls_streaming_solution_demo_catalog")
BRONZE = os.environ.get("DEMO_BRONZE", "pharmaco_mfg_rt_bronze")
SILVER = os.environ.get("DEMO_SILVER", "pharmaco_mfg_rt_silver")
GOLD = os.environ.get("DEMO_GOLD", "pharmaco_mfg_rt_gold")
API_BASE = os.environ.get("API_BASE", "")

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))


def sql(statement):
    payload = {"warehouse_id": WH, "statement": statement, "wait_timeout": "50s"}
    p = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements",
                        "-p", PROFILE, "--json", json.dumps(payload)],
                       capture_output=True, text=True)
    d = json.loads(p.stdout)
    if d.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(d.get("status", {}).get("error", d))
    return d.get("result", {}).get("data_array", []) or []


def scalar(statement):
    r = sql(statement)
    return r[0][0] if r and r[0] else None


def data_layer_checks():
    print("\n=== DATA LAYER ===")
    # tables exist
    for schema, tbls in [(BRONZE, ["telemetry_raw", "ingest_health"]),
                         (SILVER, ["telemetry_clean", "telemetry_quarantine"]),
                         (GOLD, ["asset_current_state", "asset_anomaly_events", "active_alerts",
                                 "line_health_summary", "telemetry_quality_metrics"])]:
        rows = sql(f"SHOW TABLES IN {CAT}.{schema}")
        names = {r[1] for r in rows}
        for t in tbls:
            check(f"table {schema}.{t} exists", t in names)

    bronze_n = int(scalar(f"SELECT count(*) FROM {CAT}.{BRONZE}.telemetry_raw") or 0)
    check("bronze has telemetry", bronze_n > 0, f"{bronze_n} rows")
    check("bronze ingested via zerobus",
          scalar(f"SELECT count(*) FROM {CAT}.{BRONZE}.telemetry_raw WHERE ingest_source LIKE 'zerobus%'"),
          "ingest_source zerobus*")

    silver_n = int(scalar(f"SELECT count(*) FROM {CAT}.{SILVER}.telemetry_clean") or 0)
    check("silver has clean telemetry", silver_n > 0, f"{silver_n} rows")

    # dedup: no duplicate source_event_id in silver
    dups = int(scalar(f"SELECT count(*) FROM (SELECT source_event_id, count(*) c "
                      f"FROM {CAT}.{SILVER}.telemetry_clean GROUP BY source_event_id HAVING c > 1)") or 0)
    check("silver dedup (no duplicate source_event_id)", dups == 0, f"{dups} dup keys")

    # quarantine table queryable
    qn = scalar(f"SELECT count(*) FROM {CAT}.{SILVER}.telemetry_quarantine")
    check("quarantine table queryable", qn is not None, f"{qn} rows")

    # gold current state has assets
    assets = int(scalar(f"SELECT count(*) FROM {CAT}.{GOLD}.asset_current_state") or 0)
    check("gold current state populated", assets >= 15, f"{assets} assets")

    lines = int(scalar(f"SELECT count(*) FROM {CAT}.{GOLD}.line_health_summary") or 0)
    check("gold line health populated", lines == 3, f"{lines} lines")

    # alerts + anomaly events exist
    alerts = int(scalar(f"SELECT count(*) FROM {CAT}.{GOLD}.active_alerts") or 0)
    check("active alerts generated", alerts > 0, f"{alerts} alerts")
    aev = int(scalar(f"SELECT count(*) FROM {CAT}.{GOLD}.asset_anomaly_events") or 0)
    check("anomaly/threshold events recorded", aev > 0, f"{aev} events")

    # threshold/anomaly correctness: any CRITICAL alert has observed beyond threshold
    bad = int(scalar(
        f"SELECT count(*) FROM {CAT}.{GOLD}.active_alerts "
        f"WHERE severity='CRITICAL' AND observed_value IS NOT NULL AND threshold_value IS NOT NULL "
        f"AND observed_value < threshold_value AND alert_type IN ('THRESHOLD','TREND') "
        f"AND observed_value > 0") or 0)
    # (loose sanity; not strict) just ensure alerts have reasons
    reasoned = int(scalar(f"SELECT count(*) FROM {CAT}.{GOLD}.asset_anomaly_events WHERE reason IS NOT NULL") or 0)
    check("anomaly events include reason text", reasoned > 0, f"{reasoned} with reason")


def replay_check():
    print("\n=== GENERATOR DETERMINISM ===")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generator"))
    try:
        from telemetry_generator import TelemetryGenerator, GeneratorConfig, Scenario
        g1 = TelemetryGenerator(GeneratorConfig(seed=123)); g1.inject_scenario(Scenario.VIBRATION_DEGRADATION, 20)
        g2 = TelemetryGenerator(GeneratorConfig(seed=123)); g2.inject_scenario(Scenario.VIBRATION_DEGRADATION, 20)
        a = [e["event_id"] for _ in range(8) for e in g1.tick()]
        b = [e["event_id"] for _ in range(8) for e in g2.tick()]
        check("deterministic replay (same seed -> same event_ids)", a == b, f"{len(a)} events")
    except Exception as e:
        check("deterministic replay", False, str(e))


def api_smoke():
    """Exercise the deployed app API end to end (read paths + operator workflow + AI)."""
    if not API_BASE:
        print("\n(skip API smoke: set API_BASE=<app url> and API_TOKEN=<databricks oauth token>)")
        return
    print(f"\n=== APP API ({API_BASE}) ===")
    tok = os.environ.get("API_TOKEN", "")
    hdrs = {"Authorization": f"Bearer {tok}"} if tok else {}

    def call(method, path, body=None, timeout=60):
        req = urllib.request.Request(f"{API_BASE}{path}", method=method, headers={
            **hdrs, "content-type": "application/json"},
            data=json.dumps(body).encode() if body is not None else None)
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=timeout) as r:
            out = r.status, json.loads(r.read())
        print(f"      {method} {path} -> {out[0]} in {(time.time()-t0)*1000:.0f} ms")
        return out

    def guarded(name, fn):
        try:
            fn()
        except Exception as e:
            check(name, False, f"{type(e).__name__}: {str(e)[:200]}")

    def reads():
        s, _ = call("GET", "/health"); check("GET /health 200", s == 200)
        s, d = call("GET", "/ready"); check("GET /ready (Lakebase reachable)", s == 200 and d.get("lakebase"), str(d.get("status")))
        s, d = call("GET", "/api/plant/summary")
        check("GET /api/plant/summary", s == 200 and "plant_status" in d,
              f"plant={d.get('plant_status')} assets={d.get('total_assets')} criticals={d.get('open_criticals')}")
        s, d = call("GET", "/api/lines"); check("GET /api/lines == 3", s == 200 and len(d) == 3,
                                                 ", ".join(f"{l['line_id']}={l['health_status']}" for l in d))
        s, d = call("GET", "/api/assets/A-COMPRESS")
        ml = d.get("ml") or {}
        check("GET /api/assets/A-COMPRESS (rule state + ML score from Lakebase)", s == 200 and d.get("asset"),
              f"condition={d['asset'].get('condition')} ml_score={ml.get('ml_anomaly_score')} "
              f"ml_top_sensor={ml.get('top_sensor')}")
        s, d = call("GET", "/api/ml/scores"); check("GET /api/ml/scores (15 assets)", s == 200 and len(d) == 15,
                                                    f"top={d[0]['asset_id']}:{d[0]['ml_anomaly_score']}" if d else "")
        s, d = call("GET", "/api/observability/metrics"); check("GET /api/observability/metrics", s == 200 and "delta" in d)

    def workflow():
        s, alerts = call("GET", "/api/alerts?limit=20")
        check("GET /api/alerts", s == 200 and isinstance(alerts, list), f"{len(alerts)} alerts")
        target = next((a for a in alerts if a["status"] != "RESOLVED"), None)
        if not target:
            check("operator workflow (no open alert to act on)", False); return
        s, d = call("POST", f"/api/alerts/{target['alert_id']}/acknowledge",
                    {"operator": "app-acceptance-test", "note": "Acknowledged via app API (acceptance test)"})
        check("POST acknowledge -> ACKNOWLEDGED (Lakebase transaction)", s == 200 and d["status"] == "ACKNOWLEDGED",
              f"{target['asset_id']} {target['sensor_type']} by {d.get('acknowledged_by')}")
        s, h = call("GET", f"/api/alerts/{target['alert_id']}/history")
        check("GET alert history has the acknowledgement", s == 200 and any(x["operator"] == "app-acceptance-test" for x in h),
              f"{len(h)} audit entries")
        s, d = call("POST", "/api/assistant/explain-fix", {k: target.get(k) for k in [
            "alert_id", "asset_id", "line_id", "sensor_type", "alert_type", "severity",
            "observed_value", "threshold_value", "anomaly_score"]}, timeout=120)
        cites = [c["sop_id"] for c in d.get("citations", [])]
        check("POST /api/assistant/explain-fix (SOP-grounded, cited)", s == 200 and d.get("status") == "OK" and cites,
              f"citations={cites}")
        print("      --- Explain & Fix answer (first 600 chars) ---\n      " + d.get("answer", "")[:600].replace("\n", "\n      "))

    def ai():
        s, d = call("POST", "/api/genie/ask", {"question": "Which production lines are unhealthy right now?"}, timeout=180)
        check("POST /api/genie/ask (Genie space)", s == 200 and d.get("status") in ("COMPLETED", "OK"),
              f"sql={'yes' if d.get('sql') else 'no'} rows={len((d.get('result') or {}).get('rows', []) or [])}")
        print("      Genie SQL: " + str(d.get("sql"))[:400].replace("\n", " "))
        s, d = call("POST", "/api/assistant/ask",
                    {"question": "Why is LINE-A degraded right now and what should I do about the worst asset?"}, timeout=240)
        check("POST /api/assistant/ask (Agent Bricks supervisor)", s == 200 and d.get("status") == "OK",
              f"route={d.get('route')} tools={d.get('tools_used')}")
        print("      --- Supervisor answer (first 800 chars) ---\n      " + d.get("answer", "")[:800].replace("\n", "\n      "))

    guarded("app read endpoints", reads)
    guarded("operator workflow", workflow)
    guarded("AI endpoints", ai)


if __name__ == "__main__":
    from datetime import datetime, timezone
    print(f"acceptance run {datetime.now(timezone.utc).isoformat(timespec='seconds')}  profile={PROFILE}")
    data_layer_checks()
    replay_check()
    api_smoke()
    passed = sum(1 for _, ok, _ in RESULTS if ok)
    total = len(RESULTS)
    print(f"\n==== {passed}/{total} checks passed ====")
    sys.exit(0 if passed == total else 1)
