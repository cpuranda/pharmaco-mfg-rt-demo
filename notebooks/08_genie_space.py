# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Genie — natural-language Q&A for plant managers
# MAGIC
# MAGIC Creates (idempotently) a curated **AI/BI Genie space** over the governed gold tables —
# MAGIC operational gold, the LDP line KPIs and the ML anomaly scores — then runs a real
# MAGIC conversation through the **Genie Conversation API** (the same API the app's
# MAGIC "Ask" tab and the Agent Bricks supervisor use). Generated SQL and result rows are printed.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

def hexid():
    return uuid.uuid4().hex

tables = {
    f"{CATALOG}.{GOLD}.active_alerts":
        "Current + recently resolved alerts. status NEW/ACKNOWLEDGED/RESOLVED/MAINTENANCE; severity "
        "WARNING/CRITICAL; recommended_action, root_cause_hint. Open alerts = status <> 'RESOLVED'.",
    f"{CATALOG}.{GOLD}.asset_anomaly_events":
        "Append-only anomaly/threshold event history: alert_type, severity, observed vs threshold.",
    f"{CATALOG}.{GOLD}.asset_current_state":
        "Latest state per asset: sensor values, operating_state, rule-based anomaly_score 0-1, "
        "condition HEALTHY/WATCH/DEGRADED/CRITICAL, active_alert_count.",
    f"{CATALOG}.{GOLD}.asset_ml_anomaly_scores":
        "ML anomaly-detector (IsolationForest, UC model asset_anomaly_detector) score per asset per "
        "20 s window; ml_anomaly_score 0-1, ml_is_anomaly. Use the latest scored_ts per asset.",
    f"{CATALOG}.{GOLD}.line_health_summary":
        "One row per production line: health_status, throughput_pct vs baseline, asset counts, "
        "open_warnings/open_criticals, is_slowing_down, slowdown_hint.",
    f"{CATALOG}.{LDP}.gold_line_kpis_1m":
        "Per line per minute KPIs: avg throughput, throughput_pct_of_baseline, availability "
        "(share of RUNNING readings), out_of_band_rate. Use for trends over time.",
}
questions = [
    "Which production lines are unhealthy right now?",
    "How many open critical alerts are there per line?",
    "Which assets does the ML model currently flag as anomalous?",
    "Which line has the lowest throughput versus baseline?",
    "Show availability per line over the last 30 minutes",
    "Which assets have the highest anomaly score?",
]
serialized = {
    "version": 2,
    "config": {"sample_questions": sorted([{"id": hexid(), "question": [q]} for q in questions],
                                          key=lambda x: x["id"])},
    "data_sources": {"tables": [{"identifier": t, "description": [d]}
                                for t, d in sorted(tables.items())]},
    "instructions": {"text_instructions": [{"id": hexid(), "content": [
        "SYNTHETIC pharmaceutical plant 'PharmaCo' with 3 lines: LINE-A (OSD tablet), LINE-B "
        "(sterile vial), LINE-C (packaging). Asset types: A-DISPENSE dispenser, A-GRANULATE granulator, "
        "A-BLEND blender, A-COMPRESS tablet press (not an air compressor), A-COAT coater, A-INSPECT vision "
        "inspector, B-WASH vial washer, B-DEPYRO depyrogenation tunnel, B-FILL aseptic filler, B-STOPPER "
        "stoppering unit, B-LYO lyophilizer, C-LABEL labeler, C-CARTON cartoner, C-SERIALIZE serialization "
        "unit, C-CASEPACK case packer. Sensors: temperature(degC), vibration(mm_s), "
        "pressure(bar), motor_current(amp), speed(rpm), throughput(units_min), energy(kwh). "
        "'Unhealthy' lines have health_status IN ('WATCH','DEGRADED','CRITICAL'). 'Open' alerts "
        "have status <> 'RESOLVED'. For ML questions use asset_ml_anomaly_scores, latest row per "
        "asset by scored_ts then window_end. All data is synthetic."]}]},
}

existing = w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
match = [s for s in existing if s.get("title") == GENIE_TITLE]
body = {"title": GENIE_TITLE, "warehouse_id": WAREHOUSE_ID,
        "description": "Ask about live line health, asset state, alerts and ML anomaly scores (synthetic).",
        "serialized_space": json.dumps(serialized)}
if match:
    space_id = match[0]["space_id"]
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
    print(f"Genie space updated: {space_id}")
else:
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces",
                               body={**body, "parent_path": f"/Users/{ME}/pharmaco_mfg_rt_demo"})["space_id"]
    print(f"Genie space created: {space_id}")
dbutils.jobs.taskValues.set("genie_space_id", space_id)
print("tables:", sorted(tables))

# COMMAND ----------

G = f"/api/2.0/genie/spaces/{space_id}"

def wait_msg(conv_id, msg_id, timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        m = w.api_client.do("GET", f"{G}/conversations/{conv_id}/messages/{msg_id}")
        if m["status"] in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
            return m
        time.sleep(3)
    raise TimeoutError(msg_id)

def show_answer(question, m, conv_id):
    print(f"\nQ: {question}\n   status={m['status']}")
    for att in m.get("attachments") or []:
        if att.get("text"):
            print(f"   A: {att['text'].get('content','')[:800]}")
        if att.get("query"):
            print(f"   description: {att['query'].get('description','')}")
            print(f"   generated SQL:\n{att['query']['query']}")
            r = w.api_client.do("GET", f"{G}/conversations/{conv_id}/messages/{m['message_id'] if 'message_id' in m else m['id']}"
                                       f"/attachments/{att['attachment_id']}/query-result")
            sr = r.get("statement_response", {})
            cols = [c["name"] for c in sr.get("manifest", {}).get("schema", {}).get("columns", [])]
            data = sr.get("result", {}).get("data_array", []) or []
            print(f"   result ({len(data)} rows): {cols}")
            for row in data[:10]:
                print("     ", row)

banner("Conversation with follow-up (context retained)")
q1 = "Which production lines are unhealthy right now, and how many open critical alerts does each have?"
st = w.api_client.do("POST", f"{G}/start-conversation", body={"content": q1})
conv = st["conversation_id"]
show_answer(q1, wait_msg(conv, st["message_id"]), conv)

q2 = "For the worst of those lines, which asset has the highest anomaly score and what is the recommended action?"
m2 = w.api_client.do("POST", f"{G}/conversations/{conv}/messages", body={"content": q2})
show_answer(q2, wait_msg(conv, m2.get("message_id") or m2["id"]), conv)

# COMMAND ----------

banner("Questions spanning ML scores and LDP KPIs")
for q in ["Which assets does the ML model currently flag as anomalous? Show the latest score per asset.",
          "What was the average availability and throughput percent of baseline per line in the last 30 minutes?"]:
    st = w.api_client.do("POST", f"{G}/start-conversation", body={"content": q})
    show_answer(q, wait_msg(st["conversation_id"], st["message_id"]), st["conversation_id"])
print(f"\nGenie space id: {space_id}")
