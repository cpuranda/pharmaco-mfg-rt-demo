# Executed notebook: `08_genie_space`

> Exported from Databricks job run **24808449534624** / task run **992916949961813** — result **SUCCESS**, executed 2026-10-20T13:38:50+00:00 on serverless compute in workspace `https://fevm-hls-streaming-solution-demo.cloud.databricks.com`. Outputs below are the real cell outputs of that run.

# 08 · Genie — natural-language Q&A for plant managers

Creates (idempotently) a curated **AI/BI Genie space** over the governed gold tables —
operational gold, the LDP line KPIs and the ML anomaly scores — then runs a real
conversation through the **Genie Conversation API** (the same API the app's
"Ask" tab and the Agent Bricks supervisor use). Generated SQL and result rows are printed.

```python
%run ./_config
```

```python

```

**Output:**

```text
repo root : /Workspace/Users/chandrashekhar.purandare@databricks.com/pharmaco_mfg_rt_demo
catalog   : hls_streaming_solution_demo_catalog  schemas=['pharmaco_mfg_rt_bronze', 'pharmaco_mfg_rt_silver', 'pharmaco_mfg_rt_gold', 'pharmaco_mfg_rt_ldp', 'pharmaco_mfg_rt_ml', 'pharmaco_mfg_rt_knowledge']
run as    : chandrashekhar.purandare@databricks.com   at 2026-10-20T13:38:58+00:00
```

```python
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
```

**Output:**

```text
Genie space updated: 01f1cc7fa00816ba81cd394475f36651
tables: ['hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold.active_alerts', 'hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold.asset_anomaly_events', 'hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold.asset_current_state', 'hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold.asset_ml_anomaly_scores', 'hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_gold.line_health_summary', 'hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ldp.gold_line_kpis_1m']
```

```python
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
```

**Output:**

```text

==============================================================================
Conversation with follow-up (context retained)
==============================================================================

Q: Which production lines are unhealthy right now, and how many open critical alerts does each have?
   status=COMPLETED
   description: You want to see the production lines that are currently unhealthy, meaning their health status is either WATCH, DEGRADED, or CRITICAL, and for each of these lines, you want to know how many open critical alerts they have.
   generated SQL:
SELECT `line_id`, `health_status`, `open_criticals`
FROM `hls_streaming_solution_demo_catalog`.`pharmaco_mfg_rt_gold`.`line_health_summary`
WHERE `line_id` IS NOT NULL
  AND `health_status` IS NOT NULL
  AND `open_criticals` IS NOT NULL
  AND `health_status` IN ('WATCH', 'DEGRADED', 'CRITICAL')
ORDER BY `open_criticals` DESC, `line_id` ASC
   result (1 rows): ['line_id', 'health_status', 'open_criticals']
      ['LINE-A', 'CRITICAL', '1']
   A: Right now, **LINE-A** is unhealthy with health status **CRITICAL** and has **1** open critical alert. In the returned results, **LINE-A** is the only unhealthy production line listed.

Q: For the worst of those lines, which asset has the highest anomaly score and what is the recommended action?
   status=COMPLETED
   description: You want to find the asset with the highest anomaly score on the worst production line among those currently unhealthy, and see the recommended action for that asset's most recent open alert.
   generated SQL:
WITH `ranked_lines` AS (
  SELECT
    `line_id`,
    `health_status`,
    `open_criticals`,
    RANK() OVER (
      ORDER BY
        CASE `health_status`
          WHEN 'CRITICAL' THEN 3
          WHEN 'DEGRADED' THEN 2
          WHEN 'WATCH' THEN 1
          ELSE 0
        END DESC,
        `open_criticals` DESC,
        `line_id` ASC
    ) AS `worst_line_rank`
  FROM `hls_streaming_solution_demo_catalog`.`pharmaco_mfg_rt_gold`.`line_health_summary`
  WHERE `line_id` IS NOT NULL
    AND `health_status` IS NOT NULL
    AND `open_criticals` IS NOT NULL
    AND `health_status` IN ('WATCH', 'DEGRADED', 'CRITICAL')
), `worst_lines` AS (
  SELECT `line_id`
  FROM `ranked_lines`
  WHERE `worst_line_rank` = 1
), `ranked_assets` AS (
  SELECT
    `acs`.`line_id`,
    `acs`.`asset_id`,
    `acs`.`anomaly_score`,
    RANK() OVER (
      PARTITION BY `acs`.`line_id`
      ORDER BY `acs`.`anomaly_score` DESC, `acs`.`asset_id` ASC
    ) AS `asset_rank`
  FROM `hls_streaming_solution_demo_catalog`.`pharmaco_mfg_rt_gold`.`asset_current_state` AS `acs`
  INNER JOIN `worst_lines` AS `wl`
    ON `acs`.`line_id` = `wl`.`line_id`
  WHERE `acs`.`line_id` IS NOT NULL
    AND `acs`.`asset_id` IS NOT NULL
    AND `acs`.`anomaly_score` IS NOT NULL
), `latest_open_alerts` AS (
  SELECT
    `line_id`,
    `asset_id`,
    `recommended_action`,
    ROW_NUMBER() OVER (
      PARTITION BY `line_id`, `asset_id`
      ORDER BY COALESCE(`last_event_ts`, `updated_ts`, `created_ts`) DESC, `alert_id` ASC
    ) AS `alert_row_num`
  FROM `hls_streaming_solution_demo_catalog`.`pharmaco_mfg_rt_gold`.`active_alerts`
  WHERE `line_id` IS NOT NULL
    AND `asset_id` IS NOT NULL
    AND `status` IS NOT NULL
    AND `status` <> 'RESOLVED'
)
SELECT
  `ra`.`line_id`,
  `ra`.`asset_id`,
  `ra`.`anomaly_score`,
  `loa`.`recommended_action`
FROM `ranked_assets` AS `ra`
LEFT JOIN `latest_open_alerts` AS `loa`
  ON `ra`.`line_id` = `loa`.`line_id`
 AND `ra`.`asset_id` = `loa`.`asset_id`
 AND `loa`.`alert_row_num` = 1
WHERE `ra`.`asset_rank` = 1
ORDER BY `ra`.`line_id` ASC, `ra`.`asset_id` ASC
   result (1 rows): ['line_id', 'asset_id', 'anomaly_score', 'recommended_action']
      ['LINE-A', 'A-COMPRESS', '0.6510638297872341', 'Inspect cooling / reduce load; check setpoint']
   A: For the worst unhealthy line, asset **A-COMPRESS** on **LINE-A** has the highest anomaly score at **0.6510638297872341**. The recommended action for asset **A-COMPRESS** is **"Inspect cooling / reduce load; check setpoint"**.
```

```python
banner("Questions spanning ML scores and LDP KPIs")
for q in ["Which assets does the ML model currently flag as anomalous? Show the latest score per asset.",
          "What was the average availability and throughput percent of baseline per line in the last 30 minutes?"]:
    st = w.api_client.do("POST", f"{G}/start-conversation", body={"content": q})
    show_answer(q, wait_msg(st["conversation_id"], st["message_id"]), st["conversation_id"])
print(f"\nGenie space id: {space_id}")
```

**Output:**

```text

==============================================================================
Questions spanning ML scores and LDP KPIs
==============================================================================

Q: Which assets does the ML model currently flag as anomalous? Show the latest score per asset.
   status=COMPLETED
   description: You want to see the latest machine learning anomaly scores for each asset where the model currently flags the asset as anomalous.
   generated SQL:
WITH `latest_per_asset` AS (
  SELECT
    `asset_id`,
    `line_id`,
    `window_start`,
    `window_end`,
    `ml_anomaly_score`,
    `ml_is_anomaly`,
    `top_sensor`,
    `model_name`,
    `model_version`,
    `scored_ts`,
    ROW_NUMBER() OVER (
      PARTITION BY `asset_id`
      ORDER BY `scored_ts` DESC, `window_end` DESC
    ) AS `rn`
  FROM `hls_streaming_solution_demo_catalog`.`pharmaco_mfg_rt_gold`.`asset_ml_anomaly_scores`
  WHERE `asset_id` IS NOT NULL
    AND `scored_ts` IS NOT NULL
    AND `window_end` IS NOT NULL
)
SELECT
  `asset_id`,
  `line_id`,
  `window_start`,
  `window_end`,
  `scored_ts`,
  `ml_anomaly_score`,
  `ml_is_anomaly`,
  `top_sensor`,
  `model_name`,
  `model_version`
FROM `latest_per_asset`
WHERE `rn` = 1
  AND `ml_is_anomaly` = true
ORDER BY `ml_anomaly_score` DESC, `asset_id` ASC
   result (1 rows): ['asset_id', 'line_id', 'window_start', 'window_end', 'scored_ts', 'ml_anomaly_score', 'ml_is_anomaly', 'top_sensor', 'model_name', 'model_version']
      ['A-COMPRESS', 'LINE-A', '2026-10-20T13:30:20.000Z', '2026-10-20T13:30:40.000Z', '2026-10-20T13:37:24.387Z', '1.0', 'true', 'temperature', 'hls_streaming_solution_demo_catalog.pharmaco_mfg_rt_ml.asset_anomaly_detector', '4']
   A: The ML model currently flags **asset A-COMPRESS** on **LINE-A** as anomalous, with the latest anomaly score of **1.0**. The latest scoring record for **asset A-COMPRESS** was generated at **2026-10-20T13:37:24.387Z** for the window **2026-10-20T13:30:20.000Z** to **2026-10-20T13:30:40.000Z**, and **temperature** was the top contributing sensor.

Q: What was the average availability and throughput percent of baseline per line in the last 30 minutes?
   status=COMPLETED
   description: You want to see the average availability and average throughput percentage of baseline for each production line over the last 30 minutes.
   generated SQL:
SELECT `line_id`, AVG(`availability`) AS `avg_availability`, AVG(`throughput_pct_of_baseline`) AS `avg_throughput_pct_of_baseline` FROM `hls_streaming_solution_demo_catalog`.`pharmaco_mfg_rt_ldp`.`gold_line_kpis_1m` WHERE `line_id` IS NOT NULL AND `availability` IS NOT NULL AND `throughput_pct_of_baseline` IS NOT NULL AND `minute` >= dateadd(MINUTE, -30, current_timestamp()) AND `minute` <= current_timestamp() GROUP BY `line_id` ORDER BY `line_id`
   result (3 rows): ['line_id', 'avg_availability', 'avg_throughput_pct_of_baseline']
      ['LINE-A', '0.9728216558954736', '94.61176470588236']
      ['LINE-B', '0.9824208637703448', '107.24117647058824']
      ['LINE-C', '0.9696316435070759', '105.79411764705883']
   A: Over the last **30 minutes**, average availability was **97.28%** for **LINE-A**, **98.24%** for **LINE-B**, and **96.96%** for **LINE-C**; average throughput versus baseline was **94.61%**, **107.24%**, and **105.79%** respectively. Across the 3 lines shown, **LINE-B** had the highest average availability and throughput versus baseline, while **LINE-A** was the only line averaging below its baseline throughput.

Genie space id: 01f1cc7fa00816ba81cd394475f36651
```
