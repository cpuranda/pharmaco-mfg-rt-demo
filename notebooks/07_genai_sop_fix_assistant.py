# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · GenAI — SOP-grounded "Explain & Fix" assistant (Vector Search + Foundation Model)
# MAGIC
# MAGIC An alert tells the operator *what* is wrong; in a GxP plant they also need *how to fix it,
# MAGIC according to the approved procedure*. This notebook builds the retrieval-augmented
# MAGIC Fix Assistant used by the app's **Explain & Fix** button:
# MAGIC
# MAGIC 1. 10 synthetic SOPs → UC Volume (raw docs) + `sop_chunks` Delta table (42 chunks, CDF on)
# MAGIC 2. **Vector Search** Delta-Sync index with managed embeddings (`databricks-gte-large-en`)
# MAGIC 3. Retrieval quality check — hit@3 on a labelled question set
# MAGIC 4. Grounded remediation for the **live** A-COMPRESS CRITICAL alert with
# MAGIC    `databricks-claude-sonnet-4-5`, citing SOP ids
# MAGIC 5. `ai_query()` — GenAI in SQL: a shift-handover summary straight from gold tables

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import shutil, glob
banner("1. SOP library → UC Volume + Delta chunks table")
sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{KNOWLEDGE}.sop_docs COMMENT 'Raw synthetic SOP markdown'")
vol = f"/Volumes/{CATALOG}/{KNOWLEDGE}/sop_docs"
for f in sorted(glob.glob(f"{REPO_ROOT}/knowledge/sops/*.md")):
    shutil.copy(f, f"{vol}/{os.path.basename(f)}")
print("volume files:", sorted(os.listdir(vol)))

chunks = json.load(open(f"{REPO_ROOT}/knowledge/sop_chunks.json"))
sql(f"""CREATE TABLE IF NOT EXISTS {SOP_TABLE} (
          chunk_id STRING NOT NULL, sop_id STRING, title STRING, revision STRING, section STRING,
          asset_types STRING, sensor_types STRING, scenarios STRING, source_file STRING, content STRING,
          CONSTRAINT sop_chunks_pk PRIMARY KEY (chunk_id))
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
        COMMENT 'Chunked synthetic SOPs for Vector Search (Fix Assistant + Knowledge Assistant)'""")
spark.createDataFrame(chunks).select(
    "chunk_id", "sop_id", "title", "revision", "section", "asset_types", "sensor_types",
    "scenarios", "source_file", "content").createOrReplaceTempView("_chunks")
sql(f"MERGE INTO {SOP_TABLE} t USING _chunks s ON t.chunk_id = s.chunk_id "
    f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
display(sql(f"SELECT sop_id, title, count(*) AS chunks FROM {SOP_TABLE} GROUP BY 1, 2 ORDER BY 1"))

# COMMAND ----------

banner("2. Vector Search endpoint + Delta-Sync index (managed embeddings)")
def vs_get(path):
    try:
        return w.api_client.do("GET", path)
    except Exception:
        return None

if not vs_get(f"/api/2.0/vector-search/endpoints/{VS_ENDPOINT}"):
    w.api_client.do("POST", "/api/2.0/vector-search/endpoints",
                    body={"name": VS_ENDPOINT, "endpoint_type": "STANDARD"})
    print(f"endpoint {VS_ENDPOINT} create requested")
for _ in range(90):
    st = vs_get(f"/api/2.0/vector-search/endpoints/{VS_ENDPOINT}")["endpoint_status"]["state"]
    if st == "ONLINE":
        break
    print(f"  endpoint state={st}"); time.sleep(20)
print(f"endpoint {VS_ENDPOINT}: ONLINE")

if not vs_get(f"/api/2.0/vector-search/indexes/{SOP_INDEX}"):
    w.api_client.do("POST", "/api/2.0/vector-search/indexes", body={
        "name": SOP_INDEX, "endpoint_name": VS_ENDPOINT, "primary_key": "chunk_id",
        "index_type": "DELTA_SYNC",
        "delta_sync_index_spec": {"source_table": SOP_TABLE, "pipeline_type": "TRIGGERED",
            "embedding_source_columns": [{"name": "content",
                                          "embedding_model_endpoint_name": EMBED_ENDPOINT}]}})
    print("index create requested")
else:
    try:
        w.api_client.do("POST", f"/api/2.0/vector-search/indexes/{SOP_INDEX}/sync")
    except Exception as e:
        print("sync note:", str(e)[:120])
for _ in range(90):
    s = vs_get(f"/api/2.0/vector-search/indexes/{SOP_INDEX}")["status"]
    if s.get("ready") and (s.get("indexed_row_count") or 0) >= len(chunks):
        break
    print(f"  index {s.get('detailed_state')} rows={s.get('indexed_row_count')}"); time.sleep(20)
print(f"index {SOP_INDEX}: ready={s.get('ready')} indexed_rows={s.get('indexed_row_count')}")

# COMMAND ----------

COLS = ["chunk_id", "sop_id", "title", "section", "content"]
def retrieve(q, k=5):
    r = w.api_client.do("POST", f"/api/2.0/vector-search/indexes/{SOP_INDEX}/query",
                        body={"columns": COLS, "query_text": q, "num_results": k})
    names = [c["name"] for c in r["manifest"]["columns"]]
    return [dict(zip(names, row)) for row in r["result"].get("data_array", [])]

banner("3. Retrieval quality — hit@3 on labelled operator questions")
EVAL = [
    ("Tablet press A-COMPRESS running hot above 75 degC", "SOP-TEMP-014"),
    ("Aseptic filler bearing vibration trending up", "SOP-VIB-021"),
    ("Lyophilizer chamber pressure oscillating", "SOP-PRES-009"),
    ("Line throughput dropped to 60% of baseline", "SOP-THRU-033"),
    ("Many alarms at once on the sterile line - alarm flood", "SOP-ALM-002"),
    ("Return equipment to service after maintenance", "SOP-REC-011"),
    ("Motor current higher than normal on the coater drive", "SOP-MTR-007"),
    ("When must a deviation be raised for an out-of-band reading", "SOP-GEN-001"),
]
rows, hits = [], 0
for q, exp in EVAL:
    got = [r["sop_id"] for r in retrieve(q, 3)]
    hit = exp in got; hits += hit
    rows.append((q, exp, ", ".join(got), hit))
display(spark.createDataFrame(rows, "question string, expected string, top3 string, hit boolean"))
print(f"hit@3 = {hits}/{len(EVAL)} = {hits/len(EVAL):.0%}")

# COMMAND ----------

banner("4. Explain & Fix — live CRITICAL alert from gold, grounded on retrieved SOPs")
alert = sql(f"""SELECT a.*, c.condition FROM {CATALOG}.{GOLD}.active_alerts a
                LEFT JOIN {CATALOG}.{GOLD}.asset_current_state c USING (asset_id)
                WHERE a.status <> 'RESOLVED' AND a.severity = 'CRITICAL'
                ORDER BY (a.asset_id = 'A-COMPRESS') DESC, a.anomaly_score DESC LIMIT 1""").first().asDict()
print({k: alert[k] for k in ["asset_id", "line_id", "sensor_type", "alert_type", "severity",
                             "observed_value", "threshold_value", "anomaly_score"]})
FIX_SYSTEM = (  # same prompt as app/server/assistant.py
    "You are a pharmaceutical manufacturing operations assistant for a real-time line "
    "monitoring system. Using ONLY the provided SOP excerpts, give the operator a concise, "
    "actionable remediation for the specific alert. Structure the answer as: a one-line "
    "assessment, then 'Immediate actions' (numbered, from the SOPs), then 'Likely causes', "
    "then 'Escalation' if warranted. Cite the SOP id in square brackets, e.g. [SOP-TEMP-014], "
    "next to the steps that use it. If the SOPs do not cover the issue, say so plainly. "
    "Never invent procedures or thresholds that are not in the excerpts. All data is synthetic.")
q = (f"How to diagnose and fix a {alert['severity']} {alert['sensor_type']} {alert['alert_type']} "
     f"issue on {alert['asset_id']} ({alert['line_id']}) tablet press")
ctx_chunks = retrieve(q, 6)
print("retrieved:", [(c["sop_id"], c["section"], round(float(c.get("score", 0)), 3)) for c in ctx_chunks])
context = "\n\n---\n\n".join(f"[{c['sop_id']} · {c['section']}]\n{c['content']}" for c in ctx_chunks)
t0 = time.time()
resp = w.api_client.do("POST", f"/serving-endpoints/{LLM_ENDPOINT}/invocations", body={
    "messages": [{"role": "system", "content": FIX_SYSTEM},
                 {"role": "user", "content":
                  f"Alert: {alert['severity']} {alert['alert_type']} on asset {alert['asset_id']} "
                  f"(line {alert['line_id']}). Sensor={alert['sensor_type']}, observed="
                  f"{alert['observed_value']}, threshold={alert['threshold_value']}, anomaly_score="
                  f"{alert['anomaly_score']}, condition={alert['condition']}.\n\nSOP excerpts:\n"
                  f"{context}\n\nGive the operator the remediation now."}],
    "max_tokens": 700, "temperature": 0.1})
answer = resp["choices"][0]["message"]["content"]
print(f"\n--- {LLM_ENDPOINT} answer ({time.time()-t0:.1f}s) ---\n{answer}")
import re
cited = sorted(set(re.findall(r"SOP-[A-Z]+-\d{3}", answer)))
print(f"\ncited SOPs: {cited}")
# grounded = every cited SOP id is either a retrieved chunk or referenced inside retrieved text
grounded_ids = {c["sop_id"] for c in ctx_chunks} | set(re.findall(r"SOP-[A-Z]+-\d{3}", context))
assert cited and set(cited) <= grounded_ids, f"ungrounded citation(s): {set(cited) - grounded_ids}"
print(f"CHECK PASSED: all cited SOPs {cited} come from the retrieved context (no invented sources).")

# COMMAND ----------

banner("5. ai_query() — shift-handover summary generated in SQL from gold")
display(sql(f"""
  SELECT ai_query('{LLM_ENDPOINT}',
    concat('You are a plant shift lead. In 3 bullet points, summarise for the next shift which ',
           'lines need attention and why, using only this data (synthetic): ',
           to_json(collect_list(named_struct('line', line_id, 'health', health_status,
                   'throughput_pct', throughput_pct, 'open_criticals', open_criticals,
                   'hint', slowdown_hint))))) AS shift_handover
  FROM {CATALOG}.{GOLD}.line_health_summary"""))
