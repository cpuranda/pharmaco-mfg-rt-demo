# Demo Script — PharmaCo Manufacturing Real-Time Line Monitoring

A presenter runbook for a ~20–25 min live demo. Audience: PharmaCo manufacturing /
shop-floor connectivity / data platform stakeholders.

- **App URL:** https://pharmaco-mfg-rt-demo-7474651570340482.aws.databricksapps.com
- **Workspace:** `fevm-hls-streaming-solution-demo` · CLI profile `fe-vm-hls-streaming-solution-demo`
- **Operational Lakeflow Job id (pharmaco_mfg_rt_operational):** `1012398663888207`
- **Everything is SYNTHETIC** — say this once, up front, and it is labelled in the UI.

---

## 0. The one-sentence message (say this first and last)

> "PharmaCo can start with **one governed manufacturing event flow** — device to lakehouse to
> operator — and reuse that **same foundation** for Unified Namespace, digital twins, alarm
> management, operational analytics, and future AI. You don't need a different stack for each."

And the honest framing on architecture fit:

> "Zerobus Ingest is the right tool when **Databricks is the primary destination** and you can ingest
> directly. It complements, not replaces, brokers — if you need multi-consumer fan-out or a
> constrained source, keep the broker. Here we show the direct path."

---

## 1. Pre-demo checklist (do this 10–15 min before)

Run from a terminal with the CLI profile active. (The `!`-prefix works inside Claude Code too.)

```bash
export P=fe-vm-hls-streaming-solution-demo
export URL=https://pharmaco-mfg-rt-demo-7474651570340482.aws.databricksapps.com

# 1) Make sure the pipeline job is scheduled/unpaused and kick one cycle now:
databricks jobs run-now 1012398663888207 -p $P

# 2) Confirm the app is up (should print RUNNING):
databricks apps get pharmaco-mfg-rt-demo -p $P -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['app_status']['state'])"

# 3) Open the app in your browser and log in with Databricks SSO:
open $URL   # macOS
```

In the browser:
- Go to **Scenario Controls → Start** (operator name is remembered). Leave scenario = *normal*.
- Watch **Plant Overview** for ~1–2 min until the "stale data" banner clears and tiles populate
  (trigger the operational job with `jobs run-now` or unpause its 5-min schedule; the app serving-sync refreshes Lakebase every ~5 s).
- Set the **Refresh** control (top-right) to **5s**.

You're ready when Plant Overview shows 15/15 assets and three line cards with live "Updated Ns ago".

> Tip: keep the browser at 100% zoom; the UI is laptop-friendly with no horizontal scroll.

---

## 2. Live walkthrough (the flow + talk track)

### Scene 1 — Plant Overview (2–3 min)  →  *"Which lines are healthy right now?"*
- Point out: overall plant status, running/idle/faulted tiles, open warnings/criticals, **latest event
  time + freshness** (top-right) and **API latency shown separately**.
- Three line cards: LINE-A (OSD Tablet), LINE-B (Sterile Vial), LINE-C (Packaging) — health color,
  throughput vs baseline, running count, open alerts.
- **Say:** "This is the plant manager's single pane — one governed event flow behind it."

### Scene 2 — Inject a temperature excursion (3–4 min)  →  *"Which assets are approaching unsafe conditions?"*
- Go to **Scenario Controls** → select **temperature excursion** → **Apply** (rate can stay 1/s).
- **Say:** "We're now simulating a heat rise on a tablet press. Watch the asset condition and alerts
  evolve — this is the same flow, no new pipeline."
- Wait ~1–2 pipeline cycles, return to **Plant Overview** → open **LINE-A** → open the affected asset
  (**A-COMPRESS**).
- In **Asset Detail**: show current sensor values with **threshold bands**, the **temperature trend
  chart** crossing the nominal max, and the **anomaly score** rising. Show the operating state.

### Scene 3 — Alert Console + operator action (4–5 min)  →  *"Which alarms are critical/new/acknowledged? What do I investigate first?"*
- Open **Alert Console**. Filter by **severity = CRITICAL**; sort by line/asset.
- Show each alert's **recommended action** and **root-cause hint** (e.g. "Inspect cooling / reduce
  load"), plus observed vs threshold and confidence.
- **Acknowledge** the temperature alert (enter a note) → status flips to ACKNOWLEDGED with your name +
  timestamp. Expand **history**.
- **Resolve** another as *maintenance* → status MAINTENANCE.
- **Say:** "This is a transactional workflow on Lakebase — the operator's acknowledgement is written
  back and survives the next pipeline update. Full history stays in Delta for audit."

### Scene 4 — Line slowdown (3–4 min)  →  *"Which line is slowing down, and why?"*
- **Scenario Controls** → **line slowdown** → you'll get a **confirmation dialog** (high-volume /
  multi-line) → confirm.
- **Say:** "This deliberately requires confirmation — a guardrail against high-impact actions."
- Return to **Plant Overview / Line View**: the target line's **throughput drops below baseline**,
  health goes DEGRADED/CRITICAL, and the **slowdown hint** names the most likely contributing asset.

### Scene 4b — ML anomaly model: "earlier than the band" (2 min)  →  *"Can we see trouble before it breaches spec?"*

* Open **Asset Detail** for an asset under a scenario (e.g. **B-LYO** during *pressure instability*, or
  **B-FILL** during *vibration degradation*). The **Condition** card now shows two signals side by side:
  the rule-based condition (validated band) and the **ML anomaly model** score with the sensor that drove it.
* **Say:** "The band rule only fires once the average leaves spec. This model learned each asset's own
  healthy fingerprint, so it flags drift that's still *inside* spec — like an oscillating lyophilizer
  pressure loop that the band never catches — and on temperature and vibration it fires about
  20 seconds earlier. It's an MLflow model registered in Unity Catalog, scored by the same Lakeflow Job that runs
  the pipeline, and served from Lakebase."
* Evidence to quote: precision 0.98, recall 0.76 (F1 0.86) vs band-rule recall 0.66 (F1 0.79) — notebook 06.

### Scene 5 — Architecture & Observability (3–4 min)  →  *"Can one event stream serve live action AND history?"*
- Open **Architecture & Observability**. Walk the flow left→right:
  **Generator → Zerobus Ingest → Structured Streaming → Gold Delta → Lakebase → App**.
- Point to: records in bronze/silver, quarantined/anomaly counts, **pipeline latency vs Lakebase
  serving latency shown separately**, and the streaming job status.
- **Say:** "Same governed event: live operational serving in Lakebase, and the complete history in
  Delta for analytics, digital twins, and AI — one foundation, many workloads."

### Scene 6 — Ask Genie (natural-language Q&A) (2–3 min)  →  *"Can business users self-serve?"*
- Open **Ask Genie**. Click a suggestion chip or type a question, e.g.:
  - "Which production lines are unhealthy right now?"
  - "How many open critical alerts are there per line?"
  - "Which line has the lowest throughput versus baseline?"
- Show the natural-language answer, the **generated SQL** (expand "Show SQL"), and the result table.
  Ask a **follow-up** (same conversation) to show context is retained.
- **Say:** "This is Databricks AI/BI Genie over the *same governed gold tables* — no new
  pipeline, no new copy of the data. A plant manager can just ask. It's the on-ramp to the AI
  workflows on this foundation." (All answers are on synthetic data.)

### Close (1 min)
- Repeat the one-sentence message. Offer the reuse roadmap: Unified Namespace, digital twins, alarm
  management, operational analytics, AI copilots — all on this same governed flow.

---

## 3. Manual steps — starting / stopping (jobs + generator)

### Generator (synthetic telemetry) — from the UI (preferred)
- **Start / Stop:** Scenario Controls → **Start** / **Stop**.
- **Change scenario:** pick one of *normal, temperature excursion, vibration degradation, pressure
  instability, line slowdown, alarm burst, recovery* → Apply. (alarm burst & line slowdown ask to
  confirm.)

### Generator — from CLI (if the UI ingest shows unavailable)
```bash
export P=fe-vm-hls-streaming-solution-demo
export URL=https://pharmaco-mfg-rt-demo-7474651570340482.aws.databricksapps.com
TOKEN=$(databricks auth token -p $P | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  $URL/api/demo/start -d '{"operator":"presenter"}'
# stop:
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  $URL/api/demo/stop  -d '{"operator":"presenter"}'
```

### Pipeline / streaming job (id 1012398663888207)
```bash
export P=fe-vm-hls-streaming-solution-demo
# Trigger a processing cycle immediately (otherwise it runs every 5 min when the schedule is unpaused):
databricks jobs run-now 1012398663888207 -p $P

# PAUSE the 5-minute schedule (do this after the demo to save compute):
databricks jobs update --json '{"job_id":1012398663888207,"new_settings":{"schedule":{"quartz_cron_expression":"0 0/5 * * * ?","timezone_id":"UTC","pause_status":"PAUSED"}}}' -p $P

# RESUME the schedule before the next demo (same command, pause_status:"UNPAUSED").
```

### Optional — lower-latency "live" streaming (for a high-touch presentation)
The default pipeline is a reliable 1-minute scheduled job (~1–1.5 min freshness). For ~5–10 s
latency, reconfigure the job to a classic single-node cluster with `mode=stream` /
`Trigger.ProcessingTime(5s)` — note it may self-terminate after a few minutes and need a restart, so
only use it when you're actively driving the demo. (Config in `README.md`; ask if you want it wired
as a second job so you can switch without reconfiguring.)

---

## 4. During-demo troubleshooting (quick fixes)

| Symptom | Fix |
|---|---|
| "Stale data" banner won't clear | Scenario Controls → **Start** the generator; `jobs run-now 1012398663888207`; wait one cycle. |
| Tiles empty / "backend unavailable" | Reload the page; check `.../health` and `.../ready` (should be 200). App logs: append `/logz` to the URL. |
| Ingest shows "unavailable" in Scenario Controls | Use the CLI start command in §3; check the `reason` field. |
| Alerts not changing after a scenario | Scenarios evolve over the configured duration; wait 1–2 pipeline cycles (≤ ~2 min). |
| Numbers look "too hot" (many criticals) | Set scenario back to **normal**, or restart with a fresh seed; alerts age out as readings return to band. |

---

## 5. Reset between runs / after the demo
- **Between runs:** Scenario Controls → set **normal**, or **Stop** then **Start** to calm the plant.
- **After the demo:** **Stop** the generator (UI) **and PAUSE** the schedule (§3) to stop consuming
  serverless compute. The app stays up (cheap) and Lakebase can optionally be stopped:
  `databricks database update-database-instance pharmaco-mfg-rt-demo-lb --stopped -p $P`.
- **Full teardown:** see `README.md` §6 / `docs/IMPLEMENTATION_REPORT.md` §11 (removes only this
  demo's assets).

---

## 6. FAQ the PharmaCo team may ask (crib notes)
- **"Is this real-time?"** Near-real-time here (~1–1.5 min, reliable serverless schedule; the app's
  serving layer refreshes in seconds). Sub-10s is achievable with classic ProcessingTime streaming;
  true millisecond bus-to-bus is Spark Real-Time Mode's domain (Kafka/Event Hubs), which this
  foundation extends to.
- **"Do we have to rip out our brokers?"** No. Zerobus is for direct-to-lakehouse where Databricks is
  the destination; keep brokers for multi-consumer / constrained sources.
- **"Is our data safe / governed?"** Everything is Unity Catalog-governed; bronze/silver/gold
  separation; Lakebase holds only the operational serving + workflow state; history stays in Delta.
- **"Is this production-ready?"** It's a governed demo. Production adds DR/backup, capacity sizing,
  scheduling/alerting, and validation per your GxP requirements — call these out honestly.
- **"Can it do AI?"** Yes — the **Ask Genie** tab already lets anyone query the governed gold
  tables in natural language (AI/BI Genie). The same gold/event history also feeds anomaly ML,
  predictive maintenance, and LLM copilots — all without a new pipeline.

## 7. Genie space (behind "Ask Genie")
- Space: **"PharmaCo Manufacturing — Real-Time Ops (SYNTHETIC)"** (id `01f1cc7fa00816ba81cd394475f36651`),
  curated over the four gold tables with sample questions + domain instructions. It's bound to the app
  as a resource (the app's service principal has CAN RUN). To broaden or retrain it, open the space in
  the workspace (Genie) and add tables/instructions/sample questions — the app picks up changes live.
