# Executed notebook: `09_agent_bricks_supervisor`

> Exported from Databricks job run **24808449534624** / task run **677044482310115** — result **SUCCESS**, executed 2026-10-20T13:40:21+00:00 on serverless compute in workspace `https://fevm-hls-streaming-solution-demo.cloud.databricks.com`. Outputs below are the real cell outputs of that run.

# 09 · Agent Bricks — Knowledge Assistant + Multi-Agent Supervisor

The managed, governed version of the agentic layer:
* **Knowledge Assistant** `pharmaco-mfg-sop-ka` over the SOP Vector Search index (notebook 07)
* **Multi-Agent Supervisor** `pharmaco-mfg-supervisor` with two tools:
  `sop_ka` (procedures) and `genie_data` (the Genie space from notebook 08 — live plant data)

The supervisor answers *"why is this happening AND what do I do"* in one cited answer.
The app's **Ask the Assistant** tab calls this supervisor endpoint.
Provisioning code: `knowledge/create_agent_bricks.py` (Agent Bricks Beta APIs).

```python
%pip install -q databricks-sdk==0.128.0
%restart_python
```

**Output:**

```text
ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
googleapis-common-protos 1.62.0 requires protobuf!=3.20.0,!=3.20.1,!=4.21.1,!=4.21.2,!=4.21.3,!=4.21.4,!=4.21.5,<5.0.0.dev0,>=3.19.5, but you have protobuf 6.33.6 which is incompatible.
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

```python
%run ./_config
```

```python

```

**Output:**

```text
repo root : /Workspace/Users/chandrashekhar.purandare@databricks.com/pharmaco_mfg_rt_demo
catalog   : hls_streaming_solution_demo_catalog  schemas=['pharmaco_mfg_rt_bronze', 'pharmaco_mfg_rt_silver', 'pharmaco_mfg_rt_gold', 'pharmaco_mfg_rt_ldp', 'pharmaco_mfg_rt_ml', 'pharmaco_mfg_rt_knowledge']
run as    : chandrashekhar.purandare@databricks.com   at 2026-10-20T13:40:48+00:00
```

```python
spaces = w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
GENIE_SPACE_ID = next(s["space_id"] for s in spaces if s.get("title") == GENIE_TITLE)
os.environ.update({"DEMO_CATALOG": CATALOG, "DEMO_KNOWLEDGE_SCHEMA": KNOWLEDGE,
                   "GENIE_SPACE_ID": GENIE_SPACE_ID, "KA_NAME": KA_NAME,
                   "SUPERVISOR_NAME": SUPERVISOR_NAME})
print(f"Genie space for the supervisor's data tool: {GENIE_SPACE_ID}")
import importlib, create_agent_bricks as cab
importlib.reload(cab)
cab.main()
```

**Output:**

```text
Genie space for the supervisor's data tool: 01f1cc7fa00816ba81cd394475f36651
== Knowledge Assistant ==
KA exists: knowledge-assistants/aaab91b9-aa6a-48f3-b24a-a8e985a0c240 (id=None) endpoint=ka-aaab91b9-endpoint
  knowledge source exists: Manufacturing SOP Library (index)
  KA state=ACTIVE endpoint=ka-aaab91b9-endpoint
KA endpoint: ka-aaab91b9-endpoint  id: aaab91b9-aa6a-48f3-b24a-a8e985a0c240

== Supervisor Agent ==
Supervisor exists: supervisor-agents/ed49a452-d9af-4935-8426-7f64d6fc70ed (id=ed49a452-d9af-4935-8426-7f64d6fc70ed) endpoint=mas-ed49a452-endpoint
  existing tools: ['sop_ka', 'genie_data']

SUPERVISOR endpoint: mas-ed49a452-endpoint  id: ed49a452-d9af-4935-8426-7f64d6fc70ed
(endpoints may take a few minutes to become queryable)

KA_ENDPOINT=ka-aaab91b9-endpoint
KA_ID=aaab91b9-aa6a-48f3-b24a-a8e985a0c240
SUPERVISOR_ENDPOINT=mas-ed49a452-endpoint
SUPERVISOR_ID=ed49a452-d9af-4935-8426-7f64d6fc70ed
```

```python
banner("Wait for both agent endpoints to be READY")
ka_obj = next(k for k in cab.w.knowledge_assistants.list_knowledge_assistants() if k.display_name == KA_NAME)
sup_obj = next(s for s in cab.w.supervisor_agents.list_supervisor_agents() if s.display_name == SUPERVISOR_NAME)
endpoints = {"knowledge_assistant": ka_obj.endpoint_name, "supervisor": sup_obj.endpoint_name}
for role, ep in endpoints.items():
    for _ in range(80):
        st = w.serving_endpoints.get(ep).state
        if st and str(st.ready).endswith("READY") and "NOT" not in str(st.ready):
            break
        print(f"  {role} {ep}: ready={st.ready if st else '?'} config={st.config_update if st else '?'}")
        time.sleep(20)
    print(f"{role:<20} {ep}: {w.serving_endpoints.get(ep).state.ready}")
dbutils.jobs.taskValues.set("supervisor_endpoint", endpoints["supervisor"])
dbutils.jobs.taskValues.set("ka_endpoint", endpoints["knowledge_assistant"])
```

**Output:**

```text

==============================================================================
Wait for both agent endpoints to be READY
==============================================================================
knowledge_assistant  ka-aaab91b9-endpoint: EndpointStateReady.READY
supervisor           mas-ed49a452-endpoint: EndpointStateReady.READY
```

```python
def responses_call(endpoint, question, timeout=240):
    t0 = time.time()
    r = w.api_client.do("POST", f"/serving-endpoints/{endpoint}/invocations",
                        body={"input": [{"role": "user", "content": question}]})
    out = r.get("output", []) or []
    tools = [o.get("name") for o in out if o.get("type") == "function_call"]
    msgs = [o for o in out if o.get("type") == "message" and o.get("role") == "assistant"]
    answer = "".join(c.get("text", "") for c in (msgs[-1].get("content", []) if msgs else [])
                     if c.get("type") == "output_text")
    return answer, tools, time.time() - t0

def ask(endpoint, q, retries=3):
    for i in range(retries):
        try:
            return responses_call(endpoint, q)
        except Exception as e:  # scale-from-zero cold start
            print(f"  (attempt {i+1} failed: {str(e)[:120]}; retrying)"); time.sleep(30)
    raise RuntimeError("endpoint not answering")

banner("Knowledge Assistant — procedure question")
q = "A-COMPRESS tablet press is running hot. What does the SOP say to do first?"
a, tools, s = ask(endpoints["knowledge_assistant"], q)
print(f"Q: {q}\n({s:.1f}s)\n{a}")
```

**Output:**

```text

==============================================================================
Knowledge Assistant — procedure question
==============================================================================
Q: A-COMPRESS tablet press is running hot. What does the SOP say to do first?
(7.9s)
Do NOT stop the press abruptly under load — reduce turret speed to 50% setpoint to lower compaction heat generation. This is the first immediate action per SOP-TEMP-014.

After that, follow these numbered remediation steps:

1. Reduce turret speed to 50% setpoint to lower compaction heat generation.

2. Verify the cooling/HVAC supply to the compression zone; confirm chilled-water valve is open and supply temperature is <= 12 °C.

3. Check tooling lubrication and punch/die clearance; inspect for binding.

4. Confirm the temperature sensor against a handheld reference to rule out a faulty RTD before treating as a real excursion.

5. If temperature does not trend down within 10 minutes, initiate a controlled stop and segregate any product produced above the limit.

**SOP Reference:** SOP-TEMP-014
```

```python
banner("Supervisor — live cause (Genie) + remediation (Knowledge Assistant) in one answer")
q = ("Why is LINE-A degraded right now, which asset is the worst, and what should the operator "
     "do about it according to the SOPs?")
a, tools, s = ask(endpoints["supervisor"], q)
print(f"Q: {q}\ntools used: {tools}   ({s:.1f}s)\n\n{a}")
import re
print("\nSOP citations:", sorted(set(re.findall(r"SOP-[A-Z]+-\d{3}", a))))
```

**Output:**

```text

==============================================================================
Supervisor — live cause (Genie) + remediation (Knowledge Assistant) in one answer
==============================================================================
Q: Why is LINE-A degraded right now, which asset is the worst, and what should the operator do about it according to the SOPs?
tools used: ['genie_data', 'sop_ka']   (181.9s)

## LINE-A Status Summary

**Why LINE-A is degraded:** The tablet press (A-COMPRESS) is in DEGRADED condition due to a sustained temperature trend issue. The press is currently running at 83.3°C, which exceeds the validated nominal band of 55–75°C.

**Worst asset:** A-COMPRESS (tablet press) is the worst asset with:
- Condition: DEGRADED
- 1 active critical alert (temperature trend)
- Anomaly score: 0.65
- Current temperature: 83.3°C (above validated limit)

The other LINE-A assets (A-BLEND, A-COAT, A-DISPENSE, A-GRANULATE, A-INSPECT) are all in WATCH condition but not degraded.

---

## Operator Actions per SOP-TEMP-014

**Immediate Actions:**

1. **Do NOT stop the press abruptly** — reduce turret speed to 50% setpoint to lower compaction heat generation (currently at 1213 RPM)

2. **Verify cooling system** — confirm chilled-water valve is open and supply temperature is ≤ 12°C

3. **Check tooling** — inspect for binding, verify lubrication and punch/die clearance

4. **Validate sensor** — confirm temperature reading against a handheld reference to rule out faulty RTD

5. **Monitor for 10 minutes** — if temperature does not trend down, initiate controlled stop and segregate product produced above limit

**Quality Requirements:**
- Raise a deviation per SOP-GEN-001
- Notify shift supervisor and QA immediately
- Quarantine affected batches pending disposition
- Document alert ID, peak temperature (83.3°C), and duration

SOP citations: ['SOP-GEN-001', 'SOP-TEMP-014']
```
