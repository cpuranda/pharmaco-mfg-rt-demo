# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Agent Bricks — Knowledge Assistant + Multi-Agent Supervisor
# MAGIC
# MAGIC The managed, governed version of the agentic layer:
# MAGIC * **Knowledge Assistant** `pharmaco-mfg-sop-ka` over the SOP Vector Search index (notebook 07)
# MAGIC * **Multi-Agent Supervisor** `pharmaco-mfg-supervisor` with two tools:
# MAGIC   `sop_ka` (procedures) and `genie_data` (the Genie space from notebook 08 — live plant data)
# MAGIC
# MAGIC The supervisor answers *"why is this happening AND what do I do"* in one cited answer.
# MAGIC The app's **Ask the Assistant** tab calls this supervisor endpoint.
# MAGIC Provisioning code: `knowledge/create_agent_bricks.py` (Agent Bricks Beta APIs).

# COMMAND ----------

# MAGIC %pip install -q databricks-sdk==0.128.0
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

spaces = w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
GENIE_SPACE_ID = next(s["space_id"] for s in spaces if s.get("title") == GENIE_TITLE)
os.environ.update({"DEMO_CATALOG": CATALOG, "DEMO_KNOWLEDGE_SCHEMA": KNOWLEDGE,
                   "GENIE_SPACE_ID": GENIE_SPACE_ID, "KA_NAME": KA_NAME,
                   "SUPERVISOR_NAME": SUPERVISOR_NAME})
print(f"Genie space for the supervisor's data tool: {GENIE_SPACE_ID}")
import importlib, create_agent_bricks as cab
importlib.reload(cab)
cab.main()

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

banner("Supervisor — live cause (Genie) + remediation (Knowledge Assistant) in one answer")
q = ("Why is LINE-A degraded right now, which asset is the worst, and what should the operator "
     "do about it according to the SOPs?")
a, tools, s = ask(endpoints["supervisor"], q)
print(f"Q: {q}\ntools used: {tools}   ({s:.1f}s)\n\n{a}")
import re
print("\nSOP citations:", sorted(set(re.findall(r"SOP-[A-Z]+-\d{3}", a))))
