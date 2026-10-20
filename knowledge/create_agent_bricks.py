"""
Stand up the MANAGED Agent Bricks layer: a Knowledge Assistant over the SOP corpus
and a Multi-Agent Supervisor wiring the KA + the demo Genie space.

Idempotent-ish: skips creation if an object with the same display name already exists.
Beta APIs (databricks-sdk knowledgeassistants / supervisoragents).

Run:
  .venv-introspect/bin/python knowledge/create_agent_bricks.py
"""
from __future__ import annotations

import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import knowledgeassistants as ka
from databricks.sdk.service import supervisoragents as sa

PROFILE = os.environ.get("DBX_PROFILE",
                         os.environ.get("DATABRICKS_PROFILE", "fe-vm-hls-streaming-solution-demo"))
CATALOG = os.environ.get("DEMO_CATALOG", "hls_streaming_solution_demo_catalog")
SCHEMA = os.environ.get("DEMO_KNOWLEDGE_SCHEMA", "pharmaco_mfg_rt_knowledge")
SOP_INDEX = f"{CATALOG}.{SCHEMA}.sop_index"
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")

KA_NAME = os.environ.get("KA_NAME", "pharmaco-mfg-sop-ka")
SUP_NAME = os.environ.get("SUPERVISOR_NAME", "pharmaco-mfg-supervisor")

TOPOLOGY = (
    "Plant topology — map asset ids to equipment before answering: A-* are OSD tablet line "
    "(LINE-A) assets — A-DISPENSE=dispenser, A-GRANULATE=granulator, A-BLEND=blender, "
    "A-COMPRESS=tablet press (compression machine, NOT an air compressor), A-COAT=coater, "
    "A-INSPECT=vision inspector; B-* are sterile line (LINE-B) — B-WASH=vial washer, "
    "B-DEPYRO=depyrogenation tunnel, B-FILL=aseptic filler, B-STOPPER=stoppering unit, "
    "B-LYO=lyophilizer; C-* are packaging (LINE-C) — C-LABEL=labeler, C-CARTON=cartoner, "
    "C-SERIALIZE=serialization unit, C-CASEPACK=case packer. ")

KA_INSTRUCTIONS = (
    "You are a pharmaceutical manufacturing operations assistant for a real-time line "
    "monitoring system. Answer plant operators' questions about diagnosing and fixing "
    "equipment issues using ONLY the provided SOP documents. Give concise, actionable, "
    "numbered remediation steps and ALWAYS cite the SOP id (e.g. SOP-TEMP-014). If the SOPs "
    "do not cover the question, say so plainly. " + TOPOLOGY +
    "So 'A-COMPRESS running hot' is a tablet-press temperature excursion — use SOP-TEMP-014. "
    "All content is synthetic demo data.")

SUP_INSTRUCTIONS = (
    "You coordinate a pharmaceutical manufacturing line-monitoring assistant. Route questions "
    "about current/live plant state, metrics, alerts, or history to the Genie data agent. Route "
    "'how do I fix / diagnose / which SOP applies' questions to the SOP Knowledge Assistant. For "
    "'why is X happening and what should I do' questions, use BOTH: get the live cause from Genie "
    "and the remediation from the Knowledge Assistant, then synthesize one answer that cites the "
    "SOP ids. When asking the Knowledge Assistant, name the equipment type (e.g. 'tablet press "
    "temperature excursion on A-COMPRESS'), never just the asset id. " + TOPOLOGY +
    "Never invent metrics or procedures. All data is synthetic.")

# Inside a Databricks notebook/job use the ambient identity; locally use the CLI profile.
w = WorkspaceClient() if os.environ.get("DATABRICKS_RUNTIME_VERSION") else WorkspaceClient(profile=PROFILE)


def _mask(*paths):
    from google.protobuf.field_mask_pb2 import FieldMask
    return FieldMask(paths=list(paths))


def get_or_create_ka() -> ka.KnowledgeAssistant:
    for k in w.knowledge_assistants.list_knowledge_assistants():
        if k.display_name == KA_NAME:
            print(f"KA exists: {k.name} (id={k.knowledge_assistant_id}) endpoint={k.endpoint_name}")
            if (k.instructions or "") != KA_INSTRUCTIONS:
                w.knowledge_assistants.update_knowledge_assistant(
                    k.name, ka.KnowledgeAssistant(display_name=k.display_name, description=k.description or "",
                                        instructions=KA_INSTRUCTIONS), _mask("instructions"))
                print("  instructions updated")
            return k
    print("Creating Knowledge Assistant ...")
    k = w.knowledge_assistants.create_knowledge_assistant(
        ka.KnowledgeAssistant(
            display_name=KA_NAME,
            description="Synthetic pharma manufacturing SOP assistant — grounded, cited "
                        "remediation for equipment alerts (PharmaCo MFG RT demo).",
            instructions=KA_INSTRUCTIONS))
    print(f"  created: {k.name} (id={k.knowledge_assistant_id})")
    return k


def ensure_knowledge_source(ka_obj: ka.KnowledgeAssistant):
    parent = ka_obj.name  # "knowledge-assistants/{id}"
    for s in w.knowledge_assistants.list_knowledge_sources(parent):
        print(f"  knowledge source exists: {s.display_name} ({s.source_type})")
        return s
    print("  adding knowledge source (existing Vector Search index) ...")
    src = w.knowledge_assistants.create_knowledge_source(
        parent,
        ka.KnowledgeSource(
            display_name="Manufacturing SOP Library",
            description="Synthetic SOPs / work instructions mapped to asset types, sensors, "
                        "and failure scenarios.",
            source_type="index",
            index=ka.IndexSpec(index_name=SOP_INDEX, text_col="content",
                               doc_uri_col="source_file")))
    print(f"    added: {src.name}")
    w.knowledge_assistants.sync_knowledge_sources(parent)
    print("    sync triggered")
    return src


def wait_ready_ka(ka_obj: ka.KnowledgeAssistant, secs=600):
    parent = ka_obj.name
    for _ in range(secs // 15):
        k = w.knowledge_assistants.get_knowledge_assistant(parent)
        state = getattr(k.state, "value", k.state)
        print(f"  KA state={state} endpoint={k.endpoint_name}")
        if k.endpoint_name and str(state).upper().endswith("READY"):
            return k
        if k.endpoint_name and str(state).upper() in ("ACTIVE", "ONLINE", "READY"):
            return k
        time.sleep(15)
    return w.knowledge_assistants.get_knowledge_assistant(parent)


def get_or_create_supervisor() -> sa.SupervisorAgent:
    for s in w.supervisor_agents.list_supervisor_agents():
        if s.display_name == SUP_NAME:
            print(f"Supervisor exists: {s.name} (id={s.supervisor_agent_id}) endpoint={s.endpoint_name}")
            if (s.instructions or "") != SUP_INSTRUCTIONS:
                w.supervisor_agents.update_supervisor_agent(
                    s.name, sa.SupervisorAgent(display_name=s.display_name, description=s.description or "",
                                     instructions=SUP_INSTRUCTIONS), _mask("instructions"))
                print("  instructions updated")
            return s
    print("Creating Supervisor Agent ...")
    s = w.supervisor_agents.create_supervisor_agent(
        sa.SupervisorAgent(display_name=SUP_NAME,
                           description="Routes plant-monitoring questions across the Genie data "
                                       "agent and the SOP Knowledge Assistant (PharmaCo MFG RT demo).",
                           instructions=SUP_INSTRUCTIONS))
    print(f"  created: {s.name} (id={s.supervisor_agent_id})")
    return s


def ensure_tools(sup: sa.SupervisorAgent, ka_id: str):
    parent = sup.name  # "supervisor-agents/{id}"
    existing = {t.name.split("/")[-1]: t for t in w.supervisor_agents.list_tools(parent)}
    print(f"  existing tools: {list(existing)}")
    if "sop_ka" not in existing:
        w.supervisor_agents.create_tool(parent, sa.Tool(
            tool_type="knowledge_assistant",
            description="Manufacturing SOP knowledge — how to diagnose and fix equipment issues; cites SOP ids.",
            knowledge_assistant=sa.KnowledgeAssistant(knowledge_assistant_id=ka_id)),
            tool_id="sop_ka")
        print("    + tool sop_ka (knowledge_assistant)")
    if "genie_data" not in existing and GENIE_SPACE_ID:
        w.supervisor_agents.create_tool(parent, sa.Tool(
            tool_type="genie_space",
            description="Live plant data — line health, asset state, alerts, telemetry history.",
            genie_space=sa.GenieSpace(id=GENIE_SPACE_ID, space_id=GENIE_SPACE_ID)),
            tool_id="genie_data")
        print("    + tool genie_data (genie_space)")
    elif not GENIE_SPACE_ID:
        print("    (genie_data tool skipped — GENIE_SPACE_ID not set)")


def main():
    print("== Knowledge Assistant ==")
    kobj = get_or_create_ka()
    ensure_knowledge_source(kobj)
    kobj = wait_ready_ka(kobj)
    print(f"KA endpoint: {kobj.endpoint_name}  id: {kobj.knowledge_assistant_id}")

    print("\n== Supervisor Agent ==")
    sup = get_or_create_supervisor()
    ensure_tools(sup, kobj.knowledge_assistant_id)
    print(f"\nSUPERVISOR endpoint: {sup.endpoint_name}  id: {sup.supervisor_agent_id}")
    print("(endpoints may take a few minutes to become queryable)")

    # machine-readable markers for the deploy orchestrator to capture
    print(f"\nKA_ENDPOINT={kobj.endpoint_name}")
    print(f"KA_ID={kobj.knowledge_assistant_id}")
    print(f"SUPERVISOR_ENDPOINT={sup.endpoint_name}")
    print(f"SUPERVISOR_ID={sup.supervisor_agent_id}")


if __name__ == "__main__":
    main()
