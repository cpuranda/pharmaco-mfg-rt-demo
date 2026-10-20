#!/usr/bin/env bash
# =============================================================================
# PharmaCo MFG Real-Time demo — redeployment orchestrator
# =============================================================================
# Phased, idempotent deploy of the whole demo into a Databricks workspace.
# Edit deploy/deploy.config.env first, then:
#
#   deploy/deploy.sh preflight     # check tools, auth, deps, config
#   deploy/deploy.sh all           # run every phase in dependency order
#   deploy/deploy.sh <phase>       # run a single phase (see PHASES below)
#   deploy/deploy.sh status        # show what exists + URLs
#   deploy/deploy.sh teardown      # delete demo resources (guarded)
#
# Discovered values are saved to deploy/.deploy.state.env and reused on re-run.
# NOTE: validated against the reference workspace; on a brand-new workspace run
# phase-by-phase and watch the output — infra creation (catalog/Lakebase/app) can
# require workspace entitlements. See deploy/DEPLOY.md.
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
CONFIG="$HERE/deploy.config.env"
STATE="$HERE/.deploy.state.env"
TMPL="$HERE/templates"

PHASES="preflight secret notebooks discover app grants validate status  (legacy: catalog lakebase sql pipeline knowledge agents)"

# ---- load config + discovered state ----
[ -f "$CONFIG" ] || { echo "ERROR: $CONFIG not found"; exit 1; }
set -a; . "$CONFIG"; [ -f "$STATE" ] && . "$STATE"; set +a

# python with databricks-sdk + psycopg + protobuf (override with DEPLOY_PYTHON)
PY="${DEPLOY_PYTHON:-}"
if [ -z "$PY" ]; then
  if [ -x "$ROOT/.venv-introspect/bin/python" ]; then PY="$ROOT/.venv-introspect/bin/python"; else PY="python3"; fi
fi
P="$DBX_PROFILE"

# ---- helpers ----
log()  { printf '\n\033[1;36m>> %s\033[0m\n' "$*"; }
ok()   { printf '   \033[1;32m✓ %s\033[0m\n' "$*"; }
warn() { printf '   \033[1;33m! %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

save_state() { # save_state KEY VALUE
  touch "$STATE"
  grep -v "^$1=" "$STATE" > "$STATE.tmp" 2>/dev/null || true
  echo "$1=$2" >> "$STATE.tmp"; mv "$STATE.tmp" "$STATE"
  export "$1=$2"
}

dbx() { databricks "$@" -p "$P"; }

sqlexec() { # sqlexec "<SQL>"  → runs on the warehouse, fails on error
  local out state
  out=$(dbx api post /api/2.0/sql/statements --json "$(jq -n --arg s "$1" --arg w "$DBX_WAREHOUSE_ID" \
        '{warehouse_id:$w, statement:$s, wait_timeout:"50s"}')") || die "SQL call failed"
  state=$(echo "$out" | jq -r '.status.state // "UNKNOWN"')
  [ "$state" = "SUCCEEDED" ] || die "SQL not SUCCEEDED ($state): $(echo "$out" | jq -c '.status.error // .')"
}

render() { # render <tmpl> <out>   (envsubst using currently-exported vars)
  envsubst < "$1" > "$2"
}

# =============================================================================
# PHASES
# =============================================================================
phase_preflight() {
  log "preflight"
  command -v databricks >/dev/null || die "databricks CLI not found"
  command -v jq >/dev/null || die "jq not found (brew install jq)"
  command -v envsubst >/dev/null || die "envsubst not found (brew install gettext)"
  "$PY" -c 'import databricks.sdk, psycopg' 2>/dev/null \
    || die "python at '$PY' missing databricks-sdk/psycopg — set DEPLOY_PYTHON to a venv that has them"
  dbx current-user me >/dev/null 2>&1 || die "profile '$P' not authenticated — run: databricks auth login --profile $P"
  ok "CLI + jq + envsubst + python deps present; profile '$P' authenticated"
  [ -n "${DBX_WAREHOUSE_ID:-}" ] && dbx warehouses get "$DBX_WAREHOUSE_ID" >/dev/null 2>&1 \
    && ok "warehouse $DBX_WAREHOUSE_ID reachable" || warn "warehouse $DBX_WAREHOUSE_ID not verified — set DBX_WAREHOUSE_ID"
  echo "   target: $DBX_WORKSPACE_URL  catalog: $DEMO_CATALOG"
}

phase_catalog() {
  log "catalog + schemas"
  sqlexec "CREATE CATALOG IF NOT EXISTS ${DEMO_CATALOG}"
  for s in "$DEMO_BRONZE" "$DEMO_SILVER" "$DEMO_GOLD"; do
    sqlexec "CREATE SCHEMA IF NOT EXISTS ${DEMO_CATALOG}.${s}"
  done
  sqlexec "CREATE VOLUME IF NOT EXISTS ${DEMO_CATALOG}.${DEMO_BRONZE}.checkpoints"
  ok "catalog ${DEMO_CATALOG} + bronze/silver/gold schemas + checkpoints volume"
}

phase_secret() {
  log "secret scope + Zerobus SP OAuth secret"
  dbx secrets create-scope "$SECRET_SCOPE" >/dev/null 2>&1 && ok "scope $SECRET_SCOPE created" || warn "scope $SECRET_SCOPE exists"
  if [ -n "${ZB_CLIENT_SECRET:-}" ]; then
    dbx secrets put-secret "$SECRET_SCOPE" "$SECRET_KEY" --string-value "$ZB_CLIENT_SECRET" && ok "secret stored"
  else
    warn "ZB_CLIENT_SECRET not exported — store the SP OAuth secret manually:"
    echo "     databricks secrets put-secret $SECRET_SCOPE $SECRET_KEY --string-value '<secret>' -p $P"
  fi
}

phase_lakebase() {
  log "Lakebase instance + Postgres schema"
  if dbx database get-database-instance "$LAKEBASE_INSTANCE" >/dev/null 2>&1; then
    ok "instance $LAKEBASE_INSTANCE exists"
  else
    warn "creating Lakebase instance $LAKEBASE_INSTANCE (capacity CU_1)"
    dbx database create-database-instance --json "$(jq -n --arg n "$LAKEBASE_INSTANCE" '{name:$n, capacity:"CU_1"}')" >/dev/null \
      || die "instance create failed — create it in the UI, then re-run this phase"
    warn "waiting for AVAILABLE ..."
    for _ in $(seq 1 40); do
      st=$(dbx database get-database-instance "$LAKEBASE_INSTANCE" -o json 2>/dev/null | jq -r '.state // ""')
      [ "$st" = "AVAILABLE" ] && break; sleep 15
    done
  fi
  local dns; dns=$(dbx database get-database-instance "$LAKEBASE_INSTANCE" -o json | jq -r '.read_write_dns')
  [ -n "$dns" ] && [ "$dns" != "null" ] || die "could not read Lakebase DNS"
  save_state LAKEBASE_DNS "$dns"; ok "DNS $dns"
  # apply Postgres schema (app operational tables) using a minted token
  local user token
  user=$(dbx current-user me -o json | jq -r '.userName')
  token=$("$PY" - <<PYEOF
import uuid
from databricks.sdk import WorkspaceClient
w=WorkspaceClient(profile="$P")
c=w.api_client.do("POST","/api/2.0/database/credentials",body={"request_id":str(uuid.uuid4()),"instance_names":["$LAKEBASE_INSTANCE"]})
print(c["token"] if isinstance(c,dict) else c.token)
PYEOF
)
  PGHOST="$dns" PGUSER="$user" PGPASSWORD="$token" PGDATABASE="$LAKEBASE_DB" \
    "$PY" "$ROOT/lakebase/apply_schema.py" && ok "Postgres schema '$LAKEBASE_APP_DB' applied"
}

phase_sql() {
  log "Delta bronze/silver/gold DDL"
  DBX_PROFILE="$P" DBX_WAREHOUSE_ID="$DBX_WAREHOUSE_ID" \
  DEMO_CATALOG="$DEMO_CATALOG" DEMO_BRONZE="$DEMO_BRONZE" DEMO_SILVER="$DEMO_SILVER" DEMO_GOLD="$DEMO_GOLD" \
    "$PY" "$ROOT/sql/apply_sql.py" "$ROOT/sql/01_bronze.sql" "$ROOT/sql/02_silver.sql" "$ROOT/sql/03_gold.sql" \
    && ok "bronze/silver/gold tables created"
}

phase_pipeline() {
  log "pipeline notebook + streaming job"
  local wsdir="/Workspace/Users/${APP_USER_EMAIL}/pharmaco_mfg_rt_demo/pipeline"
  dbx workspace mkdirs "$wsdir" >/dev/null 2>&1 || true
  dbx workspace import "$wsdir/rt_pipeline" --file "$ROOT/pipeline/rt_pipeline.py" \
    --language PYTHON --format SOURCE --overwrite && ok "notebook imported"
  # create or update the job (match by name)
  local jobjson; jobjson=$(mktemp); render "$TMPL/streaming_job.json.tmpl" "$jobjson"
  local jid; jid=$(dbx jobs list -o json | jq -r --arg n "$STREAMING_JOB_NAME" '.[]? | select(.settings.name==$n) | .job_id' | head -1)
  if [ -n "$jid" ] && [ "$jid" != "null" ]; then
    dbx jobs update --json "$(jq -n --argjson id "$jid" --slurpfile s "$jobjson" '{job_id:$id, new_settings:$s[0]}')" >/dev/null
    ok "job updated (id $jid)"
  else
    jid=$(dbx jobs create --json "@$jobjson" -o json | jq -r '.job_id')
    ok "job created (id $jid)"
  fi
  save_state STREAMING_JOB_ID "$jid"; rm -f "$jobjson"
}

phase_knowledge() {
  log "SOP corpus + Vector Search index"
  DBX_PROFILE="$P" DBX_WAREHOUSE_ID="$DBX_WAREHOUSE_ID" DEMO_CATALOG="$DEMO_CATALOG" \
  DEMO_KNOWLEDGE_SCHEMA="$DEMO_KNOWLEDGE_SCHEMA" VS_ENDPOINT="$VS_ENDPOINT" EMBED_ENDPOINT="$EMBED_ENDPOINT" \
  APP_SP="${APP_SP:-}" \
    "$PY" "$ROOT/knowledge/generate_sops.py"
  DBX_PROFILE="$P" DBX_WAREHOUSE_ID="$DBX_WAREHOUSE_ID" DEMO_CATALOG="$DEMO_CATALOG" \
  DEMO_KNOWLEDGE_SCHEMA="$DEMO_KNOWLEDGE_SCHEMA" VS_ENDPOINT="$VS_ENDPOINT" EMBED_ENDPOINT="$EMBED_ENDPOINT" \
  APP_SP="${APP_SP:-}" \
    "$PY" "$ROOT/knowledge/load_knowledge.py" && ok "SOP index built on $VS_ENDPOINT"
}

phase_agents() {
  log "Agent Bricks: Knowledge Assistant + Multi-Agent Supervisor"
  local out; out=$(DBX_PROFILE="$P" DEMO_CATALOG="$DEMO_CATALOG" DEMO_KNOWLEDGE_SCHEMA="$DEMO_KNOWLEDGE_SCHEMA" \
    GENIE_SPACE_ID="${GENIE_SPACE_ID:-}" KA_NAME="$KA_NAME" SUPERVISOR_NAME="$SUPERVISOR_NAME" \
    "$PY" "$ROOT/knowledge/create_agent_bricks.py" | tee /dev/tty)
  local kae mase
  kae=$(echo "$out" | sed -n 's/^KA_ENDPOINT=//p' | tail -1)
  mase=$(echo "$out" | sed -n 's/^SUPERVISOR_ENDPOINT=//p' | tail -1)
  [ -n "$kae" ] && save_state KA_ENDPOINT "$kae" && ok "KA endpoint $kae"
  [ -n "$mase" ] && save_state SUPERVISOR_ENDPOINT "$mase" && ok "supervisor endpoint $mase"
}

phase_app() {
  log "app.yaml + app deploy"
  render "$TMPL/app.yaml.tmpl" "$ROOT/app/app.yaml"; ok "app/app.yaml generated"
  dbx apps get "$APP_NAME" >/dev/null 2>&1 || { dbx apps create "$APP_NAME" >/dev/null && ok "app created"; }
  # resource bindings (lakebase, secret, genie, llm, ka, mas)
  "$PY" - <<PYEOF
from databricks.sdk import WorkspaceClient
w=WorkspaceClient(profile="$P")
app=w.api_client.do("GET","/api/2.0/apps/$APP_NAME"); res=app.get("resources",[]) or []
by={r.get("name"):r for r in res}
def put(name, r): by[name]=r
put("lakebase", {"name":"lakebase","description":"Lakebase operational store","database":{"instance_name":"$LAKEBASE_INSTANCE","database_name":"$LAKEBASE_DB","permission":"CAN_CONNECT_AND_CREATE"}})
put("sp-oauth-secret", {"name":"sp-oauth-secret","description":"Zerobus SP OAuth secret","secret":{"scope":"$SECRET_SCOPE","key":"$SECRET_KEY","permission":"READ"}})
put("assistant-llm", {"name":"assistant-llm","description":"Foundation Model for Fix Assistant","serving_endpoint":{"name":"$ASSISTANT_LLM_ENDPOINT","permission":"CAN_QUERY"}})
gid="${GENIE_SPACE_ID:-}"
if gid: put("genie-space", {"name":"genie-space","description":"Genie space","genie_space":{"space_id":gid,"permission":"CAN_RUN"}})
mas="${SUPERVISOR_ENDPOINT:-}"
if mas: put("mas-supervisor", {"name":"mas-supervisor","description":"Managed Agent Bricks supervisor","serving_endpoint":{"name":mas,"permission":"CAN_QUERY"}})
ka="${KA_ENDPOINT:-}"
if ka: put("sop-ka", {"name":"sop-ka","description":"Agent Bricks Knowledge Assistant (invoked by the supervisor)","serving_endpoint":{"name":ka,"permission":"CAN_QUERY"}})
put("sql-warehouse", {"name":"sql-warehouse","description":"SQL warehouse for gold reads","sql_warehouse":{"id":"$DBX_WAREHOUSE_ID","permission":"CAN_USE"}})
jid="${STREAMING_JOB_ID:-}"
if jid: put("operational-job", {"name":"operational-job","description":"Operational Lakeflow Job","job":{"id":jid,"permission":"CAN_MANAGE_RUN"}})
w.api_client.do("PATCH","/api/2.0/apps/$APP_NAME", query={"update_mask":"resources"}, body={"resources":list(by.values())})
print("resources:", list(by))
PYEOF
  ok "app resources bound"
  # sync source + deploy
  local wsapp="/Workspace/Users/${APP_USER_EMAIL}/pharmaco_mfg_rt_demo/app"
  ( cd "$ROOT/app/frontend" && npm install >/dev/null 2>&1 && npm run build >/dev/null 2>&1 ) && ok "frontend built"
  dbx sync "$ROOT/app" "$wsapp" --exclude node_modules --exclude .venv --exclude __pycache__ \
    --exclude .git --exclude "frontend/node_modules" --exclude "frontend/src" --exclude "frontend/public" \
    --exclude "*.log" --full >/dev/null && ok "source synced"
  dbx apps deploy "$APP_NAME" --source-code-path "$wsapp" >/dev/null && ok "app deployed"
  local sp url
  sp=$(dbx apps get "$APP_NAME" -o json | jq -r '.service_principal_client_id')
  url=$(dbx apps get "$APP_NAME" -o json | jq -r '.url')
  save_state APP_SP "$sp"; save_state APP_URL "$url"
  ok "app SP $sp  |  $url"
}

phase_grants() {
  log "grants to the app service principal"
  [ -n "${APP_SP:-}" ] || die "APP_SP unknown — run the 'app' phase first"
  local KS="${DEMO_CATALOG}.${DEMO_KNOWLEDGE_SCHEMA}"
  sqlexec "GRANT USE CATALOG ON CATALOG ${DEMO_CATALOG} TO \`${APP_SP}\`"
  for sch in "$DEMO_BRONZE" "$DEMO_SILVER" "$DEMO_GOLD" "$DEMO_LDP_SCHEMA"; do
    sqlexec "GRANT USE SCHEMA, SELECT ON SCHEMA ${DEMO_CATALOG}.${sch} TO \`${APP_SP}\`"
  done
  sqlexec "GRANT USE SCHEMA ON SCHEMA ${KS} TO \`${APP_SP}\`"
  sqlexec "GRANT SELECT ON TABLE ${KS}.sop_chunks TO \`${APP_SP}\`"
  sqlexec "GRANT SELECT ON TABLE ${KS}.sop_index TO \`${APP_SP}\`"
  sqlexec "GRANT READ VOLUME ON VOLUME ${KS}.sop_docs TO \`${APP_SP}\`"
  ok "UC grants applied"
  # Vector Search endpoint permission (CLI; needs the endpoint UUID)
  local vsid
  vsid=$(dbx vector-search-endpoints get-endpoint "$VS_ENDPOINT" -o json 2>/dev/null | jq -r '.id // empty')
  if [ -n "$vsid" ]; then
    dbx vector-search-endpoints update-permissions "$vsid" \
      --json "$(jq -n --arg sp "$APP_SP" '{access_control_list:[{service_principal_name:$sp,permission_level:"CAN_USE"}]}')" \
      >/dev/null 2>&1 && ok "VS endpoint CAN_USE granted" || warn "VS grant skipped"
  fi
  # serving endpoints (LLM + KA + MAS): permissions REST API by endpoint id
  "$PY" - <<PYEOF
from databricks.sdk import WorkspaceClient
w=WorkspaceClient(profile="$P"); sp="$APP_SP"
for name in [x for x in ["$ASSISTANT_LLM_ENDPOINT","${KA_ENDPOINT:-}","${SUPERVISOR_ENDPOINT:-}"] if x]:
    try:
        ep=w.serving_endpoints.get(name)
        if ep.id:
            w.api_client.do("PATCH", f"/api/2.0/permissions/serving-endpoints/{ep.id}",
                body={"access_control_list":[{"service_principal_name":sp,"permission_level":"CAN_QUERY"}]})
            print(f"{name} CAN_QUERY granted")
        else:
            print(f"{name}: system endpoint (usually queryable; bound as app resource)")
    except Exception as ex: print(f"{name} grant note:", str(ex)[:150])
PYEOF
  ok "endpoint grants applied"
  # Lakebase: the app's database resource creates its Postgres role; grant it the demo schema
  "$PY" - <<PYEOF
import uuid, psycopg
from databricks.sdk import WorkspaceClient
w=WorkspaceClient(profile="$P")
inst=w.api_client.do("GET","/api/2.0/database/instances/$LAKEBASE_INSTANCE")
tok=w.api_client.do("POST","/api/2.0/database/credentials",body={"request_id":str(uuid.uuid4()),"instance_names":["$LAKEBASE_INSTANCE"]})["token"]
with psycopg.connect(host=inst["read_write_dns"],dbname="$LAKEBASE_DB",user=w.current_user.me().user_name,password=tok,sslmode="require",autocommit=True) as c:
    r='"$APP_SP"'
    for q in [f"GRANT USAGE ON SCHEMA $LAKEBASE_APP_DB TO {r}",
              f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA $LAKEBASE_APP_DB TO {r}",
              f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA $LAKEBASE_APP_DB TO {r}"]:
        c.execute(q)
    print("Lakebase grants applied to", r)
PYEOF
  ok "Lakebase grants applied"
}

phase_notebooks() {
  log "build notebooks 01-10 as a serverless Lakeflow Job (outputs exported to evidence/)"
  "$PY" "$ROOT/tools/run_build.py" 01 02 03 04 05 06 07 08 09 10
}

phase_discover() {
  log "discover ids created by the notebooks (Genie space, agents, operational job, Lakebase)"
  local out; out=$("$PY" - <<PYEOF
from databricks.sdk import WorkspaceClient
w=WorkspaceClient(profile="$P")
sp=[s for s in w.api_client.do("GET","/api/2.0/genie/spaces").get("spaces",[]) if s.get("title","").startswith("PharmaCo Manufacturing")]
print("GENIE_SPACE_ID="+(sp[0]["space_id"] if sp else ""))
j=list(w.jobs.list(name="pharmaco_mfg_rt_operational")); print("STREAMING_JOB_ID="+(str(j[0].job_id) if j else ""))
print("LAKEBASE_DNS="+w.api_client.do("GET","/api/2.0/database/instances/$LAKEBASE_INSTANCE")["read_write_dns"])
try:
    print("KA_ENDPOINT="+next(k.endpoint_name for k in w.knowledge_assistants.list_knowledge_assistants() if k.display_name=="$KA_NAME"))
    print("SUPERVISOR_ENDPOINT="+next(s.endpoint_name for s in w.supervisor_agents.list_supervisor_agents() if s.display_name=="$SUPERVISOR_NAME"))
except Exception as e: print("# agents not found:", e)
PYEOF
)
  echo "$out"
  for kv in $(echo "$out" | grep -E '^[A-Z_]+=.+'); do save_state "${kv%%=*}" "${kv#*=}"; done
  ok "state saved to $STATE"
}

phase_validate() {
  log "app acceptance test (through the app API) + notebook 11 end-to-end validation"
  local tok; tok=$(dbx auth token -o json | jq -r .access_token)
  ( set -a; . "$ROOT/config.env"; set +a
    API_BASE="$APP_URL" API_TOKEN="$tok" "$PY" "$ROOT/tests/acceptance.py" ) 2>&1 | tee "$ROOT/evidence/logs/app_acceptance_test.log"
  "$PY" "$ROOT/tools/run_build.py" --no-sync 11
}

phase_status() {
  log "status"
  echo "workspace : $DBX_WORKSPACE_URL"
  echo "catalog   : $DEMO_CATALOG (bronze/silver/gold + $DEMO_KNOWLEDGE_SCHEMA)"
  echo "lakebase  : $LAKEBASE_INSTANCE  dns=${LAKEBASE_DNS:-?}"
  echo "job       : $STREAMING_JOB_NAME  id=${STREAMING_JOB_ID:-?}"
  echo "vs index  : ${DEMO_CATALOG}.${DEMO_KNOWLEDGE_SCHEMA}.sop_index  (endpoint $VS_ENDPOINT)"
  echo "agents    : KA=${KA_ENDPOINT:-?}  supervisor=${SUPERVISOR_ENDPOINT:-?}"
  echo "genie     : ${GENIE_SPACE_ID:-<not set>}"
  echo "app       : $APP_NAME  sp=${APP_SP:-?}"
  echo "app url   : ${APP_URL:-?}"
  dbx apps get "$APP_NAME" -o json 2>/dev/null | jq -r '"app state: "+(.app_status.state // "?")' || true
}

phase_teardown() {
  log "TEARDOWN — deletes demo resources in $DBX_WORKSPACE_URL"
  read -r -p "Type the catalog name to confirm ($DEMO_CATALOG): " c
  [ "$c" = "$DEMO_CATALOG" ] || die "confirmation mismatch; aborted"
  dbx apps delete "$APP_NAME" >/dev/null 2>&1 && ok "app deleted" || warn "app not deleted"
  [ -n "${STREAMING_JOB_ID:-}" ] && { dbx jobs delete "$STREAMING_JOB_ID" >/dev/null 2>&1 && ok "job deleted"; } || true
  "$PY" - <<PYEOF || true
from databricks.sdk import WorkspaceClient
w=WorkspaceClient(profile="$P")
for lst,delete,idattr in [(w.supervisor_agents.list_supervisor_agents,w.supervisor_agents.delete_supervisor_agent,"name"),
                          (w.knowledge_assistants.list_knowledge_assistants,w.knowledge_assistants.delete_knowledge_assistant,"name")]:
    for o in lst():
        if o.display_name in ("$SUPERVISOR_NAME","$KA_NAME"):
            try: delete(getattr(o,idattr)); print("deleted", o.display_name)
            except Exception as e: print("skip", o.display_name, str(e)[:100])
try: w.api_client.do("DELETE","/api/2.0/vector-search/indexes/${DEMO_CATALOG}.${DEMO_KNOWLEDGE_SCHEMA}.sop_index"); print("index deleted")
except Exception as e: print("index skip", str(e)[:100])
PYEOF
  warn "Lakebase instance, secret scope, and catalog are NOT auto-deleted (data-loss guard). Remove manually if desired:"
  echo "     databricks database delete-database-instance $LAKEBASE_INSTANCE -p $P"
  echo "     databricks secrets delete-scope $SECRET_SCOPE -p $P"
  echo "     (SQL) DROP CATALOG IF EXISTS $DEMO_CATALOG CASCADE"
}

phase_all() {
  for ph in preflight secret notebooks discover app grants validate status; do
    "phase_$ph"
  done
  log "DONE — open ${APP_URL:-the app} and run the pre-demo checklist in docs/DEMO_SCRIPT.md"
}

# ---- dispatch ----
cmd="${1:-}"; [ -n "$cmd" ] || { echo "usage: deploy.sh {all|$PHASES|teardown}"; exit 1; }
case "$cmd" in
  all|teardown) "phase_$cmd" ;;
  preflight|catalog|secret|lakebase|sql|pipeline|knowledge|agents|notebooks|discover|app|grants|validate|status) "phase_$cmd" ;;
  *) die "unknown phase '$cmd' (valid: all $PHASES teardown)" ;;
esac
