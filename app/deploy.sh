#!/usr/bin/env bash
# Deploy the PharmaCo MFG RT demo Databricks App.
# Prereqs: frontend built (npm run build -> frontend/dist), CLI authenticated.
set -euo pipefail

PROFILE="${DBX_PROFILE:-fe-vm-hls-streaming-solution-demo}"
APP_NAME="pharmaco-mfg-rt-demo"
USER_EMAIL="${DBX_USER_EMAIL:-chandrashekhar.purandare@databricks.com}"
WS_PATH="/Workspace/Users/${USER_EMAIL}/pharmaco_mfg_rt_demo/app"
HERE="$(cd "$(dirname "$0")" && pwd)"

echo ">> Building frontend..."
if [ -d "$HERE/frontend" ]; then
  ( cd "$HERE/frontend" && npm install && npm run build )
fi
[ -d "$HERE/frontend/dist" ] || { echo "ERROR: frontend/dist missing"; exit 1; }

echo ">> Syncing source to $WS_PATH ..."
databricks sync "$HERE" "$WS_PATH" \
  --exclude node_modules --exclude .venv --exclude __pycache__ \
  --exclude .git --exclude "frontend/node_modules" --exclude "frontend/src" \
  --exclude "frontend/public" --exclude "*.log" \
  --full -p "$PROFILE"

echo ">> Deploying app..."
databricks apps deploy "$APP_NAME" --source-code-path "$WS_PATH" -p "$PROFILE"

echo ">> Done. App URL:"
databricks apps get "$APP_NAME" -p "$PROFILE" -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['url'])"
