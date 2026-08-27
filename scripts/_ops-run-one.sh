#!/bin/bash
set -e
SCRIPT_PATH="$1"
[ -z "$SCRIPT_PATH" ] && { echo "usage: $0 <seed-script.mjs>" >&2; exit 2; }
PROJECT_DIR="/root/worldmonitor"
[ -f "$PROJECT_DIR/.env" ] && { set -a; . "$PROJECT_DIR/.env"; set +a; }
UPSTASH_REDIS_REST_URL="${UPSTASH_REDIS_REST_URL:-http://localhost:8079}"
[ -n "${REDIS_TOKEN:-}" ] && UPSTASH_REDIS_REST_TOKEN="$REDIS_TOKEN"
[ -z "${UPSTASH_REDIS_REST_TOKEN:-}" ] && { echo "ERROR: REDIS_TOKEN required" >&2; exit 1; }
export UPSTASH_REDIS_REST_URL UPSTASH_REDIS_REST_TOKEN
OVERRIDE="$PROJECT_DIR/docker-compose.override.yml"
if [ -f "$OVERRIDE" ]; then
  _env_tmp=$(mktemp)
  grep -E '^\s+[A-Z_]+:' "$OVERRIDE" | grep -v '#' | sed 's/^\s*//' | sed 's/: */=/' | sed 's/["'"'"']//g' \
    | grep -E '^(NASA_FIRMS|GROQ|AISSTREAM|FRED|FINNHUB|EIA|ACLED_ACCESS_TOKEN|ACLED_EMAIL|ACLED_PASSWORD|CLOUDFLARE|AVIATIONSTACK|OPENAQ_API_KEY|WAQI_API_KEY|OPENROUTER_API_KEY|LLM_API_URL|LLM_API_KEY|LLM_MODEL|OLLAMA_API_URL|OLLAMA_MODEL)' \
    | sed 's/^/export /' > "$_env_tmp"
  . "$_env_tmp"
  rm -f "$_env_tmp"
fi
cd "$PROJECT_DIR"
echo "ENV CHECK: AVIATIONSTACK_API_LEN=${#AVIATIONSTACK_API}; REDIS_TOKEN_LEN=${#REDIS_TOKEN}"
timeout -k 30 1800 node "$SCRIPT_PATH"
