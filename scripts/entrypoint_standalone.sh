#!/usr/bin/env bash
set -euo pipefail
PROFILE="${LAKEHOUSE_SIM_PROFILE:-local}"
HOST="${LAKEHOUSE_API_HOST:-0.0.0.0}"
PORT="${LAKEHOUSE_API_PORT:-8020}"

if [ ! -f "/app/.local_data/iceberg_catalog.db" ]; then
  echo "[standalone] Bootstrapping multimodal lakehouse (profile=${PROFILE})..."
  python /app/services/pipeline/run_pipeline.py --profile "${PROFILE}" --compact
fi
echo "[standalone] Starting Multimodal RAG Governance Console on http://${HOST}:${PORT}"
exec uvicorn main:app --app-dir /app/apps/rag-api --host "${HOST}" --port "${PORT}"
