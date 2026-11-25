#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROFILE="${1:-local}"
PORT="${LAKEHOUSE_API_PORT:-8020}"
HOST="${LAKEHOUSE_API_HOST:-127.0.0.1}"

cd "${REPO_ROOT}"
if command -v uv >/dev/null 2>&1; then
  uv sync --extra dev
else
  python3 -m venv .venv && .venv/bin/pip install --upgrade pip && .venv/bin/pip install -e ".[dev]"
fi
.venv/bin/python services/pipeline/run_pipeline.py --profile "${PROFILE}" --compact
echo "Starting Multimodal RAG Governance Console on http://${HOST}:${PORT}"
exec .venv/bin/uvicorn main:app --app-dir apps/rag-api --host "${HOST}" --port "${PORT}"
