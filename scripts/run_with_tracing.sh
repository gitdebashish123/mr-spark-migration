#!/bin/bash
#
# Launch Streamlit with LangSmith tracing enabled
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Load .env if it exists
if [ -f .env ]; then
    echo "📝 Loading environment from .env..."
    export $(grep -v '^#' .env | xargs)
fi

# Ensure LangSmith env vars are set for LangGraph
if [ -n "$LANGCHAIN_TRACING_V2" ]; then
    echo "🔍 LangSmith tracing enabled"
    echo "   Project: ${LANGCHAIN_PROJECT:-mr-spark-migration}"
    export LANGCHAIN_TRACING_V2
    export LANGCHAIN_API_KEY
    export LANGCHAIN_PROJECT="${LANGCHAIN_PROJECT:-mr-spark-migration}"
    export LANGCHAIN_ENDPOINT="${LANGCHAIN_ENDPOINT:-https://api.smith.langchain.com}"
else
    echo "⚠️  LangSmith tracing disabled (LANGCHAIN_TRACING_V2 not set)"
fi

echo "🚀 Launching Streamlit UI..."
uv run streamlit run ui/app.py
