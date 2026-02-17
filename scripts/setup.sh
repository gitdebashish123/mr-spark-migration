#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
#  MR → Spark Migration Pipeline · Local Setup Script (uv)
#  Usage:  bash scripts/setup.sh
# ─────────────────────────────────────────────────────────────────
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }
step()    { echo -e "\n${BOLD}$*${NC}"; }

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}  MR → Spark Migration Pipeline · Local Setup (uv)${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# ── 1. Install uv if not present ─────────────────────────────────
step "Step 1 · uv"
if command -v uv &>/dev/null; then
    UV_VER=$(uv --version 2>&1 | head -1)
    success "uv already installed: $UV_VER"
else
    info "uv not found — installing via official installer..."
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # Add uv to PATH for the rest of this script
    export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

    if ! command -v uv &>/dev/null; then
        error "uv installation failed. Install manually: https://docs.astral.sh/uv/getting-started/installation/"
    fi
    success "uv installed: $(uv --version)"
fi

# ── 2. Python 3.12+ via uv ───────────────────────────────────────
step "Step 2 · Python 3.12"
info "Ensuring Python 3.12 is available..."
# uv will download Python 3.12 automatically if not found on system
uv python install 3.12 --quiet 2>/dev/null || true
PYTHON_VER=$(uv run python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "unknown")
success "Python $PYTHON_VER available via uv"

# ── 3. Create/sync virtual environment ───────────────────────────
step "Step 3 · Virtual environment + dependencies"
info "Syncing project environment (this is fast with uv)..."

# uv sync:
#   - Creates .venv if it doesn't exist
#   - Installs all [project.dependencies] (includes dev/test tools)
#   - Generates/updates uv.lock
uv sync

success "Dependencies installed and .venv ready"
info "Lock file: uv.lock (commit this to version control)"

# ── 4. .env file ─────────────────────────────────────────────────
step "Step 4 · Environment file"
if [[ ! -f ".env" ]]; then
    cp .env.example .env
    warn ".env created from .env.example"
    warn "→ Edit .env and add your API keys before running the pipeline"
else
    success ".env already exists — skipping"
fi

# ── 5. Validate imports ──────────────────────────────────────────
step "Step 5 · Import validation"
info "Verifying all core modules import correctly..."
uv run python -c "
from src.models.state import PipelineConfig, PipelineState
from src.utils.config import settings
from src.tools.base_tools import mcp
from src.agents.graph import pipeline_graph
print('  ✓ src.models.state     — OK')
print('  ✓ src.utils.config     — OK')
print('  ✓ src.tools.base_tools — OK')
print('  ✓ src.agents.graph     — OK')
"
success "All imports OK"

# ── 6. Run Phase 1 smoke tests ───────────────────────────────────
step "Step 6 · Phase 1 smoke tests"
info "Running tests (no API keys needed)..."
uv run pytest tests/test_phase1.py -v --tb=short

# ── 7. Print next steps ──────────────────────────────────────────
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  ✓ Phase 1 setup complete!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  ${CYAN}Next steps:${NC}"
echo ""
echo -e "  1. Add API key(s) to ${YELLOW}.env${NC}:"
echo -e "       ANTHROPIC_API_KEY=sk-ant-..."
echo -e "       GOOGLE_API_KEY=AIza...      ${CYAN}# optional Gemini fallback${NC}"
echo ""
echo -e "  2. Launch the Streamlit UI:"
echo -e "       ${YELLOW}uv run streamlit run ui/app.py${NC}"
echo ""
echo -e "  3. Run tests:"
echo -e "       ${YELLOW}uv run pytest tests/ -v${NC}"
echo ""
echo -e "  4. Add a dependency:"
echo -e "       ${YELLOW}uv add <package>${NC}"
echo ""
echo -e "  5. Run any script in the project env:"
echo -e "       ${YELLOW}uv run python <script.py>${NC}"
echo ""
