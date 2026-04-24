#!/usr/bin/env bash
# scripts/setup.sh
# One-command setup for AlphaLens on a fresh machine.
# Run from the project root: bash scripts/setup.sh

set -euo pipefail
BOLD="\033[1m"; GREEN="\033[32m"; YELLOW="\033[33m"; RESET="\033[0m"

log()  { echo -e "${GREEN}[AlphaLens]${RESET} $*"; }
warn() { echo -e "${YELLOW}[WARNING]${RESET} $*"; }

cd "$(dirname "$0")/.."
PROJECT_DIR="$(pwd)"

log "${BOLD}AlphaLens Setup${RESET}"
log "Project directory: $PROJECT_DIR"

# ── Check Python version ───────────────────────────────────────────────────
PYTHON=$(command -v python3.11 || command -v python3 || echo "")
if [[ -z "$PYTHON" ]]; then
  warn "Python 3.11+ not found. Install from https://python.org"
  exit 1
fi
PY_VER=$($PYTHON --version 2>&1)
log "Python: $PY_VER"

# ── Virtual environment ────────────────────────────────────────────────────
if [[ ! -d ".venv" ]]; then
  log "Creating virtual environment..."
  $PYTHON -m venv .venv
fi
source .venv/bin/activate
log "Virtual environment activated"

# ── Install dependencies ───────────────────────────────────────────────────
log "Installing Python dependencies (this may take 5-10 minutes)..."
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
log "Dependencies installed"

# ── Environment file ───────────────────────────────────────────────────────
if [[ ! -f ".env" ]]; then
  cp .env.example .env
  warn ".env created from .env.example"
  warn "Please edit .env and add your Telegram bot token, email credentials,"
  warn "and Zerodha Kite API keys before running the application."
else
  log ".env already exists — skipping"
fi

# ── Create data directories ────────────────────────────────────────────────
mkdir -p data alphalens/models alphalens/logs alphalens/exports
log "Data directories created"

# ── Database initialisation ────────────────────────────────────────────────
log "Initialising databases and seeding stock universe..."
python main.py --init
log "Database initialisation complete"

echo ""
log "${BOLD}Setup complete!${RESET}"
echo ""
echo "Next steps:"
echo "  1. Edit .env with your API credentials (Telegram, Email, Zerodha Kite)"
echo "  2. Run the historical data backfill (takes 20-40 min on SATA HDD):"
echo "       python main.py --backfill"
echo "  3. Launch the dashboard:"
echo "       python main.py --dashboard"
echo "  4. Or start everything (dashboard + scheduler):"
echo "       python main.py"
echo ""
echo "  Dashboard URL: http://localhost:8050"
