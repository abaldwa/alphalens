# AlphaLens

AlphaLens is a machine-learning-based Indian equity research and stock-screening system. It ingests
daily NSE/BSE market data into a central DataStore, computes a 330-feature matrix, runs an ensemble of
ML models (regime detection, signal generation, pump-and-dump detection, exit timing, multibagger and
forensic scoring), and surfaces Buy/Hold/Sell signals, multibagger watchlists, exit alerts, and fraud
warnings through a DataStore API consumed by downstream systems and a dashboard. It is a decision-support
tool — all final investment decisions remain with the human user.

See `alphalens_docs/CLAUDE.md` for the full project context and `alphalens_docs/specs/08_specifications.md`
for the spec-driven development rules every change must trace to.

## Setup

Python 3.11 is pinned (do not use 3.12+ until all libraries are verified compatible).

```bash
# 1. Create and activate the conda environment
conda create -n alphalens python=3.11
conda activate alphalens

# 2. Install dependencies (phase1.txt includes phase0.txt)
pip install -r requirements/phase1.txt

# 3. Configure credentials
cp .env.example .env
# edit .env and fill in FYERS_APP_ID, FYERS_SECRET_ID, FYERS_ACCESS_TOKEN

# 4. Populate the stock universe
# config/nifty500_universe.csv ships with a small starter sample only.
# Replace it with the official Nifty 500 constituent list (NSE archives)
# before running the pipeline — see config/universe.py.
```

## Running the pipeline

```bash
# Start the DataStore API (FastAPI, localhost:8000)
uvicorn datastore.api.main:app --host 0.0.0.0 --port 8000 --reload

# Run the daily pipeline (gap detection + backfill + today's run)
python -m ingestion.scheduler.daily_pipeline

# Run the test suite (minimum bar before any commit)
pytest tests/unit/ -v --tb=short
pytest tests/ -v --cov=alphalens --cov-report=term-missing
```

API docs are available at `http://localhost:8000/docs` once the DataStore API is running.
