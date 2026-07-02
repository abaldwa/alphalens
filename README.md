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

## Paper Trading

AlphaLens ships a forward-live paper trading bot that trades real, human-reviewable signals with
simulated money (₹1 crore starting capital). This is how Phase 3 Gate 7 (≥90 genuine forward
trading days) gets cleared — see `BuildLog.md` → "Gate 7 Analysis — Paper Trading Requirement" and
"Full Project Status Review" for current gate status.

**Current status (2026-07-02): built and unit-tested, but never run for real.**
`paper_trading/executions/` is empty and `paper_trading/portfolio_state.json` does not exist yet —
the bot needs to actually be started to begin accumulating the 90 days the gate requires.

### How it works

1. The scheduler (`ingestion/scheduler/pipeline_scheduler.py`) runs the full daily pipeline at
   18:00 IST on trading days: ingestion → feature computation → model inference → signal write-back
   → **paper trade step** ([`ingestion/scheduler/daily_pipeline.py`](ingestion/scheduler/daily_pipeline.py),
   `STEPS` table in [`ingestion/scheduler/checkpoint.py`](ingestion/scheduler/checkpoint.py)).
2. The paper-trade step invokes [`scripts/run_daily_paper_trading.py`](scripts/run_daily_paper_trading.py),
   which reads back that day's already-written signals via the DataStore API (never recomputes a
   model itself) and proposes buy/sell actions through
   [`systems/ml_signal_engine/inference/paper_trading_step.py`](systems/ml_signal_engine/inference/paper_trading_step.py).
3. If `PAPER_TRADING_REQUIRE_APPROVAL=true` (the default — see `PAPER_TRADING_REQUIRE_APPROVAL` in
   [`config/settings.py:221`](config/settings.py)), proposed actions are written to
   `paper_trading/pending/{date}.json` and wait for a human accept/reject instead of executing
   automatically.
4. Portfolio state (cash, open positions, equity curve) persists across daily runs in
   `paper_trading/portfolio_state.json` ([`backtest/portfolio_state.py`](backtest/portfolio_state.py)).
   Every run — even a no-trade day — logs at least a heartbeat row to `paper_trading/executions/`,
   which is what Gate 7 counts.

### Running it

```bash
# One-off manual run for today (IST), rule-based exits, human approval required by default
python3 scripts/run_daily_paper_trading.py

# Backdated / specific date, or a different exit policy / position count
python3 scripts/run_daily_paper_trading.py --date 2026-06-29
python3 scripts/run_daily_paper_trading.py --n-positions 10 --exit-policy model

# For the 90-day gate to actually accrue, this needs to run every trading day —
# either keep the scheduler process alive continuously:
python -m ingestion.scheduler.daily_pipeline
# (no OS-level cron/systemd unit ships with the repo yet; the scheduler is
#  in-process APScheduler and only fires while this process is running —
#  see BuildLog.md "Full Project Status Review — 2026-07-02")
```

### Reviewing and approving pending trades

- **Dashboard:** the Ops app (`dashboard/static/ops/index.html`, served at
  `http://localhost:8000/ops/` once the API is running) shows pending actions, scheduler
  heartbeats, and lets you force-run any pipeline step manually. The ML app's
  [`dashboard/static/ml/positions.html`](dashboard/static/ml/positions.html) shows current open
  positions and equity curve.
- **API:** all paper trading endpoints are under `/api/v1/paper_trading/*`
  ([`datastore/api/routers/paper_trading.py`](datastore/api/routers/paper_trading.py)) — full list
  and interactive docs at `http://localhost:8000/docs#/Paper%20Trading` once running:
  - `GET /api/v1/paper_trading/state` — current portfolio (cash, positions, equity)
  - `GET /api/v1/paper_trading/trades` — trade history
  - `GET /api/v1/paper_trading/equity_curve` — equity over time
  - `GET /api/v1/paper_trading/gate_status` — live Gate 7 day-count progress
  - `GET /api/v1/paper_trading/pending` — actions awaiting human approval
  - `POST /api/v1/paper_trading/pending/{action_id}/accept` / `.../reject` — approve/reject a
    pending trade
  - `POST /api/v1/paper_trading/backdated_buy` — manually log a backdated position
- **Ops force-run** (no auth layer — deliberately, per the module docstring in
  [`datastore/api/routers/ops.py`](datastore/api/routers/ops.py)):
  `POST /api/v1/ops/steps/{step_name}/force` lets you trigger any pipeline step (including
  `paper_trade`) outside the schedule for testing.

### Historical replay (does NOT count toward Gate 7)

[`scripts/run_paper_trading_sim.py`](scripts/run_paper_trading_sim.py) replays the strategy over
historical feature parquets for backtesting purposes (output in `paper_trading/sim_reports/`). It
is explicitly blocked from writing into `paper_trading/executions/`, so it cannot be used to
artificially inflate the forward-day count — only `scripts/run_daily_paper_trading.py` running on
real, live dates counts.
