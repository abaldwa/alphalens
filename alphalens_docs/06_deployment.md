# AlphaLens — Deployment & Infrastructure
## Laptop-Only (Oracle Cloud deferred — see "Oracle Cloud (deferred)" below)

---

## Architecture Decision (current — SPEC-SCHED-009)

- **Laptop (AMD Ryzen 5 7535U, 16 GB RAM):** Everything — model training, feature
  computation, backtesting, AND all daily scraping/ingestion. A single persistent
  process (`ingestion/scheduler/daily_pipeline.py`, registered via APScheduler —
  see "Running the Scheduler" below) replaces what Oracle Cloud's always-on cron
  would have done.
- **Why laptop-only:** Oracle Cloud Free Tier's ARM A1.Flex shape (the one offering
  4 OCPU/24GB for free) had zero available capacity in `ap-mumbai-1` even at the
  smallest size (1 OCPU), and the account's Free Trial status blocked subscribing
  to an alternate region without upgrading to Pay-As-You-Go. Rather than block
  Phase 1 on Oracle capacity availability, the project moved to laptop-only
  operation — see BuildLog.md "Laptop-only pivot" for the full investigation.
- **What this costs:** the laptop must be on (not asleep) at the scheduled pipeline
  time to capture same-day data; if it's off, the gap-detector (SPEC-SCHED-003/004)
  catches up automatically on next startup for everything *except* the live
  intraday option-chain snapshot (3:25 PM IST), which is genuinely non-recoverable
  if missed. Option chain / F&O features are Phase 2 scope — not a Phase 1 blocker.
- **Why not cloud for training regardless:** LightGBM on 500 stocks × 330 features
  trains in < 2 min on a laptop. Cloud adds cost and latency with no benefit at
  Phase 1–2 scale — this part of the original reasoning is unchanged.

---

## Running the Scheduler (replaces OS-level cron — SPEC-SCHED-001)

`ingestion/scheduler/daily_pipeline.py` is the one process that owns all
recurring work. It is **not** invoked via crontab — it registers its own
recurring job with APScheduler (persistent SQLAlchemyJobStore,
`datastore/normalised/scheduler.db`) and then blocks, so the job survives
this process restarting. Run it once and leave it running:

```bash
# Foreground (for testing — Ctrl-C to stop):
.venv/bin/python3 -m ingestion.scheduler.daily_pipeline

# Background, survives terminal close:
nohup .venv/bin/python3 -m ingestion.scheduler.daily_pipeline > /tmp/daily_pipeline.log 2>&1 &
```

On start, it immediately runs a catch-up pass (gap-detect + backfill any
missed trading days, then today if not already done — SPEC-SCHED-001/003/004),
then registers the recurring job for **18:00 IST, Mon–Fri** (after typical
NSE bhavcopy/FII-DII publish times) and goes back to sleep until the next
trigger. If the laptop is off when 18:00 fires, the next time this process
starts, the startup catch-up pass picks up the missed day(s) automatically —
there is no separate "did we miss a run" step to remember.

For a more permanent setup that survives reboots without manually re-running
the `nohup` command, use a systemd user service:

```ini
# ~/.config/systemd/user/alphalens-pipeline.service
[Unit]
Description=AlphaLens daily pipeline scheduler

[Service]
WorkingDirectory=/home/user/alphalens
ExecStart=/home/user/alphalens/.venv/bin/python3 -m ingestion.scheduler.daily_pipeline
Restart=on-failure

[Install]
WantedBy=default.target
```
```bash
systemctl --user daemon-reload
systemctl --user enable --now alphalens-pipeline.service
journalctl --user -u alphalens-pipeline.service -f   # tail logs
```

What's actually wired into the recurring job today (Phase 0.6) vs. deferred:
- ✅ `download_bhavcopy` — NSE daily OHLCV + delivery
- ✅ `download_macro` — India VIX, FII/DII, USD/INR (each fails independently, non-blocking)
- ⚠️ `download_fno` — attempted, but NSE's F&O bhavcopy archive endpoint currently
  serves a PDF instead of a CSV (confirmed broken); caught and logged, never blocks
  the rest of the pipeline. Phase 2 scope regardless.
- ✅ `adjust_prices` — idempotent corporate-action adjustment across the universe
- ⛔ `compute_features`, `run_models`, `write_signals` — raise `NotImplementedError`
  on purpose; `features/` and `systems/ml_signal_engine/` aren't built yet (Phase 1).
  Each phase fills in its dispatch entry in `ingestion/scheduler/daily_pipeline.py`
  without touching the generic scheduler engine (SOLID-O).

---

## Oracle Cloud (deferred)

Everything below this point describes the Oracle Cloud Free Tier setup that
was attempted and abandoned for now (see "Architecture Decision" above).
Kept for reference in case always-on intraday capture (live option chain)
becomes necessary in Phase 2+ and Oracle capacity is available by then —
**not part of the current laptop-only setup, nothing below is required.**

### Oracle Cloud Free Tier Setup

### Provision A1 instance
```bash
# In Oracle Cloud Console:
# Compute → Instances → Create Instance
# Shape: VM.Standard.A1.Flex (Ampere ARM)
# OCPUs: 4, RAM: 24 GB
# Image: Ubuntu 22.04
# Region: ap-mumbai-1 (if available) or ap-singapore-1

# If "Out of Host Capacity" error:
# 1. Try ap-hyderabad-1
# 2. Try creating at off-peak hours (2–4 AM IST)
# 3. Upgrade to PAYG (keeps Always Free benefits, removes guardrails)
```

### Prevent idle reclamation
```bash
# Oracle reclaims instances with < 20% CPU at 95th percentile over 7 days
# Add to crontab on Oracle instance:
*/3 * * * * python3 /home/ubuntu/alphalens/keep_alive.py
```
```python
# keep_alive.py — lightweight computation to prevent reclamation
import numpy as np, time
arr = np.random.randn(1000, 1000)
_ = np.linalg.eigvals(arr)  # ~2 seconds CPU spike
```

### Oracle instance setup
```bash
# On Oracle ARM instance (Ubuntu 22.04)
sudo apt update && sudo apt install -y python3.11 python3-pip cron
pip3 install requests beautifulsoup4 pandas pyarrow schedule oci

# Install TA-Lib (ARM Ubuntu — compile from source)
wget https://github.com/ta-lib/ta-lib/releases/download/v0.4.29/ta-lib-0.4.29-src.tar.gz
tar xzf ta-lib-0.4.29-src.tar.gz && cd ta-lib-0.4.29
./configure --prefix=/usr && make && sudo make install
pip3 install ta-lib
```

---

## Cron Schedule (Oracle Cloud instance)

```bash
# crontab -e  (on Oracle instance)

# Option chain snapshot — 3:25 PM IST (UTC+5:30 = 09:55 UTC)
55 9 * * 1-5 python3 /home/ubuntu/alphalens/pipeline/ingest/option_chain.py

# NSE bhavcopy download — 4:05 PM IST (10:35 UTC)
35 10 * * 1-5 python3 /home/ubuntu/alphalens/pipeline/ingest/bhavcopy.py

# F&O bhavcopy download — 4:10 PM IST (10:40 UTC)
40 10 * * 1-5 python3 /home/ubuntu/alphalens/pipeline/ingest/fno.py

# FII/DII daily data — 6:00 PM IST (12:30 UTC)
30 12 * * 1-5 python3 /home/ubuntu/alphalens/pipeline/ingest/macro.py

# AMFI MF holdings — 5th of month 8:00 AM IST
0 2 5 * * python3 /home/ubuntu/alphalens/pipeline/ingest/amfi_holdings.py

# Keep-alive — every 3 minutes
*/3 * * * * python3 /home/ubuntu/alphalens/keep_alive.py
```

---

## Laptop Cron / Schedule (Ubuntu) — superseded by "Running the Scheduler" above

The daily pipeline is **no longer** a crontab entry — see "Running the
Scheduler" above (SPEC-SCHED-001: "don't keep jobs out of the scheduler" —
recurring work belongs in the persistent APScheduler job store, not OS-level
cron, so it gets checkpoint-resume and gap-backfill for free).

Two future jobs (`weekly_run.py`, `drift_monitor.py`) aren't built yet
(Phase 1+ scope) — when they are, they should also be registered as
APScheduler jobs in `ingestion/scheduler/daily_pipeline.py` (or a sibling
module reusing `pipeline_scheduler.create_scheduler()`), not added as new
raw crontab lines:

```bash
# Indicative target schedule, NOT crontab entries:
# Weekly multibagger run — Monday 5:45 PM IST
# Model health check — daily 7:00 AM
```

---

## Oracle $300 Trial Credit Strategy (30 days) — deferred along with Oracle Cloud

Not currently in use (see "Architecture Decision" above). Kept for
reference if Oracle capacity becomes available later and trial credit is
still on the account. Use trial credits for one-time heavy tasks, not
persistent infrastructure:

1. **Spin up 8 OCPU / 64 GB instance:** Run full 5-year OHLCV backfill for 500 stocks
   (~2–3 hours vs days on free tier). Compute baseline feature store.
2. **GPU instance (A10):** Benchmark TFT and BiLSTM training speed for Phase 3 planning.
3. **Download everything** to laptop before 30 days expires.
4. **Do NOT** use trial credits for anything you'll rely on long-term.

---

## Environment Setup (Laptop — Ubuntu 22.04)

```bash
# Install Miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh

# Create environment
conda create -n alphalens python=3.11 -y
conda activate alphalens

# Phase 1 dependencies
pip install -r requirements/phase1.txt

# TA-Lib (Ubuntu — official wheel since Aug 2025)
pip install ta-lib

# Intel acceleration
pip install daal4py
```

---

## requirements/phase1.txt

```
lightgbm>=4.5
catboost>=1.2
xgboost>=3.0
hmmlearn>=0.3.2
scikit-learn>=1.5
mapie>=1.3.0
optuna>=4.7
imbalanced-learn>=0.12
shap>=0.45
# mlfinlab intentionally omitted: not available on PyPI; triple-barrier
# labeling is implemented natively per SPEC-MODEL-002
lifelines>=0.28
scikit-survival>=0.23
ruptures>=1.1.9
hdbscan>=0.8.38
river>=0.21
pandas>=2.2
numpy>=1.26
pyarrow>=12.0
sqlalchemy>=2.0
requests>=2.31
beautifulsoup4>=4.12
schedule>=1.2          # Oracle Cloud simple cron (lightweight) — only needed if Oracle is revisited
APScheduler>=3.11      # Laptop scheduler (persistent, catch-up capable) — the one actually in use today
ta-lib>=0.6.8
daal4py
pytest>=8.0
pytest-cov
```

## requirements/phase3.txt (add to phase1)

```
torch>=2.4
pytorch-forecasting>=1.1
pytorch-tabnet>=4.1
mamba-ssm>=2.0
```

---

## Windows Users (WSL2 Setup)

```powershell
# In PowerShell (admin)
wsl --install

# After restart, in WSL2 Ubuntu terminal:
# Follow the same Ubuntu setup above
# All libraries including mamba-ssm work in WSL2
```

**Note:** Native Windows (without WSL2) works for all Phase 1–2 libraries.
mamba-ssm (Phase 3) requires WSL2 or Ubuntu.
TA-Lib: `pip install ta-lib` works natively on Windows since Aug 2025.
