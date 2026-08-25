# Unified Backtest Launch Guide

**Status:** Ready to execute  
**Total Jobs:** 124  
**Expected Duration:** 12-14 hours  
**Timeline:** Start now, results by tomorrow morning

---

## What This Does

This unified backtest in a **single queue** combines all stages:

1. **Pilot Validation (4 jobs, ~10 min)**
   - M10 subset (2 lookbacks × 2 rebalances, 7-year window 2019-2025)
   - Validates reproducibility vs. backtest_all.csv baseline

2. **M13 Full Matrix (60 jobs, ~8 hours)**
   - Nifty ranks 1-800, AllRisk only
   - 4 lookbacks × 5 rebalance cadences × 3 top-N sizes (30/40/50)
   - Full 18-year window (2008-2026)

3. **Results Aggregation & Reporting (automatic)**
   - Compares pilot to baseline (gate: must pass)
   - Selects best M1-M12 per overlapping band
   - Ranks M13 alternatives
   - Generates final recommendation report

---

## Step 1: Pre-Flight Checks (5 min)

```bash
# Check DuckDB is unlocked
fuser ~/.local/share/AlphaLens/data/*.duckdb
# Expected: no output (or just lists pids, that's OK)

# Check scheduler isn't holding lock
systemctl --user status alphalens-scheduler.service
# If running, optional to stop; if hung, restart:
# systemctl --user restart alphalens-scheduler.service

# Verify OHLCV data is current
sqlite3 ~/.local/share/AlphaLens/data/alphalens.duckdb \
  "SELECT MAX(date) FROM ohlcv_adjusted"
# Should show recent date (within last 3 days)
```

---

## Step 2: Launch Backtest (1 command)

```bash
cd /home/amit/projects/AlphaLens

bash scripts/launch_unified_backtest.sh
```

This will:
- Pre-create snapshot cache directory
- Launch queue in background with `nohup`
- Print monitoring commands
- Save process ID to `.unified_backtest_pid`

**Expected output:**
```
AlphaLens Unified Backtest Suite
Started: Fri Aug 23 19:40:00 IST 2026

[PREFLIGHT] Checking environment...
[PREFLIGHT] Checking DuckDB lock status...
[STAGE_1-5] Launching unified backtest queue...
  Total jobs: 124
  Expected runtime: 12-14 hours

[QUEUE_RUNNING] Process ID: 12345

To monitor progress:
  tail -f execution_logs/unified_backtest_*.log
```

---

## Step 3: Monitor Progress

### Real-Time Log (run in terminal)

```bash
tail -f execution_logs/unified_backtest_*.log
```

### Check Job Status (every 30 min)

```bash
# Count completed vs. total
sqlite3 ~/.local/share/AlphaLens/data/backtest.duckdb \
  "SELECT COUNT(*) FROM backtest_runs WHERE strategy_key LIKE 'm10_%' OR strategy_key LIKE 'm13_%'"

# See active query
fuser ~/.local/share/AlphaLens/data/*.duckdb

# Check running processes
ps aux | grep run_strategy_queue
```

### If Queue Stalls (lock contention)

```bash
# Kill stuck processes
kill $(cat .unified_backtest_pid)

# Restart
bash scripts/launch_unified_backtest.sh
```

---

## Step 4: Process Results (automatic after ~12-14 hours)

Once backtest completes:

```bash
python3 scripts/aggregate_unified_results.py
```

This will:
1. **Validate pilot** against baseline (must pass)
2. **Select best M1-M12** per band
3. **Rank M13** alternatives
4. **Generate final report** → `backtest/reports/unified_results_final.json`

**Output preview:**
```
[VALIDATE_PILOT] Comparing M10 subset to baseline...
  Baseline M10 max Sharpe: 0.92
  Pilot M10 max Sharpe:    0.92
  Difference: 0.000
  ✓ PASS: Pilot results within 5% of baseline

[SELECT] Identifying best M1-M12 strategies per band...
  ✓ M1/M2: M10_1_50_allrisk_lb3mo_bimonthly_top10_21d
    → Sharpe 0.92, CAGR 21.2%, MaxDD -44.1%

[RANK_M13] Top M13 strategies by Sharpe...
  strategy_key                                 sharpe  cagr       max_dd
  m13_1_800_allrisk_lb6mo_quarterly_top40      0.88    18.5%      -32.1%
  m13_1_800_allrisk_lb6mo_quarterly_top50      0.86    19.2%      -31.5%
  ...

SUMMARY
Core M1-M12 Strategies:     6
M13 Alternatives Available: 10
Total Candidates:           16
```

---

## Step 5: Decision & Deployment

Based on final report, decide:

### Option A: Keep M1-M12 (Conservative)
- Deploy 6 core strategies (1 per band)
- Skip M13 (or use as hedge only)
- **Expected:** 18% CAGR, 0.93 Sharpe, -47% MaxDD

### Option B: Replace M11/M12 with M13 (Balanced)
- Deploy 5 core strategies (M1, M4, M5, M7, M9)
- Add M13 Top 30/40/50 variants (3 strategies)
- **Expected:** 17.5% CAGR, 0.90 Sharpe, -40% MaxDD (3pp improvement on DD)

### Option C: Full 15-Strategy Portfolio (Aggressive)
- Deploy all 6 core M1-M12
- Add 3 M13 variants
- Add 2 diversifiers (alt-lookbacks from best bands)
- **Expected:** 17.5% CAGR, 0.90 Sharpe, high diversification

**Next:** Gate on Phase 7 (crash-aware overlay) implementation

---

## Real-World Timeline

| Time | Event |
|------|-------|
| **Now (Fri 19:45)** | Launch `bash scripts/launch_unified_backtest.sh` |
| **Fri 20:00** | Pilot (4 jobs) completes, validation checks pass |
| **Fri 20:30** | M13 matrix begins (60 jobs queued) |
| **Sat 04:00** | M13 matrix ~50% complete (~8 hours in) |
| **Sat 08:00** | All 124 jobs complete |
| **Sat 09:00** | Run `python3 scripts/aggregate_unified_results.py` |
| **Sat 09:30** | Final report ready for decision |

---

## Troubleshooting

### "ERROR: python3 not found"
```bash
which python3
# If nothing, use: python instead of python3
```

### "ERROR: Queue file not found"
```bash
ls -la backtest/queues/unified_m1_m13_consolidated.json
# Should exist; if not, verify you're in /home/amit/projects/AlphaLens
```

### "WARNING: DuckDB appears to be in use"
```bash
# Check what's holding the lock
fuser ~/.local/share/AlphaLens/data/*.duckdb

# If scheduler: restart it
systemctl --user restart alphalens-scheduler.service

# If pytest: wait or kill it
killall pytest
```

### "Backtest queue still running after 16 hours"
This suggests:
1. DuckDB lock contention (check `fuser` output)
2. Slow disk I/O (check `iostat`)
3. Memory pressure (check `free -h`)

**Recovery:**
```bash
# Kill queue process
kill $(cat .unified_backtest_pid)

# Check incomplete jobs
sqlite3 ~/.local/share/AlphaLens/data/backtest.duckdb \
  "SELECT COUNT(*) FROM backtest_runs WHERE strategy_key LIKE 'm13_%'"

# Resume from last completed job
# (queue will skip already-completed strategies)
bash scripts/launch_unified_backtest.sh
```

---

## Files Created

| File | Purpose |
|------|---------|
| `backtest/queues/unified_m1_m13_consolidated.json` | Main queue (124 jobs) |
| `scripts/launch_unified_backtest.sh` | Launch script |
| `scripts/aggregate_unified_results.py` | Results processor |
| `execution_logs/unified_backtest_*.log` | Live progress log |
| `backtest/reports/unified_results_final.json` | Final report (generated) |

---

## Success Criteria

✅ **Pilot passes:** M10 Sharpe within 5% of baseline  
✅ **M13 runs:** All 60 jobs complete without data errors  
✅ **Report generated:** JSON with 6 core + N M13 alternatives  
✅ **No outliers:** MaxDD > -50% or Sharpe < 0.65 flagged  
✅ **Ready for Phase 7:** Strategies identified for crash-aware overlay

---

## Ready?

Run this now:

```bash
cd /home/amit/projects/AlphaLens && bash scripts/launch_unified_backtest.sh
```

Monitor with:

```bash
tail -f execution_logs/unified_backtest_*.log
```

Come back in ~14 hours, then:

```bash
python3 scripts/aggregate_unified_results.py
```

---

**Questions?** Check:
1. `BacktestPlan_StreamlinedM1toM13.md` (detailed plan)
2. `CLAUDE.md` (project reference)
3. `we-already-have-momentum-sorted-crayon.md` (R-family spec)
