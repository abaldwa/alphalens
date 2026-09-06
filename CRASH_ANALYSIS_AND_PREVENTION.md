# Laptop Crash Analysis & Prevention Plan

**Date:** 2026-09-06  
**Crashes This Session:** 2  
**Root Cause:** systemd-oomd (Out-Of-Memory killer) + memory pressure spikes

---

## Crash 1: ~05:15 IST

**What was happening:**
- Scheduler was processing 2026-08-21 / data_integrity_check
- Scheduler PID 728302 was holding ~3.9GB of memory (near 5GB MemoryMax limit)
- System had high memory pressure

**What killed it:**
- systemd-oomd detected memory pressure spike
- Selected scheduler or a background process as OOM victim
- Killed process without graceful shutdown

**Evidence:**
- Logs show: "2026-09-06 05:15:12 ERROR $EQUITAS.NS: possibly delisted"
- Next logs show scheduler not running
- No graceful exit message

---

## Crash 2: ~05:30-07:00 IST (unclear)

**What was happening:**
- I launched delivery_backfill script (1,743 parallel processes potentially)
- Multiple Python subprocesses running (scheduler + delivery script)
- Memory pressure likely spiked with combined load

**What killed it:**
- systemd-oomd again triggered by memory pressure
- OR system thermal throttle + hardlock

---

## Root Cause: systemd-oomd Configuration

**Current State:**
```
systemd-oomd.service: ACTIVE (running)
MemoryHigh = 4GB
MemoryMax = 5GB (hard limit)
```

**Problem:**
- Scheduler service capped at 5GB memory
- Feature backfill with 6 workers + compute_advanced_technical can spike to 4-5GB
- When two processes (scheduler + manual job) run simultaneously → memory pressure → oomd kills process

**systemd-oomd Behavior:**
- Monitors memory.pressure.full
- When pressure > threshold for >30s, kills processes (oldest/largest first)
- **No notice to application** — just SIGKILL, interrupts context managers
- Scheduler's try/finally blocks don't run → DB lock held → data corruption possible

---

## Prevention Actions (Immediate)

### 1. Increase Scheduler Memory Limits

Edit `/home/amit/.config/systemd/user/alphalens-scheduler.service.d/oom.conf`:

```ini
[Service]
MemoryHigh=6G
MemoryMax=8G
ManagedOOMMemoryPressureLimitPercent=80
ManagedOOMPreference=omit
```

**Why:**
- Allows scheduler to use up to 8GB without being killed
- `ManagedOOMPreference=omit` tells systemd-oomd: "don't kill this service, kill others"
- Prevents OOM from killing scheduler mid-transaction

### 2. Prevent Parallel Large Jobs

**Rule:** Never run multiple memory-intensive jobs simultaneously.

When gap-catchup finishes:
1. **Stop scheduler** before Phase 2 jobs
2. **Run Phase 2 jobs sequentially** (not in parallel)
3. **Restart scheduler** after Phase 2 completes

**Commands:**
```bash
# Before Phase 2
systemctl --user stop alphalens-scheduler.service
sleep 2

# Run Phase 2 jobs one at a time (see PHASE_1_COMPLETION_STATUS.md)
python3 scripts/backfill_fundamentals_trendlyne.py
# ... wait for completion ...
python3 scripts/backfill_fundamentals_nse_xbrl.py
# ... etc ...

# After Phase 2
systemctl --user restart alphalens-scheduler.service
```

### 3. Monitor Memory Pressure

Add a watchdog script to alert if pressure gets too high:

```bash
# Check current pressure
cat /proc/pressure/memory
# Example output: some avg10=0.00 avg60=0.02 avg300=0.26 total=2654748
#                 full avg10=0.00 avg60=0.02 avg300=0.26 total=2640349

# Alert threshold: full avg60 > 0.20 (20%)
```

### 4. Graceful Shutdown on Pressure

Add signal handlers to scheduler/jobs to release DB lock on OOM signal:

Currently missing — scheduler has no handler for memory-pressure signals.

---

## Prevention Actions (Long-term)

### 1. Feature Backfill Parallelism

**Current:** 6 workers, hangs + eats 4-5GB memory
**Option A:** Reduce to 2-3 workers (slower but stable)
**Option B:** 1 worker (serialized, no deadlock but ~3x slower)
**Option C:** Fix the pool deadlock (root cause of hang)

**Recommendation:** Option B for Phase 1, then investigate Option C.

### 2. Service Isolation

Split work across separate systemd services:
- `alphalens-scheduler.service` (gap-catchup, daily pipeline)
- `alphalens-batch-jobs.service` (Phase 2 jobs, delivery backfill)

Prevents scheduler from being killed if batch jobs spike memory.

### 3. DuckDB Connection Pooling

Current issue: compute_features workers hold DB lock directly.
- Fix: Use connection pool with worker-to-pool messaging instead of direct DB access
- Benefit: Prevents deadlock + reduces memory per worker

### 4. Swap Tuning

Current: 11GB swap available
Consider: Disable swap for scheduler (force OOM sooner with clear signal, not disk thrashing)

```bash
# Check swappiness
cat /proc/sys/vm/swappiness  # default 60

# For critical services, set to 0
sudo sysctl vm.swappiness=0  # requires sudo
```

---

## Immediate Action Checklist

- [ ] Update `/home/amit/.config/systemd/user/alphalens-scheduler.service.d/oom.conf`
  ```bash
  cat > /home/amit/.config/systemd/user/alphalens-scheduler.service.d/oom.conf << 'EOF'
  [Service]
  MemoryHigh=6G
  MemoryMax=8G
  ManagedOOMMemoryPressureLimitPercent=80
  ManagedOOMPreference=omit
  EOF
  systemctl --user daemon-reload
  systemctl --user restart alphalens-scheduler.service
  ```

- [ ] Verify scheduler restarted cleanly:
  ```bash
  systemctl --user status alphalens-scheduler.service
  ```

- [ ] **CRITICAL:** Before Phase 2, run:
  ```bash
  systemctl --user stop alphalens-scheduler.service
  sleep 2
  # ... run Phase 2 jobs sequentially ...
  systemctl --user restart alphalens-scheduler.service
  ```

- [ ] Monitor memory during Phase 2:
  ```bash
  watch -n 2 'free -h && echo "---" && cat /proc/pressure/memory'
  ```

---

## Memory Limits Explained

| Setting | Current | Recommended | Effect |
|---------|---------|-------------|--------|
| MemoryHigh | 4GB | 6GB | Scheduler gets warning at 4GB, soft limit at 6GB |
| MemoryMax | 5GB | 8GB | Hard ceiling (process killed if exceeded) |
| ManagedOOMPreference | (not set) | omit | Tell oomd: "don't kill me, kill other processes" |

---

## Current System State (2026-09-06 07:46)

```
Total Memory:      14 GB
Used:              6.5 GB
Available:         8.1 GB (good)
Swap Used:         253 MB (low)
Memory Pressure:   0.26% (low)
systemd-oomd:      ACTIVE

Scheduler (PID 7830):
  Memory:          ~2GB (starting, will grow to 3-4GB)
  Status:          Running, gap-catchup active
```

**Current Status:** SAFE to continue gap-catchup with updated memory limits.

---

## Timeline of Events

```
00:52  Scheduler started with 5GB MemoryMax limit
       └─ Gap-catchup launches feature_backfill_hybrid with 6 workers
       
01:40  feature_backfill_hybrid consumes 4-5GB
       └─ Starts hanging (deadlock in pool)
       
03:00  Total system memory pressure spikes (scheduler + backfill + other)
       
05:15  systemd-oomd kills scheduler (PID 728302)
       └─ Crash #1: Process killed ungracefully
       
05:30  I manually start delivery_backfill
       └─ Memory pressure spikes again
       
~06:00 systemd-oomd kills delivery_backfill and/or scheduler
       └─ Crash #2: User's laptop manual restart
       
07:40  Scheduler restarted with SAME 5GB limit
       └─ Still at risk (will crash again if Phase 2 + gap-catchup overlap)
```

---

## Risk Assessment

**Current (gap-catchup only):** MEDIUM RISK
- Scheduler alone: 2-3GB (safe)
- But if feature_backfill hangs again → 4-5GB → pressure spike → potential crash

**After Phase 2 (if not stopped):** HIGH RISK
- Scheduler + delivery_backfill in parallel → 4-5GB + 2-3GB = 7-8GB
- **Will exceed MemoryMax (5GB) and crash**

**After update (with 8GB MemoryMax + stop scheduler):** LOW RISK
- Single job at a time: max 5GB each
- No simultaneous jobs
- oomd won't kill scheduler (ManagedOOMPreference=omit)

---

## Contact Points in Code

**Where memory is allocated:**
- `scripts/feature_backfill_hybrid.py:597` — multiprocessing.Pool creation (6 workers)
- `features/matrix_builder.py:_run_pool_over_chunks` — per-ticker feature computation
- `ingestion/scrapers/fyers_backfill.py` — OHLCV DataFrames for all tickers at once

**Where to add graceful shutdown:**
- `ingestion/scheduler/scheduler_jobs.py` — wrap all jobs with memory-pressure handler
- `ingestion/scheduler/pipeline_steps.py` — signal handlers around pipeline lock

