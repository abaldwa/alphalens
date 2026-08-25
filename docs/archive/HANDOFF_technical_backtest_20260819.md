# Handoff — Technical Analysis backtest launch (2026-08-19, ~22:35 IST)

Paste this into a fresh conversation. Everything below is verified state, not plan.

---

## 1. What the next session must do

**Launch the 372-job Technical sweep, three staggered shards, and merge in the morning.**

The queue file is already built and committed-ready:
`backtest/queues/ta_full_grid_20260819.json` (untracked — `git add` it).

### The one number that decides the shape

A 4-job probe measured Technical per-job cost. It is NOT like momentum.
**Probe is COMPLETE — these are final numbers, 4 ok / 0 failed:**

```
job 1 (cold, A1)   104.6s   panel hits 0/1
job 2 (warm, A2)   312.9s   panel hits 1/2    <- 3x SLOWER than the cold job
job 3 (warm, A3)   489.3s   panel hits 2/3
job 4 (warm, A4)   173.7s   panel hits 3/4
AVERAGE            270.1s/job   (18.0 min for 4 jobs)
shared_panels: ohlcv_hits 3 / misses 1, artifact_hits 3 / misses 1
```

The cache hit on every warm job and they were still slower than the cold one.
Per-template simulation cost dominates and varies 4.7x between templates
(104s to 489s). **Do not quote the momentum 31.2s→17.3s speedup for Technical.**
Round-robin sharding (`idx % num_shards`) is fine despite the variance — 372
jobs is enough to average out.

### Projected wall clock at 270.1s/job for 372 jobs

```
1 shard    27.9 h     no
2 shards   14.0 h     no — misses the morning
3 shards    9.3 h     YES — launch ~22:45, done ~08:00
4 shards    7.0 h     memory-risky, see below
```

**Recommendation: 3 shards.** The box has 14 cores, so CPU is not the
constraint — memory is. 3 x ~2.5 GB steady = ~7.5 GB against ~10 GB available.
4 shards (~10 GB steady) leaves no headroom for the 6.5 GB fetch spikes.

### Memory, measured (not assumed)

Sampled every 15s on the live probe process:

```
peak       6.47 GB   transient, ONCE, during the initial OHLCV fetch
steady     2.0-3.0 GB
```

Box is 14 GB total / 14 cores, ~10 GB available, 11 GB swap (4-5 GB already in use).

Implications:
- Three shards at steady state = ~7.5 GB of ~10 GB available. Fits.
- The 6.5 GB spike is per-process and one-time. **Stagger each shard by ~5
  minutes** so no two fetches overlap — concurrent spikes are what caused the
  VSCode swap-thrash earlier in this work.
- `MemoryHigh=8G` per scope. Do NOT use 4G: that value throttled a momentum
  shard earlier and the kernel-forced reclaim was self-inflicted, not external.
- Swap-OUT rate (`/proc/vmstat` `pswpout` delta) is the true distress signal,
  not `free` and not available memory.

### Launch commands

The average is confirmed (270.1s); these are ready to run as-is.

Each shard needs its OWN DuckDB file — DuckDB is single-writer-per-file and a
second writer fails slowly (measured 1,384s/job before giving up):

```bash
cd /home/amit/projects/AlphaLens
# shard 0
systemd-run --user --scope -p MemoryHigh=8G -p ManagedOOMPreference=omit \
  --unit=ta-shard0 -- env PYTHONPATH=$PWD \
  ALPHALENS_BACKTEST_DUCKDB_PATH=datastore/backtest_store/ta_shard0.duckdb \
  .venv/bin/python -m backtest.run_sweep_inprocess \
    --queue-file backtest/queues/ta_full_grid_20260819.json \
    --report-suffix ta_grid_20260819 --shard 0 --num-shards 3 \
  > /tmp/.../ta_shard0.log 2>&1 &

# wait ~5 min each, then shard 1 and shard 2 with --shard 1 / --shard 2,
# --num-shards 3, and their own ta_shard1.duckdb / ta_shard2.duckdb
```

**A shard DB must be seeded with `strategy_registry` before it will run.** A
fresh file has no registry and every job dies on horizon resolution. Seed it:

```python
c = duckdb.connect("datastore/backtest_store/ta_shard0.duckdb")
c.execute(f"ATTACH '{BACKTEST_DUCKDB_PATH}' AS real (READ_ONLY)")
c.execute("CREATE TABLE strategy_registry AS SELECT * FROM real.strategy_registry")
c.execute("DETACH real")
```
(7,783 rows. This bit me on the probe — first attempt failed all 4 jobs instantly.)

### Merge in the morning

```bash
PYTHONPATH=$PWD .venv/bin/python -m scripts.merge_backtest_shards \
  --shard-db datastore/backtest_store/ta_shard0.duckdb \
  --shard-db datastore/backtest_store/ta_shard1.duckdb \
  --shard-db datastore/backtest_store/ta_shard2.duckdb --dry-run
```
Drop `--dry-run` when the counts look right. The script is idempotent (skips
run_ids already present). It was rewritten this session — see §3.

---

## 2. Finding that needs YOUR decision (Amit)

**Three technical templates have no `strategy_registry` row: C3, F2, F7.**

They exist in `backtest/select_ta_strategies.py:35` ("Dual Momentum" etc.) and in
`backtest/config/derived_exit_params.json`, but were never registered. Since A95
the backtest resolves definitions from the registry with **no fallback** to the
Python dicts, so all 18 of their jobs fail instantly.

I dropped those 18 jobs (390 → 372) so the run's failure count means something,
and recorded the reason in the queue file's `_note`. **I did not invent
definitions for them** — registering a strategy is a declaration about what it
IS, and that is your call. Register them and re-add if you want the full 65.

Registry currently holds 63 active technical templates; the queue wanted 65.

---

## 3. What was completed this session (all committed)

Commit `34a17a75` "Renumber the momentum rank bands, and stop cadence living
only in a name". HEAD is now that; `b5fad906` beneath it is a CONCURRENT
session's frontend commit (shared worktree — verify branch before/after any
multi-agent git pass).

Branch: `feature/unified-backtest-report-ui`

Contents:

1. **RANK_BANDS renumbered to contiguous 1-7** + three one-shot migrations
   (retire superseded ids, purge momentum results measured on the old
   partition, drop 480 orphaned band catalog rows).

2. **`rebalance_cadence_days` now persisted in `config_json`** — your item 1.
   Until now cadence survived ONLY as a word inside strategy_id
   (`..._bimonthly_...`), so anything wanting the number had to parse a name.
   The EFFECTIVE value is stored (what `core/engine.py:1007` slices on), not
   the override, which is None whenever the horizon default was taken.
   Guarded by `test_stored_config_records_the_rebalance_cadence`
   (mutation-verified: it fails when the key is removed).

3. **FY phantom-column fix** — `collectFiscalYears` unioned the DISPLAY label
   including the trailing `*` partial marker, minting `FY2020*`/`FY2022*`
   columns populated in 1 row out of 632. Cells now read through `yoyValueFor`;
   partial-ness is stated per CELL. **CAGR was never affected** — `rollingFromYoy`
   reads each row's own array, not the shared column axis.

4. **`merge_backtest_shards.py` rewritten** after three defects surfaced on
   first real use. The dangerous one: `INSERT ... SELECT *` matches BY POSITION,
   and the two stores hold the same 30 columns in DIFFERENT order — diverging at
   index 21 (`live_eligible` BOOL vs `regime_breakdown_json` VARCHAR). DuckDB's
   type check aborted the merge, which is the only reason it was caught; a
   type-compatible mismatch would have written 630 runs with silently transposed
   values. Columns are now named explicitly.

5. **mypy debt paid off** — pre-commit's mypy hook is STRICTER than the
   shrink-only baseline gate and rejected the commit over 86 pre-existing errors
   in the 12 touched files. Fixed rather than bypassed. Two were real bugs:
   an `r` shadowed inside its own loop's format string, and `artifact_key`
   typed its cache key as `Tuple` when its only caller passes `id(frame)`.
   Repo-wide: 1073 → 1052 errors, 193 → 188 files.

**Momentum sweep is DONE and merged**: 1,260/1,260 jobs ok, 0 failures. All five
rebalance cadences (weekly/biweekly/monthly/bimonthly/quarterly) now visible in
the report — that was your "I do not see the weekly or biweekly data" item.

---

## 4. Standing constraints (violating these has cost real work before)

- **Never `systemctl restart alphalens-api.service` while a queue is running** —
  killed two queues on 2026-07-24.
- **Never run pytest against the prod DuckDB while a sweep is live** — the test
  suite write-locks it. Check `fuser` first.
- **No source edits while a queue runs** — a mid-queue edit killed 22 jobs
  (2026-08-09). The in-process sweep loads source once at start, but don't.
- **Never `git add -f` gitignored output** (`backtest/reports/`) — pre-commit's
  stash-rollback silently restores them.
- **No synthetic/test rows in the real DuckDB**, even temporarily. Use an
  isolated file (that is why the probe used a scratchpad DB).
- **`/tmp` is RAM-backed tmpfs (7.3 GB)** — big scratch files there eat the
  memory you are trying to protect. Errno 122 = tmpfs full.
- **All HTML reports publish inside the app**, never as a Claude Artifact.
- Only `run_sweep_inprocess` gets the sharing optimisations; the other three
  backtest routes structurally cannot. This is the "mistake committed last time"
  you flagged — see `backtest/run_sweep_inprocess.py`'s module docstring and
  `tests/quality/test_sweep_optimisations_wired.py`.

---

## 5. Still open (not blocking the launch)

- Register C3/F2/F7 (§2) — needs your decision.
- `max_defensive`'s 420 momentum jobs blocked on `orthogonalize_vs_size_beta` +
  `market_cap_panel`/`beta_map` wiring; `topN_curated` classification unresolved.
- A runtime guard so Technical cannot silently take a subprocess-per-job route.
  Proposed, not built — today it is a launch-time discipline, not enforced code.
- Untracked and unexplained: `CLAUDE.md` and `.claudeignore` have no git history.
  Left alone deliberately.
