# Master Issues Catalog — Code + Data (2026-09-06)

**Purpose:** Single index of every code and data issue found during the 2026-09-05/06 investigation, before any fixes are applied. Detailed writeups live in the referenced documents; this file is the comprehensive punch list.

**Trigger:** Started from anomalous returns (1374%/4547%/-954%) in the Run Analysis panel of `/momentum-campaign-results`. Expanded into (1) a full price-discontinuity data audit and (2) a strategy-code audit after the user flagged that data alone couldn't explain everything.

---

## Category A: Data Quality Issues

### A1. Corporate-Action Price Discontinuities (PRIMARY DATA ISSUE)

**Scope:** 3,886 gaps >= 5% across 1,684 tickers (full inventory); 50 tickers fixed so far (1.3%)

| Sub-issue | Detail | Doc |
|-----------|--------|-----|
| RIGHTS/DIVIDEND/OTHER never backward-adjusted | No formula existed in `price_adjuster.py` for these action types | `DATA_INTEGRITY_ISSUES_2026_09.md` |
| Fyers `-EQ` exchange segment hardcoding | Tickers migrated to `-BE`/`-BZ`/`-SM` silently failed to update (16+ tickers) | `DATA_INTEGRITY_ISSUES_2026_09.md` |
| Compound BONUS+SPLIT under-captured | Registry stores only ONE leg's ratio for multi-leg events (STER, SUNILHITEC, RASOYPR, BIRLAPOWER, CGPOWER, JAYBARMARU, GAEL) | `DATA_INTEGRITY_ISSUES_2026_09.md` |
| Multi-event tickers only partially fixed | Phase 1 fixed only the MOST RECENT event per ticker; earlier events remain (INFY: 3 events, only 1 fixed; ASHAPURMIN: 3 events, only 1 fixed; AURUM: 3 events) | `DATA_INTEGRITY_ISSUES_2026_09.md` |
| Demerger/Scheme events unmodeled | No formula for spun-off entity value (GLODYNE, KESARENT, IDFC, ABIRLANUVO, SINTEX pending; PEL fixed) | `DATA_INTEGRITY_ISSUES_2026_09.md` |
| Unlogged corporate actions | A2ZINFRA's 2012-08-02 spike (11.9% gap, 3.8M volume) has NO corresponding corporate_actions row at all | `R11_BACKTEST_INVESTIGATION_2026_09_06.md` |
| RIGHTS-type — no pricing formula | COROENGG, CNOVAPETRO — explicitly deferred per user instruction | `DATA_INTEGRITY_ISSUES_2026_09.md` |

**Ticker-level detail + proposed remediation per ticker:** see the reference table in `DATA_INTEGRITY_ISSUES_2026_09.md` and the exported CSVs in `docs/findings_export_2026_09_06/`.

**Automated detection:** `datastore/integrity/checks.py::check_corporate_action_continuity()` now runs daily at a 5% gap threshold (lowered from 20% per user instruction), wired into `step_data_integrity_check` in the daily pipeline.

### A2. Point-in-Time / Survivorship Limitations (Data Methodology, Not a Bug)

| Issue | Detail | Impact |
|-------|--------|--------|
| R10 sector classification is NOT point-in-time | `stock_master.sector` is a static CURRENT snapshot; a ticker's 2026 sector is applied retroactively to its entire 2009-2026 history | Misattributes sector momentum for any company that changed sector classification over time. Already flagged in code comments (`common/sector_data.py`), not hidden — but not yet quantified how many tickers are affected. |

### A3. Legacy Pre-2010 Data Quality (Unknown Depth)

| Issue | Detail | Impact |
|-------|--------|--------|
| 66 gaps >= 50%, mostly dated 2006-2010 | CGPOWER (425.9%), JAYBARMARU (418.6%), GAEL (307%) — pre-Fyers era, sparse volume, uncertain data provenance | Empirical factors from this era may be unreliable; may need exclusion from backtests rather than correction |

---

## Category B: Strategy Code Bugs

### B1. **CRITICAL: Position weighting is a no-op for held positions** (Confirmed, bit-for-bit verified)

**File:** `momentum_framework/backtesting/portfolio.py::Portfolio.rebalance_to_target()`

**Bug:** Weight normalization (`target[t] / total_weight`) is computed only across `to_buy` (newly-purchased tickers this rebalance), never across the full target basket, and held positions are never re-weighted. In the common case of exactly one ticker rotating in, this always reduces to `1.0` regardless of the strategy's actual computed weight.

**Verified impact:**
- **R09** (Moreira-Muir vol-scaling — project's flagship risk-managed momentum strategy): 99% identical to plain R01 (71/72 configs). Vol-scaling exposure is silently near-inert.
- **R14** (Inverse-Volatility Weighting): 100% identical to R01 (72/72 configs).
- **R16** (Target-Volatility Weighting): 100% identical to R01 AND R14.
- **R08, R15, R17**: NOT fully collapsed (larger weight/exposure dispersion partially survives the bug), but per the same mechanism, their reported outperformance is understated / not fully trustworthy as computed.

**Proof:**
```
M02_R01_top5_lb6mo_21d_allrisk:                            cagr=0.08423033991949969, trades=1005
M02_R14_top5_lb6mo_21d_allrisk_weight-inverse_volatility:  cagr=0.08423033991949969, trades=1005
```
Identical to 16 significant digits, despite R14 computing genuinely different per-ticker weights (verified directly: 0.1793–0.2140 vs. equal 0.20).

**Doc:** `STRATEGY_CODE_BUGS_2026_09_06.md`

### B2. **R16's weighting formula is mathematically redundant with R14** (Independent second bug)

**File:** `momentum_framework/common/position_weighting.py::TargetVolatilityWeighting.compute_weights()`

```python
raw = (self.target_vol / vol).clip(upper=self.leverage_cap)
return self._normalize(raw)
```

`_normalize()` divides by the sum, and `target_vol` is a uniform scalar — it algebraically cancels: `normalize(target_vol/vol_i) ≡ normalize(1/vol_i)`, identical to R14's formula, UNLESS the `leverage_cap` clip actually engages (never observed to engage in the 7 tested R16 configs). **R16 is not currently a genuinely distinct strategy from R14**, independent of the B1 portfolio bug — even a fixed portfolio engine would still show R16 ≡ R14.

**Doc:** `STRATEGY_CODE_BUGS_2026_09_06.md`

### B3. R07's "crash-reduce-sizing" (trim existing holdings) is never exercised in the standard campaign

**File:** `momentum_framework/strategies/r07_crash_aware.py::R07QueueGenerator.build_jobs()`

```python
job["crash_reduce_sizing"] = None
```

The strategy's `rebalance()` only trims existing holdings during a crash `if self.crash_reduce_sizing is not None` — but the standard grid generator hardcodes it to `None` for every job. Verified against actual run configs: `crash_reduce_sizing=None` in 100% of sampled R07 runs. This means R07's ACTUAL tested behavior is "block new buys during a crash" only — it does NOT "reduce exposure" on already-held positions as CLAUDE.md's strategy table describes ("reduces exposure during VIX-flagged downturns"). This is a gap between documented intent and what was actually tested, not a crash-causing bug, but it means R07's crash-mitigation benefit in the backtest results is smaller than the design intends.

**Status:** New finding this session, no separate doc yet — needs a decision: (a) re-run R07 grid with `crash_reduce_sizing` set to a real value (e.g., 0.5) and compare, or (b) accept "buy-disable only" as the tested design and update CLAUDE.md's description to match.

### B4. CLAUDE.md documentation mismatch for R12 (Multi-Signal Ensemble vs. 1-Month Reversal + Liquidity)

**CLAUDE.md's R-family table states:**
> R12 | Multi-Signal Ensemble | `multi_signal_ensemble` | Blends 3 signals with equal weight: 12-month trailing momentum + pct_of_52wk_high (technical) + inverted Bollinger position (mean-reversion).

**Actual code** (`momentum_framework/strategies/r12_reversal_1mo.py`, and the equivalent legacy `backtest/generate_r12_queue.py`):
```python
STRATEGY_CODE = "R12"
RANK_METHOD = "trailing_reversal_1mo"
```
R12 is "1-Month Reversal + Liquidity" — ranks by 1-month trailing return, buys the LOSERS (lowest scores), optionally restricted to an ADTV liquidity quintile. `multi_signal_ensemble` as a rank_method **does not exist anywhere in the codebase** (verified via full-repo grep — zero hits).

**Impact:** Every prior analysis (including this session's own R12/R13 backtest anomaly review earlier in this conversation) that assumed "R12 = ensemble of 3 signals" was actually analyzing 1-Month Reversal + Liquidity. CLAUDE.md needs correction. Not a runtime bug — the code is internally consistent with itself and with the legacy system — but a stale/wrong specification document that could mislead future strategy comparisons or agent-driven decisions.

**Status:** New finding this session, no separate doc yet.

### B5. Minor: narrow exception-swallowing without logging (observability gap, not correctness bug)

**Files:** `r07_crash_aware.py:141`, `r08_bsc_volscale.py:85`, `r09_mm_volscale.py:161`

```python
except (ValueError, KeyError):
    return False  # or return 1.0
```

Scoped narrowly (not a bare `except:`), and each returns a safe, documented default (no crash regime / no exposure scaling). Not a correctness bug per se, but a genuine upstream data problem (e.g., a malformed benchmark series) would silently degrade these strategies to their base-case behavior with zero log trace. Worth adding a `logger.warning` at minimum before considering this closed.

---

## Category C: Verified-Clean Areas (Checked, No Issues Found)

For completeness — these were audited and found sound:

| Area | Verification | Conclusion |
|------|--------------|------------|
| `MetricsCalculator` (Sharpe/Sortino/CAGR/MaxDD/Calmar/WinRate formulas) | Read full source, standard formulas, correct annualization (`sqrt(252)`), correct running-max drawdown calc | Clean. Note: `win_rate` is % of DAILY returns positive, not % of trades profitable — a definitional nuance worth remembering when comparing to legacy per-trade win rates, not a bug. |
| R01, R03, R08, R10, R11, R13 `rank_method` vs CLAUDE.md table | Grepped `STRATEGY_CODE`/`RANK_METHOD` in each file, compared to documented table | All match except R12 (see B4) |
| Portfolio "phantom rotation" bug (2026-08-09 legacy incident) | Confirmed FIXED 2026-09-04 per `portfolio.py`'s own docstring (`rebalance_to_target()` replaced `execute()`) | Fixed, not reopened by this investigation |
| R09 regime-switching silent-disable (legacy `EnsembleRegimeDetector` import pointing at nonexistent module) | Confirmed FIXED — new framework's `common/regime_detection.py` implements it directly, no broken import | Fixed |
| `WeightedMomentumStrategy.rebalance()` → `Signal.size_multiplier` wiring | Directly tested: R14 computes genuinely different weights per ticker (0.18–0.21 range) | Signal generation is correct; the bug (B1) is downstream in Portfolio execution, not in the strategy's own weight computation |
| `StrategyBase.size_signals()`'s `has_own_weighting` guard | Confirmed it correctly no-ops for R14-R17 (prevents double-applying a second weighting pass) | Clean |

---

## Category D: Backtest Performance Anomaly Summary (Symptom-Level, Cross-Referencing A+B)

From `BACKTEST_PERFORMANCE_ANOMALIES_ROOT_CAUSE_ANALYSIS.md`:

- **1,129 of 1,354 framework runs (83%) show abnormal metrics** (volatility >40% or max DD <-80% or negative Sharpe)
- **R11/R13 (reversal strategies):** primarily a DATA problem (Category A) — buy artificial "oversold" gaps that never mean-revert (A2ZINFRA: -₹18.8M single-ticker loss)
- **R09/R14/R16:** primarily a CODE problem (Category B1/B2) — weighting logic doesn't actually execute, so results are just R01 relabeled
- **R07:** a CODE/CONFIG gap (Category B3) — crash mitigation is weaker than documented, not necessarily "wrong," but understates the strategy's designed defensive behavior
- **R12:** a DOCUMENTATION problem (Category B4) — analysis of "R12 anomalies" earlier in this investigation was actually analyzing a different strategy than CLAUDE.md describes

**These are separable and additive.** Fixing data issues alone would still leave R09 reporting R01's numbers. Fixing the weighting bug alone would still leave R11/R13 vulnerable to A2ZINFRA-style losses.

---

## Full Document Index

| Document | Scope |
|----------|-------|
| `DATA_INTEGRITY_ISSUES_2026_09.md` | Full data issue inventory, ticker-by-ticker remediation table, 3-phase remediation plan |
| `PRICE_CONTINUITY_REMEDIATION_STRATEGY_2026_09.md` | Detailed 3-phase execution plan for data fixes |
| `R11_BACKTEST_INVESTIGATION_2026_09_06.md` | A2ZINFRA root-cause deep dive (the case that started the data investigation) |
| `BACKTEST_PERFORMANCE_ANOMALIES_ROOT_CAUSE_ANALYSIS.md` | Cross-strategy abnormal-metrics survey, ticker-level loss attribution |
| `STRATEGY_CODE_BUGS_2026_09_06.md` | B1/B2 code bugs, full technical proof and fix recommendation |
| `MASTER_ISSUES_CATALOG_2026_09_06.md` | **This document** — comprehensive index of everything above, plus B3/B4/B5 (new, no dedicated doc yet) |
| `docs/findings_export_2026_09_06/*.csv` | Raw data: all 3,886 gap findings, prioritized remediation queue, summary stats |

---

## What Is NOT Yet Investigated (Acknowledged Gaps)

To be transparent about the boundaries of this pass:

1. **R03's skip-month logic** — read the strategy_code/rank_method but did not verify the skip-month date arithmetic itself for off-by-one errors.
2. **`common/liquidity.py`'s ADTV/circuit-lock filtering** — referenced by R07/R12 but not independently audited for correctness.
3. **`common/crash_regime.py`'s actual detection formula** — R07 depends on it; the drawdown/vol-percentile thresholds were read but not backtested in isolation against known historical crash dates (e.g., does it correctly flag March 2020?).
4. **`common/bollinger_signal.py`** (R13's core signal) — not independently verified against a hand-computed reference case.
5. **Transaction costs / slippage modeling** — `portfolio.py`'s own docstring states costs.py/tax.py are "layered on once trade-by-trade parity is checked" — i.e., NOT YET MODELED. All CAGR/Sharpe figures in this entire campaign are gross of costs. This is a known, documented gap, not a hidden one, but worth restating here since it affects every single number in every document above.
6. **Queue-generation parameter grids themselves** (band/lookback/cadence choices) — not audited for whether the grid appropriately covers the strategy's intended design space.

---

## Recommendation

Do not proceed to fixes until:
1. This catalog is reviewed and any priority/sequencing decisions are made explicit (which category fixes first: A or B?)
2. CLAUDE.md's R12 entry is corrected (near-zero cost, prevents further confusion)
3. A decision is made on R07's `crash_reduce_sizing` (re-run with a real value, or accept current design and update docs)
4. Items in "What Is NOT Yet Investigated" are triaged — either explicitly deprioritized or added to the queue

