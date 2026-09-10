# Master Issues Catalog — Code + Data (2026-09-06)

**Purpose:** Single index of every code and data issue found during the 2026-09-05/06 investigation. Detailed writeups live in the referenced documents; this file is the comprehensive punch list.

**Trigger:** Started from anomalous returns (1374%/4547%/-954%) in the Run Analysis panel of `/momentum-campaign-results`. Expanded into (1) a full price-discontinuity data audit and (2) a strategy-code audit after the user flagged that data alone couldn't explain everything.

**2026-09-06 remediation pass:** User directed a systematic resolution: (Rule 1) data issues that can't be resolved without unverified/risky guesses get marked **non-backtestable** and excluded via `config/backtest_excluded_tickers.json` rather than patched with a guessed factor — no re-ranking needed, since excluded tickers simply never enter the universe. Data issues reviewed first, then code issues; resolved items marked below.

---

## Category A: Data Quality Issues

### A1. Corporate-Action Price Discontinuities (PRIMARY DATA ISSUE)

**Scope:** 3,886 gaps >= 5% across 1,684 tickers (full inventory); 50 tickers fixed so far (1.3%)

| Sub-issue | Detail | Doc | 2026-09-06 Status |
|-----------|--------|-----|-------------------|
| RIGHTS/DIVIDEND/OTHER never backward-adjusted | No formula existed in `price_adjuster.py` for these action types | `DATA_INTEGRITY_ISSUES_2026_09.md` |
| Fyers `-EQ` exchange segment hardcoding | Tickers migrated to `-BE`/`-BZ`/`-SM` silently failed to update (16+ tickers) | `DATA_INTEGRITY_ISSUES_2026_09.md` | ✅ **RESOLVED** (Phase 1, 26 tickers) |
| Compound BONUS+SPLIT under-captured | Registry stores only ONE leg's ratio for multi-leg events (STER, SUNILHITEC, RASOYPR, BIRLAPOWER, CGPOWER, JAYBARMARU, GAEL) | `DATA_INTEGRITY_ISSUES_2026_09.md` | ⛔ **NON-BACKTESTABLE** — STER/SUNILHITEC/RASOYPR/BIRLAPOWER excluded 2026-09-06 (unreconcilable ratio, no historical source to verify a derived factor). CGPOWER/JAYBARMARU/GAEL's flagged ex_dates are all pre-2009-04-01 (backtest start) — **moot for backtesting, no exclusion needed**: backward-adjustment only corrupts data *before* the ex_date, which the backtest never reads. |
| Multi-event tickers only partially fixed | Phase 1 fixed only the MOST RECENT event per ticker; earlier events remain (INFY: 3 events, only 1 fixed; ASHAPURMIN: 3 events, only 1 fixed; AURUM: 3 events) | `DATA_INTEGRITY_ISSUES_2026_09.md` | Re-reviewed 2026-09-06: INFY's and ASHAPURMIN's *unfixed* events are all pre-2009-04-01 — **moot for backtesting** (same reasoning as above). INFY's one in-window event (2014-05-29) was already empirically corrected. AURUM's 2020 events are in-window and still need review — not yet actioned. |
| Demerger/Scheme events unmodeled | No formula for spun-off entity value (GLODYNE, KESARENT, IDFC, ABIRLANUVO, SINTEX pending; PEL fixed) | `DATA_INTEGRITY_ISSUES_2026_09.md` | ⛔ **NON-BACKTESTABLE** — all 5 excluded 2026-09-06 (no historical bhavcopy pre-2020 to derive an empirical factor). IDFC flagged as high-liquidity — revisit first if remediation resumes. |
| Unlogged corporate actions | A2ZINFRA's 2012-08-02 spike (11.9% gap, 3.8M volume) has NO corresponding corporate_actions row at all | `R11_BACKTEST_INVESTIGATION_2026_09_06.md` | ⛔ **NON-BACKTESTABLE** — excluded 2026-09-06 (no CA row, nothing to derive a factor from; drove the -₹18.8M R11 loss in the tradebook analysis) |
| RIGHTS-type — no pricing formula | COROENGG, CNOVAPETRO — explicitly deferred per user instruction | `DATA_INTEGRITY_ISSUES_2026_09.md` | ⛔ **NON-BACKTESTABLE** — both excluded 2026-09-06 (formalizes the prior deferral as an explicit universe exclusion instead of a silent unfixed gap) |

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

### B1. **CRITICAL: Position weighting is a no-op for held positions** (Confirmed, bit-for-bit verified) — ✅ **RESOLVED 2026-09-06**

**Fix applied:** `momentum_framework/backtesting/portfolio.py::rebalance_to_target()` — `total_weight` now sums over the full target basket (`target.values()`, held + to_buy) instead of only `to_buy`. Verified with a direct 5-ticker rotation test: a single rotating-in ticker now receives its own computed weight share of total portfolio value rather than the entire freed-up cash at weight 1.0. `momentum_framework/tests` portfolio tests (3/3) still pass. **R09/R14/R16 backtests need re-running** — their prior results (bit-for-bit identical to R01) reflected the bug, not corrected weighting.

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

### B2. **R16's weighting formula is mathematically redundant with R14** (Independent second bug) — ✅ **RETIRED 2026-09-06**

**Resolution:** R16 retired per explicit user instruction. `r16_target_volatility.py` (strategy file + `R16QueueGenerator`) is KEPT for historical reference / parity-checking against past legacy R16 results (same convention as the never-ported R05), but removed from `tests/test_queue_generators.py::ALL_GENERATORS` and `scripts/campaign_registry.py::WEIGHTED_STRATEGIES` — no longer generated by any active campaign. New permanent regression test `test_r16_never_generated` (mirrors `test_r05_never_generated`). `strategies/__init__.py`'s module docstring updated to list R16 alongside R05 under "Permanently excluded."

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

**Status:** ⏳ OPEN — requires a decision, not a mechanical fix: (a) re-run R07 grid with `crash_reduce_sizing` set to a real value (e.g., 0.5) and compare, or (b) accept "buy-disable only" as the tested design and update CLAUDE.md's description to match. Not actioned in the 2026-09-06 pass.

### B4. CLAUDE.md documentation mismatch for R12 (Multi-Signal Ensemble vs. 1-Month Reversal + Liquidity) — ✅ **RESOLVED 2026-09-06**

**Fix applied:** CLAUDE.md's R-family table corrected to describe R12 as "1-Month Reversal + Liquidity" (`trailing_reversal_1mo`), matching the actual code, with a note on the prior stale description.

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

### B6. **CRITICAL: Mean-reversion strategy design vulnerability to structural collapses** (R11/R12/R13)

**Finding:** 102 of 126 R11/R12/R13 runs (81%) show vol > 25% + max DD < -35%. Tradebook analysis reveals worst losses are on **REAL price collapses and systemic crises**, not data artifacts:

- **ADANIENT 2015-06-03**: Real -82.77% single-day move (₹617 → ₹106, 47M share volume) followed by -13.4% next day. R11 bought at the "oversold" low (far from 52-week high), then the crash/split happened. No recovery.
- **ADANIENSOL Jan 2023**: Real -20% to -15% consecutive daily losses (structural deterioration, not gap). R12 bought the "losers" (low 1-month return), strategy bet on reversal, stock kept falling.
- **YESBANK, PNB, INDUSINDBK 2018-2020**: R12 worst losses during banking crisis (2018) and pandemic (2020), when "oversold" signals coincided with actual systemic distress.
- **STEELXIND, TVVISION, MEDICO 2017-2024**: R13 worst losses in small-cap/distressed names with structural issues, not temporary oversold bounces.

**Root cause:** All three strategies implement pure mean-reversion (buy low, sell high) without (a) stop-loss logic, (b) regime detection (don't trade during crises), or (c) quality filters (avoid stocks with structural problems). When a stock is "far from its 52-week high" or has "low 1-month returns" or trades "near its lower Bollinger Band," it could mean (i) a temporary dip (strategy's intended target) OR (ii) the beginning of a structural collapse (strategy's worst enemy). The current design cannot distinguish.

**Status:** ⏳ OPEN — strategic decision required (Option A/B/C, see Recommendation below), not a mechanical fix. Not a code bug per se (the strategy does what it's coded to do), but a fundamental design limitation. R07 has crash-regime detection (Config B3 issue, not executed); R11/R12/R13 have none.

**Severity:** This is the PRIMARY driver of R11/R12/R13's 81% of anomalous runs. Data fixes (including the 2026-09-06 non-backtestable exclusions above) will NOT resolve this.

---

### B7. Minor: narrow exception-swallowing without logging (observability gap, not correctness bug) — ✅ **RESOLVED 2026-09-06**

**Fix applied:** Added `logger.warning(...)` with exception type/message at all 3 sites (r07/r08/r09) before returning the safe default, so a malformed benchmark/equity series degrading these strategies to base-case behavior now leaves a log trace.

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
- **R11/R12/R13 (mean-reversion strategies):** primarily a STRATEGY DESIGN vulnerability, NOT data — **Updated finding 2026-09-06**. Tradebook review shows worst losses are on REAL price collapses (ADANIENT -82.77% single-day 2015-06-03 = corporate action/crash, not data gap; ADANIENSOL -20% consecutive days Jan 2023 = structural decline; YESBANK/INDUSINDBK 2018-2020 = systemic crises). These strategies bet on "oversold" signals (R11=far-from-52wk-high, R12=low-1mo-return, R13=near-lower-Bollinger-Band) that **appear during real crashes or sector stress**, then fail to recover. The strategy design offers NO stop-loss or regime guard. Even A2ZINFRA's -₹18.8M loss (82.77% gap on 2015-06-03) is a real price action, not a discontinuity artifact — the split/crash happened, R11 was long, no recovery occurred. **Data issues are secondary here; the primary issue is mean-reversion's vulnerability to structural collapses.**
- **R09/R14/R16:** primarily a CODE problem (Category B1/B2) — weighting logic doesn't actually execute, so results are just R01 relabeled
- **R07:** a CODE/CONFIG gap (Category B3) — crash mitigation is weaker than documented, not necessarily "wrong," but understates the strategy's designed defensive behavior
- **R12 (second issue):** a DOCUMENTATION problem (Category B4) — CLAUDE.md describes R12 as "Multi-Signal Ensemble", but actual code is "1-Month Reversal + Liquidity"

**These are separable and additive.** Fixing data issues alone would NOT fix R11/R12/R13's anomalies — the root issue is strategy design. Fixing the weighting bug would leave R09 corrected but not R11/R12/R13.

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
7. **R11/R12/R13 regime defense mechanisms** — ✅ Guard built + smoke-tested 2026-09-06 (Option B). Actual re-backtest still pending — see "Resolved This Session" below.

**Follow-up items opened 2026-09-06** (from the #5-item untriaged debate):
8. **3 near-empty "weekend" trading dates found in `ohlcv_adjusted`** (2022-08-13: 1 ticker, 2026-06-21: 3 tickers, 2026-08-01: 8 tickers) — ✅ **DELETED 2026-09-06**. Verified genuine weekend dates (Sat/Sun) with implausibly few tickers (real NSE special sessions show 1,600+; these showed 1, 3, and 8) — 12 rows total removed from `ohlcv_adjusted` via direct DELETE, verified gone. The other 25 weekend dates found are legitimate NSE special sessions (Budget-day Saturdays, Diwali Muhurat) and were left untouched.
9. **Portfolio._sell() phantom-fill on a circuit-locked held position** — ✅ **FIXED 2026-09-06** per explicit user instruction ("skip the sell, keep holding until it unlocks"). `Portfolio.rebalance_to_target()` now excludes circuit-locked tickers from `to_sell` before executing any sells — applies uniformly to organic rotation-out and explicit sell/forced_close signals. Since `to_sell` is recomputed fresh every rebalance, a locked position is naturally reconsidered (and sold once unlocked) the next time, never permanently stuck. Verified live: a synthetic locked position stayed held while a non-locked one sold normally in the same call.

---

## Key Finding Summary

**"Data Explains Everything" Hypothesis: REJECTED**

User's initial hypothesis — that 1,129 anomalous runs (83% of the portfolio) were driven by data discontinuities — is **partially correct but fundamentally incomplete**:

- **Data issues (Category A):** Real, ~50 tickers fixed so far, 1,684 tickers in inventory. SECONDARY in driving overall anomaly severity.
- **Strategy design vulnerability (Category B6):** PRIMARY driver. R11/R12/R13 are pure mean-reversion with no regime defense. Worst losses are on REAL crashes (ADANIENT -82.77%, ADANIENSOL structural -20%+, 2018 banking crisis, 2020 pandemic), not data artifacts. 102/126 R11/R12/R13 runs (81%) anomalous BECAUSE OF STRATEGY DESIGN.
- **Code bugs (Category B1/B2):** R09/R14/R16 collapse to R01 due to portfolio weighting bug.
- **Config/documentation gaps (Category B3/B4/B7):** R07 crash-reduction never executes; R12 mislabeled in docs.

**Fixing order:**
1. **B6 (strategy design)** — requires NEW code: add regime guards to R11/R12/R13 (optional), or accept the design risk
2. **B1 (portfolio weighting)** — fix rebalance_to_target() normalization (code bug, high impact on 3 strategies)
3. **A1 (data fixes)** — phase in 50+ tickers (will improve R11/R12/R13 marginally but NOT resolve their core issue)
4. **B3/B4** — documentation/config corrections (low cost, clarity)

---

## Resolved This Session (2026-09-06)

| Item | Resolution |
|------|-----------|
| **A1 — 13 tickers with unverifiable CA adjustments** | Marked **non-backtestable**, added to `config/backtest_excluded_tickers.json`: STER, RASOYPR, SHARONBIO, SUNILHITEC, BIRLAPOWER (compound/SPLIT, no reconcilable ratio + no historical bhavcopy), GLODYNE, KESARENT, IDFC, ABIRLANUVO, SINTEX (Demerger/Scheme, no historical bhavcopy pre-2020), COROENGG, CNOVAPETRO (RIGHTS, no pricing formula, user-deferred), A2ZINFRA (unlogged CA, drove the -₹18.8M R11 loss). No re-ranking needed — exclusion drops these from the universe entirely, same mechanism as the existing 19-ticker 2026-08-13 exclusion list (32 total now). |
| **A1 — pre-2010 "critical" gaps (CGPOWER, JAYBARMARU, GAEL, most of ASHAPURMIN/INFY)** | Found **moot for backtesting**, not requiring exclusion: their flagged ex_dates are all before 2009-04-01 (backtest start), and backward-adjustment only corrupts data *before* an ex_date — never after — so the erroneous factor never reaches the backtest window. Meaningfully shrinks the real remediation scope below what the "3,886 gaps" headline number implied. |
| **B1 — Portfolio weighting no-op** | Fixed in `portfolio.py::rebalance_to_target()` — normalization denominator now spans the full target basket, not just newly-bought tickers. Verified with a direct rotation test. **R09/R14/R16 need re-running** with the fix. |
| **B4 — CLAUDE.md R12 mismatch** | Corrected. |
| **B7 — silent exception swallowing** | Fixed — `logger.warning` added at all 3 sites (r07/r08/r09). |
| **B3 — R07 crash_reduce_sizing hardcoded to None** | ✅ Fixed — set to `0.5` (trim held positions to top half by momentum score during a detected crash). User decision 2026-09-06: re-run with a real value rather than accept buy-disable-only. |
| **B6 — R11/R12/R13 regime defense, Option B** | ✅ Guard built: `common/crash_regime.py::CrashRegimeGuardMixin`, wired into R11/R12/R13 (buy-disable only, disabled by default via `crash_regime_enabled=False`) and refactored into R07 too (replacing its former per-band-benchmark duplicate). Smoke-tested end-to-end against real 2020 COVID and 2018 banking-crisis dates. **Re-backtest still pending** — this is a smoke test per user instruction ("just do a smoke test, we will again do a backtest"), not yet run through a full campaign. |
| **Crash detector redesigned: 1 shared benchmark + optimized threshold** | ✅ Replaced per-band benchmark resolution (`common/benchmark.py`) with ONE shared `MARKET_WIDE_BENCHMARK_INDEX = "Nifty 500"` for crash detection specifically (band-scoped ranking untouched). Threshold changed `-15% → -12.5%` after validating against 8 real Indian crises (2008 GFC through 2022) on both Nifty 50 and Nifty 500 — Nifty 500 chosen since this framework's bands are overwhelmingly mid/small/micro-cap, where Nifty 50 (large-cap) understated real stress (e.g. 2013 rupee crisis: -26.4% on Nifty 500 vs -16.3% on Nifty 50). At -12.5%, all 8 known crises are now flagged (previously the -15%/per-band setup missed the 2018 banking crisis on band 2's Nifty 50 entirely — 0/50 days). User decision 2026-09-06: recall weighted over precision. |

**New: Category B8 — circuit-lock exclusion was opt-in/R07-only, now framework-level.** `common/liquidity.py::get_circuit_locked_tickers()` existed but was wired into exactly one place (R07's `new_entrants` filter). Every other strategy (R01/R03/R09/R11/R12/R13/R14-R17) could select a circuit-locked ticker as a fresh buy signal, which `Portfolio` would then execute as a phantom fill at a price never actually tradeable that day. ✅ **Fixed 2026-09-06** in `backtesting/orchestrator.py::run_native()` — the universe is filtered once, immediately after `resolve_universe()`, before any strategy sees it, so every strategy benefits uniformly with no per-strategy code. ADTV floor deliberately left as a strategy-level opt-in (unchanged) since R12's liquidity-quintile design needs the full liquidity spectrum, including illiquid names, as a first-class research variable. **Still open:** a currently-held ticker that becomes circuit-locked on a rebalance date still gets a phantom sell fill (`Portfolio._sell()` doesn't check for a lock) — see untriaged item 9 above.

**Data-issue discrepancy flagged:** while re-verifying the 12 "pending" tickers against the live `corporate_actions` table, several ex_dates/ratios in `DATA_INTEGRITY_ISSUES_2026_09.md`'s Category A/B tables didn't reconcile with what's actually stored (e.g. STER's compound event was catalogued as 2016-02-10 ratio=0.40→0.25; the DB's actual SPLIT event for STER is 2010-06-21 ratio=2.0). The exclusion reasons in `backtest_excluded_tickers.json` use the re-verified live-DB dates/ratios, which should be treated as authoritative over that doc's tables going forward.

---

## Category E: Universe/Grid Design Decisions (2026-09-06)

### E1. M13's top_n set restricted

**Finding:** M13 (full 800-stock ADTV universe) was tested at top_n = [10, 20, 30, 40]. User decision 2026-09-06: restrict to exactly **[10, 15, 20]** — `common/universe.py::TOP_N_BY_BAND[13]`. Propagates automatically to every strategy's queue generator via `QueueGenerator.band_top_n_pairs()`; verified R01 and R13 generators both now emit exactly `{10, 15, 20}` for M13.

### E2. Partitioned bands' top_n=15 dropped

**Finding:** M02/M04/M07/M09/M10/M12 were tested at top_n = [5, 10, 15]. User decision 2026-09-06: drop 15, keep **[5, 10]** only — same `TOP_N_BY_BAND` table. Verified propagation for all 6 bands.

### E3. Band-definition liquidity concern investigated — bands left as-is

**Finding:** `momentum_band_universe()` takes a FIXED count (top 800 by trailing ADTV), not a fixed liquidity value — and the market's absolute ADTV grew ~100-150x from 2012 to 2026. Measured actual ADTV at rank 800: **₹0.10 Cr/day in 2012** (right at `config/settings.py::MIN_ADT_INR`'s own ₹10L floor — only 806 tickers total cleared that floor market-wide that year) vs **₹16.22 Cr/day in 2026**. M9/M10/M12/M13 backtest results from ~2009-2020 are plausibly trading names that were not realistically executable at the position sizes implied; the same rank today is comfortably liquid. **User decision 2026-09-06: leave band definitions as-is** — no code change. Documented here so the caveat is visible when interpreting early-era M9/M10/M12/M13 results, and revisitable later if desired.

### E4. Extraordinary-return tickers — sensitivity-analysis toggle built

**Finding:** Cross-referencing `framework_backtest_trades` (1,986,459 trades across the full campaign) shows extreme P&L concentration: **top 10 tickers = 48% of total net P&L, top 30 = ~90%**, out of 1,749 distinct tickers ever traded. Top contributors: CUPID (8.9%), TITAGARH (7.6%), BCG (6.2%), MTARTECH (5.2%), ... down through TANLA (3.0%, the user's original example) and BORORENEW (1.4% at rank 30). Full ranked list: `config/extraordinary_return_tickers.py`.

**Built (not a data-quality exclusion — a deliberate, opt-in sensitivity toggle, kept separate from `config/backtest_exclusions.py`):**
- `config/extraordinary_return_tickers.py` — ranked list + `extraordinary_return_tickers(top_n)`, configurable cutoff (user decision: default **top_n=15**, was going to be a fixed top-30 list before the user asked for it to be configurable).
- `metrics/nomenclature.py::build_strategy_id()` — new `exclude_extraordinary_returns`/`extraordinary_returns_top_n` fields, appended as `exOutliersN` suffix only when the toggle is on (baseline strategy_ids are byte-for-byte unchanged).
- `backtesting/orchestrator.py::run_native()` — applies the filter at the same universe-construction point as the circuit-lock filter (B8); both `identity_fields` sets and `queues/validator.py`'s duplicate-id check updated to include the two new fields (the exact fragile spot the prior orchestrator strategy_id bug lived in — audited both call sites this time).
- `queues/generator.py::QueueGenerator.with_and_without_extraordinary_returns(jobs, top_n=15)` — doubles a built job list into baseline + excluded-outliers variants, opt-in per campaign, not a default.

**Verified end-to-end:** raw band-12 universe contains CUPID; after applying the exact orchestrator filter code path with `top_n=5`, CUPID is correctly removed. strategy_id correctly differs between baseline/excl-15/excl-30 runs (no collision risk). Full test suite re-run after these changes.

**Not yet done:** actually running the with/without campaign comparison — this only builds the capability.

### E5. Transaction costs + pre-tax/post-tax returns — IMPLEMENTED

**Built (user instruction 2026-09-06: "Implement Cost/Tax Framework"):**
- **Costs (real cash outflow, changes CAGR/Sharpe/MaxDD directly):** `Portfolio._sell()` now deducts the full round-trip cost via `backtest/costs.py::IndianTransactionCosts` (unchanged, same rate table as the legacy engine) — computed off the ENTRY price/quantity/ADTV, exactly mirroring `backtest/core/portfolio.py::_close()`'s already-validated pattern. ADTV is looked up in one batched query per rebalance (`rebalance_to_target()` gained an optional `conn` param, threaded from `orchestrator.py`) and stored on `Position.entry_adtv_cr` for the small-cap slippage-rate lookup at exit.
- **Tax (post-processing overlay, NOT deducted from cash):** new `momentum_framework/common/tax_overlay.py` builds `backtest/core/tax.py::Transaction` records from `Portfolio.trade_log`'s realized sells (buy/sell date+price now recorded on every sell entry) and runs them through the canonical FY-netted LTCG/STCG engine (12.5%/20%, holding >=365 days = LTCG, asymmetric loss set-off) — unchanged from the legacy engine. Reported as `post_tax_total_tax_inr`, `post_tax_ending_value`, `post_tax_cagr` in `BacktestResult.metrics`, additive alongside the existing pre-tax fields (never replacing them).

**Verified live** (R01, band 2, top10, 2018-2023): pre-tax CAGR 19.77% -> post-tax CAGR 17.02%, ₹269,772 total tax paid; 191 sell trades incurred ₹92,222 total transaction cost (~0.40-0.48% round-trip per trade, matching `costs.py`'s own documented SPEC-BT-002 target of 0.40-0.50%). Full `momentum_framework` test suite re-run clean after integration (portfolio/orchestrator/metrics/nomenclature/validator subset + full suite).

**Every existing/future backtest result now differs from pre-2026-09-06 numbers** — costs and (reported) tax were not modeled at all before this. Re-running prior campaigns is needed to get cost/tax-aware numbers; nothing before this date reflects them.

### E6. Phased backtest queue built (via enhanced-backtesting-agent) + stale-results exclusion

**Built (user instruction 2026-09-06: restructure the next campaign as passes by top_n, and keep old results out of reports without deleting them):**
- `momentum_framework/queues/active_generators.py` — single source of truth for the 12 currently-active `QueueGenerator`s (R16 excluded); `tests/test_queue_generators.py` now imports from here instead of maintaining a duplicate list.
- `momentum_framework/queues/pass_builder.py` — buckets every active generator's own `.generate()` output by each job's top_n RANK within its band's `TOP_N_BY_BAND` entry (not by a shared literal top_n value, since M13's values differ from the partitioned bands').
- Generated (review-only, nothing executed): `momentum_framework/results/queues/campaign_2026_09_06_pass{1,2,3}.json` — Pass 1 (1,116 jobs, top_n 5/10), Pass 2 (1,116 jobs, top_n 10/15), Pass 3 (168 jobs, M13-only, top_n 20). Verified directly (not just trusted from the agent's report): job counts, top_n/band/strategy-family sets per pass, and R16's absence all confirmed by inspection.
- R11/R12/R13's crash-regime guard (B6 Option B) turned ON by default in their `QueueGenerator.build_jobs()` — this queue reflects the current intended strategy behavior, not an opt-in variant. R07's `crash_reduce_sizing=0.5` (B3) was already in place.
- **Stale-results exclusion:** `momentum_framework/results/stale_results.py::STALE_RESULTS_CUTOFF_AT` (2026-09-06 00:00 UTC) + `datastore/api/routers/framework_backtest_runs.py`'s list endpoint gained `include_stale: bool = False`, filtering `run_executed_at >= cutoff` by default. Chosen over a schema migration or `source_commit` allowlist since it needs no migration and the fix commits hadn't landed yet when this was built. Verified directly: all 1,354 existing rows in `framework_backtest_runs` are dated 2026-09-04/05 — zero rows on/after the cutoff — so it excludes 100% of stale runs with no risk of misclassifying a new one. Old rows are never deleted, only hidden from the default view (`include_stale=true` still returns everything). Frontend (`frontend/src/shared/api/framework_backtest.ts`) needs no change — confirmed it never passes `include_stale`, so it gets the filtered view automatically.

### E7. Pass 1 execution — performance investigation and two real fixes

**Started, paused, diagnosed, fixed, restarted.** Launched Pass 1 (1,116 jobs) with `scripts/run_pass_queue.py` (new: dispatches a queue-file job dict to the right `StrategyAdapter` via `scripts/job_dispatch.py`, executing via the same thread-pool-compute/sequential-write pattern as `run_campaign.py`'s Pass 2). Initial pace: ~97-102s/job average, projecting ~30 hours for Pass 1 alone — much slower than the user's remembered ~18-40s/job from a prior campaign.

**Fix 1 — new per-rebalance queries cached.** This session's B8 (circuit-lock) and E5 (ADTV cost basis) fixes added 3 new live DB query call sites per rebalance (`get_circuit_locked_tickers` x2, `compute_adtv_cr` x1) that didn't exist in whatever produced the ~18-40s/job baseline. Built `circuit_lock_snapshots` (171,643 rows) and `adtv_snapshots` (8.1M rows) into the SAME existing `momentum_framework/cache/universe_cache.duckdb` file the momentum-rank cache already uses (user instruction: consolidate into the existing cache, don't create a new one) — both built in under 2 seconds via DuckDB window functions, verified to match the live functions to floating-point precision. New `common/liquidity.py::get_circuit_locked_tickers_auto()`/`compute_adtv_cr_auto()` prefer the cache, falling back to live on any error (same graceful-degradation convention as the rank cache). New `scripts/build_liquidity_cache.py` formalizes the one-time build.

**Fix 2 — the REAL dominant cost, found via profiling.** cProfile on the exact worst-case job showed `resolve_universe()`'s live fallback path (`momentum_band_universe()` -> `liquid_universe()` -> `market_cap_snapshot()`) at 110 of 169 seconds (65%) — a PRE-EXISTING cache (`band_universe_snapshots`, built before this session) was missing 20.3% of the exact dates a 5-day-cadence job needs, uniformly across all 7 bands. Root-caused precisely: every single missing date fell on/after 2022-08-17 — one day after the first of the 3 bogus weekend `ohlcv_adjusted` rows this session deleted (item 8/E-series cleanup, 2026-09-06). Deleting those rows shifted the trading-day calendar's index positions for every date afterward, so `calendar[::cadence]` (used identically by both the cache-build script and the runtime orchestrator) started selecting DIFFERENT dates than what the stale cache held. Fix: re-ran `scripts/build_universe_cache.py` (37 min, 1,788,090 rows) against the corrected calendar — verified 100% cache hit rate afterward (was 79%).

**Combined result, verified on the identical worst-case job:** 169.4s -> 49.9s (3.4x), same trade count (2,827), same-magnitude CAGR. Not yet back to the remembered 18-40s/job floor, but a large, real, measured improvement with both root causes identified and fixed (not guessed at).

**Lesson for future data cleanups:** deleting rows from `ohlcv_adjusted` (even genuinely bad ones) invalidates any DOWNSTREAM cache keyed by calendar-index position (`calendar[::N]`) for every date after the deletion — `band_universe_snapshots`/`momentum_rank_snapshots` should be rebuilt after any such cleanup, not just left to accumulate silent misses.

**Not yet done at time of writing:** the campaign restarted and IS now running to completion (764+/1116 as of this update, 0 errors), pace continuing to improve as competing diagnostic activity stopped.

### E8. R11 (52-week-high reversal) signal bug — universe starved to 1-2 alphabetically-first tickers — ✅ FIXED 2026-09-07

**Found via:** a live leaderboard check during Pass 1 surfaced `M02_R11_top5_lb12mo_5d_allrisk_crashaware` at **CAGR=17.00%, MaxDD=-95.2%**, an implausible combination flagged for investigation rather than accepted at face value. Dispatched a `backtest-reviewer` agent (read-only, ran alongside the live campaign without interference) to determine whether this was a genuine strategy vulnerability, a data artifact, or a bug.

**Root cause, independently verified:** `common/signals.py::PctOf52WeekHighSignal.compute()` — the only strategy using this signal (R11 exclusively; R05, which shares the rank_method, was never ported) — had:
```sql
... ORDER BY ticker, date DESC LIMIT {len(tickers)}
```
`LIMIT` here applied to the RAW ROW COUNT across the whole result set, not per ticker. Since each ticker needs up to `lookback_days+1` (253) rows for its window to compute correctly, and rows were ordered `ticker ASC, date DESC`, the global limit (e.g. 75 for a 75-ticker band) was exhausted by the first 1-2 alphabetically-sorted tickers' full histories — every other ticker in the universe got ZERO rows and no score. `top_n=5` therefore silently selected from a pool of 1-2 valid candidates, not 5, for the strategy's ENTIRE campaign history — R11 was running as a near-single-stock rotation, not the intended diversified reversal basket.

**Why it looked like a crash-guard failure but wasn't:** the -95.2% MaxDD was driven by ADANIENT's real, documented -83.2% collapse on 2015-06-03 (matches the already-known B6 research finding) consuming ~100% of NAV instead of the ~20% a genuine 5-way equal-weight basket would have absorbed. The crash guard (`_in_crash_regime()`) correctly did not fire — this was an idiosyncratic single-stock event, not a broad Nifty 500 -12.5% drawdown — so the guard's own logic was never the problem; concentration caused by the signal bug was.

**Fix:** replaced the broken global `LIMIT` with the same bounded-per-partition pattern `TrailingMomentumSignal.compute()` already uses (`ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) <= lookback_days+1` in a subquery, feeding the window-MAX computation, then `QUALIFY ROW_NUMBER() ... = 1` to keep exactly the latest row per ticker). Verified: all 75 band-2 tickers now receive a score (was 1-2); a spot-checked value (IOB, 2015-04-28) matches a manual pandas computation exactly (0.479483). Re-ran the same M02/R11/top5/lb12mo/cad5 config end-to-end: 121 distinct tickers bought (was ~10-15 alphabet-biased names), MaxDD -78.75% (down from -95.2%, still severe — consistent with R11's known, real mean-reversion vulnerability, not a bug artifact), CAGR turned negative (-2.78%, more consistent with the B6 research than the buggy version's oddly-positive 17%).

**Blast radius:** R11 only (confirmed via grep — no other strategy imports `PctOf52WeekHighSignal`). All 21 of Pass 1's R11 jobs had ALREADY completed (with the bug) by the time the fix landed — the live campaign had moved on to other strategies, so the fix required no interruption.

**R11 corrective re-run:** launched 2026-09-07 as a SEPARATE process (`results/queues/r11_rerun_2026_09_07.json`, the exact same 21 job configs Pass 1 already ran, extracted directly from `campaign_2026_09_06_pass1.json` rather than regenerated, to guarantee an exact match) — run alongside the still-active main Pass 1 process without needing its lock (bypassed `run_pass_queue.py::main()`'s single-instance lock by calling `_run()` directly, since this is a genuinely different, non-conflicting job set writing to its own progress/log files).

### E9. `excludes_extraordinary_returns` / `extraordinary_returns_top_n` columns added to `framework_backtest_runs` — ✅ DONE 2026-09-07

**Why:** user instruction — once Pass 1 (and the R11 re-run) complete, the extraordinary-returns exclusion test (E4) needs to run against the same grid, and results must be cleanly distinguishable from the baseline without parsing `config_json` or string-matching `strategy_id`'s `exOutliersN` suffix.

**Done:**
- `results/db_schema.py::SCHEMA_SQL` — two new columns (`excludes_extraordinary_returns BOOLEAN`, `extraordinary_returns_top_n INTEGER`) added to the `CREATE TABLE IF NOT EXISTS` (for fresh DBs) AND as separate `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statements (for the existing 15GB+ live table — DuckDB does not support a `NOT NULL` constraint on `ALTER TABLE ADD COLUMN`, only on `CREATE TABLE`, so the migration statement omits it; every writer call still supplies an explicit value).
- `results/db_writer.py::write()` — INSERT now populates both columns from `result.config`.
- Migration applied live against the running 15GB database (retried through the live campaign's intermittent write locks) — verified: all 2,266 pre-existing rows correctly backfilled to `FALSE`/`NULL`; a live end-to-end test with `exclude_extraordinary_returns=True, extraordinary_returns_top_n=15` correctly persisted `(True, 15)`. Diagnostic test rows cleaned up after verification.

**Bug found and fixed while verifying this:** `scripts/job_dispatch.py::strategy_from_job()`'s signature-based kwarg filtering (added 2026-09-06 to stop `skip_months`/`crash_regime_enabled`/`select_lowest`/`weight_method`/`lookback_months` collisions — see E-series job-dispatch work) was ALSO silently dropping `exclude_extraordinary_returns`/`extraordinary_returns_top_n`, since those flow through `**kwargs` generically and are never named in any strategy's own `__init__` signature — invisible to `inspect.signature`. Fixed with an explicit `_ALWAYS_FORWARD_KEYS` allowlist for genuinely strategy-agnostic passthroughs (currently just these two), forwarded regardless of signature inspection. Verified across all 12 active strategies via a real queue file before use, not discovered mid-run.

**Exclusion-test queue prepared** (not yet run): `results/queues/campaign_2026_09_07_exoutliers.json` — 1,116 jobs, same grid as Pass 1 but with R11 sourced from the FIXED re-run (not the stale buggy config) and `exclude_extraordinary_returns=True, extraordinary_returns_top_n=15` applied to every job. Deliberately held until Pass 1 + the R11 re-run both finish, per the same "don't compete with the live campaign" discipline established earlier this session.

---

## Recommendation

Still open, in priority order:
1. **Re-run the full campaign** — every prior number is stale against at least one of: B1 (portfolio weighting), the crash-regime guard + new detector (B6/R07), costs+tax (E5), R16's retirement, and the top_n grid changes (E1/E2). This is the single highest-priority remaining action; queue-building for it is in progress (see below).
2. **Run the with/without extraordinary-returns comparison (E4)** — capability is built, comparison itself hasn't been run yet.
3. **Data:** decide whether to continue Phase 1 remediation (top-100 tickers, real bhavcopy-verifiable events only) given the now-smaller real scope, or treat the current 32-ticker exclusion list as sufficient for now.

**Closed 2026-09-06:** B1 (portfolio weighting), B2 (R16 retired), B3 (R07 trim value), B4 (CLAUDE.md R12), B6/Option B (guard built + smoke-tested, backtest pending), B7 (logging), B8/circuit-lock (framework-level fix, both buy-side and sell-side), crash-detector redesign (1 shared Nifty 500 benchmark, -12.5% threshold), item #4 (Bollinger signal verified against production TA-Lib to 1e-11), E1-E2 (M13/partitioned-band top_n grids), E3 (band-definition liquidity investigated, left as-is), E4 (extraordinary-returns sensitivity toggle built), E5 (transaction costs + pre-tax/post-tax returns implemented), items 8-9 (bogus weekend rows deleted, locked-held-position sell fixed).

