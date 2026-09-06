# Strategy Code Bugs — Root Cause Analysis (2026-09-06)

**Trigger:** User flagged that abnormal backtest returns/volatility could be caused by strategy code issues, not only price-discontinuity data issues. This investigation confirms **a real, verified implementation bug** in the position-sizing pipeline, independent of the data-quality issues documented in `DATA_INTEGRITY_ISSUES_2026_09.md`.

---

## Executive Summary

**CONFIRMED BUG:** `momentum_framework/backtesting/portfolio.py::Portfolio.rebalance_to_target()` normalizes per-ticker weights (`size_multiplier`) **only across newly-bought tickers** (`to_buy`), never across the full target basket, and **never re-weights positions that remain held** across rebalances. This silently degrades weighted strategies (R09, R14, R15, R16, R17, and R08's exposure path) toward plain equal-weight (R01) behavior — with the degree of visible distortion depending on how much the strategy's computed weights actually differ from equal weight and how much turnover occurs per rebalance.

**Verified, bit-for-bit proof:**
```
M02_R01_top5_lb6mo_21d_allrisk:                             cagr=0.08423033991949969, trades=1005
M02_R14_top5_lb6mo_21d_allrisk_weight-inverse_volatility:   cagr=0.08423033991949969, trades=1005
```
Identical to 16 significant digits. R14's own `rebalance()` computes genuinely different weights per ticker (verified directly: CHOLAFIN=0.1971, ABB=0.2135, TMPV=0.2140, HAL=0.1793, IDFCFIRSTB=0.1961 vs. equal=0.20 each) — **but these weights have zero effect on the actual backtest result.**

---

## The Bug, Precisely

**File:** `momentum_framework/backtesting/portfolio.py`  
**Function:** `Portfolio.rebalance_to_target()`

```python
target: Dict[str, float] = {
    s.ticker: (s.size_multiplier or 1.0) for s in signals if s.action == "buy"
}
exposure_multipliers = [s.exposure_multiplier for s in signals if s.action == "buy"]
exposure = exposure_multipliers[0] if exposure_multipliers else 1.0
...
held = set(self.positions.keys())
to_sell = (held - target.keys()) | (explicit_sells & held)
to_buy = target.keys() - held          # <-- only NEW tickers this rebalance
...
total_weight = sum(target[t] for t in to_buy)     # <-- normalizes ONLY within to_buy
if total_weight <= 0:
    return
requested = {t: total_value * (target[t] / total_weight) * exposure for t in to_buy}
```

**Two compounding defects:**

1. **Held positions are never re-weighted.** A ticker that stays in the top_n basket across consecutive rebalances keeps whatever dollar allocation it received when it was FIRST purchased — the strategy's updated weighting view (e.g., today's inverse-volatility weight) is silently discarded for every already-held name, every rebalance, for the life of that position.

2. **New purchases are normalized only against each other, not the full target basket.** `total_weight` sums `target[t]` only for tickers in `to_buy`. In the very common case where exactly ONE ticker rotates in (one name drops out of top_5, one enters), `to_buy` has exactly one element, so:
   ```
   target[t] / total_weight = target[t] / target[t] = 1.0   (ALWAYS, regardless of the actual computed weight)
   ```
   The strategy's carefully computed weight is thrown away and replaced with "deploy all available incremental capital into this one name" — functionally identical to what equal-weighting would do in that scenario.

**Net effect:** any strategy whose ONLY distinguishing feature is per-ticker or portfolio-level weighting (not the ranking/selection signal itself) degrades toward its R01-equivalent baseline. The degradation is total when computed weights happen to be close to equal (R14 on this universe: 0.18–0.21, close to 0.20) or when turnover keeps `to_buy` small; it is partial — but still present and understated — when weights are more dispersed (R15's squared inverse-variance penalty, R17's downside-only volatility) or exposure swings are larger (R08's fixed vol-target vs R09's regime-gated exposure).

---

## Verified Impact Matrix

Pairwise identical-metrics check across the shared momentum-signal family (same ranking, only weighting/exposure differs), on 691 deduplicated (latest-run) framework backtests:

```
           R01     R07     R08     R09     R14     R15     R16     R17
R01         --      0%      1%     99%    100%      0%    100%      0%
R07        0%       --      0%      0%      0%      0%      0%      0%
R08        1%      0%       --      0%      0%      0%      0%      0%
R09       99%      0%      0%       --    100%      0%    100%      0%
R14      100%      0%      0%    100%       --      0%    100%      0%
R15        0%      0%      0%      0%      0%       --      0%      0%
R16      100%      0%      0%    100%    100%      0%       --      0%
R17        0%      0%      0%      0%      0%      0%      0%       --
```

**Confirmed collapsed (>=99% identical to R01 — weighting/exposure is a de facto no-op):**
- **R14** (Inverse-Volatility Weighting): 100% identical to R01 (72/72 configs)
- **R16** (Target-Volatility Weighting): 100% identical to R01 AND to R14 (see separate math bug below)
- **R09** (Moreira-Muir 4-Mode Vol-Scaling, project's flagship risk-managed momentum): 99% identical to R01 (71/72 configs)

**NOT collapsed (weighting/exposure produces a measurably different result — but per the bug above, still understated vs. correctly-implemented weighting):**
- R08 (Barroso-Santa-Clara vol-target exposure): differs from R01 in nearly all configs (e.g., CAGR 2.6% vs 4.1% on M02_top5_lb3mo_10d)
- R15 (Inverse-Variance Weighting — squared penalty, larger weight dispersion than R14): differs from R01/R14
- R17 (Downside-Volatility Weighting): differs from R01/R14
- R07 (Crash-Aware): differs — separate crash-detection overlay likely dominates over the sizing bug

**Interpretation:** the bug is universal (it affects every strategy that relies on `size_multiplier`/`exposure_multiplier` for its distinguishing behavior), but its VISIBILITY depends on how extreme the intended weight/exposure deviation from 1.0/equal is, and how much rebalance-to-rebalance turnover occurs. R14/R16/R09 happen to sit in the "weights close to equal, mostly held-not-rebought" regime where the bug is total; R08/R15/R17 sit in a "weights/exposure far from equal, or turnover higher" regime where the bug is partial but the underlying degradation is still present and should be corrected before trusting the reported magnitude of outperformance.

---

## Second, Independent Bug: R16's Weighting Formula is Mathematically Redundant

**File:** `momentum_framework/common/position_weighting.py::TargetVolatilityWeighting.compute_weights()`

```python
def compute_weights(self, tickers, as_of_date, conn):
    returns = daily_returns(conn, tickers, as_of_date, self.lookback_days)
    vol = realized_volatility(returns)
    vol = vol[vol > 0]
    raw = (self.target_vol / vol).clip(upper=self.leverage_cap)
    return self._normalize(raw)
```

`_normalize()` divides every value by their sum. Since `target_vol` is a SCALAR constant applied uniformly to every ticker:
```
normalize(target_vol / vol_i) = (target_vol/vol_i) / Σ(target_vol/vol_j) = (1/vol_i) / Σ(1/vol_j) = normalize(1/vol_i)
```
`target_vol` cancels out algebraically — **R16 (Target-Volatility Weighting) is mathematically identical to R14 (Inverse-Volatility Weighting) after normalization**, UNLESS the `leverage_cap` clip actually engages (truncates at least one ticker's raw weight before normalization). In the 7 R16 configs tested, the clip never engaged, so R16 never once differed from R14 — this is a design/implementation bug independent of the portfolio.py issue above: even with a correctly-fixed `rebalance_to_target()`, R16 would STILL be identical to R14 because the formula itself doesn't encode a distinct "target volatility" concept — it only reduces to relative risk-parity weighting, same as R14, unless leverage capping is actually binding.

**This means R16 (as currently implemented) is not a genuinely distinct strategy from R14** — it needs either a formula that doesn't algebraically cancel (e.g., don't fully re-normalize to sum-to-1; instead size each position independently toward its own target-vol contribution and let total book exposure float, which is what "target volatility" conventionally means), or an explicit uncapped total-exposure output rather than a normalized weight vector.

---

## Known, Already-Documented Precedent (R08/R09)

The codebase's own `Signal` dataclass docstring (`momentum_framework/backtesting/adapter.py`) already documents a related, PARTIALLY-fixed version of this issue:

> "size_multiplier vs exposure_multiplier (split 2026-09-05 after a confirmed bug: R08/R09 set size_multiplier to the SAME value on every buy signal, meaning 'scale total book exposure by this factor' — but Portfolio.rebalance_to_target() NORMALIZES size_multiplier across the buy set (target[t]/total_weight), which cancels out any UNIFORM value by construction... R08's Barroso-Santa-Clara vol-target and R09's vol-scaling modes were therefore silent no-ops for as long as this class has existed."

This confirms: (a) the team already found and partially fixed ONE manifestation of this bug class (separating `size_multiplier` from `exposure_multiplier` so a uniform portfolio-level scalar wouldn't self-cancel), but (b) the fix did not address the SECOND, still-open manifestation documented in this report — the `to_buy`-only normalization and the missing re-weighting of held positions — which is why R09 STILL shows 99% collapse to R01 today, and why R14/R16 (which never had the uniform-value problem, since their weights are genuinely per-ticker-different) collapse for the separate reason above.

---

## Recommended Fix

**In `Portfolio.rebalance_to_target()`:**
1. Compute `target` weights and `total_weight` across the **entire desired basket** (held positions that remain PLUS new buys), not just `to_buy`.
2. At every rebalance, **resize every held position** to its current target weight (sell/buy the delta), not just leave it untouched. This is standard portfolio-rebalancing semantics — the entire point of a weighted strategy is that weights get refreshed periodically, not frozen at entry.
3. This will introduce additional turnover/transaction costs versus the current (buggy) behavior — expect metrics to change materially for R09, R14, R15, R16, R17, and possibly R08.

**In `TargetVolatilityWeighting.compute_weights()` (R16):**
- Either remove the redundant `target_vol` scalar (acknowledge R16 ≡ R14 and retire one of them), or reimplement R16 to NOT fully re-normalize to sum-to-1 — e.g., size each position independently at `target_vol/vol_i` (capped), and let uninvested capital sit in cash rather than force full deployment. This is the conventional meaning of "target volatility" sizing and would make R16 genuinely distinct from R14.

**Validation after fix:**
- Re-run the pairwise identical-metrics matrix above; expect ALL off-diagonal values to drop toward 0% (each strategy should now diverge from R01 in proportion to its actual weighting/exposure logic).
- Re-validate R09 specifically since project docs identify it as the primary risk-managed momentum candidate — its current backtest history should be considered **not representative of the intended strategy** until this fix lands.

---

## Relationship to Data-Quality Findings

This strategy-code bug is **independent of, and additive to**, the price-discontinuity issues documented in `DATA_INTEGRITY_ISSUES_2026_09.md` and `BACKTEST_PERFORMANCE_ANOMALIES_ROOT_CAUSE_ANALYSIS.md`:

- **Price discontinuities** explain WHY certain trades (e.g., A2ZINFRA in R11) produce catastrophic, economically-nonsensical losses — a DATA problem.
- **This weighting bug** explains WHY certain strategies (R09, R14, R16) that are SUPPOSED to differ materially from R01 in fact do not — a CODE problem, present even on perfectly clean data.

Both must be fixed before any cross-strategy ranking or paper-trading decision is trustworthy. Fixing only the data would leave R09 (the project's flagship vol-scaled momentum candidate) still reporting R01's numbers under a different label; fixing only the code would leave R11/R13's catastrophic losses from A2ZINFRA-style artifacts unaddressed.

---

## Next Steps

1. **Do not use current R09, R14, R16 backtest results** to conclude anything about Moreira-Muir vol-scaling, inverse-volatility weighting, or target-volatility weighting — they are currently proxies for plain R01 momentum.
2. **Fix `rebalance_to_target()`** to re-weight the full basket (held + new) at every rebalance.
3. **Fix or retire `TargetVolatilityWeighting`** (R16) — currently indistinguishable from R14 by construction.
4. **Re-run the full R08/R09/R14-R17 campaign** after the fix, in parallel with the price-discontinuity remediation (Phase 1/2 in `PRICE_CONTINUITY_REMEDIATION_STRATEGY_2026_09.md`) — both fixes should land before the next round of strategy comparison.
5. **Audit R07, R08, R15, R17** (the "not collapsed" group) for a PARTIAL version of the same bug — they show real differences from R01, but per the mechanism above, those differences are likely UNDERSTATED versus a correctly-implemented weighting/exposure pipeline. Re-run and compare pre/post-fix magnitudes, don't assume current numbers are simply "correct because they differ."

