"""
Strategy Implementations — ALL 13 active R-family strategies ported
(2026-09-04). One file per strategy, all numbers zero-padded (R01, not
R1 — see project_r_number_zero_padding memory):

  r01_trailing_momentum.py    — R01, the ORIGINAL strategy (trailing_return, no skip)
  r03_jt_skipmonth.py         — R03, Jegadeesh-Titman skip-month variant
  r07_crash_aware.py          — R07, crash-regime buy-disable + trim overlay
  r08_bsc_volscale.py         — R08, Barroso-Santa-Clara portfolio vol-target
  r09_mm_volscale.py          — R09, Moreira-Muir 4-mode portfolio vol-scaling
                                 (regime_switching_enabled NOT ported — raises
                                 NotImplementedError if requested, see that file)
  r10_sector_momentum.py      — R10, Nigam-Pandey two-stage sector momentum
  r11_52wk_reversal.py        — R11, 52-week-high reversal (select_lowest=True)
  r12_reversal_1mo.py         — R12, 1-month trailing-return reversal
  r13_bollinger_reversal.py   — R13, Bollinger Band %B mean-reversion
  r14_inverse_volatility.py   — R14, weight ∝ 1/vol
  r15_inverse_variance.py     — R15, weight ∝ 1/vol² (Barroso-Santa-Clara)
  r16_target_volatility.py    — R16, weight ∝ target_vol/vol, capped
  r17_downside_volatility.py  — R17, weight ∝ 1/downside_vol (Sortino-style)
                                 R14-R17 replace the retired R0 (see
                                 project_r0_split_r14_r17 memory)

Permanently excluded (not "not yet ported" — a decided exclusion):
  R05 — 52-Week-High Momentum (non-inverted). Rejected at the Phase 3
        gate. Historical reference only in results/traceability/. Never
        port without the user explicitly raising it.

Shared ranking signals (see common/signals.py's module docstrings for
which strategies use which):
  - TrailingMomentumSignal: R01, R03, R07, R08, R09, R10 (+ sector filter),
    R12 (lookback=1mo, lowest-wins), R14, R15, R16, R17 — the SAME class,
    never reimplemented per strategy.
  - PctOf52WeekHighSignal: R11 (select_lowest=True)
  - BollingerBandSignal (common/bollinger_signal.py): R13
  - IndustryMomentumSignal (common/signals.py): R10 (wraps
    TrailingMomentumSignal + common/sector_ranking.py's two-stage filter)

Every strategy file exports:
  - A StrategyAdapter subclass (the execution logic)
  - A QueueGenerator subclass (the parameter grid for backtesting)
  - STRATEGY_CODE, RANK_METHOD module constants used by both

StrategyBase is a convenience combining shared boilerplate;
WeightedMomentumStrategy (used by R14-R17) additionally composes a
TrailingMomentumSignal with a swappable weighting_scheme_class. Neither
is required — strategies only need to satisfy the StrategyAdapter /
QueueGenerator interfaces.

Verified 2026-09-04: all 13 strategies' QueueGenerators together produce
3,630 jobs with zero strategy_id collisions (cross-checked via
metrics.nomenclature.build_strategy_id across the full set, not just
within each strategy). R10-R13's ranking logic additionally verified
against synthetic data for correct selection direction; R08/R09's
exposure-multiplier math verified against a synthetic equity curve;
R07's crash-trim/buy-disable logic verified against a synthetic crash
window. See docs/CODE_TRACEABILITY.md for exactly what remains
unverified (mainly: trade-by-trade parity against the legacy engine,
and the ADTV/circuit-lock/liquidity data plumbing several strategies
reference but don't yet consume).
"""

from momentum_framework.strategies.base import StrategyBase, WeightedMomentumStrategy

__all__ = ["StrategyBase", "WeightedMomentumStrategy"]
