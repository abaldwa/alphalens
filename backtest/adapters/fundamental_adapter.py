"""
backtest/adapters/fundamental_adapter.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 2
Owner: Platform / Backtest
Consumers: backtest/core/engine.py::BacktestOrchestrator

The first backtest capability the Fundamental channel has ever had — no
prior module (backtest.py-adjacent or otherwise) existed for it.

Reuses the same daily feature Parquet store technical_adapter.py reads
(config.settings.FEATURES_DAILY_DIR, via datastore/api/utils/feature_store
.read_feature_day) and the real screener-preset logic already backing the
live GET /api/v1/fundamental/screener endpoint
(features/fundamental_composites.py::matches_screener_preset,
SCREENER_PRESETS). Neither is modified.

`preset` names any of the 26 features.fundamental_composites.
STRATEGY_CATALOG strategies, not just a SCREENER_PRESETS key — see
BESPOKE_PRESETS (piotroski_on_value/margin_of_safety/net_net, raw-PIT-
financials strategies) and the SCORE_FUNCTIONS branch below (the 22
continuous 0-100 composite scores — QGLP, Moat, Owner Earnings, etc. —
added 2026-07-25 so every strategy in the catalog is actually
backtestable, not just the 9 binary presets). A composite-score strategy
ranks the whole universe by score and takes the top top_n, same "rank
everyone, take the top N" idiom momentum_adapter.py already uses — it
reuses this class's existing matched+_last_ratios+_composite_strength
convergence point rather than duplicating a second ranking path: every
scored ticker is added to `matched`, and _composite_strength summing a
single-entry {"score": value} dict is exactly that ticker's score, so the
existing top-N-by-composite-strength selection below sorts by score
without any additional code.

Real-data caveat this adapter does NOT need to re-litigate: raw
fundamentals coverage is near-empty before 2020 (BacktestUmbrellaPlan.md
Known Data Gaps #1), so ratio z-score columns in the feature Parquet will
mostly be NaN before then — matches_screener_preset already treats a
missing input as "conservatively fails the screen," and
backtest.core.run_context.BacktestRun independently hard-blocks any
channel="fundamental" run starting before 2020-01-01. This adapter relies
on both of those rather than adding its own date-gating logic.

Signal semantics mirror technical_adapter.py/momentum_adapter.py's
rotation pattern: hold while a ticker keeps matching the preset, sell
when it stops. Fundamental screens are naturally lower-turnover (ratios
move slowly, one filing at a time) so this will churn far less than
Technical/Momentum in practice — a property of the data, not something
this adapter special-cases.
"""

import logging
from datetime import date as date_type, datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from backtest.core.adtv import adtv_cr_for_ticker

from backtest.core.engine import Signal
from backtest.core.horizon import HorizonBucket
from datastore.api.utils.feature_store import read_feature_day
from features.fundamental import FUNDAMENTAL_FEATURES
from features.fundamental_composites import (
    PRESET_EXCLUDED_SECTORS,
    SCORE_FUNCTIONS,
    SCREENER_PRESETS,
    matches_screener_preset,
)
from features.governance import GOVERNANCE_FEATURES
from systems.fundamental_analysis.quality.net_net import LIQUIDITY_FLOOR_MARKET_CAP_CR

logger = logging.getLogger(__name__)

# [BUG FIX, 2026-07-28 second model-review, item 9] Unlike net_net.py,
# these three SCORE_FUNCTIONS composites actively reward smallness/low
# ownership (small_cap_compounders' -1.0 size weight, smile's small-size
# leg, under_followed's low-institutional-ownership proxy) with zero
# minimum market-cap or liquidity gate of their own — prone to selecting
# circuit-filter-prone, unfillable Indian small-caps. Reuses net_net.py's
# LIQUIDITY_FLOOR_MARKET_CAP_CR rather than defining a second constant.
_PRESETS_NEEDING_LIQUIDITY_FLOOR = {"small_cap_compounders", "smile", "under_followed"}

# [2026-07-28 third model-review, item 7] LIQUIDITY_FLOOR_MARKET_CAP_CR
# (Rs 50cr, copied from net_net.py) is effectively decorative within the
# Nifty 500 universe these 3 strategies actually draw from — the 10th-
# percentile market cap in that universe is roughly Rs 10,000cr, two
# orders of magnitude above this floor, so it can never bind against a
# genuinely small/illiquid name inside this universe. The project already
# has an ADTV-based liquidity floor elsewhere (config/training_universe.py's
# RECOMMENDATION_ADTV_FLOOR_CR/TRAINING_ADTV_FLOOR_CR) that would give this
# gate real bite against illiquid-but-large-market-cap names. Not wired in
# this pass — plumbing a new liquidity data source through this adapter
# (ADTV isn't currently passed to it) is a larger change than this review
# round's scope; documented here so the market-cap floor's limited
# effectiveness is a known, explicit trade-off rather than an
# unrecognized gap. A real ADTV-based floor here is the correct follow-up.

# [2026-07-25 fix] Previously a narrow, hand-maintained subset — already
# caused one real gap this session (several SCORE_FUNCTIONS composites
# need multi-year/delta/governance fields this tuple didn't list).
# Importing the full feature lists directly means every current and
# future feature is available to every strategy automatically; no more
# per-strategy whitelist to keep in sync by hand.
RATIO_FEATURES = tuple(FUNDAMENTAL_FEATURES) + tuple(GOVERNANCE_FEATURES)

# piotroski_on_value/margin_of_safety/net_net don't fit the SCREENER_PRESETS
# z-score-threshold pattern (they compare raw rupee values — F-Score gate,
# Graham Number, NCAV — to price, not sector z-scores; see
# systems/fundamental_analysis/quality/*.py's module docstrings), so
# they're handled as special preset names here rather than added to
# SCREENER_PRESETS.
PIOTROSKI_ON_VALUE_PRESET = "piotroski_on_value"
MARGIN_OF_SAFETY_PRESET = "margin_of_safety"
NET_NET_PRESET = "net_net"
BESPOKE_PRESETS = (PIOTROSKI_ON_VALUE_PRESET, MARGIN_OF_SAFETY_PRESET, NET_NET_PRESET)


class FundamentalAdapter:
    channel = "fundamental"

    def __init__(
        self, preset: str, top_n: int = 10, sector_lookup: Optional[Dict[str, str]] = None,
        db_conn: Optional[Any] = None, market_cap_lookup: Optional[Dict[str, float]] = None,
        price_panel: Optional[pd.DataFrame] = None, volume_panel: Optional[pd.DataFrame] = None,
        adtv_lookback_days: int = 20,
    ) -> None:
        if preset not in BESPOKE_PRESETS and preset not in SCREENER_PRESETS and preset not in SCORE_FUNCTIONS:
            raise ValueError(
                f"Unknown screener preset {preset!r}. "
                f"Valid: {list(SCREENER_PRESETS.keys()) + list(BESPOKE_PRESETS) + list(SCORE_FUNCTIONS.keys())}"
            )
        if top_n <= 0:
            raise ValueError("top_n must be positive")
        self.preset = preset
        self.top_n = top_n
        self._sector_lookup = sector_lookup or {}
        # [BUG FIX, 2026-07-28 second model-review, item 9] real ticker ->
        # market_cap_cr, same optional/deferred-wiring convention as
        # sector_lookup — feeds the LIQUIDITY_FLOOR_MARKET_CAP_CR gate below
        # for the three presets that have no minimum-size gate of their own
        # (small_cap_compounders, smile, under_followed). None/empty is a
        # safe no-op (gate simply doesn't apply), never a fabricated cap.
        self._market_cap_lookup = market_cap_lookup or {}
        # [BUG FIX, 4th fundamental-strategies review, item 2] optional wide
        # price/volume panels (date index, ticker columns — same shape
        # momentum_adapter.py's price_panel/volume_panel already use) so
        # emitted Signals can carry a real Signal.adtv_cr, letting
        # post_run_checks.py's check_06_liquidity actually enforce
        # MIN_ADT_INR for this channel instead of silently no-op'ing.
        # None (default) preserves prior behavior exactly (adtv_cr stays
        # unset) for any caller that doesn't pass them.
        # [BUG FIX, 5th fundamental-strategies review, item 3] adtv.py's
        # adtv_cr_for_ticker does `.loc[:ts].tail(n)` on price_panel, which
        # silently returns the WRONG (understated) window if price_panel's
        # date index isn't sorted — unlike volume_panel (sorted right
        # above), price_panel was left as whatever row order the caller
        # passed in (run_orchestrator_backtest.py's real OHLCV pivot is
        # already sorted, but nothing enforced that here, and a caller
        # that isn't gets no error, just quietly wrong ADTV).
        self.price_panel = price_panel.sort_index() if price_panel is not None else None
        self.volume_panel = volume_panel.sort_index() if volume_panel is not None else None
        self.adtv_lookback_days = adtv_lookback_days
        # db_conn is intentionally NOT required at construction time (unlike
        # the initial version of this class) — callers that only have the
        # DUCKDB_PATH connection available inside a `with get_duckdb_
        # connection(...)` block (e.g. run_orchestrator_backtest.py) need to
        # construct the adapter first and wire db_conn afterward, same
        # deferred-wiring convention already used for the technical channel's
        # `adapter._screener_cache_conn = conn`. Validated lazily in
        # generate_signals() instead, right before it's actually needed.
        self._db_conn = db_conn
        self._currently_held: set = set()
        self._last_ratios: Dict[str, Dict[str, float]] = {}  # ticker -> ratio dict, from the most recent call

    def _adtv_cr(self, ticker: str, as_of_date: date_type) -> Optional[float]:
        return adtv_cr_for_ticker(
            ticker, as_of_date, self.price_panel, self.volume_panel, self.adtv_lookback_days,
        )

    def generate_signals(self, universe: List[str], as_of_date: date_type, horizon_bucket: HorizonBucket) -> List[Signal]:
        universe_set = set(universe)
        panel = read_feature_day(str(as_of_date))
        self._last_ratios = {}

        if self.preset in BESPOKE_PRESETS:
            if self._db_conn is None:
                raise ValueError(
                    f"preset={self.preset!r} requires db_conn (needs raw fundamentals history) — "
                    "set adapter._db_conn before calling generate_signals()"
                )
            as_of_dt = datetime.combine(as_of_date, datetime.min.time())
            matched = []
            if self.preset == PIOTROSKI_ON_VALUE_PRESET:
                from systems.fundamental_analysis.quality.piotroski_on_value import compute_piotroski_on_value

                for ticker in sorted(universe_set):
                    result = compute_piotroski_on_value(self._db_conn, ticker, as_of_dt, feature_date_str=str(as_of_date))
                    if result["passes"]:
                        matched.append(ticker)
                        self._last_ratios[ticker] = {"f_score": result["f_score"]}
            elif self.preset == MARGIN_OF_SAFETY_PRESET:
                from systems.fundamental_analysis.quality.margin_of_safety import compute_margin_of_safety

                for ticker in sorted(universe_set):
                    result = compute_margin_of_safety(self._db_conn, ticker, as_of_dt)
                    if result["passes"]:
                        matched.append(ticker)
                        self._last_ratios[ticker] = {"margin_of_safety": result["margin_of_safety"]}
            else:
                from systems.fundamental_analysis.quality.net_net import compute_net_net

                for ticker in sorted(universe_set):
                    result = compute_net_net(self._db_conn, ticker, as_of_dt)
                    if result["passes"]:
                        matched.append(ticker)
                        self._last_ratios[ticker] = {"ncav_per_share": result["ncav_per_share"]}
        elif self.preset in SCORE_FUNCTIONS and self.preset not in SCREENER_PRESETS:
            # [BUG FIX, 2026-07-29] SCORE_FUNCTIONS and SCREENER_PRESETS are
            # NOT disjoint: 4 names (magic_formula, garp, fcf_low_debt,
            # quality_value) exist in both dicts, but STRATEGY_CATALOG only
            # ever classifies them with kind="preset" — SCORE_FUNCTIONS'
            # entries under those same names are internal score helpers, not
            # a second top-level strategy. Checking `self.preset in
            # SCORE_FUNCTIONS` alone (as this branch originally did) silently
            # hijacked those 4 genuine screener presets into this ranking
            # path instead of the binary matches_screener_preset path below,
            # so this branch now only claims a preset name if SCREENER_PRESETS
            # doesn't already claim it first.
            #
            # Composite-score strategies (QGLP, Moat, Owner Earnings, etc.)
            # have no binary pass/fail — every ticker with a computable
            # score is a candidate, ranked by score. `matched` here is
            # deliberately "everyone scored," not "everyone who passed a
            # threshold": the top-N-by-composite-strength selection below
            # (shared with the other two branches) does the actual ranking,
            # since _composite_strength on a single-entry {"score": v} dict
            # is exactly v.
            score_fn = SCORE_FUNCTIONS[self.preset]
            excluded_sectors = PRESET_EXCLUDED_SECTORS.get(self.preset, set())
            matched = []
            if panel is not None:
                in_universe = panel[panel["ticker"].isin(universe_set)]
                for _, row in in_universe.iterrows():
                    # [BUG FIX, 2026-07-28 model-review] Composite-score strategies
                    # (Moat, Sector-Leader, Longevity, etc.) never checked
                    # PRESET_EXCLUDED_SECTORS at all — only the plain-preset branch
                    # below did. Several of these formulas lean on ROE/ROCE/
                    # debt-to-equity just as heavily as Magic Formula (see that
                    # dict's comment in features/fundamental_composites.py), so the
                    # same Financial-Services exclusion must apply here too.
                    if excluded_sectors and self._sector_lookup.get(row["ticker"]) in excluded_sectors:
                        continue
                    if self.preset in _PRESETS_NEEDING_LIQUIDITY_FLOOR and self._market_cap_lookup:
                        mcap = self._market_cap_lookup.get(row["ticker"])
                        # market_cap_cr == 0 (or a missing lookup entry) means
                        # "unknown, not yet sourced" per config/universe.py's
                        # established convention (see its phase_1 filter) — not
                        # "genuinely tiny." Only exclude on a genuine positive
                        # market cap at/below the floor; never conflate unknown
                        # with excluded, or liquid large-caps missing from the
                        # lookup get silently dropped.
                        if mcap is not None and mcap > 0 and mcap <= LIQUIDITY_FLOOR_MARKET_CAP_CR:
                            continue
                    ratios = {c: row.get(c) for c in RATIO_FEATURES if c in in_universe.columns}
                    score = score_fn(ratios)
                    if score is None or (isinstance(score, float) and pd.isna(score)):
                        continue
                    matched.append(row["ticker"])
                    self._last_ratios[row["ticker"]] = {"score": score}
        elif panel is None:
            # No materialized feature snapshot for this date (No-Mock-Data
            # Policy: never fabricate one) — nothing new matches, but
            # existing holdings are still re-evaluated against an empty
            # match set below, so they get sold rather than held forever
            # on stale information.
            matched: List[str] = []
        else:
            in_universe = panel[panel["ticker"].isin(universe_set)]
            matched = []
            for _, row in in_universe.iterrows():
                ratios = {c: row.get(c) for c in RATIO_FEATURES if c in in_universe.columns}
                # sector_lookup was already required for Signal.sector below —
                # reused here so preset-level sector exclusions (e.g. Magic
                # Formula's Financial Services exclusion, see
                # features/fundamental_composites.py::PRESET_EXCLUDED_SECTORS)
                # apply in backtests too, not just the live API screener.
                if matches_screener_preset(ratios, self.preset, sector=self._sector_lookup.get(row["ticker"])):
                    matched.append(row["ticker"])
                    self._last_ratios[row["ticker"]] = ratios

        target = set(matched[: self.top_n]) if len(matched) <= self.top_n else set(
            sorted(matched, key=lambda t: -_composite_strength(self._last_ratios[t]))[: self.top_n]
        )

        signals: List[Signal] = []
        for ticker in sorted(self._currently_held - target):
            signals.append(Signal(
                ticker=ticker, action="sell", sector=self._sector_lookup.get(ticker, "Unknown"), conviction=0.0,
                adtv_cr=self._adtv_cr(ticker, as_of_date),
            ))
        for ticker in sorted(target - self._currently_held):
            signals.append(Signal(
                ticker=ticker, action="buy", sector=self._sector_lookup.get(ticker, "Unknown"),
                conviction=_composite_strength(self._last_ratios[ticker]), template=self.preset,
                adtv_cr=self._adtv_cr(ticker, as_of_date),
            ))

        self._currently_held = target
        return signals

    def feature_vector(self, ticker: str, as_of_date: date_type) -> Dict[str, Any]:
        ratios = self._last_ratios.get(ticker)
        if ratios is None:
            return {"preset": self.preset, "matched": False}
        return {"preset": self.preset, "matched": True, **{f"ratio__{k}": v for k, v in ratios.items()}}


def _composite_strength(ratios: Dict[str, float]) -> float:
    """Sum of sign-adjusted z-scores actually used by the preset, for
    ranking matched candidates when more than top_n qualify — matched
    tickers are all real screener passes, this only orders among them."""
    values = [v for v in ratios.values() if v is not None and not pd.isna(v)]
    return sum(values) if values else 0.0
