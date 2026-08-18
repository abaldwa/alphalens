"""
features/fundamental_composites.py

Phase: 3.x (Fundamental Analysis API Scaffolding)
Specs: SPEC-FA-008
Owner: Platform / Features
Consumers: datastore/api/routers/fundamental_analysis.py

The 30 raw fundamental ratios (27 sector-relative z-scored per SPEC-FEAT-002,
features/fundamental.py) and 12 governance features (features/governance.py)
are already computed daily and merged into the same feature Parquet
features/matrix_builder.py writes (config.settings.FEATURES_DAILY_DIR) — see
datastore/api/routers/technical.py's docstring for the equivalent TA story.

What's genuinely missing — confirmed during 2026-07-01 planning — is the
small set of composite scores (quality/growth/management) and peer-ranking
logic systems/fundamental_analysis/{quality,growth,management,peers}/ were
always meant to hold but never got built. These functions are intentionally
small (combine already-computed values, no new raw data ingestion) and are
called at API-request time, not persisted as new feature columns — there is
no new ground-truth data here, just documented arithmetic over real inputs.

quality_score/growth_score operate on the sector-relative z-scored ratios
already in the feature Parquet (the matrix has no raw, un-z-scored ratio
columns — see features/fundamental.py's compute_fundamental_features_panel),
so a positive score means "better than sector peers," not an absolute
threshold like "ROE > 15%". management_quality_score operates on raw
governance fields (not z-scored) since features/governance.py never
z-scores them.
"""

from typing import Any, Dict, List, Optional, Set

import numpy as np
import pandas as pd

# Re-exported so datastore/api/routers/fundamentals.py (and any other
# existing consumer importing from this module) can pick up the 3 new
# composite-score functions without changing its import style. Canonical
# implementations live in systems/fundamental_analysis/{quality,growth}/ —
# the location systems/copilot/strategy_spec.py and this module's own
# docstring both said was "always meant to hold" this logic.
from systems.fundamental_analysis.contrarian.normalization import normalization_value_score
from systems.fundamental_analysis.contrarian.recovery import contrarian_recovery_score
from systems.fundamental_analysis.growth.capital_efficiency import capital_efficiency_growth_score
from systems.fundamental_analysis.growth.earnings_rerating import earnings_rerating_score
from systems.fundamental_analysis.growth.garp import garp_score
from systems.fundamental_analysis.growth.longevity import longevity_score
from systems.fundamental_analysis.growth.qglp import qglp_score
from systems.fundamental_analysis.growth.small_cap_compounders import small_cap_compounder_score
from systems.fundamental_analysis.growth.smile import smile_score
from systems.fundamental_analysis.growth.story_numbers import story_numbers_score
from systems.fundamental_analysis.growth.under_followed import under_followed_growth_score
from systems.fundamental_analysis.management.governance_quality_growth import governance_quality_growth_score
from systems.fundamental_analysis.management.promoter_aligned import promoter_aligned_score
from systems.fundamental_analysis.quality.capital_allocation import capital_allocation_score
from systems.fundamental_analysis.quality.fcf_low_debt import fcf_low_debt_score
from systems.fundamental_analysis.quality.magic_formula import magic_formula_score
from systems.fundamental_analysis.quality.moat import moat_score
from systems.fundamental_analysis.quality.owner_earnings import owner_earnings_score
from systems.fundamental_analysis.quality.quality_value import quality_value_composite
from systems.fundamental_analysis.quality.sector_leader import sector_leader_score
from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

__all__ = [
    "quality_score", "growth_score", "management_quality_score", "select_peers",
    "SCREENER_PRESETS", "matches_screener_preset", "STRATEGY_CATALOG", "SCORE_FUNCTIONS",
    "PRESET_EXCLUDED_SECTORS", "is_sector_excluded", "SCREENER_PRESET_CHANGELOG", "BACKTESTED_STRATEGIES",
    "quality_value_composite", "fcf_low_debt_score", "garp_score", "magic_formula_score",
    "owner_earnings_score", "moat_score", "capital_allocation_score", "sector_leader_score",
    "qglp_score", "longevity_score", "story_numbers_score", "earnings_rerating_score",
    "small_cap_compounder_score", "smile_score", "under_followed_growth_score",
    "capital_efficiency_growth_score", "governance_quality_growth_score", "promoter_aligned_score",
    "contrarian_recovery_score", "normalization_value_score",
]

# Weights are documented, not tuned/backtested — same standing as the
# documented-but-not-backtested weights already in this codebase's other
# composite scores (e.g. forensic_classical.py's 20/40/20/20 split,
# justified in that file's own docstring as a starting point subject to
# revision once enough labeled outcomes exist).
QUALITY_WEIGHTS = {"roe": 0.30, "roce": 0.30, "net_margin": 0.20, "debt_to_equity": -0.20}
GROWTH_WEIGHTS = {"revenue_growth_yoy": 0.30, "eps_growth_yoy": 0.30, "revenue_cagr_3yr": 0.40}


# Moved to systems/fundamental_analysis/scoring_utils.py so the new
# quality_value/fcf_low_debt/magic_formula/garp scores (imported below)
# can share it without this module importing back from that package.
# Kept as a module-level alias since it's already imported elsewhere as
# `features.fundamental_composites._weighted_zscore_composite`.
_weighted_zscore_composite = weighted_zscore_composite


def quality_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative ROE/ROCE/margin (higher=better) vs. leverage (lower=better)."""
    score: Optional[float] = _weighted_zscore_composite(ratios, QUALITY_WEIGHTS)
    return score


def growth_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative revenue/EPS growth and 3yr CAGR."""
    score: Optional[float] = _weighted_zscore_composite(ratios, GROWTH_WEIGHTS)
    return score


def management_quality_score(governance: Dict[str, float]) -> Optional[float]:
    """
    0-100, built from raw (non-z-scored) governance fields:
    start at 50 (neutral), -0.5 point per 1% promoter pledge (lower pledge
    is better), -20 if promoter_pledge_spiral_flag is set, +15 if
    institutional_conviction_flag is set (FII+DII+MF all increasing qoq —
    see features/governance.py). Returns None if promoter_pledge is
    entirely missing (no shareholding data yet).
    """
    pledge = governance.get("promoter_pledge")
    if pledge is None or (isinstance(pledge, float) and np.isnan(pledge)):
        return None
    score = 50.0 - 0.5 * pledge
    if governance.get("promoter_pledge_spiral_flag"):
        score -= 20.0
    if governance.get("institutional_conviction_flag"):
        score += 15.0
    return float(np.clip(score, 0, 100))


def select_peers(
    ticker: str, panel: pd.DataFrame, sector_map: Dict[str, str], mcap_map: Dict[str, float], k: int = 5
) -> List[str]:
    """
    Real peer-selection logic (was previously unimplemented —
    systems/fundamental_analysis/peers/ was an empty stub): same sector,
    ranked by closeness in log(market_cap), top k excluding the ticker
    itself. `panel` is the day's fundamental feature rows (ticker column
    must be present) — only tickers that actually have a row in the panel
    can be peers, so the result only ever names tickers with real data.

    [2026-07-02 fix] config/build_universe.py currently hardcodes
    market_cap_cr=0 for the entire universe (NSE's free archives don't
    publish bulk market cap, and no other source is wired in yet — see
    that module's docstring). The original version of this function
    required own_mcap > 0, which meant peers could never be returned for
    any ticker while that gap exists. Falls back to sector-only selection
    (alphabetical, for determinism — no fabricated market-cap ranking)
    whenever market cap is unavailable for the ticker or its candidates.
    """
    sector = sector_map.get(ticker)
    if sector is None:
        return []
    own_mcap = mcap_map.get(ticker)
    candidates = [
        t for t in panel["ticker"]
        if t != ticker and sector_map.get(t) == sector
    ]
    if not candidates:
        return []
    # Narrowed inline rather than via a separate `have_mcap` bool: the bool
    # carried the None-check but not the narrowing, so np.log() below was
    # being handed an Optional[float] as far as the type checker could tell.
    if own_mcap is not None and own_mcap > 0:
        mcap_candidates = [t for t in candidates if mcap_map.get(t, 0) > 0]
        if mcap_candidates:
            own_log_mcap = np.log(own_mcap)
            mcap_candidates.sort(key=lambda t: abs(np.log(mcap_map[t]) - own_log_mcap))
            return mcap_candidates[:k]
    # No usable market-cap data (own or peers') — fall back to a
    # deterministic sector-only ordering rather than returning nothing.
    return sorted(candidates)[:k]


# [2026-07-25 model-review fix] No preset ever had a version/changelog
# mechanism, so an old backtest report referencing preset="X" gives no way
# to tell, from the report alone, which threshold DEFINITION of X produced
# it if X was later changed in place (backtest-reviewer's finding — this
# had already happened once, to "garp", before this changelog existed).
# Going forward, any in-place SCREENER_PRESETS mutation should get an
# entry here so an auditor can at least look this dict up, even though the
# backtest_runs table itself still doesn't stamp a preset version per run.
SCREENER_PRESET_CHANGELOG: Dict[str, List[Dict[str, str]]] = {
    "garp": [
        {
            "date": "2026-07-25",
            "change": (
                "Replaced {'revenue_growth_yoy': 0.5, 'pe_ratio': -0.5} with "
                "{'revenue_growth_yoy': 0.5, 'eps_growth_yoy': 0.5, 'pe_ratio': -0.5} "
                "(added EPS growth leg, matching the source formula catalog's GARP "
                "definition more closely). Any backtest report with preset='garp' "
                "dated before 2026-07-25 used the OLD (looser, 2-factor) definition."
            ),
        },
    ],
}

# Screener presets operate on sector-relative z-scores (the only ratio
# representation the feature Parquet carries) — "quality compounder" means
# "above sector peers on these dimensions," not an absolute % threshold.
SCREENER_PRESETS = {
    "quality_compounder": {"roe": 1.0, "roce": 1.0, "debt_to_equity": -0.5},  # min z-score per column (sign-adjusted)
    # Replaced in place with the faithful GARP formula (growth + PE
    # discipline) — previously just revenue_growth_yoy/pe_ratio at looser
    # thresholds. Historical preset="garp" backtest re-runs will now match
    # a different (stricter) set of tickers than before this change. See
    # SCREENER_PRESET_CHANGELOG above for the auditable record of this change.
    "garp": {"revenue_growth_yoy": 0.5, "eps_growth_yoy": 0.5, "pe_ratio": -0.5},
    "turnaround": {"revenue_growth_yoy": 1.0, "eps_growth_yoy": 1.0},  # strong recent acceleration vs peers
    # Magic Formula: cheap on EV/EBIT and high return on capital, both above sector peers.
    "magic_formula": {"ev_ebit_yield": 0.5, "magic_formula_roc": 0.5},
    # Quality-Value Composite: cheap (EV/EBIT, book-to-market) + quality (ROCE/ROE), above sector peers.
    "quality_value": {"ev_ebit_yield": 0.3, "book_to_market": 0.3, "roce": 0.3, "roe": 0.3},
    # FCF Yield + Low Debt: strong cash generation, below-peer leverage, above-peer interest coverage.
    "fcf_low_debt": {"fcf_ev_yield": 0.5, "net_debt_to_ebitda": -0.5, "interest_coverage": 0.3},
    # Deep Value with Solvency Filter: cheap only if leverage/liquidity are also above-peer safe.
    "deep_value_solvency": {
        "book_to_market": 0.3, "ev_ebit_yield": 0.3,
        "debt_to_equity": -0.3, "interest_coverage": 0.3, "current_ratio": 0.2,
    },
    # Cash-Flow-Backed Earnings: anti-manipulation filter — CFO/PAT and FCF/EV
    # above peers, receivable days not deteriorating vs peers.
    "cash_flow_backed_earnings": {
        "cfo_to_pat": 0.5, "fcf_ev_yield": 0.3, "receivable_days_change": -0.3,
    },
    # Turnaround with Financial Recovery: ROA/current-ratio improving, leverage
    # falling, margins expanding — all vs. sector peers, stricter than plain Turnaround.
    "turnaround_recovery": {
        "delta_roa_1y": 0.3, "delta_current_ratio_1y": 0.3,
        "delta_long_term_debt_to_assets_1y": -0.3, "margin_expansion": 0.3,
    },
}

# [2026-07-25 model-review fix] domain-expert's top finding: sector-relative
# z-scoring does NOT fix Magic Formula's EV/EBIT-yield and NWC-based ROC
# formulas being structurally meaningless for banks/NBFCs/insurers — EBIT
# isn't a coherent concept when "revenue" is net interest income, and
# "current liabilities" for a bank includes customer deposits (core
# funding, not a working-capital drag). Comparing a bank's nonsensical
# EV/EBIT-ROC only to other banks' equally nonsensical EV/EBIT-ROC still
# ranks on noise. Greenblatt's own Magic Formula excludes Financials for
# exactly this reason; that exclusion is a correctness fix, not optional.
# This project's sector taxonomy (config/sector_index_map.py) has a single
# "Financial Services" bucket covering banks/NBFCs/insurers — no separate
# per-subsector values exist.
# [BUG FIX, 2026-07-28 model-review] Magic Formula was the only strategy
# excluded here, but the same structural problem — EBIT/ROCE/debt-to-equity
# aren't coherent concepts for a bank/NBFC/insurer's financial statements
# (see the Magic Formula comment above for the full argument) — applies
# equally to every other strategy whose formula LEANS on one or more of
# ROE, ROCE (incl. its 5yr-average variant), or a leverage/solvency ratio
# (debt_to_equity, net_debt_to_ebitda, current_ratio) as a dominant
# component, not just an incidental one. Judgment call on "dominant":
# these ratios collectively account for roughly half or more of the
# formula's weight in every key added below (checked against each
# strategy's own WEIGHTS dict) — strategies where such a ratio is present
# but a minor/incidental component (e.g. earnings_rerating's 20%
# delta_roce_3y leg, contrarian_recovery's 19% net_debt_to_ebitda leg)
# were deliberately left out to avoid over-broadly excluding Financial
# Services from strategies that aren't actually dominated by these ratios.
# Keys cover both SCREENER_PRESETS ("preset" kind, enforced by
# matches_screener_preset below) and SCORE_FUNCTIONS ("composite_score"
# kind, enforced by backtest/adapters/fundamental_adapter.py's
# generate_signals — see that module's SCORE_FUNCTIONS branch).
# [BUG FIX, 2026-07-28 second model-review, item 14] Utilities added
# alongside every existing Financial Services exclusion below: Greenblatt's
# own Magic Formula excludes Utilities for the same regulated-ROE reason
# it excludes Financials — utility ROE/ROCE is set/capped by a tariff
# regulator (state electricity regulatory commissions in India), not
# determined by competitive capital allocation, so it's just as
# structurally noisy an input to an ROE/ROCE/leverage-dominated formula
# as a bank's regulated-capital-driven ROE is. config/sector_index_map.py
# has a single "Utilities" bucket (power/gas distribution & generation),
# same one-bucket-per-sector granularity as "Financial Services".
# [2026-07-28 third model-review, item 9] AMC/insurer carve-out: the
# blanket "Financial Services" exclusion above also sweeps out asset
# managers and insurers, whose ROE/ROCE aren't regulated-capital-driven
# the same way a bank's or NBFC's is — arguably these sub-industries
# shouldn't be excluded at all. This was considered and DELIBERATELY
# left unresolved in this and the prior review rounds, not simply
# forgotten: config/sector_index_map.py has only one flat "Financial
# Services" bucket with no sub-industry taxonomy (bank/NBFC/AMC/insurer
# aren't distinguished), and hand-labeling the ~500 Nifty 500 tickers
# needed to build that taxonomy isn't proportionate to this pass's scope.
# Accepted as a residual risk pending a real sub-industry classification
# source.
#
# [2026-07-28 third model-review, item 10] Related-party-transaction /
# holding-company-discount blind spot: none of the 26 fundamental
# strategies below incorporate the RPT features that already exist
# elsewhere in the codebase (features/deep_forensic.py's rpt_intensity,
# systems/ml_signal_engine/models/forensic/forensic_ml.py). A promoter-
# heavy or RPT-heavy stock can currently clear a quality/value screen
# purely on ROE/ROCE optics with no forensic penalty. Known gap, flagged
# for future consideration — wiring RPT features into these 26 strategies
# is a larger design decision (which strategies, what weight, what
# threshold) out of scope for this pass.
PRESET_EXCLUDED_SECTORS: Dict[str, Set[str]] = {
    "magic_formula": {"Financial Services", "Utilities"},
    # [BUG FIX, 2026-07-28 second model-review, item 6] quality_value meets
    # this dict's own stated "roughly half or more" bar (roce(.3)+roe(.3) =
    # 60% of its 1.2 raw weight total = 50% normalized) but was missing —
    # same key covers both the SCREENER_PRESETS "quality_value" preset
    # (ev_ebit_yield/book_to_market/roce/roe, all 0.3 each) and the
    # SCORE_FUNCTIONS "quality_value" composite
    # (systems/fundamental_analysis/quality/quality_value.py's
    # QUALITY_VALUE_WEIGHTS: ev_ebit_yield .30 + book_to_market .20 +
    # roce .30 + roe .20). Re-checked every other strategy against the same
    # bar while here (2026-07-28): fcf_low_debt's net_debt_to_ebitda leg is
    # only 30% (score)/38% (preset, normalized) on its own — below bar,
    # like contrarian_recovery's already-noted 19% leg — interest_coverage
    # isn't one of the named leverage/solvency ratios above so it's not
    # added to that total. earnings_rerating (20% delta_roce_3y),
    # story_numbers (no roce/roe/leverage leg at all), garp (its
    # "stability" leg is margin_stability_5y/cfo_to_pat, not roce/roe),
    # smile, under_followed (22.5% effective delta_roce_3y after leg
    # weighting), normalization_value, and contrarian_recovery similarly
    # stay below the bar and are left out. promoter_aligned is 75%-weighted
    # on qglp_score (already excluded below) but that 75% multiplier still
    # leaves the ROE/ROCE-heavy content as the strategy's dominant driver —
    # left undecided/out of scope for this pass since it's a composed
    # (not directly-weighted) case the existing "checked against each
    # strategy's own WEIGHTS dict" methodology doesn't cleanly cover;
    # flagged here for a human to confirm rather than silently included.
    "quality_value": {"Financial Services", "Utilities"},
    # SCREENER_PRESETS ("preset" kind)
    "quality_compounder": {"Financial Services", "Utilities"},  # roe(1.0)+roce(1.0)+debt_to_equity(-0.5): 100% leverage/ROE/ROCE
    "deep_value_solvency": {"Financial Services", "Utilities"},  # debt_to_equity+interest_coverage+current_ratio: 57% of weight
    "turnaround_recovery": {"Financial Services", "Utilities"},  # delta_roa+delta_current_ratio+delta_ltd/assets: 50% of weight
    # SCORE_FUNCTIONS ("composite_score" kind)
    "moat": {"Financial Services", "Utilities"},  # avg_roce_5y(.45)+debt_to_equity(-.25): 70% of weight
    "sector_leader": {"Financial Services", "Utilities"},  # avg_roce_5y(.35): explicitly named in the 2026-07-28 review
    "longevity": {"Financial Services", "Utilities"},  # avg_roce_5y(.35)+debt_to_equity(-.20): 55% of weight
    "owner_earnings": {"Financial Services", "Utilities"},  # roce(.30) of fcf_ev_yield/roce/reinvestment_rate
    "capital_allocation": {"Financial Services", "Utilities"},  # debt_to_equity(-.15)+interest_coverage(.15): 30% of weight
    "qglp": {"Financial Services", "Utilities"},  # roce/roe/avg_roce_5y/delta_roce_3y spread across 3 of its 4 legs
    "capital_efficiency": {"Financial Services", "Utilities"},  # roce(.30) of revenue_cagr/roce/asset_turnover/receivable_days
    "governance_quality_growth": {"Financial Services", "Utilities"},  # roce(.4) of the 70%-weighted business_quality leg (28%)
    "small_cap_compounders": {"Financial Services", "Utilities"},  # roce(.35) + debt_to_equity(-.5) across its two legs
}


def is_sector_excluded(preset: str, sector: Optional[str]) -> bool:
    """Whether `preset` excludes `sector` outright, regardless of ratios.

    [E1, 2026-08-18] THE single place this question is answered. It used to
    be answered in four: matches_screener_preset below, two branches in
    datastore/api/routers/fundamentals.py, and an inline comprehension in
    the /scores endpoint. They drifted exactly as duplicated decisions do --
    /scores skipped the exclusion entirely and shipped
    methodologically-invalid numbers to four frontend pages (a bank's
    reported ROE is not comparable to an industrial's, which is the whole
    reason the exclusion exists) before that was caught in the 2026-07-28
    model review.

    A missing sector never excludes: unknown is not the same as excluded,
    and dropping a ticker because its sector lookup failed would be a
    silent, data-quality-driven change to a strategy's universe."""
    if sector is None:
        return False
    return sector in PRESET_EXCLUDED_SECTORS.get(preset, set())


def matches_screener_preset(ratios: Dict[str, float], preset: str, sector: Optional[str] = None) -> bool:
    """True if every z-scored ratio in SCREENER_PRESETS[preset] clears its
    threshold (thresholds already sign-adjusted so 'pass' always means
    `value >= threshold`, e.g. pe_ratio's -0.5 means 'at least half a
    sector-std cheaper than peers'). Missing inputs fail the screen
    (conservative — never include a ticker on incomplete data).

    `sector`, if supplied, is checked against PRESET_EXCLUDED_SECTORS —
    tickers in an excluded sector always fail, regardless of ratios (see
    that dict's comment for why). Callers that don't have sector on hand
    for a given preset will simply never trigger this filter — a caller
    upgrade, not a new required argument."""
    if preset not in SCREENER_PRESETS:
        raise ValueError(f"Unknown screener preset: {preset}")
    if is_sector_excluded(preset, sector):
        return False
    for col, threshold in SCREENER_PRESETS[preset].items():
        v = ratios.get(col)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return False
        signed_v = -v if threshold < 0 else v
        signed_threshold = abs(threshold)
        if signed_v < signed_threshold:
            return False
    return True


# Investor-style menu metadata for the frontend (GET /api/v1/fundamentals/
# screener/catalog, datastore/api/routers/fundamentals.py) — all 26
# strategies from the India-Specific Fundamental Analysis Strategy Catalog.
# `kind` tells a caller how to run the strategy:
#   "preset"          -> matches_screener_preset(ratios, key) via SCREENER_PRESETS
#   "composite_score" -> SCORE_FUNCTIONS[key](ratios), a continuous 0-100 rank
#   "bespoke"         -> a dedicated module under systems/fundamental_analysis/
#                        that reads raw PIT financials directly (not the
#                        z-scored panel) — piotroski_on_value, margin_of_safety, net_net.
# Dict[str, Any] values, not Dict[str, str]: the entries carry a `backtested`
# BOOL alongside their string labels (set in the loop below). Declaring the
# values as str made that assignment an error, and annotating around it just
# moved the error to the consumer that unpacks the entry -- the type was
# simply wrong about what this table holds.
STRATEGY_CATALOG: Dict[str, Dict[str, Any]] = {
    "piotroski_on_value": {"label": "Piotroski-on-Value", "category": "Value", "kind": "bespoke",
                            "description": "Cheap stocks filtered by Piotroski F-Score >= 8."},
    "magic_formula": {"label": "Magic Formula", "category": "Value", "kind": "preset",
                       "description": "Earnings yield + return on capital (Greenblatt)."},
    "margin_of_safety": {"label": "Margin of Safety Value", "category": "Value", "kind": "bespoke",
                          "description": "Graham/Klarman conservative intrinsic value + solvency gate."},
    "net_net": {"label": "Net-Net / Asset Value", "category": "Value", "kind": "bespoke",
                "description": "Graham deep value: price below net current asset value."},
    "deep_value_solvency": {"label": "Deep Value with Solvency Filter", "category": "Value", "kind": "preset",
                             "description": "Cheap stocks only if leverage/liquidity are controlled."},
    "quality_value": {"label": "Quality Value Composite", "category": "Quality", "kind": "preset",
                       "description": "Valuation + ROCE/ROE + cash-flow quality."},
    "fcf_low_debt": {"label": "FCF Yield + Low Debt", "category": "Quality", "kind": "preset",
                      "description": "Cash generation with balance-sheet safety."},
    "owner_earnings": {"label": "Owner Earnings Compounders", "category": "Quality", "kind": "composite_score",
                        "description": "Buffett-style owner-earnings yield + ROCE + reinvestment."},
    "moat": {"label": "Moat Compounders", "category": "Quality", "kind": "composite_score",
             "description": "Durable-advantage proxy: 5yr ROCE persistence + margin stability."},
    "capital_allocation": {"label": "Capital Allocation Quality", "category": "Quality", "kind": "composite_score",
                            "description": "Whether retained capital creates value over time."},
    "cash_flow_backed_earnings": {"label": "Cash-Flow-Backed Earnings", "category": "Quality", "kind": "preset",
                                   "description": "Anti-manipulation filter: earnings converting to cash."},
    "sector_leader": {"label": "Sector-Leader Compounders", "category": "Quality", "kind": "composite_score",
                       "description": "Industry leaders that defend margins and compound longer."},
    "garp": {"label": "GARP / PEG", "category": "Growth", "kind": "preset",
             "description": "Growth at a reasonable price (Lynch)."},
    "qglp": {"label": "QGLP Composite", "category": "Growth", "kind": "composite_score",
             "description": "Quality + Growth + Longevity + Price (Raamdeo Agrawal)."},
    "longevity": {"label": "Longevity Compounders", "category": "Growth", "kind": "composite_score",
                  "description": "Durability over raw growth speed."},
    "story_numbers": {"label": "Story + Numbers Confirmation", "category": "Growth", "kind": "composite_score",
                       "description": "Narrative confirmed by growth + cash conversion."},
    "earnings_rerating": {"label": "Earnings Re-rating Candidates", "category": "Growth", "kind": "composite_score",
                           "description": "Fundamentals inflecting before valuation catches up."},
    "small_cap_compounders": {"label": "Small-Cap Compounders", "category": "Growth", "kind": "composite_score",
                               "description": "Small size + quality/growth + risk control (Kedia, Khanna)."},
    "smile": {"label": "SMILE Growth Framework", "category": "Growth", "kind": "composite_score",
              "description": "Small size, experience, aspiration, market potential (Kedia)."},
    "under_followed": {"label": "Under-followed Growth Improvers", "category": "Growth", "kind": "composite_score",
                        "description": "Re-rating discovery in under-owned smaller companies (Khanna)."},
    "capital_efficiency": {"label": "Capital-Efficiency Growth", "category": "Growth", "kind": "composite_score",
                            "description": "Growth without balance-sheet stress."},
    "governance_quality_growth": {"label": "Governance-Aware Quality Growth", "category": "Governance",
                                   "kind": "composite_score",
                                   "description": "Business quality paired with governance discipline (Singhania)."},
    "promoter_aligned": {"label": "Promoter-Aligned Compounders", "category": "Governance", "kind": "composite_score",
                          "description": "QGLP overlaid with promoter alignment — an overlay, not standalone."},
    "contrarian_recovery": {"label": "Contrarian Recovery Value", "category": "Contrarian", "kind": "composite_score",
                             "description": "Cheap stocks with improving fundamentals behind poor sentiment (Burry)."},
    "normalization_value": {"label": "Normalization Value", "category": "Contrarian", "kind": "composite_score",
                             "description": "Cyclicals where current earnings are temporarily depressed."},
    "turnaround_recovery": {"label": "Turnaround with Financial Recovery", "category": "Contrarian", "kind": "preset",
                             "description": "Buy distress only after measurable recovery starts."},
}

# [2026-07-25 model-review fix] product-owner's top finding: a trader
# opening the strategies page sees 26 named strategies presented as
# selectable/actionable screens with no indication that none have been
# backtested — the "hardcoded, not tuned" caveat only lived in code
# comments, not anywhere a user would see it. This set starts genuinely
# empty (no strategy has actually been backtested yet — not a
# placeholder, the real current state) and should be updated only as
# each strategy actually clears a real walk-forward backtest.
BACKTESTED_STRATEGIES: Set[str] = set()
for _key, _meta in STRATEGY_CATALOG.items():
    _meta["backtested"] = _key in BACKTESTED_STRATEGIES
del _key, _meta

# Callable registry for the "composite_score" kind above — used by the
# /{ticker}/scores endpoint to compute any of these on demand for one ticker.
SCORE_FUNCTIONS = {
    "quality": quality_score,
    "growth": growth_score,
    "quality_value": quality_value_composite,
    "fcf_low_debt": fcf_low_debt_score,
    "garp": garp_score,
    "magic_formula": magic_formula_score,
    "owner_earnings": owner_earnings_score,
    "moat": moat_score,
    "capital_allocation": capital_allocation_score,
    "sector_leader": sector_leader_score,
    "qglp": qglp_score,
    "longevity": longevity_score,
    "story_numbers": story_numbers_score,
    "earnings_rerating": earnings_rerating_score,
    "small_cap_compounders": small_cap_compounder_score,
    "smile": smile_score,
    "under_followed": under_followed_growth_score,
    "capital_efficiency": capital_efficiency_growth_score,
    "governance_quality_growth": governance_quality_growth_score,
    "promoter_aligned": promoter_aligned_score,
    "contrarian_recovery": contrarian_recovery_score,
    "normalization_value": normalization_value_score,
}
