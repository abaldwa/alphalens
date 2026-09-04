"""
Sector Ranking — R10's two-stage aggregation, layered on top of the
SAME shared TrailingMomentumSignal ranking every other trailing_return
strategy uses (see common/signals.py's module note).

Direct port of features/momentum_strategy.py::rank_sectors() and
rank_constituents_within_sectors() — same formulas, unchanged.

R10 is NOT a different ranking signal from R01/R03/R07/R08/R09/R14-R17 —
it's the identical per-ticker trailing_return computation, with a sector
filter inserted between ranking and top_n selection: (1) rank every
ticker in the band by trailing return, (2) average scores by sector to
rank sectors, (3) keep only tickers in the top_sectors sectors, (4) THEN
take top_n from what's left. See common/signals.py::IndustryMomentumSignal
for how these two stages are composed.
"""

from typing import Dict, List
import pandas as pd


def rank_sectors(
    momentum: pd.Series,
    sector_lookup: Dict[str, str],
    top_sectors: int = 5,
) -> pd.Series:
    """
    Rank sectors by average momentum of their constituents.

    momentum : ticker -> momentum_score
    sector_lookup : ticker -> sector name
    top_sectors : informational only here (caller decides how many to keep)

    Returns a Series indexed by sector name, sorted descending by average
    score. Tickers with no sector mapping are grouped as "Unknown" — never
    excluded on missing data, per this project's convention.
    """
    if momentum.empty or not sector_lookup:
        return pd.Series(dtype=float)

    sector_scores: Dict[str, List[float]] = {}
    for ticker, score in momentum.items():
        sector = sector_lookup.get(str(ticker), "Unknown")
        sector_scores.setdefault(sector, []).append(score)

    sector_avg = {sector: sum(scores) / len(scores) for sector, scores in sector_scores.items()}
    return pd.Series(sector_avg).sort_values(ascending=False)


def rank_constituents_within_sectors(
    momentum: pd.Series,
    sector_lookup: Dict[str, str],
    top_sectors_list: List[str],
) -> pd.Series:
    """
    Filter `momentum` to only tickers belonging to `top_sectors_list`,
    preserving original scores (no re-weighting) — the second stage of
    R10's two-stage ranking.
    """
    if momentum.empty or not sector_lookup or not top_sectors_list:
        return momentum

    top_sectors_set = set(top_sectors_list)
    kept = {
        ticker: score for ticker, score in momentum.items()
        if sector_lookup.get(str(ticker), "Unknown") in top_sectors_set
    }
    if not kept:
        return pd.Series(dtype=float)
    return pd.Series(kept)
