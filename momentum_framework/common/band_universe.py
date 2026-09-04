"""
Band Universe Resolution — resolves an M-band to its actual ticker
constituents on a given date.

This is what "rankings depend on the band" means concretely: a momentum
signal (TrailingMomentumSignal etc.) must only rank tickers that actually
belong to the band being traded, not the full 800-stock universe filtered
after the fact. Band membership itself is market-cap rank WITHIN the
ADTV-liquid universe — a completely different ranking axis from the
momentum SIGNAL ranking (trailing return, 52wk-high, etc.) applied to
select the top_n winners inside that resolved set.

Delegates to features/momentum_universe.py::momentum_band_universe() — THE
one canonical definition backtest/paper/live all already share (per that
function's own docstring) — rather than reimplementing the ADTV/market-cap
DB queries here. Same delegation pattern as backtesting/orchestrator.py:
read-only data resolution is safe to delegate; it's the broken NAMING
logic (registry_name()) that this framework deliberately does not reuse.
"""

from typing import Any, List

from momentum_framework.common.universe import MBANDS, MBand


def resolve_band_universe(band_id: int, as_of_date: str, conn: Any) -> List[str]:
    """
    Ticker list for `band_id`'s constituents as of `as_of_date`.

    band_id=13 (M13, the full ADTV universe) resolves rank_start=1,
    rank_end=800 — i.e. every framework band, M13 included, goes through
    the SAME market-cap-within-liquid-universe resolution; M13 isn't a
    special case here, only its top_n set (see TOP_N_BY_BAND) differs.
    """
    if band_id not in MBANDS:
        raise ValueError(f"band_id={band_id} not a known M-band: {sorted(MBANDS)}")
    band: MBand = MBANDS[band_id]

    from features.momentum_universe import momentum_band_universe

    return momentum_band_universe(conn, as_of_date, band.rank_start, band.rank_end)
