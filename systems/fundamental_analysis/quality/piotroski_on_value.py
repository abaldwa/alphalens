"""
systems/fundamental_analysis/quality/piotroski_on_value.py

Piotroski-on-Value: select stocks that are both cheap (top-quintile
sector-relative EV/EBIT yield or book-to-market) and accounting-quality
strong (Piotroski F-Score >= 8). Unlike the other 4 strategies, this one
doesn't fit features/fundamental_composites.py's SCREENER_PRESETS
z-score-threshold pattern:

- The F-Score needs raw multi-period financials (current quarter + the
  same quarter one year ago) fed into
  systems.ml_signal_engine.models.forensic.classical_scores.piotroski_f_score
  (reused directly, not reimplemented) — SCREENER_PRESETS only ever sees
  one row of already-z-scored ratios per ticker.
- The cheap-universe gate does reuse the z-scored feature panel (via
  read_feature_day, same source backtest/adapters/fundamental_adapter.py
  already reads), so it isn't a second ranking system.

[2026-07-25 model-review correction] `_build_financials`'s `cfo = fcf +
capex` is an APPROXIMATION feeding piotroski_f_score's F2 (cfo > 0) and
F4 (cfo > net_income) tests, not the company's actually reported
operating cash flow. `fcf` is a raw value from the upstream data source
(Trendlyne/NSE XBRL), not computed in this codebase as cfo - capex — if
the source's FCF used a different capex figure/period/definition than
this codebase's own `capex` column, F2/F4 can disagree with what the
company's real cash flow statement would show. No before/after
comparison against real reported CFO has been run. Treat F2/F4 (and
therefore the overall F-Score gate) as running on a best-effort proxy,
not verified-exact accounting data.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from datastore.api.pit import get_fundamentals_pit
from datastore.api.utils.feature_store import read_feature_day
from systems.ml_signal_engine.models.forensic.classical_scores import piotroski_f_score

logger = logging.getLogger(__name__)

PIOTROSKI_STRONG_GATE = 8
# Top-quintile-by-sector under a normal approximation of the z-score
# distribution (features/fundamental.py's _sector_relative_zscore output).
# Documented approximation, not an exact empirical quintile cut.
CHEAP_ZSCORE_THRESHOLD = 0.84


def _quarters_back(fiscal_year: int, quarter: int, n: int) -> tuple:
    total = (fiscal_year * 4 + (quarter - 1)) - n
    return total // 4, (total % 4) + 1


def _safe_div(a: Optional[float], b: Optional[float]) -> float:
    if a is None or b is None or pd.isna(a) or pd.isna(b) or b == 0:
        return np.nan
    return a / b


def _row_value(row: Optional[pd.Series], col: str) -> float:
    if row is None or col not in row.index:
        return np.nan
    val = row[col]
    return np.nan if val is None or pd.isna(val) else val


def _build_financials(latest: pd.Series, yoy: Optional[pd.Series]) -> Dict[str, float]:
    """Map raw fundamentals_history columns onto the keys piotroski_f_score() expects."""

    def leg(row: Optional[pd.Series]) -> Dict[str, float]:
        ni, ta = _row_value(row, "pat"), _row_value(row, "total_assets")
        ca, cl = _row_value(row, "current_assets"), _row_value(row, "current_liabilities")
        fcf, capex = _row_value(row, "fcf"), _row_value(row, "capex")
        cfo = (fcf + capex) if pd.notna(fcf) and pd.notna(capex) else np.nan
        return {
            "ni": ni,
            "ta": ta,
            "cfo": cfo,
            "roa": _safe_div(ni, ta),
            "ltd_cl": _row_value(row, "borrowings_noncurrent"),
            "current_ratio": _safe_div(ca, cl),
            "shares": _row_value(row, "shares_outstanding"),
            "gross_margin": _safe_div(_row_value(row, "gross_profit"), _row_value(row, "revenue")),
            "asset_turnover": _row_value(row, "asset_turnover"),
        }

    current = leg(latest)
    prior = leg(yoy)
    financials = dict(current)
    for key, value in prior.items():
        financials[f"{key}_yoy"] = value
    return financials


def compute_piotroski_on_value(conn: Any, ticker: str, as_of: datetime, feature_date_str: Optional[str] = None) -> Dict[str, Any]:
    """
    Parameters
    ----------
    conn : open DuckDB connection.
    ticker : str
    as_of : datetime, PIT reference date for the fundamentals lookup.
    feature_date_str : str, optional
        "YYYY-MM-DD" key into the daily feature Parquet for the
        cheap-universe z-score gate. Defaults to as_of.date().isoformat().

    Returns
    -------
    dict
        f_score (0-9 or NaN), is_cheap (bool or None), passes (bool).
        `passes` is only True when both legs are non-NaN and clear their
        gates — missing data conservatively fails, same convention as
        matches_screener_preset().
    """
    history = get_fundamentals_pit(conn, [ticker], as_of)
    if history.empty:
        return {"f_score": np.nan, "is_cheap": None, "passes": False}

    history = history.sort_values("quarter_end_date").reset_index(drop=True)
    latest = history.iloc[-1]
    fy, q = int(latest["fiscal_year"]), int(latest["quarter"])
    yoy_fy, yoy_q = _quarters_back(fy, q, 4)
    yoy_match = history[(history["fiscal_year"] == yoy_fy) & (history["quarter"] == yoy_q)]
    yoy_row = yoy_match.iloc[0] if len(yoy_match) else None

    financials = _build_financials(latest, yoy_row)
    f_result = piotroski_f_score(financials)
    f_score = f_result["f_score"]

    date_str = feature_date_str or as_of.date().isoformat()
    panel = read_feature_day(date_str)
    is_cheap: Optional[bool] = None
    if panel is not None:
        rows = panel[panel["ticker"] == ticker]
        if not rows.empty:
            row = rows.iloc[0]
            ev_z = row.get("ev_ebit_yield")
            bm_z = row.get("book_to_market")
            candidates = [z for z in (ev_z, bm_z) if z is not None and not pd.isna(z)]
            if candidates:
                is_cheap = bool(max(candidates) >= CHEAP_ZSCORE_THRESHOLD)

    passes = bool(
        not np.isnan(f_score) and f_score >= PIOTROSKI_STRONG_GATE and is_cheap is True
    )
    return {"f_score": f_score, "is_cheap": is_cheap, "passes": passes}
