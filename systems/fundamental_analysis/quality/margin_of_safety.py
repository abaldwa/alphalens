"""
systems/fundamental_analysis/quality/margin_of_safety.py

Graham/Klarman-style Margin of Safety Value: estimate conservative
intrinsic value two ways (Graham Number, Graham Value), take the lower of
the two, then require the current price to sit at a meaningful discount
plus a solvency gate. Compares absolute rupee values to price, so — like
piotroski_on_value.py — it reads raw PIT financials directly rather than
the sector-z-scored feature panel other strategies use.

preferred_equity has no column anywhere in this codebase (confirmed during
planning) and is treated as 0 — standard practice, since Indian preference
shares are rare; documented here rather than silently omitted.
"""

from datetime import datetime
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from datastore.api.pit import get_fundamentals_pit

MARGIN_OF_SAFETY_THRESHOLD = 0.30
SOLVENCY_MAX_DEBT_TO_EQUITY = 0.7
SOLVENCY_MIN_INTEREST_COVERAGE = 3.0


def _latest_close(conn: Any, ticker: str, as_of: datetime) -> Optional[float]:
    row = conn.execute(
        "SELECT close FROM ohlcv_adjusted WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        [ticker, as_of.date()],
    ).fetchone()
    return float(row[0]) if row else None


def compute_margin_of_safety(conn: Any, ticker: str, as_of: datetime) -> Dict[str, Any]:
    """
    Returns
    -------
    dict
        graham_number, graham_value, intrinsic_value (min of the two,
        None if both unavailable), margin_of_safety, passes (bool).
        Missing data conservatively fails, matching matches_screener_preset().
    """
    history = get_fundamentals_pit(conn, [ticker], as_of)
    if history.empty:
        return {"intrinsic_value": None, "margin_of_safety": np.nan, "passes": False}

    latest = history.sort_values("quarter_end_date").iloc[-1]
    close = _latest_close(conn, ticker, as_of)

    eps = latest.get("eps")
    bvps = latest.get("book_value_per_share")
    debt_to_equity = latest.get("debt_to_equity")
    interest_coverage = latest.get("interest_coverage")
    # Approximation, not an identity — see features/fundamental.py's
    # cfo_proxy comment: `fcf` is a raw source-reported value, not derived
    # here as cfo - capex, so this can diverge from true operating cash flow.
    fcf, capex = latest.get("fcf"), latest.get("capex")
    cfo_proxy = (fcf + capex) if pd.notna(fcf) and pd.notna(capex) else np.nan

    if close is None or pd.isna(eps) or pd.isna(bvps) or eps <= 0 or bvps <= 0:
        return {"intrinsic_value": None, "margin_of_safety": np.nan, "passes": False}

    graham_number = float(np.sqrt(22.5 * eps * bvps))
    # No separate "expected_growth_percent" field exists — eps_growth_yoy's
    # underlying raw eps values are already in `latest`/history, but this
    # module intentionally stays single-quarter (no yoy lookup) to keep the
    # same shape as piotroski_on_value.py's raw-financials pattern; use a
    # conservative 0% growth assumption for Graham Value when no explicit
    # growth estimate is supplied, which only makes this leg more conservative.
    graham_value = float(eps * 8.5)
    intrinsic_value = min(graham_number, graham_value)

    margin_of_safety = (intrinsic_value - close) / intrinsic_value if intrinsic_value > 0 else np.nan

    solvent = (
        pd.notna(debt_to_equity) and debt_to_equity < SOLVENCY_MAX_DEBT_TO_EQUITY
        and pd.notna(interest_coverage) and interest_coverage > SOLVENCY_MIN_INTEREST_COVERAGE
        and pd.notna(cfo_proxy) and cfo_proxy > 0
    )
    passes = bool(
        pd.notna(margin_of_safety) and margin_of_safety >= MARGIN_OF_SAFETY_THRESHOLD and solvent
    )
    return {
        "graham_number": graham_number,
        "graham_value": graham_value,
        "intrinsic_value": intrinsic_value,
        "margin_of_safety": margin_of_safety,
        "passes": passes,
    }
