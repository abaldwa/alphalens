"""
systems/fundamental_analysis/quality/net_net.py

Graham Net-Net / Asset Value: buy below net current asset value. Compares
an absolute rupee-per-share value to price, so — like margin_of_safety.py
and piotroski_on_value.py — reads raw PIT financials directly rather than
the sector-z-scored feature panel.

preferred_equity has no column anywhere (see margin_of_safety.py's
docstring) and is treated as 0. The doc's promoter_pledge_ratio==0
governance overlay is intentionally NOT included here — per this
project's Promoter-Aligned Compounders design, governance fields are an
overlay to be combined by the caller, not baked into a standalone
value screen.
"""

from datetime import datetime
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from datastore.api.pit import get_fundamentals_pit

NCAV_DISCOUNT_THRESHOLD = 0.67  # price must be <= 67% of NCAV/share (Graham's classic cutoff)
# Arbitrary but documented liquidity floor — net-nets are rare enough in
# India that without some floor, illiquid micro-caps dominate the screen.
LIQUIDITY_FLOOR_MARKET_CAP_CR = 50.0
CRORE = 1e7


def _latest_close(conn: Any, ticker: str, as_of: datetime) -> Optional[float]:
    row = conn.execute(
        "SELECT close FROM ohlcv_adjusted WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        [ticker, as_of.date()],
    ).fetchone()
    return float(row[0]) if row else None


def compute_net_net(conn: Any, ticker: str, as_of: datetime) -> Dict[str, Any]:
    """
    Returns
    -------
    dict
        ncav, ncav_per_share, market_cap_cr, passes (bool). Missing data
        conservatively fails.
    """
    history = get_fundamentals_pit(conn, [ticker], as_of)
    if history.empty:
        return {"ncav_per_share": np.nan, "passes": False}

    latest = history.sort_values("quarter_end_date").iloc[-1]
    close = _latest_close(conn, ticker, as_of)

    current_assets = latest.get("current_assets")
    total_liabilities = latest.get("total_liabilities")
    shares = latest.get("shares_outstanding")

    if close is None or pd.isna(current_assets) or pd.isna(total_liabilities) or pd.isna(shares) or shares <= 0:
        return {"ncav_per_share": np.nan, "passes": False}

    ncav = float(current_assets - total_liabilities)  # preferred_equity == 0, in rupee CRORE
    # Unit conversion (same convention as features/fundamental.py's module
    # docstring): fundamentals columns are rupee CRORE, shares_outstanding
    # is a raw share count, close is raw rupees-per-share — ncav must be
    # converted to raw rupees before dividing by a raw share count.
    ncav_per_share = (ncav * CRORE) / shares
    market_cap_cr = (close * shares) / CRORE

    passes = bool(
        ncav_per_share > 0
        and close <= NCAV_DISCOUNT_THRESHOLD * ncav_per_share
        and market_cap_cr > LIQUIDITY_FLOOR_MARKET_CAP_CR
    )
    return {"ncav": ncav, "ncav_per_share": ncav_per_share, "market_cap_cr": market_cap_cr, "passes": passes}
