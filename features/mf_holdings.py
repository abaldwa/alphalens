"""
features/mf_holdings.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-004, SPEC-DS-002
Owner: Platform / Features
Consumers: features/matrix_builder (wired in P2.3)

Computes the 12 MF-holding features named in the P2.2 build prompt
(matches 01_features.md's "MF Portfolio Holdings (12)" list exactly).

Reads directly from `datastore/normalised/mf_holdings/YYYY-MM.parquet`
(ingestion/scrapers/amfi_holdings.py) rather than via the DataStore API —
same SPEC-DS-002 exception already established for features/
macro_features.py's `load_macro_indicators()` ("direct reads permitted
within ingestion and feature layers" when no API endpoint exists yet for
that store). No API endpoint exists for MF holdings; building one was not
part of this prompt's explicit deliverable list.

[AS BUILT] `superstar_investor_flag`/`superstar_investor_change` need a
"which ace investors hold this stock" data source — the P2.2 build prompt
says "tracked via Trendlyne", but no Trendlyne subscription/scraper
exists yet (P2.6 prerequisite per CLAUDE_CODE_PROMPTS.md's Phase 2
header). Both functions accept an optional `superstar_holdings`
DataFrame (ticker, investor_name, as_of_date, holding_pct) — when not
supplied, both features are computed as NaN/0 (not fabricated), and the
real Trendlyne integration plugs in by passing this same parameter once
it exists, with zero changes needed here (SPEC-SOLID-005, Dependency
Inversion — this module depends on the data shape, not on Trendlyne).

PIT Assumptions
----------------
SPEC-PIPE-003 (CRITICAL): every row consumed here has already been
PIT-filtered by `load_mf_holdings_history()` on `availability_date <=
as_of` (never on `month` directly — the same announcement_date-not-
quarter_end_date discipline as P2.1's fundamentals/governance).
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import numpy as np
import pandas as pd

from config.settings import MF_HOLDINGS_DIR

logger = logging.getLogger(__name__)

MF_HOLDINGS_FEATURES: List[str] = [
    "mf_scheme_count",
    "mf_scheme_count_change_1m",
    "mf_total_holding_change_1m",
    "mf_smallcap_fund_holding",
    "mf_new_entry_count",
    "mf_exit_count",
    "mf_concentration_top5",
    "mf_avg_holding_period",
    "mf_sip_inflow_proxy",
    "mf_crowdedness_rank",
    "superstar_investor_flag",
    "superstar_investor_change",
]

# Real Indian AMC scheme names use both spellings interchangeably (e.g.
# "ICICI Prudential Smallcap Fund" vs "Kotak Small Cap Fund") — matched
# as a regex so "small cap"/"small-cap"/"smallcap" all hit.
_SMALLCAP_NAME_PATTERN = r"small\s*-?\s*cap"


def load_mf_holdings_history(
    as_of: datetime,
    lookback_months: int = 13,
    holdings_dir: Path = MF_HOLDINGS_DIR,
) -> pd.DataFrame:
    """
    Load every PIT-eligible monthly Parquet file within the lookback window.

    Parameters
    ----------
    as_of : datetime
        PIT reference date.
    lookback_months : int
        How many calendar months of files to consider opening — bounds
        I/O; the real PIT gate is the per-row `availability_date` filter
        applied after loading, not this window.
    holdings_dir : Path, optional
        Defaults to config.settings.MF_HOLDINGS_DIR.

    Returns
    -------
    pd.DataFrame
        Concatenated rows from every file whose name parses as 'YYYY-MM'
        within the lookback window, filtered to `availability_date <=
        as_of`. Empty DataFrame (not an error) if the directory doesn't
        exist or no files qualify.

    Spec References
    ----------------
    SPEC-PIPE-003 (CRITICAL): availability_date, never `month`, is the
    PIT gate applied here.

    Raises
    ------
    None
    """
    empty_columns = ["scheme_name", "isin", "ticker", "quantity", "value_inr", "month", "availability_date"]
    if not holdings_dir.exists():
        return pd.DataFrame(columns=empty_columns)

    cutoff = as_of - pd.DateOffset(months=lookback_months)
    frames = []
    for path in sorted(holdings_dir.glob("*.parquet")):
        try:
            file_month = pd.Period(path.stem, freq="M").to_timestamp()
        except ValueError:
            continue
        if file_month < cutoff:
            continue
        df = pd.read_parquet(path)
        df["availability_date"] = pd.to_datetime(df["availability_date"])
        frames.append(df[df["availability_date"] <= as_of])

    if not frames:
        return pd.DataFrame(columns=empty_columns)
    return pd.concat(frames, ignore_index=True)


def _latest_two_months(ticker_history: pd.DataFrame) -> Optional[List[str]]:
    months = sorted(ticker_history["month"].unique())
    return months[-2:] if months else None


def compute_mf_holdings_features(
    ticker: str,
    as_of: datetime,
    history: pd.DataFrame,
    superstar_holdings: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Compute all 12 MF-holding features for one ticker from already-PIT-filtered history.

    Parameters
    ----------
    ticker : str
    as_of : datetime
        PIT reference date.
    history : pd.DataFrame
        Output of load_mf_holdings_history() — already PIT-filtered,
        covering many tickers (this function filters to `ticker` itself).
    superstar_holdings : pd.DataFrame, optional
        Columns: ticker, investor_name, as_of_date, holding_pct. See
        module docstring — None if Trendlyne integration doesn't exist yet.

    Returns
    -------
    dict
        feature_name -> value for all 12 MF_HOLDINGS_FEATURES.
        `mf_crowdedness_rank` is always NaN here (a cross-sectional
        ranking — see compute_mf_holdings_features_panel).

    Spec References
    ----------------
    SPEC-PIPE-003 (CRITICAL).

    Raises
    ------
    None — missing/insufficient history degrades to NaN/0, not an exception.
    """
    ticker_history = history[history["ticker"] == ticker]
    return _compute_mf_holdings_features_from_group(ticker, as_of, ticker_history, history.empty, superstar_holdings)


def _compute_mf_holdings_features_from_group(
    ticker: str,
    as_of: datetime,
    ticker_history: pd.DataFrame,
    history_is_empty: bool,
    superstar_holdings: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Shared computation body for one ticker, given its already-sliced
    history sub-frame (`ticker_history`). Factored out of
    `compute_mf_holdings_features` so `compute_mf_holdings_features_panel_vectorized`
    can pass in a pre-grouped sub-frame (via one `history.groupby("ticker")`
    pass over the whole panel) instead of re-filtering the full
    multi-ticker `history` DataFrame with a boolean mask once per ticker
    (the O(n_tickers x n_rows) hotspot in the sequential per-ticker loop).
    Output is bit-for-bit identical either way — same logic, same inputs.
    """
    months = _latest_two_months(ticker_history)

    if not months:
        if history_is_empty:
            # No MF holdings data loaded for ANY ticker this period — a
            # genuinely unknown state (e.g. no AMC source registered yet),
            # not a confirmed zero. Every feature is honestly NaN.
            result = {f: np.nan for f in MF_HOLDINGS_FEATURES}
        else:
            # Data exists for this period, but zero schemes hold THIS
            # ticker — a known fact, not missing data. Count-style
            # features are correctly 0; ratio/period features that are
            # undefined over an empty set (e.g. "concentration of the top
            # 5 holders of nothing") stay NaN.
            result = {f: np.nan for f in MF_HOLDINGS_FEATURES}
            result["mf_scheme_count"] = 0
            result["mf_scheme_count_change_1m"] = 0
            result["mf_smallcap_fund_holding"] = 0.0
        result["mf_new_entry_count"] = 0
        result["mf_exit_count"] = 0
        result["superstar_investor_flag"] = 0
        result["superstar_investor_change"] = 0
        return result

    latest_month = months[-1]
    prior_month = months[-2] if len(months) == 2 else None

    latest = ticker_history[ticker_history["month"] == latest_month]
    prior = ticker_history[ticker_history["month"] == prior_month] if prior_month else ticker_history.iloc[0:0]

    latest_schemes = set(latest["scheme_name"])
    prior_schemes = set(prior["scheme_name"])

    mf_scheme_count = len(latest_schemes)
    mf_scheme_count_change_1m = (mf_scheme_count - len(prior_schemes)) if prior_month else np.nan

    latest_total = latest["value_inr"].sum()
    prior_total = prior["value_inr"].sum() if prior_month else np.nan
    mf_total_holding_change_1m = (
        (latest_total - prior_total) / prior_total if prior_month and prior_total else np.nan
    )

    mf_smallcap_fund_holding = latest[
        latest["scheme_name"].str.lower().str.contains(_SMALLCAP_NAME_PATTERN, na=False)
    ]["value_inr"].sum()

    mf_new_entry_count = len(latest_schemes - prior_schemes) if prior_month else len(latest_schemes)
    mf_exit_count = len(prior_schemes - latest_schemes) if prior_month else 0

    top5_value = latest.nlargest(5, "value_inr")["value_inr"].sum() if len(latest) else 0.0
    mf_concentration_top5 = (top5_value / latest_total) if latest_total else np.nan

    # mf_avg_holding_period: for each scheme currently holding, count
    # consecutive trailing months (within the loaded history window) it
    # has held this ticker without a gap, then average across schemes —
    # a "sticky vs speculative money" proxy (01_features.md), not a
    # precise inception-to-date holding duration (that would need the
    # scheme's full history back to its first-ever disclosure, beyond
    # what a bounded lookback window can see).
    all_months_sorted = sorted(ticker_history["month"].unique())
    scheme_months: Dict[str, Set[str]] = {}
    for m in all_months_sorted:
        for s in ticker_history[ticker_history["month"] == m]["scheme_name"]:
            scheme_months.setdefault(s, set()).add(m)
    holding_periods = []
    for scheme in latest_schemes:
        held = scheme_months.get(scheme, set())
        streak = 0
        for m in reversed(all_months_sorted):
            if m in held:
                streak += 1
            else:
                break
        holding_periods.append(streak)
    mf_avg_holding_period = float(np.mean(holding_periods)) if holding_periods else np.nan

    # mf_sip_inflow_proxy: fraction of observed month-over-month
    # transitions where this ticker's total MF-held quantity rose — a
    # "how often are MFs net-buying" consistency score, not a true SIP
    # flow figure (that would need scheme-level cashflow data this
    # codebase doesn't ingest).
    monthly_qty = ticker_history.groupby("month")["quantity"].sum().reindex(all_months_sorted)
    qty_diffs = monthly_qty.diff().dropna()
    mf_sip_inflow_proxy = float((qty_diffs > 0).mean()) if len(qty_diffs) else np.nan

    superstar_investor_flag = 0
    superstar_investor_change = 0
    if superstar_holdings is not None and len(superstar_holdings):
        sh = superstar_holdings[superstar_holdings["ticker"] == ticker].sort_values("as_of_date")
        sh = sh[pd.to_datetime(sh["as_of_date"]) <= as_of]
        if len(sh):
            latest_pct = sh.iloc[-1]["holding_pct"]
            superstar_investor_flag = int(latest_pct > 0)
            if len(sh) >= 2:
                prior_pct = sh.iloc[-2]["holding_pct"]
                superstar_investor_change = int(np.sign(latest_pct - prior_pct))

    return {
        "mf_scheme_count": mf_scheme_count,
        "mf_scheme_count_change_1m": mf_scheme_count_change_1m,
        "mf_total_holding_change_1m": mf_total_holding_change_1m,
        "mf_smallcap_fund_holding": mf_smallcap_fund_holding,
        "mf_new_entry_count": mf_new_entry_count,
        "mf_exit_count": mf_exit_count,
        "mf_concentration_top5": mf_concentration_top5,
        "mf_avg_holding_period": mf_avg_holding_period,
        "mf_sip_inflow_proxy": mf_sip_inflow_proxy,
        "mf_crowdedness_rank": np.nan,  # cross-sectional — see panel function
        "superstar_investor_flag": superstar_investor_flag,
        "superstar_investor_change": superstar_investor_change,
    }


def compute_mf_holdings_features_panel(
    tickers: List[str],
    as_of: datetime,
    tier_map: Optional[Dict[str, int]] = None,
    superstar_holdings: Optional[pd.DataFrame] = None,
    holdings_dir: Path = MF_HOLDINGS_DIR,
) -> pd.DataFrame:
    """
    Compute the 12-feature MF-holdings panel for many tickers, including
    the cross-sectional `mf_crowdedness_rank` (percentile of mf_scheme_count
    within the same market-cap tier — 01_features.md: "Percentile within
    market-cap tier").

    Parameters
    ----------
    tickers : list of str
    as_of : datetime
    tier_map : dict, optional
        ticker -> tier (e.g. from config.universe.load_universe()).
        Tickers with no tier mapping fall into a single tier-less group
        (still ranked among themselves, not against the whole universe).
    superstar_holdings : pd.DataFrame, optional
        See compute_mf_holdings_features's docstring.
    holdings_dir : Path, optional

    Returns
    -------
    pd.DataFrame
        One row per ticker, columns = ['ticker'] + MF_HOLDINGS_FEATURES.

    Spec References
    ----------------
    SPEC-PIPE-004: loading history once for the whole panel (not
    per-ticker) keeps this vectorized at the I/O level; the per-ticker
    feature loop itself is orchestration over an in-memory DataFrame.
    """
    tier_map = tier_map or {}
    history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)

    records = []
    for ticker in tickers:
        feats = compute_mf_holdings_features(ticker, as_of, history, superstar_holdings)
        feats["ticker"] = ticker
        feats["tier"] = tier_map.get(ticker, -1)
        records.append(feats)

    panel = pd.DataFrame(records)
    panel["mf_crowdedness_rank"] = panel.groupby("tier")["mf_scheme_count"].rank(pct=True)
    return panel[["ticker"] + MF_HOLDINGS_FEATURES]


def compute_mf_holdings_features_panel_vectorized(
    tickers: List[str],
    as_of: datetime,
    tier_map: Optional[Dict[str, int]] = None,
    superstar_holdings: Optional[pd.DataFrame] = None,
    holdings_dir: Path = MF_HOLDINGS_DIR,
) -> pd.DataFrame:
    """
    Alternative to `compute_mf_holdings_features_panel` (and the
    ticker-chunked `compute_mf_holdings_features_panel_chunked`, commit
    07d0122) that groups `history` by ticker ONCE (`history.groupby("ticker")`)
    instead of re-filtering the whole multi-ticker `history` DataFrame with
    a boolean mask (`history[history["ticker"] == ticker]`) inside every
    per-ticker call — the real hotspot in the sequential loop for large
    universes x long lookback windows.

    Per-ticker feature *logic* (scheme-count deltas, concentration,
    avg_holding_period's per-scheme consecutive-month streak,
    sip_inflow_proxy, superstar flags) is intentionally NOT rewritten as a
    single groupby-vectorized expression here — `mf_avg_holding_period` in
    particular is a genuinely sequential per-scheme streak computation
    (see `compute_mf_holdings_features`'s own docstring/comments) that
    resists a clean single-shot vectorization without materially changing
    its semantics; reusing the exact existing per-ticker body against a
    pre-grouped sub-frame keeps output bit-for-bit identical to
    `compute_mf_holdings_features_panel` while removing the repeated
    O(n_tickers x n_rows) filter cost.

    `tier_map` is passed in fresh by the caller every time this function
    is invoked (build_feature_matrix computes it fresh per `as_of` — see
    features/matrix_builder.py — this function does not cache or reuse it
    across dates), matching the existing sequential/chunked functions'
    behavior exactly (no new time-variance regression).

    Spec References
    ----------------
    SPEC-PIPE-004.
    """
    tier_map = tier_map or {}
    history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)
    history_is_empty = history.empty
    empty_ticker_history = history.iloc[0:0]

    grouped = {} if history_is_empty else dict(tuple(history.groupby("ticker", sort=False)))

    records = []
    for ticker in tickers:
        ticker_history = grouped.get(ticker, empty_ticker_history)
        feats = _compute_mf_holdings_features_from_group(
            ticker, as_of, ticker_history, history_is_empty, superstar_holdings
        )
        feats["ticker"] = ticker
        feats["tier"] = tier_map.get(ticker, -1)
        records.append(feats)

    panel = pd.DataFrame(records)
    panel["mf_crowdedness_rank"] = panel.groupby("tier")["mf_scheme_count"].rank(pct=True)
    return panel[["ticker"] + MF_HOLDINGS_FEATURES]


_DII_FLOW_COLUMNS = [
    "ticker", "mf_scheme_count", "mf_scheme_count_change_1m",
    "mf_new_entry_count", "mf_exit_count", "mf_total_holding_change_1m", "dii_flow_signal",
]


def find_dii_entry_exit_signals(
    tickers: List[str],
    as_of: datetime,
    holdings_dir: Path = MF_HOLDINGS_DIR,
) -> pd.DataFrame:
    """
    Screen the universe for stocks where domestic mutual funds are making
    a fresh entry or a full exit, month-over-month.

    "Domestic Institution" flow here means mutual fund scheme holdings
    specifically (this module's data source) — the broader DII category
    (insurance, banks, etc.) is a separate aggregate already available as
    `shareholding.dii_pct` (P2.1), without the scheme-level entry/exit
    detail this function provides.

    Parameters
    ----------
    tickers : list of str
        Universe to screen (e.g. config.universe.get_tickers()).
    as_of : datetime
        PIT reference date.
    holdings_dir : Path, optional

    Returns
    -------
    pd.DataFrame
        One row per ticker with scheme-level flow counts and a
        `dii_flow_signal` label:
          - 'ENTRY': at least one new scheme entered, none exited
          - 'EXIT': at least one scheme exited, none newly entered
          - 'MIXED': both happened (churn within the ticker's holder base)
          - 'NEUTRAL': no scheme-level change this month
        Sorted with the strongest entries first, then the strongest exits.
        Requires >=2 months of PIT-eligible history to be meaningful —
        with only one month ingested, every row is 'ENTRY' (every scheme
        present looks "new" against an empty prior month) by construction,
        not a real signal; see compute_mf_holdings_features's docstring on
        mf_new_entry_count for the same caveat.

    Spec References
    ----------------
    SPEC-PIPE-003 (CRITICAL): inherits compute_mf_holdings_features_panel's PIT filtering.

    Raises
    ------
    None — delegates to compute_mf_holdings_features_panel's own per-ticker error isolation.
    """
    panel = compute_mf_holdings_features_panel(tickers, as_of, holdings_dir=holdings_dir)

    def _classify(row: pd.Series) -> str:
        has_entry = row["mf_new_entry_count"] > 0
        has_exit = row["mf_exit_count"] > 0
        if has_entry and has_exit:
            return "MIXED"
        if has_entry:
            return "ENTRY"
        if has_exit:
            return "EXIT"
        return "NEUTRAL"

    panel["dii_flow_signal"] = panel.apply(_classify, axis=1)
    result = panel[_DII_FLOW_COLUMNS].copy()
    result["_entry_rank"] = result["mf_new_entry_count"].fillna(0)
    result["_exit_rank"] = result["mf_exit_count"].fillna(0)
    result = result.sort_values(by=["_entry_rank", "_exit_rank"], ascending=[False, False])
    return result.drop(columns=["_entry_rank", "_exit_rank"]).reset_index(drop=True)
