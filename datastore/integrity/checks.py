"""
datastore/integrity/checks.py

Phase: A20 (Data Integrity Checker)
Specs: FeatureBacklog.md A20
Owner: Data Layer / Ops / Scheduler
Consumers: datastore/integrity/runner.py

The four A20 checks. Each takes a connection + as_of_date and returns a
list of datastore.integrity.findings.Finding — no DB writes happen inside
a check itself; the runner is the single place findings get inserted, so
tests can assert on returned Finding objects without touching a real
table.

a. check_corporate_actions — reuses the exact jump-detection /
   factor-classification method already proven out in
   scripts/detect_missing_split_reconstruction.py (CA1's triage script),
   applied to corporate_actions rows actioned in the trailing window.
b. check_null_sweep — null/NaN rate per column vs. a baseline, skipping
   columns already known to be structurally sparse
   (ingestion.scheduler.daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS).
c. check_holiday_leakage — cross-references config.nse_holidays against
   ohlcv_adjusted rows and feature Parquet partition filenames.
d. check_spot_check — samples random (ticker, date) pairs across 5 years,
   cross-checks against two independent sources (Fyers + Yahoo Finance);
   only flags when both disagree with us, since a single-source mismatch
   is that source's own data-quality issue, not necessarily ours.
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from datetime import timedelta
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from config.nse_holidays import is_nse_holiday
from datastore.integrity.findings import Finding

logger = logging.getLogger(__name__)

# Same candidate split/bonus factors and tolerance as
# scripts/detect_missing_split_reconstruction.py — imported, not
# redefined, so the two stay in lockstep.
from scripts.detect_missing_split_reconstruction import (  # noqa: E402
    TOLERANCE_PCT,
    classify_factor,
)


def check_corporate_actions(
    conn,
    as_of_date: date_type,
    lookback_days: int = 7,
    fyers_client=None,
) -> List[Finding]:
    """
    For every corporate_actions row actioned (ex_date) in the trailing
    `lookback_days`, re-pull the same ticker/date window from Fyers and
    check whether the announced ratio is actually reflected in
    ohlcv_adjusted's price series (same jump-detection method as
    scripts/detect_missing_split_reconstruction.py). A mismatch between
    the announced ratio and the implied price-jump factor is flagged —
    it means the CA row is wrong, or was never applied by adjust_prices.

    `fyers_client` is injectable for tests; defaults to a real
    FYERSBackfill() instance — but only if a valid same-day token is
    already cached. This runs unattended from the daily pipeline's
    sanity_check step (no connected stdin), so it must never fall
    through to FYERSBackfill.get_access_token()'s interactive OAuth2
    flow — same guard as pipeline_scheduler.py's backfill_catchup job.
    2026-07-30: the cached-token pre-check below is not sufficient on its
    own — get_access_token() re-derives the token from scratch on every
    call (its own cache lookup + validation), so a transient validation
    blip after this pre-check passed could still fall through to the
    interactive input()-based flow and hang forever. non_interactive=True
    is the hard backstop: it makes get_access_token() raise instead of
    blocking, on every call, not just the first.
    """
    if fyers_client is None:
        from ingestion.scrapers.fyers_backfill import FYERSBackfill

        fb = FYERSBackfill(non_interactive=True)
        cached_token = fb._load_cached_token()
        if not cached_token or not fb._validate_token(cached_token):
            logger.warning(
                "check_corporate_actions: no valid (same-day) FYERS token cached — "
                "skipping Fyers cross-check for this run."
            )
            return []
        fyers_client = fb

    window_start = as_of_date - timedelta(days=lookback_days)
    actions = conn.execute(
        """
        SELECT ticker, ex_date, action_type, ratio
        FROM corporate_actions
        WHERE ex_date BETWEEN ? AND ?
        """,
        [window_start, as_of_date],
    ).df()

    findings: List[Finding] = []
    for row in actions.itertuples():
        ticker, ex_date, action_type, ratio = row.ticker, row.ex_date, row.action_type, row.ratio
        if action_type not in ("SPLIT", "BONUS"):
            # DIVIDEND/RIGHTS don't produce a clean multiplicative price
            # jump the same way — out of scope for this jump-detection method.
            continue

        window_start_px = pd.Timestamp(ex_date) - pd.Timedelta(days=30)
        window_end_px = pd.Timestamp(ex_date) + pd.Timedelta(days=30)
        ours = conn.execute(
            "SELECT date, close FROM ohlcv_adjusted WHERE ticker=? AND date BETWEEN ? AND ? ORDER BY date",
            [ticker, window_start_px.date(), window_end_px.date()],
        ).df()
        if len(ours) < 10:
            continue

        try:
            hist = fyers_client.download_history(
                ticker, window_start_px.strftime("%Y-%m-%d"), window_end_px.strftime("%Y-%m-%d")
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("check_corporate_actions: fyers fetch failed for %s: %s", ticker, exc)
            continue
        if hist is None or hist.empty:
            continue

        hist["date"] = pd.to_datetime(hist["date"])
        ours["date"] = pd.to_datetime(ours["date"])
        merged = pd.merge(ours, hist[["date", "close"]], on="date", suffixes=("_ours", "_fyers"))
        merged = merged[merged["close_fyers"] > 0].sort_values("date").reset_index(drop=True)
        if len(merged) < 10:
            continue

        merged["ratio"] = merged["close_ours"] / merged["close_fyers"]
        log_ratio = np.log(merged["ratio"])
        diffs = log_ratio.diff().abs()
        jump_idx = diffs.idxmax()
        if pd.isna(jump_idx) or jump_idx == 0:
            # No jump found in our series around the announced ex_date —
            # the corporate action may never have been applied.
            findings.append(
                Finding(
                    check_name="corporate_actions",
                    finding_date=as_of_date,
                    severity="warning",
                    description=(
                        f"{ticker}: no price jump found around announced {action_type} "
                        f"ex_date={ex_date} (ratio={ratio}) — action may not have been applied"
                    ),
                    ticker=ticker,
                    evidence={"ex_date": str(ex_date), "action_type": action_type, "ratio": ratio},
                )
            )
            continue

        before = merged.loc[: jump_idx - 1]
        after = merged.loc[jump_idx:]
        if len(before) < 5 or len(after) < 5:
            continue
        ratio_before = before["ratio"].median()
        ratio_after = after["ratio"].median()
        implied_factor = ratio_after / ratio_before
        pct_diff, matched_type, matched_ratio, _ = classify_factor(implied_factor)

        if matched_type != action_type or pct_diff > TOLERANCE_PCT:
            findings.append(
                Finding(
                    check_name="corporate_actions",
                    finding_date=as_of_date,
                    severity="critical",
                    description=(
                        f"{ticker}: announced {action_type} ratio={ratio} at ex_date={ex_date} "
                        f"but implied price factor matches {matched_type} ratio={matched_ratio} "
                        f"(diff={pct_diff:.2f}%)"
                    ),
                    ticker=ticker,
                    evidence={
                        "ex_date": str(ex_date),
                        "announced_action_type": action_type,
                        "announced_ratio": ratio,
                        "implied_factor": implied_factor,
                        "matched_action_type": matched_type,
                        "matched_ratio": matched_ratio,
                        "pct_diff": pct_diff,
                    },
                )
            )

    return findings


_NULL_SWEEP_TABLES = ["ohlcv_adjusted", "fundamentals", "macro_indicators"]
# Baseline null-rate tolerance for columns not otherwise known-sparse.
# Anything above this is flagged as a warning; a column that's 100% NaN
# and NOT in the known-sparse exemption list is critical.
_NULL_SWEEP_WARN_THRESHOLD = 0.10


def check_null_sweep(conn, as_of_date: date_type) -> List[Finding]:
    """
    Per-column null/NaN rate sweep over source tables (only rows for
    as_of_date, to keep this a bounded daily check rather than a full-
    history scan) plus that day's feature Parquet, skipping columns
    already known to be structurally sparse
    (_SANITY_KNOWN_SPARSE_COLUMNS — same list step_sanity_check uses, so
    A20 never re-alerts on already-accepted gaps).
    """
    from ingestion.scheduler.daily_pipeline import _SANITY_KNOWN_SPARSE_COLUMNS

    findings: List[Finding] = []
    date_col_by_table = {"ohlcv_adjusted": "date", "fundamentals": "announcement_date", "macro_indicators": "date"}

    for table in _NULL_SWEEP_TABLES:
        date_col = date_col_by_table[table]
        try:
            df = conn.execute(f"SELECT * FROM {table} WHERE {date_col} = ?", [as_of_date]).df()
        except Exception as exc:  # noqa: BLE001
            logger.warning("check_null_sweep: could not read %s: %s", table, exc)
            continue
        if df.empty:
            continue
        findings.extend(_null_rate_findings(df, table, as_of_date, _SANITY_KNOWN_SPARSE_COLUMNS))

    feature_df = _load_feature_parquet(as_of_date)
    if feature_df is not None and not feature_df.empty:
        findings.extend(_null_rate_findings(feature_df, "features", as_of_date, _SANITY_KNOWN_SPARSE_COLUMNS))

    return findings


def _null_rate_findings(df: pd.DataFrame, source: str, as_of_date: date_type, known_sparse: set) -> List[Finding]:
    findings: List[Finding] = []
    n = len(df)
    for col in df.columns:
        if col in known_sparse:
            continue
        null_rate = df[col].isna().mean()
        if null_rate >= 1.0:
            findings.append(
                Finding(
                    check_name="null_sweep",
                    finding_date=as_of_date,
                    severity="critical",
                    description=f"{source}.{col} is 100% NaN for {as_of_date} ({n} rows) and not in the known-sparse exemption list",
                    evidence={"source": source, "column": col, "null_rate": null_rate, "row_count": n},
                )
            )
        elif null_rate >= _NULL_SWEEP_WARN_THRESHOLD:
            findings.append(
                Finding(
                    check_name="null_sweep",
                    finding_date=as_of_date,
                    severity="warning",
                    description=f"{source}.{col} null rate {null_rate:.1%} for {as_of_date} exceeds {_NULL_SWEEP_WARN_THRESHOLD:.0%} baseline",
                    evidence={"source": source, "column": col, "null_rate": null_rate, "row_count": n},
                )
            )
    return findings


def _load_feature_parquet(as_of_date: date_type) -> Optional[pd.DataFrame]:
    from config.settings import FEATURES_DAILY_DIR

    path = Path(FEATURES_DAILY_DIR) / f"{as_of_date.isoformat()}.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


def check_holiday_leakage(conn, as_of_date: date_type, lookback_days: int = 30) -> List[Finding]:
    """
    Cross-reference config.nse_holidays.is_nse_holiday against
    ohlcv_adjusted dates and written feature Parquet partition filenames
    over the trailing `lookback_days` — any row/file dated on a real NSE
    holiday is the same failure mode T2 already found/fixed at the
    scraper layer; this is the recurring detection net for it.
    """
    from config.settings import FEATURES_DAILY_DIR

    findings: List[Finding] = []
    window_start = as_of_date - timedelta(days=lookback_days)

    ohlcv_dates = conn.execute(
        "SELECT DISTINCT date FROM ohlcv_adjusted WHERE date BETWEEN ? AND ?",
        [window_start, as_of_date],
    ).df()
    for d in ohlcv_dates["date"]:
        d_date = pd.Timestamp(d).date()
        if is_nse_holiday(d_date):
            findings.append(
                Finding(
                    check_name="holiday_leakage",
                    finding_date=as_of_date,
                    severity="critical",
                    description=f"ohlcv_adjusted has rows dated {d_date}, which is a known NSE holiday",
                    evidence={"leaked_date": str(d_date), "source": "ohlcv_adjusted"},
                )
            )

    features_dir = Path(FEATURES_DAILY_DIR)
    if features_dir.is_dir():
        for f in features_dir.glob("*.parquet"):
            try:
                f_date = date_type.fromisoformat(f.stem)
            except ValueError:
                continue
            if window_start <= f_date <= as_of_date and is_nse_holiday(f_date):
                findings.append(
                    Finding(
                        check_name="holiday_leakage",
                        finding_date=as_of_date,
                        severity="critical",
                        description=f"feature Parquet partition {f.name} is dated {f_date}, which is a known NSE holiday",
                        evidence={"leaked_date": str(f_date), "source": "feature_parquet", "file": str(f)},
                    )
                )

    return findings


def check_spot_check(
    conn,
    as_of_date: date_type,
    sample_size: int = 100,
    lookback_years: int = 5,
    seed: Optional[int] = None,
    fyers_client=None,
    yahoo_fetch=None,
) -> List[Finding]:
    """
    Sample `sample_size` random (ticker, date) pairs across the trailing
    `lookback_years` of ohlcv_adjusted, cross-check adjusted close
    against two independent sources (Fyers + Yahoo Finance). A mismatch
    is only flagged when BOTH independent sources disagree with us and
    agree with each other — a single-source disagreement is that
    source's own data-quality issue, not necessarily a bug in our data.

    `fyers_client`/`yahoo_fetch` are injectable for tests.
    `yahoo_fetch(ticker, date) -> Optional[float]` defaults to a thin
    yfinance-based lookup.

    Same unattended-safety guard as check_corporate_actions: only
    constructs a live FYERSBackfill() if a valid same-day token is
    already cached, otherwise this check runs Yahoo-only (never falls
    through to the interactive OAuth2 flow). non_interactive=True is the
    hard backstop against a re-derivation mid-run falling through to
    input() — see check_corporate_actions's docstring for why the
    pre-check alone isn't sufficient.
    """
    if fyers_client is None:
        from ingestion.scrapers.fyers_backfill import FYERSBackfill

        fb = FYERSBackfill(non_interactive=True)
        cached_token = fb._load_cached_token()
        if cached_token and fb._validate_token(cached_token):
            fyers_client = fb
        else:
            logger.warning(
                "check_spot_check: no valid (same-day) FYERS token cached — "
                "running Yahoo-only cross-check for this run."
            )
    if yahoo_fetch is None:
        yahoo_fetch = _yahoo_close

    window_start = as_of_date - timedelta(days=365 * lookback_years)
    universe = conn.execute(
        "SELECT ticker, date, close FROM ohlcv_adjusted WHERE date BETWEEN ? AND ?",
        [window_start, as_of_date],
    ).df()
    if universe.empty:
        return []

    sample = universe.sample(n=min(sample_size, len(universe)), random_state=seed)

    findings: List[Finding] = []
    for row in sample.itertuples():
        ticker, d, our_close = row.ticker, pd.Timestamp(row.date).date(), row.close
        date_str = d.isoformat()

        if fyers_client is None:
            fy_close = None
        else:
            try:
                fy_hist = fyers_client.download_history(ticker, date_str, date_str)
                fy_close = float(fy_hist["close"].iloc[0]) if fy_hist is not None and not fy_hist.empty else None
            except Exception:  # noqa: BLE001
                fy_close = None

        try:
            yahoo_close = yahoo_fetch(ticker, d)
        except Exception:  # noqa: BLE001
            yahoo_close = None

        if fy_close is None or yahoo_close is None or our_close in (None, 0):
            continue

        fy_mismatch = abs(our_close - fy_close) / our_close > TOLERANCE_PCT / 100
        yahoo_mismatch = abs(our_close - yahoo_close) / our_close > TOLERANCE_PCT / 100
        sources_agree = abs(fy_close - yahoo_close) / max(fy_close, 1e-9) <= TOLERANCE_PCT / 100

        if fy_mismatch and yahoo_mismatch and sources_agree:
            findings.append(
                Finding(
                    check_name="spot_check",
                    finding_date=as_of_date,
                    severity="critical",
                    description=(
                        f"{ticker}@{d}: our close={our_close} disagrees with BOTH Fyers "
                        f"({fy_close}) and Yahoo ({yahoo_close}), which agree with each other"
                    ),
                    ticker=ticker,
                    evidence={
                        "date": date_str,
                        "our_close": our_close,
                        "fyers_close": fy_close,
                        "yahoo_close": yahoo_close,
                    },
                )
            )

    return findings


# ETFs and similar instruments legitimately never have SPLIT/BONUS/
# DIVIDEND/AGM/RIGHTS rows the way an operating company does — exempted
# so check_corporate_actions_coverage doesn't re-flag them every day,
# same idea as check_null_sweep's _SANITY_KNOWN_SPARSE_COLUMNS exemption.
_KNOWN_ACTION_FREE_TICKERS = {
    "NIFTYBEES", "NIF100BEES", "GOLDSHARE", "UTINIFTETF", "UTISENSETF",
}


def check_corporate_actions_coverage(
    conn,
    as_of_date: date_type,
    min_trading_days: int = 500,
    lookback_years: int = 10,
) -> List[Finding]:
    """
    For every ticker with >= `min_trading_days` rows in ohlcv_adjusted
    within the trailing `lookback_years`, flag it if it has zero
    corporate_actions rows in that same window — a real operating company
    traded for that long should have at least one AGM/DIVIDEND on record;
    zero is a strong signal of either a genuine ingestion gap or a
    ticker-identity split (see evidence["likely_renamed_to"] below).

    2026-07-30 (A20 follow-up): found via a manual audit that 279
    actively-traded tickers had zero corporate_actions rows despite a
    full 10-year backfill — including major names like TATAMOTORS, whose
    actions turned out to be filed entirely under a different ticker
    (TMPV) after a 2025 demerger NSE's API retroactively relabels the
    whole history under. This check makes that class of gap visible daily
    instead of requiring another one-off audit.

    Always severity='warning' — a zero-coverage ticker needs human
    classification (rename vs. genuine API gap vs. legitimately
    action-free instrument) that this check cannot make on its own, so it
    must never fail the pipeline step, only surface for review.

    evidence["likely_renamed_to"] is populated when another ticker's
    first ohlcv_adjusted date falls within 5 days of this ticker's last
    date — the same signature TATAMOTORS->TMPV showed (TATAMOTORS's last
    row 2025-10-23, TMPV's series continuing through today).
    """
    window_start = as_of_date - timedelta(days=365 * lookback_years)

    rows = conn.execute(
        """
        WITH active AS (
            SELECT ticker, count(*) AS n_days, min(date) AS first_date, max(date) AS last_date
            FROM ohlcv_adjusted
            WHERE date BETWEEN ? AND ?
            GROUP BY ticker
            HAVING count(*) >= ?
        ),
        has_ca AS (
            SELECT DISTINCT ticker FROM corporate_actions WHERE ex_date BETWEEN ? AND ?
        ),
        missing AS (
            SELECT a.ticker, a.n_days, a.first_date, a.last_date
            FROM active a
            LEFT JOIN has_ca c ON a.ticker = c.ticker
            WHERE c.ticker IS NULL
        )
        SELECT
            m.ticker, m.n_days, m.first_date, m.last_date,
            (
                SELECT b.ticker FROM active b
                WHERE b.ticker != m.ticker
                  AND b.first_date BETWEEN m.last_date AND m.last_date + INTERVAL 5 DAY
                ORDER BY b.first_date
                LIMIT 1
            ) AS likely_renamed_to
        FROM missing m
        ORDER BY m.ticker
        """,
        [window_start, as_of_date, min_trading_days, window_start, as_of_date],
    ).fetchall()

    findings: List[Finding] = []
    for ticker, n_days, first_date, last_date, likely_renamed_to in rows:
        if ticker in _KNOWN_ACTION_FREE_TICKERS:
            continue
        evidence = {
            "trading_days": int(n_days),
            "first_date": str(first_date),
            "last_date": str(last_date),
        }
        rename_note = ""
        if likely_renamed_to:
            evidence["likely_renamed_to"] = likely_renamed_to
            rename_note = f" — possible rename to {likely_renamed_to}"
        findings.append(
            Finding(
                check_name="corporate_actions_coverage",
                finding_date=as_of_date,
                severity="warning",
                description=(
                    f"{ticker}: {n_days} trading days in the trailing {lookback_years}y "
                    f"({first_date}..{last_date}) but zero corporate_actions rows in that window"
                    f"{rename_note}"
                ),
                ticker=ticker,
                evidence=evidence,
            )
        )

    return findings


def _yahoo_close(ticker: str, d: date_type) -> Optional[float]:
    """Thin Yahoo Finance lookup via yfinance, for check_spot_check's second independent source."""
    import yfinance as yf

    yf_ticker = f"{ticker}.NS"
    hist = yf.Ticker(yf_ticker).history(start=d.isoformat(), end=(d + timedelta(days=1)).isoformat())
    if hist is None or hist.empty:
        return None
    return float(hist["Close"].iloc[0])
