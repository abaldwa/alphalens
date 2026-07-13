"""
ingestion/scrapers/amfi_holdings.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-SOLID-002 (Open/Closed), SPEC-MFHOLD-001
Owner: Platform / Ingestion
Consumers: ingestion/scrapers/groww_mf_holdings.py,
           ingestion/scheduler/pipeline_scheduler.py, features/mf_holdings.py

Source-agnostic registry and orchestration for scheme-wise mutual fund
portfolio holdings, written to datastore/normalised/mf_holdings/
YYYY-MM.parquet — one row per (scheme_name, isin, ticker) per month.

This module owns NO scraping logic of its own (SOLID-S — single
responsibility: registry + orchestration only). Real sources register
themselves via `register_amc(name, fetch_fn, parse_fn)`:
  - `ingestion/scrapers/groww_mf_holdings.py` — the sole source,
    covering all 49 AMCs (see SPEC-MFHOLD-001). A secondary SBI-specific
    cross-check source (sbi_mf_holdings.py) existed through P2.2 but was
    retired 2026-07-04 — Groww alone was judged sufficient, see
    FutureDevelopment.md.
`download_monthly_disclosure()` raises a clear RuntimeError if called
with no AMCs registered, rather than silently returning nothing or
fabricating data.

See SPEC-MFHOLD-001 (alphalens_docs/specs/08_specifications.md) for the
full sourcing decision and its history: AMFI does not centrally host this
data (each of ~44-49 AMCs is individually SEBI-mandated to publish its
own monthly disclosure); the original per-AMC-website scraping plan was
superseded by Groww after live verification showed it serves the same
real data, for every AMC, via a single consistent format with no login or
JavaScript required.

PIT Assumptions
----------------
SPEC-PIPE-003: scheme holdings for month M are not publicly known until
~`config.settings.MF_HOLDINGS_AVAILABILITY_DELAY_DAYS` days into month
M+1 (the regulatory disclosure deadline). `availability_date` is stored
on every row as `(month + 1).replace(day=DELAY)` — the conservative,
never-too-early default, same direction-of-safety convention as P2.1's
FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS/SHAREHOLDING_FILING_DELAY_DAYS.
"""

import logging
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pandas as pd

from config.settings import AMFI_FETCH_RATE_LIMIT_SLEEP_SECONDS, MF_HOLDINGS_AVAILABILITY_DELAY_DAYS, MF_HOLDINGS_DIR

logger = logging.getLogger(__name__)

HOLDINGS_COLUMNS = ["scheme_name", "isin", "ticker", "quantity", "value_inr", "month"]


@dataclass
class AMCSource:
    """One registered AMC's fetch + parse pair (SOLID-O: add coverage by registering, not editing)."""

    name: str
    fetch_fn: Callable[[int, int], bytes]  # (year, month) -> raw disclosure file bytes
    parse_fn: Callable[[bytes], pd.DataFrame]  # raw bytes -> DataFrame[scheme_name, isin, ticker, quantity, value_inr]


# Populated by importing a source module (groww_mf_holdings.py requires an
# explicit register_all_amcs() call — see
# module docstring).
AMC_REGISTRY: Dict[str, AMCSource] = {}


def register_amc(name: str, fetch_fn: Callable[[int, int], bytes], parse_fn: Callable[[bytes], pd.DataFrame]) -> None:
    """
    Register one AMC's real fetch + parse implementation.

    Parameters
    ----------
    name : str
        AMC display name (e.g. "SBI Mutual Fund").
    fetch_fn : callable
        (year, month) -> raw disclosure file bytes for that AMC/month.
        Must raise (not return empty) on a genuine fetch failure, so
        download_monthly_disclosure's per-AMC isolation can log it correctly.
    parse_fn : callable
        raw bytes -> DataFrame with columns scheme_name, isin, ticker,
        quantity, value_inr (no `month` column — added by the caller).

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-SOLID-002 (Open/Closed): the only sanctioned way to add AMC
    coverage to this module.
    """
    AMC_REGISTRY[name] = AMCSource(name=name, fetch_fn=fetch_fn, parse_fn=parse_fn)
    logger.info(f"Registered AMC source: {name}")


def download_monthly_disclosure(year: int, month: int, amcs: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Fetch + parse scheme holdings for the requested (or all registered) AMCs.

    Parameters
    ----------
    year : int
    month : int
    amcs : list of str, optional
        Subset of AMC_REGISTRY keys to fetch. Defaults to all registered.

    Returns
    -------
    pd.DataFrame
        Columns: scheme_name, isin, ticker, quantity, value_inr, month
        (month as 'YYYY-MM' string). Empty DataFrame (not an error) if
        every registered AMC's fetch failed for this month — same
        per-source isolation as ingestion/scrapers/macro.py.

    Spec References
    ----------------
    SPEC-PIPE-001: raw retention is the individual fetch_fn's
    responsibility (this function doesn't know each AMC's raw format).

    Raises
    ------
    RuntimeError
        If AMC_REGISTRY is empty (or `amcs` names nothing registered) —
        there is no real data source configured yet; see module docstring.
    """
    targets = amcs if amcs is not None else list(AMC_REGISTRY.keys())
    if not targets:
        raise RuntimeError(
            "No AMCs registered in AMC_REGISTRY. Call "
            "ingestion.scrapers.groww_mf_holdings.register_all_amcs() first — see this "
            "module's docstring and SPEC-MFHOLD-001."
        )
    missing = [name for name in targets if name not in AMC_REGISTRY]
    if missing:
        raise RuntimeError(f"Requested AMCs not registered: {missing}")

    month_str = f"{year:04d}-{month:02d}"
    frames = []
    for name in targets:
        source = AMC_REGISTRY[name]
        try:
            raw = source.fetch_fn(year, month)
            df = source.parse_fn(raw)
            df = df[["scheme_name", "isin", "ticker", "quantity", "value_inr"]].copy()
            df["month"] = month_str
            frames.append(df)
            logger.info(f"{name}: {len(df)} scheme-ticker rows for {month_str}")
        except Exception as exc:
            logger.warning(f"MF holdings fetch failed for {name} ({month_str}): {exc}")
        time.sleep(AMFI_FETCH_RATE_LIMIT_SLEEP_SECONDS)

    if not frames:
        return pd.DataFrame(columns=HOLDINGS_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def availability_date_for_month(year: int, month: int) -> date:
    """
    SPEC-PIPE-003: the PIT-safe date this month's holdings become public.

    Parameters
    ----------
    year : int
    month : int

    Returns
    -------
    date
        `DELAY`-th day of the following month (config.settings.
        MF_HOLDINGS_AVAILABILITY_DELAY_DAYS), e.g. June 2024 -> 2024-07-05.

    Raises
    ------
    None
    """
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    return date(next_year, next_month, MF_HOLDINGS_AVAILABILITY_DELAY_DAYS)


def save_monthly_parquet(df: pd.DataFrame, year: int, month: int, output_dir: Path = MF_HOLDINGS_DIR) -> Path:
    """
    Save one month's scheme holdings to datastore/normalised/mf_holdings/YYYY-MM.parquet.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain HOLDINGS_COLUMNS (minus availability_date, added here).
    year : int
    month : int
    output_dir : Path, optional
        Defaults to config.settings.MF_HOLDINGS_DIR.

    Returns
    -------
    Path
        The written file path.

    Spec References
    ----------------
    SPEC-PIPE-003 (CRITICAL): availability_date is stamped on every row at
    write time — features/mf_holdings.py filters on this column, never on
    `month` directly (the same announcement_date-not-quarter_end_date
    discipline as P2.1's fundamentals).

    Raises
    ------
    None
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    out["availability_date"] = availability_date_for_month(year, month)
    path = output_dir / f"{year:04d}-{month:02d}.parquet"

    # Merge with any existing file for this month rather than overwriting
    # it — download_monthly_disclosure is explicitly designed to be called
    # with a SUBSET of AMCs at a time (verification, retries, rate-limit
    # batching across 49+ AMCs). A plain overwrite would silently destroy
    # whatever other AMCs had already been saved for the same month.
    # Re-saving the SAME scheme_name(s) again (e.g. a retry) correctly
    # replaces just those rows, not duplicates them — every other AMC's
    # existing rows are left untouched.
    if path.exists():
        existing = pd.read_parquet(path)
        existing = existing[~existing["scheme_name"].isin(out["scheme_name"].unique())]
        out = pd.concat([existing, out], ignore_index=True)

    out.to_parquet(path, index=False)
    logger.info(f"Wrote {len(out)} rows to {path}")
    return path


def run_monthly_ingestion(year: int, month: int, amcs: Optional[List[str]] = None) -> Path:
    """Download, then save, one month's disclosure — the scheduler job's single entry point."""
    df = download_monthly_disclosure(year, month, amcs=amcs)
    return save_monthly_parquet(df, year, month)


def sync_duckdb_table(
    conn, year: int, month: int, output_dir: Path = MF_HOLDINGS_DIR, publish_mode: str = "staged"
) -> int:
    """
    Phase C (Big Investor Activity — plan: gentle-wobbling-swing.md): mirror
    one month's parquet snapshot into the `mf_holdings` DuckDB table, so the
    API can query month-over-month movers without loading parquet per
    request. The parquet file remains the raw/audit artifact — this is a
    read-optimized copy, delete-then-insert per month (same pattern as
    large_deals per trade_date), always safe to re-run.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
    year : int
    month : int
    output_dir : Path, optional
    publish_mode : str
        'staged' (default as of the 2026-07-10 Pipeline & Monitoring
        Remediation, A51): merge this month's replacement into a full
        snapshot of mf_holdings (datastore/staging/merge.py::
        partition_replace_merge — same "delete this month, keep every
        other month" semantics) and publish atomically via
        datastore/staging, gaining an N=7 rollback point (A25). 'direct':
        legacy DELETE+INSERT for this one month, no rollback snapshot;
        kept only as an escape hatch.

    Returns
    -------
    int
        Rows written. 0 if no parquet file exists for this month yet.
    """
    path = output_dir / f"{year:04d}-{month:02d}.parquet"
    if not path.exists():
        return 0

    df = pd.read_parquet(path)
    df = df[df["ticker"].notna() & (df["ticker"] != "")]
    month_date = date(year, month, 1)

    if df.empty:
        if publish_mode == "direct":
            conn.execute("DELETE FROM mf_holdings WHERE month = ?", [month_date])
        else:
            _sync_staged(conn, month_date, pd.DataFrame(columns=["ticker", "month", "scheme_name"]))
        return 0

    # A scheme can hold the same ticker across multiple raw disclosure
    # lines (e.g. separate lots) — mf_holdings' PRIMARY KEY is
    # (ticker, month, scheme_name), so aggregate to one row per
    # (ticker, scheme_name) before insert rather than relying on the raw
    # parquet already being deduplicated.
    df = df.groupby(["ticker", "scheme_name"], as_index=False).agg(
        isin=("isin", "first"),
        quantity=("quantity", "sum"),
        value_inr=("value_inr", "sum"),
        availability_date=("availability_date", "first"),
    )

    if publish_mode == "direct":
        conn.execute("DELETE FROM mf_holdings WHERE month = ?", [month_date])
        conn.execute(
            """
            INSERT INTO mf_holdings (ticker, month, scheme_name, isin, quantity, value_inr, availability_date)
            SELECT ticker, ?, scheme_name, isin, quantity, value_inr, availability_date FROM df
            """,
            [month_date],
        )
    else:
        df_with_month = df.copy()
        df_with_month.insert(1, "month", month_date)
        _sync_staged(conn, month_date, df_with_month)

    logger.info(f"sync_duckdb_table: {len(df)} rows for {year:04d}-{month:02d}")
    return len(df)


def _sync_staged(conn, month_date, new_month_df) -> None:
    """A25 staged path for sync_duckdb_table: partition-replace-merge this
    month against the rest of mf_holdings, then publish atomically."""
    from datastore.staging.gate import stage_dataframe
    from datastore.staging.merge import partition_replace_merge
    from datastore.staging.publish import publish_run_lock, publish_table

    existing_df = conn.execute("SELECT * FROM mf_holdings").df()
    merged_df = partition_replace_merge(existing_df, new_month_df, "month", [month_date])

    with publish_run_lock() as acquired:
        if not acquired:
            logger.error("Another publish is in progress — staged mf_holdings sync NOT published.")
            return
        stage_dataframe(conn, "mf_holdings", merged_df, validators=[])
        publish_table(conn, "mf_holdings")


def _cli() -> None:
    """
    CLI entry point:
        python3 -m ingestion.scrapers.amfi_holdings YYYY MM [--amcs "A,B"] [--all-groww]
    `--all-groww` discovers and registers every AMC Groww currently lists
    (a real network call).
    """
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="MF scheme-holdings ingestion (sole source: Groww)")
    parser.add_argument("year", type=int)
    parser.add_argument("month", type=int)
    parser.add_argument("--amcs", help="Comma-separated AMC names (default: all registered)")
    parser.add_argument(
        "--all-groww", action="store_true", help="Discover and register every Groww-listed AMC first"
    )
    args = parser.parse_args()

    if args.all_groww:
        from ingestion.scrapers.groww_mf_holdings import register_all_amcs

        n = register_all_amcs()
        print(f"Registered {n} Groww-backed AMCs", flush=True)

    amcs = [a.strip() for a in args.amcs.split(",")] if args.amcs else None
    path = run_monthly_ingestion(args.year, args.month, amcs=amcs)
    print(f"Wrote {path}", flush=True)


if __name__ == "__main__":
    _cli()
