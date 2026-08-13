#!/usr/bin/env python3
"""
scripts/repair_corporate_action_ratios.py

Repairs legacy SPLIT/BONUS corporate actions that were never applied to the
price history, and the zero ratios that prevented some of them from being
applied at all.

    dry run (default):  .venv/bin/python scripts/repair_corporate_action_ratios.py
    apply:              .venv/bin/python scripts/repair_corporate_action_ratios.py --apply

WHY A SCRIPT AND NOT A MIGRATION

Every action is judged against the price history before anything is written
(see ingestion/adjust/ratio_recovery). The verdicts are meant to be read by a
human in dry-run before --apply is ever passed, because the failure mode here
is silent: a double-adjusted price series looks exactly like a correct one.

THE PER-TICKER SAFETY GATE

The unit of repair is the TICKER, not the action, and that is not a
convenience — it is a correctness requirement.

adjust_for_corporate_actions recomputes a ticker's entire cumulative factor
from all of its actions, and recovers the unadjusted price as
`raw = current / adj_factor`. That recovery is only valid when adj_factor
truly describes what has been applied to the stored prices.

For 18 legacy actions it does not: the split is already reflected in the
prices while adj_factor still reads 1.0. Running the adjuster over such a
ticker would treat already-adjusted prices as raw and adjust them a second
time. So a ticker is repaired only when EVERY one of its actions is either
already recorded as applied or independently confirmed by the price gap. A
ticker with even one already-baked-in-but-unrecorded action is left alone and
reported for manual handling — its adj_factor column is not trustworthy enough
to rebuild from.

Spec References: SPEC-PIPE-002
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import DUCKDB_PATH  # noqa: E402
from ingestion.adjust.price_adjuster import adjust_for_corporate_actions  # noqa: E402
from ingestion.adjust.ratio_recovery import (  # noqa: E402
    GAP_WINDOW_SESSIONS,
    Verdict,
    classify_action,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("repair_ca_ratios")

# Fyers bars are pre-adjusted at source and carry adj_factor=1.0 by standing
# rule, so they show no gap whether or not this project's adjustment ran.
# Including them would make every legacy action look already-adjusted.
LEGACY_SOURCE_PREDICATE = "(source IS NULL OR source != 'fyers')"


def _load_actions(conn) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT ticker, ex_date, action_type, ratio, COALESCE(details, '') AS details
        FROM corporate_actions
        WHERE action_type IN ('SPLIT', 'BONUS')
        ORDER BY ticker, ex_date
        """
    ).df()


def _siblings(actions: pd.DataFrame, action) -> "list[tuple[str, float]]":
    """Every action sharing this ticker and ex-date, including the action
    itself — they jointly explain the one gap observed on that date."""
    same_day = actions[
        (actions.ticker == action.ticker) & (actions.ex_date == action.ex_date)
    ]
    return [(r.action_type, r.ratio, r.details) for r in same_day.itertuples()]


def _load_prices(conn, ticker: str, ex_date) -> pd.DataFrame:
    return conn.execute(
        f"""
        SELECT date, close, adj_factor
        FROM ohlcv_adjusted
        WHERE ticker = ?
          AND {LEGACY_SOURCE_PREDICATE}
          AND date BETWEEN ?::DATE - {GAP_WINDOW_SESSIONS}
                       AND ?::DATE + {GAP_WINDOW_SESSIONS}
        ORDER BY date
        """,
        [ticker, ex_date, ex_date],
    ).df()


def _already_applied(prices: pd.DataFrame, ex_date) -> bool:
    """Whether the adjustment is already recorded in adj_factor.

    Read from the bar immediately BEFORE the ex-date: a retroactive adjustment
    scales prior history, so that is where the factor lands.
    """
    if prices.empty:
        return False
    before = prices[pd.to_datetime(prices["date"]) < pd.Timestamp(ex_date)]
    if before.empty:
        return False
    return abs(float(before["adj_factor"].iloc[-1]) - 1.0) > 1e-6


def classify_all(conn) -> pd.DataFrame:
    actions = _load_actions(conn)

    # Several actions can share one ex-date (a split announced alongside a
    # bonus). They produce a SINGLE price gap equal to the product of their
    # factors, so each must be scored against that product rather than against
    # its own factor alone.
    rows = []
    for action in actions.itertuples():
        prices = _load_prices(conn, action.ticker, action.ex_date)
        applied = _already_applied(prices, action.ex_date)
        result = classify_action(
            action.ticker, action.ex_date, action.action_type,
            action.ratio, action.details, prices,
            siblings=_siblings(actions, action),
        )
        rows.append({
            "ticker": result.ticker,
            "ex_date": result.ex_date,
            "action_type": result.action_type,
            "stored_ratio": action.ratio,
            "ratio": result.ratio,
            "verdict": result.verdict.value,
            "expected_gap": result.expected_gap,
            "observed_gap": result.observed_gap,
            "already_applied": applied,
            "needs_ratio_write": (
                result.repairable
                and (action.ratio is None or action.ratio <= 0)
            ),
        })
    return pd.DataFrame(rows)


def safe_tickers(classified: pd.DataFrame) -> tuple[set, dict]:
    """Split tickers into those safe to re-adjust and those that are not.

    Safe means: no action on this ticker is already reflected in the prices
    without being recorded in adj_factor. Anything else makes
    `raw = current / adj_factor` a lie for that ticker, and the adjuster
    rebuilds from exactly that quantity.
    """
    blocked: dict = defaultdict(list)
    for row in classified.itertuples():
        if row.verdict == Verdict.ALREADY_ADJUSTED.value and not row.already_applied:
            blocked[row.ticker].append(f"{row.ex_date.date()} {row.action_type}")
        elif row.verdict == Verdict.CONTRADICTED.value:
            blocked[row.ticker].append(
                f"{row.ex_date.date()} {row.action_type} (contradicted)"
            )

    wanted = {
        r.ticker for r in classified.itertuples()
        if r.verdict == Verdict.CONFIRMED.value and not r.already_applied
    }
    return wanted - set(blocked), dict(blocked)


def report(classified: pd.DataFrame, safe: set, blocked: dict) -> None:
    print("\n" + "=" * 72)
    print("CORPORATE-ACTION REPAIR — DIAGNOSIS")
    print("=" * 72)
    print(f"\nSPLIT/BONUS actions examined: {len(classified)}")
    print("\nVerdicts:")
    print(classified["verdict"].value_counts().to_string())

    owed = classified[
        (classified.verdict == Verdict.CONFIRMED.value) & (~classified.already_applied)
    ]
    ratio_writes = classified[classified.needs_ratio_write]

    print(f"\nAdjustments owed (confirmed by the price gap): {len(owed)}")
    print(f"  of which need a ratio recovered first:      {len(ratio_writes)}")
    print(f"\nTickers safe to re-adjust:   {len(safe)}")
    print(f"Tickers blocked from repair: {len(blocked)}")
    if blocked:
        print("\n  Blocked — adj_factor does not describe these prices, so the")
        print("  adjuster cannot rebuild them safely. Manual review required:")
        for ticker, reasons in sorted(blocked.items())[:25]:
            print(f"    {ticker:<14} {'; '.join(reasons)}")
        if len(blocked) > 25:
            print(f"    ... and {len(blocked) - 25} more")
    print()


def apply_repair(conn, classified: pd.DataFrame, safe: set) -> None:
    ratio_writes = classified[classified.needs_ratio_write & classified.ticker.isin(safe)]
    logger.info("Writing %d recovered ratios", len(ratio_writes))
    for row in ratio_writes.itertuples():
        conn.execute(
            "UPDATE corporate_actions SET ratio = ? "
            "WHERE ticker = ? AND ex_date = ? AND action_type = ?",
            [float(row.ratio), row.ticker, row.ex_date.date(), row.action_type],
        )

    logger.info("Re-adjusting %d tickers", len(safe))
    for i, ticker in enumerate(sorted(safe), 1):
        try:
            adjust_for_corporate_actions(conn, ticker)
        except Exception:
            # One bad ticker must not abandon the rest mid-repair, leaving the
            # database in a half-adjusted state nobody can characterise.
            logger.exception("%s: adjustment failed — skipped", ticker)
        if i % 20 == 0:
            logger.info("  %d/%d", i, len(safe))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="write the repair (default is a dry run)")
    parser.add_argument("--db", default=str(DUCKDB_PATH))
    args = parser.parse_args()

    if not args.apply:
        conn = duckdb.connect(args.db, read_only=True)
        classified = classify_all(conn)
        safe, blocked = safe_tickers(classified)
        report(classified, safe, blocked)
        out = Path("datastore/reports/ca_repair_diagnosis.csv")
        out.parent.mkdir(parents=True, exist_ok=True)
        classified.to_csv(out, index=False)
        print(f"Full per-action verdicts written to {out}")
        print("Re-run with --apply to perform the repair.")
        return 0

    backup = Path(f"{args.db}.pre_ca_repair_"
                  f"{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    logger.info("Backing up to %s", backup)
    shutil.copy2(args.db, backup)

    conn = duckdb.connect(args.db)
    classified = classify_all(conn)
    safe, blocked = safe_tickers(classified)
    report(classified, safe, blocked)
    apply_repair(conn, classified, safe)
    conn.close()
    logger.info("Repair complete. Backup retained at %s", backup)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
