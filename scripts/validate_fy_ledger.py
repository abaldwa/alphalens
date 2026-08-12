#!/usr/bin/env python3
"""
scripts/validate_fy_ledger.py

Structural checks on a capital_mode="annual_reset" run's FY ledger.

Written 2026-08-12 after the pre-sweep smoke test found two bugs that a
9-hour, 260-job sweep would have surrendered to silently:

  1. fy_ledger was computed and then never persisted (measure 3's whole
     deliverable missing, run otherwise "successful").
  2. FY labels were wrong whenever 1 April fell on a weekend/holiday, which
     made mislabelled years pull the wrong realised-P&L bucket and withdraw
     the wrong amount. Three rows showed realised=0 despite positive returns.

Neither raised. Both produced plausible-looking numbers. So the ledger gets a
machine-checkable contract, run over every annual_reset report before its
numbers are believed.

Usage:
    python scripts/validate_fy_ledger.py backtest/reports/orchestrator_arsmoke3.json
    python scripts/validate_fy_ledger.py 'backtest/reports/orchestrator_ta*.json'
"""

import glob
import json
import sys
from pathlib import Path


def validate(path: Path) -> list:
    """Return a list of problem strings; empty means the ledger is coherent."""
    d = json.loads(path.read_text())
    led = d.get("fy_ledger") or []
    run = d.get("run") or {}
    problems = []

    mode = run.get("capital_mode")
    if mode != "annual_reset":
        return [f"capital_mode={mode!r}, not an annual_reset run"] if led else []

    if not led:
        problems.append("fy_ledger is EMPTY on an annual_reset run — measure 3 produced nothing")
        return problems

    # --- labels: one row per FY, consecutive, no duplicates -----------------
    labels = [r["fy_end"] for r in led]
    if len(labels) != len(set(labels)):
        dupes = sorted({x for x in labels if labels.count(x) > 1})
        problems.append(f"duplicate FY labels: {dupes}")
    years = sorted(int(x[:4]) for x in set(labels))
    missing = [y for y in range(years[0], years[-1] + 1) if y not in years]
    if missing:
        problems.append(f"missing FY years: {missing}")
    if any(not x.endswith("-03-31") for x in labels):
        problems.append("an fy_end is not a 31 March date")

    # --- arithmetic: each row internally consistent -------------------------
    for r in led:
        fy = r["fy_end"]
        if r["withdrawn"] > r["closing_equity"] + 1.0:
            problems.append(f"{fy}: withdrew more than closing equity")
        if r["withdrawn"] < -1e-6 or r["topped_up"] < -1e-6:
            problems.append(f"{fy}: negative withdrawal/top-up")
        if r["withdrawn"] > 0 and r["topped_up"] > 0:
            problems.append(f"{fy}: both withdrew AND topped up in the same year")
        # Withdrawal can never exceed post-tax realised profit.
        if r["withdrawn"] > max(0.0, r["realised_after_tax"]) + 1.0:
            problems.append(
                f"{fy}: withdrew {r['withdrawn']:,.0f} > realised_after_tax {r['realised_after_tax']:,.0f}"
            )
        # Pre-tax withdrawal must be >= post-tax one.
        if r["withdrawn_pretax"] + 1.0 < r["withdrawn"]:
            problems.append(f"{fy}: pre-tax withdrawal < post-tax withdrawal")
        # NOTE: there is deliberately no "a losing year must owe no tax" rule.
        # That was the original check, and it was wrong in both directions: it
        # fired on 8 runs, missed 61 more, and would reject a legitimate FY that
        # books a short-term GAIN alongside a larger long-term LOSS (a long-term
        # loss cannot shelter short-term gains, so real tax is due on a year
        # that lost money overall). Tax is checked against the trade log by
        # `_check_tax_against_trade_log` instead, which reproduces the actual
        # set-off rules. See tests/unit/test_tax_setoff_rules.py.
        # A year with real gains and real cash should not silently withdraw 0 —
        # this is the exact signature of the FY-mislabelling bug.
        if r["realised_after_tax"] > 1000 and r["withdrawn"] == 0 and r["closing_equity"] > r["opening_capital"]:
            problems.append(
                f"{fy}: realised {r['realised_after_tax']:,.0f} after tax but withdrew 0 "
                "(possible FY-attribution bug)"
            )

    # --- continuity: this year's opening == last year's post-adjustment -----
    for prev, cur in zip(led, led[1:]):
        if abs(prev["opening_capital_next"] - cur["opening_capital"]) > 1.0:
            problems.append(
                f"{cur['fy_end']}: opening {cur['opening_capital']:,.0f} != previous "
                f"opening_capital_next {prev['opening_capital_next']:,.0f}"
            )

    # --- regime actually stamped -------------------------------------------
    regimes = {r.get("regime") for r in led}
    if regimes == {"engine_default"} or None in regimes:
        problems.append(f"regime not stamped on the ledger: {regimes}")

    problems.extend(_check_tax_against_trade_log(d))

    return problems


def _check_tax_against_trade_log(report: dict) -> list:
    """Recompute each FY's tax from the run's own trade log and compare.

    The ledger stores only a net realised figure, which cannot distinguish a
    short-term loss from a long-term one — and that distinction is precisely
    what the set-off rules turn on. So this goes back to the trade book, buckets
    by holding period, and applies the real rules via backtest.core.tax. That is
    what makes this check able to see the 2026-08-12 set-off bug, which the
    ledger-only arithmetic could not.
    """
    import os
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from backtest.core.tax import apply_stcg_loss_setoff  # noqa: PLC0415

    path = report.get("trade_log_path")
    if not path or not os.path.exists(path):
        return []  # nothing to check against; not an error

    run = report.get("run") or {}
    ltcg_rate = run.get("annual_reset_ltcg_rate")
    exemption = run.get("annual_reset_ltcg_exemption") or 0.0
    if ltcg_rate is None:
        return []
    stcg_rate = 0.20

    import csv
    from datetime import date as _date

    buckets = {}
    with open(path) as fh:
        for row in csv.DictReader(fh):
            try:
                sell = _date.fromisoformat(row["sale_date"][:10])
                buy = _date.fromisoformat(row["buy_date"][:10])
                # GROSS gain, not pnl_inr. pnl_inr is net of costs, but the
                # engine taxes the gross capital gain (Transaction.gain =
                # (sell_price - buy_price) * quantity). Using pnl_inr made every
                # comparison fail low by roughly the cost drag — 2,303 FY rows,
                # all in the same direction, which is what exposed the mistake.
                pnl = (float(row["sale_price"]) - float(row["buy_price"])) * float(row["qty"])
            except (ValueError, KeyError, TypeError):
                continue
            fy_end = _date(sell.year + 1 if sell.month >= 4 else sell.year, 3, 31)
            b = buckets.setdefault(fy_end.isoformat(), [0.0, 0.0])
            if (sell - buy).days >= 365:
                b[1] += pnl
            else:
                b[0] += pnl

    problems = []
    for r in report.get("fy_ledger") or []:
        st, lt = buckets.get(r["fy_end"], (0.0, 0.0))
        st, lt = apply_stcg_loss_setoff(st, lt)
        expected = max(0.0, lt - exemption) * ltcg_rate if lt > 0 else 0.0
        expected += st * stcg_rate if st > 0 else 0.0
        # Trade-log P&L is net of costs while the engine taxes gross gains, so
        # allow a proportional tolerance rather than demanding an exact match;
        # the set-off bug produced errors far larger than this band.
        tol = max(500.0, 0.05 * max(expected, r["tax"]))
        if abs(r["tax"] - expected) > tol:
            problems.append(
                f"{r['fy_end']}: tax {r['tax']:,.0f} but trade log implies "
                f"{expected:,.0f} (STCG {st:,.0f} / LTCG {lt:,.0f} after set-off)"
            )
    return problems


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    paths = []
    for arg in sys.argv[1:]:
        paths.extend(Path(p) for p in glob.glob(arg))
    if not paths:
        raise SystemExit("no reports matched")

    bad = 0
    for p in sorted(paths):
        problems = validate(p)
        if problems:
            bad += 1
            print(f"FAIL {p.name}")
            for x in problems:
                print(f"       {x}")
        else:
            d = json.loads(p.read_text())
            led = d.get("fy_ledger") or []
            if led:
                net = sum(r["withdrawn"] for r in led) - sum(r["topped_up"] for r in led)
                print(f"OK   {p.name}  {len(led)} FYs  net extracted {net:,.0f}")
            else:
                print(f"OK   {p.name}  (not an annual_reset run)")

    print(f"\n{len(paths) - bad}/{len(paths)} reports passed")
    if bad:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
