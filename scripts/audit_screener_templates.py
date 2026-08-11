#!/usr/bin/env python3
"""
scripts/audit_screener_templates.py

Audits every screener template against the ACTUAL feature store, to catch the
D1 class of defect: a template whose stated concept and whose executed
condition have quietly drifted apart.

[2026-08-11] D1 was described as "RSI-2 Mean Reversion" but screened on
`rsi_14 < 10` — a stand-in from when rsi_2 wasn't stored. RSI-2 routinely dips
below 10; RSI-14 almost never does. D1 matched 7 ticker-days in 19 years and
silently contributed nothing to every sweep it was ever included in. Nothing
failed loudly, so nothing was noticed.

Three checks, cheapest first:

  MISSING   a referenced feature does not exist in the feature store at all.
            Such a condition can never be satisfied.

  ALL-NULL  the feature exists but is entirely null over the sample, which is
            indistinguishable from missing at screening time.

  DEAD /    the condition is satisfiable in principle but almost never (dead)
  TRIVIAL   or almost always (trivial) in practice. Dead conditions make a
            template inert like D1; trivial ones make it a no-op filter that
            silently widens the strategy.

Cross-sectional ops (top_pct / bottom_pct) are rank-based, so they always match
a fixed fraction by construction and are reported separately rather than being
scored for selectivity.

Sampling is by date across the whole history, so a feature only populated in
recent years still shows up as thin rather than absent.

Usage:
    python scripts/audit_screener_templates.py
    python scripts/audit_screener_templates.py --sample-dates 40 --dead-threshold 0.0005
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402

FEATURE_DIR = Path("datastore/features/daily")

_SCALAR_OPS = {"lt": "<", "gt": ">", "lte": "<=", "gte": ">="}
_CROSS_SECTIONAL = {"top_pct", "bottom_pct"}


def load_templates():
    from systems.technical_analysis.screener.templates import TEMPLATES

    items = TEMPLATES.values() if hasattr(TEMPLATES, "values") else TEMPLATES
    return [t for t in items if getattr(t, "name", None)]


def sample_dates(n: int):
    files = sorted(FEATURE_DIR.glob("20*.parquet"))
    if not files:
        raise SystemExit(f"no feature parquets under {FEATURE_DIR}")
    step = max(1, len(files) // n)
    return files[::step][:n]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sample-dates", type=int, default=30)
    p.add_argument("--dead-threshold", type=float, default=0.0005, help="match rate below this = DEAD")
    p.add_argument("--trivial-threshold", type=float, default=0.98, help="match rate above this = TRIVIAL")
    args = p.parse_args()

    templates = load_templates()
    files = sample_dates(args.sample_dates)
    conn = duckdb.connect()

    src = "read_parquet([" + ",".join(f"'{f}'" for f in files) + "])"
    cols = {r[0] for r in conn.execute(f"DESCRIBE SELECT * FROM {src} LIMIT 1").fetchall()}
    total = conn.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]

    print(f"templates: {len(templates)}   sampled dates: {len(files)}   rows: {total:,}\n")

    # Per-feature null check, one pass.
    used = sorted({c["feature"] for t in templates for c in t.conditions})
    missing = {f for f in used if f not in cols}
    present = [f for f in used if f in cols]
    nn = {}
    if present:
        expr = ", ".join(f'SUM(CASE WHEN "{f}" IS NOT NULL THEN 1 ELSE 0 END)' for f in present)
        row = conn.execute(f"SELECT {expr} FROM {src}").fetchone()
        nn = dict(zip(present, row))

    findings = defaultdict(list)
    for f in sorted(missing):
        findings["MISSING"].append((f, None))
    for f in present:
        if not nn.get(f):
            findings["ALL-NULL"].append((f, None))

    # Per-condition selectivity.
    rows = []
    for t in templates:
        parts, notes = [], []
        for c in t.conditions:
            feat, op, val = c["feature"], c["op"], c.get("value")
            if feat not in cols or not nn.get(feat):
                parts = None
                break
            if op in _CROSS_SECTIONAL:
                notes.append(f"{feat} {op}")
                continue
            q = f'"{feat}"'
            if op in _SCALAR_OPS:
                parts.append(f"{q} {_SCALAR_OPS[op]} {float(val)}")
            elif op == "between":
                parts.append(f"{q} BETWEEN {float(val[0])} AND {float(val[1])}")
            elif op == "eq":
                parts.append(f"{q} = {float(val)}")
        if parts is None:
            rows.append((t.name, t.description, None, "BROKEN-FEATURE"))
            continue
        if not parts:
            rows.append((t.name, t.description, None, "cross-sectional only"))
            continue
        where = " AND ".join(parts)
        n = conn.execute(f"SELECT COUNT(*) FROM {src} WHERE {where}").fetchone()[0]
        rate = n / total if total else 0.0
        if rate < args.dead_threshold:
            verdict = "DEAD"
        elif rate > args.trivial_threshold:
            verdict = "TRIVIAL"
        else:
            verdict = "ok"
        rows.append((t.name, t.description, rate, verdict + (" +xs" if notes else "")))

    for label in ("MISSING", "ALL-NULL"):
        if findings[label]:
            print(f"=== {label} FEATURES ===")
            for f, _ in findings[label]:
                print(f"  {f}")
            print()

    bad = [r for r in rows if r[3].startswith(("DEAD", "TRIVIAL", "BROKEN"))]
    print("=== TEMPLATES NEEDING ATTENTION ===")
    if not bad:
        print("  none\n")
    for name, desc, rate, verdict in sorted(bad, key=lambda r: (r[3], r[0])):
        r = "n/a" if rate is None else f"{rate*100:.4f}%"
        print(f"  {name:<6} {verdict:<14} match={r:<12} {desc}")

    print("\n=== ALL TEMPLATES (match rate, sampled) ===")
    for name, desc, rate, verdict in sorted(rows, key=lambda r: (r[2] is None, r[2] or 0)):
        r = "n/a" if rate is None else f"{rate*100:8.4f}%"
        print(f"  {name:<6} {r}  {verdict:<20} {desc[:46]}")

    conn.close()


if __name__ == "__main__":
    main()
