#!/usr/bin/env python3
"""
scripts/compare_feature_parquets.py

Compares two feature-matrix parquets cell by cell. Used to prove that a
performance change to the feature pipeline is behaviour-preserving.

Written 2026-08-12 to validate the bulk-OHLCV window cache
(datastore/client.py::enable_bulk_ohlcv_cache), which replaced ~250 identical
per-date OHLCV fetches per year with one wide fetch plus in-memory slicing.
A cache that returned subtly different rows (wrong slice bounds, dropped
boundary dates, mutated by a previous caller) would silently corrupt every
feature downstream, so "it got faster" is not sufficient evidence.

NaN == NaN here, deliberately: a genuinely absent feature is equal to a
genuinely absent feature.

Usage:
    python scripts/compare_feature_parquets.py REF.parquet NEW.parquet
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    ref_path, new_path = Path(sys.argv[1]), Path(sys.argv[2])

    ref = pd.read_parquet(ref_path)
    new = pd.read_parquet(new_path)

    print(f"ref: {ref_path}  shape={ref.shape}")
    print(f"new: {new_path}  shape={new.shape}\n")

    if list(ref.columns) != list(new.columns):
        only_ref = set(ref.columns) - set(new.columns)
        only_new = set(new.columns) - set(ref.columns)
        print(f"COLUMN MISMATCH  only_in_ref={sorted(only_ref)}  only_in_new={sorted(only_new)}")
        if not only_ref and not only_new:
            print("  (same set, different order — reordering to compare values)")
            new = new[ref.columns]
        else:
            raise SystemExit(1)

    key = "ticker" if "ticker" in ref.columns else ref.columns[0]
    ref = ref.sort_values(key).reset_index(drop=True)
    new = new.sort_values(key).reset_index(drop=True)

    if len(ref) != len(new):
        raise SystemExit(f"ROW COUNT MISMATCH: {len(ref)} vs {len(new)}")

    n_bad = 0
    for c in ref.columns:
        a, b = ref[c], new[c]
        if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b):
            # Both-NaN counts as equal; otherwise require exact bit equality,
            # since this compares the same code path on the same inputs.
            same = (a.values == b.values) | (pd.isna(a.values) & pd.isna(b.values))
        else:
            same = (a.values == b.values) | (pd.isna(a.values) & pd.isna(b.values))
        n_diff = int((~same).sum())
        if n_diff:
            n_bad += 1
            idx = np.where(~same)[0][:3]
            print(f"  DIFF {c}: {n_diff} cells")
            for i in idx:
                print(f"      row {i} ({ref[key].iloc[i]}): ref={a.iloc[i]!r}  new={b.iloc[i]!r}")

    if n_bad:
        print(f"\nFAIL — {n_bad} columns differ")
        raise SystemExit(1)
    print(f"IDENTICAL — {len(ref):,} rows x {len(ref.columns)} columns match exactly")


if __name__ == "__main__":
    main()
