#!/usr/bin/env python3
"""Collate the 2026-07-06 retrain log into a per-model completion summary."""
import re
import sys
from pathlib import Path

LOG = Path(sys.argv[1] if len(sys.argv) > 1 else "logs/retrain_all_20260706.log")

STAGE_START_RE = re.compile(r"^=== (\S+) .*START (\S+)")
STAGE_END_RE = re.compile(r"^=== (\S+) END (\S+) exit=(\d+)")
SAVED_RE = re.compile(r"Saved (\S+) -> (\S+)")
LOCK_FAIL_RE = re.compile(r"Could not set lock")

lines = LOG.read_text().splitlines()

stages = []  # list of dict: name, start, end, exit, saved(list), locked_out(bool)
current = None
for line in lines:
    m = STAGE_START_RE.match(line)
    if m:
        current = {"name": m.group(1), "start": m.group(2), "end": None, "exit": None, "saved": [], "locked_out": False}
        stages.append(current)
        continue
    m = STAGE_END_RE.match(line)
    if m and current is not None:
        current["end"] = m.group(2)
        current["exit"] = m.group(3)
        continue
    m = SAVED_RE.search(line)
    if m and current is not None:
        current["saved"].append((m.group(1), m.group(2)))
    if LOCK_FAIL_RE.search(line) and current is not None:
        current["locked_out"] = True

print("=" * 90)
print("RETRAIN COLLATED SUMMARY —", LOG)
print("=" * 90)
for s in stages:
    if s["end"] is None:
        status = "IN PROGRESS"
    elif s["locked_out"] and not s["saved"]:
        status = "FAILED (DB lock conflict)"
    elif s["exit"] == "0":
        status = "OK"
    else:
        status = f"FAILED (exit={s['exit']})"
    print(f"\n[{s['name']}]  {s['start']} -> {s['end'] or '(running)'}   status={status}")
    for model_name, path in s["saved"]:
        print(f"    saved: {model_name:20s} {path}")
    if not s["saved"] and status != "IN PROGRESS":
        print("    (no models saved this stage)")

print("\n" + "=" * 90)
