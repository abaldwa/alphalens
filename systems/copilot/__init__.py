"""systems/copilot — Co-Pilot: natural-language strategy authoring.

Turns a plain-English query into a structured, deterministic strategy spec
(technical + fundamental + valuation filter conditions), checks it against
existing strategies for duplicates, and runs it through the existing
backtest engines. No arbitrary code execution and no synthetic/mocked data
anywhere in this package — see strategy_spec.py's module docstring.
"""
