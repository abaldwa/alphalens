"""
backtest/core/

Phase 1 of the Unified Backtest & Paper Trading Umbrella
(see BacktestUmbrellaPlan.md at the repo root).

Shared, channel-agnostic building blocks that every strategy adapter
(technical, fundamental, ml, momentum) plugs into: horizon-bucket
classification and position sizing (horizon.py), the canonical metrics
module (metrics.py), the FY-end capital-gains tax engine (tax.py), and
the run-record schema (run_context.py). None of these modules know about
any specific channel's signal-generation logic.
"""
