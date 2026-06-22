"""
backtest package.

Phase: 0.1 (Project Skeleton)
Specs: SPEC-BT-001, SPEC-BT-002, SPEC-BT-003, SPEC-BT-004, SPEC-PIPE-004
Owner: Platform / Backtest
Consumers: systems/ml_signal_engine/training, dashboard

Historical backtesting engine for signal evaluation.
Computes: P&L, Sharpe ratio, max drawdown, transaction costs (SPEC-BT-002).
Enforces point-in-time correctness (SPEC-DS-003) to prevent look-ahead bias.
"""
