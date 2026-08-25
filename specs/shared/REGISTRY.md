# Shared Components Registry

This registry documents all components shared across Backtest, Paper Trading, and Live Deployment stages, plus components reused across multiple strategy domains (Technical, Momentum, ML, Fundamental, Valuation, Forensic, Big-Investors).

**Rule:** Before building a new component that touches more than one stage or domain, check this registry. If something smells reusable, it probably already exists here. If it doesn't exist yet, add it as a todo and link it in your spec.

---

## Backtest ↔ Paper Trading ↔ Live Shared Components

| Module | File Path | Consumers | Owning Spec | Purpose |
|--------|-----------|-----------|------------|---------|
| **Horizon Bucket & Position Sizing** | `backtest/core/horizon.py` | `backtest/core/engine.py`, all 4 domain adapters, `backtest/core/feature_log.py`, `backtest/core/run_context.py`, `backtest/engine.py`, `backtest/strategy_id.py`, `backtest/walk_forward/runner.py`, `backtest/paper_trading/live_runner.py` | `SPEC-PT-001` | Enum + policy for 5-day/21-day/63-day/1-year/MultiBagger horizon buckets; position sizing percentages (2%–5% base, 15%–25% sector cap) |
| **Portfolio & SIP Config** | `backtest/core/portfolio.py` | `backtest/core/engine.py`, `backtest/paper_trading/live_runner.py` | `SPEC-PT-002` | `StrategyPortfolio` class, cash flow simulation, SIP/Annual Reset config, capital tracking |
| **StrategyAdapter Protocol & Signal** | `backtest/core/engine.py` | `backtest/adapters/technical_adapter.py`, `backtest/adapters/momentum_adapter.py`, `backtest/adapters/fundamental_adapter.py`, `backtest/core/live_signal_runner.py`, `backtest/core/live_adapter_factory.py`, `backtest/paper_trading/approval_queue.py` | `SPEC-BT-001` | Protocol that all domain adapters implement; `Signal` dataclass for trade data |
| **Live Signal Runner & Factory** | `backtest/core/live_signal_runner.py`, `backtest/core/live_adapter_factory.py` | `backtest/paper_trading/approval_queue.py`, `backtest/paper_trading/live_runner.py` | `SPEC-PT-003` | Execution layer for paper trading & live; adapter factory for dynamic domain loading |
| **Feature Log** | `backtest/core/feature_log.py` | `backtest/core/engine.py`, `backtest/adapters/*.py` | `SPEC-BT-002` | Stores feature vector + trade context for each decision (enables feedback loop) |
| **Metrics & XIRR** | `backtest/core/metrics.py` | `backtest/core/engine.py`, `backtest/engine.py` (ML), `backtest/adapters/*.py` | `SPEC-BT-003` | Sharpe, Calmar, drawdown, XIRR, CAGR calculations (canonical across all channels) |

---

## Cross-Domain Shared Components

| Module | File Path | Consumers (Domains) | Owning Spec | Purpose |
|--------|-----------|-------------------|------------|---------|
| **Trade Integrity Checker** | `backtest/core/integrity_checker.py` | Technical, Momentum, Fundamental adapters | `SPEC-BT-004` | Validates trade-level correctness: cash positions, order fills, sequence |
| **Walk-Forward Validation** | `backtest/walk_forward/runner.py` | Technical, Momentum, ML adapters | `SPEC-BT-005` | Robustness testing: rolling window validation, out-of-sample confirmation |
| **Strategy Registry** | `config/strategy_registry.py` + `datastore/` | All domains (Technical, Momentum, ML, Fundamental, Valuation, Forensic) | `SPEC-SYS-001` | Single source of truth for strategy definitions; point-in-time versioned |
| **Filter Registry** | `config/filter_registry.py` | Technical, Fundamental screeners | `SPEC-TA-001`, `SPEC-FA-001` | Centralized filter definitions (universe, ADTV floor, market cap tier) |

---

## Adding a New Shared Component

If you find yourself duplicating code across stages or domains:

1. **Check this registry first** — it might already exist.
2. **If not, create the shared module** in `backtest/core/` or the appropriate `config/` subdirectory.
3. **Add a row to this registry** with the file path, all known consumers, and a link to the spec that governs it.
4. **Update the spec** to include the component's contract (function signatures, dataclass fields, protocol methods).
5. **Do not** create domain-local copies or duplicate implementations — that's how silent bugs are introduced (e.g., two universes with different ADTV floors that both claim to be "the screener").

---

## Related Specs

- [specs/shared/ARCHITECTURE.md](ARCHITECTURE.md) — High-level design of the adapter pattern and the shared engine.
- `SPEC-BT-001` through `SPEC-PT-003` — Detailed contracts for all modules above.
- `SPEC-SYS-001` — Registry governance rules (append-only, point-in-time versioning).
