"""
backtest/paper_trading/approval_queue.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 5
Owner: Platform / Backtest
Consumers: backtest/paper_trading/live_runner.py, the (future) unified
paper-trading API router

Generalizes the existing ML-only paper_trading/pending/{date}.json
human-approval pattern (systems/ml_signal_engine/inference/
paper_trading_step.py, untouched here) to be (channel, strategy_id)-
scoped, and to count paper-trading days per strategy rather than
assuming there's only ever one paper-trading strategy (the ML signal
engine) in the whole app.

File-based, matching the existing ML-only design's storage choice — a
human reviews a small daily JSON file, not a database row, before any
action executes. No-Mock-Data Policy: pending actions are Signal objects
a real adapter proposed for a real as_of_date; nothing here fabricates a
signal or a price.
"""

import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from backtest.core.engine import Signal

logger = logging.getLogger(__name__)

PAPER_TRADING_ROOT = Path("paper_trading")
PENDING_DIR = PAPER_TRADING_ROOT / "pending"
EXECUTIONS_DIR = PAPER_TRADING_ROOT / "executions"
STATE_DIR = PAPER_TRADING_ROOT / "state"

# Same threshold/policy as the existing ML-only Gate 7
# (datastore/api/routers/paper_trading.py::GATE_THRESHOLD) — Phase 3 gate
# requires >=90 real forward paper-trading days before any channel's
# strategy is eligible for live capital.
GATE_THRESHOLD = 90


@dataclass
class PendingAction:
    action_id: str
    channel: str
    strategy_id: str
    as_of_date: str
    ticker: str
    action: str  # "buy" | "sell" | "forced_close"
    sector: str
    conviction: float
    adtv_cr: Optional[float]
    status: str = "pending"  # "pending" | "accepted" | "rejected"
    proposed_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    decided_at: Optional[str] = None
    executed_price: Optional[float] = None
    executed_quantity: Optional[int] = None


def _pending_path(channel: str, strategy_id: str, as_of_date) -> Path:
    return PENDING_DIR / channel / strategy_id / f"{as_of_date}.json"


def _executions_path(channel: str, strategy_id: str, as_of_date) -> Path:
    return EXECUTIONS_DIR / channel / strategy_id / f"{as_of_date}.json"


def write_pending_actions(channel: str, strategy_id: str, as_of_date, signals: List[Signal]) -> List[PendingAction]:
    """Propose today's signals for human review — never auto-executed. One
    PendingAction per Signal, each with a fresh action_id."""
    actions = [
        PendingAction(
            action_id=str(uuid.uuid4()), channel=channel, strategy_id=strategy_id, as_of_date=str(as_of_date),
            ticker=s.ticker, action=s.action, sector=s.sector, conviction=s.conviction, adtv_cr=s.adtv_cr,
        )
        for s in signals
    ]
    path = _pending_path(channel, strategy_id, as_of_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(a) for a in actions], indent=2))
    logger.info(f"Wrote {len(actions)} pending action(s) for {channel}/{strategy_id}/{as_of_date}")
    return actions


def read_pending_actions(channel: str, strategy_id: str, as_of_date) -> List[PendingAction]:
    path = _pending_path(channel, strategy_id, as_of_date)
    if not path.exists():
        return []
    return [PendingAction(**row) for row in json.loads(path.read_text())]


def _rewrite_pending(channel: str, strategy_id: str, as_of_date, actions: List[PendingAction]) -> None:
    path = _pending_path(channel, strategy_id, as_of_date)
    path.write_text(json.dumps([asdict(a) for a in actions], indent=2))


def update_action_status(
    channel: str, strategy_id: str, as_of_date, action_id: str, status: str,
    executed_price: Optional[float] = None, executed_quantity: Optional[int] = None,
) -> PendingAction:
    """Transition one pending action to 'accepted' or 'rejected', rewriting
    that date's pending file. Raises ValueError if the action_id isn't found
    or is no longer pending — never silently no-ops on an unknown id."""
    if status not in ("accepted", "rejected"):
        raise ValueError(f"status must be 'accepted' or 'rejected', got {status!r}")
    actions = read_pending_actions(channel, strategy_id, as_of_date)
    for i, a in enumerate(actions):
        if a.action_id == action_id:
            if a.status != "pending":
                raise ValueError(f"action {action_id} is already {a.status!r}, not pending")
            actions[i] = PendingAction(
                **{**asdict(a), "status": status, "decided_at": datetime.utcnow().isoformat(),
                   "executed_price": executed_price, "executed_quantity": executed_quantity},
            )
            _rewrite_pending(channel, strategy_id, as_of_date, actions)
            return actions[i]
    raise ValueError(f"No pending action {action_id!r} found for {channel}/{strategy_id}/{as_of_date}")


def record_execution(channel: str, strategy_id: str, as_of_date, decided_action: PendingAction) -> None:
    """Append one decided (accepted or rejected) action to that date's
    executions log — a real forward trading day for Gate-7 counting
    purposes only if at least one action was decided on it."""
    path = _executions_path(channel, strategy_id, as_of_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = json.loads(path.read_text()) if path.exists() else []
    existing.append(asdict(decided_action))
    path.write_text(json.dumps(existing, indent=2))


def count_paper_trading_days(channel: str, strategy_id: str) -> int:
    """Number of distinct dated execution files for this (channel,
    strategy_id) — the Gate-7 counter, generalized per-strategy rather than
    assuming a single app-wide paper-trading track."""
    exec_dir = EXECUTIONS_DIR / channel / strategy_id
    if not exec_dir.exists():
        return 0
    return len(list(exec_dir.glob("*.json")))


def gate_status(channel: str, strategy_id: str) -> dict:
    days = count_paper_trading_days(channel, strategy_id)
    return {
        "channel": channel, "strategy_id": strategy_id, "days_completed": days,
        "gate_threshold": GATE_THRESHOLD, "gate_passed": days >= GATE_THRESHOLD,
    }
