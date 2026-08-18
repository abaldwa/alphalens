"""
backtest/core/readiness_gate.py

Phase: Unified Generator Refactor, Phase D (D2)
Owner: Platform / Backtest
Consumers: backtest/paper_trading/live_runner.py (PaperTradingRunner),
backtest/core/live_signal_runner.py (LiveSignalRunner).

The A103 "do not generate on partial data" gate, factored out of
PaperTradingRunner so the live holdings path enforces the SAME rule rather
than a second copy of it. Two copies of a refusal rule is how one of them
quietly stops refusing — which is the whole failure mode this refactor
exists to remove.
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from pathlib import Path
from typing import Any, List, Optional

from strategies.signals import UNVERSIONED

logger = logging.getLogger(__name__)


class _Uncheckable:
    """Stands in for a Readiness when the check itself could not run.

    Not-ready by construction, and carries a MissingInput so the caller's
    logging path works unchanged. It exists so that "the readiness check
    crashed" can never be mistaken for "the data is ready" -- the failure
    mode that would quietly disable the gate.
    """

    ready = False

    class _Reason:
        detail = "readiness check failed to run (see traceback above)"

    missing = (_Reason(),)


UNCHECKABLE = _Uncheckable()


def check_readiness(
    channel: str, strategy_id: str, universe: List[str], as_of_date: date_type,
    *, enforce: bool = True, checker: Optional[Any] = None,
    db_path: Optional[Path] = None,
) -> Optional[Any]:
    """Returns a Readiness, UNCHECKABLE, or None when not enforcing.

    A failure to RUN the check is treated as not-ready. The alternative --
    swallow the error and generate anyway -- would turn the one mechanism
    that stops bad signals into the mechanism that hides why they got
    through.
    """
    if not enforce:
        return None
    from backtest.core.readiness import ReadinessChecker, record_blocked

    active = checker or ReadinessChecker()
    strategy_key = f"{channel}:{strategy_id}"
    try:
        readiness = active.check(channel, as_of_date, universe=universe, strategy_key=strategy_key)
    except Exception:
        logger.exception(
            "%s: readiness check itself failed for %s — refusing to generate. "
            "An unrunnable check is not a pass.", strategy_key, as_of_date,
        )
        return UNCHECKABLE

    if not readiness.ready:
        try:
            record_blocked(
                readiness, strategy_key=strategy_key, strategy_version=UNVERSIONED,
                db_path=db_path,
            )
        except Exception:
            # The refusal still stands; only its audit row was lost.
            logger.exception("Could not record the blocked signal generation")
    return readiness
