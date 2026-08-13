"""
strategies/predicates.py

Owner: Platform / Architecture (A92)
Consumers: strategies/registry.py (validates on write), the four channel
migrations (T15, ML41, ML42, F7), backtest adapters reading definitions.

The predicate grammar shared by every channel's entry and exit criteria.

This is deliberately NOT a new evaluator. The vocabulary is exactly the one
`systems/technical_analysis/screener/engine.py` already evaluates against a
feature DataFrame, so migrating the 63 ScreenerTemplates (T15) is a data move
rather than a rewrite, and the existing evaluator remains the single
implementation. What this module adds is the part the registry needs and the
screener engine does not have: VALIDATION, so a malformed predicate is
rejected when a definition is written rather than silently treated as an
unmet condition at screen time.

That silent-failure behaviour is the reason validation belongs here. The
screener logs "Unknown screener op '%s' - condition treated as unmet" and
carries on, which is correct for a live screen (one bad rule should not take
down the run) but is exactly wrong for a definition being stored: a typo'd op
would produce a strategy that screens nothing, backtests as flat, and looks
like a genuine result.

Grammar
-------
    {"feature": <column name>, "op": <op>, "value": <scalar|[lo,hi]|[...]>}

    lt / gt / lte / gte / eq       column vs scalar             value=<scalar>
    between                        column in [lo, hi]           value=[lo, hi]
    top_pct / bottom_pct           cross-sectional percentile   value in (0, 1]
    gt_col / lt_col / gte_col /    column vs another column     feature2=<column>
      lte_col
    in / not_in                    column in a set              value=[...]

Note the col-vs-col ops take `feature2`, NOT `value` -- that is the shape the
screener engine reads (`condition.get("feature2")`), and the 63 templates are
written that way. Requiring `value` here would reject every real template
using them.

`in`/`not_in` are the one addition to the screener's vocabulary, needed by
Fundamental (sector exclusions, `PRESET_EXCLUDED_SECTORS`) which currently
expresses them as a Python set outside the condition system entirely. They are
listed in UNSUPPORTED_BY_SCREENER so a migration cannot quietly hand the
screener an op it will drop on the floor.
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence

# Kept in sync with systems/technical_analysis/screener/engine.py's
# _COL_VS_COL_OPS / _UNIVERSE_OPS and its _evaluate_condition branches.
SCALAR_OPS = frozenset({"lt", "gt", "lte", "gte", "eq"})
RANGE_OPS = frozenset({"between"})
PERCENTILE_OPS = frozenset({"top_pct", "bottom_pct"})
COL_VS_COL_OPS = frozenset({"gt_col", "lt_col", "gte_col", "lte_col"})
SET_OPS = frozenset({"in", "not_in"})

ALL_OPS = SCALAR_OPS | RANGE_OPS | PERCENTILE_OPS | COL_VS_COL_OPS | SET_OPS

# Ops the live screener engine cannot evaluate today. A definition using one
# of these is valid to STORE (Fundamental needs them) but a channel that
# routes predicates through ScreenerEngine must either extend the engine or
# translate them first -- it must not just pass them through, because the
# engine's unknown-op path treats the condition as unmet without raising.
UNSUPPORTED_BY_SCREENER = SET_OPS


class PredicateError(ValueError):
    """A predicate is malformed. Raised on write, never swallowed."""


def validate_predicate(pred: Any, *, where: str = "predicate") -> None:
    """
    Raise PredicateError unless `pred` is a well-formed predicate dict.

    Args:
        pred: the candidate predicate.
        where: context for the error message (e.g. "entry[2]").
    """
    if not isinstance(pred, dict):
        raise PredicateError(f"{where}: expected a dict, got {type(pred).__name__}")

    feature = pred.get("feature")
    if not isinstance(feature, str) or not feature:
        raise PredicateError(f"{where}: 'feature' must be a non-empty string")

    op = pred.get("op")
    if op not in ALL_OPS:
        raise PredicateError(
            f"{where}: unknown op {op!r}. Valid ops: {sorted(ALL_OPS)}"
        )

    # Col-vs-col ops carry their right-hand side in `feature2`; everything
    # else in `value`.
    if op in COL_VS_COL_OPS:
        feature2 = pred.get("feature2")
        if not isinstance(feature2, str) or not feature2:
            raise PredicateError(
                f"{where}: op {op!r} needs 'feature2' (a column name), not 'value'"
            )
        if feature2 == feature:
            raise PredicateError(f"{where}: op {op!r} compares {feature!r} to itself")
        return

    if "value" not in pred:
        raise PredicateError(f"{where}: missing 'value'")
    value = pred["value"]

    if op in SCALAR_OPS:
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise PredicateError(
                f"{where}: op {op!r} needs a scalar value, got {type(value).__name__}"
            )

    elif op in RANGE_OPS:
        if not _is_sequence(value) or len(value) != 2:
            raise PredicateError(f"{where}: op {op!r} needs value=[lo, hi]")
        lo, hi = value
        if not _is_number(lo) or not _is_number(hi):
            raise PredicateError(f"{where}: op {op!r} bounds must be numeric")
        if lo > hi:
            raise PredicateError(f"{where}: op {op!r} has lo > hi ({lo} > {hi})")

    elif op in PERCENTILE_OPS:
        # A fraction, not a percentage. 20 would silently select the whole
        # universe, which is the failure mode this check exists to catch.
        if not _is_number(value) or not (0 < value <= 1):
            raise PredicateError(
                f"{where}: op {op!r} needs a fraction in (0, 1], got {value!r}"
            )

    elif op in SET_OPS:
        if not _is_sequence(value) or len(value) == 0:
            raise PredicateError(f"{where}: op {op!r} needs a non-empty list")


def validate_predicates(preds: Any, *, where: str = "criterion") -> None:
    """Validate an ordered list of predicates. An empty list is allowed -- a
    strategy with no entry conditions (buy the whole ranked universe) is a
    real momentum case, not an error."""
    if not isinstance(preds, list):
        raise PredicateError(f"{where}: expected a list, got {type(preds).__name__}")
    for i, pred in enumerate(preds):
        validate_predicate(pred, where=f"{where}[{i}]")


def features_used(preds: Sequence[Dict[str, Any]]) -> List[str]:
    """Every feature column a predicate list touches, including the right-hand
    column of col-vs-col ops. Used to check a definition against the feature
    store before activating it, so a strategy gated on a column that is not
    backfilled fails loudly at registration instead of screening nothing."""
    out: List[str] = []
    for pred in preds:
        feature = pred.get("feature")
        if isinstance(feature, str) and feature not in out:
            out.append(feature)
        if pred.get("op") in COL_VS_COL_OPS:
            feature2 = pred.get("feature2")
            if isinstance(feature2, str) and feature2 not in out:
                out.append(feature2)
    return out


def screener_compatible(preds: Sequence[Dict[str, Any]]) -> bool:
    """True if every predicate can be evaluated by ScreenerEngine as-is."""
    return all(pred.get("op") not in UNSUPPORTED_BY_SCREENER for pred in preds)


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _is_sequence(v: Any) -> bool:
    return isinstance(v, (list, tuple))
